"""Harness for the eval-runner tests: a fake runner that answers eval_case turns
with a scripted per-input output, dialed by the real RunnerClient. Only the model
behind the runner is faked; the eval runner and grading are exercised for real.

The F3 eval-stream consumer tests additionally use a real RustFS bundle (uploaded
via ``bundles``) and, for the provisioned-runner path, the real G1 substrate wired
to a fake Kubernetes client whose sandbox resolves to the in-process fake runner.
Only the report HTTP POST is mocked (the external-service rule); Valkey, RustFS,
and Langfuse are always real."""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import uuid
import zipfile
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import pytest
from aci_protocol import Final, SessionStatus, ToolNote
from aci_protocol.s3 import build_s3_client
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.eval import EvalSuite
from curie_worker.runner_client import RunnerClient

# The committed cross-language eval-case fixture, shared by the model and stream
# tests. conftest.py -> parents[2] is apps/worker.
EVAL_CASES_EXAMPLE_PATH = (
    Path(__file__).resolve().parents[2] / "schema" / "eval-cases.example.json"
)


@pytest.fixture
def eval_cases_example_path() -> Path:
    return EVAL_CASES_EXAMPLE_PATH


_RUSTFS: dict[str, object] = {
    "s3_endpoint_url": os.environ.get("TEST_S3_ENDPOINT_URL", "http://localhost:29000"),
    "s3_access_key": os.environ.get("TEST_S3_ACCESS_KEY", "rustfs"),
    "s3_secret_key": os.environ.get("TEST_S3_SECRET_KEY", "rustfssecret"),
    "s3_region": "us-east-1",
    "bundle_bucket": os.environ.get("TEST_BUNDLE_BUCKET", "curie-bundles"),
}


class FakeEvalRunner:
    """Answers /v1/event with ``responses[input]`` (or 500 for ``fail_inputs``).

    Models a single long-lived conversation the way the real runner does: every
    delivered ``/v1/event`` input is appended to ``history`` and ``POST /v1/reset``
    clears it. An input listed in ``recall_inputs`` answers with the joined
    conversation history *so far* rather than a canned response, so a test can
    prove a case answering from a prior case's history (the #550 false green) --
    and prove that the per-case reset now clears that history.
    """

    def __init__(self) -> None:
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/v1/event", self._event),
                web.post("/v1/reset", self._reset),
                web.get("/status", self._status),
                web.get("/v1/status", self._status),
            ]
        )
        self.responses: dict[str, str] = {}
        # Inputs held in-flight until the test releases the event. This drives
        # lifecycle assertions across a real inline eval handler without
        # replacing the runner HTTP path.
        self.hold_inputs: dict[str, asyncio.Event] = {}
        self.fail_inputs: set[str] = set()
        # Inputs whose turn ends with a classified-failure final (budget/model
        # error) while still carrying text in responses[input].
        self.classified_failure_inputs: set[str] = set()
        # Inputs whose turn ends idle-awaiting-input (an incomplete turn) while
        # still carrying text in responses[input].
        self.idle_inputs: set[str] = set()
        # Inputs whose turn ends awaiting-approval (the gate held) while still
        # carrying text in responses[input].
        self.awaiting_approval_inputs: set[str] = set()
        # Per-input tool-call trajectory: the ordered tool names emitted as
        # tool_note frames before the final, so scorer-seam tests can drive the
        # tool-call sequence a turn produced.
        self.tool_calls: dict[str, list[str]] = {}
        # Per-input token usage stamped on the final ((input_tokens, output_tokens)),
        # so cost-attribution tests (#390) can drive a turn's usage. Absent inputs
        # emit a final with no usage, modelling a provider that reported none.
        self.usage: dict[str, tuple[int, int]] = {}
        # Per-input output sequence consumed one entry per delivery (clamped to the
        # last), so multi-sample tests (#332) can drive a flaky case that answers
        # differently across samples. Takes precedence over ``responses``.
        self.output_sequence: dict[str, list[str]] = {}
        self._sequence_calls: dict[str, int] = {}
        self.default_output = ""
        self.seen: list[dict[str, str]] = []
        # The accumulated conversation: every delivered input, cleared by reset.
        self.history: list[str] = []
        # Inputs that answer from `history` (prior inputs joined) instead of a
        # canned response -- the memory-recall shape #550 is about.
        self.recall_inputs: set[str] = set()
        # How many times /v1/reset was called (isolation assertions).
        self.resets = 0
        # When True, /v1/reset answers 500 -- models a runner that could not
        # establish per-case isolation.
        self.fail_reset = False
        # Inputs that keep the HTTP handler open after a valid Final. This is a
        # real transport-boundary probe for consumers that naturally iterate to
        # stream end instead of breaking at Final themselves.
        self.post_final_stall_inputs: set[str] = set()
        self.post_final_started = asyncio.Event()
        self.post_final_unblock = asyncio.Event()
        self.post_final_handler_done = asyncio.Event()

    async def _status(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "done", "turn_active": False})

    async def _reset(self, _request: web.Request) -> web.Response:
        self.resets += 1
        if self.fail_reset:
            return web.json_response({"error": "reset boom"}, status=500)
        self.history.clear()
        return web.json_response({"ok": True})

    async def _event(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        self.seen.append(body)
        text = body["text"]
        hold = self.hold_inputs.get(text)
        if hold is not None:
            await hold.wait()
        if text in self.fail_inputs:
            return web.json_response({"error": "boom"}, status=500)
        # A recall input answers from the conversation so far (the history that a
        # reset would have cleared); otherwise a per-sample output sequence (#332),
        # else the canned response.
        if text in self.recall_inputs:
            output = " | ".join(self.history)
        elif text in self.output_sequence:
            seq = self.output_sequence[text]
            idx = min(self._sequence_calls.get(text, 0), len(seq) - 1)
            self._sequence_calls[text] = idx + 1
            output = seq[idx]
        else:
            output = self.responses.get(text, self.default_output)
        self.history.append(text)
        if text in self.classified_failure_inputs:
            status = SessionStatus.CLASSIFIED_FAILURE
        elif text in self.idle_inputs:
            status = SessionStatus.IDLE_AWAITING_INPUT
        elif text in self.awaiting_approval_inputs:
            status = SessionStatus.AWAITING_APPROVAL
        else:
            status = SessionStatus.DONE
        resp = web.StreamResponse(status=200, headers={"Content-Type": "application/x-ndjson"})
        await resp.prepare(request)
        for tool in self.tool_calls.get(text, []):
            note = ToolNote(text=f"calling {tool}", tool=tool)
            await resp.write((note.model_dump_json() + "\n").encode("utf-8"))
        usage = self.usage.get(text)
        frame = Final(
            text=output,
            status=status,
            input_tokens=usage[0] if usage else None,
            output_tokens=usage[1] if usage else None,
        )
        await resp.write((frame.model_dump_json() + "\n").encode("utf-8"))
        if text in self.post_final_stall_inputs:
            self.post_final_started.set()
            try:
                await self.post_final_unblock.wait()
                await resp.write_eof()
            except (ConnectionError, OSError):
                # The client's bounded cleanup deliberately releases a peer
                # that remains stalled after its terminal frame.
                pass
            finally:
                self.post_final_handler_done.set()
            return resp
        await resp.write_eof()
        return resp


@contextlib.asynccontextmanager
async def _eval_harness() -> AsyncIterator[tuple[str, FakeEvalRunner, RunnerClient]]:
    fake = FakeEvalRunner()
    server = TestServer(fake.app)
    await server.start_server()
    base_url = f"http://127.0.0.1:{server.port}"
    client = RunnerClient(total_timeout_s=30.0)
    try:
        yield base_url, fake, client
    finally:
        with contextlib.suppress(Exception):
            await client.close()
        with contextlib.suppress(Exception):
            await server.close()


@pytest.fixture
def make_eval_harness() -> Callable[
    [], contextlib.AbstractAsyncContextManager[tuple[str, FakeEvalRunner, RunnerClient]]
]:
    def factory() -> (
        contextlib.AbstractAsyncContextManager[tuple[str, FakeEvalRunner, RunnerClient]]
    ):
        return _eval_harness()

    return factory


# --- Real RustFS bundle fixtures (the consumer loads suites from the bundle) ----


def bundle_zip(suite: EvalSuite) -> bytes:
    """A minimal plugin bundle: a zip carrying the suite at ``evals/cases.json``
    (the same layout the consumer's bundle loader reads)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("evals/cases.json", suite.model_dump_json())
    return buf.getvalue()


@pytest.fixture
def bundles() -> Iterator[tuple[BundleStore, Callable[[EvalSuite | bytes], str]]]:
    """A real read-only ``BundleStore`` plus an ``upload(suite_or_bytes) -> key``
    helper. Uploaded objects are deleted on teardown; skips if RustFS is down."""
    cfg = WorkerConfig(**_RUSTFS)  # type: ignore[arg-type]
    store = BundleStore(cfg)
    client = build_s3_client(
        endpoint_url=cfg.s3_endpoint_url,
        access_key=cfg.s3_access_key,
        secret_key=cfg.s3_secret_key,
        region=cfg.s3_region,
    )
    try:
        client.head_bucket(Bucket=cfg.bundle_bucket)
    except Exception as exc:  # noqa: BLE001 - any S3 failure means RustFS is unusable
        pytest.skip(f"RustFS bundle bucket not reachable: {exc}")
    keys: list[str] = []

    def upload(suite: EvalSuite | bytes) -> str:
        data = suite if isinstance(suite, bytes) else bundle_zip(suite)
        key = f"tests/bundles/{uuid.uuid4().hex}.zip"
        client.put_object(Bucket=cfg.bundle_bucket, Key=key, Body=data)
        keys.append(key)
        return key

    yield store, upload

    for key in keys:
        with contextlib.suppress(Exception):
            client.delete_object(Bucket=cfg.bundle_bucket, Key=key)
