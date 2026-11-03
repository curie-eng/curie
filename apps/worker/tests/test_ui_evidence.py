"""The worker's UI evidence capture and relay at their real boundaries.

The runner side is the real ``curie_runner.create_app(..., ui_evidence_workspace=)``
over a real checkout built from the shared corpus
``tests/vectors/ui-evidence-cases.json`` (builders from
``curie_test_support.ui_evidence``, shared with the runner tests), reached
through the real ``RunnerClient``. The API side is an ``httpx.MockTransport``
stand-in behind the real ``ApprovalClient`` (whose ``upload_ui_evidence`` drives the real
``UiEvidenceApi``); it answers per the case ``fault`` and records every request,
so the assertions read the exact routes, headers and bodies of the upload.

Every case also proves the code publication path is untouched: the runner
snapshot is identical before and after capture, and relay sends nothing outside
the publication's evidence routes and never raises.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app, workspace_snapshot
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_test_support.ui_evidence import (
    CASES,
    CORPUS,
    GITHUB,
    MAX_IMAGE_BYTES,
    MAX_TOTAL_BYTES,
    REPO,
    CaseCheckout,
    build_case_checkout,
    png,
    resolve,
)
from curie_worker import ui_evidence
from curie_worker.approvals import ApprovalBackendError, ApprovalClient
from curie_worker.runner_client import RunnerClient
from curie_worker.ui_evidence import capture_ui_evidence, relay_ui_evidence

TOKEN = "runner-token-value"
API = "http://api.test"
WORKER_TOKEN = "worker-token-value"
PUBLICATION_ID = str(uuid.UUID("33333333-3333-4333-8333-333333333333"))
EVIDENCE_PREFIX = f"/v1/internal/publications/{PUBLICATION_ID}/ui-evidence"
STALL_BUDGET_S = 6.0


# --- API stand-in --------------------------------------------------------------


@dataclass
class Recorded:
    method: str
    path: str
    headers: httpx.Headers
    content: bytes

    def json(self) -> Any:
        return json.loads(self.content)


@dataclass
class ApiStandIn:
    """The API's three worker-token evidence routes, answering per a corpus fault."""

    fault: str | None = None
    requests: list[Recorded] = field(default_factory=list)
    hang_s: float = 0.0
    transport_down: bool = False

    async def handle(self, request: httpx.Request) -> httpx.Response:
        content = await request.aread()
        self.requests.append(Recorded(request.method, request.url.path, request.headers, content))
        if self.transport_down:
            raise httpx.ConnectError("api unreachable", request=request)
        if self.hang_s:
            await asyncio.sleep(self.hang_s)
        path = request.url.path
        if request.method == "PUT" and path == EVIDENCE_PREFIX:
            if self.fault == "upload_rejection":
                return httpx.Response(409, json={"detail": {"code": "ui_evidence.index_conflict"}})
            body = json.loads(content)
            expected = [
                image["ordinal"]
                for image in body["images"]
                if image["refusal"] is None and image["bound"]
            ]
            return httpx.Response(201, json={"status": "receiving", "expected": expected})
        if request.method == "PUT" and path.startswith(f"{EVIDENCE_PREFIX}/images/"):
            ordinal = int(path.rsplit("/", 1)[1])
            if self.fault == "storage" and ordinal == 1:
                return httpx.Response(
                    503, json={"detail": {"code": "ui_evidence.storage_unavailable"}}
                )
            return httpx.Response(201, json={"ordinal": ordinal, "stored": True})
        if request.method == "POST" and path == f"{EVIDENCE_PREFIX}/seal":
            return httpx.Response(200, json={"status": "ready", "stored": [], "missing": []})
        return httpx.Response(404, json={"detail": "not found"})

    def to(self, method: str, path: str) -> list[Recorded]:
        return [r for r in self.requests if r.method == method and r.path == path]


def _creator(http: httpx.AsyncClient, *, worker_token: str = WORKER_TOKEN) -> ApprovalClient:
    return ApprovalClient(
        api_base_url=API,
        api_key="",
        client=http,
        read_timeout_s=5.0,
        worker_token=worker_token,
    )


# --- Runner side ---------------------------------------------------------------


def session_runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="ui-evidence-worker-test",
    )


def runner_app(runner: SessionRunner, repo: Path) -> web.Application:
    """The production runner app with the production snapshotter and evidence workspace."""

    return create_app(
        runner,
        token=TOKEN,
        snapshotter=workspace_snapshot.workspace_snapshotter(
            repo,
            trusted_origin=GITHUB,
            repository_path=REPO,
            proposal=lambda: ("Publish the agent change", "Carries every local change."),
        ),
        ui_evidence_workspace=repo,
    )


Wrap = Callable[[RunnerClient, CaseCheckout], None]


@dataclass
class Run:
    capture: Any
    api: ApiStandIn
    snapshots: tuple[tuple[bytes, tuple[str, ...]], tuple[bytes, tuple[str, ...]]]
    image_calls: list[int]
    elapsed_s: float


def _stall_middleware(release: asyncio.Event) -> Any:
    @web.middleware
    async def stall(request: web.Request, handler: Any) -> web.StreamResponse:
        if request.path == "/v1/ui-evidence/image":
            try:
                await asyncio.wait_for(release.wait(), timeout=30)
            except TimeoutError:
                pass
        return await handler(request)

    return stall


def _record_image_calls(client: RunnerClient, calls: list[int]) -> None:
    real = client.ui_evidence_image

    async def recording(*args: Any, **kwargs: Any) -> Any:
        calls.append(int(kwargs["index"]))
        return await real(*args, **kwargs)

    client.ui_evidence_image = recording  # type: ignore[method-assign]


def _wrap_index(
    client: RunnerClient, after: Callable[[dict[str, Any]], Awaitable[None] | None]
) -> None:
    real = client.ui_evidence_index

    async def wrapped(*args: Any, **kwargs: Any) -> dict[str, Any]:
        index = await real(*args, **kwargs)
        result = after(index)
        if asyncio.iscoroutine(result):
            await result
        return index

    client.ui_evidence_index = wrapped  # type: ignore[method-assign]


def _drive(
    checkout: CaseCheckout,
    *,
    fault: str | None = None,
    wrap: Wrap | None = None,
    stall: bool = False,
    remaining_s: float | None = None,
    api: ApiStandIn | None = None,
) -> Run:
    api = api if api is not None else ApiStandIn(fault=fault)

    async def go() -> Run:
        runner = session_runner()
        await runner.start()
        app = runner_app(runner, checkout.repo)
        release = asyncio.Event()
        if stall:
            app.middlewares.append(_stall_middleware(release))
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        image_calls: list[int] = []
        try:
            base_url = f"http://127.0.0.1:{server.port}"
            first = await client.snapshot(base_url, token=TOKEN, base_sha=checkout.base)
            _record_image_calls(client, image_calls)
            if wrap is not None:
                wrap(client, checkout)
            started = time.monotonic()
            capture = await capture_ui_evidence(
                client, base_url, TOKEN, base_sha=checkout.base, remaining_s=remaining_s
            )
            elapsed = time.monotonic() - started
            release.set()
            second = await client.snapshot(base_url, token=TOKEN, base_sha=checkout.base)
            async with httpx.AsyncClient(transport=httpx.MockTransport(api.handle)) as http:
                await relay_ui_evidence(_creator(http), PUBLICATION_ID, capture)
        finally:
            release.set()
            await client.close()
            await server.close()
        return Run(
            capture=capture,
            api=api,
            snapshots=(
                (first.patch, tuple(first.changed_paths)),
                (second.patch, tuple(second.changed_paths)),
            ),
            image_calls=image_calls,
            elapsed_s=elapsed,
        )

    return asyncio.run(go())


def _rewrite_after_image(client: RunnerClient, checkout: CaseCheckout) -> None:
    def rewrite(_index: dict[str, Any]) -> None:
        (checkout.evidence / "after" / "home.png").write_bytes(png("rewritten"))

    _wrap_index(client, rewrite)


def _uploadable(index: dict[str, Any]) -> list[dict[str, Any]]:
    return [image for image in index["images"] if image["refusal"] is None and image["bound"]]


def _assert_upload_shape(run: Run, expected: dict[str, Any], checkout: CaseCheckout) -> None:
    api = run.api
    assert api.requests, "nothing was uploaded"
    for request in api.requests:
        assert request.path.startswith(EVIDENCE_PREFIX), request.path
        assert request.headers["X-Curie-Worker-Token"] == WORKER_TOKEN

    first = api.requests[0]
    assert (first.method, first.path) == ("PUT", EVIDENCE_PREFIX)
    assert first.headers["Content-Type"].startswith("application/json")
    assert first.json() == expected
    assert len(api.to("PUT", EVIDENCE_PREFIX)) == 1

    for image in _uploadable(expected):
        puts = api.to("PUT", f"{EVIDENCE_PREFIX}/images/{image['ordinal']}")
        if api.fault == "upload_rejection":
            assert puts == []
            continue
        assert puts, image
        data = checkout.originals[image["path"]]
        for put in puts:
            assert put.content == data
            assert put.headers["Content-Type"] == "image/png"
            assert put.headers["X-Curie-Evidence-Sha256"] == hashlib.sha256(data).hexdigest()
            assert int(put.headers["Content-Length"]) == len(data)

    # Nothing is uploaded for an entry the worker did not fetch.
    fetched = {image["ordinal"] for image in _uploadable(expected)}
    for request in api.requests:
        if "/images/" in request.path:
            assert int(request.path.rsplit("/", 1)[1]) in fetched


# --- Corpus cases --------------------------------------------------------------


@pytest.mark.parametrize("case_id", [case["id"] for case in CORPUS["cases"] if "index" in case])
def test_corpus_case_at_the_worker(
    case_id: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = CASES[case_id]
    checkout = build_case_checkout(tmp_path / "case", case)
    kind = (case.get("fault") or {}).get("kind")
    expected = resolve(case["index"], checkout)

    wrap: Wrap | None = _rewrite_after_image if kind == "transfer" else None
    stall = kind == "capture_timeout"
    if stall:
        monkeypatch.setattr(ui_evidence, "CAPTURE_BUDGET_S", STALL_BUDGET_S)

    run = _drive(checkout, fault=kind, wrap=wrap, stall=stall)

    # code_pr_unchanged: capture never moves the code snapshot, relay never raises
    # (it returned) and never reaches any route but this publication's evidence.
    assert case["code_pr_unchanged"] is True
    assert run.snapshots[0] == run.snapshots[1]
    assert not any(".curie/ui-evidence" in path for path in run.snapshots[0][1])

    if expected is None:
        assert run.capture is None
        assert run.api.requests == []
        return

    assert run.capture is not None
    assert run.capture.index == expected
    assert all(handle.closed for handle in run.capture.files.values())
    _assert_upload_shape(run, expected, checkout)

    if stall:
        assert run.elapsed_s < STALL_BUDGET_S + 10
    if kind == "upload_rejection":
        # A 4xx stops the upload: no retry, no image, no seal.
        assert [(r.method, r.path) for r in run.api.requests] == [("PUT", EVIDENCE_PREFIX)]
        return
    if kind == "storage":
        # 5xx is retried within the budget; the other image still stores once.
        assert len(run.api.to("PUT", f"{EVIDENCE_PREFIX}/images/1")) >= 2
        assert len(run.api.to("PUT", f"{EVIDENCE_PREFIX}/images/0")) == 1
        return

    sequence = [(r.method, r.path) for r in run.api.requests]
    image_puts = [
        ("PUT", f"{EVIDENCE_PREFIX}/images/{image['ordinal']}") for image in _uploadable(expected)
    ]
    assert sequence == [
        ("PUT", EVIDENCE_PREFIX),
        *image_puts,
        ("POST", f"{EVIDENCE_PREFIX}/seal"),
    ]
    assert run.api.requests[-1].json() == {}


# --- The worker's own rules ----------------------------------------------------


@pytest.mark.parametrize(
    "case_id",
    [
        "unbound-before",
        "unbound-after-dirty-head-only",
        "unbound-after-dirty-unknown-head-only",
        "unbound-after-clean-claim-on-dirty-checkout",
        "complete",
    ],
)
def test_the_worker_rebinds_a_runner_claim_it_cannot_confirm(case_id: str, tmp_path: Path) -> None:
    """A runner index that claims every image bound is re-judged by the worker's rule."""

    case = CASES[case_id]
    checkout = build_case_checkout(tmp_path / "case", case)
    expected = resolve(case["index"], checkout)

    def lie(client: RunnerClient, _checkout: CaseCheckout) -> None:
        def claim_bound(index: dict[str, Any]) -> None:
            for image in index["images"]:
                if image["refusal"] == "unbound":
                    image["refusal"] = None
                    image["bound"] = True

        _wrap_index(client, claim_bound)

    run = _drive(checkout, wrap=lie)

    assert run.capture.index == expected
    unbound = {image["ordinal"] for image in expected["images"] if image["refusal"] == "unbound"}
    assert unbound.isdisjoint(run.image_calls)
    # Liveness: every image the rule does bind is fetched and uploaded.
    assert sorted(run.image_calls) == [image["ordinal"] for image in _uploadable(expected)]
    _assert_upload_shape(run, expected, checkout)


def _total_case() -> dict[str, Any]:
    count = MAX_TOTAL_BYTES // MAX_IMAGE_BYTES
    files: dict[str, Any] = {
        f"before/full-{number:02d}.png": {"png": "valid", "size": MAX_IMAGE_BYTES}
        for number in range(count)
    }
    files["before/one-more.png"] = {"png": "valid"}
    return {
        "fixture": {
            "checkout": "dirty",
            "manifest": {
                "version": 1,
                "status": "complete",
                "images": [
                    {"path": path, "label": f"Shot {number}", "state": "before"}
                    for number, path in enumerate(files)
                ],
            },
            "manifest_raw": None,
            "base": "P",
            "head": None,
            "candidate_dirty": True,
            "tree": "C",
            "files": files,
        }
    }


@pytest.mark.parametrize("runner_reports_the_limit", [True, False])
def test_capture_total_is_bounded_to_100_mib(
    tmp_path: Path, runner_reports_the_limit: bool
) -> None:
    checkout = build_case_checkout(tmp_path / "case", _total_case())
    count = MAX_TOTAL_BYTES // MAX_IMAGE_BYTES

    def drop_the_limit(client: RunnerClient, _checkout: CaseCheckout) -> None:
        def admit(index: dict[str, Any]) -> None:
            for image in index["images"]:
                if image["refusal"] == "total_limit":
                    image["refusal"] = None

        _wrap_index(client, admit)

    run = _drive(checkout, wrap=None if runner_reports_the_limit else drop_the_limit)

    images = run.capture.index["images"]
    assert len(images) == count + 1
    # Liveness: exactly 100 MiB is fetched whole.
    assert [image["refusal"] for image in images[:count]] == [None] * count
    assert sorted(run.image_calls) == list(range(count))
    assert sum(image["size"] for image in images[:count]) == MAX_TOTAL_BYTES
    # The next image passes the total and is never fetched, whatever the runner said.
    assert images[count]["refusal"] == "total_limit"
    assert count not in run.image_calls
    stored = [len(request.content) for request in run.api.requests if "/images/" in request.path]
    assert sum(stored) == MAX_TOTAL_BYTES


def test_capture_skips_when_the_attempt_budget_is_spent(tmp_path: Path) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])
    calls: list[str] = []

    def count_index(client: RunnerClient, _checkout: CaseCheckout) -> None:
        _wrap_index(client, lambda _index: calls.append("index"))

    spent = _drive(checkout, wrap=count_index, remaining_s=5.0)
    assert spent.capture is None
    assert calls == []
    assert spent.image_calls == []
    assert spent.api.requests == []

    # Liveness: a realistic remaining budget captures.
    live = _drive(checkout, wrap=count_index, remaining_s=60.0)
    assert calls == ["index"]
    assert live.capture is not None
    assert sorted(live.image_calls) == [0, 1]


# --- Relay is best effort ------------------------------------------------------


class _RaisingCreator:
    def __init__(self) -> None:
        self.calls = 0

    async def upload_ui_evidence(self, *_args: object, **_kwargs: object) -> None:
        self.calls += 1
        raise RuntimeError("evidence upload exploded")


class _HangingCreator:
    def __init__(self) -> None:
        self.calls = 0

    async def upload_ui_evidence(self, *_args: object, **_kwargs: object) -> None:
        self.calls += 1
        await asyncio.sleep(3600)


class _CreatorWithoutUpload:
    async def create_publication(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("relay must never create a publication")


def _fresh_capture(tmp_path: Path) -> tuple[Any, CaseCheckout]:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])

    async def go() -> Any:
        runner = session_runner()
        await runner.start()
        server = TestServer(runner_app(runner, checkout.repo))
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            return await capture_ui_evidence(
                client,
                f"http://127.0.0.1:{server.port}",
                TOKEN,
                base_sha=checkout.base,
                remaining_s=None,
            )
        finally:
            await client.close()
            await server.close()

    capture = asyncio.run(go())
    assert capture is not None
    assert sorted(capture.files) == [0, 1]
    return capture, checkout


@pytest.mark.parametrize(
    "variant",
    ["raises", "hangs", "rejected", "api_unreachable", "no_worker_token", "no_upload_method"],
)
def test_relay_never_raises_and_closes_its_files(
    variant: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture, _checkout = _fresh_capture(tmp_path)
    monkeypatch.setattr(ui_evidence, "UPLOAD_BUDGET_S", 1.0)
    api = ApiStandIn(
        fault="upload_rejection" if variant == "rejected" else None,
        transport_down=variant == "api_unreachable",
    )

    async def go() -> float:
        async with httpx.AsyncClient(transport=httpx.MockTransport(api.handle)) as http:
            creator: object
            if variant == "raises":
                creator = _RaisingCreator()
            elif variant == "hangs":
                creator = _HangingCreator()
            elif variant == "no_upload_method":
                creator = _CreatorWithoutUpload()
            elif variant == "no_worker_token":
                creator = _creator(http, worker_token="")
            else:
                creator = _creator(http)
            started = time.monotonic()
            assert await relay_ui_evidence(creator, PUBLICATION_ID, capture) is None
            if isinstance(creator, (_RaisingCreator, _HangingCreator)):
                assert creator.calls == 1
            return time.monotonic() - started

    elapsed = asyncio.run(go())

    assert elapsed < 10
    assert all(handle.closed for handle in capture.files.values())
    if variant == "no_worker_token":
        assert api.requests == []
    if variant == "rejected":
        assert [(r.method, r.path) for r in api.requests] == [("PUT", EVIDENCE_PREFIX)]


def test_an_approval_client_without_a_worker_token_refuses_the_upload(tmp_path: Path) -> None:
    capture, _checkout = _fresh_capture(tmp_path)
    api = ApiStandIn()

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(api.handle)) as http:
            with pytest.raises(ApprovalBackendError):
                await _creator(http, worker_token="").upload_ui_evidence(
                    PUBLICATION_ID, capture, budget_s=5.0
                )

    try:
        asyncio.run(go())
    finally:
        capture.close()
    assert api.requests == []


def test_relay_of_no_capture_sends_nothing() -> None:
    api = ApiStandIn()

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(api.handle)) as http:
            assert await relay_ui_evidence(_creator(http), PUBLICATION_ID, None) is None

    asyncio.run(go())
    assert api.requests == []
