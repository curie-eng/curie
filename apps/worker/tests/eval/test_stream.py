"""F3 eval-stream consumer against real Valkey + real RustFS + real Langfuse, with
only the platform-report HTTP POST mocked (the external-service rule).

The consumer reads ``curie:evals``, loads the suite from the version's RustFS
bundle, runs it against either the payload's ``target_url`` (the dev/test shortcut)
or a runner it provisions via the G1 substrate, records per-case scores to
Langfuse, POSTs a summary to the platform API, and only then acks. These tests
provoke each contract: the full seam cycle, the poison-pill drop, a missing-bundle
failed run, ack-after-report even when the report terminally fails, and a
provisioned-runner end-to-end (no ``target_url``) that tears the sandbox down.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import tarfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    operation_span,
    record_metric,
)
from curie_test_support.valkey import (
    VALKEY_HOST as _VH,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VP,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VPW,
)
from curie_worker import stream_consumer as stream_consumer_module
from curie_worker.binding import BUDGET_ENV, BUNDLE_REF_ENV, MODEL_ENV, THINKING_ENV
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.eval import (
    EvalCase,
    EvalJob,
    EvalReporter,
    EvalStreamConsumer,
    EvalSuite,
    Grader,
    GraderKind,
    LangfuseEvalRecorder,
    load_suite_from_bundle,
)
from curie_worker.eval import stream as eval_stream_module
from curie_worker.eval.models import EvalCaseResult, EvalOutcome, EvalRunResult
from curie_worker.sandbox import AffinityStore, SandboxSubstrate, SubstrateConfig
from curie_worker.sandbox.types import ClaimView, SandboxView
from opentelemetry import trace
from redis.asyncio import Redis as AsyncRedis

CONTAINS = GraderKind.CONTAINS


async def _wait_until(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


class _StubRepo:
    """The B1 repo lookup, stubbed: a channel/agent resolves to a GitHub repo."""

    def __init__(self, *, thinking: str | None = None) -> None:
        self._thinking = thinking
        self.thinking_agent_id: uuid.UUID | None = None

    async def repo_full_name(self, _agent_id: uuid.UUID) -> str:
        return "owner/repo"

    async def secrets_for(self, _agent_id: uuid.UUID) -> dict[str, str] | None:
        # No connector secrets in the eval stub; a real BindingResolver returns
        # the agent's secrets so an authed-MCP bundle authenticates during eval.
        return None

    async def thinking_for(self, agent_id: uuid.UUID) -> str | None:
        self.thinking_agent_id = agent_id
        return self._thinking


class _UnusedSubstrate:
    """A substrate that must never be touched. Passed to the target_url tests so a
    provisioning call is a hard failure, proving the shortcut path bypasses G1."""

    def claim(self, *_args: object, **_kwargs: object) -> Any:
        raise AssertionError("target_url path must not provision a sandbox")

    def release(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("target_url path must not release a sandbox")


# --- Fake Kubernetes client for the provisioned-runner test -------------------
# Sandboxes resolve to 127.0.0.1 so the real RunnerClient dials the in-process
# fake eval runner (only the model behind the runner is faked).


@dataclass
class _FakeClaim:
    name: str
    sandbox_name: str
    labels: dict[str, str]
    env: dict[str, str]


@dataclass
class _FakeK8s:
    namespace: str = "test-ns"
    claims: dict[str, _FakeClaim] = field(default_factory=dict)
    claim_envs: list[dict[str, str]] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.claim_envs.append(dict(env or {}))
        self.claims[name] = _FakeClaim(
            name=name,
            sandbox_name=f"sbx-{name}",
            labels={"curietech.ai/managed-by": "curie-sandbox-substrate", **(labels or {})},
            env=dict(env or {}),
        )

    def get_claim(self, name: str) -> ClaimView | None:
        claim = self.claims.get(name)
        if claim is None:
            return None
        # This suite never reaps; the claim is simply as new as it looks.
        return ClaimView(
            name=claim.name,
            ready=True,
            sandbox_name=claim.sandbox_name,
            created_at=datetime.now(UTC),
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
        )

    def delete_claim(self, name: str) -> None:
        self.claims.pop(name, None)
        self.deleted.append(name)

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        key, _, value = label_selector.partition("=")
        out = []
        for claim in self.claims.values():
            if claim.labels.get(key) == value:
                view = self.get_claim(claim.name)
                assert view is not None
                out.append(view)
        return out

    def get_sandbox(self, name: str) -> SandboxView | None:
        if not any(c.sandbox_name == name for c in self.claims.values()):
            return None
        return SandboxView(
            name=name, ready=True, service_fqdn="127.0.0.1", operating_mode="Running"
        )

    def set_sandbox_mode(self, name: str, mode: str) -> None:  # pragma: no cover - unused here
        pass


def _cfg(stream: str, group: str, **overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "valkey_host": _VH,
        "valkey_port": _VP,
        "valkey_password": _VPW,
        "eval_stream": stream,
        "eval_consumer_group": group,
        "read_block_ms": 100,
    }
    base.update(overrides)
    return WorkerConfig(**base)


def _item(
    *,
    suite: str,
    sha: str,
    bundle_ref: str | None,
    target_url: str | None,
    model: str | None = None,
) -> EvalJob:
    return EvalJob(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        sha=sha,
        suite=suite,
        bundle_ref=bundle_ref,
        target_url=target_url,
        model=model,
        requested_at="2026-07-05T00:00:00+00:00",
    )


def _build_consumer(
    *,
    redis_client: AsyncRedis,
    cfg: WorkerConfig,
    bundle_store: BundleStore,
    substrate: Any,
    reports: list[dict[str, Any]],
    lf_client: httpx.AsyncClient,
    report_status: int = 200,
    repo_lookup: _StubRepo | None = None,
) -> EvalStreamConsumer:
    def handler(request: httpx.Request) -> httpx.Response:
        reports.append(json.loads(request.content))
        return httpx.Response(report_status, json={"ok": report_status < 400})

    report_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reporter = EvalReporter(
        api_base_url="http://api.local",
        api_key="k",
        client=report_client,
        max_attempts=2,
        backoff_base_s=0.001,
    )
    recorder = LangfuseEvalRecorder(
        base_url=cfg.langfuse_host,
        public_key=cfg.langfuse_public_key,
        secret_key=cfg.langfuse_secret_key,
        client=lf_client,
    )
    return EvalStreamConsumer(
        redis=redis_client,
        config=cfg,
        bundle_store=bundle_store,
        substrate=substrate,
        reporter=reporter,
        recorder=recorder,
        repo_lookup=repo_lookup or _StubRepo(),
    )


async def _drain_one(consumer: EvalStreamConsumer, reports: list[dict[str, Any]]) -> None:
    task = asyncio.create_task(consumer.run())
    try:
        await _wait_until(lambda: bool(reports))
    finally:
        consumer.request_stop()
        await task


def test_eval_read_loop_demotes_idle_timeout_to_debug(make_eval_harness, bundles, caplog) -> None:
    """Mirror of the runs consumer: a blocking-read TimeoutError (routine idle) is
    logged at DEBUG, a ConnectionError (real fault) at WARNING; both back off and
    keep the eval loop alive."""
    store, _upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (_base_url, _fake, _client):
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()

                calls = {"n": 0}

                async def flaky(*args: object, **kwargs: object) -> object:
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise redis.exceptions.TimeoutError("idle eval read timeout")
                    if calls["n"] == 2:
                        raise redis.exceptions.ConnectionError("eval connection blip")
                    consumer.request_stop()  # nothing queued; stop after both faults
                    return []

                consumer._redis.xreadgroup = flaky  # type: ignore[method-assign,assignment]

                with caplog.at_level(logging.DEBUG, logger="curie_worker.eval.stream"):
                    await consumer.run()

                assert calls["n"] >= 3  # retried past both injected faults
                recs = [r for r in caplog.records if r.name == "curie_worker.eval.stream"]
                timeout_recs = [r for r in recs if "idle eval read timeout" in r.getMessage()]
                conn_recs = [r for r in recs if "eval connection blip" in r.getMessage()]
                assert timeout_recs and all(r.levelno == logging.DEBUG for r in timeout_recs)
                assert conn_recs and all(r.levelno == logging.WARNING for r in conn_recs)

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


def test_seam_full_consume_eval_report_cycle(make_eval_harness, bundles) -> None:
    """XADD the exact stream payload -> one full consume->eval->report cycle: the
    suite is loaded from the real RustFS bundle, run against target_url, scored to
    Langfuse keyed by version, reported with resolved repo + counts, and acked."""
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            fake.responses = {"2+2": "the answer is 4", "cap of france": "London"}
            bundle_ref = upload(
                EvalSuite(
                    name="basics",
                    cases=[
                        EvalCase(id="m", input="2+2", grader=Grader(kind=CONTAINS, expected="4")),
                        EvalCase(
                            id="g",
                            input="cap of france",
                            grader=Grader(kind=CONTAINS, expected="Paris"),
                        ),
                    ],
                )
            )
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                sha = f"sha-{token}"
                item = _item(suite="basics", sha=sha, bundle_ref=bundle_ref, target_url=base_url)
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                await _drain_one(consumer, reports)

                # The exact payload drove one full cycle: suite ran (1/2 passed),
                # the report carries the resolved repo + sha + counts, entry acked.
                assert reports[0]["repo_full_name"] == "owner/repo"
                assert reports[0]["sha"] == sha
                assert reports[0]["passed_count"] == 1
                assert reports[0]["total"] == 2
                assert reports[0]["target_url"] == base_url
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0

                # Scores were recorded to the real Langfuse keyed by the version tag.
                await _assert_langfuse_traces(lf_client, cfg, sha, expected=2)

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


def test_payload_with_unknown_field_still_runs_and_is_not_dropped(
    make_eval_harness, bundles
) -> None:
    """A newer API adding an optional field must not poison the job.

    The decode at the consumer is a wire READ, so it ignores fields it does not
    model. If it rejected them, the raise would land in the poison-pill branch,
    which acks and DROPS the entry with no dead letter and no page -- every
    in-flight eval destroyed by one forward-compatible field. Driven through the
    real stream decode, not the model, because that branch is the blast radius.
    """
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            fake.responses = {"2+2": "the answer is 4"}
            bundle_ref = upload(
                EvalSuite(
                    name="basics",
                    cases=[
                        EvalCase(id="m", input="2+2", grader=Grader(kind=CONTAINS, expected="4")),
                    ],
                )
            )
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                sha = f"sha-{token}"
                item = _item(suite="basics", sha=sha, bundle_ref=bundle_ref, target_url=base_url)
                # Exactly what a newer API would XADD: the payload this build
                # models, plus one field it does not.
                payload = json.loads(item.model_dump_json())
                payload["future_field"] = "from a newer api"
                await client.xadd(cfg.eval_stream, {"payload": json.dumps(payload)})

                await _drain_one(consumer, reports)

                # Ran to completion and reported: not silently acked as poison.
                assert reports and reports[0]["sha"] == sha
                assert reports[0]["passed_count"] == 1
                assert reports[0]["total"] == 1
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


def test_malformed_payload_is_dead_lettered_not_dropped(make_eval_harness, bundles) -> None:
    """A payload that will never parse on any redelivery is logged, dead-lettered
    to the eval graveyard, and acked -- never reported, never stuck pending, and
    (unlike the old bare-ack drop) observable in <eval_stream>:dead (#535)."""
    store, _upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (_base_url, _fake, _client):
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                await client.xadd(cfg.eval_stream, {"payload": "not valid json {"})

                task = asyncio.create_task(consumer.run())
                await asyncio.sleep(0.5)  # poison dead-letters fast; give a few cycles
                consumer.request_stop()
                await task

                assert reports == []  # never reported
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0  # acked, not stuck pending

                grave = cfg.eval_dead_letter_stream_name()
                rows = await client.xrange(grave)
                assert len(rows) == 1
                fields = rows[0][1]
                assert fields["dl_reason"] == "unparseable"
                assert fields["payload"] == "not valid json {"  # original recoverable

            await client.delete(cfg.eval_stream)
            await client.delete(cfg.eval_dead_letter_stream_name())
            await client.aclose()

    asyncio.run(go())


def test_over_cap_eval_is_dead_lettered_and_never_re_run(make_eval_harness, bundles) -> None:
    """An eval that has exhausted its delivery budget is dead-lettered to
    <eval_stream>:dead and acked off the group, so the reclaim loop never claims
    and re-provisions it again (#535). Without the cap a permanently-failing eval
    is re-run every tick, each redelivery burning a sandbox claim + an LLM suite."""
    store, _upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (_base_url, _fake, _client):
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(
                f"test:evals:{token}",
                f"g-{token}",
                max_delivery=2,
                reclaim_min_idle_ms=0,
            )
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                # The entry is dead-lettered before it is ever handled, so its
                # bundle/target values are immaterial.
                item = _item(
                    suite="recl", sha=f"sha-{token}", bundle_ref=None, target_url=_base_url
                )
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                # Drive the pre-claim delivery count to the cap: read it once
                # (delivered=1), then XCLAIM once more (delivered=2 == max_delivery).
                await client.xreadgroup(
                    cfg.eval_consumer_group, "dead-1", {cfg.eval_stream: ">"}, count=10
                )
                pending = await client.xpending_range(
                    cfg.eval_stream, cfg.eval_consumer_group, min="-", max="+", count=10
                )
                entry_id = str(pending[0]["message_id"])
                await client.xclaim(
                    cfg.eval_stream, cfg.eval_consumer_group, "dead-2", 0, [entry_id]
                )

                over_cap = await consumer._dead_letter_over_cap()
                assert over_cap == {entry_id}

                # Acked off the group (never re-dispatched) and in the graveyard.
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0
                grave = cfg.eval_dead_letter_stream_name()
                rows = await client.xrange(grave)
                assert len(rows) == 1
                assert rows[0][1]["dl_reason"] == "max-delivery-exceeded"
                assert int(rows[0][1]["dl_delivery_count"]) >= cfg.max_delivery

                # A subsequent reclaim never re-runs it (no report, no provision):
                # it is acked off the group, so XAUTOCLAIM finds nothing to claim.
                assert await consumer._reclaim_once() == 0
                assert reports == []

            await client.delete(cfg.eval_stream)
            await client.delete(cfg.eval_dead_letter_stream_name())
            await client.aclose()

    asyncio.run(go())


def test_missing_bundle_is_a_reported_failed_run(make_eval_harness, bundles) -> None:
    """A bundle_ref that does not exist in RustFS is an unresolvable suite: a failed
    run (0/0) is reported and the entry acked, never a consumer crash."""
    store, _upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (_base_url, _fake, _client):
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                sha = f"sha-{token}"
                item = _item(
                    suite="gone",
                    sha=sha,
                    bundle_ref=f"tests/bundles/does-not-exist-{token}.zip",
                    target_url=None,
                )
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                await _drain_one(consumer, reports)

                # A failed run is reported (0/0), distinguishable from a real run,
                # and the entry is acked (a missing bundle never redelivers forever).
                assert reports[0]["sha"] == sha
                assert reports[0]["passed_count"] == 0
                assert reports[0]["total"] == 0
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


def test_entry_is_acked_after_report_even_when_report_fails(make_eval_harness, bundles) -> None:
    """The report POST 500s on every attempt (terminal failure). The report attempt
    still completes (retried, then logged) and the entry is acked afterward, so a
    down platform API never wedges the stream -- documenting at-least-once with a
    best-effort report."""
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            fake.responses = {"q": "yes"}
            bundle_ref = upload(
                EvalSuite(
                    name="one",
                    cases=[
                        EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="yes"))
                    ],
                )
            )
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                    report_status=500,
                )
                await consumer.ensure_group()
                item = _item(
                    suite="one", sha=f"sha-{token}", bundle_ref=bundle_ref, target_url=base_url
                )
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                task = asyncio.create_task(consumer.run())
                try:
                    await _wait_until(lambda: len(reports) >= 2)  # every attempt retried
                finally:
                    consumer.request_stop()
                    await task

                # Report terminally failed (logged); the entry is acked so it is not
                # redelivered forever.
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


@pytest.mark.parametrize(
    ("platform_thinking", "agent_thinking", "item_model", "expected_thinking"),
    [
        pytest.param("adaptive", "disabled", "requested_model", "disabled"),
        pytest.param("adaptive", None, None, "adaptive"),
        pytest.param(None, "high", None, "high"),
        pytest.param(None, None, None, None),
    ],
)
def test_provisioned_runner_end_to_end(
    make_eval_harness,
    bundles,
    platform_thinking: str | None,
    agent_thinking: str | None,
    item_model: str | None,
    expected_thinking: str | None,
) -> None:
    """No target_url: the consumer provisions a runner via the G1 substrate (boot
    env carrying the bundle_ref + budget), evals against it, reports, and tears the
    sandbox down in a finally. The fake runner is the model boundary, so no real
    model is ever called."""
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            fake.responses = {"ping": "pong"}
            port = int(base_url.rsplit(":", 1)[1])
            bundle_ref = upload(
                EvalSuite(
                    name="prov",
                    cases=[
                        EvalCase(
                            id="1", input="ping", grader=Grader(kind=CONTAINS, expected="pong")
                        )
                    ],
                )
            )
            token = uuid.uuid4().hex[:8]
            if platform_thinking is None:
                cfg = _cfg(f"test:evals:{token}", f"g-{token}")
            else:
                cfg = _cfg(
                    f"test:evals:{token}",
                    f"g-{token}",
                    thinking=platform_thinking,
                )
            sandbox_prefix = f"test:curie:sandbox:{token}"
            sync_client = redis.Redis(
                host=_VH, port=_VP, password=_VPW or None, decode_responses=False
            )
            fake_k8s = _FakeK8s()
            substrate = SandboxSubstrate(
                fake_k8s,  # type: ignore[arg-type]
                AffinityStore(sync_client, key_prefix=sandbox_prefix),
                SubstrateConfig(
                    namespace="test-ns",
                    warm_pool="test-pool",
                    runner_port=port,
                    route_ttl_seconds=60,
                    claim_timeout_seconds=3.0,
                    poll_interval_seconds=0.005,
                    key_prefix=sandbox_prefix,
                ),
            )
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                repo_lookup = _StubRepo(thinking=agent_thinking)
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=substrate,
                    reports=reports,
                    lf_client=lf_client,
                    repo_lookup=repo_lookup,
                )
                await consumer.ensure_group()
                sha = f"sha-{token}"
                item = _item(
                    suite="prov",
                    sha=sha,
                    bundle_ref=bundle_ref,
                    target_url=None,
                    model=item_model,
                )
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                await _drain_one(consumer, reports)

                # The provisioned runner answered and the suite passed 1/1.
                assert reports[0]["passed_count"] == 1
                assert reports[0]["total"] == 1
                assert reports[0]["target_url"] is None  # provisioned, not a shortcut
                # The boot env carried the bundle ref and a budget (the F2 seam),
                assert fake_k8s.claim_envs, "substrate.claim was never called"
                assert fake_k8s.claim_envs[0][BUNDLE_REF_ENV] == bundle_ref
                assert BUDGET_ENV in fake_k8s.claim_envs[0]
                if expected_thinking is None:
                    assert THINKING_ENV not in fake_k8s.claim_envs[0]
                else:
                    assert fake_k8s.claim_envs[0][THINKING_ENV] == expected_thinking
                if item_model is not None:
                    assert fake_k8s.claim_envs[0][MODEL_ENV] == item_model
                if agent_thinking is not None:
                    assert repo_lookup.thinking_agent_id == item.agent_id
                # and the sandbox was torn down after the eval (finally: release).
                assert fake_k8s.deleted, "provisioned sandbox was never released"
                assert not fake_k8s.claims

                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0
                await _assert_langfuse_traces(lf_client, cfg, sha, expected=1)

            await client.delete(cfg.eval_stream)
            keys = list(sync_client.scan_iter(match=f"{sandbox_prefix}:*"))
            if keys:
                sync_client.delete(*keys)
            sync_client.close()
            await client.aclose()

    asyncio.run(go())


def test_pending_entry_from_a_dead_consumer_is_reclaimed(make_eval_harness, bundles) -> None:
    """An entry a crashed consumer took but never acked (still in the group PEL) is
    reclaimed via XAUTOCLAIM and re-run, so the at-least-once promise holds -- a
    crash before ack never strands the eval."""
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            fake.responses = {"q": "ok"}
            bundle_ref = upload(
                EvalSuite(
                    name="recl",
                    cases=[
                        EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="ok"))
                    ],
                )
            )
            token = uuid.uuid4().hex[:8]
            # Reclaim anything pending immediately, and tick the reclaim loop fast.
            cfg = _cfg(
                f"test:evals:{token}",
                f"g-{token}",
                reclaim_min_idle_ms=0,
                reclaim_interval_s=0.05,
            )
            client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                sha = f"sha-{token}"
                item = _item(suite="recl", sha=sha, bundle_ref=bundle_ref, target_url=base_url)
                await client.xadd(cfg.eval_stream, {"payload": item.model_dump_json()})

                # A dead consumer takes the entry (moves it into the PEL) and never
                # acks -- the read loop's ">" will never see it again.
                await client.xreadgroup(
                    cfg.eval_consumer_group,
                    "dead-consumer",
                    {cfg.eval_stream: ">"},
                    count=10,
                )
                pending = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert pending["pending"] == 1  # stranded under the dead consumer

                await _drain_one(consumer, reports)

                # Reclaimed, re-run against the bundle, reported, and acked.
                assert reports[0]["sha"] == sha
                assert reports[0]["passed_count"] == 1
                summary = await client.xpending(cfg.eval_stream, cfg.eval_consumer_group)
                assert summary["pending"] == 0

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())


# --- Per-sandbox runner token threading (issue #63) ---------------------------
# The env-var name is the cross-package contract with the runner; asserted by its
# literal string so the module never depends on a constant that only exists after
# the feature lands.
RUNNER_TOKEN_ENV = "CURIE_RUNNER_TOKEN"


def _suite_bundle(
    suite: EvalSuite,
    *,
    trajectory: dict[str, object] | str | None = None,
    cases_payload: bytes | None = None,
) -> bytes:
    """A minimal tar.gz carrying evals/cases.json, so the real
    load_suite_from_bundle returns a real suite (the RustFS fetch is the only
    faked boundary)."""
    payload = cases_payload or suite.model_dump_json().encode("utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo("evals/cases.json")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
        if trajectory is not None:
            trajectory_payload = (
                trajectory if isinstance(trajectory, str) else json.dumps(trajectory)
            ).encode("utf-8")
            trajectory_info = tarfile.TarInfo("evals/trajectory.json")
            trajectory_info.size = len(trajectory_payload)
            tf.addfile(trajectory_info, io.BytesIO(trajectory_payload))
    return buf.getvalue()


def _trajectory_sidecar(specs: list[dict[str, object]]) -> dict[str, object]:
    return {"specs": specs}


class _FakeBundleStore:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def get(self, _ref: str) -> bytes:
        return self._data


@dataclass
class _FakeHandle:
    base_url: str
    token: str


class _TokenSubstrate:
    """A substrate whose claim returns a handle carrying a known runner token."""

    def __init__(self, token: str) -> None:
        self._token = token
        self.released: list[str] = []

    def claim(self, _key: str, *, env: dict[str, str] | None = None) -> _FakeHandle:
        return _FakeHandle(base_url="http://sandbox.local:8080", token=self._token)

    def release(self, key: str) -> None:
        self.released.append(key)


class _FakeReporter:
    def __init__(self) -> None:
        self.reports: list[Any] = []

    async def report(self, report: Any) -> bool:
        self.reports.append(report)
        return True


def test_eval_boot_env_mints_runner_token() -> None:
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.zip", target_url=None)
    env = consumer._boot_env(item)
    assert env.get(RUNNER_TOKEN_ENV), "_boot_env must mint a non-empty runner token"


def test_eval_requested_model_boots_and_tags_that_model() -> None:
    """#526: a work item's ``model`` is booted into the provisioned sandbox
    (CURIE_MODEL wins over the worker default) AND becomes the run's model
    dimension, so a sweep row is measured under, and labelled with, the model it
    asked for -- never the worker default and never the unlabelled column."""
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(model="worker-default"),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = _item(
        suite="s", sha="deadbeef", bundle_ref="bundles/x.zip", target_url=None, model="claude-x"
    )
    env = consumer._boot_env(item)
    assert env[MODEL_ENV] == "claude-x"  # requested model wins over worker default
    assert consumer._eval_model(item) == "claude-x"  # ...and is the matrix label

    # No requested model: the worker default is booted and tagged, as before.
    default_item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.zip", target_url=None)
    assert consumer._boot_env(default_item)[MODEL_ENV] == "worker-default"
    assert consumer._eval_model(default_item) == "worker-default"


def test_eval_requested_model_labels_even_a_target_url_run() -> None:
    """A requested model is authoritative even on the ``target_url`` shortcut: the
    caller asserts which model that runner is serving, so the run is labelled with
    it rather than silently dropped into the matrix's unlabelled column (#526)."""
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    labelled = _item(
        suite="s", sha="d", bundle_ref=None, target_url="http://runner", model="claude-y"
    )
    assert consumer._eval_model(labelled) == "claude-y"
    unlabelled = _item(suite="s", sha="d", bundle_ref=None, target_url="http://runner")
    assert consumer._eval_model(unlabelled) is None


def test_eval_fake_model_install_refuses_to_label_a_model_never_called(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """#606: a fake-model install (the sealed default, or CURIE_FAKE_MODEL=1) runs
    the canned FakeModelSession regardless of the requested model. A sweep row must
    NOT be tagged with a model that was never called -- that fabricates a
    cross-model comparison indistinguishable from a real one (ADR-0041). The row is
    left unlabelled, and the discarded request is logged rather than silently
    dropped."""
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(fake_model=True, model="worker-default"),
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    booted = _item(
        suite="s",
        sha="deadbeef",
        bundle_ref="bundles/x.zip",
        target_url=None,
        model="claude-opus-4-8",
    )
    with caplog.at_level(logging.WARNING):
        assert consumer._eval_model(booted) is None  # NOT "claude-opus-4-8"
    assert any("claude-opus-4-8" in r.getMessage() for r in caplog.records), (
        "the discarded requested model must be logged, not silently dropped"
    )

    # A fake run with no requested model is unlabelled too (not the worker default,
    # which the fake session never calls either).
    default_item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.zip", target_url=None)
    assert consumer._eval_model(default_item) is None

    # The target_url runner we did not boot is exempt: our fake flag says nothing
    # about what that runner ran, so a caller-asserted label still stands.
    remote = _item(
        suite="s", sha="d", bundle_ref=None, target_url="http://runner", model="claude-y"
    )
    assert consumer._eval_model(remote) == "claude-y"


class _ConcurrencyProbeSubstrate:
    """A substrate whose ``claim`` records how many claims are in flight at once.

    ``claim`` is the sync substrate call the eval consumer runs via
    ``asyncio.to_thread`` (real OS threads), so the probe uses a threading lock to
    track the live count and the peak the semaphore ever allowed. Each claim holds
    for a beat so overlapping callers actually contend for the slot -- without the
    hold the calls could serialize by luck and hide an unbounded fan-out.
    """

    def __init__(self, hold_s: float = 0.05) -> None:
        import threading

        self._hold_s = hold_s
        self._lock = threading.Lock()
        self._live = 0
        self.peak = 0
        self.claims = 0

    def claim(self, _key: str, *, env: dict[str, str] | None = None) -> _FakeHandle:
        with self._lock:
            self._live += 1
            self.claims += 1
            self.peak = max(self.peak, self._live)
        time.sleep(self._hold_s)
        with self._lock:
            self._live -= 1
        return _FakeHandle(base_url="http://sandbox.local:8080", token="t")

    def release(self, _key: str) -> None:  # pragma: no cover - unused here
        pass


def test_eval_claim_creation_is_bounded_to_one_by_default() -> None:
    """#709: the eval consumer creates SandboxClaims one at a time by default, so a
    single-node cluster is not flooded with concurrent binds (the 504/never-bind
    storm). Firing several provisioning calls at once must never overlap more than
    the configured bound -- here the default of 1 -- inside ``_acquire_target``."""
    substrate = _ConcurrencyProbeSubstrate()
    consumer = _consumer(_cfg("s", "g"), substrate=substrate, repo_lookup=_StubRepo())
    items = [
        _item(suite="s", sha=f"sha{i}", bundle_ref="bundles/x.zip", target_url=None)
        for i in range(5)
    ]

    async def go() -> None:
        await asyncio.gather(*(consumer._acquire_target(item) for item in items))

    asyncio.run(go())

    assert substrate.claims == len(items)  # every job did provision
    assert substrate.peak == 1, (
        f"claim creation must be sequential at the default bound; saw {substrate.peak} at once"
    )


def test_eval_claim_creation_bound_admits_configured_parallelism() -> None:
    """The bound is a ceiling, not a hard-coded 1: raising
    ``eval_max_concurrent_claims`` admits that many concurrent binds (a multi-node
    cluster opts into real parallelism) while still capping the fan-out below the
    number of jobs in flight."""
    substrate = _ConcurrencyProbeSubstrate()
    consumer = _consumer(
        _cfg("s", "g", eval_max_concurrent_claims=3),
        substrate=substrate,
        repo_lookup=_StubRepo(),
    )
    items = [
        _item(suite="s", sha=f"sha{i}", bundle_ref="bundles/x.zip", target_url=None)
        for i in range(8)
    ]

    async def go() -> None:
        await asyncio.gather(*(consumer._acquire_target(item) for item in items))

    asyncio.run(go())

    assert substrate.claims == len(items)
    assert 1 < substrate.peak <= 3, (
        f"bound of 3 must admit up to 3 concurrent binds and no more; saw {substrate.peak}"
    )


def _consumer(cfg: WorkerConfig, **wiring: Any) -> EvalStreamConsumer:
    deps: dict[str, Any] = {
        "redis": None,
        "bundle_store": None,
        "substrate": None,
        "reporter": None,
        "recorder": None,
        "repo_lookup": None,
    }
    deps.update(wiring)
    return EvalStreamConsumer(config=cfg, **deps)  # type: ignore[arg-type]


def _canned_run(monkeypatch, outcomes: list[EvalOutcome]) -> None:
    """Pin ``run_eval_suite`` to a result with the given per-case outcomes.

    The report gate is what is under test here; the suite execution is the seam
    being held still, exactly as the token-threading test does.
    """
    from curie_worker.eval import stream as stream_module

    async def _fake_run(suite: EvalSuite, *, version: str, **_kw: Any) -> EvalRunResult:
        return EvalRunResult(
            version=version,
            suite=suite.name,
            model=None,
            results=[
                EvalCaseResult(
                    case_id=f"c{i}", outcome=outcome, output="all done", latency_ms=1.0
                )
                for i, outcome in enumerate(outcomes)
            ],
        )

    monkeypatch.setattr(stream_module, "run_eval_suite", _fake_run)


def _fake_job_consumer(reporter: Any) -> EvalStreamConsumer:
    suite = EvalSuite(
        name="s",
        cases=[EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="a"))],
    )
    return _consumer(
        WorkerConfig(fake_model=True),
        bundle_store=_FakeBundleStore(_suite_bundle(suite)),
        substrate=_TokenSubstrate("tok"),
        reporter=reporter,
        repo_lookup=_StubRepo(),
    )


@dataclass(frozen=True)
class _EvalMetric:
    name: str
    value: float
    attributes: dict[str, str]


class _EvalSpan:
    def add_event(
        self, _name: str, _attributes: Mapping[str, str] | None = None
    ) -> None:
        pass


class _EvalTelemetryProbe:
    def __init__(self) -> None:
        self.spans: list[tuple[str, bool]] = []
        self.metrics: list[_EvalMetric] = []

    @contextmanager
    def operation_span(
        self,
        name: str,
        *,
        kind: Any,
        parent: Any = None,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[_EvalSpan]:
        del kind, attributes
        span = trace.get_current_span(parent) if parent is not None else trace.get_current_span()
        self.spans.append((name, span.get_span_context().is_valid))
        yield _EvalSpan()

    def record_metric(
        self,
        name: str,
        value: float = 1,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        self.metrics.append(_EvalMetric(name, float(value), dict(attributes or {})))


@pytest.mark.parametrize(
    "carrier",
    [None, "not-a-valid-traceparent"],
    ids=["missing", "malformed"],
)
def test_eval_consumer_uses_a_safe_root_and_records_bounded_completion(
    monkeypatch: pytest.MonkeyPatch,
    carrier: str | None,
) -> None:
    """The eval sibling must tolerate the same carrier failures as runs."""

    assert callable(operation_span)
    assert callable(record_metric)
    probe = _EvalTelemetryProbe()
    import curie_telemetry

    monkeypatch.setattr(curie_telemetry, "operation_span", probe.operation_span)
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (eval_stream_module, stream_consumer_module):
        if hasattr(module, "operation_span"):
            monkeypatch.setattr(module, "operation_span", probe.operation_span)
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", probe.record_metric)

    _canned_run(monkeypatch, [EvalOutcome.PASS])
    reporter = _FakeReporter()
    consumer = _fake_job_consumer(reporter)
    acked: list[str] = []

    async def _record_ack(entry_id: str) -> None:
        acked.append(entry_id)

    monkeypatch.setattr(consumer, "_ack", _record_ack)
    item = _item(
        suite="s",
        sha="deadbeef",
        bundle_ref="bundles/x.tgz",
        target_url=None,
    )
    fields = {STREAM_PAYLOAD_FIELD: item.model_dump_json()}
    if carrier is not None:
        fields[TRACEPARENT_STREAM_FIELD] = carrier

    asyncio.run(consumer._handle("1-0", fields))

    assert acked == ["1-0"]
    assert ("curie.eval.process", False) in probe.spans
    completed = [point for point in probe.metrics if point.name == "curie.eval.process"]
    assert completed
    assert {point.attributes["outcome"] for point in completed} == {"success"}
    assert all(
        set(point.attributes)
        <= {"service.name", "operation", "role", "source", "outcome"}
        for point in completed
    )


def test_an_all_plumbing_run_posts_no_eval_report_but_is_still_acked(monkeypatch) -> None:
    """The frozen EvalReport carries passed_count/total only, and the API turns it
    into a GitHub commit status. That shape cannot express non-graded: 0/N posts a
    red that did not happen and N/N posts the false green this change exists to
    kill. So an all-plumbing run posts nothing at all -- and the skip still counts
    as a completed report attempt, so the entry is acked rather than redelivered
    forever."""
    _canned_run(monkeypatch, [EvalOutcome.PLUMBING_OK, EvalOutcome.PLUMBING_OK])
    reporter = _FakeReporter()
    consumer = _fake_job_consumer(reporter)
    acked: list[str] = []

    async def _record_ack(entry_id: str) -> None:
        acked.append(entry_id)

    monkeypatch.setattr(consumer, "_ack", _record_ack)
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.tgz", target_url=None)

    async def go() -> None:
        await consumer._handle("1-0", {STREAM_PAYLOAD_FIELD: item.model_dump_json()})

    asyncio.run(go())

    assert reporter.reports == []  # no commit status is better than a fabricated one
    assert acked == ["1-0"]  # ...and the job is done, not left pending


def test_a_fake_run_carrying_a_failure_still_posts_its_report(monkeypatch) -> None:
    """A fake turn that did not complete means the plumbing is genuinely broken, so
    that red is real and must reach the PR check."""
    _canned_run(monkeypatch, [EvalOutcome.PLUMBING_OK, EvalOutcome.FAIL])
    reporter = _FakeReporter()
    consumer = _fake_job_consumer(reporter)
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.tgz", target_url=None)

    async def go() -> None:
        await consumer._run_and_report(item, "test-stream-id")

    asyncio.run(go())

    assert len(reporter.reports) == 1
    report = reporter.reports[0]
    assert report.sha == "deadbeef"
    assert report.passed_count == 0  # nothing passed; the broken turn is the signal


def test_a_real_run_still_posts_its_report(monkeypatch) -> None:
    """The report gate is keyed on the non-graded outcome, not switched off wholesale:
    a graded run reports exactly as before."""
    _canned_run(monkeypatch, [EvalOutcome.PASS, EvalOutcome.PASS])
    reporter = _FakeReporter()
    consumer = _consumer(
        WorkerConfig(fake_model=False),
        bundle_store=_FakeBundleStore(
            _suite_bundle(
                EvalSuite(
                    name="s",
                    cases=[EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="a"))],
                )
            )
        ),
        substrate=_TokenSubstrate("tok"),
        reporter=reporter,
        repo_lookup=_StubRepo(),
    )
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.tgz", target_url=None)

    async def go() -> None:
        await consumer._run_and_report(item, "test-stream-id")

    asyncio.run(go())

    assert len(reporter.reports) == 1
    assert reporter.reports[0].passed_count == 2


def test_eval_boot_env_drops_reserved_connector_secret() -> None:
    """#457 eval-path parity: the eval consumer's connector-secret injection must
    honor the reserved-boot-env policy exactly like binding.boot_env does. A secret
    named after a runner-owned model credential (ANTHROPIC_BASE_URL) must never
    survive into the eval boot env -- even on the DEFAULT path where apply_model_env
    does not itself set the base URL, so nothing overwrites the injected value after
    the loop. Order-independent: the reserved key is dropped and never marked; a
    legitimate connector secret is injected and is the only key marked.

    RED until the eval path's _boot_env is hardened (it currently injects any name
    not already in env, so the reserved key leaks through on the default path)."""
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(),  # default path: model_base_url unset
        bundle_store=None,  # type: ignore[arg-type]
        substrate=None,  # type: ignore[arg-type]
        reporter=None,  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=None,
    )
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.zip", target_url=None)
    env = consumer._boot_env(
        item,
        {
            "ANTHROPIC_BASE_URL": "http://evil",
            "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_ok",
        },
    )
    # The reserved model-credential key never carries the injected value.
    assert env.get("ANTHROPIC_BASE_URL") != "http://evil"
    # The legitimate connector secret is injected...
    assert env["GITHUB_PERSONAL_ACCESS_TOKEN"] == "ghp_ok"
    # ...and is the ONLY key marked as a delivered connector secret.
    assert env.get("CURIE_CONNECTOR_SECRET_KEYS") == "GITHUB_PERSONAL_ACCESS_TOKEN"


def test_eval_threads_claim_token_into_run_eval_suite(monkeypatch) -> None:
    # The token surfaced from the provisioned handle must be threaded into the
    # eval turn driver so a token-enforcing sandbox does not 401 the eval. The
    # only faked boundary is the run_eval_suite seam (captured, not the code
    # under test) and the RustFS bundle fetch.
    from curie_worker.eval import stream as stream_module

    captured: dict[str, Any] = {}

    async def _capture_run(
        suite: EvalSuite,
        *,
        base_url: str,
        version: str,
        recorder: Any = None,
        token: Any = None,
        model: Any = None,
        fake: Any = None,
        stream_id: str | None,
        scorer: Any = None,
    ) -> EvalRunResult:
        captured["base_url"] = base_url
        captured["token"] = token
        captured["model"] = model
        captured["fake"] = fake
        captured["stream_id"] = stream_id
        captured["scorer"] = scorer
        return EvalRunResult(version=version, suite=suite.name, results=[])

    monkeypatch.setattr(stream_module, "run_eval_suite", _capture_run)

    suite = EvalSuite(
        name="s",
        cases=[EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="a"))],
    )
    consumer = EvalStreamConsumer(
        redis=None,  # type: ignore[arg-type]
        config=WorkerConfig(),
        bundle_store=_FakeBundleStore(_suite_bundle(suite)),  # type: ignore[arg-type]
        substrate=_TokenSubstrate("tok-eval-xyz"),  # type: ignore[arg-type]
        reporter=_FakeReporter(),  # type: ignore[arg-type]
        recorder=None,  # type: ignore[arg-type]
        repo_lookup=_StubRepo(),
    )
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.tgz", target_url=None)

    async def go() -> None:
        await consumer._run_and_report(item, "test-stream-id")

    asyncio.run(go())

    assert captured["base_url"] == "http://sandbox.local:8080"
    assert captured["token"] == "tok-eval-xyz"
    assert captured["stream_id"] == "test-stream-id"
    assert captured["scorer"] is None


@pytest.mark.parametrize("fake_model", [True, False])
def test_eval_threads_the_workers_fake_state_into_run_eval_suite(
    monkeypatch, fake_model: bool
) -> None:
    """The worker is the sole authority on fake-ness: nothing downstream of the
    stream can infer it (the ACI Final frame carries no model field and no fake
    marker), so the consumer must hand its own config down to the grading layer.
    Without this thread the runner would grade a canned reply -- the false green.
    """
    from curie_worker.eval import stream as stream_module

    captured: dict[str, Any] = {}

    async def _capture_run(
        suite: EvalSuite, *, version: str, fake: Any = None, **_kw: Any
    ) -> EvalRunResult:
        captured["fake"] = fake
        return EvalRunResult(version=version, suite=suite.name, results=[])

    monkeypatch.setattr(stream_module, "run_eval_suite", _capture_run)

    suite = EvalSuite(
        name="s",
        cases=[EvalCase(id="1", input="q", grader=Grader(kind=CONTAINS, expected="a"))],
    )
    consumer = _consumer(
        WorkerConfig(fake_model=fake_model),
        bundle_store=_FakeBundleStore(_suite_bundle(suite)),
        substrate=_TokenSubstrate("tok"),
        reporter=_FakeReporter(),
        repo_lookup=_StubRepo(),
    )
    item = _item(suite="s", sha="deadbeef", bundle_ref="bundles/x.tgz", target_url=None)

    async def go() -> None:
        await consumer._run_and_report(item, "test-stream-id")

    asyncio.run(go())

    assert captured["fake"] is fake_model


async def _assert_langfuse_traces(
    lf_client: httpx.AsyncClient, cfg: WorkerConfig, sha: str, *, expected: int
) -> None:
    """Poll the real Langfuse until ``expected`` traces are visible for the version
    tag (v3 ingestion is async: queued, then materialized in ClickHouse)."""
    found = 0
    for _ in range(40):
        resp = await lf_client.get(
            f"{cfg.langfuse_host}/api/public/traces",
            params={"tags": f"version:{sha}"},
            auth=(cfg.langfuse_public_key, cfg.langfuse_secret_key),
        )
        found = len(resp.json().get("data", [])) if resp.status_code == 200 else 0
        if found >= expected:
            break
        await asyncio.sleep(1)
    assert found == expected


def test_worker_boot_after_an_outage_skips_stale_backlog_but_runs_recent() -> None:
    # Real scenario: the worker was down for a while (a deploy, a weekend), so
    # eval jobs queued ~2 days ago piled up on the stream alongside one queued
    # ~10 minutes ago. When the worker boots and creates the group fresh, the
    # stale backlog must not storm in, but the recent job must still run. Stream
    # ids are millisecond timestamps, so the entries are placed at realistic
    # wall-clock ages relative to now (default window is 24h).
    async def go() -> None:
        stream = f"curie:evals:maxage:{uuid.uuid4().hex}"
        group = f"grp-{uuid.uuid4().hex}"
        cfg = _cfg(stream, group)  # default eval_stream_max_age_hours = 24
        client = AsyncRedis(host=_VH, port=_VP, password=_VPW, decode_responses=True)
        consumer = EvalStreamConsumer(
            redis=client,
            config=cfg,
            bundle_store=cast(Any, None),
            substrate=cast(Any, None),
            reporter=cast(Any, None),
            recorder=cast(Any, None),
            repo_lookup=cast(Any, None),
        )
        try:
            now_ms = int(time.time() * 1000)
            two_days_ago = now_ms - 48 * 3600 * 1000
            ten_min_ago = now_ms - 10 * 60 * 1000
            # Two stale jobs from ~2 days ago, then one queued ~10 minutes ago.
            await client.xadd(stream, {"payload": "stale-1"}, id=f"{two_days_ago}-0")
            await client.xadd(stream, {"payload": "stale-2"}, id=f"{two_days_ago + 1}-0")
            recent_id = await client.xadd(stream, {"payload": "recent"}, id=f"{ten_min_ago}-0")

            await consumer.ensure_group()  # created fresh at (now - 24h)

            resp = await client.xreadgroup(group, "c1", {stream: ">"}, count=10)
            delivered = [eid for _s, entries in (resp or []) for eid, _f in entries]
            assert delivered == [recent_id], delivered
        finally:
            await client.delete(stream)
            await client.aclose()

    asyncio.run(go())


def test_committed_fixture_loads_from_bundle_with_name_override(
    eval_cases_example_path: Path,
) -> None:
    """A tar bundle carrying the committed cross-language fixture bytes at
    evals/cases.json loads through load_suite_from_bundle: the payload suite-name
    override wins over the file's name, and the falsifiable smoke grader (#527)
    passes a turn naming the agent while failing off-topic text.
    Proves the scaffold output is platform-loadable (the latent bug in issue #8).
    """
    fixture_bytes = eval_cases_example_path.read_bytes()

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo(name="evals/cases.json")
        info.size = len(fixture_bytes)
        tf.addfile(info, io.BytesIO(fixture_bytes))
    tar_bytes = buf.getvalue()

    suite = load_suite_from_bundle(tar_bytes, "override-suite-name")

    assert suite is not None
    assert suite.name == "override-suite-name"  # payload override at stream.py:160
    assert len(suite.cases) == 1
    assert suite.cases[0].grader.grade("I am the example agent.") is True
    assert suite.cases[0].grader.grade("literally anything") is False


def test_bundle_trajectory_sidecar_scores_observed_calls_instead_of_text(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="right_order",
                        input="right",
                        grader=Grader(kind=CONTAINS, expected="text grader pass"),
                    ),
                    EvalCase(
                        id="wrong_order",
                        input="wrong",
                        grader=Grader(kind=CONTAINS, expected="text grader pass"),
                    ),
                ],
            )
            fake.responses = {
                "right": "text grader miss",
                "wrong": "text grader pass",
            }
            fake.tool_calls = {
                "right": ["search", "fetch"],
                "wrong": ["fetch", "search"],
            }
            cases_payload = (
                json.dumps(suite.model_dump(mode="json"), indent=2, sort_keys=True)
                + "\n"
            ).encode("utf-8")
            sidecar = _trajectory_sidecar(
                [
                    {
                        "case_id": case_id,
                        "expected": ["search", "fetch"],
                        "mode": "exact",
                        "threshold": 1.0,
                    }
                    for case_id in ("right_order", "wrong_order")
                ],
            )
            reporter = _FakeReporter()
            bundle_ref = upload(
                _suite_bundle(
                    suite,
                    trajectory=sidecar,
                    cases_payload=cases_payload,
                )
            )
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=reporter,
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="trajectory",
                sha="trajectory_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            by_id = {row.case_id: row for row in result.results}
            assert by_id["right_order"].outcome is EvalOutcome.PASS
            assert by_id["right_order"].detail is None
            assert by_id["wrong_order"].outcome is EvalOutcome.FAIL
            assert by_id["wrong_order"].detail == (
                "mode=exact expected=['search', 'fetch'] "
                "observed=['fetch', 'search']"
            )
            assert by_id["wrong_order"].error is None
            assert reporter.reports[0].passed_count == 1
            assert reporter.reports[0].total == 2

    asyncio.run(go())


def test_bundle_trajectory_sidecar_fails_closed_for_a_missing_spec(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="missing",
                        input="run",
                        grader=Grader(kind=CONTAINS, expected="text grader pass"),
                    )
                ],
            )
            fake.responses = {"run": "text grader pass"}
            fake.tool_calls = {"run": ["search", "fetch"]}
            cases_payload = suite.model_dump_json().encode("utf-8")
            sidecar = _trajectory_sidecar([])
            bundle_ref = upload(
                _suite_bundle(
                    suite,
                    trajectory=sidecar,
                    cases_payload=cases_payload,
                )
            )
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="trajectory",
                sha="missing_spec_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            case = result.results[0]
            assert case.outcome is EvalOutcome.FAIL
            assert case.detail == "no trajectory spec for case 'missing'"
            assert case.error is None
            assert len(fake.seen) == 1

    asyncio.run(go())


def test_bundle_without_trajectory_sidecar_keeps_ordinary_grading(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="ordinary",
                cases=[
                    EvalCase(
                        id="ordinary",
                        input="run",
                        grader=Grader(kind=CONTAINS, expected="text grader pass"),
                    )
                ],
            )
            fake.responses = {"run": "text grader pass"}
            fake.tool_calls = {"run": ["fetch", "search"]}
            bundle_ref = upload(_suite_bundle(suite))
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="ordinary",
                sha="ordinary_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            case = result.results[0]
            assert case.outcome is EvalOutcome.PASS
            assert case.detail is None
            assert len(fake.seen) == 1

    asyncio.run(go())


def test_invalid_bundle_trajectory_sidecar_fails_before_running_cases(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="known",
                        input="run",
                        grader=Grader(kind=CONTAINS, expected="text grader pass"),
                    )
                ],
            )
            fake.responses = {"run": "text grader pass"}
            cases_payload = suite.model_dump_json().encode("utf-8")
            bundle_ref = upload(
                _suite_bundle(
                    suite,
                    trajectory="{",
                    cases_payload=cases_payload,
                )
            )
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="trajectory",
                sha="invalid_sidecar_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            assert len(result.results) == 1
            case = result.results[0]
            assert case.outcome is EvalOutcome.FAIL
            assert case.detail is not None
            detail = case.detail.lower()
            assert "trajectory" in detail
            assert "invalid" in detail
            assert case.error is None
            assert fake.seen == []

    asyncio.run(go())


def test_trajectory_sidecar_ignores_specs_for_unknown_suite_cases(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="known",
                        input="run",
                        grader=Grader(kind=CONTAINS, expected="text must miss"),
                    )
                ],
            )
            fake.responses = {"run": "unrelated"}
            fake.tool_calls = {"run": ["search"]}
            sidecar = _trajectory_sidecar(
                [
                    {
                        "case_id": "known",
                        "expected": ["search"],
                        "mode": "exact",
                        "threshold": 1.0,
                    },
                    {
                        "case_id": "not_in_suite",
                        "expected": ["unused"],
                        "mode": "exact",
                        "threshold": 1.0,
                    },
                ]
            )
            bundle_ref = upload(_suite_bundle(suite, trajectory=sidecar))
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="trajectory",
                sha="unknown_spec_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            assert len(result.results) == 1
            assert result.results[0].outcome is EvalOutcome.PASS
            assert result.results[0].detail is None
            assert len(fake.seen) == 1

    asyncio.run(go())


def test_trajectory_sidecar_rejects_duplicate_suite_case_ids(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="duplicate",
                        input="first",
                        grader=Grader(kind=CONTAINS, expected="text pass"),
                    ),
                    EvalCase(
                        id="duplicate",
                        input="second",
                        grader=Grader(kind=CONTAINS, expected="text pass"),
                    ),
                ],
            )
            fake.responses = {"first": "text pass", "second": "text pass"}
            fake.tool_calls = {"first": ["Read"], "second": ["Read"]}
            cases_payload = suite.model_dump_json().encode("utf-8")
            sidecar = _trajectory_sidecar(
                [
                    {
                        "case_id": "duplicate",
                        "expected": ["Read"],
                        "mode": "exact",
                        "threshold": 1.0,
                    }
                ],
            )
            bundle_ref = upload(
                _suite_bundle(
                    suite,
                    trajectory=sidecar,
                    cases_payload=cases_payload,
                )
            )
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="trajectory",
                sha="duplicate_case_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            assert len(result.results) == 2
            assert all(row.outcome is EvalOutcome.FAIL for row in result.results)
            assert all(
                row.detail is not None
                and "duplicate" in row.detail.lower()
                and "case" in row.detail.lower()
                for row in result.results
            )
            assert fake.seen == []

    asyncio.run(go())


def test_ordinary_suite_keeps_duplicate_case_ids_when_no_sidecar_selects_trajectory(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="ordinary",
                cases=[
                    EvalCase(
                        id="duplicate",
                        input="first",
                        grader=Grader(kind=CONTAINS, expected="text pass"),
                    ),
                    EvalCase(
                        id="duplicate",
                        input="second",
                        grader=Grader(kind=CONTAINS, expected="text pass"),
                    ),
                ],
            )
            fake.responses = {"first": "text pass", "second": "text pass"}
            bundle_ref = upload(_suite_bundle(suite))
            consumer = _consumer(
                WorkerConfig(fake_model=False),
                bundle_store=store,
                substrate=_UnusedSubstrate(),
                reporter=_FakeReporter(),
                recorder=None,
                repo_lookup=_StubRepo(),
            )
            item = _item(
                suite="ordinary",
                sha="ordinary_duplicate_sha",
                bundle_ref=bundle_ref,
                target_url=base_url,
            )

            result = await consumer._run_and_report(item, "test-stream-id")

            assert [row.outcome for row in result.results] == [
                EvalOutcome.PASS,
                EvalOutcome.PASS,
            ]
            assert len(fake.seen) == 2

    asyncio.run(go())


def test_stream_records_the_trigger_identity_and_selected_trajectory_scorer(
    make_eval_harness,
    bundles,
) -> None:
    store, upload = bundles

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, _client):
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    EvalCase(
                        id="ordered",
                        input="run",
                        grader=Grader(kind=CONTAINS, expected="text must miss"),
                    )
                ],
            )
            fake.responses = {"run": "unrelated"}
            fake.tool_calls = {"run": ["Read", "Bash"]}
            cases_payload = suite.model_dump_json().encode("utf-8")
            bundle_ref = upload(
                _suite_bundle(
                    suite,
                    trajectory=_trajectory_sidecar(
                        [
                            {
                                "case_id": "ordered",
                                "expected": ["Read", "Bash"],
                                "mode": "exact",
                                "threshold": 1.0,
                            }
                        ],
                    ),
                    cases_payload=cases_payload,
                )
            )
            token = uuid.uuid4().hex[:8]
            cfg = _cfg(f"test:evals:{token}", f"g:{token}")
            client = AsyncRedis(
                host=_VH,
                port=_VP,
                password=_VPW,
                decode_responses=True,
            )
            reports: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=30.0) as lf_client:
                consumer = _build_consumer(
                    redis_client=client,
                    cfg=cfg,
                    bundle_store=store,
                    substrate=_UnusedSubstrate(),
                    reports=reports,
                    lf_client=lf_client,
                )
                await consumer.ensure_group()
                sha = f"trajectory_identity_{token}"
                item = _item(
                    suite="trajectory",
                    sha=sha,
                    bundle_ref=bundle_ref,
                    target_url=base_url,
                    model="resolved_model",
                )
                stream_id = await client.xadd(
                    cfg.eval_stream,
                    {STREAM_PAYLOAD_FIELD: item.model_dump_json()},
                )

                await _drain_one(consumer, reports)

                trace: dict[str, Any] | None = None
                for _ in range(40):
                    response = await lf_client.get(
                        f"{cfg.langfuse_host}/api/public/traces",
                        params={"tags": f"version:{sha}"},
                        auth=(cfg.langfuse_public_key, cfg.langfuse_secret_key),
                    )
                    traces = (
                        response.json().get("data", [])
                        if response.status_code == 200
                        else []
                    )
                    if traces:
                        trace = traces[0]
                        break
                    await asyncio.sleep(1)

                assert trace is not None
                metadata = trace["metadata"]
                assert metadata["stream_id"] == stream_id
                assert metadata["scorer"] == "trajectory"
                assert metadata["model"] == "resolved_model"
                assert metadata["outcome"] == "pass"
                assert metadata["case_count"] == 1

            await client.delete(cfg.eval_stream)
            await client.aclose()

    asyncio.run(go())
