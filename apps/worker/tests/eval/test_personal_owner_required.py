"""A personal installation refuses an eval before any bundle read or claim (#5378).

ADR 0268 decision 1: a personal installation runs only as a WorkItem request's
owner, and an eval has no owner. The eval lane refuses it as a decision (INFO,
a failed 0/0 report, an acked entry), never as a fault, and never touches the
bundle store or the sandbox substrate. The service installation is the
control: the same entry reads its bundle.

Real Valkey drives the real ``EvalStreamConsumer``; only the platform report
POST (an external HTTP service) and the bundle store are doubles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Literal

import httpx
import pytest
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.eval import EvalJob, EvalReporter, EvalStreamConsumer
from redis.asyncio import Redis as AsyncRedis

from apps.worker.tests.queue_fixtures import wait_until


class _RecordingBundles:
    """A bundle store with nothing in it; records every read."""

    def __init__(self) -> None:
        self.reads: list[str] = []

    def get(self, ref: str) -> bytes:
        self.reads.append(ref)
        raise KeyError(ref)


class _UntouchedSubstrate:
    def claim(self, *_args: object, **_kwargs: object) -> Any:
        raise AssertionError("an eval refused before launch must not claim a sandbox")

    def release(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("an eval refused before launch must not release a sandbox")


class _Repo:
    async def repo_full_name(self, _agent_id: uuid.UUID) -> str:
        return "acme-corp/widgets"

    async def model_settings_for(
        self, _agent_id: uuid.UUID
    ) -> tuple[str | None, str | None, str | None, dict[str, object] | None]:
        return None, None, None, None


@pytest.mark.parametrize("mode", ["personal", "service"])
def test_a_personal_installation_refuses_an_eval_before_any_bundle_read(
    names: dict[str, str],
    caplog: pytest.LogCaptureFixture,
    mode: Literal["personal", "service"],
) -> None:
    caplog.set_level(logging.INFO, logger="curie_worker.eval.stream")

    async def go() -> None:
        stream = f"{names['prefix']}:evals"
        config = WorkerConfig(
            valkey_host=VALKEY_HOST,
            valkey_port=VALKEY_PORT,
            valkey_password=VALKEY_PW,
            eval_stream=stream,
            eval_consumer_group=f"ge-{names['group']}",
            key_prefix=names["prefix"],
            read_block_ms=100,
            factory_model_auth_mode=mode,
        )
        reports: list[dict[str, Any]] = []

        def platform(request: httpx.Request) -> httpx.Response:
            reports.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True})

        bundles = _RecordingBundles()
        client = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(platform)) as report_client:
            consumer = EvalStreamConsumer(
                redis=client,
                config=config,
                bundle_store=bundles,
                substrate=_UntouchedSubstrate(),  # type: ignore[arg-type]
                reporter=EvalReporter(
                    api_base_url="http://api.local",
                    api_key="k",
                    client=report_client,
                    max_attempts=2,
                    backoff_base_s=0.001,
                ),
                recorder=None,  # type: ignore[arg-type]
                repo_lookup=_Repo(),
            )
            try:
                await consumer.ensure_group()
                job = EvalJob(
                    agent_id=uuid.uuid4(),
                    version_id=uuid.uuid4(),
                    sha=f"sha-{mode}",
                    suite="smoke",
                    bundle_ref="tests/bundles/acme-absent.zip",
                    target_url=None,
                    model=None,
                    requested_at="2026-11-11T00:00:00+00:00",
                )
                await client.xadd(stream, {"payload": job.model_dump_json()})

                task = asyncio.create_task(consumer.run())
                try:
                    await wait_until(lambda: bool(reports), "the eval report")
                finally:
                    consumer.request_stop()
                    await task

                assert reports[0]["sha"] == f"sha-{mode}"
                assert (reports[0]["passed_count"], reports[0]["total"]) == (0, 0)
                summary = await client.xpending(stream, config.eval_consumer_group)
                assert summary["pending"] == 0
            finally:
                await client.aclose()

        refusals = [
            record
            for record in caplog.records
            if record.name == "curie_worker.eval.stream"
            and "personal_owner_required" in record.getMessage()
        ]
        if mode == "personal":
            assert bundles.reads == []
            assert [record.levelno for record in refusals] == [logging.INFO]
        else:
            assert bundles.reads == ["tests/bundles/acme-absent.zip"]
            assert refusals == []

    asyncio.run(go())
