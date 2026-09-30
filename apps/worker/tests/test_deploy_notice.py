"""Deploy-notice delivery through a real Valkey stream and the Slack egress adapter."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Literal

import pytest
import redis
from aiohttp.test_utils import TestServer
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.deploy_notice import DeployNoticeConsumer
from curie_worker.reply_sink import build_reply_sink
from redis.asyncio import Redis as AsyncRedis

sys.path.insert(0, str(Path(__file__).parent))
from capture_fixtures import Capture  # noqa: E402


@pytest.mark.parametrize(
    "status,codes,expected",
    [
        ("rejected", ["git.archive_failed"], "rejected: git.archive_failed"),
        ("deployed", [], "deployed aaaaaaaa (dev)"),
    ],
)
def test_notice_posts_as_the_bound_identity_and_acks(
    sync_redis: redis.Redis,
    names: dict[str, str],
    status: Literal["rejected", "deployed"],
    codes: list[str],
    expected: str,
) -> None:
    async def go() -> None:
        capture = Capture()
        server = TestServer(capture.app)
        await server.start_server()
        port = server.port
        assert port is not None
        config = WorkerConfig(
            slack_bot_token="xoxb-default-test",
            slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
            read_block_ms=100,
        )
        sink = build_reply_sink(
            config,
            slack_tokens={"default": "xoxb-default-test", "ops": "xoxb-ops-test"},
        )
        valkey = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = DeployNoticeConsumer(
            redis=valkey,
            sink=sink,
            config=config,
            stream=names["stream"],
            group=names["group"],
            consumer="notice-test",
        )
        task = asyncio.create_task(consumer.run())
        try:
            notice: dict[str, Any] = {
                "address": "C0EXAMPLE1",
                "identity": "ops",
                "agent_name": "acme-dev",
                "status": status,
                "sha": "a" * 40,
                "environment": "dev",
                "codes": codes,
            }
            entry = await valkey.xadd(names["stream"], {"payload": json.dumps(notice)})
            for _ in range(100):
                if capture.requests:
                    break
                await asyncio.sleep(0.05)
            assert capture.paths() == ["/slack/api/chat.postMessage"]
            posted = capture.requests[0]
            assert posted["headers"]["Authorization"] == "Bearer xoxb-ops-test"
            body = json.loads(posted["body"])
            assert body["channel"] == "C0EXAMPLE1"
            assert expected in body["text"]
            assert "thread_ts" not in body
            assert await valkey.xpending_range(
                names["stream"], names["group"], min=entry, max=entry, count=1
            ) == []
        finally:
            consumer.request_stop()
            await asyncio.wait_for(task, timeout=5)
            await sink.aclose()
            await valkey.aclose()
            await server.close()

    asyncio.run(go())


def test_notice_never_falls_back_to_another_slack_identity(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def go() -> None:
        capture = Capture()
        server = TestServer(capture.app)
        await server.start_server()
        port = server.port
        assert port is not None
        config = WorkerConfig(
            slack_bot_token="xoxb-default-test",
            slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
            read_block_ms=100,
        )
        sink = build_reply_sink(config, slack_tokens={"default": "xoxb-default-test"})
        valkey = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = DeployNoticeConsumer(
            redis=valkey,
            sink=sink,
            config=config,
            stream=names["stream"],
            group=names["group"],
            consumer="notice-missing-token",
        )
        task = asyncio.create_task(consumer.run())
        try:
            entry = await valkey.xadd(
                names["stream"],
                {
                    "payload": json.dumps(
                        {
                            "address": "C0EXAMPLE1",
                            "identity": "ops",
                            "agent_name": "acme-dev",
                            "status": "deployed",
                            "sha": "a" * 40,
                            "environment": "dev",
                            "codes": [],
                        }
                    )
                },
            )
            pending: list[Any] = []
            for _ in range(100):
                pending = await valkey.xpending_range(
                    names["stream"], names["group"], min=entry, max=entry, count=1
                )
                if pending:
                    break
                await asyncio.sleep(0.05)
            assert len(pending) == 1
            assert capture.requests == []
        finally:
            consumer.request_stop()
            await asyncio.wait_for(task, timeout=5)
            await sink.aclose()
            await valkey.aclose()
            await server.close()

    asyncio.run(go())
