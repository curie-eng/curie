"""Deploy-notice delivery through a real Valkey stream and the Slack egress adapter."""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Literal

import pytest
import redis
from aiohttp.test_utils import TestServer
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.deploy_notice import DeployNoticeConsumer
from curie_worker.reply_sink import build_reply_sink
from redis.asyncio import Redis as AsyncRedis

sys.path.insert(0, str(Path(__file__).parent))
from capture_fixtures import Capture  # noqa: E402


@pytest.mark.parametrize(
    "status,codes,expected",
    [
        ("rejected", ["git.archive_failed"], "rejected: git.archive_failed"),
        ("rejected", [f"git.reason_{n}" for n in range(17)], "rejected: git.reason_0"),
        ("deployed", [], "deployed aaaaaaaa (dev)"),
    ],
)
@pytest.mark.parametrize(
    "agent_name,address",
    [
        ("acme-dev", "C0EXAMPLE1"),
        ("", "C0EXAMPLE1"),
        ("n" * 300, "C" + "A" * 300),
    ],
)
def test_notice_posts_as_the_bound_identity_and_acks(
    sync_redis: redis.Redis,
    names: dict[str, str],
    status: Literal["rejected", "deployed"],
    codes: list[str],
    expected: str,
    agent_name: str,
    address: str,
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
            key_prefix=names["prefix"],
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
            leases=DeliveryLeaseStore(valkey, config),
        )
        task = asyncio.create_task(consumer.run())
        try:
            notice: dict[str, Any] = {
                "address": address,
                "identity": "ops",
                "agent_name": agent_name,
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
            assert body["channel"] == address
            assert expected in body["text"]
            assert "thread_ts" not in body
            assert body["client_msg_id"] == str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"curie:deploy-notice:{names['stream']}:{entry}")
            )
            assert await valkey.xpending_range(
                names["stream"], names["group"], min=entry, max=entry, count=1
            ) == []
            state_key = config.delivery_state_key(names["stream"], names["group"], entry)
            for _ in range(50):
                if not await valkey.exists(state_key):
                    break
                await asyncio.sleep(0.01)
            assert not await valkey.exists(state_key)
        finally:
            consumer.request_stop()
            await asyncio.wait_for(task, timeout=5)
            await sink.aclose()
            await valkey.aclose()
            await server.close()

    asyncio.run(go())


def test_malformed_notice_is_dead_lettered_and_acked(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    async def go() -> None:
        config = WorkerConfig(read_block_ms=100, key_prefix=names["prefix"])
        sink = build_reply_sink(config, slack_tokens={})
        valkey = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = DeployNoticeConsumer(
            redis=valkey,
            sink=sink,
            config=config,
            stream=names["stream"],
            group=names["group"],
            consumer="notice-malformed",
            leases=DeliveryLeaseStore(valkey, config),
        )
        task = asyncio.create_task(consumer.run())
        try:
            entry = await valkey.xadd(names["stream"], {"payload": "not-json"})
            dead: list[Any] = []
            for _ in range(100):
                dead = await valkey.xrange(f"{names['stream']}:dead")
                if dead:
                    break
                await asyncio.sleep(0.05)
            assert len(dead) == 1
            assert dead[0][1]["dl_reason"] == "unparseable"
            assert await valkey.xpending_range(
                names["stream"], names["group"], min=entry, max=entry, count=1
            ) == []
        finally:
            consumer.request_stop()
            await asyncio.wait_for(task, timeout=5)
            await valkey.delete(f"{names['stream']}:dead")
            await sink.aclose()
            await valkey.aclose()

    asyncio.run(go())


def test_reenqueued_notice_keeps_one_slack_delivery_identity(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    """A lost SQL settlement can enqueue the same outbox row twice."""

    async def go() -> None:
        capture = Capture()
        server = TestServer(capture.app)
        await server.start_server()
        assert server.port is not None
        config = WorkerConfig(
            slack_api_base_url=f"http://127.0.0.1:{server.port}/slack/api/",
            read_block_ms=100,
            key_prefix=names["prefix"],
        )
        sink = build_reply_sink(config, slack_tokens={"ops": "xoxb-ops-test"})
        valkey = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = DeployNoticeConsumer(
            redis=valkey,
            sink=sink,
            config=config,
            stream=names["stream"],
            group=names["group"],
            consumer="notice-reenqueue",
            leases=DeliveryLeaseStore(valkey, config),
        )
        task = asyncio.create_task(consumer.run())
        try:
            notice = {
                "address": "C0EXAMPLE1",
                "identity": "ops",
                "agent_name": "acme-dev",
                "status": "deployed",
                "sha": "a" * 40,
                "environment": "dev",
                "codes": [],
                "notice_key": "b" * 64,
            }
            entries = [
                await valkey.xadd(names["stream"], {"payload": json.dumps(notice)})
                for _ in range(2)
            ]
            for _ in range(100):
                if len(capture.requests) == 2:
                    break
                await asyncio.sleep(0.05)
            assert len(capture.requests) == 2
            # https://docs.slack.dev/reference/methods/chat.postMessage/
            # documents client_msg_id; test_live.py pins the ambiguous-retry
            # behavior against a real Slack test workspace.
            assert {
                json.loads(request["body"])["client_msg_id"]
                for request in capture.requests
            } == {
                str(uuid.uuid5(uuid.NAMESPACE_URL, f"curie:deploy-notice:{notice['notice_key']}"))
            }
            for entry in entries:
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
