"""Bounded delivery of git-flow outcomes through the worker's Slack sink (#1331)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Literal

from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis

from .config import WorkerConfig
from .consumer_liveness import ConsumerLivenessStore
from .delivery_lease import DeliveryLeaseStore
from .reply_sink import ReplySinkRouter, TargetRoute
from .stream_consumer import DeliverySpec, ReadLoopSpec, StreamConsumer
from .upgrade_drain import UpgradeDrainGate

logger = logging.getLogger(__name__)

DEPLOY_NOTICE_STREAM = "curie:deploy-notices"
DEPLOY_NOTICE_GROUP = "curie-deploy-notices"
_CAP_SCAN_PAGE = 1000


class DeployNotice(BaseModel):
    """The server-selected route and stable outcome, without a provider secret."""

    model_config = ConfigDict(extra="forbid", strict=True)

    address: str = Field(min_length=1, max_length=255)
    identity: str = Field(min_length=1, max_length=64)
    agent_name: str = Field(min_length=1, max_length=255)
    status: Literal["deployed", "promoted", "rejected"]
    sha: str = Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    environment: Literal["dev", "prod"] | None
    codes: list[str] = Field(max_length=16)


def render_notice(notice: DeployNotice) -> str:
    commit = notice.sha[:8]
    environment = f" ({notice.environment})" if notice.environment else ""
    if notice.status == "rejected":
        codes = ", ".join(notice.codes) if notice.codes else "unknown reason"
        guidance = (
            "Check the API's repository clone credential and repository access."
            if "git.archive_failed" in notice.codes
            else "Check the GitHub webhook delivery response and API logs."
        )
        return f"⚠️ {notice.agent_name} — push {commit} rejected: {codes}{environment}\n{guidance}"
    verb = "promoted" if notice.status == "promoted" else "deployed"
    return f"🚀 {notice.agent_name} — {verb} {commit}{environment}"


class DeployNoticeConsumer(StreamConsumer):
    """Read one notice at a time, post it as its bound bot, then acknowledge."""

    def __init__(
        self,
        *,
        redis: Redis,
        sink: ReplySinkRouter,
        config: WorkerConfig,
        stream: str = DEPLOY_NOTICE_STREAM,
        group: str = DEPLOY_NOTICE_GROUP,
        consumer: str | None = None,
        leases: DeliveryLeaseStore | None = None,
        drain: UpgradeDrainGate | None = None,
    ) -> None:
        super().__init__(
            redis,
            leases=leases,
            drain=drain,
            liveness_store=ConsumerLivenessStore(redis),
        )
        self._config = config
        self._sink = sink
        self._stream = stream
        self._group = group
        self._consumer = consumer or f"{config.consumer_name}-deploy-notices"
        self._inflight: set[asyncio.Task[None]] = set()
        self._delivery = DeliverySpec(
            stream=stream,
            group=group,
            consumer=self._consumer,
            dead_letter_target=f"{stream}:dead",
            over_cap_reason="max-delivery-exceeded",
            max_delivery=config.max_delivery,
            dead_letter_maxlen=config.dead_letter_maxlen,
            reclaim_min_idle_ms=config.reclaim_min_idle_ms,
            dead_consumer_idle_ms=config.dead_consumer_idle_ms,
            heartbeat_ttl_ms=config.consumer_heartbeat_ttl_ms,
            capability_ttl_ms=config.consumer_capability_ttl_ms,
            read_count=config.read_count,
            cap_scan_page=_CAP_SCAN_PAGE,
            telemetry_source="deploy-notice",
            handler=self._dispatch,
            logger=logger,
            dead_letter_log="dead-lettered deploy notice %s after %d deliveries (%s) -> %s",
            dead_letter_fail_log="dead-lettering deploy notice %s failed; left pending",
            lease_expired_idle_ms=config.lease_expired_idle_ms_value(),
        )

    async def run(self) -> None:
        await self._ensure_group(self._stream, self._group, start_id="0")
        await self._run_consumer_generation(
            {
                "read": self._read_loop,
                "maintenance": self._reclaim_loop,
                "prompt-reclaim": self._prompt_reclaim_loop,
            }
        )

    def _generation_inflight_tasks(self) -> set[asyncio.Task[None]]:
        return set(self._inflight)

    def _reset_generation_resources(self) -> None:
        self._inflight.clear()

    def _transfer_capacity(self) -> int | None:
        return max(0, 1 - len(self._inflight_ids))

    async def _read_loop(self) -> None:
        await self._consume(
            ReadLoopSpec(
                stream=self._stream,
                group=self._group,
                consumer=self._consumer,
                count=1,
                block_ms=self._config.read_block_ms,
                backoff_s=0.5,
                timeout_msg="deploy notice read timed out (idle): %s",
                connection_msg="deploy notice read failed transiently: %s",
                logger=logger,
            ),
            self._dispatch,
        )

    async def _reclaim_loop(self) -> None:
        while not self._should_stop():
            try:
                await self._reclaim_once()
            except Exception:
                logger.exception("deploy notice reclaim tick failed")
            await self._sleep_or_stop(self._config.reclaim_interval_s)

    async def _dispatch(self, entry_id: str, fields: dict[str, str]) -> None:
        if entry_id in self._inflight_ids:
            return
        self._inflight_ids.add(entry_id)
        task = asyncio.create_task(self._handle(entry_id, fields))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        await asyncio.shield(task)

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            async with self._delivery_lease(entry_id, fields) as lease:
                if lease is None:
                    return
                try:
                    notice = DeployNotice.model_validate_json(fields["payload"])
                except Exception:
                    logger.exception("malformed deploy notice %s; dead-lettering", entry_id)
                    await self._dead_letter(
                        entry_id, fields, reason="unparseable", delivery_count=1
                    )
                    return
                event = ReplyPost(
                    version=PROGRESS_REPLY_WIRE_VERSION,
                    event="reply.post",
                    target=ReplyTarget(
                        kind="slack",
                        address=notice.address,
                        conversation_id=None,
                        reply_ref=None,
                    ),
                    message=OutboundMessage(version=MESSAGE_VERSION, text=render_notice(notice)),
                    requested_by="git-flow",
                    delivery_id=str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"curie:deploy-notice:{self._stream}:{entry_id}",
                        )
                    ),
                )
                try:
                    await self._sink.emit(
                        event, route=TargetRoute(adapter=notice.identity)
                    )
                except Exception:
                    logger.exception(
                        "deploy notice %s could not be delivered; left pending", entry_id
                    )
                    return
                await self._ack(entry_id)
                await self._settle_delivery_best_effort(entry_id)
        finally:
            self._inflight_ids.discard(entry_id)
