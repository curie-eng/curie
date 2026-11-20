"""Non-interactive replies to the one stored approval card. ADR 0273."""

import logging
from datetime import UTC, datetime

from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from curie_internal.approval_reminder import ApprovalReminderV1

from .approval_cards import ApprovalCardStore
from .approvals import ApprovalMissing, ApprovalReader
from .reply_sink import CLUSTER_MESSAGE_ADAPTER, ReplySink, TargetRoute

logger = logging.getLogger(__name__)


async def deliver_reminder(
    reminder: ApprovalReminderV1,
    *,
    reader: ApprovalReader,
    cards: ApprovalCardStore,
    sink: ReplySink,
) -> None:
    try:
        approval = await reader.get(reminder.approval_id)
    except ApprovalMissing:
        return
    if approval is None:
        raise RuntimeError("approval reminder state is unavailable")
    if approval.status != "pending" or (
        approval.expires_at is not None and approval.expires_at <= datetime.now(UTC)
    ):
        return
    stored = await cards.read(reminder.approval_id)
    if stored is None or not stored[0].kind or not stored[0].conversation_id:
        logger.warning("approval reminder has no stored destination for %s", reminder.approval_id)
        return
    card, _ = stored
    if card.adapter == CLUSTER_MESSAGE_ADAPTER:
        logger.info("approval reminder skipped for cluster-message relay: %s", reminder.approval_id)
        return
    await sink.emit(
        ReplyPost(
            version=PROGRESS_REPLY_WIRE_VERSION,
            event="reply.post",
            delivery_id=reminder.delivery_id,
            target=ReplyTarget(
                kind=card.kind,
                address=card.channel,
                conversation_id=card.conversation_id,
                reply_ref=card.ts,
            ),
            message=OutboundMessage(
                version=MESSAGE_VERSION, text=f"Awaiting your decision: {card.summary}"
            ),
            requested_by="",
        ),
        route=TargetRoute(endpoint=card.endpoint, adapter=card.adapter),
    )
