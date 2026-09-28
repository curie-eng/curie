"""GitHub webhook receiver (J1).

Authenticated by the HMAC signature GitHub sends (not the platform API key), so
it lives outside the X-API-Key dependency. A push to the dev branch deploys, a
push to the prod branch promotes. Review events and, when enabled, factory
issue events are separate arms. Every other event is acknowledged and ignored.
"""

import json
import logging
import uuid

from fastapi import APIRouter, Header, HTTPException, Request, status

from ..config import get_settings
from ..deps import EvalQueueDep, SessionDep, StoreDep
from ..gitflow import log_push_outcome, process_push, verify_signature
from ..github_factory import handle_factory_delivery
from ..github_factory_events import is_plain_issue
from ..github_factory_review import (
    factory_owns,
    handle_factory_review_delivery,
    is_actionable_feedback,
)
from ..github_review_audit import claim_review_delivery, settle_review_delivery
from ..github_review_events import (
    FeedbackHeld,
    FeedbackIgnored,
    FeedbackUnavailable,
    parse_feedback,
)
from ..github_review_store import admit_feedback, hold_feedback
from ..models import GitHubReviewFeedback
from ..schemas import WebhookResult
from ..wirebody import read_bounded_body

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/github", tags=["github"])


@router.post("/webhook", response_model=WebhookResult)
async def github_webhook(
    request: Request,
    session: SessionDep,
    store: StoreDep,
    eval_queue: EvalQueueDep,
    x_github_event: str = Header(default=""),
    x_github_delivery: str = Header(default=""),
    x_hub_signature_256: str | None = Header(default=None),
) -> WebhookResult:
    settings = get_settings()
    body = await read_bounded_body(
        request, settings.github_webhook_max_body_bytes, subject="webhook body"
    )
    if not verify_signature(settings.github_webhook_secret, body, x_hub_signature_256):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid signature")

    if x_github_event == "ping":
        return WebhookResult(status="pong")
    is_review = x_github_event in {
        "issue_comment",
        "pull_request_review_comment",
        "pull_request_review",
    }
    factory_enabled = settings.github_factory_ingress_enabled
    # Disabled factory intake must keep ignoring issue events before JSON parsing.
    # test_unrelated_event_remains_ignored_without_parsing posts a non-JSON body.
    if x_github_event == "issues" and not factory_enabled:
        return WebhookResult(status="ignored")
    if x_github_event != "push" and not is_review and x_github_event != "issues":
        return WebhookResult(status="ignored")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "webhook body is not valid JSON") from exc

    if x_github_event == "issues" or (
        factory_enabled and x_github_event == "issue_comment" and is_plain_issue(payload)
    ):
        return await handle_factory_delivery(
            session,
            settings=settings,
            client=request.app.state.http_client,
            event=x_github_event,
            delivery_id=x_github_delivery,
            body=body,
            payload=payload,
        )

    if is_review:
        # A factory-owned pull request answers through the factory arm (#2798).
        # Otherwise the Slack-bound review arm keeps its merged behavior; with
        # it disabled, actionable PR feedback reaches the factory's unbound guard.
        review_enabled = settings.github_review_ingress_enabled
        if factory_enabled and (
            await factory_owns(session, x_github_event, payload)
            or (
                not review_enabled
                and is_actionable_feedback(x_github_event, payload, x_github_delivery)
            )
        ):
            return await handle_factory_review_delivery(
                session,
                settings=settings,
                client=request.app.state.http_client,
                event=x_github_event,
                delivery_id=x_github_delivery,
                body=body,
                payload=payload,
            )
        if not review_enabled:
            return WebhookResult(status="feedback_disabled")
        try:
            delivery_id = uuid.UUID(x_github_delivery)
            if str(delivery_id) != x_github_delivery.lower():
                raise ValueError("noncanonical delivery")
        except (ValueError, AttributeError):
            raise HTTPException(400, {"code": "invalid_delivery"}) from None
        audit, conflict = await claim_review_delivery(
            session,
            delivery_id=delivery_id,
            event=x_github_event,
            body=body,
            payload=payload,
        )
        if conflict:
            await session.commit()
            return WebhookResult(
                status="feedback_ignored",
                errors=[{"code": "delivery_identity_conflict"}],
            )
        if audit.status in {"ignored", "rejected"}:
            assert audit.reason is not None
            await session.commit()
            return WebhookResult(status="feedback_ignored", errors=[{"code": audit.reason}])
        if audit.status == "accepted":
            # The audit FK identifies the one canonical durable outbox row. A
            # same-delivery retry is an immediate recovery opportunity when a
            # prior XADD receipt outlived its failed SQL queued mark.
            event_id = audit.event_id
            existing = (
                await session.get(GitHubReviewFeedback, event_id) if event_id is not None else None
            )
            retry_waiting = existing is not None and existing.status == "waiting"
            await session.commit()
            if retry_waiting:
                assert event_id is not None
                await request.app.state.github_review_reconciler.reconcile_once(event_id)
            return WebhookResult(status="feedback_duplicate")
        try:
            feedback = parse_feedback(x_github_event, payload, x_github_delivery)
            row, created = await admit_feedback(
                session,
                feedback,
                settings=settings,
                client=request.app.state.http_client,
                traceparent=request.headers.get("traceparent"),
            )
        except FeedbackHeld as exc:
            # The PR's lineage has not yet recorded GitHub identity (#2962).
            # GitHub never redelivers, so hold the normalized feedback durably
            # and acknowledge; the reconciler or the identity advance replays it.
            try:
                await hold_feedback(
                    request.app.state.valkey,
                    feedback,
                    traceparent=request.headers.get("traceparent"),
                    settings=settings,
                )
            except Exception:
                # Without Valkey the hold cannot be durable; degrade to the
                # pre-#2962 refusal rather than acknowledge a lost review.
                logger.warning("GitHub review hold unavailable; refusing held feedback")
                settle_review_delivery(audit, "rejected", "lineage_absent_or_ambiguous")
                await session.commit()
                return WebhookResult(
                    status="feedback_ignored",
                    errors=[{"code": "lineage_absent_or_ambiguous"}],
                )
            settle_review_delivery(audit, "retryable", exc.code)
            await session.commit()
            return WebhookResult(status="feedback_held")
        except FeedbackUnavailable as exc:
            settle_review_delivery(audit, "retryable", exc.code)
            await session.commit()
            raise HTTPException(
                503,
                {"code": exc.code},
                headers={"Retry-After": "10"},
            ) from None
        except FeedbackIgnored as exc:
            if exc.code == "invalid_delivery":
                raise HTTPException(400, {"code": exc.code}) from None
            disposition = (
                "ignored"
                if exc.code
                in {
                    "unsupported_action",
                    "non_actionable_review",
                    "empty_feedback",
                    "edited_feedback",
                    "not_pull_request",
                    "non_human_sender",
                    "app_authored",
                }
                else "rejected"
            )
            settle_review_delivery(audit, disposition, exc.code)
            await session.commit()
            return WebhookResult(status="feedback_ignored", errors=[{"code": exc.code}])
        settle_review_delivery(audit, "accepted", event_id=row.event_id)
        retry_waiting = row.status == "waiting"
        await session.commit()
        if created or retry_waiting:
            # The committed row is the recovery authority if Valkey or this API
            # process fails between persistence, XADD, and the response. This
            # also repairs a distinct accepted delivery alias for waiting work.
            await request.app.state.github_review_reconciler.reconcile_once(row.event_id)
        if not created:
            return WebhookResult(status="feedback_duplicate")
        await session.refresh(row)
        return WebhookResult(status=f"feedback_{row.status}")

    result = await process_push(session, store, settings, eval_queue, payload)
    log_push_outcome(result, payload, source="github webhook")
    return result
