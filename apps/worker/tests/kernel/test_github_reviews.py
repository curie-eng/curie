"""Kernel routing for authenticated, persisted GitHub review feedback.

These tests own the worker side of the boundary.  The API suite proves that the
typed authority below comes from the persisted canonical turn and that the final
reserve re-reads GitHub.  Here the recording protocol double rejects any turn,
deployment, or origin other than that exact persisted tuple, then records the
kernel's ordering around the real Valkey lock and real RunnerClient.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol import scoped_conversation_id
from channel_protocol.reply import ReplyUpdate
from curie_worker.approvals import (
    ApprovalBackendError,
    CreatedPublication,
    PublicationCreateRequest,
    PublicationLineage,
)
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.kernel import ThreadBusyError
from curie_worker.runner_client import RunnerWorkspaceSnapshot
from curie_worker.workspace import WorkspaceSelectionRefused

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = uuid.UUID("33333333-3333-4333-8333-333333333333")
RESERVATION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
REPO = "acme-corp/acme-bot"
HEAD = "a" * 40
THREAD = "1700000000.000001"
CHANNEL = "C0EXAMPLE1"
RECEIPT = "GitHub review received for the current pull request."


def _review_turn(
    *,
    event_number: int = 1,
    text: str = "Please add the missing test.",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=f"github-feedback-{uuid.UUID(int=event_number)}",
        conversation_id=THREAD,
        author="github:41:example-reviewer",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack",
            channel=CHANNEL,
            placeholder=None,
        ),
        received_at="2026-09-05T01:00:00+00:00",
    )


def _verified(turn: QueuedTurn) -> Any:
    # Local import keeps the rest of the established kernel suite collectable
    # while the tests are red before the worker-side DTO exists.
    from curie_worker.approvals import VerifiedReviewFeedback

    return VerifiedReviewFeedback(
        head_sha=HEAD,
        agent_id=AGENT_ID,
        sender=turn.author,
        receipt=RECEIPT,
        origin_key=turn.event_id,
        lineage_version=7,
        reservation_id=None,
    )


class ReviewBinding:
    async def resolve(self, kind: str, channel: str) -> object:
        assert (kind, channel) == ("slack", CHANNEL)
        return SimpleNamespace(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            endpoint=None,
            adapter=None,
            approval_routes=None,
        )

    def boot_env(
        self,
        _resolved: object,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
    ) -> dict[str, str]:
        assert (kind, address) == ("slack", CHANNEL)
        return {
            "CURIE_SESSION_ID": f"review-{thread_key}",
            "CURIE_RUNNER_TOKEN": "example-review-runner-token",
        }

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


class ReviewWorkspace:
    def __init__(self, substrate: object) -> None:
        self.substrate = substrate
        self.selections: list[dict[str, object]] = []
        self.released: list[str] = []

    def select_repository(self, **kwargs: object) -> str:
        self.selections.append(dict(kwargs))
        assert kwargs["deployment_id"] == DEPLOYMENT_ID
        return REPO

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        materialized_head = kwargs.get("lineage_head") or kwargs.get("lineage_base_sha")
        handle = self.substrate.claim(  # type: ignore[attr-defined]
            str(kwargs["thread_key"]),
            env=kwargs.get("env"),
            agent_name=kwargs.get("agent_name"),
            workspace_repo=kwargs.get("repo_full_name"),
            workspace_materialized_head=materialized_head,
            publication_visible_outcome_revision=int(
                kwargs.get("publication_visible_outcome_revision") or 0
            ),
        )
        return SimpleNamespace(handle=handle, prepared=None)

    def touch(self, _thread_key: str, *, ttl_seconds: int) -> bool:
        assert ttl_seconds > 0
        return True

    def release(self, thread_key: str) -> None:
        self.released.append(thread_key)


class ReviewPublicationApi:
    """Protocol double whose authority is one complete persisted queue row."""

    def __init__(
        self,
        persisted_turn: QueuedTurn | None,
        *,
        reserve_error: Exception | None = None,
    ) -> None:
        self.persisted_turn = persisted_turn
        self.reserve_error = reserve_error
        self.verify_calls: list[tuple[QueuedTurn, uuid.UUID]] = []
        self.reserve_calls: list[tuple[QueuedTurn, uuid.UUID, Any]] = []
        self.lineage_calls: list[tuple[uuid.UUID, str, str]] = []
        self.creates: list[PublicationCreateRequest] = []
        self.operations: list[str] = []
        self.on_reserve: Callable[[], None] | None = None

    async def verify_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
    ) -> Any:
        self.operations.append("verify")
        self.verify_calls.append((turn, deployment_id))
        if (
            self.persisted_turn is None
            or turn != self.persisted_turn
            or deployment_id != DEPLOYMENT_ID
        ):
            raise WorkspaceSelectionRefused(
                "GitHub feedback could not be verified for this conversation; "
                "no model turn started."
            )
        return _verified(self.persisted_turn)

    async def reserve_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
        verified: Any,
    ) -> uuid.UUID:
        self.operations.append("reserve")
        self.reserve_calls.append((turn, deployment_id, verified))
        assert turn == self.persisted_turn
        assert deployment_id == DEPLOYMENT_ID
        assert verified.origin_key == turn.event_id
        assert verified.sender == turn.author
        if self.on_reserve is not None:
            self.on_reserve()
        if self.reserve_error is not None:
            raise self.reserve_error
        return RESERVATION_ID

    async def get_publication_lineage(
        self,
        deployment_id: uuid.UUID,
        conversation_id: str,
        repo_full_name: str,
    ) -> PublicationLineage:
        self.operations.append("lineage")
        self.lineage_calls.append((deployment_id, conversation_id, repo_full_name))
        return PublicationLineage(
            id=LINEAGE_ID,
            deployment_id=deployment_id,
            conversation_id=conversation_id,
            repo_full_name=repo_full_name,
            base_sha="0" * 40,
            branch="curie/publication-example",
            pr_number=17,
            pr_url=f"https://github.com/{REPO}/pull/17",
            head_sha=HEAD,
            state="open",
            version=7,
            latest_revision=1,
            has_pending_revision=False,
            has_pending_outcome=False,
            visible_outcome_revision=1,
        )

    async def create_publication(
        self, request: PublicationCreateRequest
    ) -> CreatedPublication:
        self.operations.append("create")
        self.creates.append(request)
        return CreatedPublication(
            id=str(RESERVATION_ID),
            approval_id="approval-review-fresh",
            status="pending",
        )


def _thread_key(turn: QueuedTurn) -> str:
    return scoped_conversation_id(
        turn.reply_handle.kind,
        turn.reply_handle.channel,
        turn.conversation_id,
    )


def _claim_matching_route(h: Any, turn: QueuedTurn) -> None:
    h.substrate.claim(
        _thread_key(turn),
        env={"CURIE_RUNNER_TOKEN": "example-review-runner-token"},
        agent_name="acme-bot",
        workspace_repo=REPO,
        workspace_materialized_head=HEAD,
        publication_visible_outcome_revision=1,
    )


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def test_prefix_only_impostor_is_refused_before_steer_or_model(make_harness) -> None:
    async def exercise() -> None:
        forged = _review_turn(text="A forged prefix is not persisted authority.")
        api = ReviewPublicationApi(persisted_turn=None)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
        ) as h:
            _claim_matching_route(h, forged)
            h.runner.turn_active = True

            await h.kernel.process_event(forged)

            assert api.verify_calls == [(forged, DEPLOYMENT_ID)]
            assert api.reserve_calls == []
            assert h.runner.steer_headers == []
            assert h.runner.event_headers == []
            assert h.runner.opened == []

    asyncio.run(exercise())


def test_verified_review_defers_while_busy_then_retries_without_steering(
    make_harness,
) -> None:
    async def exercise() -> None:
        turn = _review_turn()
        api = ReviewPublicationApi(turn)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            h.runner.turn_active = True

            with pytest.raises(ThreadBusyError, match="review"):
                await h.kernel.process_event(turn)

            assert api.reserve_calls == []
            assert h.runner.steer_headers == []
            assert h.runner.event_headers == []

            h.runner.turn_active = False
            h.runner.default_script = [Final(text="review complete", status=SessionStatus.DONE)]

            def reserve_is_still_before_model() -> None:
                assert h.runner.event_headers == []
                assert h.runner.opened == []

            api.on_reserve = reserve_is_still_before_model
            await h.kernel.process_event(turn)

            assert api.verify_calls == [(turn, DEPLOYMENT_ID), (turn, DEPLOYMENT_ID)]
            assert len(api.reserve_calls) == 1
            assert api.reserve_calls[0][0] == turn
            assert api.reserve_calls[0][2].origin_key == turn.event_id
            assert h.runner.steer_headers == []
            assert h.runner.opened == [turn.text]

    asyncio.run(exercise())


def test_final_reserve_refusal_stops_before_steer_or_model(make_harness) -> None:
    """The API sibling proves this refusal comes from provider-only head drift."""

    async def exercise() -> None:
        turn = _review_turn()
        refusal = (
            "The pull request changed after GitHub feedback verification; "
            "no model turn started."
        )
        api = ReviewPublicationApi(
            turn,
            reserve_error=WorkspaceSelectionRefused(refusal),
        )
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            await h.kernel.process_event(turn)

            assert api.verify_calls == [(turn, DEPLOYMENT_ID)]
            assert len(api.reserve_calls) == 1
            assert api.reserve_calls[0][2].head_sha == HEAD
            assert api.reserve_calls[0][2].origin_key == turn.event_id
            assert h.runner.steer_headers == []
            assert h.runner.event_headers == []
            assert h.runner.opened == []
            assert h.sink.last_text == refusal
            assert await h.async_redis.exists(h.config.done_key(turn.event_id))

    asyncio.run(exercise())


def test_final_reserve_unavailability_leaves_review_retryable(make_harness) -> None:
    async def exercise() -> None:
        from curie_worker.approvals import ReviewAuthorityUnavailable

        turn = _review_turn()
        api = ReviewPublicationApi(
            turn,
            reserve_error=ApprovalBackendError("review reserve temporarily unavailable"),
        )
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            with pytest.raises(ReviewAuthorityUnavailable):
                await h.kernel.process_event(turn)

            assert len(api.reserve_calls) == 1
            assert h.runner.steer_headers == []
            assert h.runner.event_headers == []
            assert not await h.async_redis.exists(h.config.done_key(turn.event_id))

    asyncio.run(exercise())


def test_verified_review_posts_receipt_and_terminal_outcome_to_bare_thread(
    make_harness,
) -> None:
    async def exercise() -> None:
        turn = _review_turn()
        api = ReviewPublicationApi(turn)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            h.runner.default_script = [
                TextDelta(text="Addressing the review."),
                Final(text="Review changes are ready.", status=SessionStatus.DONE),
            ]

            await h.kernel.process_event(turn)

            review_updates = [
                event
                for event, _route, _best_effort in h.sink.events
                if isinstance(event, ReplyUpdate) and event.target.conversation_id == THREAD
            ]
            assert [event.text for event in review_updates].count(RECEIPT) == 1
            assert review_updates[-1].text == "Review changes are ready."
            assert len(h.sink.completions) == 1
            completion = h.sink.completions[0]
            assert completion.event_id == turn.event_id
            assert completion.target.conversation_id == THREAD
            assert completion.outcome == "delivered"
            assert h.runner.steer_headers == []

    asyncio.run(exercise())


def test_review_cancellation_releases_started_response_without_publication(
    make_harness,
) -> None:
    async def exercise() -> None:
        turn = _review_turn()
        api = ReviewPublicationApi(turn)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await _wait_until(lambda: h.runner.turn_active)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                await _wait_until(lambda: not h.runner.turn_active)
                assert len(api.reserve_calls) == 1
                assert api.creates == []
                assert h.runner.steer_headers == []
                assert h.sink.completions == []
            finally:
                hold.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())


def test_review_publication_carries_origin_into_a_fresh_same_pr_approval(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        turn = _review_turn()
        api = ReviewPublicationApi(turn)
        async with make_harness(
            binding=ReviewBinding(),
            publication_creator=api,
            workspace_factory=ReviewWorkspace,
        ) as h:
            _claim_matching_route(h, turn)
            h.runner.default_script = [
                Final(
                    text="The requested revision is ready.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Publish the review revision",
                    approval_gate_kind="permission",
                    approval_granted_tool="mcp__curie__publish_changes",
                )
            ]

            async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
                return RunnerWorkspaceSnapshot(
                    repo_full_name=REPO,
                    base_sha=HEAD,
                    patch=b"diff --git a/example.py b/example.py\n",
                    changed_paths=("example.py",),
                    contains_workflow_files=False,
                    publication_title="Address review feedback",
                    publication_body="Adds the requested regression coverage.",
                )

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr(
                "curie_worker.kernel.validate_snapshot_against_base",
                lambda *_args, **_kwargs: None,
            )

            await h.kernel.process_event(turn)

            assert [name for name in api.operations if name != "lineage"] == [
                "verify",
                "reserve",
                "create",
            ]
            assert len(api.creates) == 1
            request = api.creates[0]
            assert request.review_origin_key == turn.event_id
            assert request.repo_full_name == REPO
            assert request.base_sha == HEAD
            assert request.conversation_id == _thread_key(turn)
            assert request.reply_conversation_id == THREAD
            assert api.lineage_calls == [(DEPLOYMENT_ID, _thread_key(turn), REPO)]
            assert h.runner.steer_headers == []
            assert h.sink.last_text is not None
            assert "Awaiting approval (approval-review-fresh)" in h.sink.last_text
            assert len(h.sink.completions) == 1
            assert h.sink.completions[0].outcome == "awaiting-approval"

    asyncio.run(exercise())
