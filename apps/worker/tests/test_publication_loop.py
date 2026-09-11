"""Approval decisions reconcile to one publication Job and a direct routed result."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import threading
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from channel_protocol import scoped_conversation_id
from channel_protocol.reply import ReplyAck, ReplyTarget
from curie_worker.approval_cards import ApprovalCardRef
from curie_worker.publication_loop import PublicationReconcileError
from curie_worker.publication_store import (
    PostgresPublicationStore,
    PublicationStoreError,
)
from curie_worker.reply_sink import TargetRoute
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

PUBLICATION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
APPROVAL_ID = uuid.UUID("33333333-3333-4333-8333-333333333333")
AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
PR_URL = "https://github.com/acme-corp/acme-bot/pull/123"
RESOLVER = "U0APPROVE1"
RESOLUTION_NOTE = "Ready to publish."
CONVERSATION_ID = "1700000000.000100"
WORKSPACE_CONVERSATION_ID = scoped_conversation_id(
    "slack", "C0EXAMPLE1", CONVERSATION_ID
)
LINEAGE_ID = uuid.UUID("55555555-5555-4555-8555-555555555555")
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
LINEAGE_BRANCH = "curie/thread-lineage-example"
PRIOR_HEAD = "a" * 40
REVISION_HEAD = "b" * 40
_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def publication() -> Any:
    return importlib.import_module("curie_worker.publication_loop")


class _Store:
    def __init__(self) -> None:
        self.completed: dict[uuid.UUID, tuple[str, str | None]] = {}
        self.failures: list[tuple[uuid.UUID, str]] = []
        self.pending: dict[uuid.UUID, Any] = {}
        self.delivered: set[uuid.UUID] = set()
        self.retries: list[tuple[uuid.UUID, str]] = []
        self.delivery_retries: list[tuple[uuid.UUID, str]] = []
        self.target = _target()
        self.workspace_conversation_id = WORKSPACE_CONVERSATION_ID
        self.route = TargetRoute(endpoint=None, adapter=None)
        self.retry_terminal_after = 99
        self.card_pending: Any | None = None
        self.card_delivery_retries: list[tuple[uuid.UUID, str]] = []
        self.card_delivered: set[uuid.UUID] = set()
        self.card_retry_terminal_after = 99
        self.cleanup_pending: set[uuid.UUID] = set()
        self.cleanup_claimed: set[uuid.UUID] = set()
        self.cleanup_completed: set[uuid.UUID] = set()
        self.cleanup_retries: list[tuple[uuid.UUID, str]] = []
        self.lineage_advances: list[dict[str, Any]] = []
        self.lineage_terminals: list[dict[str, Any]] = []
        self.history_ready: set[uuid.UUID] = set()

    def claim_pending_card(self) -> Any | None:
        return self.card_pending

    def mark_card_delivered(self, publication_id: uuid.UUID) -> None:
        self.card_delivered.add(publication_id)
        self.card_pending = None

    def retry_card_delivery(self, publication_id: uuid.UUID, *, error: str) -> None:
        self.card_delivery_retries.append((publication_id, error))
        if len(self.card_delivery_retries) >= self.card_retry_terminal_after:
            self.completed[publication_id] = ("failed", None)
            self.pending[publication_id] = {
                "outcome": "failed",
                "pr_url": None,
                "error": f"publication approval card could not be delivered: {error}",
            }
            self.cleanup_pending.add(publication_id)
            self.card_pending = None

    def claim_pending_cleanup(self) -> Any | None:
        publication_id = next(iter(self.cleanup_pending), None)
        if publication_id is None:
            return None
        self.cleanup_claimed.add(publication_id)
        return SimpleNamespace(publication_id=publication_id, version=1)

    def mark_cleanup_completed(self, publication_id: uuid.UUID) -> None:
        self.cleanup_pending.discard(publication_id)
        self.cleanup_claimed.discard(publication_id)
        self.cleanup_completed.add(publication_id)

    def retry_cleanup(self, publication_id: uuid.UUID, *, error: str) -> None:
        self.cleanup_retries.append((publication_id, error))
        self.cleanup_claimed.discard(publication_id)

    def is_terminal(self, publication_id: uuid.UUID) -> bool:
        return publication_id in self.completed

    def complete(self, publication_id: uuid.UUID, *, outcome: str, pr_url: str | None) -> None:
        self.completed[publication_id] = (outcome, pr_url)

    def fail(self, publication_id: uuid.UUID, *, error: str) -> None:
        self.failures.append((publication_id, error))

    def pending_result(self, publication_id: uuid.UUID | None = None) -> Any | None:
        if publication_id is None:
            publication_id = next(iter(self.pending), None)
        if publication_id is None:
            return None
        value = self.pending.get(publication_id)
        if value is None:
            return None
        if (
            value["outcome"] in {"published", "failed"}
            and publication_id in self.cleanup_pending
        ):
            return None
        result = {
            "resolved_by": None,
            "resolution_note": None,
            **value,
        }
        return SimpleNamespace(
            publication_id=publication_id,
            approval_id=APPROVAL_ID,
            agent_id=AGENT_ID,
            workspace_conversation_id=self.workspace_conversation_id,
            target=self.target,
            route=self.route,
            attempt=1,
            version=1,
            **result,
        )

    def persist_result(
        self,
        publication_id: uuid.UUID,
        *,
        outcome: str,
        pr_url: str | None,
        error: str | None,
        **lineage: Any,
    ) -> None:
        self.completed[publication_id] = (outcome, pr_url)
        if outcome == "failed" and error is not None:
            self.failures.append((publication_id, error))
        self.pending[publication_id] = {
            "outcome": outcome,
            "pr_url": pr_url,
            "error": error,
            "resolved_by": RESOLVER if outcome != "expired" else None,
            "resolution_note": RESOLUTION_NOTE if outcome != "expired" else None,
        }
        if outcome in {"published", "failed"}:
            self.cleanup_pending.add(publication_id)
        if lineage:
            self.lineage_advances.append(dict(lineage))

    def mark_result_delivered(self, publication_id: uuid.UUID) -> None:
        self.delivered.add(publication_id)
        self.pending.pop(publication_id, None)

    def mark_outcome_history_ready(self, publication_id: uuid.UUID) -> None:
        self.history_ready.add(publication_id)

    def retry_result_delivery(self, publication_id: uuid.UUID, *, error: str) -> None:
        self.delivery_retries.append((publication_id, error))

    def retry(self, publication_id: uuid.UUID, *, error: str) -> None:
        self.retries.append((publication_id, error))
        if len(self.retries) >= self.retry_terminal_after:
            self.completed[publication_id] = ("failed", None)
            self.failures.append((publication_id, error))
            self.pending[publication_id] = {
                "outcome": "failed",
                "pr_url": None,
                "error": error,
            }
            self.cleanup_pending.add(publication_id)

    def mark_lineage_terminal(
        self,
        lineage_id: uuid.UUID,
        *,
        expected_version: int,
        expected_stored_head: str | None,
        state: str,
        pr_number: int,
        pr_url: str,
        head_sha: str,
    ) -> None:
        self.lineage_terminals.append(
            {
                "lineage_id": lineage_id,
                "expected_version": expected_version,
                "expected_stored_head": expected_stored_head,
                "state": state,
                "pr_number": pr_number,
                "pr_url": pr_url,
                "head_sha": head_sha,
            }
        )

    async def claim_next(self) -> None:
        return None


class _Credentials:
    def __init__(self, module: Any) -> None:
        self.module = module
        self.calls: list[uuid.UUID] = []
        self.error: Exception | None = None

    def redeem(self, publication_id: uuid.UUID) -> Any:
        self.calls.append(publication_id)
        if self.error is not None:
            raise self.error
        return self.module.PublicationCredential(
            clean_clone_url="https://github.com/acme-corp/acme-bot.git",
            authorization_header="Basic publication-write-credential-value",
        )


class _Cluster:
    def __init__(self, module: Any) -> None:
        self.module = module
        self.applied: list[Any] = []
        self.credentials_cleaned: list[Any] = []
        self.terminals_cleaned: list[Any] = []
        self.observation = module.PublicationJobObservation(
            phase="succeeded",
            pr_url=PR_URL,
            pr_number=123,
            commit_sha=REVISION_HEAD,
            logs=(
                f"CURIE_PR_URL={PR_URL}\n"
                f"CURIE_PR_NUMBER=123\nCURIE_COMMIT_SHA={REVISION_HEAD}\n"
            ),
        )
        self.preexisting_observation: Any | None = None
        self.raise_after_apply = False
        self.apply_error: Exception | None = None
        self.observe_after_apply_error: Exception | None = None
        self.terminal_cleanup_fail_once = False
        self.terminal_cleanup_failures_remaining = 0
        self.validated_existing: list[Any] = []
        self.active_jobs: set[str] = set()
        self.observe_release: threading.Event | None = None
        self.observe_timed_out = False

    def apply(self, resources: Any) -> None:
        self.applied.append(resources)
        if self.apply_error is not None:
            raise self.apply_error
        self.active_jobs.add(resources.names.job)
        if self.raise_after_apply:
            self.raise_after_apply = False
            raise RuntimeError("worker stopped after apiserver accepted resources")

    def validate_existing(self, resources: Any) -> None:
        self.validated_existing.append(resources)
        if self.apply_error is not None:
            raise self.apply_error

    def observe(self, job_name: str) -> Any:
        if self.observe_release is not None and not self.observe_release.wait(
            timeout=0.2
        ):
            self.observe_timed_out = True
        if self.applied and self.observe_after_apply_error is not None:
            raise self.observe_after_apply_error
        if job_name not in self.active_jobs:
            return self.preexisting_observation or self.module.PublicationJobObservation(
                phase="pending", pr_url=None, logs="", exists=False
            )
        return self.observation

    def cleanup(self, names: Any) -> None:
        self.credentials_cleaned.append(names)
        self.terminals_cleaned.append(names)

    def cleanup_credentials(self, names: Any) -> None:
        self.credentials_cleaned.append(names)

    def cleanup_terminal(self, names: Any) -> None:
        if self.terminal_cleanup_fail_once or self.terminal_cleanup_failures_remaining:
            self.terminal_cleanup_fail_once = False
            self.terminal_cleanup_failures_remaining = max(
                0, self.terminal_cleanup_failures_remaining - 1
            )
            raise RuntimeError("publication resource cleanup unavailable")
        self.terminals_cleaned.append(names)
        self.active_jobs.discard(names.job)


class _GitHub:
    def __init__(self) -> None:
        self.number_calls: list[tuple[str, int]] = []
        self.branch_calls: list[tuple[str, str]] = []
        self.recover_calls: list[tuple[str, str, str]] = []
        self.verify_calls: list[tuple[str, str, uuid.UUID, str]] = []
        self.authorization_headers: list[str] = []
        self.state = "open"
        self.head_sha = PRIOR_HEAD
        self.branch_head: str | None = None
        self.recovered_pr_url = PR_URL
        self.recovered_head_sha = REVISION_HEAD
        self.recovered_pr_state = "open"
        self.verified_revision_head: str | None = None
        self.verified_revision_id: uuid.UUID | None = None
        self.verified_expected_parent: str | None = None

    def read_pr_by_number(
        self,
        repo_full_name: str,
        pr_number: int,
        authorization_header: str,
    ) -> Any:
        self.authorization_headers.append(authorization_header)
        if not authorization_header:
            raise AssertionError(
                "GitHub lineage lookup happened before publication credential redemption"
            )
        self.number_calls.append((repo_full_name, pr_number))
        return SimpleNamespace(
            number=pr_number,
            url=PR_URL,
            state=self.state,
            head_sha=self.head_sha,
            head_ref=LINEAGE_BRANCH,
        )

    def verify_revision_commit(
        self,
        repo_full_name: str,
        commit_sha: str,
        *,
        revision_id: uuid.UUID,
        expected_parent: str,
        authorization_header: str,
    ) -> str:
        self.authorization_headers.append(authorization_header)
        if not authorization_header:
            raise AssertionError(
                "GitHub revision verification happened before credential redemption"
            )
        self.verify_calls.append(
            (repo_full_name, commit_sha, revision_id, expected_parent)
        )
        if (
            commit_sha != self.verified_revision_head
            or revision_id != self.verified_revision_id
            or expected_parent != self.verified_expected_parent
        ):
            raise PublicationReconcileError(
                "remote head is not this revision's marked commit with expected parent"
            )
        return commit_sha

    def read_branch_head(
        self,
        repo_full_name: str,
        branch: str,
        authorization_header: str,
    ) -> str | None:
        self.authorization_headers.append(authorization_header)
        self.branch_calls.append((repo_full_name, branch))
        return self.branch_head

    def recover_pr_by_head(
        self,
        repo_full_name: str,
        branch: str,
        title: str,
        body: str,
        *,
        expected_head_sha: str,
        authorization_header: str,
    ) -> Any | None:
        self.authorization_headers.append(authorization_header)
        self.recover_calls.append((repo_full_name, branch, expected_head_sha))
        if expected_head_sha != self.recovered_head_sha:
            raise PublicationReconcileError(
                "recoverable pull request head does not match the expected commit"
            )
        if self.recovered_pr_url is None:
            return None
        return SimpleNamespace(
            number=123,
            url=self.recovered_pr_url,
            state=self.recovered_pr_state,
            head_sha=self.recovered_head_sha,
            head_ref=LINEAGE_BRANCH,
        )

    def allow_exact_revision(
        self, commit_sha: str, revision_id: uuid.UUID, expected_parent: str
    ) -> None:
        self.verified_revision_head = commit_sha
        self.verified_revision_id = revision_id
        self.verified_expected_parent = expected_parent


class _Replies:
    def __init__(self) -> None:
        self.events: list[tuple[Any, TargetRoute]] = []
        self.fail_once = False
        self.on_emit: Any | None = None
        self.post_refs: dict[str, str] = {}

    async def emit(
        self, event: Any, *, route: TargetRoute, best_effort_unreachable: bool = False
    ) -> Any:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("reply transport unavailable")
        self.events.append((event, route))
        if self.on_emit is not None:
            self.on_emit()
        interaction = getattr(getattr(event, "message", None), "interaction", None)
        interaction_id = getattr(interaction, "id", None)
        if event.event == "reply.post" and interaction_id:
            ref = self.post_refs.setdefault(interaction_id, "1700000000.000050")
            return ReplyAck(ref=ref)
        return ReplyAck(ref=event.target.reply_ref)


class _Cards:
    def __init__(self) -> None:
        self.ref: ApprovalCardRef | None = None
        self.key = str(APPROVAL_ID)
        self.popped: list[str] = []
        self.restored: list[tuple[str, ApprovalCardRef]] = []
        self.remember_fail_once = False
        self.restore_failures_remaining = 0

    async def pop(self, approval_id: str) -> ApprovalCardRef | None:
        self.popped.append(approval_id)
        if approval_id != self.key:
            return None
        ref, self.ref = self.ref, None
        return ref

    async def restore(self, approval_id: str, ref: ApprovalCardRef) -> None:
        self.restored.append((approval_id, ref))
        if self.restore_failures_remaining:
            self.restore_failures_remaining -= 1
            raise RuntimeError("card ref restore unavailable")
        if self.ref is None:
            self.ref = ref
            self.key = approval_id

    async def remember(
        self,
        approval_id: str,
        *,
        channel: str,
        ts: str,
        summary: str,
        endpoint: str | None,
        requested_by: str = "",
        kind: str = "",
        adapter: str | None = None,
    ) -> None:
        if self.remember_fail_once:
            self.remember_fail_once = False
            raise RuntimeError("card ref store unavailable")
        self.ref = ApprovalCardRef(
            channel=channel,
            ts=ts,
            summary=summary,
            endpoint=endpoint,
            requested_by=requested_by,
            kind=kind,
            adapter=adapter,
        )
        self.key = approval_id


class _Transcript:
    def __init__(self) -> None:
        self.records: list[tuple[uuid.UUID, str, uuid.UUID, str]] = []
        self.failures_remaining = 0
        self.error: Exception = RuntimeError("transcript API unavailable")

    async def record_result(
        self,
        agent_id: uuid.UUID,
        conversation_id: str,
        publication_id: uuid.UUID,
        text: str,
    ) -> None:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise self.error
        self.records.append((agent_id, conversation_id, publication_id, text))


_DEFAULT_TRANSCRIPT = object()


def _card() -> ApprovalCardRef:
    return ApprovalCardRef(
        channel="C0EXAMPLE1",
        ts="1700000000.000050",
        summary="Publish these repository changes?",
        requested_by="requester@example.test",
        kind="slack",
    )


def _target(kind: str = "slack") -> ReplyTarget:
    return ReplyTarget(
        kind=kind,
        address="C0EXAMPLE1" if kind == "slack" else "agent@example.test",
        conversation_id=CONVERSATION_ID,
        reply_ref=None,
    )


def _work(module: Any, *, decision: str = "approved", kind: str = "slack") -> Any:
    return module.PublicationWork(
        publication_id=PUBLICATION_ID,
        approval_id=APPROVAL_ID,
        decision=decision,
        lineage_id=LINEAGE_ID,
        lineage_version=1,
        revision_id=REVISION_ID,
        revision_number=1,
        repo_full_name="acme-corp/acme-bot",
        branch=LINEAGE_BRANCH,
        pr_number=None,
        pr_url=None,
        expected_prior_head=PRIOR_HEAD,
        expected_remote_head=None,
        base_sha="a" * 40,
        patch=b"diff --git a/README.md b/README.md\n",
        changed_paths=("README.md",),
        title="Update repository",
        body="Approved platform publication.",
        target=_target(kind),
        route=TargetRoute(
            endpoint=None if kind == "slack" else "https://adapter.example.com/replies",
            adapter=None if kind == "slack" else "agentmail-sandbox",
        ),
        version=1,
    )


def _lineage_work(
    module: Any,
    *,
    publication_id: uuid.UUID = PUBLICATION_ID,
    revision_id: uuid.UUID = REVISION_ID,
    revision_number: int = 2,
    decision: str = "approved",
    pr_number: int | None = 123,
    pr_url: str | None = PR_URL,
    expected_prior_head: str | None = PRIOR_HEAD,
) -> Any:
    return module.PublicationWork(
        publication_id=publication_id,
        approval_id=APPROVAL_ID,
        decision=decision,
        lineage_id=LINEAGE_ID,
        lineage_version=2,
        revision_id=revision_id,
        revision_number=revision_number,
        repo_full_name="acme-corp/acme-bot",
        branch=LINEAGE_BRANCH,
        pr_number=pr_number,
        pr_url=pr_url,
        expected_prior_head=expected_prior_head,
        expected_remote_head=(expected_prior_head if pr_number is not None else None),
        base_sha=expected_prior_head or PRIOR_HEAD,
        patch=b"diff --git a/README.md b/README.md\n",
        changed_paths=("README.md",),
        title="Update repository",
        body="Approved platform publication.",
        target=_target(),
        route=TargetRoute(endpoint=None, adapter=None),
        version=1,
    )


def _card_work() -> Any:
    return SimpleNamespace(
        publication_id=PUBLICATION_ID,
        approval_id=APPROVAL_ID,
        summary="Publish these repository changes?",
        requested_by="U0REQUEST1",
        target=ReplyTarget(
            kind="slack",
            address="C0EXAMPLE1",
            conversation_id="1700000000.000100",
            reply_ref=None,
        ),
        route=TargetRoute(endpoint=None, adapter=None),
        attempt=1,
        version=1,
    )


def _loop(
    module: Any,
    cards: _Cards | None = None,
    transcript: _Transcript | None | object = _DEFAULT_TRANSCRIPT,
) -> tuple[Any, _Store, _Credentials, _Cluster, _GitHub, _Replies]:
    k8s = importlib.import_module("curie_worker.publication_k8s")
    store = _Store()
    credentials = _Credentials(module)
    cluster = _Cluster(module)
    github = _GitHub()
    replies = _Replies()
    cards = cards or _Cards()
    if transcript is _DEFAULT_TRANSCRIPT:
        transcript = _Transcript()
    loop = module.PublicationReconciler(
        store=store,
        credentials=credentials,
        cluster=cluster,
        github=github,
        replies=replies,
        card_store=cards,
        transcript=transcript if isinstance(transcript, _Transcript) else None,
        job_settings=k8s.PublicationJobSettings(
            namespace="curie",
            runner_image="ghcr.io/curie-eng/curie-runner:v0.7.0",
            image_pull_policy="IfNotPresent",
            image_pull_secrets=("registry-creds",),
            priority_class_name="curie-platform-critical",
            service_account_name="curie-publication",
            owner_name="curie-publication-owner",
            git_user_name="Curie Publisher",
            git_user_email="publisher@example.com",
            cpu_request="100m",
            cpu_limit="1",
            memory_request="256Mi",
            memory_limit="1Gi",
            ephemeral_request="1Gi",
            ephemeral_limit="4Gi",
        ),
    )
    return loop, store, credentials, cluster, github, replies


def _job_env(resources: Any) -> dict[str, str]:
    container = resources.job["spec"]["template"]["spec"]["containers"][0]
    return {item["name"]: item["value"] for item in container["env"]}


async def test_publication_card_outbox_posts_and_remembers_before_ack(
    publication: Any,
) -> None:
    cards = _Cards()
    loop, store, _, _, _, replies = _loop(publication, cards)
    store.card_pending = _card_work()

    assert await loop.deliver_pending_card() is True

    assert store.card_delivered == {PUBLICATION_ID}
    assert store.card_pending is None
    assert cards.ref is not None
    assert cards.ref.ts == "1700000000.000050"
    assert cards.key == str(APPROVAL_ID)
    event = replies.events[0][0]
    assert event.event == "reply.post"
    assert event.target.conversation_id == "1700000000.000100"
    assert event.message.interaction.id == str(APPROVAL_ID)


@pytest.mark.parametrize("legacy", [False, True], ids=["scoped", "legacy"])
async def test_terminal_result_is_recorded_for_the_next_model_turn(
    publication: Any,
    legacy: bool,
) -> None:
    transcript = _Transcript()
    loop, store, _, _, _, replies = _loop(publication, transcript=transcript)
    store.completed[PUBLICATION_ID] = ("published", PR_URL)
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
    }
    if legacy:
        store.workspace_conversation_id = CONVERSATION_ID

    assert await loop.deliver_pending_result(PUBLICATION_ID) is True

    assert transcript.records == [
        (
            AGENT_ID,
            CONVERSATION_ID if legacy else WORKSPACE_CONVERSATION_ID,
            PUBLICATION_ID,
            f"Published the approved changes: {PR_URL}",
        )
    ]
    assert store.history_ready == {PUBLICATION_ID}
    event = replies.events[0][0]
    assert PR_URL in event.text
    assert event.target.address == "C0EXAMPLE1"
    assert event.target.conversation_id == CONVERSATION_ID


async def test_transient_transcript_failure_delivers_and_settles_before_retry(
    publication: Any,
) -> None:
    cards = _Cards()
    transcript = _Transcript()
    transcript.failures_remaining = 1
    loop, store, _, _, _, replies = _loop(
        publication,
        cards,
        transcript=transcript,
    )
    cards.ref = _card()
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
        "resolved_by": RESOLVER,
        "resolution_note": RESOLUTION_NOTE,
    }

    assert await loop.deliver_pending_result(PUBLICATION_ID) is True

    assert [event.event for event, _ in replies.events] == [
        "reply.update",
        "reply.update",
    ]
    assert replies.events[1][0].settled.decision == "approved"
    assert cards.ref is None
    assert PUBLICATION_ID not in store.delivered
    assert PUBLICATION_ID not in store.history_ready
    assert store.delivery_retries == [
        (
            PUBLICATION_ID,
            "publication transcript recording failed: transcript API unavailable",
        )
    ]

    assert await loop.deliver_pending_result(PUBLICATION_ID) is True

    assert PUBLICATION_ID in store.delivered
    assert store.history_ready == {PUBLICATION_ID}
    assert len(transcript.records) == 1
    assert len(replies.events) == 3
    assert replies.events[2][0].settled is None


async def test_transcript_capacity_refusal_uses_compact_durable_outcome_before_ready(
    publication: Any,
) -> None:
    cards = _Cards()
    transcript = _Transcript()
    transcript.failures_remaining = 1
    transcript.error = publication.PublicationTranscriptPermanentError(
        "publication transcript append exceeded durable state capacity"
    )
    loop, store, _, _, _, replies = _loop(
        publication,
        cards,
        transcript=transcript,
    )
    cards.ref = _card()
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
        "resolved_by": RESOLVER,
        "resolution_note": RESOLUTION_NOTE,
    }

    assert await loop.deliver_pending_result(PUBLICATION_ID) is True

    assert PUBLICATION_ID in store.delivered
    assert store.history_ready == {PUBLICATION_ID}
    assert store.delivery_retries == []
    assert transcript.records == [
        (
            AGENT_ID,
            WORKSPACE_CONVERSATION_ID,
            PUBLICATION_ID,
            "Publication outcome: published. Details omitted because thread "
            "history is at capacity.",
        )
    ]
    assert PR_URL in replies.events[0][0].text
    assert replies.events[1][0].settled.decision == "approved"
    assert cards.ref is None


async def test_missing_transcript_wiring_is_logged_loudly(
    publication: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger="curie_worker.publication_loop"):
        _loop(publication, transcript=None)

    assert "publication transcript recording is not configured" in caplog.text


async def test_publication_card_crash_after_post_adopts_same_ref_on_retry(
    publication: Any,
) -> None:
    cards = _Cards()
    cards.remember_fail_once = True
    loop, store, _, _, _, replies = _loop(publication, cards)
    store.card_pending = _card_work()

    with pytest.raises(RuntimeError, match="card ref store unavailable"):
        await loop.deliver_pending_card()
    assert store.card_delivered == set()

    await loop.deliver_pending_card()

    assert len(replies.events) == 2
    assert {
        replies.post_refs[str(APPROVAL_ID)]
    } == {"1700000000.000050"}, "the UUID idempotency key adopts one Slack message"
    assert cards.ref is not None and cards.ref.ts == "1700000000.000050"
    assert store.card_delivered == {PUBLICATION_ID}


async def test_publication_card_delivery_cap_fails_safely_and_reports_result(
    publication: Any,
) -> None:
    cards = _Cards()
    loop, store, _, _, _, replies = _loop(publication, cards)
    store.card_pending = _card_work()
    store.card_retry_terminal_after = 2
    replies.fail_once = True

    with pytest.raises(RuntimeError, match="reply transport unavailable"):
        await loop.deliver_pending_card()
    replies.fail_once = True
    with pytest.raises(RuntimeError, match="reply transport unavailable"):
        await loop.deliver_pending_card()

    assert store.completed == {PUBLICATION_ID: ("failed", None)}
    assert store.card_pending is None
    assert store.card_delivered == set()
    assert await loop.deliver_pending_cleanup() is True
    assert await loop.deliver_pending_result(PUBLICATION_ID) is True
    assert "approval card could not be delivered" in replies.events[0][0].text


async def test_approved_publication_launches_job_and_reports_pr_url(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    work = _work(publication)

    await loop.reconcile(work)

    assert credentials.calls == [PUBLICATION_ID]
    assert len(cluster.applied) == 1
    resources = cluster.applied[0]
    assert resources.job["kind"] == "Job"
    assert PR_URL not in str(resources.secret), "the result is learned from the Job"
    assert github.number_calls == []
    assert github.verify_calls == []
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert cluster.credentials_cleaned == [resources.names]
    assert cluster.terminals_cleaned == [resources.names]
    assert len(replies.events) == 1
    event, route = replies.events[0]
    assert event.target == work.target
    assert PR_URL in event.text
    assert route == work.route
    assert not hasattr(loop, "runner") and not hasattr(loop, "model")


async def test_two_approved_revisions_keep_one_lineage_branch_and_pull_number(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, _ = _loop(publication)
    second_publication_id = uuid.UUID("66666666-6666-4666-8666-666666666666")
    second_revision_id = uuid.UUID("77777777-7777-4777-8777-777777777777")
    first = _lineage_work(publication)
    cluster.observation = publication.PublicationJobObservation(
        phase="succeeded",
        pr_url=PR_URL,
        pr_number=123,
        commit_sha=REVISION_HEAD,
        logs=(
            f"CURIE_PR_URL={PR_URL}\n"
            f"CURIE_PR_NUMBER=123\nCURIE_COMMIT_SHA={REVISION_HEAD}\n"
        ),
    )

    await loop.reconcile(first)

    github.head_sha = REVISION_HEAD
    second_head = "c" * 40
    cluster.observation = publication.PublicationJobObservation(
        phase="succeeded",
        pr_url=PR_URL,
        pr_number=123,
        commit_sha=second_head,
        logs=(
            f"CURIE_PR_URL={PR_URL}\n"
            f"CURIE_PR_NUMBER=123\nCURIE_COMMIT_SHA={second_head}\n"
        ),
    )
    second = _lineage_work(
        publication,
        publication_id=second_publication_id,
        revision_id=second_revision_id,
        revision_number=3,
        expected_prior_head=REVISION_HEAD,
    )

    await loop.reconcile(second)

    assert credentials.calls == [PUBLICATION_ID, second_publication_id]
    assert github.number_calls == [
        ("acme-corp/acme-bot", 123),
        ("acme-corp/acme-bot", 123),
    ]
    assert github.authorization_headers == [
        "Basic publication-write-credential-value",
        "Basic publication-write-credential-value",
    ]
    assert len(cluster.applied) == 2
    job_envs = [_job_env(resource) for resource in cluster.applied]
    assert {env["BRANCH"] for env in job_envs} == {LINEAGE_BRANCH}
    assert {env["PR_NUMBER"] for env in job_envs} == {"123"}
    assert [env["REVISION_ID"] for env in job_envs] == [
        str(REVISION_ID),
        str(second_revision_id),
    ]
    assert [advance["new_head"] for advance in store.lineage_advances] == [
        REVISION_HEAD,
        second_head,
    ]
    assert store.completed == {
        PUBLICATION_ID: ("published", PR_URL),
        second_publication_id: ("published", PR_URL),
    }


async def test_denied_first_revision_leaves_absent_identity_for_later_create_path(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, _ = _loop(publication)
    denied = _lineage_work(
        publication,
        decision="denied",
        revision_number=1,
        pr_number=None,
        pr_url=None,
    )

    await loop.reconcile(denied)

    assert credentials.calls == []
    assert cluster.applied == []
    assert github.number_calls == []

    later = _lineage_work(
        publication,
        publication_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
        revision_id=uuid.UUID("77777777-7777-4777-8777-777777777777"),
        revision_number=2,
        pr_number=None,
        pr_url=None,
    )
    cluster.observation = publication.PublicationJobObservation(
        phase="pending", pr_url=None, pr_number=None, commit_sha=None, logs=""
    )

    await loop.reconcile(later)

    assert credentials.calls == [later.publication_id]
    assert len(cluster.applied) == 1
    env = _job_env(cluster.applied[0])
    assert env["REVISION_NUMBER"] == "2"
    assert env["PR_NUMBER"] == ""
    assert env["EXPECTED_REMOTE_HEAD"] == ""


async def test_foreign_remote_head_is_never_adopted_as_the_approved_revision(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.head_sha = "d" * 40

    await loop.reconcile(_lineage_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.number_calls == [("acme-corp/acme-bot", 123)]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", "d" * 40, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.authorization_headers == [
        "Basic publication-write-credential-value",
        "Basic publication-write-credential-value",
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []
    assert store.retries == [
        (PUBLICATION_ID, "pull request head no longer matches the stored lineage head")
    ]


async def test_nonapproved_work_is_inert_in_the_job_reconciler(publication: Any) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    work = _work(publication, decision="denied")

    await loop.reconcile(work)

    assert credentials.calls == []
    assert cluster.applied == []
    assert cluster.credentials_cleaned == []
    assert cluster.terminals_cleaned == []
    assert github.number_calls == []
    assert store.completed == {}
    assert replies.events == []


async def test_worker_crash_reuses_the_same_job_and_cannot_duplicate_publication(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    work = _work(publication)
    cluster.raise_after_apply = True

    await loop.reconcile(work)
    await loop.reconcile(work)  # terminal duplicate callback is a no-op

    job_names = [resources.names.job for resources in cluster.applied]
    assert len(set(job_names)) == 1
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert len(replies.events) == 1
    assert github.number_calls == []


async def test_succeeded_job_retry_uses_validated_markers_without_a_second_credential(
    publication: Any,
) -> None:
    """A rotated installation token cannot make a completed Job unadoptable."""

    loop, store, credentials, cluster, _, replies = _loop(publication)
    work = _work(publication)
    cluster.observation = publication.PublicationJobObservation(
        phase="pending", pr_url=None, pr_number=None, commit_sha=None, logs=""
    )

    await loop.reconcile(work)

    cluster.observation = publication.PublicationJobObservation(
        phase="succeeded",
        pr_url=PR_URL,
        pr_number=123,
        commit_sha=REVISION_HEAD,
        logs=(
            f"CURIE_PR_URL={PR_URL}\n"
            f"CURIE_PR_NUMBER=123\nCURIE_COMMIT_SHA={REVISION_HEAD}\n"
        ),
    )
    await loop.reconcile(work)

    assert credentials.calls == [PUBLICATION_ID]
    assert len(cluster.applied) == 1
    assert len(cluster.validated_existing) == 1
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert len(replies.events) == 1


async def test_ttl_deleted_first_revision_job_recovers_exact_marked_commit_and_pr(
    publication: Any,
) -> None:
    """A distinct retry credential adopts GitHub truth after Job TTL deletion."""

    class TtlCluster(_Cluster):
        def __init__(self, module: Any) -> None:
            super().__init__(module)
            self.job_exists = False

        def apply(self, resources: Any) -> None:
            super().apply(resources)
            self.job_exists = True

        def observe(self, job_name: str) -> Any:
            if not self.job_exists:
                return self.module.PublicationJobObservation(
                    phase="pending", pr_url=None, logs="", exists=False
                )
            return self.observation

    class RotatingCredentials(_Credentials):
        def redeem(self, publication_id: uuid.UUID) -> Any:
            self.calls.append(publication_id)
            ordinal = len(self.calls)
            return self.module.PublicationCredential(
                clean_clone_url="https://github.com/acme-corp/acme-bot.git",
                authorization_header=f"Bearer rotated-installation-token-{ordinal}",
            )

    loop, store, _, _, github, replies = _loop(publication)
    cluster = TtlCluster(publication)
    credentials = RotatingCredentials(publication)
    loop._cluster = cluster
    loop._credentials = credentials
    work = _work(publication)
    cluster.observation = publication.PublicationJobObservation(
        phase="pending", pr_url=None, pr_number=None, commit_sha=None, logs=""
    )

    await loop.reconcile(work)
    assert len(cluster.applied) == 1

    cluster.job_exists = False
    github.branch_head = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    await loop.reconcile(work)

    assert credentials.calls == [PUBLICATION_ID, PUBLICATION_ID]
    assert len(cluster.applied) == 1, "recovery must not recreate the publication Job"
    assert github.branch_calls == [
        ("acme-corp/acme-bot", LINEAGE_BRANCH),
        ("acme-corp/acme-bot", LINEAGE_BRANCH),
    ]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.recover_calls == [
        ("acme-corp/acme-bot", LINEAGE_BRANCH, REVISION_HEAD)
    ]
    assert github.authorization_headers == [
        "Bearer rotated-installation-token-1",
        "Bearer rotated-installation-token-2",
        "Bearer rotated-installation-token-2",
        "Bearer rotated-installation-token-2",
    ]
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert store.lineage_advances[0]["pr_number"] == 123
    assert store.lineage_advances[0]["new_head"] == REVISION_HEAD
    assert len(replies.events) == 1


async def test_first_revision_recovery_refuses_pr_head_replaced_after_commit_proof(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.branch_head = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    github.recovered_head_sha = "c" * 40

    await loop.reconcile(_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.recover_calls == [
        ("acme-corp/acme-bot", LINEAGE_BRANCH, REVISION_HEAD)
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert store.retries == [
        (
            PUBLICATION_ID,
            "recoverable pull request head does not match the expected commit",
        )
    ]
    assert replies.events == []


@pytest.mark.parametrize("terminal_state", ["closed", "merged"])
async def test_first_revision_recovery_persists_terminal_pull_without_repost(
    publication: Any,
    terminal_state: str,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.branch_head = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    github.recovered_pr_state = terminal_state

    await loop.reconcile(_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.recover_calls == [
        ("acme-corp/acme-bot", LINEAGE_BRANCH, REVISION_HEAD)
    ]
    assert store.lineage_terminals == [
        {
            "lineage_id": LINEAGE_ID,
            "expected_version": 1,
            "expected_stored_head": None,
            "state": terminal_state,
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": REVISION_HEAD,
        }
    ]
    assert store.retries == [
        (
            PUBLICATION_ID,
            f"pull request lineage is {terminal_state}; start a new thread",
        )
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []


@pytest.mark.parametrize("terminal_state", ["closed", "merged"])
async def test_stored_terminal_pull_never_adopts_a_foreign_replacement_head(
    publication: Any,
    terminal_state: str,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    foreign_head = "c" * 40
    github.state = terminal_state
    github.head_sha = foreign_head

    await loop.reconcile(_lineage_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", foreign_head, REVISION_ID, PRIOR_HEAD)
    ]
    assert store.lineage_terminals == [
        {
            "lineage_id": LINEAGE_ID,
            "expected_version": 2,
            "expected_stored_head": PRIOR_HEAD,
            "state": terminal_state,
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": PRIOR_HEAD,
        }
    ]
    assert store.retries == [
        (
            PUBLICATION_ID,
            f"pull request lineage is {terminal_state}; start a new thread",
        )
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []


async def test_stored_terminal_pull_accepts_an_exact_verified_revision_head(
    publication: Any,
) -> None:
    loop, store, _, cluster, github, replies = _loop(publication)
    github.state = "merged"
    github.head_sha = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)

    await loop.reconcile(_lineage_work(publication))

    assert github.verify_calls == [
        ("acme-corp/acme-bot", REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    ]
    assert store.lineage_terminals == [
        {
            "lineage_id": LINEAGE_ID,
            "expected_version": 2,
            "expected_stored_head": PRIOR_HEAD,
            "state": "merged",
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": REVISION_HEAD,
        }
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []


@pytest.mark.parametrize("terminal_state", ["closed", "merged"])
async def test_terminal_first_pr_job_marker_persists_lineage_without_recovery(
    publication: Any,
    terminal_state: str,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    cluster.preexisting_observation = publication.PublicationJobObservation(
        phase="failed",
        pr_url=PR_URL,
        pr_number=123,
        commit_sha=REVISION_HEAD,
        pr_state=terminal_state,
        logs=(
            f"CURIE_PR_URL={PR_URL}\n"
            "CURIE_PR_NUMBER=123\n"
            f"CURIE_COMMIT_SHA={REVISION_HEAD}\n"
            f"CURIE_PR_STATE={terminal_state}\n"
        ),
        error=f"stored pull request is {terminal_state}",
    )

    await loop.reconcile(_work(publication))

    assert len(cluster.validated_existing) == 1
    assert credentials.calls == []
    assert github.branch_calls == []
    assert github.recover_calls == []
    assert store.lineage_terminals == [
        {
            "lineage_id": LINEAGE_ID,
            "expected_version": 1,
            "expected_stored_head": None,
            "state": terminal_state,
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": REVISION_HEAD,
        }
    ]
    assert store.retries == [
        (
            PUBLICATION_ID,
            f"pull request lineage is {terminal_state}; start a new thread",
        )
    ]
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []


async def test_terminal_job_state_without_exact_facts_cannot_close_lineage(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    cluster.preexisting_observation = publication.PublicationJobObservation(
        phase="failed",
        pr_url=None,
        pr_number=None,
        commit_sha=None,
        pr_state="closed",
        logs="CURIE_PR_STATE=closed\n",
        error="stored pull request is closed",
    )

    await loop.reconcile(_work(publication))

    assert store.lineage_terminals == []
    assert store.retries == [
        (
            PUBLICATION_ID,
            "publication Job terminal state omitted exact pull request facts",
        )
    ]
    assert credentials.calls == []
    assert github.recover_calls == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert replies.events == []


async def test_terminal_job_reconcile_replay_is_idempotent_in_real_store(
    publication: Any,
) -> None:
    engine: AsyncEngine = create_async_engine(_DB_URL)
    schema: str | None = None
    try:
        try:
            async with engine.connect():
                pass
        except SQLAlchemyError as exc:
            pytest.skip(f"Postgres not reachable at {_DB_URL}: {exc}")

        schema = f"test_publication_{uuid.uuid4().hex}"
        durable = PostgresPublicationStore(
            engine,
            schema=schema,
            lease_owner="terminal-replay-test",
        )
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema}".thread_publication_lineages ('
                    "id uuid PRIMARY KEY, pr_number integer, pr_url text, "
                    "head_sha text, status text NOT NULL, version integer NOT NULL, "
                    "updated_at timestamp NOT NULL DEFAULT now())"
                )
            )
            await connection.execute(
                text(
                    f'INSERT INTO "{schema}".thread_publication_lineages '
                    "(id, pr_number, pr_url, head_sha, status, version) "
                    "VALUES (:id, 123, :pr_url, :head_sha, 'open', 2)"
                ),
                {"id": LINEAGE_ID, "pr_url": PR_URL, "head_sha": PRIOR_HEAD},
            )

        class RealTerminalStore(_Store):
            async def mark_lineage_terminal(
                self,
                lineage_id: uuid.UUID,
                *,
                expected_version: int,
                expected_stored_head: str | None,
                state: str,
                pr_number: int,
                pr_url: str,
                head_sha: str,
            ) -> None:
                await durable.mark_lineage_terminal(
                    lineage_id,
                    expected_version=expected_version,
                    expected_stored_head=expected_stored_head,
                    state=state,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    head_sha=head_sha,
                )

        loop, _, credentials, cluster, github, replies = _loop(publication)
        store = RealTerminalStore()
        loop._store = store
        cluster.preexisting_observation = publication.PublicationJobObservation(
            phase="failed",
            pr_url=PR_URL,
            pr_number=123,
            commit_sha=REVISION_HEAD,
            pr_state="merged",
            logs=(
                f"CURIE_PR_URL={PR_URL}\n"
                "CURIE_PR_NUMBER=123\n"
                f"CURIE_COMMIT_SHA={REVISION_HEAD}\n"
                "CURIE_PR_STATE=merged\n"
            ),
            error="stored pull request is merged",
        )
        work = _lineage_work(publication)

        await loop.reconcile(work)
        await loop.reconcile(work)

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        f'SELECT status, pr_number, pr_url, head_sha, version '
                        f'FROM "{schema}".thread_publication_lineages WHERE id = :id'
                    ),
                    {"id": LINEAGE_ID},
                )
            ).mappings().one()
        assert dict(row) == {
            "status": "merged",
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": REVISION_HEAD,
            "version": 3,
        }
        assert len(store.retries) == 2
        assert credentials.calls == []
        assert github.recover_calls == []
        assert store.completed == {}
        assert store.lineage_advances == []
        assert replies.events == []

        for conflict_state, conflict_head in (
            ("closed", REVISION_HEAD),
            ("merged", "c" * 40),
        ):
            with pytest.raises(PublicationStoreError, match="terminal CAS was lost"):
                await durable.mark_lineage_terminal(
                    LINEAGE_ID,
                    expected_version=2,
                    expected_stored_head=PRIOR_HEAD,
                    state=conflict_state,
                    pr_number=123,
                    pr_url=PR_URL,
                    head_sha=conflict_head,
                )

        concurrent_id = uuid.uuid4()
        foreign_head = "d" * 40
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f'INSERT INTO "{schema}".thread_publication_lineages '
                    "(id, pr_number, pr_url, head_sha, status, version) "
                    "VALUES (:id, 123, :pr_url, :head_sha, 'open', 2)"
                ),
                {"id": concurrent_id, "pr_url": PR_URL, "head_sha": foreign_head},
            )
        with pytest.raises(PublicationStoreError, match="terminal CAS was lost"):
            await durable.mark_lineage_terminal(
                concurrent_id,
                expected_version=2,
                expected_stored_head=PRIOR_HEAD,
                state="closed",
                pr_number=123,
                pr_url=PR_URL,
                head_sha=PRIOR_HEAD,
            )
        async with engine.connect() as connection:
            concurrent = (
                await connection.execute(
                    text(
                        f'SELECT status, head_sha, version FROM "{schema}".'
                        "thread_publication_lineages WHERE id = :id"
                    ),
                    {"id": concurrent_id},
                )
            ).mappings().one()
        assert dict(concurrent) == {
            "status": "open",
            "head_sha": foreign_head,
            "version": 2,
        }
    finally:
        if schema is not None:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def test_missing_job_never_overwrites_an_unmarked_lineage_branch_head(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.branch_head = "d" * 40

    await loop.reconcile(_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", "d" * 40, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.recover_calls == []
    assert cluster.applied == []
    assert store.completed == {}
    assert store.lineage_advances == []
    assert store.retries == [
        (
            PUBLICATION_ID,
            "remote head is not this revision's marked commit with expected parent",
        )
    ]
    assert replies.events == []


async def test_exact_marked_remote_revision_is_adopted_before_recreating_a_missing_job(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.head_sha = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    work = _lineage_work(publication)

    await loop.reconcile(work)

    assert credentials.calls == [PUBLICATION_ID]
    assert github.number_calls == [("acme-corp/acme-bot", 123)]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.authorization_headers == [
        "Basic publication-write-credential-value",
        "Basic publication-write-credential-value",
    ]
    assert cluster.applied == [], "remote adoption must happen before a replacement Job"
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert store.lineage_advances[0]["new_head"] == REVISION_HEAD
    assert PR_URL in replies.events[0][0].text


async def test_non_slack_publication_result_uses_the_stored_adapter_route_without_model(
    publication: Any,
) -> None:
    transcript = _Transcript()
    loop, store, _, _, _, replies = _loop(publication, transcript=transcript)
    work = _work(publication, kind="email")
    store.target = work.target
    store.route = work.route
    email_workspace_conversation_id = scoped_conversation_id(
        work.target.kind,
        work.target.address,
        work.target.conversation_id,
    )
    store.workspace_conversation_id = email_workspace_conversation_id

    await loop.reconcile(work)

    event, route = replies.events[0]
    assert event.target.kind == "email"
    assert event.target.address == "agent@example.test"
    assert event.target.conversation_id == CONVERSATION_ID
    assert route == TargetRoute(
        endpoint="https://adapter.example.com/replies", adapter="agentmail-sandbox"
    )
    assert transcript.records == [
        (
            AGENT_ID,
            email_workspace_conversation_id,
            PUBLICATION_ID,
            f"Published the approved changes: {PR_URL}",
        )
    ]
    assert all(record[1] != WORKSPACE_CONVERSATION_ID for record in transcript.records)
    assert PR_URL in event.text
    assert store.completed[PUBLICATION_ID] == ("published", PR_URL)
    assert not hasattr(loop, "runner") and not hasattr(loop, "model")


@pytest.mark.parametrize(
    ("outcome", "pr_url", "error", "decision", "text_fragment"),
    [
        ("published", PR_URL, None, "approved", "Published the approved changes"),
        ("denied", None, None, "rejected", "request was denied"),
        ("failed", None, "push failed", "approved", "failed safely after approval"),
        ("expired", None, None, None, "approval expired"),
    ],
)
async def test_terminal_result_settles_card_with_durable_resolution_identity(
    publication: Any,
    outcome: str,
    pr_url: str | None,
    error: str | None,
    decision: str | None,
    text_fragment: str,
) -> None:
    cards = _Cards()
    loop, store, _, _, _, replies = _loop(publication, cards)
    cards.ref = _card()
    store.pending[PUBLICATION_ID] = {
        "outcome": outcome,
        "pr_url": pr_url,
        "error": error,
        "resolved_by": RESOLVER if decision is not None else None,
        "resolution_note": RESOLUTION_NOTE if decision is not None else None,
    }

    await loop.deliver_pending_result(PUBLICATION_ID)

    assert text_fragment in replies.events[0][0].text
    card_update, card_route = replies.events[1]
    assert card_update.target.reply_ref == "1700000000.000050"
    assert card_update.message.text == "Publish these repository changes?"
    assert card_update.settled.decision == decision
    assert card_update.settled.requested_by == "requester@example.test"
    assert card_update.settled.resolver == (
        RESOLVER if decision is not None else None
    )
    assert card_update.settled.note == (
        RESOLUTION_NOTE if decision is not None else None
    )
    assert card_route == TargetRoute(endpoint=None, adapter=None)
    assert cards.ref is None
    assert cards.restored == []


async def test_result_does_not_consume_a_card_stored_under_another_approval(
    publication: Any,
) -> None:
    cards = _Cards()
    loop, store, _, _, _, replies = _loop(publication, cards)
    mismatched = _card()
    cards.ref = mismatched
    cards.key = "44444444-4444-4444-8444-444444444444"
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
    }
    await loop.deliver_pending_result(PUBLICATION_ID)

    assert cards.ref == mismatched
    assert cards.restored == []
    assert len(replies.events) == 1
    assert replies.events[0][0].settled is None
    assert PUBLICATION_ID in store.delivered


async def test_only_exact_approved_decision_can_redeem_write_credential(
    publication: Any,
) -> None:
    for decision in ("denied", "expired", "pending"):
        loop, _, credentials, cluster, github, _ = _loop(publication)
        await loop.reconcile(_work(publication, decision=decision))
        assert credentials.calls == [], decision
        assert cluster.applied == [], decision
        assert github.number_calls == [], decision


async def test_terminal_result_is_persisted_and_credentials_removed_before_reply_retry(
    publication: Any,
) -> None:
    cards = _Cards()
    loop, store, credentials, cluster, _, replies = _loop(publication, cards)
    replies.fail_once = True
    work = _work(publication)
    cards.ref = _card()

    with pytest.raises(RuntimeError, match="reply transport unavailable"):
        await loop.reconcile(work)

    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert PUBLICATION_ID in store.pending
    assert len(cluster.credentials_cleaned) == 1
    assert len(cluster.terminals_cleaned) == 1
    assert credentials.calls == [PUBLICATION_ID]
    assert cards.ref == _card()
    assert cards.restored == [(str(APPROVAL_ID), _card())]

    await loop.deliver_pending_result(PUBLICATION_ID)

    assert PUBLICATION_ID in store.delivered
    assert store.pending == {}
    assert len(cluster.credentials_cleaned) == 1
    assert len(cluster.terminals_cleaned) == 1
    assert credentials.calls == [PUBLICATION_ID]
    assert len(replies.events) == 2
    assert replies.events[1][0].settled.decision == "approved"
    assert replies.events[1][0].settled.resolver == RESOLVER
    assert replies.events[1][0].settled.note == RESOLUTION_NOTE
    assert cards.ref is None
    assert store.delivery_retries == [(PUBLICATION_ID, "reply transport unavailable")]


async def test_card_restore_failure_preserves_original_error_and_ref_for_retry(
    publication: Any,
) -> None:
    cards = _Cards()
    cards.ref = _card()
    cards.restore_failures_remaining = 1
    loop, store, _, _, _, replies = _loop(publication, cards)
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
        "resolved_by": RESOLVER,
        "resolution_note": RESOLUTION_NOTE,
    }
    replies.fail_once = True

    with pytest.raises(RuntimeError, match="reply transport unavailable"):
        await loop.deliver_pending_result(PUBLICATION_ID)

    assert cards.ref is None
    assert store.delivery_retries == [(PUBLICATION_ID, "reply transport unavailable")]

    assert await loop.deliver_pending_result(PUBLICATION_ID) is True

    assert PUBLICATION_ID in store.delivered
    assert replies.events[1][0].settled.decision == "approved"
    assert cards.ref is None


async def test_supervisor_drains_terminal_result_outbox_without_job_work(
    publication: Any,
) -> None:
    reconciler, store, _, cluster, _, replies = _loop(publication)
    store.completed[PUBLICATION_ID] = ("published", PR_URL)
    store.pending[PUBLICATION_ID] = {
        "outcome": "published",
        "pr_url": PR_URL,
        "error": None,
    }
    store.cleanup_pending.add(PUBLICATION_ID)
    shutdown = asyncio.Event()
    replies.on_emit = shutdown.set
    supervisor = publication.PublicationReconcileLoop(
        store=store,
        reconciler=reconciler,
        interval_seconds=0.01,
    )

    await supervisor.run_forever(shutdown)

    assert PUBLICATION_ID in store.delivered
    assert len(cluster.credentials_cleaned) == 1
    assert len(cluster.terminals_cleaned) == 1
    assert PR_URL in replies.events[0][0].text


@pytest.mark.parametrize("phase", ["failed", "succeeded"])
async def test_terminal_job_recovers_exact_marked_revision_after_lost_response(
    publication: Any,
    phase: str,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    github.head_sha = REVISION_HEAD
    github.allow_exact_revision(REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    cluster.preexisting_observation = publication.PublicationJobObservation(
        phase=phase,
        pr_url=None,
        pr_number=None,
        commit_sha=None,
        logs="publication process exited without a marker\n",
        error="job process exited" if phase == "failed" else None,
    )

    await loop.reconcile(_lineage_work(publication))

    assert credentials.calls == [PUBLICATION_ID]
    assert github.number_calls == [("acme-corp/acme-bot", 123)]
    assert github.verify_calls == [
        ("acme-corp/acme-bot", REVISION_HEAD, REVISION_ID, PRIOR_HEAD)
    ]
    assert github.authorization_headers == [
        "Basic publication-write-credential-value",
        "Basic publication-write-credential-value",
    ]
    assert cluster.applied == []
    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert PR_URL in replies.events[0][0].text


async def test_running_job_is_validated_without_redeeming_another_credential(
    publication: Any,
) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    cluster.preexisting_observation = publication.PublicationJobObservation(
        phase="running", pr_url=None, logs=""
    )

    await loop.reconcile(_work(publication))

    assert len(cluster.validated_existing) == 1
    assert credentials.calls == []
    assert github.number_calls == []
    assert cluster.applied == []
    assert store.completed == {}
    assert replies.events == []


async def test_blocking_cluster_client_does_not_stall_event_loop_ticker(
    publication: Any,
) -> None:
    loop, _, _, cluster, _, _ = _loop(publication)
    release = threading.Event()
    cluster.observe_release = release
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        await asyncio.sleep(0)
        ticks += 1
        release.set()

    await asyncio.gather(loop.reconcile(_work(publication)), ticker())

    assert ticks == 1
    assert cluster.observe_timed_out is False


async def test_credential_setup_failure_is_bounded_and_terminalized(publication: Any) -> None:
    loop, store, credentials, cluster, github, replies = _loop(publication)
    credentials.error = publication.PublicationReconcileError(
        "publication credential endpoint is unreachable"
    )
    work = _work(publication)
    store.retry_terminal_after = 2

    await loop.reconcile(work)
    assert store.retries == [
        (PUBLICATION_ID, "publication credential endpoint is unreachable")
    ]
    assert store.completed == {}
    assert replies.events == []

    await loop.reconcile(work)
    assert store.completed == {PUBLICATION_ID: ("failed", None)}
    assert store.failures == [
        (PUBLICATION_ID, "publication credential endpoint is unreachable")
    ]
    assert cluster.applied == []
    assert github.number_calls == []
    assert "failed safely" in replies.events[0][0].text.lower()


async def test_hostile_resource_collision_is_bounded_and_terminalized(
    publication: Any,
) -> None:
    k8s = importlib.import_module("curie_worker.publication_k8s")
    loop, store, _, cluster, _, replies = _loop(publication)
    cluster.apply_error = k8s.PublicationResourceError(
        "existing publication Job metadata contract does not match"
    )
    store.retry_terminal_after = 2
    work = _work(publication)

    await loop.reconcile(work)
    await loop.reconcile(work)

    assert len(cluster.applied) == 2
    assert store.completed == {PUBLICATION_ID: ("failed", None)}
    assert len(store.retries) == 2
    assert "failed safely" in replies.events[0][0].text.lower()


async def test_unvalidated_terminal_marker_cannot_bypass_resource_adoption(
    publication: Any,
) -> None:
    k8s = importlib.import_module("curie_worker.publication_k8s")
    loop, store, _, cluster, _, replies = _loop(publication)
    cluster.preexisting_observation = publication.PublicationJobObservation(
        phase="succeeded",
        pr_url="https://github.com/other-corp/other-repo/pull/9",
        logs="CURIE_PR_URL=https://github.com/other-corp/other-repo/pull/9\n",
    )
    cluster.apply_error = k8s.PublicationResourceError(
        "existing publication Job metadata contract does not match"
    )

    await loop.reconcile(_work(publication))

    assert len(cluster.validated_existing) == 1
    assert cluster.applied == []
    assert store.completed == {}
    assert len(store.retries) == 1
    assert replies.events == []


async def test_foreign_repository_pr_url_is_bounded_instead_of_reported(
    publication: Any,
) -> None:
    loop, store, _, cluster, _, replies = _loop(publication)
    cluster.observation = publication.PublicationJobObservation(
        phase="succeeded",
        pr_url="https://github.com/other-corp/other-repo/pull/9",
        logs="CURIE_PR_URL=https://github.com/other-corp/other-repo/pull/9\n",
    )

    await loop.reconcile(_work(publication))

    assert store.completed == {}
    assert store.retries == [
        (
            PUBLICATION_ID,
            "publication result URL does not belong to the requested repository",
        )
    ]
    assert replies.events == []


async def test_result_url_accepts_github_canonical_repository_casing(
    publication: Any,
) -> None:
    loop, store, _, cluster, _, replies = _loop(publication)
    work = _work(publication)
    work = publication.PublicationWork(
        **{**work.__dict__, "repo_full_name": "Acme-Corp/Acme-Bot"}
    )
    cluster.observation = publication.PublicationJobObservation(
        phase="succeeded", pr_url=PR_URL, logs=f"CURIE_PR_URL={PR_URL}\n"
    )

    await loop.reconcile(work)

    assert store.completed == {PUBLICATION_ID: ("published", PR_URL)}
    assert PR_URL in replies.events[0][0].text


async def test_repeated_apiserver_failure_after_apply_is_dead_lettered(
    publication: Any,
) -> None:
    loop, store, _, cluster, _, replies = _loop(publication)
    cluster.apply_error = RuntimeError("apiserver response was lost")
    cluster.observe_after_apply_error = RuntimeError("apiserver is unavailable")
    store.retry_terminal_after = 2
    work = _work(publication)

    await loop.reconcile(work)
    await loop.reconcile(work)

    assert store.completed == {PUBLICATION_ID: ("failed", None)}
    assert len(store.retries) == 2
    assert all("apiserver" in error for _, error in store.retries)
    assert "failed safely" in replies.events[0][0].text.lower()


async def test_cleanup_retries_beyond_result_cap_before_result_outbox_ack(
    publication: Any,
) -> None:
    loop, store, _, cluster, _, replies = _loop(publication)
    cluster.terminal_cleanup_failures_remaining = 6
    work = _work(publication)

    with pytest.raises(RuntimeError, match="resource cleanup unavailable"):
        await loop.reconcile(work)

    for _ in range(5):
        with pytest.raises(RuntimeError, match="resource cleanup unavailable"):
            await loop.deliver_pending_cleanup()

    assert PUBLICATION_ID not in store.delivered
    assert PUBLICATION_ID in store.pending
    assert replies.events == []
    assert len(store.cleanup_retries) == 6
    assert store.delivery_retries == []

    assert await loop.deliver_pending_cleanup() is True
    await loop.deliver_pending_result(PUBLICATION_ID)

    assert PUBLICATION_ID in store.delivered
    assert cluster.terminals_cleaned
    assert PR_URL in replies.events[0][0].text


async def test_claim_next_failure_names_the_cause_and_still_escapes(
    publication: Any, caplog: pytest.LogCaptureFixture
) -> None:
    reconciler, store, *_ = _loop(publication)

    async def boom() -> None:
        raise RuntimeError("publication claim CAS was lost")

    store.claim_next = boom
    supervisor = publication.PublicationReconcileLoop(
        store=store,
        reconciler=reconciler,
        interval_seconds=0.01,
    )
    shutdown = asyncio.Event()
    with caplog.at_level(logging.ERROR, logger="curie_worker.publication_loop"):
        with pytest.raises(RuntimeError, match="publication claim CAS was lost"):
            await supervisor.run_forever(shutdown)
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "publication claim_next failed" in message
        and "RuntimeError" in message
        and "publication claim CAS was lost" in message
        for message in messages
    )


async def test_idle_publication_loop_does_not_page(
    publication: Any, caplog: pytest.LogCaptureFixture
) -> None:
    reconciler, store, *_ = _loop(publication)
    shutdown = asyncio.Event()
    claims = {"n": 0}
    original = store.claim_next

    async def idle_claim() -> None:
        claims["n"] += 1
        return await original()

    store.claim_next = idle_claim

    async def stop_after_one_interval() -> None:
        while claims["n"] < 1:
            await asyncio.sleep(0)
        await asyncio.sleep(0.02)
        shutdown.set()

    supervisor = publication.PublicationReconcileLoop(
        store=store,
        reconciler=reconciler,
        interval_seconds=0.01,
    )
    with caplog.at_level(logging.ERROR):
        await asyncio.wait_for(
            asyncio.gather(
                supervisor.run_forever(shutdown),
                stop_after_one_interval(),
            ),
            timeout=2,
        )
    assert claims["n"] >= 1
    messages = [record.getMessage() for record in caplog.records]
    assert not any("claim_next failed" in message for message in messages)
    assert not any("crashed; restarting" in message for message in messages)
