"""Worker-owned reconciliation of approval-gated repository publications."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from channel_protocol import MESSAGE_VERSION, Action, ConfirmIntent, OutboundMessage
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
)

from .approval_cards import ApprovalCardRef, ApprovalCardStore
from .publication_k8s import (
    PublicationJobSettings,
    PublicationPayload,
    PublicationResourceError,
    PublicationResourceNames,
    build_publication_resources,
    deterministic_publication_branch,
    publication_resource_names,
)
from .reply_sink import ReplySink, TargetRoute

_PR_MARKER = re.compile(r"^CURIE_PR_URL=(https://github\.com/[^\s]+/pull/\d+)$", re.MULTILINE)
_PR_NUMBER_MARKER = re.compile(r"^CURIE_PR_NUMBER=([1-9][0-9]*)$", re.MULTILINE)
_COMMIT_MARKER = re.compile(r"^CURIE_COMMIT_SHA=([0-9a-f]{40,64})$", re.MULTILINE)
_PR_STATE_MARKER = re.compile(r"^CURIE_PR_STATE=(closed|merged)$", re.MULTILINE)
logger = logging.getLogger(__name__)


class PublicationReconcileError(RuntimeError):
    """A durable publication could not reach a safe terminal state."""


class PublicationTranscriptPermanentError(PublicationReconcileError):
    """A transcript result cannot be recorded by retrying the same payload."""


@dataclass(frozen=True)
class PublicationCredential:
    clean_clone_url: str
    authorization_header: str


@dataclass(frozen=True)
class PublicationPullState:
    number: int
    url: str
    state: Literal["open", "closed", "merged"]
    head_sha: str
    head_ref: str


@dataclass(frozen=True)
class PublicationJobObservation:
    phase: str
    pr_url: str | None
    logs: str
    pr_number: int | None = None
    commit_sha: str | None = None
    pr_state: Literal["closed", "merged"] | None = None
    error: str | None = None
    exists: bool = True


@dataclass(frozen=True)
class PublicationWork:
    publication_id: uuid.UUID
    approval_id: uuid.UUID
    decision: str
    lineage_id: uuid.UUID
    lineage_version: int
    revision_id: uuid.UUID
    revision_number: int
    repo_full_name: str
    branch: str
    pr_number: int | None
    pr_url: str | None
    expected_prior_head: str
    expected_remote_head: str | None
    base_sha: str
    patch: bytes
    changed_paths: tuple[str, ...]
    title: str
    body: str
    target: ReplyTarget
    route: TargetRoute
    version: int


class PublicationStore(Protocol):
    def claim_pending_card(self) -> Any: ...

    def mark_card_delivered(
        self, publication_id: uuid.UUID
    ) -> None | Awaitable[None]: ...

    def retry_card_delivery(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None | Awaitable[None]: ...

    def claim_pending_cleanup(self) -> Any: ...

    def mark_cleanup_completed(
        self, publication_id: uuid.UUID
    ) -> None | Awaitable[None]: ...

    def retry_cleanup(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None | Awaitable[None]: ...

    def is_terminal(self, publication_id: uuid.UUID) -> bool | Awaitable[bool]: ...

    def persist_result(
        self,
        publication_id: uuid.UUID,
        *,
        outcome: str,
        pr_url: str | None,
        error: str | None,
        **lineage: Any,
    ) -> None | Awaitable[None]: ...

    def pending_result(self, publication_id: uuid.UUID | None = None) -> Any: ...

    def mark_result_delivered(
        self, publication_id: uuid.UUID
    ) -> None | Awaitable[None]: ...

    def mark_outcome_history_ready(
        self, publication_id: uuid.UUID
    ) -> None | Awaitable[None]: ...

    def retry_result_delivery(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None | Awaitable[None]: ...

    def retry(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None | Awaitable[None]: ...

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
    ) -> None | Awaitable[None]: ...


class PublicationCredentialSource(Protocol):
    def redeem(
        self, publication_id: uuid.UUID
    ) -> PublicationCredential | Awaitable[PublicationCredential]: ...


class PublicationCluster(Protocol):
    def apply(self, resources: Any) -> None | Awaitable[None]: ...

    def validate_existing(self, resources: Any) -> None | Awaitable[None]: ...

    def observe(
        self, job_name: str
    ) -> PublicationJobObservation | Awaitable[PublicationJobObservation]: ...

    def cleanup_credentials(
        self, names: PublicationResourceNames
    ) -> None | Awaitable[None]: ...

    def cleanup_terminal(
        self, names: PublicationResourceNames
    ) -> None | Awaitable[None]: ...


class PublicationGitHub(Protocol):
    def read_pr_by_number(
        self,
        repo_full_name: str,
        pr_number: int,
        authorization_header: str,
    ) -> PublicationPullState | Awaitable[PublicationPullState]: ...

    def verify_revision_commit(
        self,
        repo_full_name: str,
        commit_sha: str,
        *,
        revision_id: uuid.UUID,
        expected_parent: str,
        authorization_header: str,
    ) -> str | Awaitable[str]: ...

    def read_branch_head(
        self,
        repo_full_name: str,
        branch: str,
        authorization_header: str,
    ) -> str | None | Awaitable[str | None]: ...

    def recover_pr_by_head(
        self,
        repo_full_name: str,
        branch: str,
        title: str,
        body: str,
        *,
        expected_head_sha: str,
        authorization_header: str,
    ) -> PublicationPullState | None | Awaitable[PublicationPullState | None]: ...


class PublicationTranscript(Protocol):
    def record_result(
        self,
        agent_id: uuid.UUID,
        workspace_conversation_id: str,
        publication_id: uuid.UUID,
        text: str,
    ) -> None | Awaitable[None]: ...


async def _resolve[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await cast(Awaitable[T], value)
    return value


async def _cluster_call[T](
    operation: Callable[..., T | Awaitable[T]],
    *args: Any,
) -> T:
    """Run the synchronous Kubernetes seam without blocking the worker loop."""

    result = await asyncio.to_thread(operation, *args)
    return await _resolve(result)


def _marker_url(logs: str) -> str | None:
    match = _PR_MARKER.search(logs)
    return match.group(1) if match else None


def _marker_number(logs: str) -> int | None:
    match = _PR_NUMBER_MARKER.search(logs)
    return int(match.group(1)) if match else None


def _marker_commit(logs: str) -> str | None:
    match = _COMMIT_MARKER.search(logs)
    return match.group(1) if match else None


def _marker_state(logs: str) -> Literal["closed", "merged"] | None:
    match = _PR_STATE_MARKER.search(logs)
    return cast(Literal["closed", "merged"], match.group(1)) if match else None


def _validated_pr_url(work: PublicationWork, url: str | None) -> str | None:
    """Accept only a pull request URL for the publication's exact repository."""

    if url is None:
        return None
    expected = re.compile(
        rf"https://github\.com/{re.escape(work.repo_full_name)}/pull/[1-9][0-9]*",
        re.IGNORECASE,
    )
    if expected.fullmatch(url) is None:
        raise PublicationReconcileError(
            "publication result URL does not belong to the requested repository"
        )
    return url


class PublicationReconciler:
    """Converge one durable decision onto one deterministic Job and result."""

    def __init__(
        self,
        *,
        store: PublicationStore,
        credentials: PublicationCredentialSource,
        cluster: PublicationCluster,
        github: PublicationGitHub,
        replies: ReplySink,
        job_settings: PublicationJobSettings,
        card_store: ApprovalCardStore | None = None,
        transcript: PublicationTranscript | None = None,
    ) -> None:
        self._store = store
        self._credentials = credentials
        self._cluster = cluster
        self._github = github
        self._replies = replies
        self._job_settings = job_settings
        self._card_store = card_store
        self._transcript = transcript
        self._retained_card_refs: dict[str, ApprovalCardRef] = {}
        if transcript is None:
            logger.error(
                "publication transcript recording is not configured; "
                "terminal results will be reported without durable turn history"
            )

    async def deliver_pending_card(self) -> bool:
        """Deliver one persisted publication approval card independently."""

        work = await _resolve(self._store.claim_pending_card())
        if work is None:
            return False
        if self._card_store is None:
            error = "durable approval-card reference storage is unavailable"
            await _resolve(
                self._store.retry_card_delivery(work.publication_id, error=error)
            )
            raise PublicationReconcileError(error)
        try:
            message = OutboundMessage(
                version=MESSAGE_VERSION,
                text=work.summary,
                interaction=ConfirmIntent(
                    kind="confirm",
                    id=str(work.approval_id),
                    prompt=work.summary,
                    confirm=Action(label="Approve", value=str(work.approval_id)),
                    cancel=Action(label="Reject", value=str(work.approval_id)),
                    allow_free_text=True,
                ),
            )
            ack = await self._replies.emit(
                ReplyPost(
                    version=REPLY_WIRE_VERSION,
                    event="reply.post",
                    target=work.target,
                    message=message,
                    requested_by=work.requested_by,
                ),
                route=work.route,
                best_effort_unreachable=False,
            )
            if not ack.ref:
                raise PublicationReconcileError(
                    "publication approval card post returned no durable reply ref"
                )
            conversation_id = work.target.conversation_id
            if conversation_id is None:
                raise PublicationReconcileError(
                    "publication approval card has no session conversation"
                )
            await self._card_store.remember(
                str(work.approval_id),
                channel=work.target.address,
                ts=ack.ref,
                summary=work.summary,
                endpoint=work.route.endpoint,
                requested_by=work.requested_by,
                kind=work.target.kind,
                adapter=work.route.adapter,
            )
        except Exception as exc:
            error = str(exc)[:2000] or type(exc).__name__
            await _resolve(
                self._store.retry_card_delivery(work.publication_id, error=error)
            )
            raise
        await _resolve(self._store.mark_card_delivered(work.publication_id))
        return True

    async def deliver_pending_cleanup(self) -> bool:
        """Remove one terminal publication's resources on an unbounded outbox."""

        work = await _resolve(self._store.claim_pending_cleanup())
        if work is None:
            return False
        names = publication_resource_names(work.publication_id)
        try:
            await self._cleanup_credentials(names)
            await self._cleanup_terminal(names)
        except Exception as exc:
            await _resolve(
                self._store.retry_cleanup(
                    work.publication_id,
                    error=(str(exc)[:2000] or type(exc).__name__),
                )
            )
            raise
        await _resolve(self._store.mark_cleanup_completed(work.publication_id))
        return True

    async def _report(self, target: ReplyTarget, route: TargetRoute, text: str) -> None:
        await self._replies.emit(
            ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=target,
                text=text,
            ),
            route=route,
            best_effort_unreachable=False,
        )

    async def _persist_result(
        self,
        work: PublicationWork,
        *,
        outcome: str,
        pr_url: str | None = None,
        error: str | None = None,
        pr_number: int | None = None,
        new_head: str | None = None,
    ) -> None:
        await _resolve(
            self._store.persist_result(
                work.publication_id,
                outcome=outcome,
                pr_url=pr_url,
                error=error,
                lineage_id=work.lineage_id,
                lineage_version=work.lineage_version,
                revision_id=work.revision_id,
                expected_prior_head=work.expected_prior_head,
                pr_number=pr_number,
                new_head=new_head,
            )
        )

    async def _cleanup_credentials(self, names: PublicationResourceNames) -> None:
        await _cluster_call(self._cluster.cleanup_credentials, names)

    async def _cleanup_terminal(self, names: PublicationResourceNames) -> None:
        await _cluster_call(self._cluster.cleanup_terminal, names)

    async def _settle_card(self, result: Any, ref: ApprovalCardRef) -> None:
        decision: Literal["approved", "rejected"] | None
        if result.outcome in {"published", "failed"}:
            decision = "approved"
        elif result.outcome == "denied":
            decision = "rejected"
        else:
            decision = None
        resolver = result.resolved_by if decision is not None else None
        note = result.resolution_note if decision is not None else None
        if decision is not None and (
            not isinstance(resolver, str) or not resolver.strip()
        ):
            raise PublicationReconcileError(
                "resolved publication approval has no durable resolver identity"
            )
        await self._replies.emit(
            ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=ReplyTarget(
                    kind=ref.kind or result.target.kind,
                    address=ref.channel,
                    conversation_id=result.target.conversation_id,
                    reply_ref=ref.ts,
                ),
                message=OutboundMessage(version=MESSAGE_VERSION, text=ref.summary),
                settled=SettledOutcome(
                    requested_by=ref.requested_by,
                    decision=decision,
                    resolver=resolver,
                    note=note,
                ),
            ),
            route=TargetRoute(
                endpoint=ref.endpoint,
                adapter=ref.adapter if ref.kind else result.route.adapter,
            ),
            best_effort_unreachable=False,
        )

    async def deliver_pending_result(
        self,
        publication_id: uuid.UUID | None = None,
    ) -> bool:
        result = await _resolve(self._store.pending_result(publication_id))
        if result is None:
            return False
        if result.outcome == "published":
            text = f"Published the approved changes: {result.pr_url}"
        elif result.outcome == "denied":
            text = (
                "Changes were not published: the publication request was denied, so no "
                "credential was redeemed and no branch was pushed."
            )
        elif result.outcome == "expired":
            text = (
                "Changes were not published: the publication approval expired before "
                "a decision was recorded."
            )
        else:
            text = f"Publication failed safely after approval: {result.error}"
        card_ref: ApprovalCardRef | None = None
        approval_id = str(result.approval_id)
        transcript_retry_error: Exception | None = None
        try:
            if self._card_store is not None:
                card_ref = self._retained_card_refs.pop(approval_id, None)
                if card_ref is None:
                    card_ref = await self._card_store.pop(approval_id)
            if self._transcript is not None:
                try:
                    await _resolve(
                        self._transcript.record_result(
                            result.agent_id,
                            result.workspace_conversation_id,
                            result.publication_id,
                            text,
                        )
                    )
                except PublicationTranscriptPermanentError:
                    # A full transcript may reject the detailed result (most
                    # commonly a long failure string or URL). Preserve the
                    # semantic outcome with the smallest useful marker before
                    # releasing the next-turn fence. The same publication id
                    # keeps this retry idempotent.
                    compact_text = (
                        f"Publication outcome: {result.outcome}. Details omitted "
                        "because thread history is at capacity."
                    )
                    try:
                        await _resolve(
                            self._transcript.record_result(
                                result.agent_id,
                                result.workspace_conversation_id,
                                result.publication_id,
                                compact_text,
                            )
                        )
                    except Exception as exc:
                        transcript_retry_error = exc
                        logger.warning(
                            "publication compact transcript outcome failed "
                            "publication_id=%s; retaining the durable fence",
                            result.publication_id,
                            exc_info=True,
                        )
                except Exception as exc:
                    transcript_retry_error = exc
                    logger.warning(
                        "publication transcript recording failed transiently "
                        "publication_id=%s; delivering the routed result before retry",
                        result.publication_id,
                        exc_info=True,
                    )
                if transcript_retry_error is None:
                    await _resolve(
                        self._store.mark_outcome_history_ready(result.publication_id)
                    )
            else:
                transcript_retry_error = PublicationReconcileError(
                    "publication transcript recording is not configured"
                )
            await self._report(result.target, result.route, text)
            if card_ref is not None:
                await self._settle_card(result, card_ref)
        except Exception as exc:
            if card_ref is not None and self._card_store is not None:
                try:
                    await self._card_store.restore(
                        approval_id,
                        card_ref,
                    )
                except Exception:
                    # Keep the only surviving copy available to this process.
                    # The routed-delivery error remains the failure charged to
                    # the outbox instead of being masked by a Valkey outage.
                    self._retained_card_refs[approval_id] = card_ref
                    logger.exception(
                        "publication approval card restore failed after result "
                        "delivery error publication_id=%s original_error=%s",
                        result.publication_id,
                        str(exc)[:2000] or type(exc).__name__,
                    )
            try:
                await _resolve(
                    self._store.retry_result_delivery(
                        result.publication_id,
                        error=(str(exc)[:2000] or type(exc).__name__),
                    )
                )
            except Exception as retry_exc:
                raise exc from retry_exc
            raise
        if transcript_retry_error is not None:
            transcript_error = (
                str(transcript_retry_error)[:1950]
                or type(transcript_retry_error).__name__
            )
            await _resolve(
                self._store.retry_result_delivery(
                    result.publication_id,
                    error=(
                        "publication transcript recording failed: "
                        f"{transcript_error}"
                    ),
                )
            )
            return True
        await _resolve(self._store.mark_result_delivered(result.publication_id))
        return True

    async def _terminalize(
        self,
        work: PublicationWork,
        *,
        outcome: str,
        pr_url: str | None = None,
        error: str | None = None,
        pr_number: int | None = None,
        new_head: str | None = None,
        names: PublicationResourceNames,
    ) -> None:
        # The durable outcome is the source of truth. Resource cleanup and reply
        # delivery are independent outboxes; result claims remain gated until
        # cleanup has durably completed.
        await self._persist_result(
            work,
            outcome=outcome,
            pr_url=pr_url,
            error=error,
            pr_number=pr_number,
            new_head=new_head,
        )
        await self.deliver_pending_cleanup()
        await self.deliver_pending_result(work.publication_id)

    async def _mark_lineage_terminal(
        self,
        work: PublicationWork,
        state: Literal["closed", "merged"],
        *,
        pr_number: int,
        pr_url: str,
        head_sha: str,
    ) -> None:
        await _resolve(
            self._store.mark_lineage_terminal(
                work.lineage_id,
                expected_version=work.lineage_version,
                expected_stored_head=work.expected_remote_head,
                state=state,
                pr_number=pr_number,
                pr_url=pr_url,
                head_sha=head_sha,
            )
        )

    async def _bounded_setup_failure(
        self,
        work: PublicationWork,
        exc: Exception,
    ) -> None:
        error = str(exc)[:2000] or type(exc).__name__
        await _resolve(self._store.retry(work.publication_id, error=error))
        # retry() terminalizes at its durable cap. If it did, drain the newly
        # available cleanup and result outboxes; otherwise these are no-ops.
        await self.deliver_pending_cleanup()
        await self.deliver_pending_result(work.publication_id)

    @staticmethod
    def _payload(work: PublicationWork, *, clean_clone_url: str) -> PublicationPayload:
        return PublicationPayload(
            publication_id=work.publication_id,
            revision_id=work.revision_id,
            revision_number=work.revision_number,
            repo_full_name=work.repo_full_name,
            clean_clone_url=clean_clone_url,
            base_sha=work.base_sha,
            expected_prior_head=work.expected_prior_head,
            expected_remote_head=work.expected_remote_head,
            patch=work.patch,
            branch=work.branch,
            pr_number=work.pr_number,
            pr_url=work.pr_url,
            title=work.title,
            body=work.body,
        )

    async def _read_stored_pull(
        self,
        work: PublicationWork,
        authorization_header: str,
    ) -> PublicationPullState | None:
        if work.pr_number is None:
            return None
        pull = await _resolve(
            self._github.read_pr_by_number(
                work.repo_full_name,
                work.pr_number,
                authorization_header,
            )
        )
        if pull.head_ref != work.branch:
            raise PublicationReconcileError(
                "pull request head branch no longer matches the stored lineage branch"
            )
        validated_url = _validated_pr_url(work, pull.url)
        if (
            validated_url is None
            or work.pr_url is None
            or validated_url.casefold() != work.pr_url.casefold()
        ):
            raise PublicationReconcileError(
                "pull request URL no longer matches the stored lineage identity"
            )
        return pull

    async def _finish_observation(
        self,
        work: PublicationWork,
        observation: PublicationJobObservation,
        names: PublicationResourceNames,
    ) -> bool:
        if observation.phase in {"pending", "running"}:
            return False
        pr_url = _validated_pr_url(
            work, observation.pr_url or _marker_url(observation.logs)
        )
        pr_number = observation.pr_number or _marker_number(observation.logs)
        commit_sha = observation.commit_sha or _marker_commit(observation.logs)
        pr_state = observation.pr_state or _marker_state(observation.logs)
        if pr_state is not None:
            if pr_url is None or pr_number is None or commit_sha is None:
                raise PublicationReconcileError(
                    "publication Job terminal state omitted exact pull request facts"
                )
            if pr_number != int(pr_url.rsplit("/", 1)[1]):
                raise PublicationReconcileError(
                    "publication Job terminal pull request facts are inconsistent"
                )
            if work.pr_number is not None and pr_number != work.pr_number:
                raise PublicationReconcileError(
                    "publication Job returned a different stored pull request"
                )
            if (
                work.pr_url is not None
                and pr_url.casefold() != work.pr_url.casefold()
            ):
                raise PublicationReconcileError(
                    "publication Job returned a different stored pull request"
                )
            await self._mark_lineage_terminal(
                work,
                pr_state,
                pr_number=pr_number,
                pr_url=pr_url,
                head_sha=commit_sha,
            )
            raise PublicationReconcileError(
                f"pull request lineage is {pr_state}; start a new thread"
            )
        if pr_url is not None and pr_number is not None and commit_sha is not None:
            if work.pr_number is not None and pr_number != work.pr_number:
                raise PublicationReconcileError(
                    "publication Job returned a different stored pull request"
                )
            await self._terminalize(
                work,
                outcome="published",
                pr_url=pr_url,
                pr_number=pr_number,
                new_head=commit_sha,
                names=names,
            )
            return True
        # Jobs created by the immediately preceding release emitted only the
        # URL marker. Preserve their terminal outbox behavior without claiming
        # a lineage head that they did not prove. New lineage Jobs always emit
        # all three markers and therefore take the CAS path above.
        if pr_url is not None and pr_number is None and commit_sha is None:
            await self._terminalize(
                work,
                outcome="published",
                pr_url=pr_url,
                names=names,
            )
            return True
        if observation.phase == "failed":
            raise PublicationReconcileError(
                observation.error or "publication Job failed without lineage markers"
            )
        raise PublicationReconcileError(
            "publication Job succeeded without complete lineage markers"
        )

    async def reconcile(self, work: PublicationWork) -> None:
        names = publication_resource_names(work.publication_id)
        if await _resolve(self._store.is_terminal(work.publication_id)):
            return

        # Pending, expired, and unknown states are never authority. Expiry is
        # terminalized by the API/store lane, as is denial; neither creates a
        # cluster or GitHub side effect here.
        if work.decision != "approved":
            return

        try:
            observation = await _cluster_call(self._cluster.observe, names.job)
        except Exception as exc:
            await self._bounded_setup_failure(work, exc)
            return

        if observation.exists:
            # Validate deterministic collisions before trusting either an
            # in-flight state or terminal Job markers. The placeholder is used
            # only to construct the expected immutable Secret shape;
            # validate_existing deliberately does not compare Secret bytes, so
            # a rotated installation token is never needed merely to adopt the
            # already-created Job.
            try:
                probe_resources = build_publication_resources(
                    self._payload(
                        work,
                        clean_clone_url=f"https://github.com/{work.repo_full_name}.git",
                    ),
                    credential="validation-placeholder",
                    settings=self._job_settings,
                )
                await _cluster_call(self._cluster.validate_existing, probe_resources)
            except Exception as exc:
                await self._bounded_setup_failure(work, exc)
                return
            if observation.phase in {"pending", "running"}:
                return
            marker_url = observation.pr_url or _marker_url(observation.logs)
            marker_number = observation.pr_number or _marker_number(observation.logs)
            marker_commit = observation.commit_sha or _marker_commit(observation.logs)
            marker_state = observation.pr_state or _marker_state(observation.logs)
            if marker_state is not None or (
                marker_url is not None
                and marker_number is not None
                and marker_commit is not None
            ) or (
                marker_url is not None
                and marker_number is None
                and marker_commit is None
            ):
                try:
                    await self._finish_observation(
                        work, observation, probe_resources.names
                    )
                except Exception as exc:
                    if await _resolve(self._store.is_terminal(work.publication_id)):
                        raise
                    await self._bounded_setup_failure(work, exc)
                return

        credential: PublicationCredential
        try:
            # The approved decision is the only authority to redeem. Redeem
            # exactly once, then use this authorization for every private
            # GitHub observation and for the one immutable Job Secret.
            credential = await _resolve(self._credentials.redeem(work.publication_id))
            pull = await self._read_stored_pull(
                work,
                credential.authorization_header,
            )
            if pull is None:
                branch_head = await _resolve(
                    self._github.read_branch_head(
                        work.repo_full_name,
                        work.branch,
                        credential.authorization_header,
                    )
                )
                if branch_head is not None:
                    await _resolve(
                        self._github.verify_revision_commit(
                            work.repo_full_name,
                            branch_head,
                            revision_id=work.revision_id,
                            expected_parent=work.expected_prior_head,
                            authorization_header=credential.authorization_header,
                        )
                    )
                    recovered = await _resolve(
                        self._github.recover_pr_by_head(
                            work.repo_full_name,
                            work.branch,
                            work.title,
                            work.body,
                            expected_head_sha=branch_head,
                            authorization_header=credential.authorization_header,
                        )
                    )
                    if recovered is None:
                        raise PublicationReconcileError(
                            "verified publication branch has no recoverable pull request"
                        )
                    validated_url = _validated_pr_url(work, recovered.url)
                    if (
                        validated_url is None
                        or recovered.number != int(validated_url.rsplit("/", 1)[1])
                        or recovered.head_ref != work.branch
                        or recovered.head_sha != branch_head
                    ):
                        raise PublicationReconcileError(
                            "recovered pull request does not match the verified publication"
                        )
                    if recovered.state == "closed" or recovered.state == "merged":
                        await self._mark_lineage_terminal(
                            work,
                            recovered.state,
                            pr_number=recovered.number,
                            pr_url=validated_url,
                            head_sha=recovered.head_sha,
                        )
                        raise PublicationReconcileError(
                            "pull request lineage is "
                            f"{recovered.state}; start a new thread"
                        )
                    if recovered.state != "open":
                        raise PublicationReconcileError(
                            "GitHub pull request state is invalid"
                        )
                    await self._terminalize(
                        work,
                        outcome="published",
                        pr_url=validated_url,
                        pr_number=recovered.number,
                        new_head=recovered.head_sha,
                        names=names,
                    )
                    return
            if pull is not None and pull.state != "open":
                trusted_head = work.expected_remote_head
                if pull.head_sha != trusted_head:
                    try:
                        verified_head = await _resolve(
                            self._github.verify_revision_commit(
                                work.repo_full_name,
                                pull.head_sha,
                                revision_id=work.revision_id,
                                expected_parent=work.expected_prior_head,
                                authorization_header=credential.authorization_header,
                            )
                        )
                        if verified_head != pull.head_sha:
                            raise PublicationReconcileError(
                                "revision verification returned a different commit"
                            )
                    except PublicationReconcileError:
                        if trusted_head is None:
                            raise PublicationReconcileError(
                                "terminal pull request head has no trusted lineage commit"
                            ) from None
                    else:
                        trusted_head = verified_head
                if trusted_head is None:
                    raise PublicationReconcileError(
                        "terminal pull request head has no trusted lineage commit"
                    )
                await self._mark_lineage_terminal(
                    work,
                    pull.state,
                    pr_number=pull.number,
                    pr_url=pull.url,
                    head_sha=trusted_head,
                )
                raise PublicationReconcileError(
                    f"pull request lineage is {pull.state}; start a new thread"
                )
            if pull is not None and pull.head_sha != work.expected_prior_head:
                try:
                    await _resolve(
                        self._github.verify_revision_commit(
                            work.repo_full_name,
                            pull.head_sha,
                            revision_id=work.revision_id,
                            expected_parent=work.expected_prior_head,
                            authorization_header=credential.authorization_header,
                        )
                    )
                except Exception as exc:
                    raise PublicationReconcileError(
                        "pull request head no longer matches the stored lineage head"
                    ) from exc
        except Exception as exc:
            await self._bounded_setup_failure(work, exc)
            return

        if pull is not None and pull.head_sha != work.expected_prior_head:
            # Verification above proved the remote head is this revision's
            # exact marked commit with the expected parent. Persisting the
            # lineage CAS may expose cleanup or reply-outbox failures; those
            # must escape as outbox work, never be charged as another attempt
            # at the already completed publication mutation.
            await self._terminalize(
                work,
                outcome="published",
                pr_url=pull.url,
                pr_number=pull.number,
                new_head=pull.head_sha,
                names=names,
            )
            return

        if observation.exists:
            # A validated terminal Job without usable markers may still be
            # recoverable from GitHub above (the immediately preceding release
            # emitted only a URL, and a crash can also lose pod logs). If GitHub
            # still names the expected prior head or no branch exists, preserve
            # the Job's terminal failure instead of attempting to mutate or
            # replace deterministic resources.
            try:
                await self._finish_observation(work, observation, names)
            except Exception as exc:
                await self._bounded_setup_failure(work, exc)
            return

        try:
            resources = build_publication_resources(
                self._payload(work, clean_clone_url=credential.clean_clone_url),
                credential=credential.authorization_header,
                settings=self._job_settings,
            )
        except Exception as exc:
            await self._bounded_setup_failure(work, exc)
            return

        # Server-side apply/create-or-adopt validates every deterministic
        # resource before any Job log marker is trusted. This includes a Job
        # observed before the apply: an attacker cannot plant a same-name Job
        # and make its marker authoritative without passing the full spec and
        # ownership contract.
        try:
            await _cluster_call(self._cluster.apply, resources)
        except PublicationResourceError as exc:
            await self._bounded_setup_failure(work, exc)
            return
        except Exception as apply_exc:
            # The apiserver may have accepted the resources and lost only the
            # response. Observe the deterministic name, then recover the
            # deterministic remote head, before charging a bounded retry.
            try:
                observation = await _cluster_call(
                    self._cluster.observe, resources.names.job
                )
                if observation.exists and observation.phase in {"pending", "running"}:
                    return
                if observation.exists and await self._finish_observation(
                    work, observation, resources.names
                ):
                    return
            except Exception as recovery_exc:
                if await _resolve(self._store.is_terminal(work.publication_id)):
                    raise
                await self._bounded_setup_failure(
                    work,
                    PublicationReconcileError(
                        f"ambiguous publication apply could not be recovered: {recovery_exc}"
                    ),
                )
                return
            await self._bounded_setup_failure(work, apply_exc)
            return

        try:
            observation = await _cluster_call(
                self._cluster.observe, resources.names.job
            )
            await self._finish_observation(work, observation, resources.names)
        except Exception as exc:
            if await _resolve(self._store.is_terminal(work.publication_id)):
                raise
            await self._bounded_setup_failure(work, exc)
            return


class PublicationReconcileLoop:
    """Poll durable work under the worker's ordinary task supervisor."""

    def __init__(
        self,
        *,
        store: Any,
        reconciler: PublicationReconciler,
        interval_seconds: float = 2.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("publication reconciliation interval must be positive")
        self._store = store
        self._reconciler = reconciler
        self._interval = interval_seconds

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await self._reconciler.deliver_pending_card()
            except Exception:
                logger.exception("publication approval card delivery failed")
            try:
                await self._reconciler.deliver_pending_cleanup()
            except Exception:
                # Cleanup is deliberately unbounded. Its released lease is
                # reclaimed until every deterministic resource is absent.
                logger.exception("publication resource cleanup failed")
            try:
                await self._reconciler.deliver_pending_result()
            except Exception:
                # The result lease was released (or dead-lettered) before the
                # error escaped. Publication mutation remains terminal and is
                # never repeated because a reply transport is unavailable.
                logger.exception("publication result delivery failed")
            try:
                work = await self._store.claim_next()
            except Exception as exc:
                logger.exception(
                    "publication claim_next failed cause=%s: %s",
                    type(exc).__name__,
                    exc,
                )
                raise
            if work is not None:
                try:
                    await self._reconciler.reconcile(work)
                except Exception:
                    # The lease is intentionally left in place. A worker crash
                    # or ambiguous apiserver response is retried only after it
                    # expires, adopting the deterministic resource names.
                    logger.exception(
                        "publication reconciliation failed publication_id=%s",
                        work.publication_id,
                    )
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval)
            except TimeoutError:
                pass


__all__ = [
    "PublicationCredential",
    "PublicationJobObservation",
    "PublicationPullState",
    "PublicationReconcileError",
    "PublicationReconciler",
    "PublicationReconcileLoop",
    "PublicationTranscriptPermanentError",
    "PublicationWork",
    "deterministic_publication_branch",
]
