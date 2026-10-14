"""Worker-owned reconciliation of approval-gated repository publications."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit

from channel_protocol import MESSAGE_VERSION, Action, ConfirmIntent, OutboundMessage
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
)

from .approval_cards import ApprovalCardRef, ApprovalCardStore
from .approvals import decided_field
from .publication_k8s import (
    HeaderForm,
    PublicationJobSettings,
    PublicationPayload,
    PublicationResourceError,
    PublicationResourceNames,
    PublicationTransport,
    build_publication_resources,
    deterministic_publication_branch,
    publication_branch_is_valid,
    publication_resource_names,
)
from .reply_sink import InvalidReplyTargetError, ReplySink, TargetRoute

# The Job's one marker: the commit it pushed, printed after the push.
_COMMIT_MARKER = re.compile(r"^CURIE_COMMIT_SHA=([0-9a-f]{40,64})$", re.MULTILINE)
# How many CONSECUTIVE unavailable identity reads one publication may escape
# reconcile() uncharged before it falls back to the ordinary bounded path.
# publication_authority.py maps 401, 403, 404, 429 and every 5xx onto
# AuthorityUnavailable, so a permanent condition (repository deleted, App
# uninstalled, App rate limited) is indistinguishable at the wire from a GitHub
# incident. Escaping uncharged forever would re-mint an installation token every
# lease and, on the lost-Job recovery path, redeem another write credential and
# append another credential_redemption_audit_entries row every lease, with no
# bound; before #2903 those conditions dead-lettered at reconcile_max_attempts.
# The uncharged retry is paced by publication_lease_seconds (60s default), so
# ten escapes give a real outage roughly ten minutes of free retries and still
# converge a permanent one onto a visible failed publication naming the reason.
_MAX_UNCHARGED_IDENTITY_ESCAPES = 10
logger = logging.getLogger(__name__)


class PublicationReconcileError(RuntimeError):
    """A durable publication could not reach a safe terminal state."""


class PublicationTranscriptPermanentError(PublicationReconcileError):
    """A transcript result cannot be recorded by retrying the same payload."""


class PublicationIdentityUnavailable(PublicationReconcileError):
    """The verified identity could not be read; the refusal is not stable."""


class PublicationRemoteTerminalError(PublicationReconcileError):
    """Provider truth says this pull request is already merged or closed."""

    state: Literal["merged", "closed"]

    def __init__(self, state: Literal["merged", "closed"]) -> None:
        super().__init__(f"pull request lineage is {state}; start a new thread")
        self.state = state


@dataclass(frozen=True)
class PublicationCredential:
    """A push credential and its transport facts, as the API issued them (ADR 0197)."""

    clean_clone_url: str
    authorization_header: str
    origin: str
    header_form: HeaderForm
    ca_bundle_ref: str | None = None

    @property
    def transport(self) -> PublicationTransport:
        return PublicationTransport(
            origin=self.origin,
            header_form=self.header_form,
            ca_bundle_ref=self.ca_bundle_ref,
        )


@dataclass(frozen=True)
class PublicationPullState:
    number: int
    url: str
    state: Literal["open", "closed", "merged"]
    head_sha: str
    head_ref: str
    updated_at: datetime | None = None


@dataclass(frozen=True)
class PublicationJobObservation:
    phase: str
    logs: str
    commit_sha: str | None = None
    error: str | None = None
    exists: bool = True
    # The transport facts the existing Job was built with; adoption rebuilds
    # the expected resources from them. None when no Job exists.
    transport: PublicationTransport | None = None


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
    lease_owner: str
    # False only when this publication already launched and its execution is
    # no longer running. New launches stay excluded by the claim query.
    owner_running: bool = True
    branch_prefix: str | None = None


class PublicationStore(Protocol):
    def claim_pending_card(self) -> Any: ...

    def mark_card_delivered(self, publication_id: uuid.UUID) -> None | Awaitable[None]: ...

    def retry_card_delivery(
        self, publication_id: uuid.UUID, *, error: str, permanent: bool
    ) -> None | Awaitable[None]: ...

    def claim_pending_cleanup(self) -> Any: ...

    def mark_cleanup_completed(self, publication_id: uuid.UUID) -> None | Awaitable[None]: ...

    def retry_cleanup(self, publication_id: uuid.UUID, *, error: str) -> None | Awaitable[None]: ...

    def is_terminal(self, publication_id: uuid.UUID) -> bool | Awaitable[bool]: ...

    def persist_result(
        self,
        publication_id: uuid.UUID,
        *,
        outcome: str,
        pr_url: str | None,
        error: str | None,
        metadata_updated_at: datetime | None,
    ) -> None | Awaitable[None]: ...

    def pending_result(self, publication_id: uuid.UUID | None = None) -> Any: ...

    def mark_result_delivered(self, publication_id: uuid.UUID) -> None | Awaitable[None]: ...

    def mark_outcome_history_ready(self, publication_id: uuid.UUID) -> None | Awaitable[None]: ...

    def retry_result_delivery(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None | Awaitable[None]: ...

    def retry(self, publication_id: uuid.UUID, *, error: str) -> None | Awaitable[None]: ...

    def release(self, publication_id: uuid.UUID) -> None | Awaitable[None]: ...

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


class PublicationLineageRefused(PublicationReconcileError):
    """The API refused the lineage advance for this exact publication outcome."""


class PublicationLineageAuthority(Protocol):
    def advance(
        self,
        publication_id: uuid.UUID,
        *,
        expected_version: int,
        expected_head_sha: str | None,
        expected_publication_version: int,
        lease_owner: str,
        pr_number: int,
        pr_url: str,
        head_sha: str,
        metadata_updated_at: datetime | None,
    ) -> None | Awaitable[None]: ...


class PublicationCluster(Protocol):
    def apply(self, resources: Any) -> None | Awaitable[None]: ...

    def validate_existing(self, resources: Any) -> None | Awaitable[None]: ...

    def observe(
        self, job_name: str
    ) -> PublicationJobObservation | Awaitable[PublicationJobObservation]: ...

    def cleanup_credentials(self, names: PublicationResourceNames) -> None | Awaitable[None]: ...

    def cleanup_terminal(self, names: PublicationResourceNames) -> None | Awaitable[None]: ...


class PublicationCodeHost(Protocol):
    """Pull request, branch and commit facts for one publication, from the API.

    ADR 0197 "Two ports" item 6: the worker holds no forge code; each call
    names the publication and the API derives everything else from it.
    """

    def read_pull_request(
        self, publication_id: uuid.UUID, pr_number: int
    ) -> PublicationPullState | Awaitable[PublicationPullState]: ...

    def verify_revision_commit(
        self,
        publication_id: uuid.UUID,
        commit_sha: str,
        *,
        revision_id: uuid.UUID,
        expected_parent: str,
    ) -> str | Awaitable[str]: ...

    def read_branch_head(self, publication_id: uuid.UUID) -> str | None | Awaitable[str | None]: ...

    def recover_pull_request(
        self, publication_id: uuid.UUID, *, expected_head_sha: str
    ) -> PublicationPullState | None | Awaitable[PublicationPullState | None]: ...

    def update_pull_request_metadata(
        self, publication_id: uuid.UUID
    ) -> PublicationPullState | Awaitable[PublicationPullState]: ...


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


def _marker_commit(logs: str) -> str | None:
    match = _COMMIT_MARKER.search(logs)
    return match.group(1) if match else None


def _checked_pr_url(url: str, number: int) -> str:
    """A pull request URL the API returned, checked for shape only.

    The API derived it through the code host and checks it again against the
    lineage when it advances; the worker knows no forge URL pattern.
    """

    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not url.endswith(f"/{number}")
    ):
        raise PublicationReconcileError("the pull request URL is not a clean HTTPS URL")
    return url


class PublicationReconciler:
    """Converge one durable decision onto one deterministic Job and result."""

    def __init__(
        self,
        *,
        store: PublicationStore,
        credentials: PublicationCredentialSource,
        cluster: PublicationCluster,
        code_host: PublicationCodeHost,
        replies: ReplySink,
        lineage: PublicationLineageAuthority,
        job_settings: PublicationJobSettings,
        card_store: ApprovalCardStore | None = None,
        transcript: PublicationTranscript | None = None,
    ) -> None:
        self._store = store
        self._lineage = lineage
        self._credentials = credentials
        self._cluster = cluster
        self._code_host = code_host
        self._replies = replies
        self._job_settings = job_settings
        self._card_store = card_store
        self._transcript = transcript
        self._retained_card_refs: dict[str, ApprovalCardRef] = {}
        # Consecutive uncharged identity escapes per publication, bounded by
        # _MAX_UNCHARGED_IDENTITY_ESCAPES. In-process on purpose: a restart
        # restores the full allowance, which is the right bias, because a worker
        # that just started has no evidence the condition is permanent and the
        # durable reconcile_attempts counter still bounds the publication.
        self._identity_escapes: dict[uuid.UUID, int] = {}
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
                self._store.retry_card_delivery(work.publication_id, error=error, permanent=False)
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
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            error = str(exc)[:2000] or type(exc).__name__
            await _resolve(
                self._store.retry_card_delivery(
                    work.publication_id,
                    error=error,
                    # An unaddressable reply target is refused before any
                    # transport attempt and fails identically on every retry.
                    permanent=isinstance(exc, InvalidReplyTargetError),
                )
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
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
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
        metadata_updated_at: datetime | None,
    ) -> None:
        await _resolve(
            self._store.persist_result(
                work.publication_id,
                outcome=outcome,
                pr_url=pr_url,
                error=error,
                metadata_updated_at=metadata_updated_at,
            )
        )
        # Terminal: this publication is never reconciled again, so its escape
        # count would otherwise sit in a long-lived worker forever.
        self._identity_escapes.pop(work.publication_id, None)

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
        decided = result.resolved_at if decision is not None else None
        if decision is not None and (not isinstance(resolver, str) or not resolver.strip()):
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
                message=OutboundMessage(
                    version=MESSAGE_VERSION,
                    text=ref.summary,
                    # The click's decision time, so the rebuild keeps it (ADR-0179).
                    fields=[decided_field(decided)] if decided is not None else [],
                ),
                settled=SettledOutcome(
                    requested_by=ref.requested_by,
                    decision=decision,
                    resolver=resolver,
                    note=note,
                ),
            ),
            route=TargetRoute(
                endpoint=ref.endpoint,
                # An empty kind is a pre-identity ref. Its card was posted by
                # the historical default transport, never by the later result
                # route's identity.
                adapter=ref.adapter if ref.kind else None,
            ),
            best_effort_unreachable=False,
        )

    async def _result_target(self, result: Any, approval_id: str) -> ReplyTarget:
        # #2721: the approval row is persisted before delivery, so a ref-less
        # target's pending notice ref lives only in the card store. Edit that
        # notice instead of posting a second message; lookup is best-effort.
        target: ReplyTarget = result.target
        if target.reply_ref is not None or self._card_store is None:
            return target
        try:
            notice_ref = await self._card_store.read_notice_ref(approval_id)
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.warning(
                "publication notice ref lookup failed publication_id=%s",
                result.publication_id,
                exc_info=True,
            )
            return target
        if notice_ref is None:
            return target
        return target.model_copy(update={"reply_ref": notice_ref})

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
                    except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                        transcript_retry_error = exc
                        logger.warning(
                            "publication compact transcript outcome failed "
                            "publication_id=%s; retaining the durable fence",
                            result.publication_id,
                            exc_info=True,
                        )
                except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                    transcript_retry_error = exc
                    logger.warning(
                        "publication transcript recording failed transiently "
                        "publication_id=%s; delivering the routed result before retry",
                        result.publication_id,
                        exc_info=True,
                    )
                if transcript_retry_error is None:
                    await _resolve(self._store.mark_outcome_history_ready(result.publication_id))
            else:
                transcript_retry_error = PublicationReconcileError(
                    "publication transcript recording is not configured"
                )
            await self._report(await self._result_target(result, approval_id), result.route, text)
            if card_ref is not None:
                await self._settle_card(result, card_ref)
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            if card_ref is not None and self._card_store is not None:
                try:
                    await self._card_store.restore(
                        approval_id,
                        card_ref,
                    )
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
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
            except Exception as retry_exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                raise exc from retry_exc
            raise
        if transcript_retry_error is not None:
            transcript_error = (
                str(transcript_retry_error)[:1950] or type(transcript_retry_error).__name__
            )
            await _resolve(
                self._store.retry_result_delivery(
                    result.publication_id,
                    error=(f"publication transcript recording failed: {transcript_error}"),
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
        metadata_updated_at: datetime | None,
    ) -> None:
        # The durable outcome is the source of truth. Resource cleanup and reply
        # delivery are independent outboxes; result claims remain gated until
        # cleanup has durably completed.
        if outcome == "published" and not work.patch and metadata_updated_at is None:
            raise PublicationReconcileError("metadata-only publication has no GitHub update time")
        if new_head is not None:
            if pr_url is None or pr_number is None:
                raise PublicationReconcileError("publication success omitted pull request identity")
            await self._advance_lineage(
                work,
                pr_url=pr_url,
                pr_number=pr_number,
                new_head=new_head,
                metadata_updated_at=metadata_updated_at,
            )
        await self._persist_result(
            work,
            outcome=outcome,
            pr_url=pr_url,
            error=error,
            metadata_updated_at=metadata_updated_at,
        )
        await self.deliver_pending_cleanup()
        await self.deliver_pending_result(work.publication_id)

    async def _advance_lineage(
        self,
        work: PublicationWork,
        *,
        pr_url: str,
        pr_number: int,
        new_head: str,
        metadata_updated_at: datetime | None,
    ) -> None:
        # ADR 0143: the API verifies GitHub identity and advances the lineage
        # with the publication outcome in one compare-and-set, fenced by this
        # worker's claimed publication version and lease.
        if (not work.patch) != (metadata_updated_at is not None):
            raise PublicationReconcileError(
                "a GitHub update time is required only for metadata only publications"
            )
        try:
            await _resolve(
                self._lineage.advance(
                    work.publication_id,
                    expected_version=work.lineage_version,
                    expected_head_sha=work.expected_remote_head,
                    expected_publication_version=work.version,
                    lease_owner=work.lease_owner,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    head_sha=new_head,
                    metadata_updated_at=metadata_updated_at,
                )
            )
        except PublicationRemoteTerminalError as terminal:
            await self._mark_lineage_terminal(
                work,
                terminal.state,
                pr_number=pr_number,
                pr_url=pr_url,
                head_sha=new_head,
            )
            raise PublicationReconcileError(str(terminal)) from None
        except PublicationLineageRefused:
            # A replay after a lost response finds its own settled outcome.
            if not await _resolve(self._store.is_terminal(work.publication_id)):
                raise
        else:
            self._identity_escapes.pop(work.publication_id, None)

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

    async def _identity_unavailable(
        self,
        work: PublicationWork,
        exc: PublicationIdentityUnavailable,
    ) -> None:
        """Escape reconcile() uncharged, or bound a condition that is not transient.

        Re-raising leaves the lease in place for an uncharged lease-expiry retry,
        which is the right cost for a GitHub incident and the wrong cost for a
        deleted repository. At the bound the escape stops and this method charges
        the attempt itself through the ordinary bounded path, so the publication
        converges on a visible failure carrying the provider's reason (#2903).
        """

        escapes = self._identity_escapes.get(work.publication_id, 0) + 1
        self._identity_escapes[work.publication_id] = escapes
        if escapes <= _MAX_UNCHARGED_IDENTITY_ESCAPES:
            raise exc
        logger.warning(
            "publication identity has been unavailable for %d consecutive leases; "
            "charging a bounded reconcile attempt publication_id=%s reason=%s",
            escapes,
            work.publication_id,
            exc,
        )
        await self._bounded_setup_failure(work, exc)

    async def _bounded_setup_failure(
        self,
        work: PublicationWork,
        exc: Exception,
    ) -> None:
        error = str(exc)[:2000] or type(exc).__name__
        await _resolve(self._store.retry(work.publication_id, error=error))
        # retry() terminalizes at its durable cap. If it did, drain the newly
        # available cleanup and result outboxes; otherwise these are no-ops.
        # The dict membership test comes first on purpose: is_terminal() is a
        # database round trip, this is the common failure path for every
        # publication, and the only thing its answer decides here is whether to
        # pop an escape count that almost never exists.
        if work.publication_id in self._identity_escapes and await _resolve(
            self._store.is_terminal(work.publication_id)
        ):
            # Dead-lettered inside the store, so _persist_result never ran and
            # nothing else would ever drop this publication's escape count.
            self._identity_escapes.pop(work.publication_id, None)
        await self.deliver_pending_cleanup()
        await self.deliver_pending_result(work.publication_id)

    @staticmethod
    def _payload(work: PublicationWork, transport: PublicationTransport) -> PublicationPayload:
        return PublicationPayload(
            publication_id=work.publication_id,
            revision_id=work.revision_id,
            revision_number=work.revision_number,
            repo_full_name=work.repo_full_name,
            clean_clone_url=f"{transport.origin}/{work.repo_full_name}.git",
            base_sha=work.base_sha,
            expected_prior_head=work.expected_prior_head,
            expected_remote_head=work.expected_remote_head,
            patch=work.patch,
            branch=work.branch,
            title=work.title,
            transport=transport,
            branch_prefix=work.branch_prefix,
        )

    async def _read_stored_pull(self, work: PublicationWork) -> PublicationPullState | None:
        if work.pr_number is None:
            return None
        pull = await _resolve(
            self._code_host.read_pull_request(work.publication_id, work.pr_number)
        )
        if pull.head_ref != work.branch:
            raise PublicationReconcileError(
                "pull request head branch no longer matches the stored lineage branch"
            )
        if (
            work.pr_url is None
            or _checked_pr_url(pull.url, pull.number).casefold() != work.pr_url.casefold()
        ):
            raise PublicationReconcileError(
                "pull request URL no longer matches the stored lineage identity"
            )
        return pull

    async def _record_terminal_pull(
        self, work: PublicationWork, pull: PublicationPullState
    ) -> None:
        """Record a merged or closed pull request on a head this lineage trusts, then stop.

        The pull request's head is trusted when it is the stored lineage head,
        or when the API proves it is this revision's marked commit on the
        expected parent. Otherwise the stored head is recorded, and with none
        the lineage cannot be closed safely.
        """

        assert pull.state != "open"
        trusted_head = work.expected_remote_head
        if pull.head_sha != trusted_head:
            try:
                verified_head = await _resolve(
                    self._code_host.verify_revision_commit(
                        work.publication_id,
                        pull.head_sha,
                        revision_id=work.revision_id,
                        expected_parent=work.expected_prior_head,
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
            pr_url=_checked_pr_url(pull.url, pull.number),
            head_sha=trusted_head,
        )
        raise PublicationReconcileError(f"pull request lineage is {pull.state}; start a new thread")

    async def _settle_pull(
        self,
        work: PublicationWork,
        pull: PublicationPullState,
        *,
        pushed_head: str,
        names: PublicationResourceNames,
        metadata_updated_at: datetime | None = None,
    ) -> None:
        """Publish ``pushed_head`` on ``pull``, or record the pull request terminal.

        A pull request merged or closed while the Job was scheduled or pushing
        is recorded from this read (ADR 0197, Consequence 6).
        """

        url = _checked_pr_url(pull.url, pull.number)
        if pull.head_ref != work.branch:
            raise PublicationReconcileError(
                "pull request head branch no longer matches the stored lineage branch"
            )
        if work.pr_number is not None and pull.number != work.pr_number:
            raise PublicationReconcileError("the API returned a different stored pull request")
        if pull.state != "open":
            if pull.head_sha == pushed_head:
                await self._mark_lineage_terminal(
                    work,
                    pull.state,
                    pr_number=pull.number,
                    pr_url=url,
                    head_sha=pushed_head,
                )
                raise PublicationReconcileError(
                    f"pull request lineage is {pull.state}; start a new thread"
                )
            await self._record_terminal_pull(work, pull)
        if pull.head_sha != pushed_head:
            raise PublicationReconcileError(
                "pull request head does not match the pushed publication commit"
            )
        await self._terminalize(
            work,
            outcome="published",
            pr_url=url,
            pr_number=pull.number,
            new_head=pushed_head,
            names=names,
            metadata_updated_at=metadata_updated_at,
        )

    async def _settle_pushed(
        self, work: PublicationWork, commit_sha: str, names: PublicationResourceNames
    ) -> None:
        """After the Job's push: prove the commit, then find or open its pull request.

        The Job's marker is not authority on its own: the API proves the
        remote commit is this revision's marked commit on the expected parent
        before anything is recorded.
        """

        await _resolve(
            self._code_host.verify_revision_commit(
                work.publication_id,
                commit_sha,
                revision_id=work.revision_id,
                expected_parent=work.expected_prior_head,
            )
        )
        if work.pr_number is None:
            pull = await _resolve(
                self._code_host.recover_pull_request(
                    work.publication_id, expected_head_sha=commit_sha
                )
            )
            if pull is None:
                raise PublicationReconcileError(
                    "the pushed publication branch is absent on the code host"
                )
        else:
            pull = await self._read_stored_pull(work)
            assert pull is not None
        await self._settle_pull(work, pull, pushed_head=commit_sha, names=names)

    async def _finish_observation(
        self,
        work: PublicationWork,
        observation: PublicationJobObservation,
        names: PublicationResourceNames,
    ) -> bool:
        commit_sha = observation.commit_sha or _marker_commit(observation.logs)
        if commit_sha is None:
            if observation.phase in {"pending", "running"}:
                return False
            if observation.phase == "failed":
                raise PublicationReconcileError(
                    observation.error or "publication Job failed without a pushed commit"
                )
            raise PublicationReconcileError(
                "publication Job succeeded without its pushed commit marker"
            )
        # The commit marker is the script's final line, printed only after the
        # push, so it settles the publication now instead of waiting on pod
        # exit and Job status (#3074). The API finds or opens the pull request.
        await self._settle_pushed(work, commit_sha, names)
        return True

    async def _publish_metadata(self, work: PublicationWork) -> None:
        """A metadata-only revision: the API updates the stored pull request.

        It has nothing to push, so no Job runs. A merged or closed pull request
        is recorded terminal instead.
        """

        names = publication_resource_names(work.publication_id)
        try:
            if work.pr_number is None:
                raise PublicationReconcileError(
                    "metadata-only publication requires a stored pull request"
                )
            pull = await _resolve(self._code_host.update_pull_request_metadata(work.publication_id))
            if pull.number != work.pr_number or (
                work.pr_url is None
                or _checked_pr_url(pull.url, pull.number).casefold() != work.pr_url.casefold()
            ):
                raise PublicationReconcileError(
                    "pull request URL no longer matches the stored lineage identity"
                )
            if pull.state == "open" and pull.updated_at is None:
                raise PublicationReconcileError(
                    "metadata-only publication has no code host update time"
                )
        except PublicationIdentityUnavailable as identity_exc:
            await self._identity_unavailable(work, identity_exc)
            return
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._bounded_setup_failure(work, exc)
            return
        try:
            await self._settle_pull(
                work,
                pull,
                pushed_head=work.base_sha,
                names=names,
                metadata_updated_at=pull.updated_at if pull.state == "open" else None,
            )
        except PublicationIdentityUnavailable as identity_exc:
            await self._identity_unavailable(work, identity_exc)
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            if await _resolve(self._store.is_terminal(work.publication_id)):
                raise
            await self._bounded_setup_failure(work, exc)

    async def reconcile(self, work: PublicationWork, *, allow_launch: bool = True) -> None:
        names = publication_resource_names(work.publication_id)
        if await _resolve(self._store.is_terminal(work.publication_id)):
            # Terminalized by another lane (denial, expiry, a peer worker);
            # drop any escape count so it cannot outlive the publication.
            self._identity_escapes.pop(work.publication_id, None)
            return

        # Pending, expired, and unknown states are never authority. Expiry is
        # terminalized by the API/store lane, as is denial; neither creates a
        # cluster or code host side effect here.
        if work.decision != "approved":
            return
        if not publication_branch_is_valid(work.branch, work.branch_prefix):
            await self._bounded_setup_failure(
                work,
                PublicationReconcileError(
                    "publication branch does not carry the required prefix"
                    if work.branch_prefix and not work.branch.startswith(work.branch_prefix)
                    else "publication branch is not a valid stored lineage branch"
                ),
            )
            return

        if not work.patch:
            if not allow_launch:
                await _resolve(
                    self._store.persist_result(
                        work.publication_id,
                        outcome="failed",
                        pr_url=None,
                        error="the factory run already ended",
                        metadata_updated_at=None,
                    )
                )
                return
            await self._publish_metadata(work)
            return

        try:
            observation = await _cluster_call(self._cluster.observe, names.job)
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._bounded_setup_failure(work, exc)
            return

        if observation.exists:
            # Validate deterministic collisions before trusting either an
            # in-flight state or the Job's commit marker. The placeholder is
            # used only to construct the expected immutable Secret shape;
            # validate_existing deliberately does not compare Secret bytes, so
            # a rotated credential is never needed merely to adopt the
            # already-created Job. Its transport facts are rebuilt into the
            # expected Job and validated with everything else.
            try:
                if observation.transport is None:
                    raise PublicationResourceError(
                        "existing publication Job names no code host transport"
                    )
                probe_resources = build_publication_resources(
                    self._payload(work, observation.transport),
                    credential="validation-placeholder",
                    settings=self._job_settings,
                )
                await _cluster_call(self._cluster.validate_existing, probe_resources)
            except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                await self._bounded_setup_failure(work, exc)
                return
            if observation.phase in {"pending", "running"} or (
                observation.commit_sha or _marker_commit(observation.logs)
            ):
                try:
                    if await self._finish_observation(work, observation, probe_resources.names):
                        return
                except PublicationIdentityUnavailable as identity_exc:
                    # A transient lineage verification failure must never
                    # consume a reconcile attempt. Lease expiry retries it
                    # uncharged, bounded so a permanent failure still converges.
                    await self._identity_unavailable(work, identity_exc)
                    return
                except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                    if await _resolve(self._store.is_terminal(work.publication_id)):
                        raise
                    await self._bounded_setup_failure(work, exc)
                    return
                if not allow_launch:
                    await _resolve(
                        self._store.persist_result(
                            work.publication_id,
                            outcome="failed",
                            pr_url=None,
                            error="the factory run already ended",
                            metadata_updated_at=None,
                        )
                    )
                    return
                await self._release_in_flight(work)
                return

        if not allow_launch:
            await _resolve(
                self._store.persist_result(
                    work.publication_id,
                    outcome="failed",
                    pr_url=None,
                    error="the factory run already ended",
                    metadata_updated_at=None,
                )
            )
            return

        credential: PublicationCredential
        try:
            # The approved decision is the only authority to redeem. Redeem
            # exactly once, for the one immutable Job Secret. The stored pull
            # request is checked here, before the Job launches (ADR 0197,
            # Consequence 6); the pull request, branch and commit reads go
            # through the API's code host.
            credential = await _resolve(self._credentials.redeem(work.publication_id))
            pull = await self._read_stored_pull(work)
            if pull is None:
                branch_head = await _resolve(self._code_host.read_branch_head(work.publication_id))
                if branch_head is not None:
                    # A lost Job or result: the branch already holds a pushed
                    # revision. Prove it is this one, then settle its pull request.
                    await _resolve(
                        self._code_host.verify_revision_commit(
                            work.publication_id,
                            branch_head,
                            revision_id=work.revision_id,
                            expected_parent=work.expected_prior_head,
                        )
                    )
                    recovered = await _resolve(
                        self._code_host.recover_pull_request(
                            work.publication_id, expected_head_sha=branch_head
                        )
                    )
                    if recovered is None:
                        raise PublicationReconcileError(
                            "verified publication branch has no recoverable pull request"
                        )
                    await self._settle_pull(work, recovered, pushed_head=branch_head, names=names)
                    return
            if pull is not None and pull.state != "open":
                await self._record_terminal_pull(work, pull)
            if pull is not None and pull.head_sha != work.expected_prior_head:
                try:
                    await _resolve(
                        self._code_host.verify_revision_commit(
                            work.publication_id,
                            pull.head_sha,
                            revision_id=work.revision_id,
                            expected_parent=work.expected_prior_head,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                    raise PublicationReconcileError(
                        "pull request head no longer matches the stored lineage head"
                    ) from exc
        except PublicationIdentityUnavailable as identity_exc:
            await self._identity_unavailable(work, identity_exc)
            return
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._bounded_setup_failure(work, exc)
            return

        if pull is not None and pull.head_sha != work.expected_prior_head:
            # Verification above proved the remote head is this revision's
            # exact marked commit with the expected parent. Persisting the
            # lineage CAS may expose cleanup or reply-outbox failures; those
            # must escape as outbox work, never be charged as another attempt
            # at the already completed publication mutation. The API's lineage
            # advance is not yet committed, so its refusal or outage is bounded.
            try:
                await self._advance_lineage(
                    work,
                    pr_url=pull.url,
                    pr_number=pull.number,
                    new_head=pull.head_sha,
                    metadata_updated_at=None,
                )
            except PublicationIdentityUnavailable as identity_exc:
                await self._identity_unavailable(work, identity_exc)
                return
            except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                await self._bounded_setup_failure(work, exc)
                return
            await self._terminalize(
                work,
                outcome="published",
                pr_url=pull.url,
                names=names,
                metadata_updated_at=None,
            )
            return

        if observation.exists:
            # A validated terminal Job without its commit marker. The code host
            # reads above found nothing pushed: the stored pull request still
            # names the expected prior head, or no branch exists. Preserve the
            # Job's terminal failure instead of mutating or replacing
            # deterministic resources.
            if observation.phase == "failed":
                # A failed Job (backoffLimit 0) cannot change outcome, so
                # bounded retries would only hide the reason. Terminalize once
                # so the thread sees why and may request a new approval. The
                # store caps error at 2000 chars; bound the reason so the
                # ask-again instruction always survives.
                reason = (observation.error or "publication Job failed")[:1800]
                logger.warning(
                    "publication Job failed before any push publication_id=%s reason=%s",
                    work.publication_id,
                    reason,
                )
                error = (
                    f"{reason.rstrip('. ')}. Nothing was pushed to {work.repo_full_name}; "
                    "ask again to request a new publication approval."
                )
                await self._terminalize(
                    work,
                    outcome="failed",
                    error=error,
                    names=names,
                    metadata_updated_at=None,
                )
                return
            try:
                await self._finish_observation(work, observation, names)
            except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                await self._bounded_setup_failure(work, exc)
            return

        try:
            resources = build_publication_resources(
                self._payload(work, credential.transport),
                credential=credential.authorization_header,
                settings=self._job_settings,
            )
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._bounded_setup_failure(work, exc)
            return

        # Server-side apply/create-or-adopt validates every deterministic
        # resource before any Job log marker is trusted. This includes a Job
        # observed before the apply: an attacker cannot plant a same-name Job
        # and make its marker authoritative without passing the full spec and
        # ownership contract, and the marker's commit is proven by the API.
        try:
            await _cluster_call(self._cluster.apply, resources)
        except PublicationResourceError as exc:
            await self._bounded_setup_failure(work, exc)
            return
        except Exception as apply_exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            # The apiserver may have accepted the resources and lost only the
            # response. Observe the deterministic name, then recover the
            # deterministic remote head, before charging a bounded retry.
            try:
                observation = await _cluster_call(self._cluster.observe, resources.names.job)
                in_flight = False
                if observation.exists:
                    if await self._finish_observation(work, observation, resources.names):
                        return
                    in_flight = observation.phase in {"pending", "running"}
            except PublicationIdentityUnavailable as identity_exc:
                await self._identity_unavailable(work, identity_exc)
                return
            except Exception as recovery_exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                if await _resolve(self._store.is_terminal(work.publication_id)):
                    raise
                await self._bounded_setup_failure(
                    work,
                    PublicationReconcileError(
                        f"ambiguous publication apply could not be recovered: {recovery_exc}"
                    ),
                )
                return
            if in_flight:
                await self._release_in_flight(work)
                return
            await self._bounded_setup_failure(work, apply_exc)
            return

        try:
            observation = await _cluster_call(self._cluster.observe, resources.names.job)
            finished = await self._finish_observation(work, observation, resources.names)
        except PublicationIdentityUnavailable as identity_exc:
            await self._identity_unavailable(work, identity_exc)
            return
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            if await _resolve(self._store.is_terminal(work.publication_id)):
                raise
            await self._bounded_setup_failure(work, exc)
            return
        if not finished:
            await self._release_in_flight(work)

    async def _release_in_flight(self, work: PublicationWork) -> None:
        # The Job is still in flight. Release the lease uncharged so the next
        # pass observes it promptly instead of waiting out the lease. A
        # re-claim adopts the deterministic Job and never re-redeems, because
        # redeem runs only when no Job exists.
        await _resolve(self._store.release(work.publication_id))


class PublicationReconcileLoop:
    """Poll durable work under the worker's ordinary task supervisor."""

    def __init__(
        self,
        *,
        store: Any,
        reconciler: PublicationReconciler,
        interval_seconds: float = 2.0,
        batch_limit: int = 16,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("publication reconciliation interval must be positive")
        if batch_limit <= 0:
            raise ValueError("publication reconciliation batch limit must be positive")
        self._store = store
        self._reconciler = reconciler
        self._interval = interval_seconds
        self._batch_limit = batch_limit

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await self._reconciler.deliver_pending_card()
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.exception("publication approval card delivery failed")
            try:
                await self._reconciler.deliver_pending_cleanup()
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                # Cleanup is deliberately unbounded. Its released lease is
                # reclaimed until every deterministic resource is absent.
                logger.exception("publication resource cleanup failed")
            try:
                await self._reconciler.deliver_pending_result()
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                # The result lease was released (or dead-lettered) before the
                # error escaped. Publication mutation remains terminal and is
                # never repeated because a reply transport is unavailable.
                logger.exception("publication result delivery failed")
            # Drain claimable work each pass so concurrent publications do not
            # serialize one per interval, bounded so outboxes still run. An
            # in-flight Job releases its lease, so the claim excludes what this
            # pass already reconciled instead of returning the oldest one again
            # and starving the rest behind it.
            seen: set[uuid.UUID] = set()
            for _ in range(self._batch_limit):
                try:
                    work = await self._store.claim_next(exclude=seen)
                except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                    logger.exception(
                        "publication claim_next failed cause=%s: %s",
                        type(exc).__name__,
                        exc,
                    )
                    raise
                if work is None:
                    break
                seen.add(work.publication_id)
                try:
                    if not work.owner_running:
                        # Observe a pull request the job already opened.
                        # Do not launch a new job after the factory run ended.
                        await self._reconciler.reconcile(work, allow_launch=False)
                    else:
                        await self._reconciler.reconcile(work)
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
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
    "PublicationIdentityUnavailable",
    "PublicationJobObservation",
    "PublicationLineageAuthority",
    "PublicationLineageRefused",
    "PublicationPullState",
    "PublicationReconcileError",
    "PublicationReconciler",
    "PublicationReconcileLoop",
    "PublicationRemoteTerminalError",
    "PublicationTranscriptPermanentError",
    "PublicationWork",
    "deterministic_publication_branch",
]
