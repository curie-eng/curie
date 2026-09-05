"""The worker's approval-record client (#244, ADR-0010).

When a run ends ``awaiting-approval`` the kernel persists a durable ``Approval``
record before suspending the session, so the pending human decision survives
every component restarting. The record lives server-side with the API (the
authorizer of #246 is enforced there, where it cannot be spoofed from inside a
sandbox); this module is the thin write client, mirroring the eval lane's
``EvalReporter`` (same base URL + shared API key).

Creation is idempotent: ``dedupe_key`` carries the triggering event id, so a
reclaimed/redelivered turn that re-requests the same approval adopts the
existing record (the API answers 200 instead of 201) rather than forking a
second pending record for one human decision.
"""

from __future__ import annotations

import base64
import logging
import re
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from aci_protocol import ApprovalRequest, QueuedTurn
from curie_telemetry import inject_trace_context

from .workspace import WorkspaceSelectionRefused

# Re-exported so this module stays the kernel-facing seam for the approval
# payload: ``ApprovalRequest`` is now the shared wire model (#492), not a
# lane-local mirror of the API's schema.
logger = logging.getLogger(__name__)

_PUBLICATION_REFUSAL_CODES = {
    "publication.github_unavailable",
    "publication.lineage_stale",
    "publication.lineage_terminal",
}
_TERMINAL_WORKSPACE_CONFLICT_DETAILS = {
    "conversation has no selected repository workspace",
    "publication repository differs from the thread workspace",
    "thread workspace repository is no longer allowed",
}


def _publication_refusal(response: httpx.Response) -> str | None:
    """Return a safe API-classified refusal instead of making it retryable."""

    if response.status_code not in (409, 502, 503):
        return None
    try:
        detail = response.json()["detail"]
    except (KeyError, TypeError, ValueError):
        return None
    if isinstance(detail, dict):
        code = detail.get("code")
        message = detail.get("message")
        if (
            isinstance(code, str)
            and code in _PUBLICATION_REFUSAL_CODES
            and isinstance(message, str)
            and message.strip()
        ):
            return message
        return None
    if isinstance(detail, str) and detail in _TERMINAL_WORKSPACE_CONFLICT_DETAILS:
        return detail
    return None


__all__ = [
    "ApprovalBackendError",
    "ApprovalClient",
    "ApprovalCreator",
    "ApprovalReader",
    "ApprovalRequest",
    "CreatedApproval",
    "SettledApproval",
    "CreatedPublication",
    "PublicationCreateRequest",
    "PublicationCreator",
    "PublicationLineage",
]


@dataclass(frozen=True)
class CreatedApproval:
    """What the kernel needs back: the record's identity and its status."""

    id: str
    status: str


@dataclass(frozen=True)
class PublicationCreateRequest:
    """API-local atomic Approval+Publication write request."""

    deployment_id: uuid.UUID
    conversation_id: str
    repo_full_name: str
    author: str
    summary: str
    reply_kind: str
    reply_channel: str
    reply_placeholder: str | None
    reply_endpoint: str | None
    reply_adapter: str | None
    dedupe_key: str
    base_sha: str
    patch: bytes
    changed_paths: tuple[str, ...]
    expires_in_seconds: int
    title: str
    body: str
    reply_conversation_id: str | None = None
    max_patch_bytes: int = 900_000
    review_origin_key: str | None = None

    def to_json(self) -> dict[str, Any]:
        if len(self.patch) > self.max_patch_bytes:
            raise ApprovalBackendError(
                f"publication patch exceeds {self.max_patch_bytes} raw bytes"
            )
        payload: dict[str, Any] = {
            "deployment_id": str(self.deployment_id),
            "conversation_id": self.conversation_id,
            "repo_full_name": self.repo_full_name,
            "author": self.author,
            "summary": self.summary,
            "reply_kind": self.reply_kind,
            "reply_channel": self.reply_channel,
            "reply_placeholder": self.reply_placeholder,
            "reply_endpoint": self.reply_endpoint,
            "reply_adapter": self.reply_adapter,
            "dedupe_key": self.dedupe_key,
            "base_sha": self.base_sha,
            "patch_b64": base64.b64encode(self.patch).decode("ascii"),
            "changed_paths": list(self.changed_paths),
            "expires_in_seconds": self.expires_in_seconds,
            "title": self.title,
            "body": self.body,
        }
        if self.reply_conversation_id is not None:
            payload["reply_conversation_id"] = self.reply_conversation_id
        if self.review_origin_key is not None:
            payload["review_origin_key"] = self.review_origin_key
        return payload


@dataclass(frozen=True)
class CreatedPublication:
    id: str
    approval_id: str
    status: str


@dataclass(frozen=True)
class PublicationLineage:
    id: uuid.UUID
    deployment_id: uuid.UUID
    conversation_id: str
    repo_full_name: str
    base_sha: str
    branch: str
    pr_number: int | None
    pr_url: str | None
    head_sha: str | None
    state: str
    version: int
    latest_revision: int
    has_pending_revision: bool
    has_pending_outcome: bool
    visible_outcome_revision: int


class ApprovalBackendError(Exception):
    """The approval record could not be created; the kernel escalates rather
    than suspending a session no resolution could ever wake."""


class ReviewAuthorityUnavailable(Exception):
    """Review authority is unavailable before any model turn was accepted.

    Keep the delivery pending for the shared bounded reclaim/dead-letter lane;
    spending the model attempt budget would settle feedback that never ran.
    """


@dataclass(frozen=True)
class SettledApproval:
    """A resolved record's outcome, for stamping its card (#1084).

    Read from the record rather than parsed out of the platform-authored resume
    turn. That turn does carry all three facts in prose, and the kernel already
    keys off its ``[approval expired]`` marker, but a marker is a stable literal
    while "was approved by X. Note: Y." is a sentence -- reconstructing a
    decision by regex over it would make the card's correctness depend on the
    wording of a string built for a language model to read.
    """

    status: str
    resolved_by: str | None
    resolution_note: str | None


class ApprovalCreator(Protocol):
    """The kernel-facing seam; tests supply a recording fake."""

    async def create(self, request: ApprovalRequest) -> CreatedApproval: ...


@dataclass(frozen=True)
class VerifiedReviewFeedback:
    head_sha: str
    agent_id: uuid.UUID
    sender: str
    receipt: str
    origin_key: str
    lineage_version: int
    reservation_id: uuid.UUID | None


class PublicationCreator(Protocol):
    """Atomic trusted write seam used only for exact publish provenance."""

    async def create_publication(self, request: PublicationCreateRequest) -> CreatedPublication: ...

    async def get_publication_lineage(
        self,
        deployment_id: uuid.UUID,
        conversation_id: str,
        repo_full_name: str,
    ) -> PublicationLineage | None: ...

    async def verify_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
    ) -> VerifiedReviewFeedback: ...

    async def reserve_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
        verified: VerifiedReviewFeedback,
    ) -> uuid.UUID: ...


class ApprovalReader(Protocol):
    """Read one settled record back. Separate from ``ApprovalCreator`` because
    the kernel's pause path needs only the create half, and a fake for it should
    not have to grow a method it never calls."""

    async def get(self, approval_id: str) -> SettledApproval | None: ...


class ApprovalClient:
    """HTTP implementation against the platform API's /approvals endpoint."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
        read_timeout_s: float,
        worker_token: str = "",
        review_timeout_s: float = 30.0,
    ) -> None:
        self._url = f"{api_base_url.rstrip('/')}/approvals"
        self._publication_url = f"{api_base_url.rstrip('/')}/v1/internal/publications"
        self._review_url = f"{api_base_url.rstrip('/')}/v1/internal/github/reviews"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._worker_headers = {"X-Curie-Worker-Token": worker_token} if worker_token else {}
        self._client = client
        self._read_timeout_s = read_timeout_s
        self._review_timeout_s = review_timeout_s

    async def verify_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
    ) -> VerifiedReviewFeedback:
        """Read the API's current App/binding/head authority, never a credential."""
        refusal = (
            "GitHub feedback could not be verified for this conversation; no model turn started."
        )
        if (
            not self._worker_headers
            or re.fullmatch(
                r"github-feedback-[0-9a-f-]{36}",
                turn.event_id,
            )
            is None
        ):
            raise WorkspaceSelectionRefused(refusal)
        try:
            response = await self._client.post(
                f"{self._review_url}/{turn.event_id}/verify",
                json={"turn": turn.model_dump(mode="json"), "deployment_id": str(deployment_id)},
                headers=self._worker_headers,
                follow_redirects=False,
                timeout=self._review_timeout_s,
            )
        except httpx.HTTPError:
            raise ApprovalBackendError(
                "GitHub feedback verification transport unavailable"
            ) from None
        if response.status_code in {401, 403, 404, 429} or response.status_code >= 500:
            raise ApprovalBackendError("GitHub feedback verification temporarily unavailable")
        if response.status_code != 200:
            # Response bodies may contain upstream/model text; expose only our
            # fixed policy refusal. Infrastructure/auth/rollout failures above
            # remain retryable and cannot silently ACK an unexecuted turn.
            raise WorkspaceSelectionRefused(refusal)
        try:
            body = response.json()
            head, sender, receipt = body["head_sha"], body["sender"], body["receipt"]
            if (
                not isinstance(head, str)
                or re.fullmatch(r"[0-9a-f]{40}", head) is None
                or sender != turn.author
                or not isinstance(receipt, str)
                or not receipt
                or len(receipt) > 1024
                or body["origin_key"] != turn.event_id
                or type(body["lineage_version"]) is not int
                or body["lineage_version"] < 1
            ):
                raise ValueError("invalid verified feedback")
            return VerifiedReviewFeedback(
                head,
                uuid.UUID(body["agent_id"]),
                sender,
                receipt,
                body["origin_key"],
                body["lineage_version"],
                uuid.UUID(body["reservation_id"]) if body["reservation_id"] is not None else None,
            )
        except (ValueError, TypeError, KeyError):
            raise WorkspaceSelectionRefused(refusal) from None

    async def reserve_review_feedback(
        self,
        turn: QueuedTurn,
        deployment_id: uuid.UUID,
        verified: VerifiedReviewFeedback,
    ) -> uuid.UUID:
        """Bind a verified origin to its own revision after the idle fence."""
        if not self._worker_headers or verified.origin_key != turn.event_id:
            raise WorkspaceSelectionRefused("GitHub feedback revision identity was refused.")
        try:
            response = await self._client.post(
                f"{self._review_url}/{turn.event_id}/reserve",
                json={
                    "turn": turn.model_dump(mode="json"),
                    "deployment_id": str(deployment_id),
                    "expected_lineage_version": verified.lineage_version,
                    "expected_head_sha": verified.head_sha,
                },
                headers=self._worker_headers,
                follow_redirects=False,
                # This endpoint performs only SQL CAS and must remain short
                # while the worker holds the idle thread's route lock.
                timeout=self._read_timeout_s,
            )
        except httpx.HTTPError:
            raise ApprovalBackendError("GitHub review reservation transport unavailable") from None
        if response.status_code in {401, 403, 404, 429} or response.status_code >= 500:
            raise ApprovalBackendError("GitHub review reservation temporarily unavailable")
        if response.status_code != 200:
            raise WorkspaceSelectionRefused("GitHub feedback revision is no longer executable.")
        try:
            body = response.json()
            if body["origin_key"] != turn.event_id:
                raise ValueError("wrong review origin")
            return uuid.UUID(body["reservation_id"])
        except (ValueError, TypeError, KeyError):
            raise WorkspaceSelectionRefused(
                "GitHub feedback revision identity was refused."
            ) from None

    async def create(self, request: ApprovalRequest) -> CreatedApproval:
        headers = {**self._headers, "Content-Type": "application/json"}
        inject_trace_context(headers)
        try:
            response = await self._client.post(
                self._url,
                content=request.model_dump_json(),
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise ApprovalBackendError(f"approval create failed: {exc}") from exc
        # 201 is a fresh record; 200 is the idempotent dedupe_key replay.
        if response.status_code not in (200, 201):
            raise ApprovalBackendError(
                f"approval create failed: HTTP {response.status_code}: {response.text}"
            )
        body = response.json()
        return CreatedApproval(id=str(body["id"]), status=str(body["status"]))

    async def get(self, approval_id: str) -> SettledApproval | None:
        """The record's settled outcome, or None when it cannot be read (#1084).

        Never raises. Its only caller is best-effort card teardown on a resume
        turn: the resolution already happened and the session is already waking,
        so a failed read costs a stamped card and nothing else. Raising here
        would turn a cosmetic gap into a dead-lettered resume.
        """

        headers = dict(self._headers)
        inject_trace_context(headers)
        try:
            response = await self._client.get(
                f"{self._url}/{approval_id}",
                headers=headers,
                timeout=self._read_timeout_s,
            )
        except httpx.HTTPError as exc:
            logger.warning("approval read failed for %s: %s", approval_id, exc)
            return None
        if response.status_code != 200:
            logger.warning(
                "approval read failed for %s: HTTP %s", approval_id, response.status_code
            )
            return None
        try:
            body = response.json()
            return SettledApproval(
                status=str(body["status"]),
                resolved_by=body.get("resolved_by"),
                resolution_note=body.get("resolution_note"),
            )
        except (ValueError, KeyError) as exc:
            logger.warning("approval read returned an unusable body for %s: %s", approval_id, exc)
            return None

    async def create_publication(self, request: PublicationCreateRequest) -> CreatedPublication:
        """Atomically persist the approval and its private patch.

        The ordinary platform API key is intentionally not accepted on this
        route.  If the dedicated worker credential is absent, fail before any
        request so a local/non-cluster install cannot create a stranded card.
        """

        if not self._worker_headers:
            raise ApprovalBackendError(
                "repository publication is cluster-only and requires internal worker auth"
            )
        headers = {**self._worker_headers, "Content-Type": "application/json"}
        inject_trace_context(headers)
        try:
            response = await self._client.post(
                self._publication_url,
                json=request.to_json(),
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise ApprovalBackendError(f"publication create failed: {exc}") from exc
        refusal = _publication_refusal(response)
        if refusal is not None:
            raise WorkspaceSelectionRefused(refusal)
        if response.status_code not in (200, 201):
            raise ApprovalBackendError(
                f"publication create failed: HTTP {response.status_code}: {response.text}"
            )
        try:
            body = response.json()
            return CreatedPublication(
                id=str(body["id"]),
                approval_id=str(body["approval_id"]),
                status=str(body["status"]),
            )
        except (ValueError, KeyError) as exc:
            raise ApprovalBackendError("publication create returned an unusable body") from exc

    async def get_publication_lineage(
        self,
        deployment_id: uuid.UUID,
        conversation_id: str,
        repo_full_name: str,
    ) -> PublicationLineage | None:
        """Read credential-free lineage before choosing a runner route."""

        if not self._worker_headers:
            return None
        try:
            response = await self._client.get(
                f"{self._publication_url}/lineage",
                params={
                    "deployment_id": str(deployment_id),
                    "conversation_id": conversation_id,
                    "repo_full_name": repo_full_name,
                },
                headers=self._worker_headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise ApprovalBackendError(f"publication lineage read failed: {exc}") from exc
        if response.status_code == 404:
            return None
        refusal = _publication_refusal(response)
        if refusal is not None:
            raise WorkspaceSelectionRefused(refusal)
        if response.status_code != 200:
            raise ApprovalBackendError(
                f"publication lineage read failed: HTTP {response.status_code}: {response.text}"
            )
        try:
            body = response.json()
            return PublicationLineage(
                id=uuid.UUID(str(body["id"])),
                deployment_id=uuid.UUID(str(body["deployment_id"])),
                conversation_id=str(body["conversation_id"]),
                repo_full_name=str(body["repo_full_name"]),
                base_sha=str(body["base_sha"]),
                branch=str(body["branch"]),
                pr_number=int(body["pr_number"]) if body.get("pr_number") is not None else None,
                pr_url=str(body["pr_url"]) if body.get("pr_url") is not None else None,
                head_sha=str(body["head_sha"]) if body.get("head_sha") is not None else None,
                state=str(body["state"]),
                version=int(body["version"]),
                latest_revision=int(body["latest_revision"]),
                has_pending_revision=bool(body["has_pending_revision"]),
                has_pending_outcome=bool(body["has_pending_outcome"]),
                visible_outcome_revision=int(body["visible_outcome_revision"]),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise ApprovalBackendError(
                "publication lineage read returned an unusable body"
            ) from exc
