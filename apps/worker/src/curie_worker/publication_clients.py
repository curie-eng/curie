"""Worker-authenticated API clients for publication recovery."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Literal, cast
from urllib.parse import quote, urlsplit

import httpx

from .publication_k8s import HEADER_FORMS, HeaderForm, clean_origin, valid_repository_path
from .publication_loop import (
    PublicationCredential,
    PublicationIdentityUnavailable,
    PublicationLineageRefused,
    PublicationPullState,
    PublicationReconcileError,
    PublicationRemoteTerminalError,
    PublicationTranscriptPermanentError,
)


class PublicationTranscriptClient:
    """Expose a publication result with marker-based retry recovery."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
    ) -> None:
        if not api_key:
            raise ValueError("publication transcript recording requires platform auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key}
        self._client = client

    async def record_result(
        self,
        agent_id: uuid.UUID,
        workspace_conversation_id: str,
        publication_id: uuid.UUID,
        text: str,
    ) -> None:
        key = quote(workspace_conversation_id, safe="")
        url = f"{self._base}/agents/{agent_id}/state/transcript/{key}"
        marker = str(publication_id)
        item = {
            "user": "Platform publication outcome",
            "assistant": text,
            "ts": datetime.now(UTC).isoformat(),
            "publication_id": marker,
        }
        if await self._has_marker(url, marker):
            return
        try:
            appended = await self._client.post(
                f"{url}/append",
                headers=self._headers,
                json={"item": item},
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            # The atomic append may have committed before its response was
            # lost. Recover once now; the durable outbox's next attempt repeats
            # the same marker preflight if this lookup also cannot prove it.
            try:
                recovered = await self._has_marker(url, marker)
            except PublicationReconcileError as recovery_exc:
                raise PublicationReconcileError(
                    "publication transcript append outcome could not be recovered"
                ) from recovery_exc
            if recovered:
                return
            raise PublicationReconcileError(
                "publication transcript append was unreachable"
            ) from exc
        if appended.status_code == 200:
            return
        if appended.status_code == 413:
            raise PublicationTranscriptPermanentError(
                "publication transcript append exceeded durable state capacity"
            )
        raise PublicationReconcileError(
            f"publication transcript append returned HTTP {appended.status_code}"
        )

    async def _has_marker(self, url: str, marker: str) -> bool:
        try:
            current = await self._client.get(
                url,
                headers=self._headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise PublicationReconcileError(
                "publication transcript lookup was unreachable"
            ) from exc
        if current.status_code == 404:
            return False
        if current.status_code != 200:
            raise PublicationReconcileError(
                f"publication transcript lookup returned HTTP {current.status_code}"
            )
        try:
            value = current.json()["value"]
        except (KeyError, TypeError, ValueError) as exc:
            raise PublicationReconcileError("publication transcript response was unusable") from exc
        if not isinstance(value, list):
            raise PublicationReconcileError("publication transcript is not an append-only log")
        return any(
            isinstance(existing, dict) and existing.get("publication_id") == marker
            for existing in value
        )


class PublicationCredentialClient:
    """Redeem write auth only for the approved server-derived publication."""

    def __init__(
        self,
        *,
        api_base_url: str,
        worker_token: str,
        client: httpx.AsyncClient,
    ) -> None:
        if not worker_token:
            raise ValueError("publication credentials require internal worker auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client

    async def redeem(self, publication_id: uuid.UUID) -> PublicationCredential:
        try:
            response = await self._client.post(
                f"{self._base}/v1/internal/publications/{publication_id}/credential",
                headers=self._headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise PublicationReconcileError(
                "publication credential endpoint is unreachable"
            ) from exc
        if response.status_code != 200:
            raise PublicationReconcileError(
                f"publication credential redemption returned HTTP {response.status_code}"
            )
        if "no-store" not in response.headers.get("Cache-Control", "").lower():
            raise PublicationReconcileError(
                "publication credential response omitted Cache-Control: no-store"
            )
        try:
            body = response.json()
            repo = str(body["repo_full_name"])
            clone_url = str(body["clone_url"])
            authorization = str(body["authorization_header"])
            origin = str(body["origin"])
            header_form = str(body["header_form"])
            ca_bundle_ref = body.get("ca_bundle_ref")
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise PublicationReconcileError("publication credential response was unusable") from exc
        # The origin, path and header form are data from the API (ADR 0197);
        # the worker checks only that they agree and carry no credential.
        if (
            not clean_origin(origin)
            or not valid_repository_path(repo)
            or urlsplit(clone_url).username is not None
            or clone_url != f"{origin}/{repo}.git"
        ):
            raise PublicationReconcileError(
                "publication credential response carried a non-canonical clone URL"
            )
        if header_form not in HEADER_FORMS:
            raise PublicationReconcileError(
                "publication credential response named an unknown header form"
            )
        if ca_bundle_ref is not None and (
            not isinstance(ca_bundle_ref, str) or not ca_bundle_ref.startswith("/")
        ):
            raise PublicationReconcileError(
                "publication credential response carried an invalid CA bundle reference"
            )
        if not authorization or any(char in authorization for char in ("\r", "\n", "\0")):
            raise PublicationReconcileError(
                "publication credential response carried invalid authorization"
            )
        return PublicationCredential(
            clean_clone_url=clone_url,
            authorization_header=authorization,
            origin=origin,
            header_form=cast(HeaderForm, header_form),
            ca_bundle_ref=ca_bundle_ref,
        )


class PublicationLineageClient:
    """Ask the API to verify GitHub identity and advance a published lineage."""

    def __init__(
        self,
        *,
        api_base_url: str,
        worker_token: str,
        client: httpx.AsyncClient,
    ) -> None:
        if not worker_token:
            raise ValueError("publication lineage requires internal worker auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client

    async def advance(
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
    ) -> None:
        try:
            response = await self._client.patch(
                f"{self._base}/v1/internal/publications/{publication_id}/lineage",
                headers=self._headers,
                json={
                    "expected_version": expected_version,
                    "expected_head_sha": expected_head_sha,
                    "expected_publication_version": expected_publication_version,
                    "lease_owner": lease_owner,
                    "state": "open",
                    "pr_number": pr_number,
                    "pr_url": pr_url,
                    "head_sha": head_sha,
                    "metadata_updated_at": (
                        metadata_updated_at.isoformat() if metadata_updated_at is not None else None
                    ),
                },
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise PublicationIdentityUnavailable(
                "publication lineage endpoint is unreachable"
            ) from exc
        if response.status_code == 409:
            terminal = self._remote_terminal_state(response)
            if terminal is not None:
                raise PublicationRemoteTerminalError(terminal)
            raise PublicationLineageRefused(
                f"publication lineage advance was refused: {response.text[:500]}"
            )
        if response.status_code == 503:
            raise PublicationIdentityUnavailable(
                "publication lineage verification is temporarily unavailable"
            )
        if response.status_code != 200:
            raise PublicationReconcileError(
                f"publication lineage advance returned HTTP {response.status_code}"
            )

    @staticmethod
    def _remote_terminal_state(
        response: httpx.Response,
    ) -> Literal["merged", "closed"] | None:
        try:
            detail = response.json().get("detail")
        except ValueError:
            return None
        if not isinstance(detail, dict):
            return None
        if detail.get("code") != "publication.lineage_terminal":
            return None
        observed = detail.get("observed_state")
        if observed not in {"merged", "closed"}:
            return None
        return cast(Literal["merged", "closed"], observed)


class PublicationCodeHostClient:
    """Read and open a publication's pull request through the API (ADR 0197, item 6).

    The worker holds no forge code. Each call names one stored publication;
    the API derives the repository, branch and contract from that row, acts
    through its code host, and returns the facts as data.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        worker_token: str,
        client: httpx.AsyncClient,
    ) -> None:
        if not worker_token:
            raise ValueError("publication code host calls require internal worker auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client

    def _url(self, publication_id: uuid.UUID, suffix: str) -> str:
        return f"{self._base}/v1/internal/publications/{publication_id}/{suffix}"

    async def _call(
        self,
        method: str,
        url: str,
        what: str,
        *,
        params: dict[str, int] | None = None,
        body: dict[str, object] | None = None,
    ) -> httpx.Response:
        try:
            response = await self._client.request(
                method,
                url,
                params=params,
                json=body,
                headers=self._headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise PublicationReconcileError(f"{what} was unreachable") from exc
        if response.status_code in (200, 204):
            return response
        message = _refusal_message(response)
        raise PublicationReconcileError(
            f"{what} returned HTTP {response.status_code}" + (f": {message}" if message else "")
        )

    async def read_pull_request(
        self, publication_id: uuid.UUID, pr_number: int
    ) -> PublicationPullState:
        if pr_number <= 0:
            raise PublicationReconcileError("stored pull request lookup is invalid")
        response = await self._call(
            "GET",
            self._url(publication_id, "pull-request"),
            "stored pull request lookup",
            params={"pr_number": pr_number},
        )
        pull = _pull_state(response)
        if pull.number != pr_number:
            raise PublicationReconcileError("the API returned the wrong stored pull request")
        return pull

    async def read_branch_head(self, publication_id: uuid.UUID) -> str | None:
        """The deterministic branch head, or None when the branch does not exist."""

        response = await self._call(
            "GET", self._url(publication_id, "branch-head"), "branch lookup"
        )
        try:
            head_sha = response.json()["head_sha"]
        except (KeyError, TypeError, ValueError) as exc:
            raise PublicationReconcileError("branch lookup response was unusable") from exc
        if head_sha is None:
            return None
        if not isinstance(head_sha, str) or _SHA.fullmatch(head_sha) is None:
            raise PublicationReconcileError("branch lookup returned an invalid branch head")
        return head_sha

    async def verify_revision_commit(
        self,
        publication_id: uuid.UUID,
        commit_sha: str,
        *,
        revision_id: uuid.UUID,
        expected_parent: str,
    ) -> str:
        response = await self._call(
            "POST",
            self._url(publication_id, "revision-commit"),
            "revision verification",
            body={
                "commit_sha": commit_sha,
                "revision_id": str(revision_id),
                "expected_parent": expected_parent,
            },
        )
        try:
            observed = response.json()["commit_sha"]
        except (KeyError, TypeError, ValueError) as exc:
            raise PublicationReconcileError("revision verification response was unusable") from exc
        if observed != commit_sha:
            raise PublicationReconcileError("revision verification returned a different commit")
        return commit_sha

    async def update_pull_request_metadata(self, publication_id: uuid.UUID) -> PublicationPullState:
        """Apply a metadata-only revision to its stored pull request, through the API."""

        response = await self._call(
            "POST",
            self._url(publication_id, "pull-request/metadata"),
            "pull request metadata update",
        )
        return _pull_state(response)

    async def recover_pull_request(
        self, publication_id: uuid.UUID, *, expected_head_sha: str
    ) -> PublicationPullState | None:
        """Adopt the branch's pull request, or open it when the branch exists."""

        if _SHA.fullmatch(expected_head_sha) is None:
            raise PublicationReconcileError(
                "deterministic-head recovery expected commit is invalid"
            )
        response = await self._call(
            "POST",
            self._url(publication_id, "pull-request"),
            "deterministic-head recovery",
            body={"expected_head_sha": expected_head_sha},
        )
        if response.status_code == 204:
            return None
        pull = _pull_state(response)
        if pull.head_sha != expected_head_sha:
            raise PublicationReconcileError(
                "recovered pull request head does not match the expected commit"
            )
        return pull


_SHA = re.compile(r"[0-9a-f]{40,64}")


def _refusal_message(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail")
    except (AttributeError, ValueError):
        return ""
    if isinstance(detail, dict) and isinstance(detail.get("message"), str):
        return str(detail["message"])[:500]
    return detail[:500] if isinstance(detail, str) else ""


def _pull_state(response: httpx.Response) -> PublicationPullState:
    """A pull request the API returned, checked for shape, never trusted for identity."""

    try:
        row = response.json()
        number = row["number"]
        url = row["url"]
        state = row["state"]
        head_sha = row["head_sha"]
        head_ref = row["head_ref"]
        raw_updated_at = row.get("updated_at")
        updated_at = datetime.fromisoformat(raw_updated_at) if raw_updated_at is not None else None
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise PublicationReconcileError("the API returned an invalid pull request") from exc
    parsed = urlsplit(url) if isinstance(url, str) else None
    if (
        isinstance(number, bool)
        or not isinstance(number, int)
        or number <= 0
        or parsed is None
        or parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or state not in {"open", "closed", "merged"}
        or not isinstance(head_sha, str)
        or _SHA.fullmatch(head_sha) is None
        or not isinstance(head_ref, str)
        or not head_ref
        or (updated_at is not None and updated_at.tzinfo is None)
    ):
        raise PublicationReconcileError("the API returned an invalid pull request")
    return PublicationPullState(
        number=number,
        url=url,
        state=cast(Literal["open", "closed", "merged"], state),
        head_sha=head_sha,
        head_ref=head_ref,
        updated_at=updated_at,
    )
