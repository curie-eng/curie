"""Authoritative base branches and remote commit proofs for publication."""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .branches import valid_base_branch
from .forges.errors import ForgeError, NotFound, Unauthorized, Unavailable
from .forges.hosts import resolve_stored
from .forges.ports import CodeHost
from .forges.types import Commit, RepositoryRef
from .models import ExecutionRequest, Publication, ThreadPublicationLineage, WorkItem
from .publication_pulls import PullRequestRefused, revision_refusal

_COMMIT_SHA = re.compile(r"[0-9a-f]{40,64}")
_ANCESTRY_READ_LIMIT = 1024
_ANCESTRY_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class VerifiedPublicationCommit:
    """Remote proof bound to the stored approval and lineage snapshot."""

    publication_id: uuid.UUID
    publication_version: int
    lineage_id: uuid.UUID
    lineage_version: int
    approved_base_sha: str
    expected_prior_head: str
    commit_sha: str
    parent_sha: str
    base_branch: str
    first_publication: bool


def forge_failure(exc: ForgeError) -> HTTPException:
    """A fixed, payload-free answer for a code host failure."""

    reason = str(exc) or type(exc).__name__
    if isinstance(exc, Unauthorized):
        code, status_code = "code_host_credential_refused", status.HTTP_502_BAD_GATEWAY
    elif isinstance(exc, NotFound):
        code, status_code = "code_host_not_found", status.HTTP_404_NOT_FOUND
    elif isinstance(exc, Unavailable):
        code, status_code = "code_host_unavailable", status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        code, status_code = "code_host_refused", status.HTTP_502_BAD_GATEWAY
    return HTTPException(
        status_code,
        {"code": f"publication.{code}", "message": f"the code host answered {reason}"},
        headers={"Cache-Control": "no-store"},
    )


def _valid_base_ref(branch: str | None) -> bool:
    return (
        valid_base_branch(branch)
        and isinstance(branch, str)
        and len(branch) <= 1024
        and not branch.startswith(("refs/", "/"))
        and not branch.endswith((".", "/"))
        and branch != "@"
        and "@{" not in branch
        and "//" not in branch
        and re.fullmatch(r"[0-9a-fA-F]{40,64}", branch) is None
        and not any(ord(char) < 32 or ord(char) == 127 or char in "~^:?*[\\" for char in branch)
        and all(
            not part.startswith(".") and not part.endswith(".lock") for part in branch.split("/")
        )
    )


def stored_publication_base_ref(
    publication: Publication,
    lineage: ThreadPublicationLineage | None,
    execution_request: ExecutionRequest | None,
    work_item: WorkItem | None,
) -> str | None:
    """Classify persisted authority; None requires server-side repository resolution."""

    if lineage is not None and lineage.base_ref is not None:
        if not _valid_base_ref(lineage.base_ref):
            raise Unavailable("publication_base_branch_unproved")
        return lineage.base_ref
    if publication.execution_request_id is None:
        return None
    if (
        execution_request is None
        or work_item is None
        or execution_request.id != publication.execution_request_id
        or execution_request.work_item_id != work_item.id
        or lineage is None
        or lineage.id != publication.lineage_id
    ):
        raise Unavailable("publication_base_branch_unproved")
    if (
        work_item.base_branch is None
        and work_item.base_source is None
        and work_item.base_commit is None
    ):
        return None
    if (
        not _valid_base_ref(work_item.base_branch)
        or work_item.base_source not in ("label", "default")
        or not isinstance(work_item.base_commit, str)
        or _COMMIT_SHA.fullmatch(work_item.base_commit) is None
    ):
        raise Unavailable("publication_base_branch_unproved")
    return work_item.base_branch


async def publication_base_ref(
    session: AsyncSession,
    code_host: CodeHost,
    publication: Publication,
    repository: RepositoryRef,
) -> str:
    """Resolve the verified lineage base, recorded base, or authoritative repository default."""

    lineage = publication.lineage
    execution_request: ExecutionRequest | None = None
    work_item: WorkItem | None = None
    if (
        lineage is None or lineage.base_ref is None
    ) and publication.execution_request_id is not None:
        result = await session.execute(
            select(ExecutionRequest, WorkItem)
            .join(WorkItem, ExecutionRequest.work_item_id == WorkItem.id)
            .where(ExecutionRequest.id == publication.execution_request_id)
            .execution_options(populate_existing=True)
        )
        row = result.one_or_none()
        if row is not None:
            execution_request, work_item = row
    branch = stored_publication_base_ref(publication, lineage, execution_request, work_item)
    if branch is None:
        branch = (await resolve_stored(code_host, repository)).default_branch
    if not _valid_base_ref(branch):
        raise Unavailable("publication_base_branch_unproved")
    assert branch is not None
    return branch


def _valid_commit(commit: Commit, sha: str) -> bool:
    return (
        isinstance(commit, Commit)
        and commit.sha == sha
        and _COMMIT_SHA.fullmatch(commit.sha) is not None
        and isinstance(commit.message, str)
        and isinstance(commit.parents, tuple)
        and all(
            isinstance(parent, str) and _COMMIT_SHA.fullmatch(parent) is not None
            for parent in commit.parents
        )
        and sha not in commit.parents
    )


async def verify_publication_commit(
    code_host: CodeHost,
    repository: RepositoryRef,
    publication: Publication,
    *,
    commit_sha: str,
    base_branch: str,
) -> VerifiedPublicationCommit:
    """Prove the exact revision parent, or both first-publication ancestry relations."""

    lineage = publication.lineage
    if lineage is None or lineage.id != publication.lineage_id:
        raise PullRequestRefused("lineage_absent", "publication has no thread lineage")
    if (lineage.head_sha is None) != (lineage.pr_number is None):
        raise PullRequestRefused("lineage_stale", "stored publication lineage is incomplete")
    first_publication = lineage.head_sha is None and lineage.pr_number is None
    expected_prior_head = publication.expected_prior_head
    if (
        _COMMIT_SHA.fullmatch(commit_sha) is None
        or expected_prior_head is None
        or _COMMIT_SHA.fullmatch(expected_prior_head) is None
        or _COMMIT_SHA.fullmatch(publication.base_sha) is None
        or not _valid_base_ref(base_branch)
    ):
        raise PullRequestRefused("revision_mismatch", "publication revision facts are invalid")
    if first_publication and (
        publication.status not in {"approved", "launching", "running"}
        or lineage.status != "open"
        or publication.revision_number != lineage.latest_revision
        or expected_prior_head != publication.base_sha
        or lineage.base_sha != publication.base_sha
    ):
        raise PullRequestRefused("revision_mismatch", "first publication authority is not current")
    snapshot = (
        publication.id,
        publication.version,
        lineage.id,
        lineage.version,
        publication.base_sha,
        expected_prior_head,
    )
    commit = await code_host.read_commit(repository, commit_sha)
    if not _valid_commit(commit, commit_sha) or len(commit.parents) != 1:
        raise PullRequestRefused("revision_mismatch", "remote revision commit is invalid")
    parent_sha = commit.parents[0]
    refusal = revision_refusal(
        commit,
        commit_sha=commit_sha,
        revision_id=publication.id,
        expected_parent=parent_sha if first_publication else expected_prior_head,
    )
    if refusal is not None:
        raise PullRequestRefused("revision_mismatch", refusal)
    if first_publication:
        cache = {commit_sha: commit}
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _ANCESTRY_TIMEOUT_SECONDS

        async def reachable(start: str, target: str) -> bool:
            pending = [start]
            visited: set[str] = set()
            while pending:
                if loop.time() >= deadline:
                    raise Unavailable("publication_ancestry_unproved")
                sha = pending.pop()
                if sha == target:
                    return True
                if sha in visited:
                    continue
                visited.add(sha)
                if sha not in cache:
                    if len(cache) - 1 >= _ANCESTRY_READ_LIMIT:
                        raise Unavailable("publication_ancestry_unproved")
                    ancestor = await code_host.read_commit(repository, sha)
                    if not _valid_commit(ancestor, sha):
                        raise Unavailable("publication_ancestry_unproved")
                    cache[sha] = ancestor
                pending.extend(reversed(cache[sha].parents))
            return False

        try:
            async with asyncio.timeout_at(deadline):
                tip = await code_host.branch_head(repository, base_branch)
                if not isinstance(tip, str) or _COMMIT_SHA.fullmatch(tip) is None:
                    raise Unavailable("publication_ancestry_unproved")
                if not await reachable(parent_sha, snapshot[4]) or not await reachable(
                    tip, parent_sha
                ):
                    raise PullRequestRefused(
                        "base_ancestry_mismatch",
                        "the publication parent is not on the approved base branch history",
                    )
        except (ForgeError, TimeoutError):
            raise Unavailable("publication_ancestry_unproved") from None
    return VerifiedPublicationCommit(
        *snapshot,
        commit_sha=commit_sha,
        parent_sha=parent_sha,
        base_branch=base_branch,
        first_publication=first_publication,
    )
