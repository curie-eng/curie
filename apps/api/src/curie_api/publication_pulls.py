"""Adopt or open a publication's pull request, and prove a revision commit, via CodeHost.

These are the decisions the publication worker used to make against GitHub
directly (ADR 0197, "Two ports" item 6). They are forge-neutral: they read and
write only through `CodeHost`, and the internal worker routes in
`curie_api.routers.publication_code_host` call them for one stored publication.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass

from curie_api.forges.errors import Ambiguous, ForgeError
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import (
    Commit,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
)


class PullRequestRefused(Exception):
    """A stable refusal the worker reports; never a code host payload."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PullRequestContract:
    """What the approved publication says its pull request is."""

    branch: str
    base: str
    title: str
    body: str
    draft: bool


def revision_refusal(
    commit: Commit, *, commit_sha: str, revision_id: uuid.UUID, expected_parent: str
) -> str | None:
    """Why ``commit`` is not this revision's marked commit on its expected parent."""

    if commit.sha != commit_sha or f"Curie-Revision: {revision_id}" not in (
        commit.message.splitlines()
    ):
        return "remote commit has no matching revision marker"
    if len(commit.parents) != 1 or commit.parents[0] != expected_parent:
        return "remote revision has the wrong expected parent"
    return None


def _adopt(pull: PullRequest, contract: PullRequestContract, expected_head_sha: str) -> PullRequest:
    if (pull.title, pull.body, pull.head_ref, pull.base_ref) != (
        contract.title,
        contract.body,
        contract.branch,
        contract.base,
    ):
        raise PullRequestRefused(
            "pull_request_mismatch",
            "the pull request does not match the approved publication contract",
        )
    if contract.draft and not pull.draft:
        raise PullRequestRefused(
            "pull_request_mismatch", "the pull request is not the required draft"
        )
    if pull.head_sha != expected_head_sha:
        raise PullRequestRefused(
            "pull_request_mismatch", "the pull request head does not match the expected commit"
        )
    return pull


async def _find(code_host: CodeHost, repository: RepositoryRef, branch: str) -> PullRequest | None:
    try:
        return await code_host.find_pull_request(repository, head_ref=branch)
    except Ambiguous:
        raise PullRequestRefused(
            "multiple_pull_requests",
            "the code host lists more than one pull request from the publication branch",
        ) from None


async def adopt_or_open(
    code_host: CodeHost,
    repository: RepositoryRef,
    contract: PullRequestContract,
    *,
    expected_head_sha: str,
) -> PullRequest | None:
    """The branch's pull request, adopted or opened; None when the branch is absent.

    Any pull request from the branch, in any state, is adopted only when it
    matches the contract and ``expected_head_sha``. Otherwise the branch must
    hold exactly ``expected_head_sha`` before one is opened. A lost or refused
    open is followed by one more read, so a concurrent reconciler's pull
    request is adopted and never doubled; the open's failure is raised only
    when that read finds nothing. More than one pull request from the branch
    is refused, never resolved by picking one.
    """

    existing = await _find(code_host, repository, contract.branch)
    if existing is not None:
        return _adopt(existing, contract, expected_head_sha)
    head = await code_host.branch_head(repository, contract.branch)
    if head is None:
        return None
    if head != expected_head_sha:
        raise PullRequestRefused(
            "branch_moved", "the publication branch no longer matches the expected commit"
        )
    try:
        opened = await code_host.open_pull_request(
            repository,
            head_ref=contract.branch,
            base_ref=contract.base,
            title=contract.title,
            body=contract.body,
            draft=contract.draft,
        )
    except ForgeError as failure:
        recovered = await _find(code_host, repository, contract.branch)
        if recovered is None:
            raise failure from None
        return _adopt(recovered, contract, expected_head_sha)
    return _adopt(opened, contract, expected_head_sha)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


async def update_metadata(
    code_host: CodeHost,
    pull_request: PullRequestRef,
    contract: PullRequestContract,
    *,
    expected_head_sha: str,
    observed_title_sha256: str,
    observed_body_sha256: str,
) -> PullRequest:
    """Apply an approved title and body change to the stored pull request.

    A metadata-only revision pushes nothing, so no publication Job runs: this
    is its whole effect. The pull request must still be the lineage's, on the
    expected head, and unchanged since the approver saw it (its title and body
    hash to the observed digests). A merged or closed pull request is returned
    unchanged so the caller records the lineage terminal. The update's answer
    must carry the code host's change time, which CI freshness is judged after.
    """

    current = await code_host.read_pull_request(pull_request)
    if current.head_ref != contract.branch or current.base_ref != contract.base:
        raise PullRequestRefused(
            "pull_request_mismatch",
            "the pull request does not match the approved publication contract",
        )
    if current.head_sha != expected_head_sha:
        raise PullRequestRefused(
            "pull_request_mismatch", "the pull request head does not match the expected commit"
        )
    if current.state is not PullRequestState.OPEN:
        return current
    if contract.draft and not current.draft:
        raise PullRequestRefused(
            "pull_request_mismatch", "the pull request is not the required draft"
        )
    if (_sha256(current.title), _sha256(current.body)) != (
        observed_title_sha256,
        observed_body_sha256,
    ):
        raise PullRequestRefused(
            "metadata_changed", "pull request metadata changed after publication approval"
        )
    if (current.title, current.body) == (contract.title, contract.body):
        raise PullRequestRefused(
            "metadata_unchanged", "pull request metadata already matches the proposal"
        )
    updated = await code_host.update_pull_request(
        pull_request,
        title=contract.title if current.title != contract.title else None,
        body=contract.body if current.body != contract.body else None,
    )
    if (updated.title, updated.body, updated.head_ref, updated.head_sha) != (
        contract.title,
        contract.body,
        contract.branch,
        expected_head_sha,
    ) or updated.updated_at is None:
        raise PullRequestRefused(
            "metadata_unconfirmed", "the pull request metadata update was not confirmed"
        )
    return updated
