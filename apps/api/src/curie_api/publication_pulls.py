"""Adopt or open a publication's pull request, and prove a revision commit, via CodeHost.

These are the decisions the publication worker used to make against GitHub
directly (ADR 0197, "Two ports" item 6). They are forge-neutral: they read and
write only through `CodeHost`, and the internal worker routes in
`curie_api.routers.publication_code_host` call them for one stored publication.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from curie_api.forges.errors import Ambiguous, ForgeError
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import Commit, PullRequest, RepositoryRef


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
