"""Fail-closed rediscovery of an issue's earlier factory publications (#4629).

A reinstall, or any loss of factory storage, leaves still-labelled issues whose
pull or merge requests the code host still has. Admission with no local
WorkItem, and publication right before it opens a new pull request, ask the
code host for the publications its own identity opened from factory
publication branches (`CodeHost.list_factory_publications`) and decide from
them alone. A tracker state label is never evidence.

A listed publication matches the issue when:

1. a line of its body is exactly `issue_marker(issue)`, the canonical marker
   publication bodies carry; or
2. its body carries no ``curie-factory-issue`` marker at all (a body written
   before the marker) and one of the scope-qualified references the tracker
   names (`Tracker.publication_references`) occurs in it, not preceded by
   ``[\\w/.-]`` and not followed by ``[\\w-]``.

A publication whose ownership the code host could not verify
(``owner_verified`` False) is never evidence of the factory's own work. The
verdict, in order, from verified publications: more than one open match is
``ambiguous``; exactly one is ``open``. Then, on Bitbucket Cloud only, whose
token cannot read its own account so nothing it lists is ever verified (#4747),
any unverified open match is ``unconfirmed``: admission fails closed and asks
the operator whether that pull request is the factory's, rather than trust a
lookalike branch or open a duplicate. Every other code host ignores
unverified publications. Then a verified completed (merged or closed) match
whose completion time is unknown, or that the label event did not strictly
follow (an unknown event time included), is ``completed``, the replay of the
trigger that already ran; otherwise ``clear``, which includes a verified label
add after every completion (an intentional rerun). An unverified completed
match is ignored everywhere. Any `ForgeError` propagates; callers defer on it.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import httpx

from .config import Settings
from .forges.hosts import code_host_for, forge_code_host
from .forges.ports import CodeHost, Tracker
from .forges.types import (
    BITBUCKET_CLOUD,
    GITHUB,
    FactoryPublication,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)

logger = logging.getLogger(__name__)

# The marked status comment that names an issue's existing publication.
EXISTING_PUBLICATION_MARKER = "curie-factory-existing-publication"

_MARKER_TAG = "curie-factory-issue"

Verdict = Literal["clear", "open", "ambiguous", "unconfirmed", "completed"]

# Verdicts that refuse a new run or a new pull request and are reported.
REFUSING: frozenset[str] = frozenset({"open", "ambiguous", "unconfirmed"})


@dataclass(frozen=True)
class Rediscovery:
    """What the code host says about the issue's earlier publications.

    ``publications`` are the matching ones the verdict rests on: the verified
    open matches for ``open`` and ``ambiguous``, the unverified open matches
    on Bitbucket Cloud for ``unconfirmed``, the suppressing verified completed
    ones for ``completed``, none for ``clear``.
    """

    verdict: Verdict
    publications: tuple[FactoryPublication, ...]


def issue_marker(issue: TrackerIssueRef) -> str:
    """The canonical marker line naming ``issue`` by its immutable identity.

    The display key (a Jira key) is not part of it, so a moved issue keeps it.
    """

    return (
        f"<!-- {_MARKER_TAG} kind={issue.kind} host={issue.host} "
        f"scope={issue.scope_id} issue={issue.issue_id} -->"
    )


def publishing_code_host(
    settings: Settings, client: httpx.AsyncClient, repository: RepositoryRef
) -> CodeHost:
    """The code host that opens publications on ``repository``.

    The install's own GitHub for a GitHub repository (`hosts.code_host_for`),
    else the forge entry the repository names (`hosts.forge_code_host`). The
    publication routes and rediscovery both use it, so the identity that lists
    publications is the one that opened them.
    """

    if repository.kind == GITHUB:
        return code_host_for(settings, client)
    return forge_code_host(settings, client, repository)


def _bounded(reference: str, body: str) -> bool:
    pattern = rf"(?<![\w/.-]){re.escape(reference)}(?![\w-])"
    return re.search(pattern, body) is not None


def _matches(publication: FactoryPublication, marker: str, references: Sequence[str]) -> bool:
    body = publication.body
    if any(line.strip() == marker for line in body.splitlines()):
        return True
    if _MARKER_TAG in body:
        # A marked body names its issue by the marker alone.
        return False
    return any(reference and _bounded(reference, body) for reference in references)


def _suppresses(publication: FactoryPublication, event_at: datetime | None) -> bool:
    closed_at = publication.closed_at
    return closed_at is None or event_at is None or event_at <= closed_at


async def rediscover(
    tracker: Tracker,
    code_host: CodeHost,
    issue: TrackerIssueRef,
    repository: RepositoryRef,
    *,
    event_at: datetime | None,
    exclude_branch: str | None = None,
) -> Rediscovery:
    """The verdict on ``issue``'s earlier publications in ``repository``.

    ``event_at`` is when the admitting label was added (None when unknown).
    ``exclude_branch`` drops the caller's own publication branch.
    """

    listed = await code_host.list_factory_publications(repository)
    references = await tracker.publication_references(issue, repository)
    marker = issue_marker(issue)
    every_match = [
        publication
        for publication in listed
        if publication.head_ref != exclude_branch and _matches(publication, marker, references)
    ]
    matched = [p for p in every_match if p.owner_verified]
    opened = [p for p in matched if p.state is PullRequestState.OPEN]
    if len(opened) > 1:
        return Rediscovery("ambiguous", tuple(opened))
    if opened:
        return Rediscovery("open", tuple(opened))
    if repository.kind == BITBUCKET_CLOUD:
        # Never proof of ownership, but never a reason to open a second pull
        # request either: the operator confirms which it is (#4747).
        unconfirmed = [
            p for p in every_match if not p.owner_verified and p.state is PullRequestState.OPEN
        ]
        if unconfirmed:
            return Rediscovery("unconfirmed", tuple(unconfirmed))
    elif any(not p.owner_verified for p in listed):
        logger.info(
            "factory rediscovery: could not verify ownership on the code host for %s, "
            "so unverified publications were ignored",
            repository.path,
        )
    completed = [
        p for p in matched if p.state is not PullRequestState.OPEN and _suppresses(p, event_at)
    ]
    if completed:
        return Rediscovery("completed", tuple(completed))
    return Rediscovery("clear", ())


def _report_body(found: Rediscovery) -> str:
    # Only the adapter-validated URL is echoed, inside angle brackets on its
    # own numbered line; titles and bodies never are.
    links = "\n".join(f"{index}. <{p.url}>" for index, p in enumerate(found.publications, 1))
    if found.verdict == "unconfirmed":
        return (
            "Curie did not start a run for this issue: an open pull request from a "
            "factory publication branch names it, and Bitbucket Cloud does not let "
            "Curie verify who opened it. Please confirm whether it is the factory's "
            "pull request. To confirm it is, remove the trigger label from this issue "
            "and review or merge the pull request as usual: Curie stops trying to "
            "start the issue. Curie does not count a merged or declined pull request "
            "here, so an issue that keeps the label would run again. If it is not the "
            "factory's, close it: Curie admits the issue on a later intake pass while "
            f"it keeps the label.\n\n{links}"
        )
    if found.verdict == "open":
        return (
            "Curie did not start another run for this issue: its factory pull request is "
            f"still open.\n\n{links}"
        )
    return (
        "Curie did not start another run for this issue: more than one open factory "
        "pull request names it, and Curie refuses to choose between them. Close all "
        f"but one to continue.\n\n{links}"
    )


async def report(tracker: Tracker, issue: TrackerIssueRef, found: Rediscovery) -> None:
    """Keep one marked status comment on the issue naming what refused it.

    Written for ``open``, ``ambiguous`` and ``unconfirmed`` (which asks the
    operator to confirm the pull request is the factory's), all under one
    marker, so repeated passes keep one comment; ``completed`` is only logged
    and ``clear`` writes nothing. A `ForgeError` propagates.
    """

    if found.verdict == "completed":
        logger.info(
            "factory rediscovery: issue %s/%s already has a completed publication; "
            "the old trigger is not run again",
            issue.kind,
            issue.issue_id,
        )
        return
    if found.verdict not in REFUSING:
        return
    await tracker.marked_comments.upsert_marked(
        ReplyTarget.on_issue(issue), EXISTING_PUBLICATION_MARKER, _report_body(found)
    )
