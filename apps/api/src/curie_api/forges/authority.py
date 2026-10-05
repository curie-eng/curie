"""Who may start a run and whose review feedback is acted on (ADR 0197, "Authority")."""

from __future__ import annotations

from collections.abc import Collection

from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.config import (
    AccountAllowlist,
    BindingConfig,
    GroupAuthority,
    RepositoryBindingConfig,
    StartAuthority,
    WriteCheck,
)
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.types import Actor, RepositoryRef, ReviewFeedback, TrackerIssueRef


async def may_start_with(
    authority: StartAuthority, issue: TrackerIssueRef, actor: Actor, *, tracker: Tracker
) -> bool:
    """Whether ``actor`` may start a run on ``issue`` under a binding's authority.

    A write check defers to the tracker's own ``may_start`` (a native tracker's
    repository write check, authority 1). An allowlist matches the actor's
    immutable account id, never an email or login (alternative 3). A group asks
    the tracker; a tracker that declares no group membership cannot confirm
    anyone, so the start is refused rather than guessed (authority 2).
    Forge errors propagate, so an unanswerable check retries instead of refusing.
    """

    if isinstance(authority, WriteCheck):
        return await tracker.may_start(issue, actor)
    if isinstance(authority, AccountAllowlist):
        return actor.id in authority.accounts
    if isinstance(authority, GroupAuthority):
        if not supports(tracker.capabilities, Operation.GROUP_MEMBERSHIP):
            return False
        return await tracker.in_group(actor, authority.group_id)
    raise TypeError(f"unknown start authority {authority!r}")


def feedback_allowlist(binding: BindingConfig, repo: RepositoryBindingConfig) -> frozenset[str]:
    """The binding's fallback account ids on ``repo``'s code host.

    Account ids are unique only within one code host, so each host has its own list.
    """

    if repo not in binding.repos:
        raise ValueError(f"repository {repo.alias!r} is not on binding {binding.name!r}")
    return binding.review_feedback_allowlist.get(repo.code_host, frozenset())


async def binding_may_act_on_feedback(
    binding: BindingConfig,
    repo: RepositoryBindingConfig,
    code_host: CodeHost,
    repository: RepositoryRef,
    actor: Actor,
) -> bool:
    """`may_act_on_feedback` with ``binding``'s allowlist for ``repo``'s code host."""

    return await may_act_on_feedback(
        code_host, repository, actor, feedback_allowlist(binding, repo)
    )


async def may_act_on_feedback(
    code_host: CodeHost,
    repository: RepositoryRef,
    actor: Actor,
    allowlist: Collection[str],
) -> bool:
    """Whether feedback by ``actor`` may be acted on.

    A code host that reports write access decides alone, and the allowlist is
    not consulted. One that cannot (Bitbucket without an admin token) falls back
    to the binding's allowlist of code-host account ids.
    """

    if supports(code_host.capabilities, Operation.USER_CAN_WRITE):
        return await code_host.user_can_write(repository, actor)
    return actor.id in allowlist


async def feedback_actionable(
    code_host: CodeHost, feedback: ReviewFeedback, allowlist: Collection[str]
) -> bool:
    """The feedback is still current and its author may have it acted on."""

    if not await code_host.verify_feedback(feedback):
        return False
    return await may_act_on_feedback(
        code_host, feedback.pull_request.repository, feedback.author, allowlist
    )
