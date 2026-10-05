"""Who may have review feedback acted on (ADR 0197, "Authority" item 3)."""

from __future__ import annotations

from collections.abc import Collection

from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import Actor, RepositoryRef, ReviewFeedback


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
