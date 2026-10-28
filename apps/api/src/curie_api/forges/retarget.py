"""Shared invariants for a target-only pull request update."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable

from curie_api.branches import valid_base_branch
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState

_SHA = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")


def require_retargetable(
    pull: PullRequest,
    ref: PullRequestRef,
    *,
    expected_head_sha: str,
    base_ref: str,
    previous: PullRequest | None = None,
) -> None:
    """Refuse an unreadable identity, closed pull, moved source or moved head."""

    if (
        pull.ref != ref
        or pull.state is not PullRequestState.OPEN
        or not valid_base_branch(base_ref)
        or not valid_base_branch(pull.head_ref)
        or not valid_base_branch(pull.base_ref)
        or _SHA.fullmatch(expected_head_sha) is None
        or _SHA.fullmatch(pull.head_sha) is None
        or pull.head_sha.lower() != expected_head_sha.lower()
    ):
        raise Unavailable("retarget_precondition")
    if previous is not None and (pull.head_ref != previous.head_ref or pull.base_ref != base_ref):
        raise Unavailable("retarget_changed")


async def retarget_checked(
    ref: PullRequestRef,
    *,
    base_ref: str,
    expected_head_sha: str,
    read: Callable[[], Awaitable[PullRequest]],
    update: Callable[[], Awaitable[PullRequest]],
) -> PullRequest:
    """Read, update and re-read the same open pull request at one expected head."""

    before = await read()
    require_retargetable(before, ref, expected_head_sha=expected_head_sha, base_ref=base_ref)
    if before.base_ref == base_ref:
        return before
    updated = await update()
    require_retargetable(
        updated, ref, expected_head_sha=expected_head_sha, base_ref=base_ref, previous=before
    )
    after = await read()
    require_retargetable(
        after, ref, expected_head_sha=expected_head_sha, base_ref=base_ref, previous=before
    )
    return after
