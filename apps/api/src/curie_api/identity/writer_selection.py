"""Scope classification for approval writers that do not run public creation.

Publication and remediation approvals are inserted by internal API code, not by
``POST /approvals``. They classify the row they are about to insert with the
same owner public creation and every stored-row reader use, so a writer cannot
disagree with them about which scope is HTTP-selected.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import identity_fence
from curie_api.models import Approval

from .http_approval import approval_is_selected


async def http_selected_scope(session: AsyncSession, approval: Approval) -> bool:
    """Whether the unsaved row's agent or any route candidate is HTTP-selected.

    Takes the shared identity lock public creation takes, so a concurrent
    adoption cannot change the answer before this transaction ends. Raises the
    fixed 503 ``authority_unavailable`` when classification cannot be made;
    unavailable is never legacy. @spec HTTP-APPROVAL-CONTINUITY-1.
    """

    await identity_fence.lock_identity_reader(session)
    return await approval_is_selected(session, approval)
