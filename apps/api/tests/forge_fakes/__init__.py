"""Shared in-process forge fakes and real factory dispatch setup."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from curie_api.workitem_dispatch import FactoryWake


async def factory_wake(session: AsyncSession, request_id: uuid.UUID) -> FactoryWake:
    from curie_api.workitem_dispatch import FactoryWake, load_execute_wake

    loaded = await load_execute_wake(session, request_id)
    assert loaded is not None, request_id
    request, _work_item, binding = loaded
    assert binding is not None, request_id
    assert request.reply_kind is not None
    assert request.reply_address is not None
    assert request.reply_conversation_id is not None
    return FactoryWake(
        kind=request.reply_kind,
        address=request.reply_address,
        adapter=binding.adapter,
        conversation_id=request.reply_conversation_id,
    )
