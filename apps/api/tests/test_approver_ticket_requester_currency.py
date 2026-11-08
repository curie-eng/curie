"""ADR 0271 decision 4: a dynamic creation needs the original requester, current.

@spec REQUEST-APPROVAL-4.

Ordinary HTTP approval admission keeps ADR 0246's migrated raw-account
exception: a sender named by a ``surface_account`` entry of the invocation
role is admitted even when the fresh directory lookup is unresolved or
inactive, or resolves to another canonical human. A creation carrying an
approver ticket never relies on it. Through the real creation route, with a
real ingress capsule, the per-sender synthetic TLS directory, Postgres and
Valkey, each such lookup refuses the ticket and spends nothing, while an
ordinary approval from the same sender and lookup is still admitted and the
unchanged requester still creates. A requester principal disabled after the
caller phase refuses in the final writer, and the next attempt creates.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from approver_ticket_support import (
    OTHER_ROUTE,
    approval_rows,
    carried,
    create,
    dynamic_request,
    enroll,
    exactly,
    ingress,
)
from approver_ticket_support import ticket_case as ticket_case
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.identity import http_approval
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_creation import accepted, rows
from apps.api.tests.test_http_requester_projection import CHANGED_SUBJECT, admit_raw_sender
from apps.api.tests.test_http_requester_projection import synthetic_provider as synthetic_provider

PLAIN: dict[str, Any] = {"target": "acct-example-1"}
# How the directory now answers for the original sender.
LOOKUPS: dict[str, Callable[[SimpleNamespace], None]] = {
    "unresolved": lambda provider: provider.outcomes.update({membership.SENDER: "unresolved"}),
    "inactive": lambda provider: provider.outcomes.update({membership.SENDER: "inactive"}),
    "changed-association": lambda provider: provider.subjects.update(
        {membership.SENDER: CHANGED_SUBJECT},
    ),
}


def raw_enrolled(case: SimpleNamespace) -> None:
    """Admit the sender through the raw-account exception, then register the pair."""
    admit_raw_sender(case.scope)
    enroll(case)


@pytest.mark.parametrize("lookup", sorted(LOOKUPS))
def test_a_ticket_needs_the_original_requester_freshly_resolved(
    ticket_case: SimpleNamespace, lookup: str,
) -> None:
    """@spec REQUEST-APPROVAL-4: the raw-account exception never admits a ticket.

    The ordinary approval from the same capsule and lookup is admitted, so the
    refusal belongs to the ticket rule, not to admission.
    """
    case = ticket_case
    raw_enrolled(case)
    agent = case.scope["agent"]
    LOOKUPS[lookup](case.provider)

    request = dynamic_request(case, carried(case, PLAIN))
    exactly(create(case.client, request), 403, "caller_not_allowed")
    assert approval_rows(agent) == []

    ordinary = dynamic_request(
        case, {}, route=OTHER_ROUTE, tool="synthetic_read", gate_kind="permission",
    )
    accepted(create(case.client, ordinary), ordinary)
    assert [row["selection_mode"] for row in approval_rows(agent)] == ["ordinary"]

    case.provider.outcomes.clear()
    case.provider.subjects.clear()
    owning = ingress(case)
    current = dynamic_request(owning, carried(case, PLAIN))
    accepted(create(case.client, current), current)
    assert sorted(row["selection_mode"] for row in approval_rows(agent)) == [
        "dynamic", "ordinary",
    ]


def test_a_requester_disabled_before_the_writer_refuses_there(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-4: the final writer rereads the requester in its transaction.

    The principal is disabled after the caller phase closed and before the
    writer begins; nothing is spent, and the next attempt, whose fresh lookup
    re-applies the active account, creates.
    """
    case = ticket_case
    enroll(case)
    agent, tenant = case.scope["agent"], case.scope["tenant"]
    disabled: list[str] = []
    original = identity_fence.mutation

    @asynccontextmanager
    async def mutation(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        if not disabled:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with engine.begin() as connection:
                    status = await connection.scalar(text(
                        "UPDATE curie.principals SET status = 'disabled' "
                        "WHERE tenant_id = :tenant AND idp_subject = :subject RETURNING status"
                    ), {"tenant": uuid.UUID(tenant), "subject": membership.SUBJECT})
                    disabled.append(status)
            finally:
                await engine.dispose()
        async with original(*args, **kwargs) as writer:
            yield writer

    request = dynamic_request(case, carried(case, PLAIN))
    with monkeypatch.context() as patched:
        patched.setattr(
            http_approval, "identity_fence",
            SimpleNamespace(**{**vars(identity_fence), "mutation": mutation}),
        )
        exactly(create(case.client, request), 503, "authority_unavailable")
    assert disabled == ["disabled"]
    assert rows(request["approval"]["dedupe_key"]) == []
    assert approval_rows(agent) == []
    accepted(create(case.client, request), request)
    assert [row["selection_mode"] for row in approval_rows(agent)] == ["dynamic"]
