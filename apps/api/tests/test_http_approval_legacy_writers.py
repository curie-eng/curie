"""Legacy approval writers cannot mutate HTTP-selected rows around their seal.

Actual ASGI routes, sweeper, reconciler, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-1/5/10.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from curie_api import approval_principal
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_api.sweeper import sweep_expired_approvals
from role_policy_support import run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    opened,
    post,
    rows,
)
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

RECOVERY_ENV = "CURIE_APPROVAL_RECOVERY_ENABLED"


def created(case: SimpleNamespace) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3: a real selected row through the private owner."""
    accepted(post(case), case.request)
    found = rows(case.request["approval"]["dedupe_key"])
    assert len(found) == 1
    return found[0]


def audits(approval_id: uuid.UUID) -> list[dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-10: observe the committed audit log."""
    found: list[dict[str, Any]] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            result = await session.execute(
                text(f'SELECT action, actor FROM "{SCHEMA}".approval_audit_entries '
                     "WHERE approval_id = :id ORDER BY created_at"),
                {"id": approval_id},
            )
            found.extend(dict(row) for row in result.mappings())

    run(read)
    return found


def lapse(case: SimpleNamespace, row: dict[str, Any]) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-10: an owned fixture of a validly resealed lapsed row."""
    module, ring, capsule = opened(case, row)
    lapsed = {**row, "expires_at": datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=5)}
    data = capsule.model_dump(mode="json")
    data["approval_record_sha256"] = digest(canonical({key: lapsed[key] for key in RECORD_FIELDS}))
    token = module.seal_approval_context(module.ApprovalContextV1.model_validate(data), ring)

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            await session.execute(
                text(f'UPDATE "{SCHEMA}".approvals SET expires_at=:at, identity_context=:token '
                     "WHERE id=:id"),
                {"at": lapsed["expires_at"], "token": token, "id": row["id"]},
            )
            await session.commit()

    run(write)
    return rows(row["dedupe_key"])[0]


def assert_expired_and_resealed(case: SimpleNamespace, dedupe: str) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-5/10: status, digest and null actor seal together."""
    row = rows(dedupe)[0]
    assert row["status"] == "expired" and row["resolved_by"] is None
    _, _, capsule = opened(case, row)
    assert capsule.status == "expired"
    assert capsule.approval_record_sha256 == digest(
        canonical({key: row[key] for key in RECORD_FIELDS})
    )
    assert all(
        getattr(capsule, name) is None
        for name in type(capsule).model_fields if name.startswith("actor_")
    )
    assert [entry["action"] for entry in audits(row["id"])] == ["expired"]
    return row


def chat_headers(approval_id: uuid.UUID, channel: str) -> dict[str, str]:
    """@spec HTTP-APPROVAL-CONTINUITY-10: the existing attested chat principal alone."""
    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret, subject="U0EXAMPLE1", kind="chat",
        actor_channel=channel, approval_id=str(approval_id),
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
    )
    return {"X-API-Key": get_settings().api_key, "X-Curie-Approval-Principal": token}


def operator_headers() -> dict[str, str]:
    """@spec HTTP-APPROVAL-CONTINUITY-10: platform key plus an operator principal."""
    token = approval_principal.mint(
        get_settings().api_key, subject="U0OPERATOR1", kind="operator",
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
    )
    return {"X-API-Key": get_settings().api_key, "X-Curie-Approval-Principal": token}


def sweep(case: SimpleNamespace, **kwargs: Any) -> int:
    """@spec HTTP-APPROVAL-CONTINUITY-10: one real sweeper pass with the app's own stores."""
    state = case.client.app.state
    result: list[int] = []

    async def go(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            result.append(await sweep_expired_approvals(
                session, state.resume_queue,
                now=datetime.now(UTC).replace(tzinfo=None) + timedelta(days=1), **kwargs,
            ))

    run(go)
    return result[0]


def test_legacy_resolve_refuses_a_pending_selected_row_before_authorization(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: no legacy admission, team lookup, CAS or audit."""
    case = creation_case
    row = created(case)
    for decision in ("approved", "rejected"):
        response = case.client.post(
            f"/approvals/{row['id']}/resolve", json={"decision": decision},
            headers=chat_headers(row["id"], row["card_channel"]),
        )
        assert response.status_code == 409, response.text
        assert response.json() == {"detail": "unsupported_approval"}
    assert rows(row["dedupe_key"]) == [row]
    assert audits(row["id"]) == []
    assert len(case.provider.requests) == 2


def test_resolve_time_expiry_reseals_a_lapsed_selected_row_without_an_actor(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: preserved 410, sealed with the expiry CAS."""
    case = creation_case
    row = lapse(case, created(case))
    response = case.client.post(
        f"/approvals/{row['id']}/resolve", json={"decision": "approved"},
        headers=chat_headers(row["id"], row["card_channel"]),
    )
    assert response.status_code == 410, response.text
    expired = assert_expired_and_resealed(case, row["dedupe_key"])
    assert expired["resumed_at"] is None


def test_sweeper_expires_a_selected_row_through_the_sealing_owner(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: the background writer reseals too."""
    case = creation_case
    row = lapse(case, created(case))
    sweep(case, http_expiry=case.client.app.state.http_approval_expiry)
    expired = assert_expired_and_resealed(case, row["dedupe_key"])
    assert expired["resumed_at"] is None


def test_sweeper_without_the_sealing_owner_leaves_a_selected_row_pending(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: the legacy writer never expires an adopted row."""
    case = creation_case
    row = lapse(case, created(case))
    sweep(case)
    assert rows(row["dedupe_key"]) == [row]
    assert audits(row["id"]) == []


def test_adopted_row_without_provenance_is_never_legacy_expired(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/10: null provenance is not legacy authority."""
    case = creation_case
    row = created(case)
    forged = str(uuid.uuid4())

    async def insert(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: an out-of-band null row on the adopted route."""
        async with factory() as session:
            await session.execute(
                text(f'INSERT INTO "{SCHEMA}".approvals (id, agent_id, conversation_id, author, '
                     "summary, reply_kind, reply_channel, reply_adapter, reply_placeholder, "
                     "dedupe_key, status, expires_at, card_channel) "
                     "SELECT :id, agent_id, conversation_id, author, summary, reply_kind, "
                     "reply_channel, reply_adapter, reply_placeholder, :dedupe, 'pending', "
                     f':at, card_channel FROM "{SCHEMA}".approvals WHERE id = :source'),
                {"id": forged, "dedupe": uuid.uuid4().hex, "source": row["id"],
                 "at": datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=5)},
            )
            await session.commit()

    run(insert)
    sweep(case, http_expiry=case.client.app.state.http_approval_expiry)
    sweep(case)
    response = case.client.post(
        f"/approvals/{forged}/resolve", json={"decision": "approved"},
        headers=chat_headers(uuid.UUID(forged), row["card_channel"]),
    )
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "unsupported_approval"}

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            status = (await session.execute(
                text(f'SELECT status FROM "{SCHEMA}".approvals WHERE id = :id'), {"id": forged},
            )).scalar_one()
            assert status == "pending"

    run(read)
    assert audits(uuid.UUID(forged)) == []


def test_recovery_refuses_a_selected_row_before_mutation(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: no unqualified recovery actor for selected rows."""
    case = creation_case
    row = created(case)
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{row['id']}/recover",
            json={"disposition": "rejected", "reason": "synthetic stranded approval",
                  "recovery_key": f"rk-{uuid.uuid4().hex}"},
            headers=operator_headers(),
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "unsupported_approval"}
    assert rows(row["dedupe_key"]) == [row]
    assert audits(row["id"]) == []


def test_reconciler_keeps_a_selected_wake_owed_instead_of_publishing_it(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/10: no context-free resume for a selected row."""
    case = creation_case
    row = lapse(case, created(case))
    sweep(case, http_expiry=case.client.app.state.http_approval_expiry)
    assert_expired_and_resealed(case, row["dedupe_key"])
    state = case.client.app.state
    queue: ResumeQueue = state.resume_queue

    async def reconcile(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: a pass past the grace horizon."""
        reconciler = ResumeReconciler(
            factory, queue, interval_seconds=1, grace_seconds=0, batch_limit=10,
        )
        await asyncio.sleep(0.01)
        assert await reconciler.reconcile_once() == 0

    run(reconcile)
    assert rows(row["dedupe_key"])[0]["resumed_at"] is None
