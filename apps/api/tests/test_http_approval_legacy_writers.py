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

from curie_api import approval_principal, identity_fence
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.identity.http_approval import http_expiry_owner
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_api.sweeper import sweep_expired_approvals
from redis import asyncio as redis_async
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


def sweep(case: SimpleNamespace, *, owner: bool) -> int:
    """@spec HTTP-APPROVAL-CONTINUITY-10: one real sweeper pass.

    Its stores and the production owner factory are built on this pass's own event
    loop; the app's pooled clients belong to the TestClient loop.
    """
    runtime = case.client.app.state.http_identity_runtime
    result: list[int] = []

    async def go(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        try:
            stores = SimpleNamespace(
                sessionmaker=factory, valkey=valkey, http_identity_runtime=runtime,
            )
            async with factory() as session:
                result.append(await sweep_expired_approvals(
                    session, ResumeQueue(valkey),
                    now=datetime.now(UTC).replace(tzinfo=None) + timedelta(days=1),
                    http_expiry=http_expiry_owner(stores) if owner else None,
                ))
        finally:
            await valkey.aclose()

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
        assert response.status_code == 403, response.text
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
    sweep(case, owner=True)
    expired = assert_expired_and_resealed(case, row["dedupe_key"])
    assert expired["resumed_at"] is None


def test_sweeper_without_the_sealing_owner_leaves_a_selected_row_pending(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: the legacy writer never expires an adopted row."""
    case = creation_case
    row = lapse(case, created(case))
    sweep(case, owner=False)
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
    sweep(case, owner=True)
    sweep(case, owner=False)
    response = case.client.post(
        f"/approvals/{forged}/resolve", json={"decision": "approved"},
        headers=chat_headers(uuid.UUID(forged), row["card_channel"]),
    )
    assert response.status_code == 403, response.text
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
    sweep(case, owner=True)
    assert_expired_and_resealed(case, row["dedupe_key"])

    async def reconcile(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: a pass past the grace horizon."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        try:
            reconciler = ResumeReconciler(
                factory, ResumeQueue(valkey), interval_seconds=1, grace_seconds=0,
                batch_limit=10,
            )
            await asyncio.sleep(0.01)
            assert await reconciler.reconcile_once() == 0
        finally:
            await valkey.aclose()

    run(reconcile)
    assert rows(row["dedupe_key"])[0]["resumed_at"] is None


def seed_rows(
    source: dict[str, Any], specs: list[dict[str, Any]],
) -> list[uuid.UUID]:
    """@spec HTTP-APPROVAL-CONTINUITY-10: owned raw rows copied from a real selected row."""
    ids: list[uuid.UUID] = []

    async def insert(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            for spec in specs:
                approval_id = uuid.uuid4()
                await session.execute(
                    text(f'INSERT INTO "{SCHEMA}".approvals (id, agent_id, conversation_id, '
                         "author, summary, reply_kind, reply_channel, reply_adapter, "
                         "reply_placeholder, dedupe_key, status, expires_at, resolved_at, "
                         "card_channel, identity_context) "
                         "SELECT :id, CASE WHEN :legacy THEN NULL ELSE agent_id END, "
                         "conversation_id, author, summary, reply_kind, "
                         "CASE WHEN :legacy THEN :unbound ELSE reply_channel END, "
                         "CASE WHEN :legacy THEN NULL ELSE reply_adapter END, reply_placeholder, "
                         ":dedupe, :status, :expires, :resolved, card_channel, :context "
                         f'FROM "{SCHEMA}".approvals WHERE id = :source'),
                    {
                        "id": approval_id, "source": source["id"], "dedupe": uuid.uuid4().hex,
                        "legacy": spec.get("legacy", False),
                        "unbound": "C0EXAMPLEUNBOUND" + uuid.uuid4().hex[:6].upper(),
                        "status": spec["status"], "expires": spec.get("expires"),
                        "resolved": spec.get("resolved"), "context": spec.get("context"),
                    },
                )
                ids.append(approval_id)
            await session.commit()

    run(insert)
    return ids


def row_by_id(approval_id: uuid.UUID) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-10."""
    found: list[dict[str, Any]] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10."""
        async with factory() as session:
            result = await session.execute(
                text(f'SELECT * FROM "{SCHEMA}".approvals WHERE id = :id'), {"id": approval_id},
            )
            found.extend(dict(item) for item in result.mappings())

    run(read)
    return found[0]


def test_reconciler_batches_never_fill_with_selected_rows(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: selected owed wakes cannot starve legacy wakes."""
    case = creation_case
    source = created(case)
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=2)
    selected = seed_rows(source, [
        {"status": "expired", "expires": old, "resolved": old, "context": "synthetic-sealed"}
        for _ in range(3)
    ])
    (legacy,) = seed_rows(source, [{
        "status": "expired", "expires": old + timedelta(minutes=30),
        "resolved": old + timedelta(minutes=30), "legacy": True,
    }])
    stream = f"test:curie:runs:fairness:{uuid.uuid4().hex}"

    async def reconcile(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: a separate owned stream."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        try:
            reconciler = ResumeReconciler(
                factory, ResumeQueue(valkey, stream=stream), interval_seconds=1,
                grace_seconds=0, batch_limit=2,
            )
            assert await reconciler.reconcile_once() == 1
            assert await valkey.xlen(stream) == 1
        finally:
            await valkey.delete(stream)
            await valkey.aclose()

    run(reconcile)
    assert row_by_id(legacy)["resumed_at"] is not None
    assert all(row_by_id(item)["resumed_at"] is None for item in selected)


def test_sweeper_batches_never_fill_with_rows_it_must_leave_pending(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: unverifiable and unsealed selected rows are passed."""
    case = creation_case
    source = created(case)
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=2)
    stuck = seed_rows(source, [
        {"status": "pending", "expires": old, "context": "synthetic-unverifiable"}
        for _ in range(3)
    ] + [{"status": "pending", "expires": old} for _ in range(3)])
    (legacy,) = seed_rows(source, [{
        "status": "pending", "expires": old + timedelta(minutes=30), "legacy": True,
    }])
    runtime = case.client.app.state.http_identity_runtime

    async def go(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: limit smaller than the stuck backlog."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        stream = f"test:curie:runs:fairness:{uuid.uuid4().hex}"
        try:
            stores = SimpleNamespace(
                sessionmaker=factory, valkey=valkey, http_identity_runtime=runtime,
            )
            async with factory() as session:
                await sweep_expired_approvals(
                    session, ResumeQueue(valkey, stream=stream),
                    now=datetime.now(UTC).replace(tzinfo=None), limit=2,
                    http_expiry=http_expiry_owner(stores),
                )
        finally:
            await valkey.delete(stream)
            await valkey.aclose()

    run(go)
    assert row_by_id(legacy)["status"] == "expired"
    assert all(row_by_id(item)["status"] == "pending" for item in stuck)


def test_expiry_owner_takes_no_identity_writer_when_nothing_expires(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9/10: no global fence rotation for a no-op."""
    case = creation_case
    row = created(case)
    runtime = case.client.app.state.http_identity_runtime

    async def go(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: a pending row whose deadline has not passed."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        try:
            before = await valkey.get(identity_fence.FENCE_KEY)
            owner = http_expiry_owner(SimpleNamespace(
                sessionmaker=factory, valkey=valkey, http_identity_runtime=runtime,
            ))
            assert await owner(row["id"]) is None
            assert await valkey.get(identity_fence.FENCE_KEY) == before
        finally:
            await valkey.aclose()

    run(go)
    assert rows(row["dedupe_key"]) == [row]


def test_resolve_reports_expiry_owner_failure_as_unavailable(
    creation_case: SimpleNamespace, monkeypatch: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: bounded 503, never a raw 500."""
    case = creation_case
    row = lapse(case, created(case))

    async def broken(_approval_id: uuid.UUID) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-10: a synthetic writer outage."""
        raise RuntimeError("synthetic identity writer outage")

    monkeypatch.setattr(case.client.app.state, "http_approval_expiry", broken)
    response = case.client.post(
        f"/approvals/{row['id']}/resolve", json={"decision": "approved"},
        headers=chat_headers(row["id"], row["card_channel"]),
    )
    assert response.status_code == 503, response.text
    assert response.json() == {"detail": "authority_unavailable"}
    assert rows(row["dedupe_key"]) == [row]


def test_resolve_after_a_concurrent_expiry_keeps_the_existing_410(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: a lost expiry race is still the 410 answer."""
    case = creation_case
    row = lapse(case, created(case))
    sweep(case, owner=True)
    expired = assert_expired_and_resealed(case, row["dedupe_key"])
    response = case.client.post(
        f"/approvals/{row['id']}/resolve", json={"decision": "approved"},
        headers=chat_headers(row["id"], row["card_channel"]),
    )
    assert response.status_code == 410, response.text
    assert rows(row["dedupe_key"]) == [expired]
