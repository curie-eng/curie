"""Break-glass approval recovery (#2753): enablement, atomicity, tombstone, report.

Stream B's surface, driven through the real HTTP routes against the disposable
Postgres and the compose Valkey. Nothing here mocks the database or the queue;
Slack is the only faked service, and it is faked precisely so a recovery that
touches a card fails the test.

Three routes are under test, all on a router separate from ``routers/approvals.py``:

* ``GET  /approvals/identity-report``     -- per-row FACTS, no Slack call, never
  a claim that a row is unresolvable (plan ruling 7).
* ``POST /approvals/{id}/recover``        -- administrative disposition, one
  transaction, one commit, replay-safe on ``recovery_key``.
* ``POST /approvals/{id}/resume/cancel``  -- tombstones an owed resume; retains
  every column and every audit row.

Authority is the EXISTING ``require_platform_key`` plus the EXISTING ADR-0106
operator principal for attribution only, gated by ``Settings.approval_recovery_enabled``.
There is no new credential and no new auth scheme. Route membership is never
consulted, which is what the AC3 pairing below proves: the same operator
principal that ``POST /{id}/resolve`` refuses with 403 at ``authorizer.py:79-91``
recovers the very same approval.
"""

import asyncio
import json
import os
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from typing import Any

import httpx
import pytest
import redis
from curie_api import approval_principal, crud
from curie_api.config import get_settings
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_api.slack_usergroups import SlackUserGroupClient
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

RECOVERY_ENV = "CURIE_APPROVAL_RECOVERY_ENABLED"

_BROAD = "C0BROAD01"
_ELSEWHERE = "C0ELSE001"
_OPERATOR = "U0OPERAT1"

# The declaration document the migration fence (Stream A) consumes. The report
# emits this skeleton for every row it flags reply_identity_unreconstructable;
# the operator fills in the values and feeds the file back to the next upgrade.
DECLARATION_KEYS = {
    "approval_id",
    "reply_kind",
    "reply_adapter",
    "actor",
    "reason",
}


# --- fixtures -----------------------------------------------------------------


def _client_with_recovery(enabled: bool) -> Iterator[TestClient]:
    os.environ[RECOVERY_ENV] = "true" if enabled else "false"
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()


@pytest.fixture
def recovery_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """The grant is ON. Built after ``runs_stream`` so the app reads the
    per-test stream, and after the env write so ``Settings`` reads the flag."""

    yield from _client_with_recovery(True)


@pytest.fixture
def disabled_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """The default installation: the grant is OFF."""

    yield from _client_with_recovery(False)


# --- helpers ------------------------------------------------------------------


def _payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": "U1",
        "summary": "Give ACME a 20% discount",
        "reply_kind": "slack",
        "reply_channel": "C1",
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    base.update(overrides)
    return base


def _operator_headers(
    actor: str = _OPERATOR, *, base: Mapping[str, str] | None = None
) -> dict[str, str]:
    """An ADR-0106 operator principal, minted exactly as test_approvals.py does."""

    token = approval_principal.mint(
        get_settings().api_key,
        subject=actor,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    return {**dict(base or {}), "X-Curie-Approval-Principal": token}


def _slack_spy(client: TestClient) -> list[httpx.Request]:
    """Wire the API's Slack surface to a transport that RECORDS every request.

    A recovery path that reaches for a card shows up here, which is how the
    "makes no Slack call" contract is proven rather than assumed.
    """

    calls: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": []})

    group_client = SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_handler)),
        token="xoxb-test",
    )
    client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(
        group_client
    )
    return calls


def _agent_with_routes(client: TestClient, headers: dict[str, str], routes: dict[str, Any]) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"routed-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": "C0AGENT001"},
            "approval_routes": routes,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _channel_membership_approval(client: TestClient, headers: dict[str, str]) -> dict[str, Any]:
    """An approval on a HEALTHY channel-membership route.

    A bare ``resolution`` binding with no ``approvers`` block selects
    ``SlackChannelMembers``, whose ``operator_eligible`` is False
    (slack_approvers.py:61). This is the exact route shape an operator
    principal cannot resolve and an ordinary chat click resolves fine.
    """

    agent_id = _agent_with_routes(
        client, headers, {"managers": {"resolution": {"kind": "slack", "address": _BROAD}}}
    )
    created = client.post(
        "/approvals",
        json=_payload(agent_id=agent_id, route="managers", card_channel=_BROAD),
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return created.json()


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def _run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                if result.returns_rows:
                    return [dict(row) for row in result.mappings().all()]
                return []
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _approval_row(approval_id: str) -> dict[str, Any]:
    rows = _sql(
        "SELECT status, reply_kind, reply_channel, reply_placeholder, recovery_key, "
        "resolved_at, resumed_at, resume_cancelled_at, resume_cancelled_reason, "
        "resume_cancelled_by FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(approval_id)},
    )
    assert len(rows) == 1
    return rows[0]


def _force_resolved_unresumed(approval_id: str) -> None:
    """The reconciler's work-list state: resolved, owes a wake, not yet woken.

    Written directly because the inline resolver marks ``resumed_at`` in the
    same request, and this state is what the resume reconciler exists to pick
    up -- it is the only state in which cancellation is legal.
    """

    _sql(
        "UPDATE curie.approvals SET status = 'approved', resolved_by = 'U1', "
        "resolved_at = now(), resumed_at = NULL WHERE id = :id",
        {"id": uuid.UUID(approval_id)},
    )


def _force_resumed(approval_id: str) -> None:
    _sql(
        "UPDATE curie.approvals SET status = 'approved', resolved_by = 'U1', "
        "resolved_at = now(), resumed_at = now() WHERE id = :id",
        {"id": uuid.UUID(approval_id)},
    )


def _seed_raw_pending(*, reply_placeholder: str | None, reply_kind: str = "slack") -> str:
    """A pending approval inserted with raw SQL, so a column the wire schema
    requires (the card's placeholder) can legitimately be absent."""

    approval_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, "
        "reply_kind, reply_channel, reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :conv, 'U1', :summary, :kind, 'C1', :ph, :dedupe, 'pending')",
        {
            "id": approval_id,
            "conv": f"th-{approval_id.hex[:8]}",
            "summary": "raw seeded row",
            "kind": reply_kind,
            "ph": reply_placeholder,
            "dedupe": uuid.uuid4().hex,
        },
    )
    return str(approval_id)


def _audit(client: TestClient, headers: dict[str, str], approval_id: str) -> list[Any]:
    got = client.get(f"/approvals/{approval_id}/audit", headers=headers)
    assert got.status_code == 200, got.text
    return list(got.json())


def _recover_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "disposition": "rejected",
        "reason": "upgrade stranded this approval; settled administratively",
        "recovery_key": f"rk-{uuid.uuid4().hex}",
    }
    body.update(overrides)
    return body


def _cancel_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "reason": "the suspended session was abandoned during the upgrade",
        "recovery_key": f"ck-{uuid.uuid4().hex}",
    }
    body.update(overrides)
    return body


def _report_entry(body: Any, approval_id: str) -> dict[str, Any]:
    matches = [row for row in body["approvals"] if row["id"] == approval_id]
    assert len(matches) == 1, body
    return matches[0]


# --- enablement and authority -------------------------------------------------


def test_disabled_grant_refuses_every_recovery_route_naming_the_setting(
    disabled_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The default installation refuses, and says WHY in its own words.

    The refusal must name the setting and must not read like a membership or
    credential failure -- an operator debugging a disabled grant that echoed
    "not an approver" would go looking at the route's approvers for something
    that is not there.
    """

    created = _channel_membership_approval(disabled_client, auth_headers)
    headers = _operator_headers(base=auth_headers)

    refusals = [
        disabled_client.post(
            f"/approvals/{created['id']}/recover", json=_recover_body(), headers=headers
        ),
        disabled_client.post(
            f"/approvals/{created['id']}/resume/cancel",
            json=_cancel_body(),
            headers=headers,
        ),
    ]
    for refused in refusals:
        assert refused.status_code == 403, refused.text
        detail = refused.json()["detail"]
        assert "api.approvalRecovery.enabled" in detail, detail
        assert "not enabled" in detail, detail
        # Textually distinct from every membership/eligibility refusal.
        assert "not an approver" not in detail, detail
        assert "can resolve only routes" not in detail, detail
        assert "no longer bound" not in detail, detail

    # Nothing happened: still pending, no audit row, no tombstone.
    row = _approval_row(created["id"])
    assert row["status"] == "pending"
    assert row["recovery_key"] is None
    assert row["resume_cancelled_at"] is None
    assert _audit(disabled_client, auth_headers, created["id"]) == []


def test_recovery_requires_the_platform_key(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Enabled is not open. The existing platform boundary still applies."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    # Principal present, platform key absent.
    refused = recovery_client.post(
        f"/approvals/{created['id']}/recover",
        json=_recover_body(),
        headers=_operator_headers(),
    )
    assert refused.status_code == 401, refused.text
    assert _approval_row(created["id"])["status"] == "pending"


def test_recovery_requires_an_operator_principal_for_attribution(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The platform key alone cannot act: an unattributed recovery would leave
    an audit row naming nobody, which is the one thing the accepted blast
    radius relies on to stay reviewable."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    refused = recovery_client.post(
        f"/approvals/{created['id']}/recover", json=_recover_body(), headers=auth_headers
    )
    assert refused.status_code == 401, refused.text
    assert _approval_row(created["id"])["status"] == "pending"


def test_recovery_succeeds_where_operator_resolution_is_refused_403(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3's proof: recovery does not depend on, and does not widen, membership.

    The SAME operator principal, on the SAME approval, in the SAME request
    sequence: ``POST /resolve`` is refused 403 at authorizer.py:79-91 because a
    channel-membership set sets ``operator_eligible = False``, and recovery then
    succeeds anyway. If recovery had been built on ``authorize_approval`` this
    test could not pass; if it had widened the authorizer, the 403 below would
    have turned into a 200 and this test would also fail.
    """

    calls = _slack_spy(recovery_client)
    created = _channel_membership_approval(recovery_client, auth_headers)
    headers = _operator_headers(base=auth_headers)

    refused = recovery_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=headers,
    )
    assert refused.status_code == 403, refused.text
    assert "can resolve only routes" in refused.json()["detail"]

    recovered = recovery_client.post(
        f"/approvals/{created['id']}/recover", json=_recover_body(), headers=headers
    )
    assert recovered.status_code == 200, recovered.text

    row = _approval_row(created["id"])
    assert row["status"] == "rejected"
    assert row["resolved_at"] is not None
    # The resume is enqueued on the normal path so the suspended session wakes.
    assert len(valkey.xrange(runs_stream)) == 1

    # And the ordinary path is still exactly as closed as it was before.
    still_refused = recovery_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_operator_headers(base=auth_headers),
    )
    assert still_refused.status_code in (403, 409), still_refused.text

    # No card was touched at any point.
    assert calls == []


def test_recovery_refuses_an_approved_disposition(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """There is no approve-on-behalf-of. Rejected is the only disposition."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    refused = recovery_client.post(
        f"/approvals/{created['id']}/recover",
        json=_recover_body(disposition="approved"),
        headers=_operator_headers(base=auth_headers),
    )
    assert refused.status_code == 422, refused.text
    assert _approval_row(created["id"])["status"] == "pending"


@pytest.mark.parametrize("reason", ["", "   "])
def test_recovery_refuses_an_empty_reason(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    reason: str,
) -> None:
    """The reason is the whole after-the-fact review surface; a blank one makes
    the accepted blast radius unreviewable."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    refused = recovery_client.post(
        f"/approvals/{created['id']}/recover",
        json=_recover_body(reason=reason),
        headers=_operator_headers(base=auth_headers),
    )
    assert refused.status_code == 422, refused.text
    assert _approval_row(created["id"])["status"] == "pending"


def test_recovery_audit_row_records_actor_reason_and_facts(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """One audit row, attributed to the operator, carrying FACTS only.

    Ruling 7: the evidence may state what the reporter observed; it may never
    assert that the ordinary path was unavailable.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)
    body = _recover_body()
    ok = recovery_client.post(
        f"/approvals/{created['id']}/recover",
        json=body,
        headers=_operator_headers(base=auth_headers),
    )
    assert ok.status_code == 200, ok.text

    rows = _audit(recovery_client, auth_headers, created["id"])
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["action"] == "administratively_recovered"
    assert row["actor"] == _OPERATOR
    assert row["authorizer"] == "approval_recovery"
    assert row["authorized"] is True
    assert row["reason"] == body["reason"]
    assert row["evidence"]["kind"] == "administrative_recovery"
    assert row["evidence"]["recovery_key"] == body["recovery_key"]
    assert "facts" in row["evidence"]
    serialized = json.dumps(row["evidence"])
    assert "unresolvable" not in serialized, serialized
    assert "unavailable" not in serialized, serialized


# --- exactly-once under retry and crash (AC3) ---------------------------------


def test_recover_replay_with_the_same_key_returns_the_recorded_outcome(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """A retried request is a read of what already happened, not a second act.

    One status change, one audit row, one stream entry, identical body.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)
    headers = _operator_headers(base=auth_headers)
    body = _recover_body()

    first = recovery_client.post(f"/approvals/{created['id']}/recover", json=body, headers=headers)
    assert first.status_code == 200, first.text
    first_row = _approval_row(created["id"])

    second = recovery_client.post(f"/approvals/{created['id']}/recover", json=body, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json() == first.json()

    assert _approval_row(created["id"]) == first_row
    assert len(_audit(recovery_client, auth_headers, created["id"])) == 1
    assert len(valkey.xrange(runs_stream)) == 1


def test_recover_with_a_different_key_on_a_recovered_approval_is_a_conflict(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """A second key is a second intent, and there is nothing left to act on."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    headers = _operator_headers(base=auth_headers)

    first = recovery_client.post(
        f"/approvals/{created['id']}/recover", json=_recover_body(), headers=headers
    )
    assert first.status_code == 200, first.text
    before = _approval_row(created["id"])

    conflict = recovery_client.post(
        f"/approvals/{created['id']}/recover", json=_recover_body(), headers=headers
    )
    assert conflict.status_code == 409, conflict.text

    assert _approval_row(created["id"]) == before
    assert len(_audit(recovery_client, auth_headers, created["id"])) == 1
    assert len(valkey.xrange(runs_stream)) == 1


def test_recover_rolls_back_entirely_when_the_audit_append_fails(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The crash window ``claim_approval_resolution`` + ``append_approval_audit``
    would have left open, closed by construction.

    Both of those commit internally (crud.py:1867 and crud.py:2110), so composing
    them can strand a flipped status with no audit row. The failure is injected
    BETWEEN the sub-steps by making the audit-row construction raise, which is
    only reachable if the status CAS already ran in the same open transaction.
    A whole-operation rollback is the only way the assertions below hold.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)

    boom = RuntimeError("injected failure between the status CAS and the audit append")

    def _explode(*args: Any, **kwargs: Any) -> Any:
        raise boom

    # The atomic function builds its audit row from this ORM class, exactly as
    # append_approval_audit does; patching it is the seam between the two writes.
    monkeypatch.setattr(crud, "ApprovalAuditEntry", _explode)

    with pytest.raises(RuntimeError):
        recovery_client.post(
            f"/approvals/{created['id']}/recover",
            json=_recover_body(),
            headers=_operator_headers(base=auth_headers),
        )

    monkeypatch.undo()

    row = _approval_row(created["id"])
    assert row["status"] == "pending", row
    assert row["resolved_at"] is None, row
    assert row["recovery_key"] is None, row
    assert _audit(recovery_client, auth_headers, created["id"]) == []
    assert valkey.xrange(runs_stream) == []


# --- cancellation semantics ---------------------------------------------------


def test_cancel_is_refused_while_the_approval_is_still_pending(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """There is no owed resume to cancel until something resolved it."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    refused = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel",
        json=_cancel_body(),
        headers=_operator_headers(base=auth_headers),
    )
    assert refused.status_code == 409, refused.text
    assert _approval_row(created["id"])["resume_cancelled_at"] is None


def test_cancel_is_refused_once_the_resume_is_already_marked(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """``resumed_at`` set means the wake was already dispatched and owed
    nothing further; a tombstone there would claim a veto it cannot deliver."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    _force_resumed(created["id"])

    refused = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel",
        json=_cancel_body(),
        headers=_operator_headers(base=auth_headers),
    )
    assert refused.status_code == 409, refused.text
    assert _approval_row(created["id"])["resume_cancelled_at"] is None


def test_cancel_tombstones_without_deleting_identity_or_history(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The tombstone RETAINS. Every reply-identity column and every pre-existing
    audit row survives; the only change is the new columns and one new row."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    _force_resolved_unresumed(created["id"])
    before_row = _approval_row(created["id"])
    before_audit = _audit(recovery_client, auth_headers, created["id"])

    body = _cancel_body()
    ok = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel",
        json=body,
        headers=_operator_headers(base=auth_headers),
    )
    assert ok.status_code == 200, ok.text

    after_row = _approval_row(created["id"])
    for column in ("reply_kind", "reply_channel", "reply_placeholder", "status"):
        assert after_row[column] == before_row[column], column
    assert after_row["resumed_at"] is None
    assert after_row["resume_cancelled_at"] is not None
    assert after_row["resume_cancelled_reason"] == body["reason"]
    assert after_row["resume_cancelled_by"] == _OPERATOR

    after_audit = _audit(recovery_client, auth_headers, created["id"])
    assert after_audit[: len(before_audit)] == before_audit
    assert len(after_audit) == len(before_audit) + 1
    row = after_audit[-1]
    assert row["action"] == "resume_cancelled"
    assert row["actor"] == _OPERATOR
    assert row["authorizer"] == "approval_recovery"
    assert row["reason"] == body["reason"]


def test_cancel_replay_with_the_same_key_returns_the_recorded_outcome(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    created = _channel_membership_approval(recovery_client, auth_headers)
    _force_resolved_unresumed(created["id"])
    headers = _operator_headers(base=auth_headers)
    body = _cancel_body()

    first = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel", json=body, headers=headers
    )
    assert first.status_code == 200, first.text
    first_row = _approval_row(created["id"])

    second = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel", json=body, headers=headers
    )
    assert second.status_code == 200, second.text
    assert second.json() == first.json()
    assert _approval_row(created["id"]) == first_row
    assert len(_audit(recovery_client, auth_headers, created["id"])) == 1


def test_cancel_with_a_different_key_on_a_tombstoned_approval_is_a_conflict(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    created = _channel_membership_approval(recovery_client, auth_headers)
    _force_resolved_unresumed(created["id"])
    headers = _operator_headers(base=auth_headers)

    assert (
        recovery_client.post(
            f"/approvals/{created['id']}/resume/cancel",
            json=_cancel_body(),
            headers=headers,
        ).status_code
        == 200
    )
    before = _approval_row(created["id"])

    conflict = recovery_client.post(
        f"/approvals/{created['id']}/resume/cancel", json=_cancel_body(), headers=headers
    )
    assert conflict.status_code == 409, conflict.text
    assert _approval_row(created["id"]) == before
    assert len(_audit(recovery_client, auth_headers, created["id"])) == 1


def test_cancel_rolls_back_entirely_when_the_audit_append_fails(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same crash window as the recover path, same closure: the tombstone write
    and its audit row are one transaction with one commit, or neither lands."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    _force_resolved_unresumed(created["id"])

    def _explode(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("injected failure between the tombstone CAS and the audit")

    monkeypatch.setattr(crud, "ApprovalAuditEntry", _explode)

    with pytest.raises(RuntimeError):
        recovery_client.post(
            f"/approvals/{created['id']}/resume/cancel",
            json=_cancel_body(),
            headers=_operator_headers(base=auth_headers),
        )

    monkeypatch.undo()

    row = _approval_row(created["id"])
    assert row["resume_cancelled_at"] is None, row
    assert row["resume_cancelled_reason"] is None, row
    assert row["resume_cancelled_by"] is None, row
    assert row["recovery_key"] is None, row
    assert _audit(recovery_client, auth_headers, created["id"]) == []


# --- identity-report ----------------------------------------------------------


def test_identity_report_needs_only_the_platform_key_and_makes_no_slack_call(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """It is a pure read an operator runs on a broken installation. It must not
    depend on Slack being reachable, and a second call must be identical."""

    calls = _slack_spy(recovery_client)
    _channel_membership_approval(recovery_client, auth_headers)

    first = recovery_client.get("/approvals/identity-report", headers=auth_headers)
    assert first.status_code == 200, first.text
    second = recovery_client.get("/approvals/identity-report", headers=auth_headers)
    assert second.json() == first.json()
    assert calls == []

    assert recovery_client.get("/approvals/identity-report").status_code == 401


def test_identity_report_does_not_call_a_healthy_channel_route_unresolvable(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The correction of the earlier draft, stated as an assertion.

    ``operator_eligible == False`` describes a HEALTHY channel-membership route
    that any attested chat click resolves. The report may state facts about this
    row; it may not claim the row cannot be resolved, and it may not exclude it.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)
    body = recovery_client.get("/approvals/identity-report", headers=auth_headers).json()

    entry = _report_entry(body, created["id"])
    assert "reply_identity_unreconstructable" not in entry["facts"]
    assert "route_declared_but_unbound" not in entry["facts"]
    assert "approver_set_malformed" not in entry["facts"]
    serialized = json.dumps(entry)
    assert "unresolvable" not in serialized, serialized
    assert "operator_eligible" not in serialized, serialized


def test_identity_report_names_a_declared_but_unbound_route(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """An approval naming a route with no binding behind it -- the ADR-0123
    fail-closed shape -- is reported with that specific fact and no other."""

    created = recovery_client.post(
        "/approvals",
        json=_payload(agent_id=None, route="managers", card_channel=_BROAD),
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    approval_id = created.json()["id"]

    body = recovery_client.get("/approvals/identity-report", headers=auth_headers).json()
    entry = _report_entry(body, approval_id)
    assert "route_declared_but_unbound" in entry["facts"], entry


def test_identity_report_names_missing_card_identity(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A row with no reply placeholder has no card to address a reply to."""

    approval_id = _seed_raw_pending(reply_placeholder=None)
    healthy_id = _seed_raw_pending(reply_placeholder="p-1")

    body = recovery_client.get("/approvals/identity-report", headers=auth_headers).json()
    assert "card_identity_missing" in _report_entry(body, approval_id)["facts"]
    assert "card_identity_missing" not in _report_entry(body, healthy_id)["facts"]


def test_identity_report_emits_a_round_tripping_declaration_skeleton(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    tmp_path: Any,
) -> None:
    """The skeleton is the artifact the migration workflow consumes.

    An operator fills in the values, writes the document to the declaration
    Secret, and the fence honors it. So it must serialize to JSON and read back
    byte-for-value identical, and it must carry exactly the fields the fence
    needs -- no more (a stray field is a stale declaration the fence refuses)
    and no fewer.
    """

    # The 0024 shape: a non-Slack approval whose reply must be authenticated
    # with an egress identity, and no adapter-bearing binding names one. Nothing
    # in the schema can reconstruct it, which is exactly what the declaration
    # document exists to supply. (`reply_kind` itself is NOT NULL since 0022, so
    # unreconstructable never means a null kind.)
    unreconstructable_id = _seed_raw_pending(reply_placeholder="p-1", reply_kind="email")

    body = recovery_client.get("/approvals/identity-report", headers=auth_headers).json()

    entry = _report_entry(body, unreconstructable_id)
    assert "reply_identity_unreconstructable" in entry["facts"], entry

    declarations = body["declarations"]
    ids = {row["approval_id"] for row in declarations}
    assert unreconstructable_id in ids, declarations
    for row in declarations:
        assert set(row) == DECLARATION_KEYS, row
        # Everything but the id is the operator's to fill in; the report must
        # never pre-fill provenance, which would be the fabrication AC4 forbids.
        assert row["reply_kind"] is None, row
        assert row["reply_adapter"] is None, row

    path = tmp_path / "declarations.json"
    path.write_text(json.dumps(declarations), encoding="utf-8")
    assert json.loads(path.read_text(encoding="utf-8")) == declarations


# --- no stranded effect, no duplicated one ------------------------------------
#
# The two guarantees an administrative recovery owes beyond its own row. First,
# a recovery must leave nothing else half-settled: a publication approval drags
# a publication behind it, and the ordinary reject settles both. Second, the
# recovery's veto must be arbitrated against EXECUTION rather than against a
# read, so a cancellation cannot race a resume that already started and a
# duplicate delivery of the same resume cannot run twice.


def _seed_publication_deployment() -> uuid.UUID:
    """One agent, version and active deployment: a publication's FK parents."""

    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"pub-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v1', NULL, 'recovery-test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, CAST('dev' AS curie.environment), 'active')",
        {"id": deployment_id, "agent_id": agent_id, "version_id": version_id},
    )
    _sql(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch) "
        "VALUES (:id, :agent_id, :deployment_id, :conv, 'acme/bot', :sha, 'curie/pub-1')",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conv": f"th-{deployment_id.hex[:8]}",
            "sha": "a" * 40,
        },
    )
    return deployment_id


def _seed_pending_publication() -> tuple[str, str]:
    """A pending publication approval and the pending publication behind it."""

    deployment_id = _seed_publication_deployment()
    lineage = _sql(
        "SELECT id FROM curie.thread_publication_lineages WHERE deployment_id = :d",
        {"d": deployment_id},
    )[0]["id"]
    approval_id = uuid.uuid4()
    publication_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, reply_kind, "
        "reply_channel, reply_placeholder, dedupe_key, status, purpose) VALUES "
        "(:id, :conv, 'U1', 'Publish repository changes', 'slack', 'C1', 'p-1', "
        ":dedupe, 'pending', 'publication')",
        {
            "id": approval_id,
            "conv": f"th-{approval_id.hex[:8]}",
            "dedupe": uuid.uuid4().hex,
        },
    )
    _sql(
        "INSERT INTO curie.publications (id, approval_id, deployment_id, lineage_id, "
        "revision_number, repo_full_name, status, base_sha, patch_bytes, changed_paths, "
        "title, body, reply_kind, reply_channel) VALUES "
        "(:id, :approval_id, :deployment_id, :lineage, 1, 'acme/bot', 'pending', :sha, "
        ":patch, CAST('[\"README.md\"]' AS jsonb), 'Update README', 'body', 'slack', 'C1')",
        {
            "id": publication_id,
            "approval_id": approval_id,
            "deployment_id": deployment_id,
            "lineage": lineage,
            "sha": "a" * 40,
            "patch": b"private-patch-bytes",
        },
    )
    return str(approval_id), str(publication_id)


def _publication_row(publication_id: str) -> dict[str, Any]:
    rows = _sql(
        "SELECT status, version, terminal_at, patch_bytes FROM curie.publications "
        "WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )
    assert len(rows) == 1
    return rows[0]


def test_recovering_a_publication_approval_settles_its_publication(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A recovery leaves NO stranded effect (finding 1).

    Recovering a publication approval rejects the approval. Without this fix the
    publication behind it stays ``pending`` forever: the router deliberately
    enqueues no resume for a publication, and the expiry sweeper only selects
    PENDING approvals, so nothing in the system ever touches that row again and
    its private patch bytes are retained indefinitely. The ordinary reject
    settles both, and so must the administrative one -- in the same transaction.
    """

    approval_id, publication_id = _seed_pending_publication()
    before = _publication_row(publication_id)
    assert before["status"] == "pending"

    recovered = recovery_client.post(
        f"/approvals/{approval_id}/recover",
        json=_recover_body(),
        headers=_operator_headers(base=auth_headers),
    )
    assert recovered.status_code == 200, recovered.text

    after = _publication_row(publication_id)
    assert after["status"] == "denied", "the publication was left stranded as pending"
    assert after["terminal_at"] is not None, "no terminal instant was recorded"
    assert after["patch_bytes"] is None, "the private patch was retained after denial"
    assert after["version"] == before["version"] + 1, "the version check was not carried"

    # And the approval owes no wake: the router enqueues none for a publication,
    # so a NULL resumed_at here would put the row on the reconciler's work-list
    # for a resume that is never coming.
    row = _approval_row(approval_id)
    assert row["status"] == "rejected"
    assert row["resumed_at"] is not None


def test_publication_settlement_and_recovery_roll_back_together(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """One transaction, one commit -- across BOTH rows.

    The publication moves under the recovery (its version advances between the
    read and the settlement). The whole act must roll back: an approval settled
    against a publication the recovery did not actually deny is exactly the
    split-brain the single transaction exists to prevent.
    """

    approval_id, publication_id = _seed_pending_publication()

    original = crud.get_publication_by_approval

    async def _bump_then_read(session: Any, wanted_id: Any) -> Any:
        publication = await original(session, wanted_id)
        if publication is not None:
            # Another writer advances the version after this transaction read
            # it. Driven from a THREAD with its own event loop and its own
            # connection, so it is a genuinely concurrent writer rather than
            # this transaction editing its own snapshot.
            bump = threading.Thread(
                target=_sql,
                args=(
                    "UPDATE curie.publications SET version = version + 1 WHERE id = :id",
                    {"id": uuid.UUID(publication_id)},
                ),
            )
            bump.start()
            bump.join(timeout=30)
        return publication

    crud.get_publication_by_approval = _bump_then_read  # type: ignore[assignment]
    try:
        conflicted = recovery_client.post(
            f"/approvals/{approval_id}/recover",
            json=_recover_body(),
            headers=_operator_headers(base=auth_headers),
        )
    finally:
        crud.get_publication_by_approval = original  # type: ignore[assignment]

    assert conflicted.status_code == 409, conflicted.text
    assert "publication" in conflicted.json()["detail"]
    row = _approval_row(approval_id)
    assert row["status"] == "pending", "the approval settled without its publication"
    assert row["recovery_key"] is None
    assert _publication_row(publication_id)["status"] == "pending"
    assert _audit(recovery_client, auth_headers, approval_id) == []


def _record_execution(
    approval_id: str, *, lease_key: str | None, owner: str | None, generation: int | None
) -> None:
    """Write the worker's execution record, exactly as binding.py does."""

    _sql(
        "UPDATE curie.approvals SET resume_executing_at = now(), "
        "resume_executing_lease_key = :key, resume_executing_owner = :owner, "
        "resume_executing_generation = :gen "
        "WHERE id = :id AND resume_cancelled_at IS NULL",
        {"id": uuid.UUID(approval_id), "key": lease_key, "owner": owner, "gen": generation},
    )


def _valkey() -> redis.Redis:
    return redis.Redis.from_url(get_settings().valkey_dsn())


def _cancel(client: TestClient, headers: dict[str, str], approval_id: str) -> httpx.Response:
    return client.post(
        f"/approvals/{approval_id}/resume/cancel",
        json=_cancel_body(),
        headers=_operator_headers(base=headers),
    )


@pytest.mark.parametrize(
    ("lease_key", "lease_value"),
    [
        ("live", "owner-a"),  # the recorded holder still holds its lease
        ("live", None),  # the lease expired: the worker crashed
        ("live", "some-other-owner"),  # a later generation holds it
        (None, None),  # an unfenced delivery recorded no lease at all
    ],
)
def test_a_started_resume_cannot_be_cancelled_whatever_its_lease_state(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    lease_key: str | None,
    lease_value: str | None,
) -> None:
    """The regression for both round-3 P1s. A recorded execution means a
    delivery started this resume, and an absent or reassigned lease proves only
    lost delivery authority, never a stopped runner. Every lease state refuses
    with 409 naming the record, and no tombstone lands.

    ``resumed_at`` is still NULL here on purpose: the API marks it only after
    the enqueue returns, so a resume can be under way with that column empty.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)
    approval_id = created["id"]
    _force_resolved_unresumed(approval_id)
    key = f"test-2753:lease:{uuid.uuid4().hex}" if lease_key is not None else None
    owner = "owner-a" if key is not None else None
    _record_execution(approval_id, lease_key=key, owner=owner, generation=3 if key else None)
    client = _valkey()
    if key is not None and lease_value is not None:
        client.set(key, lease_value, ex=60)
    try:
        refused = _cancel(recovery_client, auth_headers, approval_id)
    finally:
        if key is not None:
            client.delete(key)
        client.close()
    assert refused.status_code == 409, refused.text
    detail = refused.json()["detail"]
    assert "started executing" in detail, detail
    if key is not None:
        assert key in detail and "owner-a" in detail and "generation 3" in detail, detail
    assert _approval_row(approval_id)["resume_cancelled_at"] is None


def test_a_committed_tombstone_makes_the_worker_record_match_nothing(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The reverse ordering: cancellation commits first, so the worker's
    record UPDATE (binding.py's exact statement) returns zero rows, which is
    the kernel's veto, and records nothing."""

    created = _channel_membership_approval(recovery_client, auth_headers)
    approval_id = created["id"]
    _force_resolved_unresumed(approval_id)
    assert _cancel(recovery_client, auth_headers, approval_id).status_code == 200

    rows = _sql(
        "UPDATE curie.approvals SET resume_executing_at = now(), "
        "resume_executing_lease_key = 'k', resume_executing_owner = 'late-worker', "
        "resume_executing_generation = 1 "
        "WHERE id = :id AND resume_cancelled_at IS NULL RETURNING id",
        {"id": uuid.UUID(approval_id)},
    )
    assert rows == []
    assert _approval_row(approval_id)["resume_cancelled_at"] is not None
    assert _sql(
        "SELECT resume_executing_at FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(approval_id)},
    )[0]["resume_executing_at"] is None


def test_cancellation_loses_to_a_record_that_commits_under_it(
    recovery_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The row-lock serialization, driven as a real interleaving.

    A worker's execution record is held open in an uncommitted transaction
    while the cancellation is in flight. The tombstone UPDATE blocks on the row
    lock and is re-evaluated once the record commits; ``resume_executing_at IS
    NULL`` no longer matches, so the cancel refuses with 409 naming the record.
    """

    created = _channel_membership_approval(recovery_client, auth_headers)
    approval_id = created["id"]
    _force_resolved_unresumed(approval_id)

    outcome: dict[str, Any] = {}
    record_taken = threading.Event()

    def _cancel_in_thread() -> None:
        record_taken.wait(timeout=10)
        started = time.monotonic()
        response = _cancel(recovery_client, auth_headers, approval_id)
        outcome["elapsed"] = time.monotonic() - started
        outcome["status"] = response.status_code
        outcome["detail"] = response.json().get("detail")

    async def _hold_the_record() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.approvals SET resume_executing_at = now(), "
                        "resume_executing_lease_key = 'k-racer', "
                        "resume_executing_owner = 'worker-racer', "
                        "resume_executing_generation = 2 "
                        "WHERE id = :id AND resume_cancelled_at IS NULL"
                    ),
                    {"id": uuid.UUID(approval_id)},
                )
                # Uncommitted: the cancel reads around it, then its UPDATE
                # blocks on this row.
                record_taken.set()
                await asyncio.sleep(1.0)
        finally:
            await engine.dispose()

    canceller = threading.Thread(target=_cancel_in_thread)
    canceller.start()
    asyncio.run(_hold_the_record())
    canceller.join(timeout=30)
    assert not canceller.is_alive()

    assert outcome["status"] == 409, outcome
    assert "worker-racer" in outcome["detail"], outcome
    assert "started executing" in outcome["detail"], outcome
    # It blocked on the uncommitted record rather than racing past it.
    assert outcome["elapsed"] >= 0.5, outcome
    assert _approval_row(approval_id)["resume_cancelled_at"] is None
    assert _sql(
        "SELECT resume_executing_owner FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(approval_id)},
    )[0]["resume_executing_owner"] == "worker-racer"


def test_a_deadlock_victim_gets_a_retryable_conflict_not_a_server_error() -> None:
    """A read that cycles with an identity migration's fence is aborted by
    Postgres with 40P01. The recovery routes must answer that with a
    retryable 409, and must still surface any other database error as-is."""

    from curie_api.routers.approval_recovery import _deadlock_as_retryable_conflict
    from fastapi import HTTPException
    from sqlalchemy.exc import DBAPIError

    class _Driver(Exception):
        def __init__(self, sqlstate: str) -> None:
            super().__init__(sqlstate)
            self.sqlstate = sqlstate

    def _raising(sqlstate: str):
        @_deadlock_as_retryable_conflict
        async def handler() -> None:
            raise DBAPIError("SELECT 1", {}, _Driver(sqlstate))

        return handler

    import asyncio

    with pytest.raises(HTTPException) as caught:
        asyncio.run(_raising("40P01")())
    assert caught.value.status_code == 409
    assert "retry" in str(caught.value.detail)

    with pytest.raises(DBAPIError):
        asyncio.run(_raising("23505")())
