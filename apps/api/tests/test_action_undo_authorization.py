"""Who may undo (ADR-0117 decision 3), against real Postgres.

"An undo requires the authorization the forward action required, and no more."

Both halves of that sentence are load-bearing and this file tests both. An action
whose tool was gated by an approval policy needs an authorizer of that same
route, resolved against membership the way ADR-0034 resolves an approver. An
action nobody had to approve is not gated on the way back either -- the state
being restored is one the cluster was already in, and it got there without anyone
approving it.

ADR-0106's authenticated-principal contract applies to approval resolution, not
this ADR-0117 action-undo seam. Undo preserves ADR-0117's existing approver-set
check over the request's actor and actor channel without adding a
distinct-requester rule. Adding that rule would be MORE authorization than the
forward action needed, which is the half of decision 3 that says "and no more".

A granted undo now answers ``202`` with a requested restore execution
(ACTION-EXECUTOR-3); the executor setting is on for this module, since it is
off by default (ACTION-EXECUTOR-1).

Every record here is fully undoable under the connector action executor rule
(ACTION-EXECUTOR-11, ``_sealed_actions``) unless a test says otherwise, so the
verdict each test observes is the authorizer's and not the snapshot rule's.
Authorization still runs first (ACTION-EXECUTOR-3 keeps the refusal ordering):
an actor who may not undo learns nothing about the record's reversibility.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from _sealed_actions import (
    ENVELOPE,
    POST_VERSION,
    executions_of,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    sealed_action,
    undoable_agent,
)
from curie_api import adapter_principal, approval_principal
from curie_api.approvers import MembershipVerdict
from curie_api.config import get_settings
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")


class _Set:
    """An approver set that answers however the test says."""

    def __init__(self, verdict: MembershipVerdict, name: str = "explicit-users") -> None:
        self._verdict = verdict
        self._name = name

    @property
    def audit_name(self) -> str:
        return self._name

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return self._verdict


@pytest.fixture
def gated_client(_disposable_db: Any, request: Any) -> Any:
    """A client whose approver set is whatever the test parametrized."""

    verdict = getattr(request, "param", MembershipVerdict(member=True))
    app = create_app()
    app.dependency_overrides[get_approver_sets] = lambda: lambda approval, binding: _Set(verdict)
    with TestClient(app) as client:
        yield client


def _seed_approval(client: Any, headers: Any) -> str:
    """A real approval to gate against.

    Not a made-up id: an unreadable gate is its own refusal path (see
    ``test_a_deleted_gate_fails_closed``), so the member/non-member tests have to
    exercise a gate that genuinely resolves.
    """

    created = client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
            "author": "U-author",
            "summary": "scale public/api to 10",
            "reply_kind": "slack",
            "reply_channel": "C1",
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _gated_action(
    client: Any, headers: Any, approval_id: str | None, tmp_path: Path, **overrides: Any
) -> dict[str, Any]:
    """A fully undoable record of a sealed, probed agent, gated by ``approval_id``."""

    agent_id = undoable_agent(client, headers, tmp_path)
    return sealed_action(client, headers, agent_id, gate_approval_id=approval_id, **overrides)


def _undo(client: Any, headers: Any, action_id: str, actor: str = "U-operator") -> Any:
    """Rule as the authenticated operator ``actor`` (ADR 0106 principal).

    ``headers`` is kept for call sites; the principal replaces the platform key.
    """

    return client.post(f"/actions/{action_id}/undo", json={}, headers=operator_headers(actor))


def test_an_ungated_action_is_not_gated_on_the_way_back(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """Nobody approved the change, so nobody has to approve putting it back."""

    action = _gated_action(client, auth_headers, None, tmp_path)

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 202, response.text
    assert len(executions_of(action["id"])) == 1
    audit = client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert audit[0]["authorizer"] == "ungated"
    assert audit[0]["authorized"] is True


def test_an_ungated_action_that_is_not_undoable_is_refused_with_its_code(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-11: ungated is not unconditional.

    The authorizer allows it, and the ruling then follows ``undoable``: a
    cleartext prior state is refused ``refused_unsealed`` and no granted-undo
    audit row is written.
    """

    action = _gated_action(
        client, auth_headers, None, tmp_path, prior_state={"spec": {"replicas": 3}}
    )
    assert action["undoable"] is False

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    audit = client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert [entry["action"] for entry in audit] == ["refused_unsealed"]
    assert not any(entry["authorized"] for entry in audit)
    assert executions_of(action["id"]) == []


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_a_member_of_the_gating_route_may_undo(
    gated_client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """Someone who could have permitted the change may put it back."""

    action = _gated_action(
        gated_client, auth_headers, _seed_approval(gated_client, auth_headers), tmp_path
    )

    response = _undo(gated_client, auth_headers, action["id"])

    assert response.status_code == 202, response.text
    assert len(executions_of(action["id"])) == 1
    audit = gated_client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert audit[0]["authorizer"] == "explicit-users"


@pytest.mark.parametrize(
    "gated_client", [MembershipVerdict(member=False, reason="not in #sre")], indirect=True
)
def test_a_non_member_is_refused_with_the_set_s_own_reason(
    gated_client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """The set explains itself: only it knows whether it refused on a list or a group."""

    action = _gated_action(
        gated_client, auth_headers, _seed_approval(gated_client, auth_headers), tmp_path
    )

    response = _undo(gated_client, auth_headers, action["id"])

    assert response.status_code == 403
    assert response.json()["detail"] == "not in #sre"
    entry = gated_client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()[0]
    assert entry["action"] == "refused_unauthorized"
    assert entry["authorized"] is False
    # @spec ACTION-EXECUTOR-3: every refusal creates no execution, and "an
    # unauthorized actor learns no version" -- nor the snapshot.
    assert executions_of(action["id"]) == []
    assert POST_VERSION not in response.text
    assert ENVELOPE["ciphertext"] not in response.text
    assert POST_VERSION not in str(entry["evidence"])


@pytest.mark.parametrize(
    "gated_client",
    [MembershipVerdict(member=True, undetermined=True, reason="Slack unreachable")],
    indirect=True,
)
def test_an_undetermined_set_fails_closed(
    gated_client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """`member` is meaningless when the set could not establish membership.

    Failing open here would let a Slack outage authorize a write into a
    customer's infrastructure.
    """

    action = _gated_action(
        gated_client, auth_headers, _seed_approval(gated_client, auth_headers), tmp_path
    )

    assert _undo(gated_client, auth_headers, action["id"]).status_code == 403


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_a_refused_authorization_leaves_the_record_untouched(
    gated_client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """Same invariant as the conflict rule: a refusal changes nothing."""

    action = _gated_action(
        gated_client, auth_headers, _seed_approval(gated_client, auth_headers), tmp_path
    )
    gated_client.app.dependency_overrides[get_approver_sets] = lambda: (
        lambda approval, binding: _Set(MembershipVerdict(member=False, reason="no"))
    )

    _undo(gated_client, auth_headers, action["id"])

    after = gated_client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert after["undone_at"] is None
    assert after["undoable"] is True


@pytest.mark.parametrize(
    "gated_client", [MembershipVerdict(member=False, reason="not in #sre")], indirect=True
)
def test_authorization_is_ruled_before_reversibility(
    gated_client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-11: the refusal ordering is kept.

    A non-member asking to undo a record that is also not undoable is told it
    may not undo, not which ingredient the record lacks.
    """

    action = _gated_action(
        gated_client,
        auth_headers,
        _seed_approval(gated_client, auth_headers),
        tmp_path,
        prior_state={"spec": {"replicas": 3}},
    )

    response = _undo(gated_client, auth_headers, action["id"])

    assert response.status_code == 403
    audit = gated_client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert [entry["action"] for entry in audit] == ["refused_unauthorized"]


def test_the_gating_approval_is_recorded_from_the_worker(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """The record carries which approval gated the call, or None when none did."""

    approval_id = str(uuid.uuid4())

    action = _gated_action(client, auth_headers, approval_id, tmp_path)

    assert action["gate_approval_id"] == approval_id


def test_a_gate_that_cannot_be_read_fails_closed(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """A deleted approval is an unreadable gate, not an absent one.

    Reading it as absent would let the approval sweeper turn a gated action into
    a freely undoable one -- a permission check quietly deleting itself.
    """

    action = _gated_action(client, auth_headers, str(uuid.uuid4()), tmp_path)

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 403
    assert "can no longer be read" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# The actor is an authenticated principal (executor route decisions, ADR 0106)
# --------------------------------------------------------------------------- #

CONSOLE_COOKIE = "__Host-curie_console_session"


def _console_headers(client: Any, auth_headers: dict[str, str], subject: str) -> dict[str, str]:
    """A live console session for ``subject``, presented as a same-origin browser would."""

    minted = client.post("/console/login-codes", json={"subject": subject}, headers=auth_headers)
    assert minted.status_code == 201, minted.text
    exchanged = client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = client.cookies.get(CONSOLE_COOKIE)
    assert token
    client.cookies.clear()
    return {"Cookie": f"{CONSOLE_COOKIE}={token}", "Origin": "http://testserver"}


def _authorized_rows(client: Any, headers: dict[str, str], action_id: str) -> list[dict[str, Any]]:
    audit = client.get(f"/actions/{action_id}/audit", headers=headers).json()
    return [entry for entry in audit if entry["authorized"]]


@pytest.mark.parametrize("body", [{"actor": "U-operator"}, {}], ids=["body actor", "no body actor"])
def test_the_platform_key_alone_cannot_rule_an_undo(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, body: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-3, route decisions: "a self-asserted ``actor`` in the
    request body is not authority ... a platform key alone cannot impersonate an
    approver". The ruling now causes a real restore, so it is refused and
    requests nothing.
    """

    action = _gated_action(client, auth_headers, None, tmp_path)

    response = client.post(f"/actions/{action['id']}/undo", json=body, headers=auth_headers)

    assert response.status_code == 401, response.text
    assert executions_of(action["id"]) == []
    assert _authorized_rows(client, auth_headers, action["id"]) == []


def test_an_operator_principal_rules_and_is_recorded_as_the_actor(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: the actor is derived from the authenticated
    operator principal and recorded on the execution and the audit row.
    """

    action = _gated_action(client, auth_headers, None, tmp_path)

    response = client.post(
        f"/actions/{action['id']}/undo", json={}, headers=operator_headers("U-principal")
    )

    assert response.status_code == 202, response.text
    assert [row["requested_by"] for row in executions_of(action["id"])] == ["U-principal"]
    rows = _authorized_rows(client, auth_headers, action["id"])
    assert [row["actor"] for row in rows] == ["U-principal"]


def test_a_console_principal_rules_and_is_recorded_as_the_actor(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: a console session is an authenticated principal."""

    action = _gated_action(client, auth_headers, None, tmp_path)
    headers = _console_headers(client, auth_headers, "U-console")

    response = client.post(f"/actions/{action['id']}/undo", json={}, headers=headers)

    assert response.status_code == 202, response.text
    assert [row["requested_by"] for row in executions_of(action["id"])] == ["U-console"]
    rows = _authorized_rows(client, auth_headers, action["id"])
    assert [row["actor"] for row in rows] == ["U-console"]


def test_a_body_actor_that_matches_the_principal_is_accepted(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: only a body actor that differs is refused."""

    action = _gated_action(client, auth_headers, None, tmp_path)

    response = client.post(
        f"/actions/{action['id']}/undo",
        json={"actor": "U-principal"},
        headers=operator_headers("U-principal"),
    )

    assert response.status_code == 202, response.text
    assert [row["requested_by"] for row in executions_of(action["id"])] == ["U-principal"]


def test_a_body_actor_that_differs_from_the_principal_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "a body actor that differs from the principal is
    refused", and nothing is requested under either name.
    """

    action = _gated_action(client, auth_headers, None, tmp_path)

    response = client.post(
        f"/actions/{action['id']}/undo",
        json={"actor": "U-somebody-else"},
        headers=operator_headers("U-principal"),
    )

    assert response.status_code in {401, 403, 422}, response.text
    assert executions_of(action["id"]) == []
    assert _authorized_rows(client, auth_headers, action["id"]) == []


def test_two_principal_credentials_together_are_ambiguous(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "exactly as the approval resolver does": any two
    principal credentials together fail closed rather than choosing one.
    """

    action = _gated_action(client, auth_headers, None, tmp_path)
    headers = {
        **_console_headers(client, auth_headers, "U-console"),
        **operator_headers("U-principal"),
    }

    response = client.post(f"/actions/{action['id']}/undo", json={}, headers=headers)

    assert response.status_code == 401, response.text
    assert executions_of(action["id"]) == []


def test_the_gating_set_is_asked_about_the_principal_not_a_body_actor(
    _disposable_db: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: membership is resolved for the authenticated
    principal's subject and channel evidence, as for an approval.
    """

    asked: list[tuple[str, str | None]] = []

    class _Recording(_Set):
        async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
            asked.append((actor, actor_channel))
            return MembershipVerdict(member=True)

    app = create_app()
    app.dependency_overrides[get_approver_sets] = lambda: (
        lambda approval, binding: _Recording(MembershipVerdict(member=True))
    )
    with TestClient(app) as gated:
        action = _gated_action(gated, auth_headers, _seed_approval(gated, auth_headers), tmp_path)

        response = gated.post(
            f"/actions/{action['id']}/undo", json={}, headers=operator_headers("U-principal")
        )

    assert response.status_code == 202, response.text
    assert asked == [("U-principal", None)]


# --------------------------------------------------------------------------- #
# Chat and adapter principals (route decisions, review round 2 N2)
# --------------------------------------------------------------------------- #

CHAT_SUBJECT = "U0CHATTER1"
CARD_CHANNEL = "C0EXAMPLE9"


def _chat_headers(approval_id: str, subject: str = CHAT_SUBJECT) -> dict[str, str]:
    """A dispatcher chat attestation bound to ``approval_id`` (ADR 0106)."""

    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=subject,
        kind="chat",
        actor_channel=CARD_CHANNEL,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {"X-Curie-Approval-Principal": token}


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_a_chat_principal_may_undo_the_action_its_approval_gated(
    gated_client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "A chat principal may undo only an action whose gating
    approval its token names"; its subject is the recorded actor.
    """

    gate = _seed_approval(gated_client, auth_headers)
    action = _gated_action(gated_client, auth_headers, gate, tmp_path)

    response = gated_client.post(
        f"/actions/{action['id']}/undo", json={}, headers=_chat_headers(gate)
    )

    assert response.status_code == 202, response.text
    assert [row["requested_by"] for row in executions_of(action["id"])] == [CHAT_SUBJECT]


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_a_chat_principal_for_another_approval_is_refused(
    gated_client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: a chat token naming a different approval is no credential
    for this action, even when the set would admit its subject.
    """

    gate = _seed_approval(gated_client, auth_headers)
    other = _seed_approval(gated_client, auth_headers)
    action = _gated_action(gated_client, auth_headers, gate, tmp_path)

    response = gated_client.post(
        f"/actions/{action['id']}/undo", json={}, headers=_chat_headers(other)
    )

    assert response.status_code in {401, 403}, response.text
    assert executions_of(action["id"]) == []


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_an_ungated_action_accepts_no_chat_credential(
    gated_client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: a chat credential is bound to an approval, so an action
    no approval gated admits none, whichever approval the token names.
    """

    some_approval = _seed_approval(gated_client, auth_headers)
    action = _gated_action(gated_client, auth_headers, None, tmp_path)

    response = gated_client.post(
        f"/actions/{action['id']}/undo", json={}, headers=_chat_headers(some_approval)
    )

    assert response.status_code in {401, 403}, response.text
    assert executions_of(action["id"]) == []


def _binding_id(agent_id: str) -> str:
    async def run() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text("SELECT id FROM curie.agent_channels WHERE agent_id = :aid"),
                    {"aid": agent_id},
                )
                return str(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _routed_approval(client: Any, auth_headers: dict[str, str]) -> tuple[str, str]:
    """An approval whose route resolves on a binding: (approval id, binding id)."""

    channel = f"C0ADP{uuid.uuid4().hex[:8].upper()}"
    route = f"route-{uuid.uuid4().hex[:8]}"
    agent = client.post(
        "/agents",
        json={
            "name": f"adapter-route-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel},
            "approval_routes": {
                route: {
                    "resolution": {"kind": "slack", "address": channel},
                    "approvers": {"users": ["U0SENDER01"]},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = str(agent.json()["id"])
    approval = client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
            "author": "U-author",
            "summary": "scale public/api to 10",
            "reply_kind": "slack",
            "reply_channel": channel,
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent_id,
            "route": route,
            "card_channel": channel,
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert approval.status_code == 201, approval.text
    return str(approval.json()["id"]), _binding_id(agent_id)


def _adapter_headers(bindings: list[str], actor: str = "U0SENDER01") -> dict[str, str]:
    token = adapter_principal.mint(
        get_settings().api_key,
        subject="mail-adapter-test",
        bindings=bindings,
        exp=int(time.time()) + 600,
    )
    return {"X-Curie-Adapter-Principal": token, "X-Curie-Approval-Actor": actor}


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
def test_an_adapter_may_undo_an_action_whose_gating_approval_it_serves(
    gated_client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "an adapter principal only an action whose gating approval
    it serves"; the sender it vouches for is the recorded actor.
    """

    gate, binding = _routed_approval(gated_client, auth_headers)
    action = _gated_action(gated_client, auth_headers, gate, tmp_path)

    response = gated_client.post(
        f"/actions/{action['id']}/undo", json={}, headers=_adapter_headers([binding])
    )

    assert response.status_code == 202, response.text
    assert [row["requested_by"] for row in executions_of(action["id"])] == ["U0SENDER01"]


@pytest.mark.parametrize("gated_client", [MembershipVerdict(member=True)], indirect=True)
@pytest.mark.parametrize("gating", ["unserved approval", "ungated"])
def test_an_adapter_reads_an_action_it_does_not_serve_as_not_found(
    gated_client: Any, auth_headers: dict[str, str], tmp_path: Path, gating: str
) -> None:
    """Route decisions: an action whose gating approval the adapter does not serve,
    or that no approval gated, "reads as a missing action" -- the same 404 as an
    action that does not exist, so an adapter learns nothing beyond its bindings.
    """

    _, served_binding = _routed_approval(gated_client, auth_headers)
    unserved_gate, _ = _routed_approval(gated_client, auth_headers)
    gate = unserved_gate if gating == "unserved approval" else None
    action = _gated_action(gated_client, auth_headers, gate, tmp_path)
    headers = _adapter_headers([served_binding])

    response = gated_client.post(f"/actions/{action['id']}/undo", json={}, headers=headers)
    missing = gated_client.post(f"/actions/{uuid.uuid4()}/undo", json={}, headers=headers)

    assert response.status_code == 404, response.text
    assert response.json() == missing.json()
    assert executions_of(action["id"]) == []


# --------------------------------------------------------------------------- #
# Authentication before lookup (route decisions, review round 2 N4)
# --------------------------------------------------------------------------- #


def _junk_operator() -> dict[str, str]:
    return {"X-Curie-Approval-Principal": "not-a-principal"}


def _wrong_key_operator() -> dict[str, str]:
    token = approval_principal.mint(
        "not-the-platform-key",
        subject="U-operator",
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {"X-Curie-Approval-Principal": token}


@pytest.mark.parametrize(
    "credential",
    ["none", "platform key", "junk principal", "wrongly signed operator", "expired cookie"],
)
def test_an_unauthenticated_undo_cannot_tell_existing_actions_from_missing_ones(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, credential: str
) -> None:
    """Route decisions: "The undo route authenticates the principal before it looks
    up the action, so an unauthenticated caller cannot learn which action
    identifiers exist": an existing and a nonexistent action answer the same
    authentication refusal, never a 404 for one of them.
    """

    action = _gated_action(client, auth_headers, None, tmp_path)
    headers = {
        "none": {},
        "platform key": auth_headers,
        "junk principal": _junk_operator(),
        "wrongly signed operator": _wrong_key_operator(),
        "expired cookie": {
            "Cookie": f"{CONSOLE_COOKIE}=not-a-live-session",
            "Origin": "http://testserver",
        },
    }[credential]

    existing = client.post(f"/actions/{action['id']}/undo", json={}, headers=headers)
    missing = client.post(f"/actions/{uuid.uuid4()}/undo", json={}, headers=headers)

    assert existing.status_code == 401, existing.text
    assert missing.status_code == 401, missing.text
    assert missing.json() == existing.json()
    assert executions_of(action["id"]) == []
