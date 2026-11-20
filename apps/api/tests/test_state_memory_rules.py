"""A non-factory agent's learned rules on the state router (#5801, ADR 0287 decision 7).

``POST /agents/{agent_id}/state/_rules`` stores one ``proposed`` rule per turn,
authorized only by the per turn memory write credential of ADR 0188, and
``GET`` answers the accepted rules a session loads at its start. Everything but
the text comes from the credential: the scope key from its ``binding`` claim,
the source turn from ``turn`` and the proposer from ``sender``.

Runs against the real app, Postgres and Valkey the conftest provisions; nothing
is mocked. Tokens are minted with the shared ``sandbox_token`` module, and the
handoff cases mint them with the worker's own code (``curie_worker.binding``)
and feed the read route's answer to the runner's own renderer.
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import personal_enrollment_support as enrollment
import personal_execution_support as pe
import pytest
from curie_api.config import get_settings
from curie_api.crud.console import DEFAULT_TENANT_ID
from curie_internal.memory_rules import rule_number_lock_key
from curie_internal.sandbox_token import mint

_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time
CHANNEL_1 = "C0EXAMPLE1"
CHANNEL_2 = "C0EXAMPLE2"
BINDING_1 = f"slack:{CHANNEL_1}"
BINDING_2 = f"slack:{CHANNEL_2}"
# Another agent's own channels: one agent per route.
OTHER_CHANNELS = ("C0EXAMPLE3", "C0EXAMPLE4")
OTHER_BINDING = f"slack:{OTHER_CHANNELS[0]}"
SENDER = "U0SENDER01"
RULE = "Always use British English spelling in replies."
REASON = "The requester asked for it as a standing instruction."
STAMP = datetime(2026, 11, 20, 9, 0, tzinfo=UTC)
CLOSE_URL = "/v1/internal/memory/closed-turns"

READ_ONLY = "memory credential is read-only"
TURN_CREDENTIAL_ONLY = "rules are proposed only with a turn credential"
OTHER_AGENT = "credential is for another agent"
TURN_ENDED = "this conversation's turn has ended"


# --- setup ---------------------------------------------------------------------------


def _agent(
    client: Any,
    auth_headers: dict[str, str],
    *,
    proposals: bool = True,
    name: str = "rules",
    channels: tuple[str, str] = (CHANNEL_1, CHANNEL_2),
) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"{name}-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channels[0]},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    aid: str = created.json()["id"]
    second = client.post(
        f"/agents/{aid}/channels",
        json={"kind": "slack", "address": channels[1]},
        headers=auth_headers,
    )
    assert second.status_code == 201, second.text
    _switch(client, auth_headers, aid, proposals)
    return aid


def _switch(client: Any, auth_headers: dict[str, str], aid: str, on: bool) -> None:
    patched = client.patch(
        f"/agents/{aid}", json={"memory_rule_proposals": on}, headers=auth_headers
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["memory_rule_proposals"] is on


def _token(aid: str, claims: dict[str, str | None] | None, *, scope: str = "state") -> str:
    return mint(get_settings().api_key, agent=aid, scope=scope, exp=_FAR_FUTURE, claims=claims)


def _turn() -> str:
    return f"evt-{uuid.uuid4().hex}#1"


def _writer(
    aid: str, *, binding: str | None = BINDING_1, turn: str | None = None, sender: str = SENDER
) -> dict[str, str]:
    claims = {"binding": binding, "memory": "write", "sender": sender, "turn": turn or _turn()}
    return {"X-API-Key": _token(aid, claims)}


def _reader(aid: str, *, binding: str | None = BINDING_1) -> dict[str, str]:
    return {"X-API-Key": _token(aid, {"binding": binding, "memory": "read"})}


def _legacy(aid: str) -> dict[str, str]:
    return {"X-API-Key": _token(aid, None)}


def _app(aid: str) -> dict[str, str]:
    return {"X-API-Key": _token(aid, None, scope="state.app")}


def _url(aid: str) -> str:
    return f"/agents/{aid}/state/_rules"


RULES_PATH = "/agents/{agent_id}/state/_rules"


def _declared_schema(client: Any, method: str, status: int) -> dict[str, Any]:
    """The JSON body schema the app's own OpenAPI declares for one answer of the
    rules route, with the document's components alongside so its ``$ref``s resolve."""

    document = client.app.openapi()
    declared = document["paths"][RULES_PATH][method]["responses"][str(status)]
    schema: dict[str, Any] = declared["content"]["application/json"]["schema"]
    return {**schema, "components": document["components"]}


def _assert_matches_contract(client: Any, response: Any, method: str) -> None:
    import jsonschema

    jsonschema.validate(response.json(), _declared_schema(client, method, response.status_code))


def _propose(client: Any, aid: str, headers: dict[str, str], body: Any = None) -> Any:
    payload = {"rule": RULE, "reason": REASON} if body is None else body
    return client.post(_url(aid), json=payload, headers=headers)


def _close(client: Any, aid: str, turn: str) -> None:
    closed = client.post(
        CLOSE_URL,
        json={"agent_id": aid, "turn": turn},
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    assert closed.status_code == 204, closed.text


# --- reading the store back ----------------------------------------------------------


def _rules(aid: str) -> list[dict[str, Any]]:
    return pe.rows(
        "SELECT id, scope_kind, scope_key, scope_label, number, state, text, reason, "
        "proposed_by, source_turn, source_request_id, source_lineage_id, source_url, version "
        "FROM curie.memory_rules WHERE agent_id = :agent ORDER BY proposed_at, id",
        {"agent": aid},
    )


def _events(aid: str) -> list[dict[str, Any]]:
    return pe.rows(
        "SELECT rule_id, event, from_state, to_state, actor_kind, actor, principal_id "
        "FROM curie.memory_rule_events WHERE agent_id = :agent ORDER BY id",
        {"agent": aid},
    )


def _stored(aid: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return _rules(aid), _events(aid)


def _code(response: Any) -> str:
    detail = response.json()["detail"]
    assert isinstance(detail, dict), detail
    assert detail["message"] and detail["fix"], detail
    return str(detail["code"])


def _refused_plain(response: Any, status: int, reason: str) -> None:
    assert response.status_code == status, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json()["detail"] == reason


def _accepted_proposal(response: Any) -> dict[str, Any]:
    assert response.status_code == 201, response.text
    assert response.headers["Cache-Control"] == "no-store"
    found: dict[str, Any] = response.json()
    assert found["state"] == "proposed"
    return found


# --- AC1: a proposal lands in the credential's own scope -----------------------------


def test_channel_proposal_is_stored_proposed_in_its_binding_scope(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: pytest.LogCaptureFixture
) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    with caplog.at_level(logging.DEBUG):
        response = _propose(client, aid, _writer(aid, turn=turn))

    found = _accepted_proposal(response)
    ((row,), (event,)) = _stored(aid)
    assert row == {
        "id": uuid.UUID(found["id"]),
        "scope_kind": "binding",
        "scope_key": BINDING_1,
        "scope_label": BINDING_1,
        "number": 1,
        "state": "proposed",
        "text": RULE,
        "reason": REASON,
        "proposed_by": SENDER,
        "source_turn": turn,
        "source_request_id": None,
        "source_lineage_id": None,
        "source_url": None,
        "version": 1,
    }
    assert event == {
        "rule_id": row["id"],
        "event": "propose",
        "from_state": None,
        "to_state": "proposed",
        "actor_kind": "agent",
        "actor": aid,
        "principal_id": None,
    }
    assert {key: found[key] for key in ("number", "label", "scope_kind", "scope_key")} == {
        "number": 1,
        "label": "R1",
        "scope_kind": "binding",
        "scope_key": BINDING_1,
    }
    assert found["proposed_by"] == SENDER and found["accepted_by"] is None
    # The text and the reason never reach a log line.
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert RULE not in logged and REASON not in logged

    # The next turn's proposal takes the next free number of the same scope.
    again = _accepted_proposal(
        _propose(client, aid, _writer(aid), {"rule": "Be brief.", "reason": REASON})
    )
    assert (again["number"], again["scope_key"]) == (2, BINDING_1)


def test_agent_scope_proposal_lands_in_the_agent_scope(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, name="agent-scope")
    name = client.get(f"/agents/{aid}", headers=auth_headers).json()["name"]
    # A binding scope rule first: agent scope numbers are their own.
    _accepted_proposal(_propose(client, aid, _writer(aid)))
    turn = _turn()
    body = {"rule": RULE, "reason": REASON, "scope": "agent"}
    found = _accepted_proposal(_propose(client, aid, _writer(aid, turn=turn), body))

    assert (found["scope_kind"], found["scope_key"], found["number"]) == ("agent", "", 1)
    (row,) = [rule for rule in _rules(aid) if rule["scope_kind"] == "agent"]
    assert (row["scope_key"], row["scope_label"], row["number"]) == ("", name, 1)
    assert (row["source_turn"], row["proposed_by"], row["state"]) == (turn, SENDER, "proposed")


@pytest.mark.parametrize(
    "extra",
    [
        {"scope_key": BINDING_2},
        {"state": "accepted"},
        {"proposed_by": "U0FORGED01"},
    ],
    ids=["scope_key", "state", "proposed_by"],
)
def test_extra_body_fields_are_refused_and_nothing_is_stored(
    client: Any, auth_headers: dict[str, str], clean_db: None, extra: dict[str, str]
) -> None:
    aid = _agent(client, auth_headers)
    writer = _writer(aid)
    response = _propose(client, aid, writer, {"rule": RULE, "reason": REASON, **extra})

    assert response.status_code == 422, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert _code(response) == "invalid_body"
    for value in (*extra.values(), RULE, REASON):
        assert value not in response.text
    assert _stored(aid) == ([], [])
    # Liveness: the same credential's well formed proposal is stored.
    _accepted_proposal(_propose(client, aid, writer))


@pytest.mark.parametrize(
    "body",
    [
        {"rule": RULE, "reason": REASON, "scope": "repository"},
        {"rule": RULE},
        {"rule": 7, "reason": REASON},
        [RULE, REASON],
    ],
    ids=["unknown_scope", "missing_reason", "rule_not_a_string", "not_an_object"],
)
def test_malformed_body_is_refused_without_echo(
    client: Any, auth_headers: dict[str, str], clean_db: None, body: Any
) -> None:
    aid = _agent(client, auth_headers)
    writer = _writer(aid)
    response = _propose(client, aid, writer, body)
    assert response.status_code == 422, response.text
    assert _code(response) == "invalid_body"
    assert RULE not in response.text
    assert _stored(aid) == ([], [])
    _accepted_proposal(_propose(client, aid, writer))


# --- AC2: refusals, each storing nothing ---------------------------------------------


def test_boot_read_credential_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _refused_plain(_propose(client, aid, _reader(aid)), 403, READ_ONLY)
    assert _stored(aid) == ([], [])
    _accepted_proposal(_propose(client, aid, _writer(aid)))


@pytest.mark.parametrize("credential", ["platform", "app", "legacy"])
def test_credentials_other_than_a_turn_credential_are_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, credential: str
) -> None:
    aid = _agent(client, auth_headers)
    headers = {"platform": auth_headers, "app": _app(aid), "legacy": _legacy(aid)}[credential]
    _refused_plain(_propose(client, aid, headers), 403, TURN_CREDENTIAL_ONLY)
    assert _stored(aid) == ([], [])
    _accepted_proposal(_propose(client, aid, _writer(aid)))


@pytest.mark.parametrize("missing", ["binding", "sender", "turn"])
def test_write_credential_missing_a_claim_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, missing: str
) -> None:
    aid = _agent(client, auth_headers)
    claims: dict[str, str | None] = {
        "binding": BINDING_1,
        "memory": "write",
        "sender": SENDER,
        "turn": _turn(),
    }
    claims.pop(missing)
    response = _propose(client, aid, {"X-API-Key": _token(aid, claims)})
    assert response.status_code == 403, response.text
    assert _stored(aid) == ([], [])
    _accepted_proposal(_propose(client, aid, _writer(aid)))


def test_another_agents_credential_is_refused_403(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    other = _agent(client, auth_headers, name="other", channels=OTHER_CHANNELS)
    for headers in (_writer(other, binding=OTHER_BINDING), _reader(other, binding=OTHER_BINDING)):
        _refused_plain(_propose(client, aid, headers), 403, OTHER_AGENT)
        _refused_plain(client.get(_url(aid), headers=headers), 403, OTHER_AGENT)
        # The existing state routes keep their 401 for the same credential.
        assert client.get(f"/agents/{aid}/state/memory", headers=headers).status_code == 401
    # A credential signed with another key is not this platform's: 401, not 403.
    forged = mint("not-the-platform-key", agent=other, scope="state", exp=_FAR_FUTURE)
    refused = _propose(client, aid, {"X-API-Key": forged})
    assert refused.status_code == 401, refused.text
    assert refused.headers["Cache-Control"] == "no-store"
    assert _stored(aid) == ([], []) and _stored(other) == ([], [])
    # Liveness: each agent's own credential proposes on its own route.
    _accepted_proposal(_propose(client, aid, _writer(aid)))
    _accepted_proposal(_propose(client, other, _writer(other, binding=OTHER_BINDING)))


def test_switch_off_is_not_eligible(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, proposals=False)
    writer = _writer(aid)
    response = _propose(client, aid, writer)
    assert response.status_code == 403, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert _code(response) == "rule_proposal_not_eligible"
    assert RULE not in response.text
    assert _stored(aid) == ([], [])
    _switch(client, auth_headers, aid, True)
    _accepted_proposal(_propose(client, aid, writer))


def test_closed_turn_is_refused(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    _close(client, aid, turn)
    _refused_plain(_propose(client, aid, _writer(aid, turn=turn)), 403, TURN_ENDED)
    assert _stored(aid) == ([], [])
    # Liveness: a turn still open proposes.
    _accepted_proposal(_propose(client, aid, _writer(aid)))


def test_second_proposal_in_one_turn_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    writer = _writer(aid)
    _accepted_proposal(_propose(client, aid, writer))
    before = _stored(aid)
    for scope in ("channel", "agent"):
        body = {"rule": "Another standing rule.", "reason": REASON, "scope": scope}
        response = _propose(client, aid, writer, body)
        assert response.status_code == 409, response.text
        assert _code(response) == "rule_already_proposed"
        assert _stored(aid) == before
    # Liveness: the next turn proposes again.
    _accepted_proposal(_propose(client, aid, _writer(aid)))


def _await_number_lock_waiters(minimum: int, pending: list[Any]) -> None:
    """Wait until ``minimum`` requests wait on a transaction advisory lock."""

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        (found,) = pe.rows(
            "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = current_database() "
            "AND wait_event_type = 'Lock' AND wait_event = 'advisory'"
        )
        if int(found["n"]) >= minimum:
            return
        done = [request.result().text for request in pending if request.done()]
        assert not done, f"a request finished before it waited on the number lock: {done}"
    raise AssertionError(f"fewer than {minimum} requests waited on the number lock")


def test_two_racing_proposals_of_one_turn_store_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Both pass the unlocked check for the turn; the unique source turn index
    refuses the second once the first commits."""

    aid = _agent(client, auth_headers)
    writer = _writer(aid)
    key = rule_number_lock_key(uuid.UUID(aid), "binding", BINDING_1)
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        with enrollment.row_locked("SELECT pg_advisory_xact_lock(:key)", {"key": key}):
            pending = [pool.submit(_propose, client, aid, writer) for _ in range(2)]
            _await_number_lock_waiters(2, pending)
        responses = [request.result(timeout=60) for request in pending]
    finally:
        pool.shutdown(wait=True)

    assert sorted(response.status_code for response in responses) == [201, 409]
    (refused,) = [response for response in responses if response.status_code == 409]
    assert _code(refused) == "rule_already_proposed"
    rules, events = _stored(aid)
    assert [rule["number"] for rule in rules] == [1]
    assert [event["event"] for event in events] == ["propose"]


@pytest.mark.parametrize("field", ["rule", "reason"])
def test_rule_over_240_bytes_is_too_large(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str
) -> None:
    aid = _agent(client, auth_headers)
    writer = _writer(aid)
    long = "x" * 241
    body = {"rule": RULE, "reason": REASON, field: long}
    response = _propose(client, aid, writer, body)
    assert response.status_code == 422, response.text
    assert _code(response) == "too_large"
    assert long not in response.text
    assert _stored(aid) == ([], [])
    # Liveness: 240 bytes is within the bound.
    found = _accepted_proposal(_propose(client, aid, writer, {**body, field: "x" * 240}))
    assert len(found[field if field == "reason" else "text"]) == 240


def test_database_failure_is_503_and_logs_no_text(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: pytest.LogCaptureFixture
) -> None:
    """Postgres refuses a NUL in a text column, so the insert itself fails; the
    statement error, which displays the bound rule and reason, is dropped."""

    aid = _agent(client, auth_headers)
    with caplog.at_level(logging.DEBUG):
        response = _propose(client, aid, _writer(aid, sender="U0\x00BROKEN"))
    assert response.status_code == 503, response.text
    assert _code(response) == "rule_proposal_unavailable"
    assert RULE not in response.text and REASON not in response.text
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert RULE not in logged and REASON not in logged
    assert _stored(aid) == ([], [])
    _accepted_proposal(_propose(client, aid, _writer(aid)))


# --- AC3: the session start read -----------------------------------------------------


@pytest.fixture
def decider() -> Iterator[uuid.UUID]:
    """A person of the default tenant, for seeded decisions; removed afterwards."""

    principal_id = uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
        "VALUES (:id, :tenant, :subject, 'human', 'active', 'Rules Decider')",
        {"id": principal_id, "tenant": DEFAULT_TENANT_ID, "subject": f"rules-{principal_id}"},
    )
    yield principal_id
    pe.rows(
        "DELETE FROM curie.memory_rules WHERE :id IN (accepted_by, rejected_by, removed_by)",
        {"id": principal_id},
    )
    pe.rows("DELETE FROM curie.principals WHERE id = :id", {"id": principal_id})


def _seed(
    aid: str,
    decider: uuid.UUID,
    *,
    state: str,
    number: int,
    text: str,
    scope_kind: str = "binding",
    scope_key: str = BINDING_1,
    minute: int = 0,
) -> uuid.UUID:
    (agent,) = pe.rows("SELECT tenant_id FROM curie.agents WHERE id = :id", {"id": aid})
    at = STAMP + timedelta(minutes=minute)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": agent["tenant_id"],
        "agent_id": aid,
        "scope_kind": scope_kind,
        "scope_key": scope_key,
        "scope_label": scope_key or "agent",
        "number": number,
        "state": state,
        "text": text,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "format_version": 1,
        "reason": REASON,
        "proposed_by": SENDER,
        "proposed_at": STAMP,
    }
    if state in ("accepted", "removed"):
        values |= {"accepted_by": decider, "accepted_at": at}
    if state == "rejected":
        values |= {"rejected_by": decider, "rejected_at": at}
    if state == "removed":
        values |= {"removed_by": decider, "removed_at": at + timedelta(seconds=1)}
    pe.rows(
        f"INSERT INTO curie.memory_rules ({', '.join(values)}) "
        f"VALUES ({', '.join(':' + name for name in values)})",
        values,
    )
    return values["id"]  # type: ignore[no-any-return]


AGENT_FIRST = "Answer in plain language."
AGENT_SECOND = "Name the source of every figure."
C1_FIRST = "Post summaries as a numbered list."
C1_SECOND = "Tag the finance lead on invoices."
C1_PROPOSED = "Proposed only, never loaded."
C1_REJECTED = "Rejected, never loaded."
C1_REMOVED = "Removed, never loaded."
C2_ACCEPTED = "Another channel's rule, never loaded here."


def _seed_world(aid: str, decider: uuid.UUID) -> dict[str, uuid.UUID]:
    """Accepted rules out of insertion order, plus every row the read must skip."""

    agent = {"scope_kind": "agent", "scope_key": ""}
    return {
        AGENT_SECOND: _seed(
            aid, decider, state="accepted", number=2, text=AGENT_SECOND, minute=5, **agent
        ),
        AGENT_FIRST: _seed(
            aid, decider, state="accepted", number=1, text=AGENT_FIRST, minute=1, **agent
        ),
        C1_SECOND: _seed(aid, decider, state="accepted", number=2, text=C1_SECOND, minute=4),
        C1_FIRST: _seed(aid, decider, state="accepted", number=1, text=C1_FIRST, minute=2),
        C1_PROPOSED: _seed(aid, decider, state="proposed", number=3, text=C1_PROPOSED),
        C1_REJECTED: _seed(aid, decider, state="rejected", number=4, text=C1_REJECTED, minute=3),
        C1_REMOVED: _seed(aid, decider, state="removed", number=5, text=C1_REMOVED, minute=3),
        C2_ACCEPTED: _seed(
            aid, decider, state="accepted", number=1, text=C2_ACCEPTED, scope_key=BINDING_2
        ),
    }


def test_read_answers_accepted_agent_then_binding_rules(
    client: Any, auth_headers: dict[str, str], clean_db: None, decider: uuid.UUID
) -> None:
    aid = _agent(client, auth_headers)
    ids = _seed_world(aid, decider)

    for headers in (_reader(aid), _writer(aid)):
        response = client.get(_url(aid), headers=headers)
        assert response.status_code == 200, response.text
        assert response.headers["Cache-Control"] == "no-store"
        found = response.json()
        assert found["proposals_enabled"] is True
        assert [(rule["scope_kind"], rule["text"]) for rule in found["rules"]] == [
            ("agent", AGENT_FIRST),
            ("agent", AGENT_SECOND),
            ("binding", C1_FIRST),
            ("binding", C1_SECOND),
        ]
        first = found["rules"][2]
        assert set(first) == {
            "id",
            "scope_kind",
            "scope_key",
            "scope_label",
            "number",
            "text",
            "accepted_at",
        }
        assert (first["id"], first["scope_key"], first["number"]) == (
            str(ids[C1_FIRST]),
            BINDING_1,
            1,
        )
        for hidden in (C1_PROPOSED, C1_REJECTED, C1_REMOVED, C2_ACCEPTED):
            assert hidden not in response.text

    # The other channel's credential reads its own binding's rules only.
    other = client.get(_url(aid), headers=_reader(aid, binding=BINDING_2)).json()
    assert [rule["text"] for rule in other["rules"]] == [AGENT_FIRST, AGENT_SECOND, C2_ACCEPTED]
    # A credential naming no binding reads the agent scope only.
    unbound = client.get(_url(aid), headers=_reader(aid, binding=None)).json()
    assert [rule["text"] for rule in unbound["rules"]] == [AGENT_FIRST, AGENT_SECOND]

    _switch(client, auth_headers, aid, False)
    off = client.get(_url(aid), headers=_reader(aid)).json()
    assert off["proposals_enabled"] is False
    # Accepted rules apply whatever the switch says.
    assert len(off["rules"]) == 4


def test_read_carries_at_most_sixteen_rules_per_scope(
    client: Any, auth_headers: dict[str, str], clean_db: None, decider: uuid.UUID
) -> None:
    aid = _agent(client, auth_headers)
    texts = [f"Keep rule {index:02d}." for index in range(17)]
    for index, text in enumerate(texts):
        _seed(aid, decider, state="accepted", number=index + 1, text=text, minute=index)
    found = client.get(_url(aid), headers=_reader(aid)).json()
    assert [rule["text"] for rule in found["rules"]] == texts[:16]


def test_read_without_memory_claims_lists_the_rules_key_value_namespace(
    client: Any, auth_headers: dict[str, str], clean_db: None, decider: uuid.UUID
) -> None:
    """``_rules`` is also a key/value namespace any state caller may write. The
    platform key, the bundle token and a legacy token list it as the generic
    namespace listing answers it; only a memory credential gets the rules."""

    aid = _agent(client, auth_headers)
    _seed(aid, decider, state="accepted", number=1, text=C1_FIRST)
    marker = "stored by a bundle under the rules namespace name"
    put = client.put(f"{_url(aid)}/bundle-key", json={"value": {"note": marker}}, headers=_app(aid))
    assert put.status_code == 200, put.text
    stored = put.json()
    assert (stored["namespace"], stored["key"]) == ("_rules", "bundle-key")

    for headers in (_app(aid), auth_headers, _legacy(aid)):
        listed = client.get(_url(aid), headers=headers)
        assert listed.status_code == 200, listed.text
        (entry,) = listed.json()
        assert set(entry) == {"namespace", "key", "value", "version", "updated_at"}
        assert {name: entry[name] for name in ("namespace", "key", "value", "version")} == {
            "namespace": "_rules",
            "key": "bundle-key",
            "value": {"note": marker},
            "version": stored["version"],
        }
        assert C1_FIRST not in listed.text
        _assert_matches_contract(client, listed, "get")

    rules = client.get(_url(aid), headers=_reader(aid))
    assert rules.status_code == 200, rules.text
    assert [rule["text"] for rule in rules.json()["rules"]] == [C1_FIRST]
    assert marker not in rules.text and "bundle-key" not in rules.text
    _assert_matches_contract(client, rules, "get")


def test_rules_route_does_not_shadow_other_state(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """``_rules`` is matched before the key/value routes; the rest are unchanged."""

    aid = _agent(client, auth_headers)
    put = client.put(f"/agents/{aid}/state/workflow/k", json={"value": 1}, headers=_writer(aid))
    assert put.status_code == 200, put.text
    assert client.get(f"/agents/{aid}/state/workflow", headers=_reader(aid)).status_code == 200
    assert client.get(_url(aid), headers=_reader(aid)).json() == {
        "proposals_enabled": True,
        "rules": [],
    }


def test_both_403_shapes_match_the_declared_contract(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A credential refusal answers a plain string ``detail`` and a coded refusal
    an object ``detail``; the generated contract declares both for the 403."""

    import jsonschema

    aid = _agent(client, auth_headers, proposals=False)
    plain = _propose(client, aid, _reader(aid))
    _refused_plain(plain, 403, READ_ONLY)
    coded = _propose(client, aid, _writer(aid))
    assert coded.status_code == 403, coded.text
    assert _code(coded) == "rule_proposal_not_eligible"

    for response in (plain, coded):
        _assert_matches_contract(client, response, "post")
    # The read's 403 is a credential refusal too.
    other = _agent(client, auth_headers, name="other", channels=OTHER_CHANNELS)
    foreign = client.get(_url(aid), headers=_reader(other, binding=OTHER_BINDING))
    _refused_plain(foreign, 403, OTHER_AGENT)
    _assert_matches_contract(client, foreign, "get")
    # The check has teeth: the coded schema alone refuses the string body.
    coded_only = {
        "$ref": "#/components/schemas/ChannelRuleRefusal",
        "components": client.app.openapi()["components"],
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(plain.json(), coded_only)


# --- handoffs: the worker's real credentials and the runner's real renderer ----------


def _worker_resolver() -> Any:
    from curie_worker.binding import BindingResolver
    from curie_worker.config import WorkerConfig

    # Constructed as apps/worker/tests/binding/test_memory_credential.py does.
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key=get_settings().api_key)  # type: ignore[attr-defined]
    return resolver


def _worker_resolved(aid: str) -> Any:
    from curie_worker.binding import ResolvedDeployment

    return ResolvedDeployment(
        agent_name="rules",
        agent_id=uuid.UUID(aid),
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        memory_writes=True,
    )


def test_handoff_worker_turn_credential_proposes_and_boot_credential_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Handoff (a): the credentials the worker really mints. The per turn
    credential from ``turn_memory_token`` proposes; the boot credential from
    ``boot_env`` (``memory: "read"``) is refused, and reads the rules."""

    aid = _agent(client, auth_headers)
    resolver, resolved = _worker_resolver(), _worker_resolved(aid)
    boot = resolver.boot_env(resolved, "thread-1", kind="slack", address=CHANNEL_1)
    boot_headers = {"X-API-Key": boot["CURIE_MEMORY_TOKEN"]}

    _refused_plain(_propose(client, aid, boot_headers), 403, READ_ONLY)
    assert _stored(aid) == ([], [])
    read = client.get(_url(aid), headers=boot_headers)
    assert read.status_code == 200, read.text

    turn = _turn()
    token = resolver.turn_memory_token(
        resolved,
        kind="slack",
        address=CHANNEL_1,
        thread_key="thread-1",
        sender=SENDER,
        turn=turn,
        ttl_s=300.0,
    )
    assert token is not None
    found = _accepted_proposal(_propose(client, aid, {"X-API-Key": token}))
    assert (found["scope_kind"], found["scope_key"], found["proposed_by"]) == (
        "binding",
        BINDING_1,
        SENDER,
    )
    ((row,), _) = _stored(aid)
    assert row["source_turn"] == turn


def test_handoff_read_answer_renders_through_the_runner(
    client: Any, auth_headers: dict[str, str], clean_db: None, decider: uuid.UUID
) -> None:
    """Handoff (b): the read route's real answer, fed to the runner's renderer,
    yields a section holding the accepted agent and channel rules and nothing
    proposed or of another channel."""

    from curie_runner.channel_memory_rules import render_channel_rules

    aid = _agent(client, auth_headers)
    _seed_world(aid, decider)
    response = client.get(_url(aid), headers=_reader(aid))
    assert response.status_code == 200, response.text

    rendered = render_channel_rules(response.json())

    assert rendered.proposals_enabled is True
    section = rendered.section
    assert isinstance(section, str)
    for shown in (AGENT_FIRST, AGENT_SECOND, C1_FIRST, C1_SECOND):
        assert shown in section, shown
    for hidden in (C1_PROPOSED, C1_REJECTED, C1_REMOVED, C2_ACCEPTED):
        assert hidden not in section, hidden
