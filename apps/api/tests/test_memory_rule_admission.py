"""Accepted learned rules are pinned at a WorkItem's first admission (#5798).

Every case drives the trusted internal admission route (and, for continuations,
the lifecycle functions the internal routes call) against real Postgres.
Accepted rules are seeded directly in ``curie.memory_rules``, the storage the
accept route writes, never through another ticket's route. The pin is the
WorkItem's ``memory_rules_*`` columns plus one snapshot row; every execution
request of the WorkItem copies its evidence state.
"""

# ruff: noqa: F401, F811  (fixtures are imported, then requested)

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import personal_execution_support as support
import pytest
import test_personal_execution_lifecycle as scenarios
from curie_api import workitem_dispatch
from curie_api.workitems.lifecycle import WorkItemOutcome
from curie_internal.memory_rules import canonical_text, rules_sha256
from fastapi.testclient import TestClient
from test_personal_execution_lifecycle import WORKER_HEADERS, api, world

REPOSITORY_KEY = "github:github.com:101"
STAMP = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
FIRST = "Prefer early returns over nested branches."
SECOND = "Name every test after the behavior it proves."
LATER = "Keep each migration reversible."
# A canary every rule text carries; no read but the envelope may hold it.
CANARY = "CANARY-LEARNED-5798"


def seed_rule(
    tenant: uuid.UUID,
    agent: uuid.UUID,
    principal: uuid.UUID,
    number: int,
    text: str,
    *,
    at: int = 0,
    key: str = REPOSITORY_KEY,
) -> uuid.UUID:
    """One accepted repository rule, written as the accept route stores it."""

    rule_id = uuid.uuid4()
    support.rows(
        "INSERT INTO curie.memory_rules (id, tenant_id, agent_id, scope_kind, scope_key, "
        "scope_label, number, state, text, sha256, format_version, reason, proposed_by, "
        "proposed_at, accepted_by, accepted_at) VALUES (:id, :tenant, :agent, 'repository', "
        ":key, 'acme-corp/acme-bot', :number, 'accepted', :text, :sha, 1, "
        "'A reviewer asked for it.', 'reviewer-example', :stamp, :principal, :accepted)",
        {
            "id": rule_id,
            "tenant": tenant,
            "agent": agent,
            "key": key,
            "number": number,
            "text": text,
            "sha": hashlib.sha256(text.encode()).hexdigest(),
            "stamp": STAMP,
            "principal": principal,
            "accepted": STAMP + timedelta(minutes=at),
        },
    )
    return rule_id


def remove_rule(rule_id: uuid.UUID, principal: uuid.UUID) -> None:
    support.rows(
        "UPDATE curie.memory_rules SET state = 'removed', removed_by = :p, removed_at = now() "
        "WHERE id = :id",
        {"id": rule_id, "p": principal},
    )


def _two_rules(world: support.World, agent: uuid.UUID) -> tuple[uuid.UUID, uuid.UUID]:
    return (
        seed_rule(world.tenant_id, agent, world.owner_id, 1, f"{FIRST} {CANARY}", at=0),
        seed_rule(world.tenant_id, agent, world.owner_id, 4, f"{SECOND} {CANARY}", at=1),
    )


TWO_TEXT = canonical_text([(1, f"{FIRST} {CANARY}"), (4, f"{SECOND} {CANARY}")])
TWO_DIGEST = rules_sha256(TWO_TEXT)


def _item(work_item_id: uuid.UUID) -> dict[str, Any]:
    [row] = support.rows(
        "SELECT memory_rules_state, memory_rules_count, memory_rules_sha256, "
        "memory_rules_format_version FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )
    return row


def _snapshot(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return support.rows(
        "SELECT rule_numbers, text, sha256 FROM curie.work_item_memory_rule_snapshots "
        "WHERE work_item_id = :id",
        {"id": work_item_id},
    )


def _evidence(work_item_id: uuid.UUID) -> dict[uuid.UUID, str]:
    return {
        row["id"]: row["memory_rules_evidence"]
        for row in support.rows(
            "SELECT id, memory_rules_evidence FROM curie.execution_requests "
            "WHERE work_item_id = :id",
            {"id": work_item_id},
        )
    }


def _assert_two_pinned(work_item_id: uuid.UUID) -> None:
    assert _item(work_item_id) == {
        "memory_rules_state": "selected",
        "memory_rules_count": 2,
        "memory_rules_sha256": TWO_DIGEST,
        "memory_rules_format_version": 1,
    }
    assert _snapshot(work_item_id) == [
        {"rule_numbers": [1, 4], "text": TWO_TEXT, "sha256": TWO_DIGEST}
    ]


def _post(api: TestClient, facts: workitem_dispatch.Admission) -> Any:
    return api.post(
        f"{support.INTERNAL_PREFIX}/admissions",
        json=support.facts_json(facts),
        headers=WORKER_HEADERS,
    )


# --- first admission ---------------------------------------------------------------


def test_two_accepted_rules_pin_selected_with_the_canonical_digest(
    api: TestClient, world: support.World, auth_headers: dict[str, str]
) -> None:
    agent = support.insert_agent(world.tenant_id)
    _two_rules(world, agent)
    # Another repository's and a still proposed rule are not this scope's.
    seed_rule(
        world.tenant_id, agent, world.owner_id, 9, "Other repository.", key="github:github.com:202"
    )
    support.rows(
        "INSERT INTO curie.memory_rules (id, tenant_id, agent_id, scope_kind, scope_key, "
        "scope_label, number, state, text, sha256, format_version, reason, proposed_by, "
        "proposed_at) VALUES (:id, :t, :a, 'repository', :key, 'acme-corp/acme-bot', 7, "
        "'proposed', 'Not yet accepted.', :sha, 1, 'Proposed.', 'reviewer-example', now())",
        {
            "id": uuid.uuid4(),
            "t": world.tenant_id,
            "a": agent,
            "key": REPOSITORY_KEY,
            "sha": hashlib.sha256(b"Not yet accepted.").hexdigest(),
        },
    )
    facts = support.facts(agent, issue_number=579801)

    response = _post(api, facts)

    assert response.status_code == 200, response.text
    work_item = response.json()["work_item"]
    assert work_item["memory_rules"] == {
        "state": "selected",
        "count": 2,
        "rule_numbers": [1, 4],
        "sha256": TWO_DIGEST,
        "format_version": 1,
    }
    assert response.json()["request"]["memory_rules_evidence"] == "pending"
    assert CANARY not in response.text
    item_id = uuid.UUID(work_item["id"])
    _assert_two_pinned(item_id)
    assert set(_evidence(item_id).values()) == {"pending"}

    # The operator read shows the numbers and digest, never the text.
    for path in (f"/work-items/{item_id}", f"/work-items?agent_id={agent}"):
        read = api.get(path, headers=auth_headers)
        assert read.status_code == 200, read.text
        assert CANARY not in read.text and FIRST not in read.text
        projected = read.json().get("items", [read.json()])[0]
        assert projected["memory_rules"] == work_item["memory_rules"]
        assert [r["memory_rules_evidence"] for r in projected["requests"]] == ["pending"]
    internal = api.get(
        f"{support.INTERNAL_PREFIX}/requests/{facts.request_id}", headers=WORKER_HEADERS
    )
    assert internal.status_code == 200, internal.text
    assert CANARY not in internal.text


def test_a_repository_with_no_accepted_rule_pins_absent(
    api: TestClient, world: support.World, auth_headers: dict[str, str]
) -> None:
    agent = support.insert_agent(world.tenant_id)
    admitted = support.admit_service(
        api, WORKER_HEADERS, agent, issue_number=579802, conversation="1700000000.000200"
    )

    assert _item(admitted.work_item_id) == {
        "memory_rules_state": "absent",
        "memory_rules_count": 0,
        "memory_rules_sha256": None,
        "memory_rules_format_version": None,
    }
    assert _snapshot(admitted.work_item_id) == []
    assert set(_evidence(admitted.work_item_id).values()) == {"none"}
    read = api.get(f"/work-items/{admitted.work_item_id}", headers=auth_headers)
    assert read.json()["memory_rules"] == {
        "state": "absent",
        "count": 0,
        "rule_numbers": [],
        "sha256": None,
        "format_version": None,
    }


def test_accept_and_remove_after_admission_change_only_the_next_work_item(
    api: TestClient, world: support.World
) -> None:
    agent = support.insert_agent(world.tenant_id)
    first_rule, _ = _two_rules(world, agent)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent, issue_number=579803)
    epoch = support.start_request(first.request_id)

    # A rule accepted and a rule removed while the ticket runs.
    seed_rule(world.tenant_id, agent, world.owner_id, 5, LATER, at=2)
    remove_rule(first_rule, world.owner_id)

    # A revision admitted later on the same WorkItem keeps its pin.
    revision = support.facts(
        agent, issue_number=579803, requester=support.OTHER_REQUESTER, objective="Follow up"
    )
    revised = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(revised, WorkItemOutcome), revised
    assert revised.request is not None
    assert revised.request.memory_rules_evidence == "pending"
    assert revised.work_item.memory_rules.rule_numbers == [1, 4]
    support.finish_failed(first.request_id, epoch)
    _assert_two_pinned(first.work_item_id)
    assert set(_evidence(first.work_item_id).values()) == {"pending"}

    # The next ticket reads the set as it is now: R4 and R5, not R1.
    second = support.admit_personal(
        api,
        WORKER_HEADERS,
        world,
        agent,
        issue_number=579804,
        conversation="1700000000.000300",
    )
    expected = canonical_text([(4, f"{SECOND} {CANARY}"), (5, LATER)])
    assert _snapshot(second.work_item_id) == [
        {"rule_numbers": [4, 5], "text": expected, "sha256": rules_sha256(expected)}
    ]
    assert _item(second.work_item_id)["memory_rules_count"] == 2


@pytest.mark.parametrize(
    "scenario",
    [
        scenarios.test_revision_from_another_requester_snapshots_the_original_owner,
        scenarios.test_relabel_and_base_reconcile_readmissions_keep_the_original_owner,
        scenarios.test_ci_fix_hold_keeps_the_owner_on_the_running_request,
        scenarios.test_owner_lost_successor_carries_the_original_owner,
    ],
    ids=["revision", "relabel", "ci-repair", "recovery"],
)
def test_every_request_of_a_pinned_work_item_copies_pending_evidence(
    api: TestClient, world: support.World, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    agents: list[uuid.UUID] = []
    insert_agent = support.insert_agent

    def insert_with_rules(tenant_id: uuid.UUID, **kwargs: Any) -> uuid.UUID:
        agent = insert_agent(tenant_id, **kwargs)
        _two_rules(world, agent)
        agents.append(agent)
        return agent

    monkeypatch.setattr(support, "insert_agent", insert_with_rules)
    scenario(api, world)
    # A rule accepted after every continuation changes none of them.
    seed_rule(world.tenant_id, agents[0], world.owner_id, 5, LATER, at=2)

    items = support.rows("SELECT id FROM curie.work_items WHERE agent_id = :a", {"a": agents[0]})
    assert items
    for item in items:
        _assert_two_pinned(item["id"])
        assert set(_evidence(item["id"]).values()) == {"pending"}
    # Every continuation the scenario admitted is a request of a pinned WorkItem.
    assert len(
        support.rows(
            "SELECT r.id FROM curie.execution_requests r JOIN curie.work_items w "
            "ON w.id = r.work_item_id WHERE w.agent_id = :a",
            {"a": agents[0]},
        )
    ) >= (1 if scenario is scenarios.test_ci_fix_hold_keeps_the_owner_on_the_running_request else 2)


# --- fail closed at the bounds -------------------------------------------------------

# 13 digit numbers make 16 rules reach the 4096 byte snapshot bound exactly:
# 16 prefixes of 16 bytes, 15 line feeds, 15 texts of 240 bytes and one of 225.
_BASE = 1_000_000_000_000


def _bounded_rules(last: int) -> list[tuple[int, str]]:
    rules = [(_BASE + index, f"Rule {index:02d} " + "x" * 232) for index in range(15)]
    rules.append((_BASE + 15, "Rule 15 " + "y" * (last - 8)))
    return rules


@pytest.mark.parametrize("last, accepted", [(225, True), (226, False)])
def test_the_snapshot_byte_bound_pins_exactly_4096_and_refuses_one_more(
    api: TestClient, world: support.World, last: int, accepted: bool
) -> None:
    agent = support.insert_agent(world.tenant_id)
    rules = _bounded_rules(last)
    assert all(len(text.encode()) <= 240 for _, text in rules)
    for at, (number, text) in enumerate(rules):
        seed_rule(world.tenant_id, agent, world.owner_id, number, text, at=at)
    size = len(canonical_text(rules).encode())
    assert size == (4096 if accepted else 4097)

    response = _post(api, support.facts(agent, issue_number=579805))

    if accepted:
        assert response.status_code == 200, response.text
        assert response.json()["work_item"]["memory_rules"]["count"] == 16
        [snapshot] = _snapshot(uuid.UUID(response.json()["work_item"]["id"]))
        assert len(snapshot["text"].encode()) == 4096
    else:
        assert response.status_code == 409, response.text
        assert response.json() == {"detail": {"code": "memory_rules_unavailable"}}
        _assert_nothing_admitted()


@pytest.mark.parametrize("count, accepted", [(16, True), (17, False)])
def test_a_scope_over_sixteen_accepted_rules_refuses_admission(
    api: TestClient, world: support.World, count: int, accepted: bool
) -> None:
    agent = support.insert_agent(world.tenant_id)
    for number in range(1, count + 1):
        seed_rule(world.tenant_id, agent, world.owner_id, number, f"Rule {number}.", at=number)

    response = _post(api, support.facts(agent, issue_number=579806))

    if accepted:
        assert response.status_code == 200, response.text
        assert response.json()["work_item"]["memory_rules"]["rule_numbers"] == list(range(1, 17))
    else:
        assert response.status_code == 409, response.text
        assert response.json() == {"detail": {"code": "memory_rules_unavailable"}}
        _assert_nothing_admitted()


def _assert_nothing_admitted() -> None:
    assert support.rows("SELECT id FROM curie.work_items") == []
    assert support.rows("SELECT id FROM curie.execution_requests") == []
    assert support.rows("SELECT work_item_id FROM curie.work_item_memory_rule_snapshots") == []
    assert support.rows("SELECT work_item_id FROM curie.work_item_coding_snapshots") == []
