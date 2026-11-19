"""Selecting accepted learned rules and deleting an agent that owns rules (#5795).

Real Postgres at the head revision. ``select_memory_rules`` has no production
caller yet (the accept and admission tickets add them), so the proof drives it
directly; ``delete_agent`` is the production caller the new tables already meet.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import personal_execution_support as pe
import pytest
from curie_api.crud.agents import delete_agent
from curie_api.models import DEFAULT_TENANT_ID
from curie_internal.memory_rules import (
    MAX_ACCEPTED_PER_SCOPE,
    AbsentMemoryRules,
    InvalidRule,
    ScopeOverfull,
    SelectedMemoryRules,
    canonical_text,
    rules_sha256,
    select_memory_rules,
)
from sqlalchemy.ext.asyncio import AsyncSession

STAMP = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
REPO = "github:github.com:101"
OTHER_REPO = "github:github.com:202"
BINDING = "slack:C0EXAMPLE1"


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"selection-{agent_id.hex[:8]}"},
    )
    return agent_id


def _principal() -> uuid.UUID:
    principal = uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status) "
        "VALUES (:id, :tenant, :subject, 'human', 'active')",
        {"id": principal, "tenant": DEFAULT_TENANT_ID, "subject": f"selection-{principal}"},
    )
    return principal


def _rule(agent: uuid.UUID, number: int | None, text: str, **changes: Any) -> uuid.UUID:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_ID,
        "agent_id": agent,
        "scope_kind": "repository",
        "scope_key": REPO,
        "scope_label": "acme-corp/acme-bot",
        "number": number,
        "state": "proposed",
        "text": text,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "format_version": 1,
        "reason": "A general rule.",
        "proposed_by": "reviewer-example",
        "proposed_at": STAMP,
        **changes,
    }
    pe.rows(
        f"INSERT INTO curie.memory_rules ({', '.join(values)}) "
        f"VALUES ({', '.join(':' + name for name in values)})",
        values,
    )
    return values["id"]  # type: ignore[no-any-return]


def _accepted(
    agent: uuid.UUID, principal: uuid.UUID, number: int, text: str, at: int = 0, **changes: Any
) -> uuid.UUID:
    return _rule(
        agent,
        number,
        text,
        state="accepted",
        accepted_by=principal,
        accepted_at=STAMP + timedelta(minutes=at),
        **changes,
    )


def _select(agent: uuid.UUID, kind: str = "repository", key: str = REPO) -> Any:
    async def run(session: AsyncSession) -> Any:
        selected = await select_memory_rules(
            session, agent_id=agent, scope_kind=kind, scope_key=key
        )
        await session.commit()
        return selected

    return pe.with_session(run)


def test_nothing_accepted_is_absent(clean_db: None) -> None:
    agent = _agent()
    assert _select(agent) == AbsentMemoryRules(reason="no_rules")
    # Rules in every other state of the scope select nothing.
    request = _work_item_request(agent)
    _rule(agent, None, "hidden candidate", state="candidate", source_request_id=request)
    _rule(agent, 1, "waiting for a person")
    _rule(agent, 2, "turned down", state="rejected", rejected_by=_principal(), rejected_at=STAMP)
    _rule(agent, None, "replaced", state="superseded")
    _rule(agent, None, "dropped", state="discarded")
    _rule(
        agent,
        3,
        "taken back",
        state="removed",
        accepted_by=(person := _principal()),
        accepted_at=STAMP,
        removed_by=person,
        removed_at=STAMP,
    )
    assert _select(agent) == AbsentMemoryRules(reason="no_rules")


def test_only_the_exact_scope_of_the_exact_agent_is_selected_in_acceptance_order(
    clean_db: None,
) -> None:
    agent, other_agent, person = _agent(), _agent(), _principal()
    # Number order differs from acceptance order, and two rules tie on time. The
    # tied ids sort opposite to their insertion order, so only ``ORDER BY id``
    # (not heap order) puts them right.
    low = uuid.UUID("00000000-0000-0000-0000-000000000001")
    high = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    _accepted(agent, person, 5, "Prefer small functions.", at=0)
    _accepted(agent, person, 9, "Tied nine.", at=1, id=high)
    _accepted(agent, person, 4, "Tied four.", at=1, id=low)
    _accepted(agent, person, 2, "Name tests after behavior.", at=3)
    # The same text and key elsewhere never leak in: another scope kind with the
    # same key, another key, the agent scope, and another agent.
    _accepted(agent, person, 1, "Prefer small functions.", scope_kind="binding", scope_key=REPO)
    _accepted(agent, person, 1, "Prefer small functions.", scope_key=OTHER_REPO)
    _accepted(agent, person, 1, "Prefer small functions.", scope_kind="agent", scope_key="")
    _accepted(other_agent, person, 1, "Prefer small functions.")

    selected = _select(agent)

    assert isinstance(selected, SelectedMemoryRules)
    assert selected.rule_numbers == (5, 4, 9, 2)
    assert selected.text == canonical_text(
        [
            (5, "Prefer small functions."),
            (4, "Tied four."),
            (9, "Tied nine."),
            (2, "Name tests after behavior."),
        ]
    )
    assert selected.sha256 == rules_sha256(selected.text)
    assert selected.count == 4
    assert selected.format_version == 1
    # The binding scope with the repository's key selects its own rule only.
    binding = _select(agent, "binding", REPO)
    assert isinstance(binding, SelectedMemoryRules)
    assert binding.text == "R1: Prefer small functions."


@pytest.mark.parametrize(
    ("kind", "key"),
    [("repository", OTHER_REPO), ("binding", BINDING), ("agent", "")],
)
def test_each_scope_selects_only_its_own_rule(clean_db: None, kind: str, key: str) -> None:
    agent, person = _agent(), _principal()
    _accepted(agent, person, 1, "Repository rule.", at=0)
    _accepted(agent, person, 1, "Other repository rule.", scope_key=OTHER_REPO)
    _accepted(agent, person, 1, "Binding rule.", scope_kind="binding", scope_key=BINDING)
    _accepted(agent, person, 1, "Agent rule.", scope_kind="agent", scope_key="")
    wanted = {
        OTHER_REPO: "Other repository rule.",
        BINDING: "Binding rule.",
        "": "Agent rule.",
    }[key]

    selected = _select(agent, kind, key)

    assert isinstance(selected, SelectedMemoryRules)
    assert selected.text == f"R1: {wanted}"
    assert selected.rule_numbers == (1,)


def test_a_scope_of_sixteen_selects_and_a_seventeenth_is_refused(clean_db: None) -> None:
    agent, person = _agent(), _principal()
    for number in range(1, MAX_ACCEPTED_PER_SCOPE + 1):
        _accepted(agent, person, number, f"Rule number {number}.", at=number)
    selected = _select(agent)
    assert isinstance(selected, SelectedMemoryRules)
    assert selected.count == MAX_ACCEPTED_PER_SCOPE
    assert list(selected.rule_numbers) == list(range(1, MAX_ACCEPTED_PER_SCOPE + 1))

    _accepted(agent, person, MAX_ACCEPTED_PER_SCOPE + 1, "One too many.", at=99)
    with pytest.raises(ScopeOverfull):
        _select(agent)


@pytest.mark.parametrize(("kind", "key"), [("team", "x"), ("repository", None), ("", "")])
def test_an_unknown_scope_is_refused_without_a_query(clean_db: None, kind: str, key: Any) -> None:
    with pytest.raises(InvalidRule):
        _select(_agent(), kind, key)


def _work_item_request(agent: uuid.UUID, issue: str = "1") -> uuid.UUID:
    item, request = uuid.uuid4(), uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 1, 2, :stamp, :stamp)",
        {
            "id": item,
            "issue": issue,
            "agent": agent,
            "conversation": f"selection-{item.hex[:8]}",
            "stamp": STAMP,
        },
    )
    pe.rows(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status) "
        "VALUES (:id, :item, 1, 'queued')",
        {"id": request, "item": item},
    )
    return request


def test_deleting_an_agent_removes_its_rules_even_when_a_candidate_names_a_request(
    clean_db: None,
) -> None:
    agent, person = _agent(), _principal()
    request = _work_item_request(agent)
    candidate = _rule(agent, None, "hidden candidate", state="candidate", source_request_id=request)
    other = _accepted(agent, person, 1, "An accepted rule.")
    pe.rows(
        "INSERT INTO curie.memory_rule_events "
        "(rule_id, tenant_id, agent_id, event, from_state, to_state, actor_kind, actor) "
        "VALUES (:rule, :tenant, :agent, 'propose', NULL, 'candidate', 'platform', 'factory')",
        {"rule": candidate, "tenant": DEFAULT_TENANT_ID, "agent": agent},
    )

    async def run(session: AsyncSession) -> None:
        await delete_agent(session, agent)

    pe.with_session(run)

    survivors = pe.rows(
        "SELECT id FROM curie.memory_rules WHERE id = ANY(:ids)", {"ids": [candidate, other]}
    )
    assert survivors == []
    assert pe.rows("SELECT id FROM curie.agents WHERE id = :id", {"id": agent}) == []
    assert pe.rows("SELECT id FROM curie.memory_rule_events") == []
