"""Learned rules reach the implementer only, never a tracker result request (#5798).

A write-access human labels an issue on a declared tracker result binding (the
signed GitHub webhook the tracker result admission tests use), so the WorkItem
pins the accepted rules of its repository and admits one ``tracker_result``
request. That request is started through the functions the internal routes call,
then the worker's two memory rules routes are driven: it is served an absent
envelope and may only acknowledge ``none``. The control is the ordinary
implementation request of the same repository, served the selected envelope.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import hashlib
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_internal.memory_rules import canonical_text, repository_scope_key, rules_sha256
from forge_fakes.github import REPO_ID, GitHubAPI
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from test_factory_tracker_result_admission import (
    WITH_TICKET,
    _agent_id,
    _deployed,
    _label,
    _requests,
    _set_publication_policy,
)
from test_factory_tracker_results import _start_running

WORKER_HEADERS = {"X-Curie-Worker-Token": "factory-terminus-worker"}
PREFIX = "/v1/internal/work-items"
RULE = "Prefer early returns over nested branches."
RULE_TEXT = canonical_text([(1, RULE)])
RULE_DIGEST = rules_sha256(RULE_TEXT)


def _seed_rule(agent_id: str, forge: Any) -> None:
    """One accepted repository rule for the label's repository, as the accept route stores it.

    The scope key is the one admission derives: the code host's kind, its host (the
    stand-in forge's address) and the repository id.
    """

    host, port = forge.server_address

    [agent] = _rows(
        "SELECT tenant_id FROM curie.agents WHERE id = :id", {"id": uuid.UUID(agent_id)}
    )
    principal = uuid.uuid4()
    _rows_write(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
        "VALUES (:id, :tenant, :subject, 'human', 'active', 'Acme Owner')",
        {"id": principal, "tenant": agent["tenant_id"], "subject": f"rule-owner-{principal}"},
    )
    _rows_write(
        "INSERT INTO curie.memory_rules (id, tenant_id, agent_id, scope_kind, scope_key, "
        "scope_label, number, state, text, sha256, format_version, reason, proposed_by, "
        "proposed_at, accepted_by, accepted_at) VALUES (:id, :tenant, :agent, 'repository', "
        ":key, 'acme-corp/acme-bot', 1, 'accepted', :text, :sha, 1, "
        "'A reviewer asked for it.', 'reviewer-example', :stamp, :principal, :stamp)",
        {
            "id": uuid.uuid4(),
            "tenant": agent["tenant_id"],
            "agent": uuid.UUID(agent_id),
            "key": repository_scope_key("github", f"{host}:{port}", str(REPO_ID)),
            "text": RULE,
            "sha": hashlib.sha256(RULE.encode()).hexdigest(),
            "stamp": datetime(2026, 11, 19, 12, 0, tzinfo=UTC),
            "principal": principal,
        },
    )


def _rows_write(statement: str, params: dict[str, Any]) -> None:
    import asyncio

    from curie_api.config import get_settings
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _admitted(client: Any, github: GitHubAPI, number: int, *, declared: bool) -> Any:
    """Label one issue; return its single request row, its work item and its pin state."""

    agent_id = _agent_id()
    _set_publication_policy(client, agent_id, "auto")
    if declared:
        _deployed(client, agent_id, WITH_TICKET)
    _label(client, github, number)
    (request,) = _requests(number)
    [item] = _rows(
        "SELECT w.id, w.memory_rules_state, r.memory_rules_evidence "
        "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE r.id = :id",
        {"id": request["id"]},
    )
    return request, item


def _context(client: Any, request_id: uuid.UUID, epoch: int) -> Any:
    return client.post(
        f"{PREFIX}/requests/{request_id}/memory-rules-context",
        json={"runtime_epoch": epoch},
        headers=WORKER_HEADERS,
    )


def _ack(client: Any, request_id: uuid.UUID, item_id: uuid.UUID, epoch: int, outcome: str) -> Any:
    acknowledgment: dict[str, Any] = {
        "envelope_version": 1,
        "work_item_id": str(item_id),
        "execution_request_id": str(request_id),
        "execution_fence": epoch,
        "outcome": outcome,
    }
    if outcome == "applied":
        acknowledgment["sha256"] = RULE_DIGEST
    return client.post(
        f"{PREFIX}/requests/{request_id}/memory-rules-context/acknowledgment",
        json={"runtime_epoch": epoch, "acknowledgment": acknowledgment},
        headers=WORKER_HEADERS,
    )


def test_a_tracker_result_request_is_served_no_rules_and_may_only_acknowledge_none(
    admitted: Any,
) -> None:
    client, github, forge = admitted
    _seed_rule(_agent_id(), forge)

    request, item = _admitted(client, github, 9201, declared=True)

    assert request["completion_contract"] == "tracker_result"
    # The WorkItem pinned the rules at admission; the request does not carry them.
    assert item["memory_rules_state"] == "selected"
    assert item["memory_rules_evidence"] == "none"
    epoch = _start_running(client, request["id"])

    served = _context(client, request["id"], epoch)

    assert served.status_code == 200, served.text
    assert served.json()["selection"] == {"kind": "absent", "reason": "no_rules"}
    applied = _ack(client, request["id"], item["id"], epoch, "applied")
    assert applied.status_code == 409, applied.text
    assert applied.json()["detail"]["code"] == "acknowledgment_mismatch"
    kept = _ack(client, request["id"], item["id"], epoch, "none")
    assert kept.status_code == 200, kept.text
    assert kept.json() == {"evidence": "none"}


def test_the_implementation_request_of_the_same_repository_is_served_the_rules(
    admitted: Any,
) -> None:
    # The control: only the completion contract differs from the test above.
    client, github, forge = admitted
    _seed_rule(_agent_id(), forge)

    request, item = _admitted(client, github, 9202, declared=False)

    assert request["completion_contract"] == "implementation"
    assert item["memory_rules_state"] == "selected"
    assert item["memory_rules_evidence"] == "pending"
    epoch = _start_running(client, request["id"])

    served = _context(client, request["id"], epoch)

    assert served.status_code == 200, served.text
    assert served.json()["selection"] == {
        "kind": "selected",
        "rule_numbers": [1],
        "sha256": RULE_DIGEST,
        "format_version": 1,
        "text": RULE_TEXT,
    }
    assert _ack(client, request["id"], item["id"], epoch, "none").status_code == 409
    applied = _ack(client, request["id"], item["id"], epoch, "applied")
    assert applied.status_code == 200, applied.text
    assert applied.json() == {"evidence": "applied"}
