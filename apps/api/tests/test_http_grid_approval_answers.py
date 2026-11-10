"""Authenticated answers to a selected approval on an Enterprise Grid workspace receiver.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
organisation, workspace and user ids only. No live Grid installation is qualified.
@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.resumequeue import resume_event_id

from apps.api.tests.test_http_approval_answers import (
    answer,
    approver_role,
    resolve,
    sealed_actor,
    slack_evidence,
    unchanged,
)
from apps.api.tests.test_http_approval_creation import request_for
from apps.api.tests.test_http_approval_legacy_writers import created
from apps.api.tests.test_http_invocation_membership import GRID_ORG, GRID_WORKSPACE, SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import delivery_state, queued_invocation


@pytest.fixture
def grid_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21: a real Grid row."""
    with queued_invocation(synthetic_provider, valkey, runs_stream, grid=True) as case:
        case.provider = synthetic_provider
        case.request = request_for(case)
        before = delivery_state(valkey, runs_stream, case.scope["binding"])
        case.row = created(case)
        approver_role(case)
        case.stream_before = valkey.xrange(runs_stream)
        yield case
        for entry_id, fields in valkey.xrange(runs_stream)[len(case.stream_before):]:
            assert QueuedTurn.model_validate_json(fields["payload"]).event_id == resume_event_id(
                case.row["id"],
            )
            valkey.xdel(runs_stream, entry_id)
        assert delivery_state(valkey, runs_stream, case.scope["binding"]) == before


def grid_evidence(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-21: a block action from the registered workspace."""
    return {
        **slack_evidence(interaction_user_team_id=GRID_WORKSPACE, enterprise_ids=[GRID_ORG]),
        **changes,
    }


def grid_answer(case: SimpleNamespace, **changes: Any) -> tuple[bytes, dict[str, str]]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the same Grid evidence in the apr and the proof."""
    evidence = grid_evidence(**changes)
    proof = {**evidence, "enterprise_ids": tuple(evidence["enterprise_ids"])}
    return answer(case, apr_evidence=evidence, slack=proof)


def test_grid_answer_from_the_registered_workspace_resolves_in_the_organisation(
    grid_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21."""
    case = grid_case
    before = len(case.provider.requests)
    response = resolve(case, *grid_answer(case))
    assert response.status_code == 200, response.text
    _, capsule = sealed_actor(case, "approved")
    assert capsule.actor_scope is not None
    assert capsule.actor_scope.namespace.model_dump()["kind"] == "slack_enterprise"
    assert capsule.actor_scope.namespace.model_dump()["key"] == GRID_ORG
    assert capsule.actor_scope.native_id == SENDER
    approvals = [item for item in case.provider.requests[before:] if item["purpose"] == "approval"]
    assert approvals and all(
        item["scope"]["sender"]["namespace"]["kind"] == "slack_enterprise" for item in approvals
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"interaction_user_team_id": "T0EXAMPLESIBLING"},
        {"enterprise_ids": ["E0EXAMPLEPARTNER"]},
        {"enterprise_ids": []},
        {"enterprise_ids": [GRID_ORG, "is_enterprise_install"]},
    ],
)
def test_grid_answer_outside_the_registered_workspace_or_organisation_refuses(
    grid_case: SimpleNamespace, changes: dict[str, Any],
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21: before lookup or CAS."""
    case = grid_case
    before = len(case.provider.requests)
    response = resolve(case, *grid_answer(case, **changes))
    assert response.status_code in (401, 403), response.text
    unchanged(case, before)
