"""Authenticated answers to a selected approval on an Enterprise Grid workspace receiver.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
organisation, workspace and user ids only. No live Grid installation is qualified.
@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import redis

from apps.api.tests.test_http_approval_answers import (
    answer,
    resolve,
    sealed_actor,
    slack_evidence,
    unchanged,
)
from apps.api.tests.test_http_invocation_membership import GRID_ORG, GRID_WORKSPACE, SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_trusted_partner_namespaces import approval_case


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


def test_grid_answer_binds_the_workspace_and_organization(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    with approval_case(synthetic_provider, valkey, runs_stream, grid=True) as case:
        lookups = len(case.provider.requests)
        for changes in (
            {"interaction_user_team_id": "T0EXAMPLESIBLING"},
            {"enterprise_ids": ["E0EXAMPLEPARTNER"]},
            {"enterprise_ids": []},
            {"enterprise_ids": [GRID_ORG, "is_enterprise_install"]},
        ):
            response = resolve(case, *grid_answer(case, **changes))
            assert response.status_code == 401, response.text
            assert response.json() == {"detail": "missing or invalid approval principal"}
            unchanged(case, lookups)
            assert valkey.xrange(runs_stream) == case.stream_before
        response = resolve(case, *grid_answer(case))
        assert (response.status_code, response.json()["resolved_by"]) == (200, SENDER)
        _, seal = sealed_actor(case, "approved")
        namespace = seal.actor_scope.namespace.model_dump()
        assert namespace == case.scope["namespace"].model_dump()
        assert (namespace["kind"], namespace["key"]) == ("slack_enterprise", GRID_ORG)
