"""Authenticated answers to a selected approval on an Enterprise Grid workspace receiver.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
organisation, workspace and user ids only. No live Grid installation is qualified.
@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-21.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from apps.api.tests.test_http_approval_answers import (
    answer,
    slack_evidence,
)
from apps.api.tests.test_http_invocation_membership import GRID_ORG, GRID_WORKSPACE
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider


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


