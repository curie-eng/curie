"""#4939: the worker carries an agent's per-run output allowance into CURIE_BUDGET.

The factory quickstart sets ``max_output_tokens_per_run`` to 250000 on the
dark factory agent; an operator may set another value. A null keeps the
platform default of 100000. The daily dollar cap is resolved independently
and never moves with the token allowance.
"""

from __future__ import annotations

import json
import uuid

import pytest
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("44444444-4444-4444-8444-444444444444")


def _resolved(max_output_tokens_per_run: int | None) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name="dark-factory",
        agent_id=_AGENT,
        version_id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        version_label="v1",
        bundle_ref="bundles/factory.zip",
        max_usd_per_day=5.0,
        max_output_tokens_per_run=max_output_tokens_per_run,
    )


def _resolver() -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()
    return resolver


@pytest.mark.parametrize(
    ("stored", "expected"),
    [(None, 100000), (250000, 250000), (400000, 400000)],
)
def test_budget_for_resolves_the_output_allowance(stored: int | None, expected: int) -> None:
    resolver = _resolver()
    assert resolver._config.default_max_output_tokens_per_run == 100000

    budget = resolver.budget_for(_resolved(stored))

    assert budget.max_output_tokens_per_run == expected
    assert budget.max_usd_per_day == 5.0


@pytest.mark.parametrize(
    ("stored", "expected"),
    [(None, 100000), (250000, 250000), (400000, 400000)],
)
def test_budget_for_reaches_the_sandbox_boot_env(stored: int | None, expected: int) -> None:
    env = _resolver().boot_env(_resolved(stored), "thread-1")

    budget = json.loads(env["CURIE_BUDGET"])

    assert budget["max_output_tokens_per_run"] == expected
    assert budget["max_usd_per_day"] == 5.0
