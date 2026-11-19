"""The API and the worker agree on the effective hook claim lease.

The process default budget is 600. The chart's 10800 is a deployment value,
not the value these constructors use when the variable is unset.
"""

from __future__ import annotations

import pytest
from curie_api.config import Settings
from curie_worker.config import WorkerConfig
from pydantic import ValidationError

BUDGET = "CURIE_DELIVERY_BUDGET_S"
LEASE = "CURIE_HOOK_CLAIM_LEASE_S"


def _prepare(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for name in (BUDGET, LEASE):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)


def _constructed(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str]
) -> tuple[Settings, WorkerConfig]:
    _prepare(monkeypatch, env)
    # Ignore a developer .env so only this case's environment is visible.
    return Settings(_env_file=None), WorkerConfig()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, 600),
        ({BUDGET: "1800"}, 1800),
        ({BUDGET: "600", LEASE: "7200"}, 7200),
        ({BUDGET: "600", LEASE: "600"}, 600),
    ],
    ids=["unset", "budget-only", "lease-above-budget", "lease-equal-budget"],
)
def test_effective_hook_claim_lease_agrees(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: int
) -> None:
    api, worker = _constructed(monkeypatch, env)
    assert api.effective_hook_claim_lease_s == worker.effective_hook_claim_lease_s == expected


@pytest.mark.parametrize(
    "env",
    [
        {BUDGET: "600", LEASE: "599"},
        {LEASE: ""},
    ],
    ids=["lease-shorter-than-budget", "empty-lease"],
)
def test_effective_hook_claim_lease_refuses_the_same_inputs(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str]
) -> None:
    _prepare(monkeypatch, env)
    with pytest.raises((ValidationError, ValueError)):
        Settings(_env_file=None)  # type: ignore[call-arg]
    with pytest.raises((ValidationError, ValueError)):
        WorkerConfig()
