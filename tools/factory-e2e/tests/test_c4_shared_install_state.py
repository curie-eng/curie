"""Pure rules for the shared kind install used by the admitted agent cases.

Each case must start from a verified clean install. These tests pin
leftover_state: which snapshot fields must be zero or empty, which must equal
the baseline, and that messages never leak env values or forge content.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402
from c4_shared_install import (  # noqa: E402
    CaseIsolationError,
    InstallSnapshot,
    leftover_state,
)


def clean_baseline() -> InstallSnapshot:
    return InstallSnapshot(
        agents=0,
        work_items=0,
        execution_requests=0,
        tenants=frozenset({"tenant-a", "tenant-b"}),
        sandbox_claims=frozenset(),
        publication_jobs=frozenset(),
        api_forges={
            "trackers": [{"name": "gitlab", "kind": "gitlab"}],
            "bindings": [],
        },
        worker_env=(
            ("CURIE_STREAM", "curie:runs"),
            ("CURIE_WORKER_TOKEN", '{"secretKeyRef": {"name": "curie-secrets"}}'),
        ),
        worker_pods=0,
        deployments=frozenset({"curie-api", "curie-worker", "curie-ui"}),
        network_policies=frozenset({"curie-runner-ingress"}),
        runs_stream_length=0,
        streams=frozenset({"curie:runs"}),
        claimed_sandboxes=frozenset(),
        terminating_pods=frozenset(),
        table_rows=(
            ("agents", 0),
            ("factory_poll_cursors", 2),
            ("tenants", 1),
            ("work_items", 0),
        ),
        persistent_keys=frozenset({"curie:runs"}),
    )


def test_equal_snapshot_is_clean() -> None:
    baseline = clean_baseline()
    assert leftover_state(baseline, baseline) == []


def test_separately_built_equal_snapshot_is_clean() -> None:
    baseline = clean_baseline()
    current = InstallSnapshot(
        agents=0,
        work_items=0,
        execution_requests=0,
        tenants=frozenset(["tenant-b", "tenant-a"]),
        sandbox_claims=frozenset(),
        publication_jobs=frozenset(),
        api_forges={
            "bindings": [],
            "trackers": [{"kind": "gitlab", "name": "gitlab"}],
        },
        worker_env=(
            ("CURIE_STREAM", "curie:runs"),
            ("CURIE_WORKER_TOKEN", '{"secretKeyRef": {"name": "curie-secrets"}}'),
        ),
        worker_pods=0,
        deployments=frozenset(["curie-ui", "curie-worker", "curie-api"]),
        network_policies=frozenset(["curie-runner-ingress"]),
        runs_stream_length=0,
        streams=frozenset(["curie:runs"]),
        claimed_sandboxes=frozenset(),
        terminating_pods=frozenset(),
        table_rows=tuple(
            sorted(
                [
                    ("work_items", 0),
                    ("tenants", 1),
                    ("factory_poll_cursors", 2),
                    ("agents", 0),
                ]
            )
        ),
        persistent_keys=frozenset(["curie:runs"]),
    )
    assert leftover_state(baseline, current) == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agents", 1),
        ("work_items", 2),
        ("execution_requests", 3),
        ("tenants", frozenset({"tenant-a", "tenant-b", "tenant-c"})),
        ("sandbox_claims", frozenset({"claim-1"})),
        ("publication_jobs", frozenset({"publish-1"})),
        ("api_forges", {"trackers": [], "bindings": []}),
        (
            "worker_env",
            (
                ("CURIE_STREAM", "curie:runs"),
                ("CURIE_WORKER_TOKEN", '{"secretKeyRef": {"name": "curie-secrets"}}'),
                ("EXTRA_FLAG", "1"),
            ),
        ),
        ("worker_pods", 1),
        ("deployments", frozenset({"curie-api", "curie-worker", "curie-ui", "extra"})),
        ("network_policies", frozenset()),
        ("runs_stream_length", 4),
        ("streams", frozenset({"curie:runs", "curie:other"})),
        ("claimed_sandboxes", frozenset({"sandbox-1"})),
        ("terminating_pods", frozenset({"pod-1"})),
        (
            "table_rows",
            (
                ("agents", 1),
                ("factory_poll_cursors", 2),
                ("tenants", 1),
                ("work_items", 0),
            ),
        ),
        ("persistent_keys", frozenset({"curie:runs", "curie:other"})),
    ],
)
def test_single_violation_yields_one_message_naming_field(field: str, value: object) -> None:
    baseline = clean_baseline()
    current = replace(baseline, **{field: value})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert field in messages[0]


def test_zero_rules_ignore_baseline() -> None:
    baseline = replace(
        clean_baseline(),
        agents=2,
        worker_pods=1,
        claimed_sandboxes=frozenset({"sandbox-1"}),
        terminating_pods=frozenset({"pod-1"}),
    )
    current = replace(baseline)
    messages = leftover_state(baseline, current)
    assert len(messages) == 4
    assert any("agents" in m for m in messages)
    assert any("worker_pods" in m for m in messages)
    assert any("claimed_sandboxes" in m for m in messages)
    assert any("terminating_pods" in m for m in messages)


def test_leftover_claimed_sandbox_and_terminating_pod_are_named() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        claimed_sandboxes=frozenset({"acme-claimed-sandbox"}),
        terminating_pods=frozenset({"acme-runner-pod"}),
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 2
    sandbox = [m for m in messages if "claimed_sandboxes" in m]
    pod = [m for m in messages if "terminating_pods" in m]
    assert len(sandbox) == 1
    assert len(pod) == 1
    assert "acme-claimed-sandbox" in sandbox[0]
    assert "acme-runner-pod" not in sandbox[0]
    assert "acme-runner-pod" in pod[0]
    assert "acme-claimed-sandbox" not in pod[0]


def test_extra_deployment_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, deployments=baseline.deployments | {"c4-held-worker"})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "deployments" in messages[0]
    assert "c4-held-worker" in messages[0]


def test_missing_deployment_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, deployments=baseline.deployments - {"curie-ui"})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "deployments" in messages[0]
    assert "curie-ui" in messages[0]


def test_extra_stream_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, streams=baseline.streams | {"curie:runs:c4-owner"})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "streams" in messages[0]
    assert "curie:runs:c4-owner" in messages[0]


def test_missing_stream_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, streams=frozenset())
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "curie:runs" in messages[0]


def test_extra_tenant_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, tenants=baseline.tenants | {"tenant-leftover"})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "tenants" in messages[0]
    assert "tenant-leftover" in messages[0]


def test_missing_tenant_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, tenants=baseline.tenants - {"tenant-b"})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "tenant-b" in messages[0]


def test_extra_network_policy_is_named() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        network_policies=baseline.network_policies | {"c4-held-worker-runner-ingress"},
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "network_policies" in messages[0]
    assert "c4-held-worker-runner-ingress" in messages[0]


def test_leftover_claim_and_job_are_named() -> None:
    baseline = clean_baseline()
    claims = replace(baseline, sandbox_claims=frozenset({"claim-xyz"}))
    jobs = replace(baseline, publication_jobs=frozenset({"publish-xyz"}))
    assert "claim-xyz" in leftover_state(baseline, claims)[0]
    assert "publish-xyz" in leftover_state(baseline, jobs)[0]


def test_worker_env_message_names_variable_not_values() -> None:
    baseline = clean_baseline()
    env = dict(baseline.worker_env)
    env["CURIE_STREAM"] = "curie:runs:c4-owner"
    current = replace(baseline, worker_env=tuple(sorted(env.items())))
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "worker_env" in messages[0]
    assert "CURIE_STREAM" in messages[0]
    assert "c4-owner" not in messages[0]
    assert "secretKeyRef" not in messages[0]


def test_worker_env_added_and_removed_names_are_listed() -> None:
    baseline = clean_baseline()
    env = dict(baseline.worker_env)
    del env["CURIE_WORKER_TOKEN"]
    env["NEW_SECRET_VAR"] = "hunter2"
    current = replace(baseline, worker_env=tuple(sorted(env.items())))
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "CURIE_WORKER_TOKEN" in messages[0]
    assert "NEW_SECRET_VAR" in messages[0]
    assert "hunter2" not in messages[0]
    assert "curie-secrets" not in messages[0]


def test_api_forges_message_does_not_leak_content() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        api_forges={"trackers": [{"name": "jira", "kind": "jira"}], "bindings": []},
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "api_forges" in messages[0]
    assert "gitlab" not in messages[0]
    assert "jira" not in messages[0]


def test_two_violations_give_two_messages() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        agents=1,
        deployments=baseline.deployments | {"c4-held-worker"},
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 2
    assert any("agents" in m for m in messages)
    assert any("deployments" in m for m in messages)


def test_leftover_and_changed_table_rows_are_named_with_counts() -> None:
    baseline = replace(
        clean_baseline(),
        table_rows=(
            ("agents", 0),
            ("factory_poll_cursors", 2),
            ("factory_terminal_notices", 0),
            ("tenants", 1),
            ("work_items", 0),
        ),
    )
    current = replace(
        baseline,
        table_rows=(
            ("agents", 0),
            ("factory_poll_cursors", 3),
            ("factory_terminal_notices", 3),
            ("tenants", 1),
            ("work_items", 0),
        ),
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "table_rows" in messages[0]
    assert "factory_terminal_notices 0 -> 3" in messages[0]
    assert "factory_poll_cursors 2 -> 3" in messages[0]
    assert "tenants" not in messages[0]
    assert "work_items" not in messages[0]


def test_table_present_only_in_current_is_named() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        table_rows=tuple(sorted([*baseline.table_rows, ("factory_extra_rows", 4)])),
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "table_rows" in messages[0]
    assert "factory_extra_rows" in messages[0]


def test_table_present_only_in_baseline_is_named() -> None:
    baseline = clean_baseline()
    current = replace(
        baseline,
        table_rows=tuple(row for row in baseline.table_rows if row[0] != "tenants"),
    )
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "table_rows" in messages[0]
    assert "tenants" in messages[0]


def test_extra_kill_flag_is_named() -> None:
    baseline = clean_baseline()
    kill_flag = "curie:kill:00000000-0000-0000-0000-000000000001"
    current = replace(baseline, persistent_keys=baseline.persistent_keys | {kill_flag})
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "persistent_keys" in messages[0]
    assert kill_flag in messages[0]


def test_missing_persistent_key_is_named() -> None:
    baseline = clean_baseline()
    current = replace(baseline, persistent_keys=frozenset())
    messages = leftover_state(baseline, current)
    assert len(messages) == 1
    assert "persistent_keys" in messages[0]
    assert "curie:runs" in messages[0]


def test_case_isolation_error_is_runtime_error() -> None:
    assert issubclass(CaseIsolationError, RuntimeError)
