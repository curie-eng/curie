"""@spec STARTABILITY-1 through STARTABILITY-7; fixture reads are not runtime proof."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from aci_protocol import BootEnv
from curie_worker.sandbox.types import claim_warm_pool
from plugin_format import is_reserved_boot_env_name

HERE = Path(__file__).resolve().parent
PROGRAM = HERE / "observer.py"
IMAGE = "ghcr.io/acme-corp/worker@sha256:" + "a" * 64
SENTINEL = "EXAMPLE-CREDENTIAL-MUST-NOT-APPEAR"
ARGS = [
    "--worker-namespace",
    "acme-workers",
    "--worker-deployment",
    "acme-worker",
    "--worker-container",
    "worker",
    "--dispatcher-namespace",
    "acme-dispatchers",
    "--dispatcher-deployment",
    "acme-dispatcher",
    "--dispatcher-container",
    "dispatcher",
    "--sandbox-namespace",
    "acme-sandboxes",
    "--runner-container",
    "runner",
    "--check-image",
    IMAGE,
]


def load_observer() -> ModuleType:
    """@spec STARTABILITY-7; absence must be an assertion failure, not import error."""
    assert PROGRAM.is_file(), "STARTABILITY-7: public observer command is not implemented"
    spec = importlib.util.spec_from_file_location("startability_observer", PROGRAM)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def binding(
    agent: str = "acme-bot",
    kind: str = "slack",
    identity: str = "default",
    secret_names: tuple[str, ...] = (),
    deployed: bool = True,
) -> dict[str, Any]:
    """@spec STARTABILITY-1."""
    return {
        "agent": agent,
        "kind": kind,
        "address": "C0EXAMPLE1" if kind == "slack" else "acme@example.com",
        "identity": identity,
        "secret_names": list(secret_names),
        "deployed": deployed,
    }


def judge(observer: ModuleType, bindings: list[dict[str, Any]], **changes: Any) -> list[dict]:
    """@spec STARTABILITY-2; use the actual current worker and protocol helpers."""
    inputs: dict[str, Any] = {
        "worker_env": {"CURIE_WARM_POOL": "acme-runner-pool"},
        "lanes": {
            "SLACK_APP_TOKEN": (True, "credential is nonblank"),
            "SLACK_BOT_TOKEN": (True, "credential is nonblank"),
        },
        "pools": {"acme-runner-pool": "acme-runner"},
        "templates": {"acme-runner": set()},
        "identity_lanes": {"default": ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN")},
    }
    inputs.update(changes)
    return observer.judge(
        bindings,
        **inputs,
        claim=claim_warm_pool,
        reserved=is_reserved_boot_env_name,
        marker=BootEnv.env_key("connector_secret_keys"),
    )


def split(lines: list[dict]) -> tuple[list[dict], dict]:
    """@spec STARTABILITY-6."""
    assert lines and lines[-1]["agent_readiness"] == "total"
    assert all(line["agent_readiness"] == "binding" for line in lines[:-1])
    return (lines[:-1], lines[-1])


def test_independent_bindings_and_distinct_agent_total() -> None:
    """@spec STARTABILITY-1 STARTABILITY-6."""
    observer = load_observer()
    rows, total = split(judge(observer, [binding(), binding(kind="email"), binding("acme-dev")]))
    assert [row["ready"] for row in rows] == [1, 1, 1]
    assert all(
        set(row) == {"agent_readiness", "agent", "kind", "address", "identity", "ready", "reason"}
        for row in rows
    )
    assert total == {"agent_readiness": "total", "bindings": 3, "not_ready": 0, "agents": 2}
    assert "secret_names" not in json.dumps(rows)


def test_no_bindings_is_a_zero_total() -> None:
    """@spec STARTABILITY-1 STARTABILITY-6."""
    observer = load_observer()
    assert judge(observer, []) == [
        {"agent_readiness": "total", "bindings": 0, "not_ready": 0, "agents": 0}
    ]


def test_inactive_binding_fails_only_its_row() -> None:
    """@spec STARTABILITY-1."""
    observer = load_observer()
    rows, total = split(judge(observer, [binding(), binding("acme-dev", deployed=False)]))
    assert [row["ready"] for row in rows] == [1, 0]
    assert "active deployment" in rows[1]["reason"]
    assert total["not_ready"] == 1


@pytest.mark.parametrize("lane", ["SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"])
def test_missing_slack_lane_does_not_fail_email(lane: str) -> None:
    """@spec STARTABILITY-3 STARTABILITY-4."""
    observer = load_observer()
    states = {
        name: (True, "credential is nonblank")
        for name in ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN")
        if name != lane
    }
    rows, total = split(judge(observer, [binding(), binding(kind="email")], lanes=states))
    assert [row["ready"] for row in rows] == [0, 1]
    assert lane in rows[0]["reason"] and total["not_ready"] == 1


def test_declaration_names_actual_indexed_lanes() -> None:
    """@spec STARTABILITY-3."""
    observer = load_observer()
    raw = json.dumps(
        [
            {
                "name": "default",
                "app_token_env": "SLACK_APP_TOKEN",
                "bot_token_env": "SLACK_BOT_TOKEN",
            },
            {
                "name": "support",
                "app_token_env": "CURIE_SLACK_APP_TOKEN__3",
                "bot_token_env": "CURIE_SLACK_BOT_TOKEN__3",
            },
        ]
    )
    mapping = observer.identity_lanes([{"name": "CURIE_SLACK_IDENTITIES", "value": raw}])
    assert mapping["support"] == ("CURIE_SLACK_APP_TOKEN__3", "CURIE_SLACK_BOT_TOKEN__3")
    states = {name: (True, "credential is nonblank") for name in mapping["support"]}
    rows, _ = split(
        judge(observer, [binding(identity="support")], lanes=states, identity_lanes=mapping)
    )
    assert rows[0]["ready"] == 1
    rows, _ = split(
        judge(observer, [binding(identity="absent")], lanes=states, identity_lanes=mapping)
    )
    assert rows[0]["ready"] == 0 and "declaration" in rows[0]["reason"]


@pytest.mark.parametrize("raw", [None, "", "  "])
def test_absent_or_blank_declaration_uses_default(raw: str | None) -> None:
    """@spec STARTABILITY-3."""
    observer = load_observer()
    env = [] if raw is None else [{"name": "CURIE_SLACK_IDENTITIES", "value": raw}]
    assert observer.identity_lanes(env) == {"default": ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN")}


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        "not-json",
        '[{"name":"support"}]',
        '[{"name":"default","app_token_env":"SLACK_APP_TOKEN","bot_token_env":"SLACK_BOT_TOKEN"},{"name":"default","app_token_env":"SLACK_APP_TOKEN","bot_token_env":"SLACK_BOT_TOKEN"}]',
    ],
)
def test_invalid_identity_declaration_fails(raw: str) -> None:
    """@spec STARTABILITY-3."""
    observer = load_observer()
    with pytest.raises(ValueError):
        observer.identity_lanes([{"name": "CURIE_SLACK_IDENTITIES", "value": raw}])


def test_legacy_judge_caller_can_omit_declaration() -> None:
    """@spec STARTABILITY-3; compatibility for the existing pure judge call shape."""
    observer = load_observer()
    rows, _ = split(
        judge(
            observer,
            [binding(identity="help-desk")],
            identity_lanes=None,
            lanes={"SLACK_APP_TOKEN_HELP_DESK", "SLACK_BOT_TOKEN_HELP_DESK"},
        )
    )
    assert rows[0]["ready"] == 1


@pytest.mark.parametrize("value, expected", [("", False), (" \n\t", False), (SENTINEL, True)])
def test_literal_credentials_reduce_to_boolean(value: str, expected: bool) -> None:
    """@spec STARTABILITY-4."""
    observer = load_observer()
    states = observer.credential_states(
        [{"name": "SLACK_APP_TOKEN", "value": value}],
        lambda _: pytest.fail("literal needs no Secret read"),
    )
    assert states["SLACK_APP_TOKEN"][0] is expected
    assert SENTINEL not in repr(states)


def test_secret_credentials_cache_missing_blank_and_unreadable() -> None:
    """@spec STARTABILITY-4."""
    observer = load_observer()
    calls: list[str] = []

    def load(name: str) -> dict[str, bytes]:
        calls.append(name)
        if name == "acme-unreadable":
            raise PermissionError(SENTINEL)
        return {"appToken": b" \n"}

    states = observer.credential_states(
        [
            {"name": "SLACK_APP_TOKEN", "secret": {"name": "acme-slack", "key": "appToken"}},
            {"name": "SLACK_BOT_TOKEN", "secret": {"name": "acme-slack", "key": "botToken"}},
            {
                "name": "CURIE_SLACK_APP_TOKEN__3",
                "secret": {"name": "acme-unreadable", "key": "appToken"},
            },
            {"name": "UNRELATED", "secret": {"name": "acme-ignored", "key": "value"}},
        ],
        load,
    )
    assert calls == ["acme-slack", "acme-unreadable"]
    assert set(states) == {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN", "CURIE_SLACK_APP_TOKEN__3"}
    assert all(not state[0] for state in states.values())
    assert "blank" in states["SLACK_APP_TOKEN"][1]
    assert "botToken" in states["SLACK_BOT_TOKEN"][1]
    assert "PermissionError" in states["CURIE_SLACK_APP_TOKEN__3"][1]
    assert SENTINEL not in repr(states)


def test_secret_claim_uses_current_worker_marker_and_pool_policy() -> None:
    """@spec STARTABILITY-2."""
    observer = load_observer()
    rows, _ = split(judge(observer, [binding(secret_names=("CONNECTOR_ROOT",))]))
    assert rows[0]["ready"] == 0
    assert "connectorSecrets" in rows[0]["reason"]
    env = {
        "CURIE_WARM_POOL": "acme-runner-pool",
        "CURIE_AGENT_SANDBOX_POOLS": " acme-bot , acme-dev ",
        "CURIE_AGENT_CONNECTOR_SECRET_POOLS": " acme-bot , acme-dev ",
    }
    rows, _ = split(
        judge(
            observer,
            [binding(secret_names=("CONNECTOR_ROOT", "CURIE_MODEL"))],
            worker_env=env,
            pools={"acme-agent-acme-bot-runner-pool": "acme-agent-runner"},
            templates={"acme-agent-runner": {"CONNECTOR_ROOT"}},
        )
    )
    assert rows[0]["ready"] == 1


@pytest.mark.parametrize(
    "pools, templates, fragment",
    [({}, {}, "acme-runner-pool"), ({"acme-runner-pool": "acme-runner"}, {}, "acme-runner")],
)
def test_missing_pool_or_template_fails(pools: dict, templates: dict, fragment: str) -> None:
    """@spec STARTABILITY-2."""
    observer = load_observer()
    rows, _ = split(judge(observer, [binding()], pools=pools, templates=templates))
    assert rows[0]["ready"] == 0 and fragment in rows[0]["reason"]


def test_missing_connector_reference_fails() -> None:
    """@spec STARTABILITY-2."""
    observer = load_observer()
    rows, _ = split(
        judge(
            observer,
            [binding(secret_names=("CONNECTOR_ROOT",))],
            worker_env={
                "CURIE_WARM_POOL": "acme-runner-pool",
                "CURIE_AGENT_CONNECTOR_SECRET_POOLS": "acme-bot",
            },
            pools={"acme-agent-acme-bot-runner-pool": "acme-agent-runner"},
            templates={"acme-agent-runner": set()},
        )
    )
    assert rows[0]["ready"] == 0 and "CONNECTOR_ROOT" in rows[0]["reason"]


def snapshot() -> dict[str, Any]:
    """@spec STARTABILITY-5 STARTABILITY-7; anonymous external boundary fixtures."""
    declaration = json.dumps(
        [
            {
                "name": "default",
                "app_token_env": "SLACK_APP_TOKEN",
                "bot_token_env": "SLACK_BOT_TOKEN",
            },
            {
                "name": "support",
                "app_token_env": "CURIE_SLACK_APP_TOKEN__3",
                "bot_token_env": "CURIE_SLACK_BOT_TOKEN__3",
            },
        ]
    )
    return {
        "worker_image": IMAGE,
        "worker_env": [
            {"name": "CURIE_WARM_POOL", "value": "acme-runner-pool"},
            {"name": "CURIE_AGENT_SANDBOX_POOLS", "value": "acme-bot"},
            {
                "name": "CURIE_AGENT_CONNECTOR_SECRET_POOLS",
                "config_map": {"name": "acme-worker-config", "key": "secretPools"},
            },
        ],
        "dispatcher_env": [
            {
                "name": "CURIE_SLACK_IDENTITIES",
                "secret": {"name": "acme-declaration", "key": "identities"},
            },
            {"name": "SLACK_APP_TOKEN", "secret": {"name": "acme-slack", "key": "appToken"}},
            {"name": "SLACK_BOT_TOKEN", "secret": {"name": "acme-slack", "key": "botToken"}},
            {"name": "CURIE_SLACK_APP_TOKEN__3", "value": SENTINEL},
            {"name": "CURIE_SLACK_BOT_TOKEN__3", "value": SENTINEL},
            {"name": "UNRELATED", "secret": {"name": "acme-ignore", "key": "unused"}},
        ],
        "secrets": {
            "acme-declaration": {"identities": declaration},
            "acme-slack": {"appToken": SENTINEL, "botToken": SENTINEL},
        },
        "config_maps": {"acme-worker-config": {"secretPools": "acme-bot"}},
        "bindings": [
            binding(secret_names=("CONNECTOR_ROOT",)),
            binding(kind="email"),
            binding("acme-dev", identity="support"),
        ],
        "pools": [
            {
                "metadata": {"name": "acme-runner-pool"},
                "spec": {"sandboxTemplateRef": {"name": "acme-runner"}},
            },
            {
                "metadata": {"name": "acme-agent-acme-bot-runner-pool"},
                "spec": {"sandboxTemplateRef": {"name": "acme-agent-runner"}},
            },
        ],
        "templates": [
            {
                "metadata": {"name": name},
                "spec": {
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {"name": "runner", "env": env},
                                {
                                    "name": "sidecar",
                                    "env": [
                                        {
                                            "name": "SIDECAR_ONLY",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": "acme-ignore",
                                                    "key": "unused",
                                                }
                                            },
                                        }
                                    ],
                                },
                            ]
                        }
                    }
                },
            }
            for name, env in [
                ("acme-runner", []),
                (
                    "acme-agent-runner",
                    [
                        {
                            "name": "CONNECTOR_ROOT",
                            "valueFrom": {
                                "secretKeyRef": {"name": "acme-connector", "key": "root"}
                            },
                        }
                    ],
                ),
            ]
        ],
    }


def run_cli(
    tmp_path: Path,
    data: dict[str, Any],
    args: list[str] | None = None,
    database_url: str | None = "postgresql://acme:" + SENTINEL + "@database.example.com/acme",
) -> tuple[subprocess.CompletedProcess[str], list[dict]]:
    """@spec STARTABILITY-7; only external clients are replaced, never the observer."""
    assert PROGRAM.is_file(), "STARTABILITY-7: public observer command is not implemented"
    data_path, trace_path = (tmp_path / "snapshot.json", tmp_path / "reads.jsonl")
    data_path.write_text(json.dumps(data), encoding="utf-8")
    env = dict(os.environ)
    env.update(
        {
            "STARTABILITY_TEST_SNAPSHOT": str(data_path),
            "STARTABILITY_TEST_TRACE": str(trace_path),
            "PYTHONPATH": str(HERE / "fixtures"),
        }
    )
    if database_url is None:
        env.pop("DATABASE_URL", None)
    else:
        env["DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, str(PROGRAM), *(ARGS if args is None else args)],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    trace = (
        [json.loads(line) for line in trace_path.read_text().splitlines()]
        if trace_path.exists()
        else []
    )
    assert SENTINEL not in result.stdout + result.stderr
    return (result, trace)


def assert_error(result: subprocess.CompletedProcess[str]) -> dict:
    """@spec STARTABILITY-6."""
    assert result.returncode == 1, (result.returncode, result.stdout, result.stderr)
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(lines) == 1 and lines[0]["agent_readiness"] == "error"
    assert set(lines[0]) == {"agent_readiness", "reason"}
    assert isinstance(lines[0]["reason"], str) and 0 < len(lines[0]["reason"]) <= 1000
    assert "Traceback" not in result.stderr
    return lines[0]


def test_cli_reads_actual_scoped_resources_and_external_environment(tmp_path: Path) -> None:
    """@spec STARTABILITY-1 STARTABILITY-2 STARTABILITY-3 STARTABILITY-5 STARTABILITY-6."""
    result, trace = run_cli(tmp_path, snapshot())
    assert result.returncode == 0, (result.stdout, result.stderr)
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [1, 1, 1]
    assert total == {"agent_readiness": "total", "bindings": 3, "not_ready": 0, "agents": 2}
    assert {tuple(event["args"]) for event in trace if event["operation"] == "deployment"} == {
        ("acme-worker", "acme-workers"),
        ("acme-dispatcher", "acme-dispatchers"),
    }
    secret_reads = [tuple(event["args"]) for event in trace if event["operation"] == "secret"]
    assert set(secret_reads) == {
        ("acme-declaration", "acme-dispatchers"),
        ("acme-slack", "acme-dispatchers"),
    }
    assert secret_reads.count(("acme-slack", "acme-dispatchers")) == 1
    assert [event["args"] for event in trace if event["operation"] == "config_map"] == [
        ["acme-worker-config", "acme-workers"]
    ]
    assert {tuple(event["args"]) for event in trace if event["operation"] == "custom"} == {
        ("extensions.agents.x-k8s.io", "v1beta1", "acme-sandboxes", "sandboxwarmpools"),
        ("extensions.agents.x-k8s.io", "v1beta1", "acme-sandboxes", "sandboxtemplates"),
    }
    db_events = [event for event in trace if event["operation"] == "database"]
    assert len(db_events) == 1 and db_events[0]["asyncpg"] is True
    sql = " ".join(db_events[0]["sql"].split())
    assert "c.adapter AS identity" in sql and "c.identity" not in sql
    assert "d.status = 'active'" in sql and "v.id = d.version_id AND v.agent_id = a.id" in sql
    assert "jsonb_object_keys" in sql and "bundle_ref" not in sql
    assert any(event["operation"] == "database_dispose" for event in trace)


@pytest.mark.parametrize("key", ["appToken", "botToken"])
def test_cli_blank_credential_fails_only_default_slack_binding(tmp_path: Path, key: str) -> None:
    """@spec STARTABILITY-3 STARTABILITY-4 STARTABILITY-6."""
    data = snapshot()
    data["secrets"]["acme-slack"][key] = " \n"
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [0, 1, 1]
    assert key in rows[0]["reason"] and total["not_ready"] == 1


def test_cli_unreadable_credentials_are_binding_failures_without_raw_error(tmp_path: Path) -> None:
    """@spec STARTABILITY-4 STARTABILITY-6."""
    data = snapshot()
    data["secret_error"] = "acme-slack"
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [0, 1, 1] and total["not_ready"] == 1
    assert "PermissionError" in rows[0]["reason"]


def test_cli_incompatible_image_emits_no_success_total(tmp_path: Path) -> None:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    data = snapshot()
    data["worker_image"] = IMAGE[:-64] + "b" * 64
    result, trace = run_cli(tmp_path, data)
    assert_error(result)
    assert not any(event["operation"] == "database" for event in trace)


@pytest.mark.parametrize("failure", ["database_error", "custom_error", "deployment_error"])
def test_cli_failed_collection_never_echoes_exception_or_connection_data(
    tmp_path: Path, failure: str
) -> None:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    data = snapshot()
    data[failure] = True
    result, _ = run_cli(tmp_path, data)
    assert_error(result)
    assert "database.example.com" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "args, database_url",
    [
        ([], "postgresql://example"),
        (ARGS, None),
        (ARGS, " \n"),
        ([*ARGS, "--worker-namespace", ""], "postgresql://example"),
    ],
)
def test_cli_configuration_errors_are_structured_before_external_reads(
    tmp_path: Path, args: list[str], database_url: str | None
) -> None:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    result, trace = run_cli(tmp_path, snapshot(), args=args, database_url=database_url)
    assert_error(result)
    assert trace == []


def test_cli_empty_bindings_has_zero_total(tmp_path: Path) -> None:
    """@spec STARTABILITY-1 STARTABILITY-6."""
    data = snapshot()
    data["bindings"] = []
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        {"agent_readiness": "total", "bindings": 0, "not_ready": 0, "agents": 0}
    ]


def test_cli_relevant_env_from_refuses_to_guess(tmp_path: Path) -> None:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    data = snapshot()
    data["worker_env_from"] = [{"config_map_ref": {"name": "acme-inherited"}}]
    result, _ = run_cli(tmp_path, data)
    assert_error(result)


def test_cli_missing_secret_key_is_an_observed_binding_failure(tmp_path: Path) -> None:
    """@spec STARTABILITY-4 STARTABILITY-6."""
    data = snapshot()
    del data["secrets"]["acme-slack"]["botToken"]
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [0, 1, 1] and total["not_ready"] == 1
    assert "botToken" in rows[0]["reason"]


def test_cli_inactive_binding_preserves_other_binding_results(tmp_path: Path) -> None:
    """@spec STARTABILITY-1 STARTABILITY-6."""
    data = snapshot()
    data["bindings"][1]["deployed"] = False
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [1, 0, 1] and total["not_ready"] == 1


def test_cli_sidecar_reference_cannot_cover_missing_runner_reference(tmp_path: Path) -> None:
    """@spec STARTABILITY-2 STARTABILITY-5."""
    data = snapshot()
    containers = data["templates"][1]["spec"]["podTemplate"]["spec"]["containers"]
    containers[1]["env"] = containers[0]["env"]
    containers[0]["env"] = []
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [0, 1, 1] and total["not_ready"] == 1
    assert "CONNECTOR_ROOT" in rows[0]["reason"]


def test_cli_unreadable_identity_declaration_is_a_collection_error(tmp_path: Path) -> None:
    """@spec STARTABILITY-3 STARTABILITY-5 STARTABILITY-6."""
    data = snapshot()
    data["secret_error"] = "acme-declaration"
    result, _ = run_cli(tmp_path, data)
    assert_error(result)


@pytest.mark.parametrize("flag", ["--worker-container", "--dispatcher-container"])
def test_cli_missing_selected_container_is_a_collection_error(tmp_path: Path, flag: str) -> None:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    result, _ = run_cli(tmp_path, snapshot(), args=[*ARGS, flag, "acme-missing"])
    assert_error(result)


def test_cli_empty_identity_list_keeps_dispatcher_default(tmp_path: Path) -> None:
    """@spec STARTABILITY-3 STARTABILITY-7; [] is the dispatcher's legacy default."""
    data = snapshot()
    data["secrets"]["acme-declaration"]["identities"] = "[]"
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert [row["ready"] for row in rows] == [1, 1, 0]
    assert total["not_ready"] == 1


@pytest.mark.parametrize(
    "violation",
    ["default_lanes", "missing_default", "shared_lanes", "reserved_identity",
     "long_identity", "invalid_identity", "extra_field"],
)
def test_cli_rejects_dispatcher_invalid_declarations(tmp_path: Path, violation: str) -> None:
    """@spec STARTABILITY-3 STARTABILITY-6 STARTABILITY-7."""
    data = snapshot()
    declaration = json.loads(data["secrets"]["acme-declaration"]["identities"])
    if violation == "default_lanes":
        declaration[0]["app_token_env"] = "CURIE_SLACK_APP_TOKEN__3"
    elif violation == "missing_default":
        declaration = declaration[1:]
    elif violation == "shared_lanes":
        declaration.append({**declaration[1], "name": "other"})
    elif violation == "reserved_identity":
        declaration[1]["name"] = "curie-cluster-message"
    elif violation == "long_identity":
        declaration[1]["name"] = "a" * 41
    elif violation == "invalid_identity":
        declaration[1]["name"] = "help_desk"
    else:
        declaration[0]["unexpected"] = SENTINEL
    data["secrets"]["acme-declaration"]["identities"] = json.dumps(declaration)
    result, _ = run_cli(tmp_path, data)
    assert_error(result)


@pytest.mark.parametrize(
    "name",
    ["E2E_CLUSTER_KUBECONFIG", "E2E_REGISTRY_PUSH_CONFIG", "E2E_BUILD_CACHE_CONFIG",
     "SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED"],
)
def test_cli_withheld_secret_names_do_not_require_sandbox_pool(tmp_path: Path, name: str) -> None:
    """@spec STARTABILITY-2 STARTABILITY-7; worker withholds these before marking."""
    data = snapshot()
    data["bindings"] = [binding(kind="email", secret_names=(name,))]
    data["worker_env"] = [{"name": "CURIE_WARM_POOL", "value": "acme-runner-pool"}]
    data["pools"] = data["pools"][:1]
    data["templates"] = data["templates"][:1]
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert rows[0]["ready"] == 1 and total["not_ready"] == 0
    assert name not in result.stdout + result.stderr


def test_cli_blank_worker_pool_is_not_replaced_with_default(tmp_path: Path) -> None:
    """@spec STARTABILITY-2 STARTABILITY-7; explicit empty config stays empty."""
    data = snapshot()
    data["bindings"] = [binding(kind="email")]
    data["worker_env"] = [{"name": "CURIE_WARM_POOL", "value": ""}]
    data["pools"] = data["pools"][:1]
    data["pools"][0]["metadata"]["name"] = "curie-runner-pool"
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert rows[0]["ready"] == 0 and total["not_ready"] == 1


@pytest.mark.parametrize("generic_present", [False, True])
def test_cli_unlisted_existing_agent_pool_is_selected(
    tmp_path: Path, generic_present: bool
) -> None:
    """@spec STARTABILITY-2 STARTABILITY-7; discovered pool overrides generic choice."""
    data = snapshot()
    data["bindings"] = [binding(kind="email")]
    data["worker_env"] = [{"name": "CURIE_WARM_POOL", "value": "acme-runner-pool"}]
    if generic_present:
        data["templates"] = data["templates"][:1]
    else:
        data["pools"] = data["pools"][1:]
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    expected = 0 if generic_present else 1
    assert rows[0]["ready"] == expected and total["not_ready"] == 1 - expected
    if generic_present:
        assert "acme-agent-runner" in rows[0]["reason"]


def test_cli_discovered_pool_does_not_bypass_connector_secret_refusal(tmp_path: Path) -> None:
    """@spec STARTABILITY-2 STARTABILITY-7; only a generic choice can be overridden."""
    data = snapshot()
    data["bindings"] = [binding(kind="email", secret_names=("CONNECTOR_ROOT",))]
    data["worker_env"] = [{"name": "CURIE_WARM_POOL", "value": "acme-runner-pool"}]
    result, _ = run_cli(tmp_path, data)
    assert result.returncode == 0
    rows, total = split([json.loads(line) for line in result.stdout.splitlines()])
    assert rows[0]["ready"] == 0 and total["not_ready"] == 1
    assert "worker refuses" in rows[0]["reason"]
