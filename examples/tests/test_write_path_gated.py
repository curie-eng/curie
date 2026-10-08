"""Red paths for `scripts/check-write-path-gated.py`.

A guard that has only ever been observed passing is not a guard. The repository
state is green by construction, so running the checker in CI proves it does not
false-positive and nothing else -- it cannot tell you the check still fails on
the incident it was written for. These tests build isolated violating bundles and
execute the real script, asserting both the non-zero exit and the diagnostic that
makes the failure actionable.

Every case here is a mistake that reached a real install, or a way an earlier
version of the checker could be fooled into passing one.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CHECKER = REPO / "scripts" / "check-write-path-gated.py"

WRITE_SERVER = """\
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

mcp = FastMCP("k8s-write")
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False)


@mcp.tool(annotations=WRITE)
def restart_deployment(namespace: str, name: str) -> str:
    return "ok"
"""

READ_SERVER = """\
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

mcp = FastMCP("reader")
READ_ONLY = ToolAnnotations(readOnlyHint=True)


@mcp.tool(annotations=READ_ONLY)
def list_things() -> str:
    return "ok"
"""

GATE = "mcp__k8s-write__restart_deployment"

READ_ONLY_WITH_PROSE = READ_SERVER.replace(
    'mcp = FastMCP("reader")',
    '# Docs note: a write tool would set readOnlyHint=False here.\nmcp = FastMCP("reader")',
)


def role(rules: list[dict[str, object]], namespace: str = "prod") -> str:
    return yaml.safe_dump(
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": "writer", "namespace": namespace},
            "rules": rules,
        }
    )


def deployment_rule(names: list[str], verbs: list[str] | None = None) -> dict[str, object]:
    return {
        "apiGroups": ["apps"],
        "resources": ["deployments"],
        "resourceNames": names,
        "verbs": verbs if verbs is not None else ["get", "patch"],
    }


def bundle(
    root: Path,
    *,
    gates: list[str],
    connectors: dict[str, object],
    servers: dict[str, str] | None = None,
    role_yaml: str | None = None,
    tool_policy: dict[str, object] | None = None,
) -> Path:
    """Write one example bundle under an isolated examples/ directory."""
    examples = root / "examples"
    b = examples / "fixture"
    (b / ".claude-plugin").mkdir(parents=True)
    manifest: dict[str, object] = {
        "name": "fixture",
        "version": "0.0.1",
        "approvalPolicy": {"gates": [{"gate": g} for g in gates]},
    }
    if tool_policy is not None:
        manifest["toolPolicy"] = tool_policy
    (b / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    (b / "connectors.yaml").write_text(yaml.safe_dump({"connectors": connectors}), encoding="utf-8")
    for name, source in (servers or {}).items():
        d = b / "connectors" / name
        d.mkdir(parents=True)
        (d / "server.py").write_text(source, encoding="utf-8")
    if role_yaml is not None:
        (b / "manifests").mkdir(parents=True)
        (b / "manifests" / "write-role.yaml").write_text(role_yaml, encoding="utf-8")
    return examples


def run(examples: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), "--examples", str(examples)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_missing_gate_fails_and_names_the_gate(tmp_path: Path) -> None:
    """The four-and-a-half-day incident: connector enabled, gate forgotten."""
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"k8s-write": {"image": "x", "env": {}}},
        servers={"k8s-write": WRITE_SERVER},
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout + r.stderr
    assert GATE in r.stderr, r.stderr


def test_declared_gate_passes(tmp_path: Path) -> None:
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={"k8s-write": {"image": "x", "env": {}}},
        servers={"k8s-write": WRITE_SERVER},
    )
    assert run(examples).returncode == 0


def test_a_literal_approval_required_entry_gates_the_write_tool(tmp_path: Path) -> None:
    """A write tool the owner names in approvalRequired still waits for a person."""
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"k8s-write": {"image": "x", "env": {}}},
        servers={"k8s-write": WRITE_SERVER},
        tool_policy={
            "enforcement": "curie/mcp-tool-policy@1",
            "approvalRequired": ["k8s-write/restart_deployment"],
        },
    )
    r = run(examples)
    assert r.returncode == 0, r.stderr


def test_an_allow_entry_does_not_excuse_an_ungated_write_tool(tmp_path: Path) -> None:
    """Allowing a write lets it run with nobody deciding; that is what this guard stops."""
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"k8s-write": {"image": "x", "env": {}}},
        servers={"k8s-write": WRITE_SERVER},
        tool_policy={
            "enforcement": "curie/mcp-tool-policy@1",
            "allow": ["k8s-write/restart_deployment"],
        },
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout + r.stderr
    assert GATE in r.stderr, r.stderr


def test_glob_tool_policy_entry_does_not_classify_the_write_tool(tmp_path: Path) -> None:
    """A glob would wave through a write tool nobody looked at."""
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"k8s-write": {"image": "x", "env": {}}},
        servers={"k8s-write": WRITE_SERVER},
        tool_policy={
            "enforcement": "curie/mcp-tool-policy@1",
            "approvalRequired": ["k8s-write/*"],
        },
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout + r.stderr
    assert GATE in r.stderr, r.stderr


def test_read_only_tool_needs_no_gate(tmp_path: Path) -> None:
    """A read connector must not be dragged in by a neighbour's write annotation.

    The earlier implementation searched each file for the literal
    `readOnlyHint=False` and then claimed every decorated function in it, so one
    write tool made its own file's read tools look write-shaped -- and a mention
    of the literal in prose was enough on its own.
    """
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"reader": {"image": "x", "env": {}}},
        servers={"reader": READ_ONLY_WITH_PROSE},
    )
    r = run(examples)
    assert r.returncode == 0, r.stderr


def test_mixed_server_gates_only_the_write_tool(tmp_path: Path) -> None:
    mixed = (
        READ_SERVER
        + """

WRITE = ToolAnnotations(readOnlyHint=False)


@mcp.tool(annotations=WRITE)
def mutate(namespace: str) -> str:
    return "ok"
"""
    )
    examples = bundle(
        tmp_path,
        gates=["mcp__mixed__mutate"],
        connectors={"mixed": {"image": "x", "env": {}}},
        servers={"mixed": mixed},
    )
    r = run(examples)
    assert r.returncode == 0, r.stderr


def test_unannotated_tool_is_unclassified_not_assumed_safe(tmp_path: Path) -> None:
    """Unclassified is the one place the tri-state default applies here."""
    unannotated = """\
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("vague")


@mcp.tool()
def do_something(namespace: str) -> str:
    return "ok"
"""
    examples = bundle(
        tmp_path,
        gates=[],
        connectors={"vague": {"image": "x", "env": {}}},
        servers={"vague": unannotated},
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "no readable readOnlyHint" in r.stderr, r.stderr


def test_allowlist_entry_without_resource_name_fails(tmp_path: Path) -> None:
    """Drift direction that 403s AFTER a human approved the call."""
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={"k8s-write": {"image": "x", "env": {"K8S_WRITE_ALLOWLIST": "prod/api"}}},
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([deployment_rule(["other"])]),
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "prod/api" in r.stderr and "403" in r.stderr, r.stderr


def test_resource_name_without_allowlist_entry_fails(tmp_path: Path) -> None:
    """The other direction: granted, unreachable through the tool, unexplained."""
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={"k8s-write": {"image": "x", "env": {"K8S_WRITE_ALLOWLIST": "prod/api"}}},
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([deployment_rule(["api", "worker"])]),
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "prod/worker" in r.stderr, r.stderr


def test_agreeing_ceilings_pass(tmp_path: Path) -> None:
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={"k8s-write": {"image": "x", "env": {"K8S_WRITE_ALLOWLIST": "prod/api"}}},
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([deployment_rule(["api"])]),
    )
    assert run(examples).returncode == 0


@pytest.mark.parametrize(
    "rule",
    [
        pytest.param(
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "resourceNames": ["api"],
                "verbs": ["patch"],
            },
            id="unrelated-resource",
        ),
        pytest.param(deployment_rule(["api"], verbs=["get", "list"]), id="read-only-verbs"),
    ],
)
def test_unrelated_grant_does_not_align_a_deployment_allowlist(
    tmp_path: Path, rule: dict[str, object]
) -> None:
    """Naming `api` on some other rule must not satisfy a Deployment allowlist.

    An earlier version unioned every `resourceNames` entry from every rule with
    no regard for apiGroup, resource, or verb, so a ConfigMap grant -- or a
    read-only Deployment grant -- made the ceilings look aligned.
    """
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={"k8s-write": {"image": "x", "env": {"K8S_WRITE_ALLOWLIST": "prod/api"}}},
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([rule]),
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "prod/api" in r.stderr, r.stderr


def test_placeholder_only_allowlist_is_skipped(tmp_path: Path) -> None:
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={
            "k8s-write": {"image": "x", "env": {"K8S_WRITE_ALLOWLIST": "<namespace>/<deployment>"}}
        },
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([deployment_rule(["my-app"])]),
    )
    assert run(examples).returncode == 0


def test_placeholder_mixed_with_a_real_entry_fails(tmp_path: Path) -> None:
    """The bypass used to trigger on any `<` or `>` anywhere in the value.

    So a half-filled allowlist -- one real target plus the leftover placeholder --
    skipped the comparison entirely and shipped a grant nobody checked.
    """
    examples = bundle(
        tmp_path,
        gates=[GATE],
        connectors={
            "k8s-write": {
                "image": "x",
                "env": {"K8S_WRITE_ALLOWLIST": "prod/api,<namespace>/<deployment>"},
            }
        },
        servers={"k8s-write": WRITE_SERVER},
        role_yaml=role([deployment_rule(["other"])]),
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "mixes the placeholder" in r.stderr, r.stderr


def test_second_write_connector_allowlist_is_compared(tmp_path: Path) -> None:
    """`K8S_SCALE_ALLOWLIST` is a ceiling too; only `K8S_WRITE_*` used to be read."""
    scale = WRITE_SERVER.replace("restart_deployment", "scale_deployment")
    examples = bundle(
        tmp_path,
        gates=["mcp__k8s-scale__scale_deployment"],
        connectors={"k8s-scale": {"image": "x", "env": {"K8S_SCALE_ALLOWLIST": "prod/api"}}},
        servers={"k8s-scale": scale},
        role_yaml=role([deployment_rule(["other"])]),
    )
    r = run(examples)
    assert r.returncode == 1, r.stdout
    assert "prod/api" in r.stderr, r.stderr


def test_no_bundles_is_a_failure_not_a_vacuous_pass(tmp_path: Path) -> None:
    empty = tmp_path / "examples"
    empty.mkdir()
    r = run(empty)
    assert r.returncode == 1
    assert "vacuously" in r.stderr, r.stderr
