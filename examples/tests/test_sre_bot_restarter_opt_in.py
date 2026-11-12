"""The SRE bot's opt-in restarter connector stays off until an operator opts in.

The default bundle neither declares ``connectors/restarter`` nor names its tools,
so an install that does not opt in deploys nothing new. The opt-in is
``connectors/restarter/opt-in.yaml``; these tests apply exactly that file to a copy
of the shipped bundle and run the validators an intake runs on it
(``plugin_format.validate_bundle`` and the API's sealing key custody check), so
the documented opt-in cannot drift into a bundle that intake refuses.

The credential, ``manifests/restarter-access.yaml``, is checked against the same
declaration: its Role must name the connector's namespace and allowlist and
grant nothing but ``get`` and ``patch`` on the allowlisted ``deployments``. A live API
server enforcing that Role was observed on a disposable cluster (recorded in
the pull request that added the connector).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from curie_api.bundles import sealing_key_custody_issues
from plugin_format import TOOL_POLICY_ENFORCEMENT, validate_bundle

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
OPT_IN = BUNDLE / "connectors" / "restarter" / "opt-in.yaml"
ACCESS = BUNDLE / "manifests" / "restarter-access.yaml"
DEFAULT_ACCESS = BUNDLE / "manifests" / "kubernetes-access.yaml"
SCALER_ACCESS = BUNDLE / "manifests" / "scaler-access.yaml"
SCALER_OPT_IN = BUNDLE / "connectors" / "scaler" / "opt-in.yaml"


def _opt_in() -> dict[str, Any]:
    return yaml.safe_load(OPT_IN.read_text("utf-8"))


def _errors(root: Path) -> list[tuple[str, str]]:
    result = validate_bundle(root, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    return [(issue.code, issue.message.split(":", 1)[0]) for issue in result.errors]


# The shipped bundle's own intake errors: its source-built connectors carry no
# lock until `curie build` writes one.
_SHIPPED_ERRORS = [
    ("connectors.lock_missing", "connectors.self-upgrade"),
    ("connectors.lock_missing", "connectors.tempo"),
]


def _opted_in(tmp_path: Path, fragment: dict[str, Any] | None = None) -> Path:
    """A copy of the shipped bundle with the opt-in fragment applied."""

    fragment = fragment if fragment is not None else _opt_in()
    root = tmp_path / "sre-bot"
    shutil.copytree(BUNDLE, root)

    connectors_path = root / "connectors.yaml"
    connectors = yaml.safe_load(connectors_path.read_text("utf-8"))
    connectors["connectors"].update(fragment.get("connector", {}))
    connectors_path.write_text(yaml.safe_dump(connectors, sort_keys=False), "utf-8")

    manifest_path = root / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    plugin = fragment.get("plugin", {})
    for key, entries in plugin.get("toolPolicy", {}).items():
        manifest["toolPolicy"][key].extend(entries)
    manifest["approvalPolicy"]["gates"].extend(plugin.get("approvalPolicy", {}).get("gates", []))
    manifest_path.write_text(json.dumps(manifest, indent=2), "utf-8")
    return root


def test_the_shipped_bundle_does_not_declare_or_name_the_restarter() -> None:
    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text("utf-8"))
    assert "restarter" not in connectors["connectors"]
    manifest = json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text("utf-8"))
    named = json.dumps([manifest["toolPolicy"], manifest["approvalPolicy"]])
    assert "restarter" not in named
    assert _errors(BUNDLE) == _SHIPPED_ERRORS


def test_the_opt_in_fragment_yields_a_bundle_intake_accepts(tmp_path: Path) -> None:
    root = _opted_in(tmp_path)

    # Exactly one more intake error than the shipped bundle: the restarter, like its
    # siblings, is pinned by the lock `curie build` writes.
    assert sorted(_errors(root)) == sorted(
        [*_SHIPPED_ERRORS, ("connectors.lock_missing", "connectors.restarter")]
    )
    assert sealing_key_custody_issues(root) == []


def test_the_opt_in_gates_the_one_write() -> None:
    plugin = _opt_in()["plugin"]
    gates = {gate["gate"] for gate in plugin["approvalPolicy"]["gates"]}
    assert gates == {"mcp__restarter__restart_deployment"}
    assert plugin["toolPolicy"]["approvalRequired"] == ["restarter/restart_deployment"]
    # Nothing is allowed ungated: the connector serves no read, and no restore.
    assert "allow" not in plugin["toolPolicy"]
    assert "restore" not in json.dumps(plugin)
    assert "observe_version" not in json.dumps(plugin)


def test_a_sealing_key_outside_a_secret_ref_is_refused_by_intake(tmp_path: Path) -> None:
    """The custody check runs on the opted-in bundle: a literal key is named."""

    fragment = _opt_in()
    restarter = fragment["connector"]["restarter"]
    restarter["secrets"] = [
        entry for entry in restarter["secrets"] if entry["name"] != "SNAPSHOT_SEALING_KEY"
    ]
    restarter["env"]["SNAPSHOT_SEALING_KEY"] = "example-key-a:AAAA"
    issues = sealing_key_custody_issues(_opted_in(tmp_path, fragment))
    assert issues
    assert any("SNAPSHOT_SEALING_KEY" in issue.message for issue in issues)


def test_naming_the_restarter_without_declaring_it_is_refused(tmp_path: Path) -> None:
    """The plugin half alone is invalid, so the two halves travel together."""

    fragment = {"plugin": _opt_in()["plugin"]}
    codes = {code for code, _ in _errors(_opted_in(tmp_path, fragment))}
    assert {"tool_policy.unknown_server", "approval_policy.gate_not_namespaced"} <= codes


# --------------------------------------------------------------------------- #
# The credential agrees with the declaration.
# --------------------------------------------------------------------------- #


def _objects(path: Path) -> dict[str, dict[str, Any]]:
    return {doc["kind"]: doc for doc in yaml.safe_load_all(path.read_text("utf-8")) if doc}


def test_the_role_grants_only_get_and_patch_on_the_allowlisted_deployments() -> None:
    env = _opt_in()["connector"]["restarter"]["env"]
    objects = _objects(ACCESS)

    role = objects["Role"]
    assert role["metadata"]["namespace"] == env["RESTARTER_NAMESPACE"]
    (rule,) = role["rules"]
    assert rule["apiGroups"] == ["apps"]
    assert rule["resources"] == ["deployments"]
    assert sorted(rule["verbs"]) == ["get", "patch"]
    assert rule["resourceNames"] == env["RESTARTER_DEPLOYMENTS"].split(",")

    account = objects["ServiceAccount"]["metadata"]
    binding = objects["RoleBinding"]
    assert binding["metadata"]["namespace"] == env["RESTARTER_NAMESPACE"]
    assert binding["roleRef"]["name"] == role["metadata"]["name"]
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": account["name"], "namespace": account["namespace"]}
    ]
    token = objects["Secret"]
    assert token["type"] == "kubernetes.io/service-account-token"
    assert token["metadata"]["annotations"]["kubernetes.io/service-account.name"] == account["name"]
    assert "ClusterRole" not in objects and "ClusterRoleBinding" not in objects


def test_the_restarter_identity_is_neither_the_general_nor_the_scaler_identity() -> None:
    """Each acting credential is its own: sharing one would widen the other."""

    general = _objects(DEFAULT_ACCESS)["ServiceAccount"]["metadata"]["name"]
    scaler = _objects(SCALER_ACCESS)["ServiceAccount"]["metadata"]["name"]
    restarter = _objects(ACCESS)["ServiceAccount"]["metadata"]["name"]
    assert len({general, scaler, restarter}) == 3
    scaler_files = yaml.safe_load(SCALER_OPT_IN.read_text("utf-8"))["connector"]["scaler"]
    assert not set(_opt_in()["connector"]["restarter"]["secret_files"]) & set(
        scaler_files["secret_files"]
    )
    scaler_sealing = {entry["from_secret"] for entry in scaler_files["secrets"]}
    restarter_sealing = {
        entry["from_secret"] for entry in _opt_in()["connector"]["restarter"]["secrets"]
    }
    assert not scaler_sealing & restarter_sealing
    secret_files = _opt_in()["connector"]["restarter"]["secret_files"]
    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text("utf-8"))["connectors"]
    for declared in connectors.values():
        assert not set(secret_files) & set(declared.get("secret_files", {}))


@pytest.mark.parametrize("variable", ["RESTARTER_NAMESPACE", "RESTARTER_DEPLOYMENTS"])
def test_the_opt_in_names_its_bounds(variable: str) -> None:
    assert _opt_in()["connector"]["restarter"]["env"][variable].strip()
