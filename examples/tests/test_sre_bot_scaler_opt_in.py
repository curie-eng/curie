"""The SRE bot's opt-in scaler connector stays off until an operator opts in.

The default bundle neither declares ``connectors/scaler`` nor names its tools,
so an install that does not opt in deploys nothing new. The opt-in is
``connectors/scaler/opt-in.yaml``; these tests apply exactly that file to a copy
of the shipped bundle and run the validators an intake runs on it
(``plugin_format.validate_bundle`` and the API's sealing key custody check), so
the documented opt-in cannot drift into a bundle that intake refuses.

The credential, ``manifests/scaler-access.yaml``, is checked against the same
declaration: its Role must name the connector's namespace and allowlist and
grant nothing but ``get`` and ``update`` on ``deployments/scale``. A live API
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
OPT_IN = BUNDLE / "connectors" / "scaler" / "opt-in.yaml"
ACCESS = BUNDLE / "manifests" / "scaler-access.yaml"
DEFAULT_ACCESS = BUNDLE / "manifests" / "kubernetes-access.yaml"


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


def test_the_shipped_bundle_does_not_declare_or_name_the_scaler() -> None:
    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text("utf-8"))
    assert "scaler" not in connectors["connectors"]
    manifest = json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text("utf-8"))
    named = json.dumps([manifest["toolPolicy"], manifest["approvalPolicy"]])
    assert "scaler" not in named
    assert _errors(BUNDLE) == _SHIPPED_ERRORS


def test_the_opt_in_fragment_yields_a_bundle_intake_accepts(tmp_path: Path) -> None:
    root = _opted_in(tmp_path)

    # Exactly one more intake error than the shipped bundle: the scaler, like its
    # siblings, is pinned by the lock `curie build` writes.
    assert sorted(_errors(root)) == sorted(
        [*_SHIPPED_ERRORS, ("connectors.lock_missing", "connectors.scaler")]
    )
    assert sealing_key_custody_issues(root) == []


def test_the_opt_in_gates_the_write_and_leaves_observe_version_ungated() -> None:
    plugin = _opt_in()["plugin"]
    gates = {gate["gate"] for gate in plugin["approvalPolicy"]["gates"]}
    assert gates == {"mcp__scaler__scale_deployment"}
    assert plugin["toolPolicy"]["approvalRequired"] == ["scaler/scale_deployment"]
    assert plugin["toolPolicy"]["allow"] == ["scaler/observe_version"]
    assert "scaler/restore" not in json.dumps(plugin)


def test_a_sealing_key_outside_a_secret_ref_is_refused_by_intake(tmp_path: Path) -> None:
    """The custody check runs on the opted-in bundle: a literal key is named."""

    fragment = _opt_in()
    scaler = fragment["connector"]["scaler"]
    scaler["secrets"] = [
        entry for entry in scaler["secrets"] if entry["name"] != "SNAPSHOT_SEALING_KEY"
    ]
    scaler["env"]["SNAPSHOT_SEALING_KEY"] = "example-key-a:AAAA"
    issues = sealing_key_custody_issues(_opted_in(tmp_path, fragment))
    assert issues
    assert any("SNAPSHOT_SEALING_KEY" in issue.message for issue in issues)


def test_naming_the_scaler_without_declaring_it_is_refused(tmp_path: Path) -> None:
    """The plugin half alone is invalid, so the two halves travel together."""

    fragment = {"plugin": _opt_in()["plugin"]}
    codes = {code for code, _ in _errors(_opted_in(tmp_path, fragment))}
    assert {"tool_policy.unknown_server", "approval_policy.gate_not_namespaced"} <= codes


# --------------------------------------------------------------------------- #
# The credential agrees with the declaration.
# --------------------------------------------------------------------------- #


def _objects(path: Path) -> dict[str, dict[str, Any]]:
    return {doc["kind"]: doc for doc in yaml.safe_load_all(path.read_text("utf-8")) if doc}


def test_the_role_grants_only_get_and_update_on_the_allowlisted_scales() -> None:
    env = _opt_in()["connector"]["scaler"]["env"]
    objects = _objects(ACCESS)

    role = objects["Role"]
    assert role["metadata"]["namespace"] == env["SCALER_NAMESPACE"]
    (rule,) = role["rules"]
    assert rule["apiGroups"] == ["apps"]
    assert rule["resources"] == ["deployments/scale"]
    assert sorted(rule["verbs"]) == ["get", "update"]
    assert rule["resourceNames"] == env["SCALER_DEPLOYMENTS"].split(",")

    account = objects["ServiceAccount"]["metadata"]
    binding = objects["RoleBinding"]
    assert binding["metadata"]["namespace"] == env["SCALER_NAMESPACE"]
    assert binding["roleRef"]["name"] == role["metadata"]["name"]
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": account["name"], "namespace": account["namespace"]}
    ]
    token = objects["Secret"]
    assert token["type"] == "kubernetes.io/service-account-token"
    assert token["metadata"]["annotations"]["kubernetes.io/service-account.name"] == account["name"]
    assert "ClusterRole" not in objects and "ClusterRoleBinding" not in objects


def test_the_scaler_identity_is_not_the_general_connector_identity() -> None:
    general = _objects(DEFAULT_ACCESS)["ServiceAccount"]["metadata"]["name"]
    scaler = _objects(ACCESS)["ServiceAccount"]["metadata"]["name"]
    assert scaler != general
    secret_files = _opt_in()["connector"]["scaler"]["secret_files"]
    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text("utf-8"))["connectors"]
    for declared in connectors.values():
        assert not set(secret_files) & set(declared.get("secret_files", {}))


@pytest.mark.parametrize("variable", ["SCALER_NAMESPACE", "SCALER_DEPLOYMENTS"])
def test_the_opt_in_names_its_bounds(variable: str) -> None:
    assert _opt_in()["connector"]["scaler"]["env"][variable].strip()
