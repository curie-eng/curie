"""The SRE bot's opt-in metrics connector stays off until an operator opts in.

The default bundle neither declares ``connectors/metrics`` nor names its tool,
so an install that does not opt in deploys nothing new. The opt-in is
``connectors/metrics/opt-in.yaml``; these tests apply exactly that file to a
copy of the shipped bundle and run the validators an intake runs on it
(``plugin_format.validate_bundle``).

The connector exists to be a remediation verifier (ADR 0203), so the example
policy beside it, ``connectors/metrics/example-policy.json``, pairs it with the
opt-in scaler: it must pass the platform's own document validator, and the two
connectors must pass the platform's independence check (a different connector,
disjoint header secret names).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import yaml
from curie_api.remediation_policy_document import validate_document
from curie_api.remediation_verifier import header_secret_names
from plugin_format import TOOL_POLICY_ENFORCEMENT, validate_bundle
from plugin_format.connectors import ConnectorSpec

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
OPT_IN = BUNDLE / "connectors" / "metrics" / "opt-in.yaml"
SCALER_OPT_IN = BUNDLE / "connectors" / "scaler" / "opt-in.yaml"
POLICY = BUNDLE / "connectors" / "metrics" / "example-policy.json"


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text("utf-8"))


def _errors(root: Path) -> list[tuple[str, str]]:
    result = validate_bundle(root, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    return [(issue.code, issue.message.split(":", 1)[0]) for issue in result.errors]


# The shipped bundle's own intake errors: its source-built connectors carry no
# lock until `curie build` writes one.
_SHIPPED_ERRORS = [
    ("connectors.lock_missing", "connectors.self-upgrade"),
    ("connectors.lock_missing", "connectors.tempo"),
]


def _opted_in(tmp_path: Path, *fragments: dict[str, Any]) -> Path:
    """A copy of the shipped bundle with the opt-in fragments applied."""

    root = tmp_path / "sre-bot"
    shutil.copytree(BUNDLE, root)
    connectors_path = root / "connectors.yaml"
    connectors = yaml.safe_load(connectors_path.read_text("utf-8"))
    manifest_path = root / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    for fragment in fragments:
        connectors["connectors"].update(fragment.get("connector", {}))
        plugin = fragment.get("plugin", {})
        for key, entries in plugin.get("toolPolicy", {}).items():
            manifest["toolPolicy"][key].extend(entries)
        manifest["approvalPolicy"]["gates"].extend(
            plugin.get("approvalPolicy", {}).get("gates", [])
        )
    connectors_path.write_text(yaml.safe_dump(connectors, sort_keys=False), "utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2), "utf-8")
    return root


def test_the_shipped_bundle_does_not_declare_or_name_the_metrics_connector() -> None:
    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text("utf-8"))
    assert "metrics" not in connectors["connectors"]
    manifest = json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text("utf-8"))
    assert "metrics/" not in json.dumps([manifest["toolPolicy"], manifest["approvalPolicy"]])
    assert _errors(BUNDLE) == _SHIPPED_ERRORS


def test_the_opt_in_fragment_yields_a_bundle_intake_accepts(tmp_path: Path) -> None:
    root = _opted_in(tmp_path, _load(OPT_IN))
    assert sorted(_errors(root)) == sorted(
        [*_SHIPPED_ERRORS, ("connectors.lock_missing", "connectors.metrics")]
    )


def test_both_opt_ins_together_yield_a_bundle_intake_accepts(tmp_path: Path) -> None:
    root = _opted_in(tmp_path, _load(SCALER_OPT_IN), _load(OPT_IN))
    assert sorted(_errors(root)) == sorted(
        [
            *_SHIPPED_ERRORS,
            ("connectors.lock_missing", "connectors.metrics"),
            ("connectors.lock_missing", "connectors.scaler"),
        ]
    )


def test_the_opt_in_allows_the_read_and_gates_nothing() -> None:
    plugin = _load(OPT_IN)["plugin"]
    assert plugin["toolPolicy"] == {"allow": ["metrics/query_value"]}
    assert "approvalPolicy" not in plugin


def test_naming_the_metrics_tool_without_declaring_it_is_refused(tmp_path: Path) -> None:
    """The plugin half alone is invalid, so the two halves travel together."""

    codes = {code for code, _ in _errors(_opted_in(tmp_path, {"plugin": _load(OPT_IN)["plugin"]}))}
    assert "tool_policy.unknown_server" in codes


def test_the_opt_in_names_a_prometheus_url() -> None:
    env = _load(OPT_IN)["connector"]["metrics"]["env"]
    assert env["METRICS_URL"].startswith(("http://", "https://"))


def test_the_example_policy_is_a_valid_document_pairing_scaler_and_metrics() -> None:
    document = json.loads(POLICY.read_text("utf-8"))
    validated = validate_document(document)
    (action,) = validated["actions"]
    assert action["connector"] == "scaler"
    assert action["tool"] == "scale_deployment"
    for read in ("precondition", "verifier"):
        assert action[read]["connector"] == "metrics"
        assert action[read]["tool"] == "query_value"
        assert action[read]["pointer"] == "/data/0/value/1"
    scaler_env = _load(SCALER_OPT_IN)["connector"]["scaler"]["env"]
    assert action["target"]["allowed"] == scaler_env["SCALER_DEPLOYMENTS"].split(",")
    assert action["automatic"] is False
    assert action["qualification"] is None


def test_the_metrics_connector_is_independent_of_the_scaler() -> None:
    """The credential half of ``verifier_not_independent``, on the real declarations."""

    scaler = ConnectorSpec.model_validate(_load(SCALER_OPT_IN)["connector"]["scaler"])
    metrics = ConnectorSpec.model_validate(_load(OPT_IN)["connector"]["metrics"])
    assert not header_secret_names(scaler) & header_secret_names(metrics)
