"""@spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md"""

from __future__ import annotations

import copy
import subprocess
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "charts" / "curie"
VALKEY_ALLOW = "curie-valkey-allow-app-ingress"


def render(overlay: dict | None = None) -> dict[str, dict]:
    with tempfile.TemporaryDirectory() as tmp:
        command = ["helm", "template", "curie", str(CHART), "-f", str(CHART / "values-dev.yaml")]
        if overlay is not None:
            values = Path(tmp) / "values.yaml"
            values.write_text(yaml.safe_dump(overlay))
            command += ["-f", str(values)]
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode:
        raise AssertionError(result.stderr)
    objects = [obj for obj in yaml.safe_load_all(result.stdout) if obj]
    return {
        obj["metadata"]["name"]: obj
        for obj in objects
        if obj.get("kind") == "NetworkPolicy"
    }


def peer_rule(policy: dict, component: str) -> dict:
    return next(
        rule
        for rule in policy["spec"]["ingress"]
        if any(
            peer.get("podSelector", {})
            .get("matchLabels", {})
            .get("app.kubernetes.io/component")
            == component
            for peer in rule.get("from", [])
        )
    )


def app_rule(policy: dict) -> dict:
    return next(
        rule
        for rule in policy["spec"]["ingress"]
        if any(
            peer.get("podSelector", {})
            .get("matchLabels", {})
            .get("app.kubernetes.io/name")
            == "curie"
            and peer["podSelector"]["matchLabels"].get("app.kubernetes.io/instance") == "curie"
            for peer in rule.get("from", [])
        )
    )


def test_omitted_and_old_release_values_preserve_the_mcp_connector_peer():
    """@spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md"""
    omitted = render()
    old_values = render({"security": {"dataTierNetworkPolicy": {"enabled": True}}})
    explicit_true = render({"security": {"dataTierNetworkPolicy": {
        "allowMcpConnectorValkeyIngress": True,
    }}})
    assert omitted == old_values == explicit_true
    connector = peer_rule(omitted[VALKEY_ALLOW], "mcp-connector")
    assert connector["ports"] == [{"protocol": "TCP", "port": 6379}]


def test_null_uses_the_same_default_on_behavior_as_omission():
    """@spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md"""
    omitted = render()
    explicit_null = render({"security": {"dataTierNetworkPolicy": {
        "allowMcpConnectorValkeyIngress": None,
    }}})
    assert explicit_null == omitted
    assert peer_rule(explicit_null[VALKEY_ALLOW], "mcp-connector")["ports"] == [
        {"protocol": "TCP", "port": 6379},
    ]


def test_false_removes_only_the_mcp_connector_valkey_peer():
    """@spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md"""
    current = render()
    opted_out = render({"security": {"dataTierNetworkPolicy": {
        "allowMcpConnectorValkeyIngress": False,
    }}})
    expected = copy.deepcopy(current)
    expected[VALKEY_ALLOW]["spec"]["ingress"].remove(
        peer_rule(expected[VALKEY_ALLOW], "mcp-connector")
    )
    assert opted_out == expected
    assert app_rule(opted_out[VALKEY_ALLOW])["ports"] == [
        {"protocol": "TCP", "port": 6379},
    ]


def test_explicit_non_boolean_values_fail_chart_validation():
    """@spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md"""
    invalid = ("false", 0, [], {})
    for value in invalid:
        with tempfile.TemporaryDirectory() as tmp:
            values = Path(tmp) / "values.yaml"
            values.write_text(yaml.safe_dump({"security": {"dataTierNetworkPolicy": {
                "allowMcpConnectorValkeyIngress": value,
            }}}))
            result = subprocess.run(
                [
                    "helm",
                    "template",
                    "curie",
                    str(CHART),
                    "-f",
                    str(CHART / "values-dev.yaml"),
                    "-f",
                    str(values),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        assert result.returncode != 0, f"accepted invalid value type: {type(value).__name__}"


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_") and callable(test):
            test()
            print(f"PASS {name}")
