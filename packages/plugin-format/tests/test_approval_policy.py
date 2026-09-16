"""Unit table for ``plugin_format.grantable_routes`` (#558).

The operator opt-in ``grantableViaPolicy`` marks a gate whose policy-gate
approval MAY mint a one-shot grant for the tool the gate names (its ``gate``
field, MANIFEST-supplied, never model-supplied). ``grantable_routes`` derives the
``{route: tool}`` map the runner and worker consume, plus the set of routes that
are AMBIGUOUS (one route claimed by more than one distinct grantable tool) and so
excluded from the map. Comparison is case-sensitive and both fields are stripped,
mirroring ``load_approval_policy``'s normalization so a config that validates
green at deploy resolves identically at runtime (#453).
"""

import json
import shutil
from pathlib import Path

import yaml
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    ApprovalGate,
    grantable_routes,
    validate_bundle,
)


def _gate(gate: str, route: str, grantable: bool = False) -> ApprovalGate:
    return ApprovalGate.model_validate(
        {"gate": gate, "route": route, "grantableViaPolicy": grantable}
    )


def test_empty_input_yields_empty_map_and_no_ambiguity() -> None:
    assert grantable_routes([]) == ({}, set())


def test_single_grantable_gate_maps_route_to_tool() -> None:
    routes, ambiguous = grantable_routes([_gate("close_issue", "deal-desk", grantable=True)])
    assert routes == {"deal-desk": "close_issue"}
    assert ambiguous == set()


def test_non_grantable_gates_are_ignored() -> None:
    # Absent / false grantableViaPolicy contributes nothing.
    routes, ambiguous = grantable_routes(
        [
            _gate("close_issue", "deal-desk", grantable=False),
            _gate("escalate", "managers"),
        ]
    )
    assert routes == {}
    assert ambiguous == set()


def test_route_with_two_distinct_tools_is_ambiguous_and_excluded() -> None:
    routes, ambiguous = grantable_routes(
        [
            _gate("close_issue", "deal-desk", grantable=True),
            _gate("escalate", "deal-desk", grantable=True),
        ]
    )
    # Excluded from the resolved map AND surfaced in the ambiguous set.
    assert routes == {}
    assert ambiguous == {"deal-desk"}


def test_duplicate_same_tool_same_route_is_not_ambiguous() -> None:
    # One route, one DISTINCT tool (declared twice) is a duplicate, not a
    # conflict: a single entry, nothing ambiguous.
    routes, ambiguous = grantable_routes(
        [
            _gate("close_issue", "deal-desk", grantable=True),
            _gate("close_issue", "deal-desk", grantable=True),
        ]
    )
    assert routes == {"deal-desk": "close_issue"}
    assert ambiguous == set()


def test_gate_and_route_are_stripped() -> None:
    routes, ambiguous = grantable_routes(
        [_gate("  close_issue  ", "  deal-desk  ", grantable=True)]
    )
    assert routes == {"deal-desk": "close_issue"}
    assert ambiguous == set()


def test_blank_gate_or_route_is_ignored() -> None:
    # A grantable gate whose gate or route is empty once stripped keys nothing.
    routes, ambiguous = grantable_routes(
        [
            _gate("   ", "deal-desk", grantable=True),
            _gate("close_issue", "   ", grantable=True),
        ]
    )
    assert routes == {}
    assert ambiguous == set()


def test_route_matching_is_case_sensitive() -> None:
    # Deal-Desk and deal-desk are distinct routes, so two grantable gates on
    # them do not collide: both resolve, nothing is ambiguous.
    routes, ambiguous = grantable_routes(
        [
            _gate("close_issue", "Deal-Desk", grantable=True),
            _gate("escalate", "deal-desk", grantable=True),
        ]
    )
    assert routes == {"Deal-Desk": "close_issue", "deal-desk": "escalate"}
    assert ambiguous == set()


# --------------------------------------------------------------------------- #
# A gate may only name a connector the bundle declares -- #1495, and #1691's
# constraint that it must keep holding once a third connector form exists
# --------------------------------------------------------------------------- #
def test_sre_bot_declares_policy_scoped_kubernetes_connector_and_validates(
    tmp_path: Path,
) -> None:
    source = Path(__file__).resolve().parents[3] / "examples" / "sre-bot"
    plugin = json.loads(
        (source / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8")
    )
    connectors = yaml.safe_load(
        (source / "connectors.yaml").read_text(encoding="utf-8")
    )

    # Platform upgrades keep their separate zero-argument connector gates. The
    # general Kubernetes surface is classified by toolPolicy instead.
    assert plugin["approvalPolicy"]["gates"] == [
        {
            "gate": "mcp__self-upgrade__upgrade_self",
            "route": "sre-approvals",
        },
        {
            "gate": "mcp__self-upgrade__upgrade_platform",
            "route": "sre-approvals",
        },
    ]
    assert set(plugin["toolPolicy"]) == {
        "enforcement",
        "allow",
        "approvalRequired",
        "deny",
    }
    assert "k8s-write" not in connectors["connectors"]
    assert "k8s-scale" not in connectors["connectors"]
    assert connectors["connectors"]["kubernetes"]["secret_files"] == {
        "K8S_KUBECONFIG": "/secrets/kubeconfig"
    }
    # A separate kubeconfig, and the one bound to the widest grant in the bundle:
    # `create` on `jobs` cannot be narrowed with `resourceNames`, so the ceiling
    # is the connector taking no arguments rather than RBAC
    # (manifests/upgrade-role.yaml). Reusing any of the other three would either
    # widen them into the platform's own namespace or widen this one into the
    # workloads the bot may touch.
    assert connectors["connectors"]["self-upgrade"]["secret_files"] == {
        "SELF_UPGRADE_KUBECONFIG": "/secrets/kubeconfig"
    }
    assert connectors["connectors"]["self-upgrade"]["build"] == {
        "context": "connectors/self-upgrade",
        "platforms": ["linux/amd64", "linux/arm64"],
    }

    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    bundle = tmp_path / "sre-bot"
    shutil.copytree(source, bundle)
    locked_connectors = {}
    for name, declaration in connectors["connectors"].items():
        if "build" not in declaration:
            continue
        build = ConnectorBuild.model_validate(declaration["build"])
        context = bundle / declaration["build"]["context"]
        locked_connectors[name] = {
            "image": _LOCAL_IMAGE,
            "delivery": "local-daemon",
            "platforms": declaration["build"]["platforms"],
            "source_digest": connector_lock.source_digest_of(context, build),
        }
    (bundle / connector_lock.CONNECTOR_LOCK_FILE).write_text(
        yaml.safe_dump(
            {"version": 1, "connectors": locked_connectors}, sort_keys=False
        ),
        encoding="utf-8",
    )

    result = validate_bundle(
        str(bundle), enforces_tool_policy=TOOL_POLICY_ENFORCEMENT
    )
    assert result.valid, [(error.code, error.message) for error in result.errors]


def _gated_bundle(root: Path, connectors_yaml: str, gate: str) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": "acme-bot",
                "version": "0.1.0",
                "description": "t",
                "approvalPolicy": {"gates": [{"gate": gate, "route": "acme-oncall"}]},
            }
        ),
        encoding="utf-8",
    )
    (root / "skills" / "acme-bot").mkdir(parents=True, exist_ok=True)
    (root / "skills" / "acme-bot" / "SKILL.md").write_text(
        "---\nname: acme-bot\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors_yaml, encoding="utf-8")
    _materialize_build_inputs(root)
    return root


BUILD_ONLY = (
    "connectors:\n"
    "  k8s-write:\n"
    "    build:\n"
    "      context: connectors/k8s-write\n"
    "      platforms: [linux/amd64, linux/arm64]\n"
)

# A bare `sha256:` plus 64 lowercase hex is the local image id
# `docker image inspect --format {{.Id}}` reports, which is the form a
# `delivery: local-daemon` lock records. Placeholder hex, not a real digest.
_LOCAL_IMAGE = "sha256:1111111111111111111111111111111111111111111111111111111111111111"


def _materialize_build_inputs(root: Path) -> None:
    """Make the declared build context real and lock it.

    A `build:` declaration names a directory inside the bundle and a lock that
    pins what it resolved to; a fixture that ships neither is not a bundle
    intake would ever accept, so a gate test written against one would be
    asserting about a shape that cannot exist. These two tests are about the
    APPROVAL GATE, so the bundle around the gate has to be otherwise valid or a
    failure here says nothing about the gate.
    """

    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    context = root / "connectors" / "k8s-write"
    context.mkdir(parents=True, exist_ok=True)
    (context / "Dockerfile").write_text(
        "FROM scratch\nCOPY server.py /server.py\n", encoding="utf-8"
    )
    (context / "server.py").write_text("print('acme')\n", encoding="utf-8")

    build = ConnectorBuild.model_validate(
        {"context": "connectors/k8s-write", "platforms": ["linux/amd64", "linux/arm64"]}
    )
    (root / connector_lock.CONNECTOR_LOCK_FILE).write_text(
        "version: 1\n"
        "connectors:\n"
        "  k8s-write:\n"
        f"    image: {_LOCAL_IMAGE}\n"
        "    delivery: local-daemon\n"
        "    platforms: [linux/amd64, linux/arm64]\n"
        f"    source_digest: {connector_lock.source_digest_of(context, build)}\n",
        encoding="utf-8",
    )


def test_a_gate_naming_a_build_only_connector_validates(tmp_path: Path) -> None:
    # The accepted tool-name prefix set is derived from the DECLARED connector
    # names, and a bundle whose whole tool surface is source-built has only
    # build: forms. If the derivation ever narrows to connectors carrying an
    # `image:`, every gate such a bundle could write is rejected and no
    # connector tool can be gated at all -- which is #1691's write connector,
    # ungatable, so the example could not ship its security shape.
    root = _gated_bundle(tmp_path, BUILD_ONLY, "mcp__k8s-write__resources_create_or_update")
    result = validate_bundle(str(root))
    assert result.valid, [e.code for e in result.errors]


def test_a_gate_naming_an_undeclared_connector_still_fails(tmp_path: Path) -> None:
    # The other half, and the one that makes the test above mean something: a
    # derivation that accepted every prefix would pass the positive case
    # vacuously. A typo'd connector name arms a literal the runtime never
    # produces, so the gate validates green and silently never fires (#453).
    root = _gated_bundle(tmp_path, BUILD_ONLY, "mcp__k8s-write-typo__resources_create_or_update")
    result = validate_bundle(str(root))
    assert not result.valid
    assert "approval_policy.gate_not_namespaced" in {e.code for e in result.errors}
