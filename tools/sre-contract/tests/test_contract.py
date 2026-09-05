"""Consumer tests use temporary bundles and HTTP fixtures, never live proof."""

import copy
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
CHECK = ROOT / "tools/sre-contract/check.py"


def run(bundle, *args):
    env = dict(os.environ, PYTHONPATH=str(ROOT / "packages/plugin-format/src"))
    return subprocess.run(
        [sys.executable, str(CHECK), "--bundle", str(bundle), *args],
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def healthy(tmp_path):
    bundle = tmp_path / "sre-bot"
    shutil.copytree(ROOT / "examples/sre-bot", bundle)
    return bundle


def test_observability_policy_mutation_and_restoration(healthy):
    result = run(healthy)
    assert result.returncode == 0, result.stdout + result.stderr
    path = healthy / ".claude-plugin/plugin.json"
    original = path.read_bytes()
    manifest = json.loads(original)
    manifest["toolPolicy"]["allow"] = [
        item
        for item in manifest["toolPolicy"]["allow"]
        if not item.startswith(("grafana/", "tempo/")) and item != "self-upgrade/latest_release"
    ]
    path.write_text(json.dumps(manifest))
    result = run(healthy)
    assert result.returncode == 1
    assert "grafana/query_loki_logs" in result.stderr
    assert "self-upgrade/latest_release" in result.stderr
    path.write_bytes(original)
    assert run(healthy).returncode == 0


@pytest.mark.parametrize(
    "mutation, expected",
    [
        ("policy", "kubernetes/pods_list"),
        ("connector", "connector set"),
        ("permission", "permission map"),
        ("duplicate-map", "permission map"),
        ("gate", "self-upgrade/upgrade_self"),
        ("tier", "schema"),
        ("schema", "schema"),
    ],
)
def test_deliberate_mutations_reject_then_restore(healthy, mutation, expected):
    paths = [
        healthy / ".claude-plugin/plugin.json",
        healthy / "connectors.yaml",
        healthy / "supported-surface.json",
        healthy / "docs/PERMISSION-MAP.md",
    ]
    originals = {path: path.read_bytes() for path in paths}
    manifest = json.loads(paths[0].read_text())
    contract = json.loads(paths[2].read_text())
    if mutation == "policy":
        manifest["toolPolicy"]["deny"].append("kubernetes/pods_list")
        paths[0].write_text(json.dumps(manifest))
    elif mutation == "connector":
        data = yaml.safe_load(paths[1].read_text())
        del data["connectors"]["tempo"]
        paths[1].write_text(yaml.safe_dump(data))
    elif mutation == "permission":
        paths[3].write_text(
            paths[3].read_text().replace("`kubernetes/pods_list`", "`kubernetes/missing`")
        )
    elif mutation == "duplicate-map":
        paths[3].write_text("| `kubernetes/pods_list` | `deny` |\n" + paths[3].read_text())
    elif mutation == "gate":
        manifest["approvalPolicy"]["gates"] = manifest["approvalPolicy"]["gates"][1:]
        paths[0].write_text(json.dumps(manifest))
    elif mutation == "tier":
        del contract["tiers"]["local"]
        paths[2].write_text(json.dumps(contract))
    else:
        contract["connectors"]["kubernetes"]["tools"]["pods_list"] = "maybe"
        paths[2].write_text(json.dumps(contract))
    result = run(healthy)
    assert result.returncode == 1, result.stdout + result.stderr
    assert expected in result.stderr
    for path, content in originals.items():
        path.write_bytes(content)
    result = run(healthy)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture
def catalogs(healthy, tmp_path):
    """Actual MCP client HTTP transport against synthetic external servers.

    Wire shape: https://modelcontextprotocol.io/specification/2025-06-18/server/tools
    This deliberately does not claim pinned connector image or model coverage.
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    surface = json.loads((healthy / "supported-surface.json").read_text())
    responses = {
        name: [
            {
                "name": tool,
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": decision == "allow"},
            }
            for tool, decision in spec["tools"].items()
        ]
        for name, spec in surface["connectors"].items()
    }
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(405)
            self.end_headers()

        def do_DELETE(self):
            self.send_response(204)
            self.end_headers()

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            name = self.path.strip("/")
            calls.append((name, payload["method"]))
            if "id" not in payload:
                self.send_response(202)
                self.end_headers()
                return
            result = (
                {
                    "protocolVersion": payload["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fixture", "version": "1"},
                }
                if payload["method"] == "initialize"
                else (
                    responses[name](payload.get("params", {}).get("cursor"))
                    if callable(responses[name])
                    else {"tools": responses[name]}
                )
            )
            body = json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    endpoint = tmp_path / "endpoints.json"
    endpoint.write_text(
        json.dumps({n: f"http://127.0.0.1:{server.server_port}/{n}" for n in responses})
    )
    try:
        yield responses, calls, endpoint
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_image_only_live_consumer_mutation_and_restoration(healthy, catalogs):
    responses, calls, endpoint = catalogs
    assert not (healthy / "connectors/kubernetes/server.py").exists()
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 0, result.stdout + result.stderr
    # Both the SDK catalog reader and existing gate checker really ran.
    assert calls.count(("kubernetes", "tools/list")) == 2
    responses["kubernetes"].append(
        {
            "name": "new_write",
            "inputSchema": {"type": "object"},
            "annotations": {"readOnlyHint": False},
        }
    )
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    assert "kubernetes/new_write: uncovered live tool" in result.stderr
    assert "assert-gates-are-live-tools: failed" in result.stderr
    responses["kubernetes"].pop()
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0


def test_pinned_grafana_alert_read_catalog_through_consumer(healthy, catalogs):
    # The pinned producer selects ManageRulesRead under -disable-write:
    # https://github.com/grafana/mcp-grafana/blob/130384b1be0ce618e35e0b9c8f38c4ec17bf9367/tools/alerting.go
    # Its operation enum comes from ManageRulesReadParams, not the bundle policy:
    # https://github.com/grafana/mcp-grafana/blob/130384b1be0ce618e35e0b9c8f38c4ec17bf9367/tools/alerting_manage_rules_types.go
    responses, _, endpoint = catalogs
    responses["grafana"] = [
        tool
        for tool in responses["grafana"]
        if tool["name"] not in {"list_alert_rules", "alerting_manage_rules"}
    ] + [
        {
            "name": "alerting_manage_rules",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "operation": {"type": "string", "enum": ["list", "get", "versions"]}
                },
                "required": ["operation"],
            },
            "annotations": {"readOnlyHint": True},
        }
    ]
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 0, result.stdout + result.stderr

    # A server advertising the write-capable variant must not inherit the read grant.
    original = copy.deepcopy(responses["grafana"])
    responses["grafana"][-1]["annotations"]["readOnlyHint"] = False
    responses["grafana"][-1]["inputSchema"]["properties"]["operation"]["enum"] += [
        "create",
        "update",
        "delete",
    ]
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    assert "grafana/alerting_manage_rules: ungated live write" in result.stderr
    responses["grafana"] = original
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0


@pytest.mark.parametrize(
    "mutation, expected",
    [
        ("empty", "empty or duplicate"),
        ("duplicate", "empty or duplicate"),
        ("malformed", "malformed tool name"),
        ("write", "ungated live write"),
        ("forbidden", "forbidden tool"),
        ("missing-endpoint", "every declared connector"),
    ],
)
def test_catalog_failure_is_not_a_skip(healthy, catalogs, mutation, expected):
    responses, _, endpoint = catalogs
    original = copy.deepcopy(responses)
    endpoint_original = endpoint.read_bytes()
    if mutation == "empty":
        responses["grafana"] = []
    elif mutation == "duplicate":
        responses["grafana"].append(copy.deepcopy(responses["grafana"][0]))
    elif mutation == "malformed":
        responses["grafana"][0]["name"] = "bad/name"
    elif mutation == "write":
        responses["grafana"][0]["annotations"]["readOnlyHint"] = False
    elif mutation == "forbidden":
        responses["kubernetes"].append(
            {
                "name": "configuration_view",
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": True},
            }
        )
    else:
        data = json.loads(endpoint.read_text())
        del data["kubernetes"]
        endpoint.write_text(json.dumps(data))
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    assert expected in result.stderr
    responses.clear()
    responses.update(original)
    endpoint.write_bytes(endpoint_original)
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0


def test_workflow_and_verification_entry_are_bound_to_consumers():
    workflow = yaml.safe_load((ROOT / ".github/workflows/plugin-compat.yaml").read_text())
    jobs = workflow["jobs"]
    commands = [s.get("run") for s in jobs["sre-contract"]["steps"]]
    assert "python tools/sre-contract/check.py" in commands
    assert "python -m pytest -q tools/sre-contract/tests" in commands
    assert jobs["sre-live-catalog"]["needs"] == "sre-contract"
    assert "python tools/sre-contract/catalog_ci.py" in [
        s.get("run") for s in jobs["sre-live-catalog"]["steps"]
    ]
    docs = (ROOT / "docs/agents.md").read_text()
    assert 'python tools/sre-contract/check.py --endpoints "$SRE_CONNECTOR_ENDPOINTS"' in docs
    assert "python tools/sre-contract/catalog_ci.py" in docs


def test_ci_driver_cleans_its_resources_on_launch_failure(tmp_path):
    # Docker is an external process fixture. No daemon or image is used.
    executable = tmp_path / "docker"
    log = tmp_path / "docker.jsonl"
    executable.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ['DOCKER_TEST_LOG'], 'a') as f:
    f.write(json.dumps(sys.argv[1:]) + '\\n')
sys.exit(1 if sys.argv[1] == 'run' else 0)
""")
    executable.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp_path}:{os.environ['PATH']}", DOCKER_TEST_LOG=str(log))
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools/sre-contract/catalog_ci.py")],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    launch, cleanup = commands
    assert launch[0] == "run"
    name = launch[launch.index("--name") + 1]
    assert name.startswith("sre-catalog-")
    assert cleanup == ["rm", "-f", name]
    assert "127.0.0.1::8000" in launch
    assert "--cap-drop=ALL" in launch
    mount = Path(launch[launch.index("-v") + 1].split(":")[0])
    assert not mount.exists()


@pytest.mark.parametrize("mutation", ["second-page-write", "repeated-cursor", "page-bound"])
def test_paginated_catalog_mutations_reject_then_restore(healthy, catalogs, mutation):
    responses, _, endpoint = catalogs
    original = copy.deepcopy(responses["grafana"])

    def healthy_pages(cursor):
        return (
            {"tools": original[:1], "nextCursor": "second"}
            if cursor is None
            else {"tools": original[1:]}
        )

    responses["grafana"] = healthy_pages
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 0, result.stdout + result.stderr
    if mutation == "second-page-write":

        def bad_pages(cursor):
            page = healthy_pages(cursor)
            if cursor is not None:
                page = copy.deepcopy(page)
                page["tools"][0]["annotations"]["readOnlyHint"] = False
            return page
    elif mutation == "repeated-cursor":

        def bad_pages(cursor):
            return {"tools": original, "nextCursor": "loop"}
    else:

        def bad_pages(cursor):
            return {"tools": original, "nextCursor": str(int(cursor or "0") + 1)}

    responses["grafana"] = bad_pages
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    if mutation == "second-page-write":
        assert "ungated live write" in result.stderr
    else:
        assert "SRE contract failed:" in result.stderr
    responses["grafana"] = healthy_pages
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0


# Name/readOnlyHint projection of the pinned producer; raw descriptions and schemas
# belong in private evidence. Registrations and read variants are bound by:
# https://github.com/grafana/mcp-grafana/blob/130384b1be0ce618e35e0b9c8f38c4ec17bf9367/cmd/mcp-grafana/main.go
# The actual-image consumer remains mandatory; this external HTTP fixture cannot
# establish image identity, configured operation refusal or upstream effects.
PINNED_GRAFANA_READ_TOOLS = (
    "alerting_manage_routing",
    "alerting_manage_rules",
    "analyze_loki_labels",
    "check_datasources_health",
    "generate_deeplink",
    "get_alert_group",
    "get_annotation_tags",
    "get_annotations",
    "get_assertions",
    "get_current_oncall_users",
    "get_dashboard_by_uid",
    "get_dashboard_panel_queries",
    "get_dashboard_property",
    "get_dashboard_summary",
    "get_datasource",
    "get_oncall_shift",
    "get_query_examples",
    "get_sift_analysis",
    "get_sift_investigation",
    "list_alert_groups",
    "list_cloudwatch_dimensions",
    "list_cloudwatch_metrics",
    "list_cloudwatch_namespaces",
    "list_datasources",
    "list_loki_label_names",
    "list_loki_label_values",
    "list_oncall_schedules",
    "list_oncall_teams",
    "list_oncall_users",
    "list_prometheus_label_names",
    "list_prometheus_label_values",
    "list_prometheus_metric_metadata",
    "list_prometheus_metric_names",
    "list_pyroscope_label_names",
    "list_pyroscope_label_values",
    "list_pyroscope_profile_types",
    "list_sift_investigations",
    "query_cloudwatch",
    "query_loki_logs",
    "query_loki_patterns",
    "query_loki_stats",
    "query_prometheus",
    "query_prometheus_histogram",
    "query_pyroscope",
    "run_panel_query",
    "search_dashboards",
    "search_folders",
    "suggest_loki_alloy_label_config",
)


@pytest.fixture
def pinned_grafana_catalog(catalogs):
    responses, calls, endpoint = catalogs
    responses["grafana"] = [
        {
            "name": name,
            "inputSchema": {"type": "object"},
            "annotations": {"readOnlyHint": True},
        }
        for name in PINNED_GRAFANA_READ_TOOLS
    ]
    return responses, calls, endpoint


def test_complete_pinned_grafana_catalog(healthy, pinned_grafana_catalog):
    _, _, endpoint = pinned_grafana_catalog
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads((healthy / ".claude-plugin/plugin.json").read_text())
    for collection in ("allow", "approvalRequired", "deny"):
        assert all(not any(c in item for c in "*?[") for item in manifest["toolPolicy"][collection])


@pytest.mark.parametrize("tool", ["query_loki_logs", "alerting_manage_rules", "list_datasources"])
def test_pinned_read_grant_removal_rejects_then_restores(healthy, pinned_grafana_catalog, tool):
    _, _, endpoint = pinned_grafana_catalog
    path = healthy / ".claude-plugin/plugin.json"
    original = path.read_bytes()
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0
    manifest = json.loads(original)
    manifest["toolPolicy"]["allow"].remove("grafana/" + tool)
    path.write_text(json.dumps(manifest))
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    assert f"grafana/{tool}: expected allow, got deny" in result.stderr
    assert f"grafana/{tool}: uncovered live tool" in result.stderr
    path.write_bytes(original)
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0


@pytest.mark.parametrize("mutation", ["missing", "extra", "write-as-allow"])
def test_complete_catalog_mutations_reject_then_restore(healthy, pinned_grafana_catalog, mutation):
    responses, _, endpoint = pinned_grafana_catalog
    original = copy.deepcopy(responses["grafana"])
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0
    if mutation == "missing":
        responses["grafana"] = [t for t in original if t["name"] != "alerting_manage_rules"]
    elif mutation == "extra":
        responses["grafana"].append(
            {
                "name": "unreviewed_read",
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": True},
            }
        )
    else:
        next(t for t in responses["grafana"] if t["name"] == "alerting_manage_rules")[
            "annotations"
        ]["readOnlyHint"] = False
    result = run(healthy, "--endpoints", str(endpoint))
    assert result.returncode == 1
    if mutation == "write-as-allow":
        assert "grafana/alerting_manage_rules: ungated live write" in result.stderr
    else:
        assert "grafana: live catalog differs from supported surface" in result.stderr
    if mutation == "extra":
        assert "grafana/unreviewed_read: uncovered live tool" in result.stderr
    responses["grafana"] = original
    assert run(healthy, "--endpoints", str(endpoint)).returncode == 0
