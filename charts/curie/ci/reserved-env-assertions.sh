#!/usr/bin/env bash
# Exercise the Helm consumer, including a guard-removed duplicate control (#2297).
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)"
python3 - "$CHART" <<'PY'
import collections
import pathlib
import shutil
import subprocess
import sys
import tempfile

import yaml

chart = pathlib.Path(sys.argv[1])
base = {
    "dispatcher": {"slack": {"appToken": "xapp-example", "botToken": "xoxb-example"}},
    "agentSandbox": {"runner": {"credentials": "example", "workspace": {"enabled": True}}},
}
workloads = {
    "worker": ("worker.yaml", "worker"),
    "api": ("api.yaml", "api"),
    "dispatcher": ("dispatcher.yaml", "dispatcher"),
    "agentSandbox.runner": ("agent-sandbox.yaml", "runner"),
    "otelCollector": ("otel-collector.yaml", "otel-collector"),
}
overrides = {"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_HEADERS"}

def envs(output, filename, container):
    docs = yaml.safe_load_all((output / "curie/templates" / filename).read_text())
    def walk(value):
        if isinstance(value, dict):
            if value.get("name") == container and "env" in value:
                yield value["env"]
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)
    return next(env for doc in docs for env in walk(doc))

def sandbox_runner_envs(output, template_name):
    docs = yaml.safe_load_all((output / "curie/templates/agent-sandbox.yaml").read_text())
    templates = [doc for doc in docs if doc and doc.get("kind") == "SandboxTemplate" and doc["metadata"]["name"] == template_name]
    assert len(templates) == 1, f"expected one SandboxTemplate named {template_name}"
    runners = [container for container in templates[0]["spec"]["podTemplate"]["spec"]["containers"] if container["name"] == "runner"]
    assert len(runners) == 1, f"expected one runner in {template_name}"
    return runners[0]["env"]

with tempfile.TemporaryDirectory() as tmp:
    tmp = pathlib.Path(tmp)
    values = tmp / "values.yaml"
    values.write_text(yaml.safe_dump(base))
    count = 0
    def render(source=chart, *args, ok=True):
        global count
        count += 1
        output = tmp / str(count)
        result = subprocess.run(["helm", "template", "acme", str(source), "-f", str(values), "--output-dir", str(output), *args], text=True, capture_output=True)
        if ok:
            assert result.returncode == 0, result.stderr
        return result, output
    _, baseline = render()
    for workload, (filename, container) in workloads.items():
        for entry in envs(baseline, filename, container):
            name = entry["name"]
            if name in overrides or workload == "worker" and name in {"CURIE_API_URL", "CURIE_API_KEY"}:
                continue  # Existing helpers replace, rather than duplicate, these entries.
            result, _ = render(chart, "--set", f"{workload}.extraEnv[0].name={name}", "--set-string", f"{workload}.extraEnv[0].value=conflict", ok=False)
            assert result.returncode != 0, f"accepted reserved {workload}.extraEnv {name}"
            assert f"{workload}.extraEnv" in result.stderr and name in result.stderr, result.stderr
    # Optional branches are reserved even before their feature is enabled.
    for workload, name, replacement in [
        ("worker", "SLACK_API_BASE_URL", "worker.slackApiBaseUrl"),
        ("worker", "CURIE_CONNECTOR_RECONCILE", "worker.connectorReconciler.enabled"),
        ("agentSandbox.runner", "ANTHROPIC_BASE_URL", "inference.service.port"),
    ]:
        result, _ = render(chart, "--set", f"{workload}.extraEnv[0].name={name}", "--set-string", f"{workload}.extraEnv[0].value=conflict", ok=False)
        assert result.returncode != 0 and replacement in result.stderr, result.stderr
    # Upgrade lifecycle inputs belong to Helm even when a particular worker
    # process does not consume every field. Reserving all three prevents a
    # retained worker.extraEnv value from rebinding a fresh installation to an
    # old drain key, revision, or mixed-version compatibility mode (#2374).
    for name in (
        "CURIE_INSTALLATION_ID",
        "CURIE_UPGRADE_REVISION",
        "CURIE_UPGRADE_LEGACY_QUIESCE",
    ):
        result, _ = render(
            chart,
            "--set",
            f"worker.extraEnv[0].name={name}",
            "--set-string",
            "worker.extraEnv[0].value=conflict",
            ok=False,
        )
        assert result.returncode != 0, f"accepted reserved worker.extraEnv {name}"
        assert "worker.extraEnv" in result.stderr and name in result.stderr, result.stderr
    for workload in workloads:
        result, _ = render(chart, "--set", f"{workload}.extraEnv[0].name=ACME_EXTENSION", "--set-string", f"{workload}.extraEnv[0].value=same", "--set", f"{workload}.extraEnv[1].name=ACME_EXTENSION", "--set-string", f"{workload}.extraEnv[1].value=same", ok=False)
        assert result.returncode != 0 and "repeats environment variable ACME_EXTENSION" in result.stderr, result.stderr
    for value in ("600", "901"):
        result, _ = render(chart, "--set", "worker.runnerTotalTimeoutSeconds=600", "--set", "worker.extraEnv[0].name=CURIE_RUNNER_TOTAL_TIMEOUT_S", "--set-string", f"worker.extraEnv[0].value={value}", ok=False)
        assert result.returncode != 0, "accepted exact or conflicting timeout duplicate"
        assert "worker.runnerTotalTimeoutSeconds" in result.stderr, result.stderr
    # Ordinary extension envs and the existing single-entry override contract survive.
    # Rail 1 requires a declared peer when CURIE_API_URL is external (#2367).
    _, output = render(
        chart,
        "--set",
        "worker.extraEnv[0].name=ACME_EXTENSION",
        "--set-string",
        "worker.extraEnv[0].value=preserved",
        "--set",
        "worker.extraEnv[1].name=CURIE_API_URL",
        "--set-string",
        "worker.extraEnv[1].value=https://api.example.com",
        "--set",
        "api.egress[0].cidr=192.0.2.21/32",
        "--set",
        "api.egress[0].ports[0].protocol=TCP",
        "--set",
        "api.egress[0].ports[0].port=443",
    )
    env = envs(output, "worker.yaml", "worker")
    assert [e for e in env if e["name"] == "ACME_EXTENSION"] == [{"name": "ACME_EXTENSION", "value": "preserved"}]
    assert [e for e in env if e["name"] == "CURIE_API_URL"] == [{"name": "CURIE_API_URL", "value": "https://api.example.com"}]
    # Dynamic connector names belong to the per-agent template, not the generic
    # runner selected by envs(). Pin both templates by name for these controls.
    connector_name = "GITHUB_PERSONAL_ACCESS_TOKEN"
    connector_path = f"agentSandbox.connectorSecrets.acme-a.{connector_name}"
    _, output = render(chart, "--set", f"agentSandbox.runner.extraEnv[0].name={connector_name}", "--set-string", "agentSandbox.runner.extraEnv[0].value=example-extension")
    generic_env = sandbox_runner_envs(output, "acme-curie-runner")
    assert [entry for entry in generic_env if entry["name"] == connector_name] == [{"name": connector_name, "value": "example-extension"}]
    _, output = render(chart, "--set-string", f"{connector_path}=example-secret", "--set", "agentSandbox.runner.extraEnv[0].name=ACME_CONNECTOR_EXTENSION", "--set-string", "agentSandbox.runner.extraEnv[0].value=preserved")
    for template_name in ("acme-curie-runner", "acme-curie-agent-acme-a-runner"):
        runner_env = sandbox_runner_envs(output, template_name)
        assert [entry for entry in runner_env if entry["name"] == "ACME_CONNECTOR_EXTENSION"] == [{"name": "ACME_CONNECTOR_EXTENSION", "value": "preserved"}]
        connector_entries = [entry for entry in runner_env if entry["name"] == connector_name]
        if template_name == "acme-curie-runner":
            assert connector_entries == [], connector_entries
        else:
            assert connector_entries == [{"name": connector_name, "valueFrom": {"secretKeyRef": {"name": "acme-curie-agent-acme-a-connector-secrets", "key": connector_name, "optional": False}}}], connector_entries
    for value in ("example-secret", "example-conflict"):
        result, _ = render(chart, "--set-string", f"{connector_path}=example-secret", "--set", f"agentSandbox.runner.extraEnv[0].name={connector_name}", "--set-string", f"agentSandbox.runner.extraEnv[0].value={value}", ok=False)
        assert result.returncode != 0, f"accepted per-agent connector duplicate with extraEnv value {value}"
        assert "agentSandbox.runner.extraEnv" in result.stderr and connector_path in result.stderr, result.stderr
    mutant = tmp / "mutant"
    shutil.copytree(chart, mutant)
    helper = mutant / "templates/_reserved-env.tpl"
    text = helper.read_text()
    start = '{{- if hasKey $reserved $name -}}'
    assert text.count(start) == 1
    helper.write_text(text.replace(start, '{{- if false -}}'))
    _, output = render(mutant, "--set", "worker.extraEnv[0].name=CURIE_RUNNER_TOTAL_TIMEOUT_S", "--set-string", "worker.extraEnv[0].value=901")
    duplicates = [e for e in envs(output, "worker.yaml", "worker") if e["name"] == "CURIE_RUNNER_TOTAL_TIMEOUT_S"]
    assert len(duplicates) == 2 and duplicates[1]["value"] == "901", duplicates
    print(f"OK: {count} Helm renders; chart-owned names rejected; exact/conflicting duplicates rejected; removed-guard control reproduced two timeout entries")
PY
