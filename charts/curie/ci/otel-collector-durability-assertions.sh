#!/usr/bin/env bash
# Static contract for the durable, three-signal Collector (#1818, #1819).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local output="$1"
  shift
  helm template curie "$CHART" "$@" > "$output"
}

cat > "$TMP/safe-extra-exporter.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-sink:
      endpoint: http://otel-sink.example.com:4318
      retry_on_failure:
        enabled: true
        initial_interval: 1s
        max_interval: 5s
        max_elapsed_time: 30s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-sink]
  extraLogPipelineExporters: [otlphttp/acme-sink]
  extraMetricPipelineExporters: [otlphttp/acme-sink]
YAML

cat > "$TMP/secret-backed-extra-exporter.yaml" <<'YAML'
otelCollector:
  extraEnv:
    - name: BACKEND_AUTH
      valueFrom:
        secretKeyRef:
          name: acme-otel-backend
          key: authorization
  extraExporters:
    otlphttp/acme-secure-sink:
      endpoint: https://otel-backend.example.com:4318
      headers:
        Authorization: ${env:BACKEND_AUTH}
      retry_on_failure:
        enabled: true
        initial_interval: 1s
        max_interval: 5s
        max_elapsed_time: 30s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-secure-sink]
YAML

cat > "$TMP/literal-sensitive-header.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-unsafe-sink:
      endpoint: https://otel-backend.example.com:4318
      headers:
        Authorization: Basic placeholder-not-a-credential
      retry_on_failure:
        enabled: true
        max_interval: 5s
        max_elapsed_time: 30s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-unsafe-sink]
YAML

cat > "$TMP/shadow-built-in-exporter.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/langfuse: {}
YAML

cat > "$TMP/metrics-ingress.yaml" <<'YAML'
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: observability
        podSelector:
          matchLabels:
            app.kubernetes.io/name: prometheus
YAML

cat > "$TMP/empty-metrics-peer.yaml" <<'YAML'
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - {}
YAML

cat > "$TMP/wildcard-pod-metrics-peer.yaml" <<'YAML'
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - podSelector: {}
YAML

cat > "$TMP/wildcard-namespace-metrics-peer.yaml" <<'YAML'
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - namespaceSelector: {}
YAML

cat > "$TMP/external-otel-workload-env.yaml" <<'YAML'
otelCollector:
  deploy: false
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
  extraEnv: &externalOtelEnv
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: https://otel.example.com:4318
    - name: OTEL_EXPORTER_OTLP_PROTOCOL
      value: http/protobuf
    - name: OTEL_EXPORTER_OTLP_HEADERS
      valueFrom:
        secretKeyRef:
          name: acme-otel-auth
          key: headers
api:
  extraEnv: *externalOtelEnv
worker:
  extraEnv: *externalOtelEnv
agentSandbox:
  runner:
    extraEnv: *externalOtelEnv
YAML

cat > "$TMP/internal-runner-otel-override.yaml" <<'YAML'
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
agentSandbox:
  runner:
    extraEnv:
      - name: OTEL_EXPORTER_OTLP_ENDPOINT
        value: https://otel.example.com:4318
      - name: OTEL_EXPORTER_OTLP_PROTOCOL
        value: grpc
YAML

cat > "$TMP/chart-owned-external.yaml" <<'YAML'
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  # Rail 1 needs a CIDR peer for an external collector (#2317); this
  # fixture only cares that the endpoint reaches the workloads.
  egress:
    - cidr: 192.0.2.40/32
      ports: [{ protocol: TCP, port: 4318 }]
  protocol: http/protobuf
  headersExistingSecret: acme-otel-auth
  headersSecretKey: headers
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
YAML

cat > "$TMP/chart-owned-external-literal-headers.yaml" <<'YAML'
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  # Rail 1 needs a CIDR peer for an external collector (#2317); this
  # fixture only cares that the endpoint reaches the workloads.
  egress:
    - cidr: 192.0.2.40/32
      ports: [{ protocol: TCP, port: 4318 }]
  headers: "x-scope-orgid=acme"
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
YAML

cat > "$TMP/explicit-disable.yaml" <<'YAML'
security:
  checkDefaultCredentials: true
otelCollector:
  deploy: false
  telemetryDisabled: true
langfuse:
  existingSecret: my-langfuse
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
YAML

cat > "$TMP/chart-owned-external-plus-extraenv-override.yaml" <<'YAML'
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  # Rail 1 needs a CIDR peer for an external collector (#2317); this
  # fixture only cares that the endpoint reaches the workloads.
  egress:
    - cidr: 192.0.2.40/32
      ports: [{ protocol: TCP, port: 4318 }]
  protocol: http/protobuf
dispatcher:
  deploy: true
  slack:
    appToken: placeholder-app-token
    botToken: placeholder-bot-token
agentSandbox:
  runner:
    extraEnv:
      - name: OTEL_EXPORTER_OTLP_ENDPOINT
        value: https://otel-override.example.com:4318
      - name: OTEL_EXPORTER_OTLP_PROTOCOL
        value: grpc
YAML

cat > "$TMP/both-header-sources.yaml" <<'YAML'
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  # Rail 1 needs a CIDR peer for an external collector (#2317); this
  # fixture only cares that the endpoint reaches the workloads.
  egress:
    - cidr: 192.0.2.40/32
      ports: [{ protocol: TCP, port: 4318 }]
  headers: "x-scope-orgid=acme"
  headersExistingSecret: acme-otel-auth
YAML

cat > "$TMP/disabled-with-endpoint.yaml" <<'YAML'
otelCollector:
  deploy: false
  telemetryDisabled: true
  endpoint: https://otel.example.com:4318
YAML

cat > "$TMP/disabled-with-deploy.yaml" <<'YAML'
otelCollector:
  deploy: true
  telemetryDisabled: true
YAML

cat > "$TMP/literal-sensitive-workload-headers.yaml" <<'YAML'
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  # Rail 1 needs a CIDR peer for an external collector (#2317); this
  # fixture only cares that the endpoint reaches the workloads.
  egress:
    - cidr: 192.0.2.40/32
      ports: [{ protocol: TCP, port: 4318 }]
  headers: "Authorization=Basic placeholder-not-a-credential"
YAML

render "$TMP/default.yaml"
render "$TMP/workload-wiring.yaml" \
  --set dispatcher.deploy=true \
  --set dispatcher.slack.appToken=placeholder-app-token \
  --set dispatcher.slack.botToken=placeholder-bot-token
render "$TMP/dev.yaml" -f "$CHART/values-dev.yaml"
render "$TMP/ephemeral.yaml" --set otelCollector.persistence.enabled=false
render "$TMP/storage-class.yaml" --set otelCollector.persistence.storageClass=acme-storage
render "$TMP/external.yaml" --set otelCollector.deploy=false
render "$TMP/external-workload-wiring.yaml" \
  --set otelCollector.deploy=false \
  --set dispatcher.deploy=true \
  --set dispatcher.slack.appToken=placeholder-app-token \
  --set dispatcher.slack.botToken=placeholder-bot-token
render "$TMP/extra.yaml" -f "$TMP/safe-extra-exporter.yaml"
render "$TMP/secret-backed-extra-exporter.yaml.out" \
  -f "$TMP/secret-backed-extra-exporter.yaml"
render "$TMP/metrics-ingress.yaml.out" -f "$TMP/metrics-ingress.yaml"
render "$TMP/custom-metrics-port.yaml" -f "$TMP/metrics-ingress.yaml" \
  --set otelCollector.service.metricsPort=9999
render "$TMP/network-policy-disabled.yaml" \
  --set security.otelCollectorNetworkPolicy.enabled=false
render "$TMP/external-otel-workload-env.yaml.out" \
  -f "$TMP/external-otel-workload-env.yaml"
render "$TMP/internal-runner-otel-override.yaml.out" \
  -f "$TMP/internal-runner-otel-override.yaml"
render "$TMP/chart-owned-external.yaml.out" \
  -f "$TMP/chart-owned-external.yaml"
render "$TMP/chart-owned-external-literal-headers.yaml.out" \
  -f "$TMP/chart-owned-external-literal-headers.yaml"
render "$TMP/explicit-disable.yaml.out" \
  -f "$TMP/explicit-disable.yaml" \
  --set-string otelCollector.otlpAuthHeader='Basic cGstbGYtb3duZWQ6c2stbGYtb3duZWQ='
render "$TMP/chart-owned-external-plus-extraenv-override.yaml.out" \
  -f "$TMP/chart-owned-external-plus-extraenv-override.yaml"

expect_render_failure() {
  local label="$1" expected="$2"
  shift 2
  local stderr="$TMP/$label.stderr"
  if helm template curie "$CHART" "$@" >/dev/null 2>"$stderr"; then
    fail "$label unexpectedly rendered"
  fi
  if ! grep -Fq "$expected" "$stderr"; then
    fail "$label failed without naming $expected: $(<"$stderr")"
  fi
}

# A pipeline may never name a component the rendered config does not define.
expect_render_failure missing-log-exporter otlphttp/missing \
  --set 'otelCollector.extraLogPipelineExporters[0]=otlphttp/missing'
expect_render_failure missing-metric-exporter otlphttp/missing \
  --set 'otelCollector.extraMetricPipelineExporters[0]=otlphttp/missing'

# The ConfigMap must never receive a literal credential header, and chart-owned
# exporters cannot be overridden through the otherwise extensible map. The
# rendered secretKeyRef/env expansion fixture above is the positive control.
expect_render_failure literal-sensitive-header 'must use Collector environment expansion ${env:NAME}' \
  -f "$TMP/literal-sensitive-header.yaml"
expect_render_failure shadow-built-in-exporter 'must not replace built-in exporter' \
  -f "$TMP/shadow-built-in-exporter.yaml"
expect_render_failure metrics-port-collides-with-otlp 'must not match an unrestricted Collector ingress port' \
  -f "$TMP/metrics-ingress.yaml" \
  --set otelCollector.service.metricsPort=4317

# A user-supplied network exporter is part of the same durability contract as
# the built-in Langfuse exporter. Pin both fail-closed branches independently:
# endpoint-only fails at retry validation, while retry-without-queue reaches and
# fails the persistent queue validation.
expect_render_failure endpoint-only-network-exporter retry_on_failure \
  --set 'otelCollector.extraExporters.otlphttp/acme-sink.endpoint=http://otel-sink.example.com:4318' \
  --set 'otelCollector.extraPipelineExporters[0]=otlphttp/acme-sink'

cat > "$TMP/retry-no-queue.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-sink:
      endpoint: http://otel-sink.example.com:4318
      retry_on_failure:
        enabled: true
        max_interval: 5s
        max_elapsed_time: 30s
  extraPipelineExporters: [otlphttp/acme-sink]
YAML
expect_render_failure retry-without-queue sending_queue \
  -f "$TMP/retry-no-queue.yaml"

cat > "$TMP/zero-max-elapsed-time.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-sink:
      endpoint: http://otel-sink.example.com:4318
      retry_on_failure:
        enabled: true
        max_interval: 5s
        max_elapsed_time: 0.0s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-sink]
YAML
expect_render_failure zero-max-elapsed-time "finite, non-zero" \
  -f "$TMP/zero-max-elapsed-time.yaml"

cat > "$TMP/zero-max-interval.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-sink:
      endpoint: http://otel-sink.example.com:4318
      retry_on_failure:
        enabled: true
        max_interval: 00s
        max_elapsed_time: 30s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-sink]
YAML
expect_render_failure zero-max-interval "finite, non-zero" \
  -f "$TMP/zero-max-interval.yaml"

cat > "$TMP/uppercase-retry-unit.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/acme-sink:
      endpoint: http://otel-sink.example.com:4318
      retry_on_failure:
        enabled: true
        max_interval: 5S
        max_elapsed_time: 30s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 8
  extraPipelineExporters: [otlphttp/acme-sink]
YAML
expect_render_failure uppercase-retry-unit "supported finite" \
  -f "$TMP/uppercase-retry-unit.yaml"

# An empty NetworkPolicyPeer, or an explicitly empty selector, matches every
# source. Refuse those fail-open shapes rather than turning a metrics allowlist
# into unrestricted Collector self-metrics access.
expect_render_failure empty-metrics-peer metricsIngress \
  -f "$TMP/empty-metrics-peer.yaml"
expect_render_failure wildcard-pod-metrics-peer metricsIngress \
  -f "$TMP/wildcard-pod-metrics-peer.yaml"
expect_render_failure wildcard-namespace-metrics-peer metricsIngress \
  -f "$TMP/wildcard-namespace-metrics-peer.yaml"

# Production-hardening gate: deploy=false without a chart-owned endpoint or an
# explicit disable is accidental missing, not an implied external collector.
expect_render_failure accidental-missing-external-endpoint telemetryDisabled \
  --set security.checkDefaultCredentials=true \
  --set otelCollector.deploy=false \
  --set langfuse.existingSecret=my-langfuse \
  --set-string otelCollector.otlpAuthHeader='Basic cGstbGYtb3duZWQ6c2stbGYtb3duZWQ='
expect_render_failure both-header-sources headersExistingSecret \
  -f "$TMP/both-header-sources.yaml"
expect_render_failure disabled-with-endpoint telemetryDisabled \
  -f "$TMP/disabled-with-endpoint.yaml"
expect_render_failure disabled-with-deploy telemetryDisabled \
  -f "$TMP/disabled-with-deploy.yaml"
expect_render_failure literal-sensitive-workload-headers headersExistingSecret \
  -f "$TMP/literal-sensitive-workload-headers.yaml"

python3 - \
  "$TMP/default.yaml" "$TMP/dev.yaml" "$TMP/ephemeral.yaml" \
  "$TMP/storage-class.yaml" "$TMP/external.yaml" "$TMP/extra.yaml" \
  "$TMP/workload-wiring.yaml" "$TMP/external-workload-wiring.yaml" \
  "$TMP/metrics-ingress.yaml.out" "$TMP/custom-metrics-port.yaml" \
  "$TMP/network-policy-disabled.yaml" \
  "$TMP/external-otel-workload-env.yaml.out" \
  "$TMP/secret-backed-extra-exporter.yaml.out" \
  "$TMP/internal-runner-otel-override.yaml.out" \
  "$TMP/chart-owned-external.yaml.out" \
  "$TMP/chart-owned-external-literal-headers.yaml.out" \
  "$TMP/explicit-disable.yaml.out" \
  "$TMP/chart-owned-external-plus-extraenv-override.yaml.out" <<'PY'
import re
import sys

import yaml


def documents(path):
    return [doc for doc in yaml.safe_load_all(open(path)) if doc]


def collector_documents(path):
    docs = documents(path)
    selected = []
    for doc in docs:
        labels = doc.get("metadata", {}).get("labels", {}) or {}
        name = doc.get("metadata", {}).get("name", "")
        if labels.get("app.kubernetes.io/component") == "otel-collector" or "otel-collector" in name:
            selected.append(doc)
    return selected


def one(docs, kind, label):
    matches = [doc for doc in docs if doc.get("kind") == kind]
    assert len(matches) == 1, f"{label}: expected one {kind}, found {len(matches)}"
    return matches[0]


def collector_config(path, label):
    config_map = one(collector_documents(path), "ConfigMap", label)
    raw = config_map.get("data", {}).get("collector-config.yaml")
    assert raw, f"{label}: collector-config.yaml is absent"
    return yaml.safe_load(raw)


def assert_pipeline_graph(config, label, expect_debug):
    pipelines = config.get("service", {}).get("pipelines", {})
    assert set(pipelines) == {"traces", "logs", "metrics"}, (
        f"{label}: expected traces/logs/metrics pipelines, got {sorted(pipelines)}"
    )
    component_sets = {
        key: set(config.get(key, {})) for key in ("receivers", "processors", "exporters")
    }
    for signal, pipeline in pipelines.items():
        for component_type in component_sets:
            for reference in pipeline.get(component_type, []):
                assert reference in component_sets[component_type], (
                    f"{label}: {signal} references undefined {component_type[:-1]} "
                    f"{reference!r}"
                )
        processors = pipeline.get("processors", [])
        assert "memory_limiter" in processors and "batch" in processors, (
            f"{label}: {signal} does not contain memory_limiter and batch: {processors!r}"
        )
        assert processors.index("memory_limiter") < processors.index("batch"), (
            f"{label}: {signal} batches before memory limiting: {processors!r}"
        )
        exporters = pipeline.get("exporters", [])
        assert ("debug" in exporters) is expect_debug, (
            f"{label}: {signal} debug exporter opt-in mismatch: {exporters!r}"
        )

    assert ("debug" in config.get("exporters", {})) is expect_debug, (
        f"{label}: debug exporter declaration opt-in mismatch"
    )
    assert any(name.split("/", 1)[0] == "nop" for name in pipelines["logs"]["exporters"]), (
        f"{label}: production logs need an explicit nop transport default"
    )
    assert any(name.split("/", 1)[0] == "nop" for name in pipelines["metrics"]["exporters"]), (
        f"{label}: production metrics need an explicit nop transport default"
    )

    extensions = config.get("extensions", {})
    for reference in config.get("service", {}).get("extensions", []):
        assert reference in extensions, (
            f"{label}: service.extensions references undefined extension {reference!r}"
        )
    assert "file_storage" in extensions, f"{label}: file_storage is not declared"
    assert "file_storage" in config.get("service", {}).get("extensions", []), (
        f"{label}: file_storage is not enabled through service.extensions"
    )
    directory = extensions["file_storage"].get("directory")
    assert isinstance(directory, str) and directory.startswith("/"), (
        f"{label}: file_storage.directory is not absolute: {directory!r}"
    )

    telemetry_metrics = config.get("service", {}).get("telemetry", {}).get("metrics", {})
    assert telemetry_metrics.get("address") == "0.0.0.0:8888", (
        f"{label}: Collector 0.119 self-metrics must listen on 0.0.0.0:8888"
    )
    return directory


def assert_network_exporters(config, label):
    for name, exporter in config.get("exporters", {}).items():
        if name.split("/", 1)[0] not in {"otlp", "otlphttp"}:
            continue
        retry = exporter.get("retry_on_failure", {})
        assert retry.get("enabled") is True, f"{label}: {name} retry is disabled"
        assert retry.get("max_interval"), f"{label}: {name} max_interval is absent"
        assert retry.get("max_elapsed_time") not in (None, "0", "0s"), (
            f"{label}: {name} retry is unbounded"
        )
        queue = exporter.get("sending_queue", {})
        assert queue.get("enabled") is True, f"{label}: {name} queue is disabled"
        assert queue.get("storage") == "file_storage", (
            f"{label}: {name} queue is not persistent"
        )
        size = queue.get("queue_size")
        assert isinstance(size, int) and 0 < size <= 100_000, (
            f"{label}: {name} queue_size is not a finite positive bound: {size!r}"
        )


def quantity_is_finite(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[1-9][0-9]*(?:Mi|Gi)", value))


(
    default_path,
    dev_path,
    ephemeral_path,
    storage_path,
    external_path,
    extra_path,
    workload_wiring_path,
    external_workload_wiring_path,
    metrics_ingress_path,
    custom_metrics_port_path,
    network_policy_disabled_path,
    external_otel_workload_env_path,
    secret_backed_extra_exporter_path,
    internal_runner_otel_override_path,
    chart_owned_external_path,
    chart_owned_external_literal_headers_path,
    explicit_disable_path,
    chart_owned_external_plus_extraenv_override_path,
) = sys.argv[1:]


def workload_containers(path):
    """Return the api/dispatcher/worker Deployments and the runner SandboxTemplate container, by role.

    Not the authoritative instrumented set: the mail adapter also carries the
    shared OTLP env and is pinned in ci/mail-adapter-wiring-assertions.sh. It
    cannot appear here because these renders never set mailAdapter.deploy.
    """
    selected = {}
    for doc in documents(path):
        kind = doc.get("kind")
        component = (doc.get("metadata", {}).get("labels", {}) or {}).get(
            "app.kubernetes.io/component"
        )
        if kind == "Deployment" and component in {"api", "dispatcher", "worker"}:
            pod = doc.get("spec", {}).get("template", {}).get("spec", {})
            expected_name = component
        elif kind == "SandboxTemplate" and component == "agent-sandbox":
            pod = doc.get("spec", {}).get("podTemplate", {}).get("spec", {})
            expected_name = "runner"
            component = "runner"
        else:
            continue
        matches = [c for c in pod.get("containers", []) if c.get("name") == expected_name]
        assert len(matches) == 1, (
            f"{path}: expected one {expected_name!r} container for {component}, "
            f"found {[c.get('name') for c in pod.get('containers', [])]}"
        )
        selected[component] = matches[0]
    assert set(selected) == {"api", "dispatcher", "worker", "runner"}, (
        f"{path}: expected API, dispatcher, worker, and runner workloads; "
        f"found {sorted(selected)}"
    )
    return selected


def environment(container):
    return {entry.get("name"): entry.get("value") for entry in container.get("env", [])}


in_chart_endpoint = "http://curie-otel-collector:4318"
for role, container in workload_containers(workload_wiring_path).items():
    env = environment(container)
    assert env.get("OTEL_EXPORTER_OTLP_ENDPOINT") == in_chart_endpoint, (
        f"{role}: chart-owned Collector is enabled but endpoint is "
        f"{env.get('OTEL_EXPORTER_OTLP_ENDPOINT')!r}, expected {in_chart_endpoint!r}"
    )
    assert env.get("OTEL_EXPORTER_OTLP_PROTOCOL") == "http/protobuf", (
        f"{role}: standard OTEL protocol is {env.get('OTEL_EXPORTER_OTLP_PROTOCOL')!r}, "
        "expected 'http/protobuf'"
    )
    for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"):
        assert sum(entry.get("name") == name for entry in container.get("env", [])) == 1, (
            f"{role}: chart-owned {name} must render exactly once"
        )

runner_override = workload_containers(internal_runner_otel_override_path)["runner"]
runner_override_entries = runner_override.get("env", [])
for name, expected in (
    ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://otel.example.com:4318"),
    ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
):
    matches = [entry for entry in runner_override_entries if entry.get("name") == name]
    assert len(matches) == 1, (
        f"runner: operator override for {name} must render exactly once, got {matches!r}"
    )
    assert matches[0].get("value") == expected, (
        f"runner: operator override for {name} was not preserved: {matches!r}"
    )
assert not any(
    entry.get("name") == "OTEL_EXPORTER_OTLP_ENDPOINT"
    and entry.get("value") == in_chart_endpoint
    for entry in runner_override_entries
), "runner: chart synthesized its endpoint despite an operator override"

for role, container in workload_containers(external_workload_wiring_path).items():
    env = environment(container)
    for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"):
        assert name not in env, (
            f"{role}: otelCollector.deploy=false injected {name}={env[name]!r}; "
            "no in-chart endpoint exists and no-endpoint mode must remain inert"
        )

external_endpoint = "https://otel.example.com:4318"
external_headers_ref = {
    "secretKeyRef": {"name": "acme-otel-auth", "key": "headers"}
}
for role, container in workload_containers(external_otel_workload_env_path).items():
    entries = container.get("env", [])
    otel_entries = {
        name: [entry for entry in entries if entry.get("name") == name]
        for name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_HEADERS",
        )
    }
    for name, matches in otel_entries.items():
        assert len(matches) == 1, (
            f"{role}: external Collector override must render {name} exactly once, "
            f"got {matches!r}"
        )
    assert otel_entries["OTEL_EXPORTER_OTLP_ENDPOINT"][0].get("value") == external_endpoint, (
        f"{role}: external OTEL endpoint was not preserved verbatim"
    )
    assert otel_entries["OTEL_EXPORTER_OTLP_PROTOCOL"][0].get("value") == "http/protobuf", (
        f"{role}: external OTEL protocol was not preserved verbatim"
    )
    assert otel_entries["OTEL_EXPORTER_OTLP_HEADERS"][0].get("valueFrom") == external_headers_ref, (
        f"{role}: external OTEL headers valueFrom was dropped or rewritten"
    )
    assert not any(
        "curie-otel-collector" in str(entry.get("value", "")) for entry in entries
    ), f"{role}: otelCollector.deploy=false synthesized the absent internal endpoint"

chart_owned_endpoint = "https://otel.example.com:4318"
chart_owned_headers_ref = {
    "secretKeyRef": {"name": "acme-otel-auth", "key": "headers"}
}
for role, container in workload_containers(chart_owned_external_path).items():
    entries = container.get("env", [])
    otel_entries = {
        name: [entry for entry in entries if entry.get("name") == name]
        for name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_HEADERS",
        )
    }
    for name, matches in otel_entries.items():
        assert len(matches) == 1, (
            f"{role}: chart-owned external Collector must render {name} exactly once, "
            f"got {matches!r}"
        )
    assert otel_entries["OTEL_EXPORTER_OTLP_ENDPOINT"][0].get("value") == chart_owned_endpoint, (
        f"{role}: chart-owned external endpoint was {otel_entries['OTEL_EXPORTER_OTLP_ENDPOINT'][0].get('value')!r}"
    )
    assert otel_entries["OTEL_EXPORTER_OTLP_PROTOCOL"][0].get("value") == "http/protobuf", (
        f"{role}: chart-owned external protocol was {otel_entries['OTEL_EXPORTER_OTLP_PROTOCOL'][0].get('value')!r}"
    )
    assert otel_entries["OTEL_EXPORTER_OTLP_HEADERS"][0].get("valueFrom") == chart_owned_headers_ref, (
        f"{role}: chart-owned headersExistingSecret was dropped or rewritten: "
        f"{otel_entries['OTEL_EXPORTER_OTLP_HEADERS'][0]!r}"
    )
    assert not any(
        "curie-otel-collector" in str(entry.get("value", "")) for entry in entries
    ), f"{role}: chart-owned external mode synthesized the absent internal endpoint"
assert not collector_documents(chart_owned_external_path), (
    "chart-owned external: otelCollector.deploy=false rendered chart-owned Collector resources"
)

for role, container in workload_containers(chart_owned_external_literal_headers_path).items():
    entries = container.get("env", [])
    header_matches = [entry for entry in entries if entry.get("name") == "OTEL_EXPORTER_OTLP_HEADERS"]
    assert header_matches == [
        {"name": "OTEL_EXPORTER_OTLP_HEADERS", "value": "x-scope-orgid=acme"}
    ], f"{role}: chart-owned literal headers were {header_matches!r}"

for role, container in workload_containers(explicit_disable_path).items():
    env = environment(container)
    for name in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS",
    ):
        assert name not in env, (
            f"{role}: explicit telemetryDisabled still injected {name}={env[name]!r}"
        )
assert not collector_documents(explicit_disable_path), (
    "explicit disable: otelCollector.deploy=false rendered chart-owned Collector resources"
)

override_workloads = workload_containers(chart_owned_external_plus_extraenv_override_path)
for role, container in override_workloads.items():
    env = environment(container)
    if role == "runner":
        assert env.get("OTEL_EXPORTER_OTLP_ENDPOINT") == "https://otel-override.example.com:4318", (
            f"runner: extraEnv override lost to chart-owned endpoint {env.get('OTEL_EXPORTER_OTLP_ENDPOINT')!r}"
        )
        assert env.get("OTEL_EXPORTER_OTLP_PROTOCOL") == "grpc", (
            f"runner: extraEnv protocol override lost: {env.get('OTEL_EXPORTER_OTLP_PROTOCOL')!r}"
        )
        assert sum(
            entry.get("name") == "OTEL_EXPORTER_OTLP_ENDPOINT"
            for entry in container.get("env", [])
        ) == 1, "runner: chart-owned endpoint must not duplicate an extraEnv override"
    else:
        assert env.get("OTEL_EXPORTER_OTLP_ENDPOINT") == chart_owned_endpoint, (
            f"{role}: chart-owned external endpoint was {env.get('OTEL_EXPORTER_OTLP_ENDPOINT')!r}"
        )
        assert env.get("OTEL_EXPORTER_OTLP_PROTOCOL") == "http/protobuf", (
            f"{role}: chart-owned external protocol was {env.get('OTEL_EXPORTER_OTLP_PROTOCOL')!r}"
        )

default_docs = collector_documents(default_path)
default_config = collector_config(default_path, "default")
storage_directory = assert_pipeline_graph(default_config, "default", expect_debug=False)
assert_network_exporters(default_config, "default")

dev_config = collector_config(dev_path, "values-dev")
assert_pipeline_graph(dev_config, "values-dev", expect_debug=True)
assert_network_exporters(dev_config, "values-dev")

extra_config = collector_config(extra_path, "safe extra exporter")
assert_pipeline_graph(extra_config, "safe extra exporter", expect_debug=False)
assert_network_exporters(extra_config, "safe extra exporter")
for signal in ("traces", "logs", "metrics"):
    exporters = extra_config["service"]["pipelines"][signal]["exporters"]
    assert exporters.count("otlphttp/acme-sink") == 1, (
        f"safe extra exporter: {signal} must include otlphttp/acme-sink exactly once, "
        f"got {exporters!r}"
    )

secret_config = collector_config(
    secret_backed_extra_exporter_path, "secret-backed extra exporter"
)
secret_exporter = secret_config["exporters"]["otlphttp/acme-secure-sink"]
assert secret_exporter.get("headers", {}).get("Authorization") == "${env:BACKEND_AUTH}", (
    "secret-backed extra exporter: Collector config must retain env indirection"
)
secret_docs = collector_documents(secret_backed_extra_exporter_path)
secret_deployment = one(secret_docs, "Deployment", "secret-backed extra exporter")
secret_container = next(
    container
    for container in secret_deployment["spec"]["template"]["spec"]["containers"]
    if container.get("name") == "otel-collector"
)
backend_auth_entries = [
    entry
    for entry in secret_container.get("env", [])
    if entry.get("name") == "BACKEND_AUTH"
]
assert backend_auth_entries == [
    {
        "name": "BACKEND_AUTH",
        "valueFrom": {
            "secretKeyRef": {
                "name": "acme-otel-backend",
                "key": "authorization",
            }
        },
    }
], (
    "secret-backed extra exporter: full EnvVar valueFrom must render only on the Collector Pod"
)
secret_ref_locations = [
    (doc.get("kind"), doc.get("metadata", {}).get("name"))
    for doc in documents(secret_backed_extra_exporter_path)
    if "acme-otel-backend" in yaml.safe_dump(doc)
]
assert secret_ref_locations == [("Deployment", "curie-otel-collector")], (
    "secret-backed extra exporter: backend Secret reference escaped the Collector Pod: "
    f"{secret_ref_locations!r}"
)

deployment = one(default_docs, "Deployment", "default")
service = one(default_docs, "Service", "default")
pvc = one(default_docs, "PersistentVolumeClaim", "default")
pod_spec = deployment["spec"]["template"]["spec"]
assert deployment["spec"].get("strategy", {}).get("type") == "Recreate", (
    "default: durable single-writer Collector must use Recreate"
)
grace = pod_spec.get("terminationGracePeriodSeconds")
assert isinstance(grace, int) and 0 < grace <= 300, (
    f"default: terminationGracePeriodSeconds must be finite, got {grace!r}"
)
fs_group = pod_spec.get("securityContext", {}).get("fsGroup")
assert isinstance(fs_group, int) and fs_group > 0, f"default: positive fsGroup required, got {fs_group!r}"

container = next(c for c in pod_spec["containers"] if c["name"] == "otel-collector")
assert container.get("image") == "otel/opentelemetry-collector-contrib:0.119.0", (
    "default: Collector image changed without updating the pinned self-metric contract"
)
assert container.get("securityContext", {}).get("runAsNonRoot") is True
mounts = [m for m in container.get("volumeMounts", []) if m.get("mountPath") == storage_directory]
assert len(mounts) == 1 and mounts[0].get("readOnly") is not True, (
    f"default: file_storage path {storage_directory!r} must have one writable mount"
)
storage_volume_name = mounts[0]["name"]
volume = next(v for v in pod_spec.get("volumes", []) if v["name"] == storage_volume_name)
assert volume.get("persistentVolumeClaim", {}).get("claimName") == pvc["metadata"]["name"], (
    "default: storage volume does not use the rendered PVC"
)
request = pvc.get("spec", {}).get("resources", {}).get("requests", {}).get("storage")
assert quantity_is_finite(request), f"default: PVC request must be a finite Mi/Gi bound, got {request!r}"
assert pvc.get("spec", {}).get("accessModes") == ["ReadWriteOnce"]

service_ports = {p.get("name"): p for p in service.get("spec", {}).get("ports", [])}
container_ports = {p.get("name"): p for p in container.get("ports", [])}
assert service_ports.get("metrics", {}).get("port") == 8888
assert service_ports["metrics"].get("targetPort") in (8888, "metrics")
assert container_ports.get("metrics", {}).get("containerPort") == 8888


def collector_network_policy(path, label):
    policies = [
        doc for doc in collector_documents(path) if doc.get("kind") == "NetworkPolicy"
    ]
    assert len(policies) == 1, (
        f"{label}: expected one Collector NetworkPolicy, found "
        f"{[(doc.get('metadata', {}).get('name'), doc.get('spec', {})) for doc in policies]}"
    )
    return policies[0]


def ingress_ports(rule):
    return {
        (port.get("protocol", "TCP"), port.get("port"))
        for port in rule.get("ports", [])
    }


def assert_collector_policy(path, label, metrics_port, metrics_peer=None):
    policy = collector_network_policy(path, label)
    expected_selector = {
        "app.kubernetes.io/name": "curie",
        "app.kubernetes.io/instance": "curie",
        "app.kubernetes.io/component": "otel-collector",
    }
    selector = policy.get("spec", {}).get("podSelector", {}).get("matchLabels")
    assert selector == expected_selector, (
        f"{label}: policy must select the exact Collector workload identity, got {selector!r}"
    )
    assert policy.get("spec", {}).get("policyTypes") == ["Ingress"], (
        f"{label}: Collector policy must isolate ingress only"
    )
    rules = policy.get("spec", {}).get("ingress", [])
    for otlp_port in (4317, 4318):
        unrestricted = [
            rule
            for rule in rules
            if ("TCP", otlp_port) in ingress_ports(rule) and "from" not in rule
        ]
        assert unrestricted, (
            f"{label}: OTLP {otlp_port} must remain reachable from unrestricted senders"
        )

    health_rules = [
        rule for rule in rules if ("TCP", 13133) in ingress_ports(rule)
    ]
    assert health_rules and all("from" not in rule for rule in health_rules), (
        f"{label}: health_check port 13133 must be admitted independently of "
        f"the scoped self-metrics peer, got {health_rules!r}"
    )

    metrics_rules = [
        rule for rule in rules if ("TCP", metrics_port) in ingress_ports(rule)
    ]
    if metrics_peer is None:
        assert not metrics_rules, (
            f"{label}: empty metricsIngress must not open self-metrics port {metrics_port}"
        )
    else:
        assert len(metrics_rules) == 1, (
            f"{label}: expected one scoped self-metrics rule, got {metrics_rules!r}"
        )
        assert metrics_rules[0].get("from") == [metrics_peer], (
            f"{label}: self-metrics rule widened or changed its configured peer: "
            f"{metrics_rules[0].get('from')!r}"
        )
        for rule in rules:
            if "from" in rule:
                assert ingress_ports(rule) == {("TCP", metrics_port)}, (
                    f"{label}: configured metrics peer was also granted non-metrics ports: {rule!r}"
                )


metrics_peer = {
    "namespaceSelector": {
        "matchLabels": {"kubernetes.io/metadata.name": "observability"}
    },
    "podSelector": {"matchLabels": {"app.kubernetes.io/name": "prometheus"}},
}
assert_collector_policy(default_path, "default", 8888)
assert_collector_policy(metrics_ingress_path, "configured metrics ingress", 8888, metrics_peer)
assert_collector_policy(custom_metrics_port_path, "custom metrics port", 9999, metrics_peer)

custom_docs = collector_documents(custom_metrics_port_path)
custom_service = one(custom_docs, "Service", "custom metrics port")
custom_deployment = one(custom_docs, "Deployment", "custom metrics port")
assert {
    port.get("name"): port.get("port")
    for port in custom_service.get("spec", {}).get("ports", [])
}.get("metrics") == 9999, "custom metrics port: Service and NetworkPolicy diverged"
custom_container = next(
    container
    for container in custom_deployment["spec"]["template"]["spec"]["containers"]
    if container.get("name") == "otel-collector"
)
assert {
    port.get("name"): port.get("containerPort")
    for port in custom_container.get("ports", [])
}.get("metrics") == 9999, "custom metrics port: container and NetworkPolicy diverged"

assert not [
    doc
    for doc in collector_documents(network_policy_disabled_path)
    if doc.get("kind") == "NetworkPolicy"
], "security.otelCollectorNetworkPolicy.enabled=false still rendered a policy"

storage_class_pvc = one(collector_documents(storage_path), "PersistentVolumeClaim", "storage class")
assert storage_class_pvc["spec"].get("storageClassName") == "acme-storage"

ephemeral_docs = collector_documents(ephemeral_path)
assert not [doc for doc in ephemeral_docs if doc.get("kind") == "PersistentVolumeClaim"], (
    "ephemeral: persistence.enabled=false still rendered a PVC"
)
ephemeral_deployment = one(ephemeral_docs, "Deployment", "ephemeral")
ephemeral_pod = ephemeral_deployment["spec"]["template"]["spec"]
ephemeral_container = next(
    c for c in ephemeral_pod["containers"] if c["name"] == "otel-collector"
)
ephemeral_mount = next(
    m for m in ephemeral_container.get("volumeMounts", []) if m.get("mountPath") == storage_directory
)
ephemeral_volume = next(v for v in ephemeral_pod["volumes"] if v["name"] == ephemeral_mount["name"])
size_limit = ephemeral_volume.get("emptyDir", {}).get("sizeLimit")
assert quantity_is_finite(size_limit), (
    f"ephemeral: emptyDir must retain a finite sizeLimit, got {size_limit!r}"
)

external_docs = collector_documents(external_path)
assert not external_docs, (
    "external collector: otelCollector.deploy=false rendered chart-owned Collector resources: "
    f"{[(doc.get('kind'), doc.get('metadata', {}).get('name')) for doc in external_docs]}"
)
for doc in documents(external_path):
    pod_specs = []
    if doc.get("kind") == "SandboxTemplate":
        pod_specs.append(doc.get("spec", {}).get("podTemplate", {}).get("spec", {}))
    for pod in pod_specs:
        for container in pod.get("containers", []) + pod.get("initContainers", []):
            names = {entry.get("name") for entry in container.get("env", [])}
            assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in names, (
                "external collector: chart injected its absent in-chart endpoint into the sandbox"
            )

print("PASS: durable three-signal Collector render contract")
PY
