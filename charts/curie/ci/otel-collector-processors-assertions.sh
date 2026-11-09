#!/usr/bin/env bash
# @spec charts/curie/README.md: Optional trace processors.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
COLLECTOR_CONTAINER=""
cleanup() {
  if [[ -n "$COLLECTOR_CONTAINER" ]]; then
    docker rm -f "$COLLECTOR_CONTAINER" >/dev/null
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render_config() {
  helm template curie "$CHART" -s templates/otel-collector.yaml "$@" \
    | awk '/^  collector-config.yaml: \|$/ { config=1; next } config && /^    / { sub(/^    /, ""); print; next } config { exit }'
}

refuses() {
  local name="$1" pattern="$2"
  if render_config -f "$TMP/$name.yaml" > "$TMP/$name.out" 2>&1; then
    fail "$name rendered successfully"
  fi
  grep -Fq "$pattern" "$TMP/$name.out" || fail "$name failed without expected reason: $(cat "$TMP/$name.out")"
}

# Langfuse-only trace contract (#5496). The unfiltered traces pipeline is
# absent unless debug or an extra trace exporter is configured.
assert_langfuse_pipeline() {
  local file="$1" label="$2"
  shift 2
  python3 - "$file" "$label" "$@" <<'PY'
import sys
import yaml

path, label, *extra = sys.argv[1:]
config = yaml.safe_load(open(path))
pipelines = config["service"]["pipelines"]
langfuse = pipelines.get("traces/langfuse")
if langfuse is None:
    sys.exit(f"{label}: traces/langfuse pipeline missing")
processors = langfuse.get("processors")
expected = ["memory_limiter", *extra, "filter/langfuse_platform_noise", "batch"]
if processors != expected:
    sys.exit(f"{label}: traces/langfuse processors {processors!r} != {expected!r}")
if langfuse.get("exporters") != ["otlphttp/langfuse"]:
    sys.exit(f"{label}: traces/langfuse exporters {langfuse.get('exporters')!r}")
if langfuse.get("receivers") != ["otlp"]:
    sys.exit(f"{label}: traces/langfuse receivers {langfuse.get('receivers')!r}")
filt = config.get("processors", {}).get("filter/langfuse_platform_noise")
expected_spans = [
    (
        'name == "http.server.request" and '
        'attributes["service.name"] == "curie-api" and '
        'attributes["source"] == "GET" and '
        '(attributes["operation"] == "/health" or '
        'attributes["operation"] == "/ready" or '
        'attributes["operation"] == "/agents" or '
        'attributes["operation"] == "/agents/{agent_id}" or '
        'attributes["operation"] == "/approvals" or '
        'attributes["operation"] == "/approvals/{approval_id}" or '
        'attributes["operation"] == "/approvals/{approval_id}/audit" or '
        'attributes["operation"] == "/langfuse/traces" or '
        'attributes["operation"] == "/langfuse/traces/{trace_id}" or '
        'attributes["operation"] == "/observability/metrics/summary" or '
        'attributes["operation"] == "/observability/metrics/series" or '
        'attributes["operation"] == "/observability/runners")'
    ),
    'IsMatch(name, "^curie\\\\.background\\\\.")',
    'name == "curie.sandbox.cleanup"',
]
if not isinstance(filt, dict) or filt.get("error_mode") != "ignore" or filt.get("traces", {}).get("span") != expected_spans:
    sys.exit(f"{label}: platform noise filter is {filt!r}")
for signal in ("logs", "metrics"):
    if "filter/langfuse_platform_noise" in pipelines[signal].get("processors", []):
        sys.exit(f"{label}: {signal} pipeline runs the langfuse noise filter")
PY
}

assert_unfiltered_traces() {
  local file="$1" label="$2" exporters="$3"
  shift 3
  python3 - "$file" "$label" "$exporters" "$@" <<'PY'
import sys
import yaml

path, label, exporters, *extra = sys.argv[1:]
config = yaml.safe_load(open(path))
pipeline = config["service"]["pipelines"].get("traces")
if pipeline is None:
    sys.exit(f"{label}: unfiltered traces pipeline missing")
expected_processors = ["memory_limiter", *extra, "batch"]
if pipeline.get("processors") != expected_processors:
    sys.exit(
        f"{label}: traces processors {pipeline.get('processors')!r} != {expected_processors!r}"
    )
if "filter/langfuse_platform_noise" in pipeline.get("processors", []):
    sys.exit(f"{label}: unfiltered traces pipeline drops platform noise")
expected_exporters = exporters.split(",") if exporters else []
if pipeline.get("exporters") != expected_exporters:
    sys.exit(f"{label}: traces exporters {pipeline.get('exporters')!r} != {expected_exporters!r}")
if "otlphttp/langfuse" in pipeline.get("exporters", []):
    sys.exit(f"{label}: unfiltered traces pipeline exports to Langfuse")
PY
}

assert_no_unfiltered_traces() {
  local file="$1" label="$2"
  python3 - "$file" "$label" <<'PY'
import sys
import yaml

path, label = sys.argv[1:]
pipelines = yaml.safe_load(open(path))["service"]["pipelines"]
if "traces" in pipelines:
    sys.exit(f"{label}: unfiltered traces pipeline rendered")
PY
}

validate_one() {
  local file="$1" label="$2"
  COLLECTOR_CONTAINER="$(docker create otel/opentelemetry-collector-contrib:0.119.0 \
    validate --config=/c.yaml)"
  docker cp "$file" "$COLLECTOR_CONTAINER:/c.yaml"
  if ! docker start -a "$COLLECTOR_CONTAINER"; then
    fail "$label collector config failed validation"
  fi
  docker rm "$COLLECTOR_CONTAINER" >/dev/null
  COLLECTOR_CONTAINER=""
}

assert_filter_behavior() {
  local file="$1" label="$2" port
  python3 - "$file" "$TMP" <<'PYTHON'
import json
import pathlib
import sys
import time

import yaml

source = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2])
config = {
    "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
    "processors": {"filter/langfuse_platform_noise": source["processors"]["filter/langfuse_platform_noise"]},
    "exporters": {"file": {"path": "/output/retained.json"}},
    "service": {"pipelines": {"traces": {
        "receivers": ["otlp"], "processors": ["filter/langfuse_platform_noise"], "exporters": ["file"],
    }}},
}
(root / "behavior.yaml").write_text(yaml.safe_dump(config))
cases = [
    ("http.server.request", "GET", "/health", False),
    ("http.server.request", "GET", "/ready", False),
    ("http.server.request", "GET", "/approvals", False),
    ("http.server.request", "GET", "/langfuse/traces", False),
    ("http.server.request", "POST", "/channels/turns", True),
    ("curie.queue.enqueue", "POST", "/channels/turns", True),
    ("http.server.request", "POST", "/approvals/{approval_id}/resolve", True),
    ("http.server.request", "POST", "/hooks/{agent_id}/{hook}", True),
    ("http.server.request", "POST", "/github/webhook", True),
    ("http.server.request", "POST", "/agents/{agent_id}/hooks/{name}/fire", True),
    ("http.server.request", "GET", "/unknown", True),
    ("http.server.request", "POST", "/health", True),
    ("http.server.request", "GET", None, True),
    ("curie.background.sweep", "", "", False),
    ("curie.sandbox.cleanup", "", "", False),
    ("curie.runner.execute", "", "", True),
]
spans, expected = [], []
now = time.time_ns()
for index, (name, method, route, keep) in enumerate(cases, 1):
    span_id = f"{index:016x}"
    attributes = {"service.name": "curie-api", "source": method}
    if route is not None:
        attributes["operation"] = route
    span = {
        "traceId": f"{1 if index == 6 else index:032x}", "spanId": span_id,
        "name": name, "kind": 2, "startTimeUnixNano": str(now),
        "endTimeUnixNano": str(now + 1000),
        "attributes": [{"key": key, "value": {"stringValue": value}} for key, value in attributes.items()],
    }
    if index == 6:
        span["traceId"] = f"{5:032x}"
        span["parentSpanId"] = f"{5:016x}"
    spans.append(span)
    if keep:
        expected.append(span_id)
(root / "spans.json").write_text(json.dumps({"resourceSpans": [{"scopeSpans": [{"spans": spans}]}]}))
(root / "expected.json").write_text(json.dumps(expected))
PYTHON
  COLLECTOR_CONTAINER="$(docker create -p 4318 otel/opentelemetry-collector-contrib:0.119.0 --config=/c.yaml)"
  mkdir -p "$TMP/output"
  chmod 777 "$TMP/output"
  docker cp "$TMP/output" "$COLLECTOR_CONTAINER:/output"
  docker cp "$TMP/behavior.yaml" "$COLLECTOR_CONTAINER:/c.yaml"
  docker start "$COLLECTOR_CONTAINER" >/dev/null
  if ! port="$(docker port "$COLLECTOR_CONTAINER" 4318/tcp | head -1 | sed 's/.*://')"; then
    docker logs "$COLLECTOR_CONTAINER"
    fail "$label collector did not publish its OTLP port"
  fi
  python3 - "$port" "$TMP/spans.json" <<'PYTHON'
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

request = urllib.request.Request(
    f"http://127.0.0.1:{sys.argv[1]}/v1/traces",
    data=pathlib.Path(sys.argv[2]).read_bytes(), headers={"Content-Type": "application/json"},
)
deadline = time.monotonic() + 30
while True:
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            assert response.status == 200
            result = response.read()
            assert not json.loads(result).get("partialSuccess"), result
        break
    except urllib.error.URLError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.2)
PYTHON
  docker stop "$COLLECTOR_CONTAINER" >/dev/null
  docker cp "$COLLECTOR_CONTAINER:/output/retained.json" "$TMP/retained.json"
  docker rm "$COLLECTOR_CONTAINER" >/dev/null
  COLLECTOR_CONTAINER=""
  python3 - "$TMP" "$label" <<'PYTHON'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
spans = [
    span
    for line in (root / "retained.json").read_text().splitlines()
    for resource in json.loads(line)["resourceSpans"]
    for scope in resource["scopeSpans"]
    for span in scope["spans"]
]
actual = {span["spanId"] for span in spans}
expected = set(json.loads((root / "expected.json").read_text()))
assert actual == expected, (sys.argv[2], actual, expected)
queue = next(span for span in spans if span["name"] == "curie.queue.enqueue")
assert queue["parentSpanId"] in actual
print(f"{sys.argv[2]}: probes and polling dropped; ingress root and queue child, approvals, hooks, webhook and unknown routes retained")
PYTHON
}

if [[ "${1:-}" == "validate-collector" ]]; then
  render_config > "$TMP/chart-collector.yaml"
  validate_one "$TMP/chart-collector.yaml" chart
  validate_one "$SCRIPT_DIR/../../../otel/collector-config.yaml" compose
  assert_filter_behavior "$TMP/chart-collector.yaml" chart
  assert_filter_behavior "$SCRIPT_DIR/../../../otel/collector-config.yaml" compose
  echo 'collector configs and filter behavior validated'
  exit 0
fi

render_config > "$TMP/default.config"
assert_langfuse_pipeline "$TMP/default.config" default
assert_no_unfiltered_traces "$TMP/default.config" default
grep -Fxq '      processors: [memory_limiter, batch]' "$TMP/default.config" || fail 'default log processor order changed'
[[ $(grep -Fxc '      processors: [memory_limiter, batch]' "$TMP/default.config") -eq 1 ]] || fail 'default log pipeline no longer the only memory_limiter, batch list'
grep -Fxq '      processors: [memory_limiter, transform/runner_identity, batch]' "$TMP/default.config" || fail 'default metrics processor order changed'

# @spec charts/curie/README.md: Optional trace processors. A retained release
# supplies its own values.yaml on --reuse-values, without the new keys.
cp -R "$CHART" "$TMP/reuse-chart"
sed '/^  extraProcessors: {}$/d; /^  extraTracePipelineProcessors: \[\]$/d' \
  "$CHART/values.yaml" > "$TMP/reuse-chart/values.yaml"
helm template curie "$TMP/reuse-chart" --is-upgrade -s templates/otel-collector.yaml \
  | awk '/^  collector-config.yaml: \|$/ { config=1; next } config && /^    / { sub(/^    /, ""); print; next } config { exit }' \
  > "$TMP/reuse.config"
assert_langfuse_pipeline "$TMP/reuse.config" 'retained release'
assert_no_unfiltered_traces "$TMP/reuse.config" 'retained release'
grep -Fxq '      processors: [memory_limiter, transform/runner_identity, batch]' "$TMP/reuse.config" || fail 'retained release without processor keys changed metrics'

cat > "$TMP/selected.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    filter/routine_spans:
      error_mode: ignore
      traces:
        span:
          - 'IsMatch(name, "^(health|background)")'
    attributes/annotate:
      actions:
        - key: test.source
          value: synthetic
          action: insert
  extraTracePipelineProcessors: [filter/routine_spans, attributes/annotate]
YAML
render_config -f "$TMP/selected.yaml" > "$TMP/selected.config"
grep -Fxq '  filter/routine_spans:' "$TMP/selected.config" || fail 'filter definition missing'
grep -Fxq '  attributes/annotate:' "$TMP/selected.config" || fail 'second processor definition missing'
assert_langfuse_pipeline "$TMP/selected.config" 'selected processors' filter/routine_spans attributes/annotate
assert_no_unfiltered_traces "$TMP/selected.config" 'selected processors'
grep -Fxq '      processors: [memory_limiter, batch]' "$TMP/selected.config" || fail 'logs processor order changed'
grep -Fxq '      processors: [memory_limiter, transform/runner_identity, batch]' "$TMP/selected.config" || fail 'metrics processor order changed'

cat > "$TMP/debug.yaml" <<'YAML'
otelCollector:
  debugExporter:
    enabled: true
YAML
render_config -f "$TMP/debug.yaml" > "$TMP/debug.config"
assert_langfuse_pipeline "$TMP/debug.config" 'debug exporter'
assert_unfiltered_traces "$TMP/debug.config" 'debug exporter' debug

cat > "$TMP/extra-trace-exporter.yaml" <<'YAML'
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
YAML
render_config -f "$TMP/extra-trace-exporter.yaml" > "$TMP/extra-trace-exporter.config"
assert_langfuse_pipeline "$TMP/extra-trace-exporter.config" 'extra trace exporter'
assert_unfiltered_traces "$TMP/extra-trace-exporter.config" 'extra trace exporter' otlphttp/acme-sink

cat > "$TMP/debug-and-extra.yaml" <<'YAML'
otelCollector:
  debugExporter:
    enabled: true
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
  extraTracePipelineProcessors: [filter/routine_spans]
  extraProcessors:
    filter/routine_spans:
      error_mode: ignore
      traces:
        span:
          - 'IsMatch(name, "^(health|background)")'
YAML
render_config -f "$TMP/debug-and-extra.yaml" > "$TMP/debug-and-extra.config"
assert_langfuse_pipeline "$TMP/debug-and-extra.config" 'debug and extra' filter/routine_spans
assert_unfiltered_traces "$TMP/debug-and-extra.config" 'debug and extra' 'debug,otlphttp/acme-sink' filter/routine_spans

cat > "$TMP/shadow-noise-filter.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    filter/langfuse_platform_noise:
      error_mode: ignore
YAML
refuses shadow-noise-filter 'must not replace built-in processor "filter/langfuse_platform_noise"'

cat > "$TMP/select-noise-filter.yaml" <<'YAML'
otelCollector:
  extraTracePipelineProcessors: [filter/langfuse_platform_noise]
YAML
refuses select-noise-filter 'must not select built-in processor "filter/langfuse_platform_noise"'

cat > "$TMP/shadow.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    batch: {}
YAML
refuses shadow 'must not replace built-in processor'

cat > "$TMP/unknown.yaml" <<'YAML'
otelCollector:
  extraTracePipelineProcessors: [filter/missing]
YAML
refuses unknown 'references undefined processor'

cat > "$TMP/duplicate.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    filter/example: {}
  extraTracePipelineProcessors: [filter/example, filter/example]
YAML
refuses duplicate 'duplicates processor'

cat > "$TMP/builtin.yaml" <<'YAML'
otelCollector:
  extraTracePipelineProcessors: [batch]
YAML
refuses builtin 'must not select built-in processor'

cat > "$TMP/malformed.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    'filter/example, batch': {}
YAML
refuses malformed 'has invalid component ID'

cat > "$TMP/nonmap.yaml" <<'YAML'
otelCollector:
  extraProcessors:
    filter/example: disabled
YAML
refuses nonmap 'must be a map'

cat > "$TMP/nonmap-top-level.yaml" <<'YAML'
otelCollector:
  extraProcessors: disabled
YAML
refuses nonmap-top-level 'otelCollector.extraProcessors must be a map'

cat > "$TMP/nonlist-top-level.yaml" <<'YAML'
otelCollector:
  extraTracePipelineProcessors: disabled
YAML
refuses nonlist-top-level 'otelCollector.extraTracePipelineProcessors must be a list'

echo 'otel collector processor assertions passed'
