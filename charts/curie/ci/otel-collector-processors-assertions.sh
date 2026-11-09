#!/usr/bin/env bash
# @spec charts/curie/README.md: Optional trace processors.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

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
    'name == "http.server.request"',
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

# Collector validate cannot bind-mount a checkout on the remote engine. Copy
# the file into a short-lived container and remove it. One container at a time.
validate_one() {
  local file="$1" label="$2" name="n5496-otel-validate"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker create --name "$name" otel/opentelemetry-collector-contrib:0.119.0 \
    validate --config=/c.yaml >/dev/null
  docker cp "$file" "$name:/c.yaml"
  if ! docker start -a "$name"; then
    docker rm -f "$name" >/dev/null 2>&1 || true
    fail "$label collector config failed validation"
  fi
  docker rm -f "$name" >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "validate-collector" ]]; then
  render_config > "$TMP/chart-collector.yaml"
  validate_one "$TMP/chart-collector.yaml" chart
  validate_one "$SCRIPT_DIR/../../../otel/collector-config.yaml" compose
  echo 'collector configs validated'
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
