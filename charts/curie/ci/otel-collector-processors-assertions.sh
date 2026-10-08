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

render_config > "$TMP/default.config"
grep -Fxq '      processors: [memory_limiter, batch]' "$TMP/default.config" || fail 'default trace/log processor order changed'
[[ $(grep -Fxc '      processors: [memory_limiter, batch]' "$TMP/default.config") -eq 2 ]] || fail 'default trace/log pipelines changed'
grep -Fxq '      processors: [memory_limiter, transform/runner_identity, batch]' "$TMP/default.config" || fail 'default metrics processor order changed'

# @spec charts/curie/README.md: Optional trace processors. A retained release
# supplies its own values.yaml on --reuse-values, without the new keys.
cp -R "$CHART" "$TMP/reuse-chart"
sed '/^  extraProcessors: {}$/d; /^  extraTracePipelineProcessors: \[\]$/d' \
  "$CHART/values.yaml" > "$TMP/reuse-chart/values.yaml"
helm template curie "$TMP/reuse-chart" --is-upgrade -s templates/otel-collector.yaml \
  | awk '/^  collector-config.yaml: \|$/ { config=1; next } config && /^    / { sub(/^    /, ""); print; next } config { exit }' \
  > "$TMP/reuse.config"
grep -Fxq '      processors: [memory_limiter, batch]' "$TMP/reuse.config" || fail 'retained release without processor keys changed traces'
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
grep -Fxq '      processors: [memory_limiter, filter/routine_spans, attributes/annotate, batch]' "$TMP/selected.config" || fail 'selected trace order wrong'
grep -Fxq '      processors: [memory_limiter, batch]' "$TMP/selected.config" || fail 'logs processor order changed'
grep -Fxq '      processors: [memory_limiter, transform/runner_identity, batch]' "$TMP/selected.config" || fail 'metrics processor order changed'

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
