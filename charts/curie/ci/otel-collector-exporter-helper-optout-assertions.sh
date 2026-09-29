#!/usr/bin/env bash
# @spec charts/curie/README.md: Collector exporters without exporterhelper.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

cat > "$TMP/awsemf.yaml" <<'YAML'
otelCollector:
  extraExporters:
    awsemf/curie:
      region: us-east-1
      namespace: Curie/Platform
      log_group_name: /aws/eks/test/metrics
      max_retries: 10
  extraMetricPipelineExporters: [awsemf/curie]
  exportersWithoutExporterHelper:
    awsemf/curie: >-
      awsemf carries max_retries and rejects retry_on_failure and sending_queue;
      checked against contrib 0.119.0.
YAML

if ! helm template curie "$CHART" -f "$TMP/awsemf.yaml" > "$TMP/rendered.yaml" 2> "$TMP/error"; then
  fail "declared awsemf exporter refused to render: $(cat "$TMP/error")"
fi
grep -q 'awsemf/curie' "$TMP/rendered.yaml" || fail "awsemf is absent from the rendered collector"
if grep -A12 'awsemf/curie:' "$TMP/rendered.yaml" | grep -qE 'retry_on_failure|sending_queue'; then
  fail "awsemf received exporterhelper fields its schema rejects"
fi

cat > "$TMP/undeclared.yaml" <<'YAML'
otelCollector:
  extraExporters:
    awsemf/curie:
      region: us-east-1
      log_group_name: /aws/eks/test/metrics
  extraMetricPipelineExporters: [awsemf/curie]
YAML
if helm template curie "$CHART" -f "$TMP/undeclared.yaml" >/dev/null 2>&1; then
  fail "network exporter without helper durability or exemption rendered"
fi

cat > "$TMP/stale.yaml" <<'YAML'
otelCollector:
  exportersWithoutExporterHelper:
    awsemf/gone: "awsemf/curie under contrib 0.119.0 was removed"
YAML
if helm template curie "$CHART" -f "$TMP/stale.yaml" > "$TMP/stale-rendered" 2> "$TMP/stale-error"; then
  fail "exemption without a configured exporter rendered"
fi
grep -q 'names no configured exporter' "$TMP/stale-error" || fail "stale exemption failed for an unrelated reason"

cat > "$TMP/no-reason.yaml" <<'YAML'
otelCollector:
  extraExporters:
    awsemf/curie:
      region: us-east-1
      log_group_name: /aws/eks/test/metrics
  extraMetricPipelineExporters: [awsemf/curie]
  exportersWithoutExporterHelper:
    awsemf/curie: ""
YAML
if helm template curie "$CHART" -f "$TMP/no-reason.yaml" > "$TMP/no-reason-rendered" 2> "$TMP/no-reason-error"; then
  fail "exemption without a reason rendered"
fi
grep -q 'must give a reason' "$TMP/no-reason-error" || fail "empty reason failed for an unrelated reason"

echo 'PASS: collector exporterhelper exemption is scoped and rendered'
