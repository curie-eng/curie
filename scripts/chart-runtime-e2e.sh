#!/usr/bin/env bash
#
# chart-runtime-e2e.sh -- one-command RUNTIME e2e for the Curie Helm chart.
#
# WHY THIS EXISTS
# ---------------
# `helm lint` and `helm template` render manifests but NEVER run a container.
# They cannot catch a bug that only manifests when an init container executes --
# for example issue #56, where the bundle fetch client wrote the S3 credential
# in cleartext to a config file,
# and that dir sat on the `bundles` emptyDir the untrusted `runner` container
# also mounts. The acceptance criterion is a RUNTIME exec:
#   kubectl exec <sandbox> -c runner -- find /bundles -name config.json   # empty
# This harness makes that check one command.
#
# THE PATTERN (reusable -- this is NOT a #56 one-off)
# ---------------------------------------------------
# 1. Install a trimmed chart slice on k8scratch with RustFS and agent sandbox.
# 2. Seed a real bundle object into RustFS.
# 3. Render the SandboxTemplate.spec.podTemplate into a BOUND sandbox Pod
#    (CURIE_BUNDLE_REF pointing at the seeded object) and apply it -- so the
#    real bundle-fetch/bundle-extract init containers actually run.
# 4. Wait for the init pair to complete and the runner container to be Running.
# 5. `kubectl exec` into the runner and ASSERT on what it can see.
# 6. Drive traces, logs, and metrics through the chart Collector into a private
#    task-owned sink; then prove outage queueing, PVC-backed restart recovery,
#    and explicit bounded-queue overflow/loss using Collector 0.119 self-metrics.
#
# HOW TO ADD ANOTHER RUNTIME ASSERTION
# ------------------------------------
# The generic seam is "render template -> bind bundle -> exec runner -> assert".
# To add a new runtime check, add a new `kubectl exec e2e-bound-sandbox -c runner
# -- <cmd>` inside run_assertions() and fold its result into the PASS/FAIL logic.
# Reuse exec_echo() so the command and its raw output are auditable in the log.
#
set -euo pipefail

# --------------------------------------------------------------------------
# Config / flags
# --------------------------------------------------------------------------
NAMESPACE="curie-e2eharness"
RELEASE="e2eharness"
CHART="charts/curie"
KEEP=0
FORCE=0
RUNNER_IMAGE=""
EXPECT_VULNERABLE=0
POD_NAME="e2e-bound-sandbox"
BUNDLE_REF="e2e/probe.tgz"
OTEL_SINK="e2e-otlp-sink"
OTEL_OBSERVER="e2e-otel-observer"
OTEL_UNSELECTED_OBSERVER="e2e-otel-unselected-observer"
OTEL_METRICS_TEST_ALLOW="e2e-otel-metrics-test-allow"
OTEL_STORAGE_OBSERVER="e2e-otel-storage-observer"
OTEL_FIXTURES="e2e-otlp-fixtures"
OTEL_EXPORTER="otlphttp/e2e-sink"
OTEL_QUEUE_SIZE=2
# Queue gauges have one series per traces/logs/metrics pipeline. metric_value
# intentionally sums series, so the signal-wide capacity is three queue bounds.
OTEL_QUEUE_CAPACITY_TOTAL=$((OTEL_QUEUE_SIZE * 3))
OTEL_PVC_SIZE="128Mi"
OTEL_MEMORY_LIMIT="96Mi"

usage() {
  cat <<'EOF'
Usage: scripts/chart-runtime-e2e.sh [options]

Stands up a trimmed Curie chart slice on the k8scratch cluster, seeds a real
bundle into RustFS, and retains the #56 credential-isolation proof. It also
drives all three OTLP signals through the chart Collector into a private sink,
then proves outage queueing, PVC-backed restart recovery, and bounded overflow.

Options:
  --namespace <ns>       Namespace to use (default: curie-e2eharness)
  --release <name>       Helm release name (default: e2eharness)
  --chart <path>         Chart path, relative to repo root (default: charts/curie)
  --runner-image <img>   Override ONLY the runner container image with this image
                         (command: sleep 3600, probes/ports stripped). Robustness
                         fallback when the real runner image will not reach Running
                         on the cluster; the #56 assertion only needs the runner's
                         VIEW of the shared bundle mount. Default: real runner image.
  --expect-vulnerable    Negative-control mode: INVERT the security assertion, so
                         PASS means the credential IS exposed. Point at an unfixed
                         template to prove the harness discriminates.
  --keep                 Skip teardown (leave namespace + release for debugging).
  --force                Allow running against a non-k8scratch kube context.
  --help                 Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --release) RELEASE="$2"; shift 2 ;;
    --chart) CHART="$2"; shift 2 ;;
    --runner-image) RUNNER_IMAGE="$2"; shift 2 ;;
    --expect-vulnerable) EXPECT_VULNERABLE=1; shift ;;
    --keep) KEEP=1; shift ;;
    --force) FORCE=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# Resolve repo root from this script's location so --chart is repo-relative.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
case "$CHART" in
  /*) CHART_PATH="$CHART" ;;
  *) CHART_PATH="$REPO_ROOT/$CHART" ;;
esac

# Derived resource names -- mirror the chart's `curie.fullname` helper exactly
# (nameOverride/fullnameOverride empty): fullname == release if it already
# contains the chart name "curie", else "<release>-curie".
if [[ "$RELEASE" == *curie* ]]; then
  FULLNAME="$RELEASE"
else
  FULLNAME="$RELEASE-curie"
fi
SANDBOX_TEMPLATE="$FULLNAME-runner"
RUSTFS_SVC="$FULLNAME-rustfs"
SECRET_NAME="$FULLNAME-secrets"
OTEL_COLLECTOR_DEPLOYMENT="$FULLNAME-otel-collector"
OTEL_COLLECTOR_SERVICE="$FULLNAME-otel-collector"
RUSTFS_BUCKET="curie-bundles"
RUSTFS_ACCESS_KEY="rustfs"
E2E_TMP="$(mktemp -d /tmp/curie-chart-runtime-e2e.XXXXXX)"
# Ownership label stamped on any namespace THIS script creates. The script only
# ever deletes a namespace carrying this label, so pointing --namespace at a
# pre-existing namespace (e.g. `default`) can never destroy it.
OWNED_LABEL="curie-e2e-harness/owned"

banner() { echo; echo "== $* =="; }
fail() { echo; echo "FAIL: $*"; exit 1; }

# ns_is_owned <ns> : true iff the namespace exists AND carries the ownership label.
ns_is_owned() {
  local ns="$1" val
  val="$(kubectl get ns "$ns" -o "jsonpath={.metadata.labels['curie-e2e-harness/owned']}" 2>/dev/null || echo "")"
  [[ "$val" == "true" ]]
}

# create_owned_ns <ns> : create the namespace and stamp the ownership label on it.
create_owned_ns() {
  local ns="$1"
  kubectl create ns "$ns"
  kubectl label ns "$ns" "${OWNED_LABEL}=true" --overwrite >/dev/null
}

# --------------------------------------------------------------------------
# Guardrails
# --------------------------------------------------------------------------
CURRENT_CTX="$(kubectl config current-context 2>/dev/null || echo "")"
if [[ "$CURRENT_CTX" != "k8scratch" && "$FORCE" -ne 1 ]]; then
  fail "kube context is '$CURRENT_CTX', not 'k8scratch'. Refusing (override with --force)."
fi

# --------------------------------------------------------------------------
# Teardown (trap on EXIT). Leaves the cluster-scoped sandbox* CRDs alone.
# --------------------------------------------------------------------------
teardown() {
  local rc=$?
  rm -rf "$E2E_TMP"
  if [[ "$KEEP" -eq 1 ]]; then
    banner "TEARDOWN skipped (--keep); namespace $NAMESPACE left in place"
    return $rc
  fi
  banner "TEARDOWN"
  helm uninstall "$RELEASE" -n "$NAMESPACE" --no-hooks >/dev/null 2>&1 || true
  # Only ever delete a namespace THIS script created (carries the ownership label).
  if ns_is_owned "$NAMESPACE"; then
    kubectl delete ns "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
  else
    echo "namespace $NAMESPACE not owned by this harness; leaving it in place"
  fi
  return $rc
}
trap teardown EXIT

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

# exec_echo <label> <exec-args...> : run a kubectl exec, echo the command, its
# exit code, and its stdout/stderr SEPARATELY. Captures stdout into the global
# EXEC_OUT, stderr into EXEC_ERR, and the exit code into EXEC_RC for the caller.
# Verdicts MUST key on EXEC_OUT (stdout) only -- folding stderr into the captured
# value can make an empty result look non-empty (e.g. a "Permission denied" line)
# and yield a false PASS.
exec_echo() {
  local label="$1"; shift
  echo "--- $label"
  echo "\$ kubectl exec $POD_NAME -n $NAMESPACE $*"
  local out_file err_file
  out_file="$(mktemp /tmp/e2e-exec-out.XXXXXX)"
  err_file="$(mktemp /tmp/e2e-exec-err.XXXXXX)"
  set +e
  kubectl exec "$POD_NAME" -n "$NAMESPACE" "$@" >"$out_file" 2>"$err_file"
  EXEC_RC=$?
  set -e
  EXEC_OUT="$(cat "$out_file")"
  EXEC_ERR="$(cat "$err_file")"
  rm -f "$out_file" "$err_file"
  echo "[exit $EXEC_RC] stdout:"
  printf '%s\n' "$EXEC_OUT" | sed 's/^/    /'
  if [[ -n "$EXEC_ERR" ]]; then
    echo "stderr:"
    printf '%s\n' "$EXEC_ERR" | sed 's/^/    /'
  fi
}

# Collector 0.119.0 self-metric names consumed below. These are intentionally
# literal: an image bump must update the harness together with the expected
# Prometheus contract instead of silently turning every resilience check into a
# grep for a metric the selected Collector no longer emits.
OTELCOL_RECEIVER_METRICS=(
  otelcol_receiver_accepted_spans
  otelcol_receiver_accepted_log_records
  otelcol_receiver_accepted_metric_points
)
OTELCOL_REFUSED_METRICS=(
  otelcol_receiver_refused_spans
  otelcol_receiver_refused_log_records
  otelcol_receiver_refused_metric_points
)
OTELCOL_SENT_METRICS=(
  otelcol_exporter_sent_spans
  otelcol_exporter_sent_log_records
  otelcol_exporter_sent_metric_points
)
OTELCOL_FAILED_METRICS=(
  otelcol_exporter_send_failed_spans
  otelcol_exporter_send_failed_log_records
  otelcol_exporter_send_failed_metric_points
)
OTELCOL_ENQUEUE_FAILED_METRICS=(
  otelcol_exporter_enqueue_failed_spans
  otelcol_exporter_enqueue_failed_log_records
  otelcol_exporter_enqueue_failed_metric_points
)
OTELCOL_QUEUE_SIZE_METRIC=otelcol_exporter_queue_size
OTELCOL_QUEUE_CAPACITY_METRIC=otelcol_exporter_queue_capacity

# metrics_snapshot <service> : fetch a Collector Prometheus endpoint through
# the task-owned observer Pod and save the complete payload in METRICS_OUT.
metrics_snapshot() {
  local service="$1"
  if ! METRICS_OUT="$(kubectl exec "$OTEL_OBSERVER" -n "$NAMESPACE" -- \
      curl -fsS "http://$service:8888/metrics")"; then
    fail "could not read Collector self-metrics from $service:8888"
  fi
}

# metrics_scrape_from <observer-pod> : require the named observer to reach the
# Collector metrics endpoint and see a Collector-owned metric. Retry across
# NetworkPolicy propagation so a transient policy update cannot false-red.
metrics_scrape_from() {
  local observer="$1" deadline=$((SECONDS + 45)) output=""
  while (( SECONDS < deadline )); do
    if output="$(kubectl exec "$observer" -n "$NAMESPACE" -- \
        curl -fsS --connect-timeout 2 --max-time 5 \
        "http://$OTEL_COLLECTOR_SERVICE:8888/metrics" 2>/dev/null)" && \
        grep -q '^otelcol_process_uptime' <<<"$output"; then
      return 0
    fi
    sleep 2
  done
  fail "$observer could not scrape Collector self-metrics"
}

# assert_metrics_scrape_denied <observer-pod> : three denied attempts after a
# selected peer has already proved endpoint health distinguish enforcement from
# a transient service failure.
assert_metrics_scrape_denied() {
  local observer="$1" attempt
  for attempt in 1 2 3; do
    if kubectl exec "$observer" -n "$NAMESPACE" -- \
        curl -fsS --connect-timeout 2 --max-time 5 \
        "http://$OTEL_COLLECTOR_SERVICE:8888/metrics" >/dev/null 2>&1; then
      fail "$observer reached Collector self-metrics without an allowed peer"
    fi
  done
}

# Prove the chart policy and the cluster CNI both enforce the intended peer:
# selected observer succeeds, an otherwise equivalent same-namespace pod is
# denied, then succeeds only while a narrowly targeted additive policy exists.
assert_collector_metrics_network_policy() {
  metrics_scrape_from "$OTEL_OBSERVER"
  assert_metrics_scrape_denied "$OTEL_UNSELECTED_OBSERVER"

  cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: $OTEL_METRICS_TEST_ALLOW
  labels:
    app.kubernetes.io/name: curie
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: e2e-otel-metrics-test-allow
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: curie
      app.kubernetes.io/instance: $RELEASE
      app.kubernetes.io/component: otel-collector
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: curie
              app.kubernetes.io/instance: $RELEASE
              app.kubernetes.io/component: e2e-otel-unselected-observer
      ports:
        - protocol: TCP
          port: 8888
EOF
  metrics_scrape_from "$OTEL_UNSELECTED_OBSERVER"
  kubectl delete networkpolicy "$OTEL_METRICS_TEST_ALLOW" \
    -n "$NAMESPACE" --wait=true --timeout=60s
  echo "Collector metrics policy: selected peer allowed; unselected peer denied; targeted control allow proved CNI enforcement"
}

# metric_value <name> [exporter] : sum every series with this exact 0.119 name,
# optionally selecting the named exporter label.
metric_value() {
  local name="$1" exporter="${2:-}"
  printf '%s\n' "$METRICS_OUT" | python3 -c '
import re, sys
name, exporter = sys.argv[1:]
total = 0.0
seen = False
for line in sys.stdin:
    if not re.match(r"^" + re.escape(name) + r"(?:\{|\s)", line):
        continue
    if exporter and f"exporter=\"{exporter}\"" not in line:
        continue
    try:
        total += float(line.rsplit(None, 1)[1])
    except (IndexError, ValueError):
        continue
    seen = True
if not seen:
    raise SystemExit(4)
print(total)
' "$name" "$exporter"
}

# wait_metrics_positive <service> <exporter-or-empty> <metric...>
wait_metrics_positive() {
  local service="$1" exporter="$2"
  shift 2
  local deadline=$((SECONDS + 90)) value all_positive
  while (( SECONDS < deadline )); do
    metrics_snapshot "$service"
    all_positive=1
    for metric in "$@"; do
      value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo 0)"
      if ! python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) > 0 else 1)' "$value"; then
        all_positive=0
      fi
    done
    [[ "$all_positive" -eq 1 ]] && return 0
    sleep 3
  done
  for metric in "$@"; do
    value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
    echo "$service $metric exporter=${exporter:-any}: $value"
  done
  fail "$service did not expose positive values for all required Collector 0.119 metrics"
}

# wait_metrics_present <service> <exporter-or-empty> <metric...>
# A recoverable exporter retry must not be mistaken for terminal signal loss.
# This helper pins the self-observability contract without requiring a failure
# counter to move before the configured retry window is exhausted.
wait_metrics_present() {
  local service="$1" exporter="$2"
  shift 2
  local deadline=$((SECONDS + 90)) value all_present
  while (( SECONDS < deadline )); do
    metrics_snapshot "$service"
    all_present=1
    for metric in "$@"; do
      value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
      [[ "$value" != missing ]] || all_present=0
    done
    [[ "$all_present" -eq 1 ]] && return 0
    sleep 3
  done
  for metric in "$@"; do
    value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
    echo "$service $metric exporter=${exporter:-any}: $value"
  done
  fail "$service omitted required Collector 0.119 self-metrics"
}

wait_collector_retry_log() {
  local pod="$1" deadline=$((SECONDS + 90)) logs=""
  while (( SECONDS < deadline )); do
    logs="$(kubectl logs "$pod" -n "$NAMESPACE" 2>&1 || true)"
    if grep -Eiq 'exporting failed.*retry|failed to export.*retry|will retry' <<<"$logs"; then
      return 0
    fi
    sleep 3
  done
  printf '%s\n' "$logs" | tail -80
  fail "Collector did not preserve its stderr exporter-retry diagnostic"
}

wait_metric_equals() {
  local service="$1" metric="$2" exporter="$3" expected="$4"
  local deadline=$((SECONDS + 90)) value="missing"
  while (( SECONDS < deadline )); do
    metrics_snapshot "$service"
    value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
    if [[ "$value" != missing ]] && python3 -c '
import math, sys
raise SystemExit(0 if math.isclose(float(sys.argv[1]), float(sys.argv[2])) else 1)
' "$value" "$expected"; then
      return 0
    fi
    sleep 3
  done
  fail "$service $metric exporter=$exporter was $value, expected $expected"
}

assert_metric_between() {
  local service="$1" metric="$2" exporter="$3" lower="$4" upper="$5"
  local value
  metrics_snapshot "$service"
  value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
  [[ "$value" != missing ]] || fail "$service omitted required self-metric $metric"
  python3 -c '
import sys
value, lower, upper = map(float, sys.argv[1:])
raise SystemExit(0 if lower <= value <= upper else 1)
' "$value" "$lower" "$upper" || \
    fail "$service $metric exporter=$exporter was $value, expected range [$lower, $upper]"
}

assert_metrics_zero() {
  local service="$1" exporter="$2"
  shift 2
  local metric value
  wait_metrics_present "$service" "$exporter" "$@"
  metrics_snapshot "$service"
  for metric in "$@"; do
    value="$(metric_value "$metric" "$exporter" 2>/dev/null || echo missing)"
    [[ "$value" != missing ]] || fail "$service omitted required self-metric $metric"
    python3 -c '
import sys
raise SystemExit(0 if float(sys.argv[1]) == 0 else 1)
' "$value" || fail "$service $metric exporter=${exporter:-any} was $value, expected zero"
  done
}

assert_sustained_outage_bounded() {
  # Observe a full minute of continuous exporter failure. This is long enough
  # to cross multiple batch/retry cycles instead of sampling one transient.
  local pod="$1" samples=20 interval=3 sample ready queue capacity used_kib
  local pod_uid restart_count current_uid current_restart_count last_reason
  local first_used_kib=0 max_used_kib=0 bound_kib growth_allowance_kib=4096
  pod_uid="$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')"
  restart_count="$(kubectl get pod "$pod" -n "$NAMESPACE" \
    -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  [[ -n "$pod_uid" && "$restart_count" =~ ^[0-9]+$ ]] || \
    fail "could not capture Collector pod identity/restart baseline"
  bound_kib="$(python3 -c '
import re, sys
value = sys.argv[1]
match = re.fullmatch(r"([0-9]+)(Ki|Mi|Gi)", value)
if not match:
    raise SystemExit(f"unsupported PVC quantity {value!r}")
amount = int(match.group(1))
scale = {"Ki": 1, "Mi": 1024, "Gi": 1024 * 1024}[match.group(2)]
print(amount * scale)
' "$PVC_REQUEST")"
  metrics_snapshot "$OTEL_COLLECTOR_SERVICE"
  capacity="$(metric_value "$OTELCOL_QUEUE_CAPACITY_METRIC" "$OTEL_EXPORTER" 2>/dev/null || echo missing)"
  [[ "$capacity" != missing ]] || fail "Collector omitted queue capacity during sustained outage"
  python3 -c '
import sys
raise SystemExit(0 if float(sys.argv[1]) == float(sys.argv[2]) else 1)
' "$capacity" "$OTEL_QUEUE_CAPACITY_TOTAL" || \
    fail "persistent queue capacity was $capacity, expected $OTEL_QUEUE_CAPACITY_TOTAL"

  for sample in $(seq 1 "$samples"); do
    ready="$(kubectl get pod "$pod" -n "$NAMESPACE" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
    [[ "$ready" == True ]] || \
      fail "Collector stopped being Ready during sustained exporter outage (sample $sample: $ready)"
    current_uid="$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')"
    current_restart_count="$(kubectl get pod "$pod" -n "$NAMESPACE" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}')"
    last_reason="$(kubectl get pod "$pod" -n "$NAMESPACE" \
      -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}')"
    [[ "$current_uid" == "$pod_uid" && "$current_restart_count" == "$restart_count" ]] || \
      fail "Collector restarted during sustained exporter outage (uid=$current_uid restarts=$current_restart_count)"
    [[ "$last_reason" != OOMKilled ]] || \
      fail "Collector was OOMKilled during sustained exporter outage"
    metrics_snapshot "$OTEL_COLLECTOR_SERVICE"
    queue="$(metric_value "$OTELCOL_QUEUE_SIZE_METRIC" "$OTEL_EXPORTER" 2>/dev/null || echo missing)"
    [[ "$queue" != missing ]] || fail "Collector omitted queue size during sustained outage"
    python3 -c '
import sys
value, capacity = map(float, sys.argv[1:])
raise SystemExit(0 if 0 < value <= capacity else 1)
' "$queue" "$capacity" || \
      fail "persistent queue was $queue outside (0, $capacity] during outage"
    used_kib="$(kubectl exec "$OTEL_STORAGE_OBSERVER" -n "$NAMESPACE" -- sh -c \
      'du -sk /var/lib/otelcol 2>/dev/null | cut -f1')"
    [[ "$used_kib" =~ ^[0-9]+$ ]] || fail "could not measure Collector PVC usage: $used_kib"
    if (( sample == 1 )); then
      first_used_kib="$used_kib"
    fi
    (( used_kib <= bound_kib )) || \
      fail "Collector PVC used ${used_kib}Ki beyond declared ${bound_kib}Ki bound"
    (( used_kib > max_used_kib )) && max_used_kib="$used_kib"
    sleep "$interval"
  done
  (( used_kib <= first_used_kib + growth_allowance_kib )) || \
    fail "Collector PVC grew from ${first_used_kib}Ki to ${used_kib}Ki while retrying a fixed bounded queue"
  echo "sustained outage: Collector remained Ready without restart/OOM for $((samples * interval))s; queue<=${capacity}, pvc-used=${first_used_kib}..${max_used_kib}Ki/${bound_kib}Ki"
}

create_otlp_probe_resources() {
  cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: $OTEL_SINK
  labels:
    app.kubernetes.io/name: curie-e2e-otel
    app.kubernetes.io/instance: $RELEASE
data:
  collector-config.yaml: |
    receivers:
      otlp:
        protocols:
          http:
            endpoint: 0.0.0.0:4318
    exporters:
      nop: {}
      # The private test sink uses detailed stderr output as an independent
      # receipt ledger. This does not enable the production chart's debug
      # exporter; it lets the harness count the uniquely named queued sample.
      debug:
        verbosity: detailed
    extensions:
      health_check:
        endpoint: 0.0.0.0:13133
    service:
      extensions: [health_check]
      telemetry:
        metrics:
          address: 0.0.0.0:8888
      pipelines:
        traces: {receivers: [otlp], exporters: [nop, debug]}
        logs: {receivers: [otlp], exporters: [nop, debug]}
        metrics: {receivers: [otlp], exporters: [nop, debug]}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: $OTEL_FIXTURES
  labels:
    app.kubernetes.io/name: curie-e2e-otel
    app.kubernetes.io/instance: $RELEASE
data:
  traces.json: |
    {"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"acme-e2e"}}]},"scopeSpans":[{"scope":{"name":"acme.e2e"},"spans":[{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","name":"trace-__LABEL__-probe","kind":1,"startTimeUnixNano":"1000000000","endTimeUnixNano":"1000000001","status":{"code":1}}]}]}]}
  logs.json: |
    {"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"acme-e2e"}}]},"scopeLogs":[{"scope":{"name":"acme.e2e"},"logRecords":[{"timeUnixNano":"1000000000","severityNumber":9,"severityText":"INFO","body":{"stringValue":"log-__LABEL__-probe"}}]}]}]}
  metrics.json: |
    {"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"acme-e2e"}}]},"scopeMetrics":[{"scope":{"name":"acme.e2e"},"metrics":[{"name":"metric.__LABEL__.probe","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asInt":"1"}]}}]}]}]}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $OTEL_SINK
  labels:
    app.kubernetes.io/name: curie-e2e-otel
    app.kubernetes.io/instance: $RELEASE
spec:
  replicas: 1
  selector:
    matchLabels: {app: $OTEL_SINK}
  template:
    metadata:
      labels: {app: $OTEL_SINK}
    spec:
      containers:
        - name: collector
          image: otel/opentelemetry-collector-contrib:0.119.0
          args: ["--config=/etc/otelcol/collector-config.yaml"]
          ports:
            - {name: otlp-http, containerPort: 4318}
            - {name: metrics, containerPort: 8888}
            - {name: health, containerPort: 13133}
          readinessProbe:
            httpGet: {path: /, port: health}
          resources:
            requests: {cpu: 10m, memory: 32Mi}
            limits: {cpu: 100m, memory: 64Mi}
          volumeMounts:
            - {name: config, mountPath: /etc/otelcol, readOnly: true}
      volumes:
        - name: config
          configMap: {name: $OTEL_SINK}
---
apiVersion: v1
kind: Service
metadata:
  name: $OTEL_SINK
  labels:
    app.kubernetes.io/name: curie-e2e-otel
    app.kubernetes.io/instance: $RELEASE
spec:
  selector: {app: $OTEL_SINK}
  ports:
    - {name: otlp-http, port: 4318, targetPort: otlp-http}
    - {name: metrics, port: 8888, targetPort: metrics}
---
apiVersion: v1
kind: Pod
metadata:
  name: $OTEL_OBSERVER
  labels:
    app.kubernetes.io/name: curie
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: e2e-otel-observer
spec:
  restartPolicy: Never
  containers:
    - name: observer
      image: curlimages/curl:8.12.1
      command: ["sh", "-c", "sleep 3600"]
      resources:
        requests: {cpu: 5m, memory: 8Mi}
        limits: {cpu: 50m, memory: 32Mi}
---
apiVersion: v1
kind: Pod
metadata:
  name: $OTEL_UNSELECTED_OBSERVER
  labels:
    app.kubernetes.io/name: curie
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: e2e-otel-unselected-observer
spec:
  restartPolicy: Never
  containers:
    - name: observer
      image: curlimages/curl:8.12.1
      command: ["sh", "-c", "sleep 3600"]
      resources:
        requests: {cpu: 5m, memory: 8Mi}
        limits: {cpu: 50m, memory: 32Mi}
EOF
  kubectl rollout status deployment/$OTEL_SINK -n "$NAMESPACE" --timeout=120s
  kubectl wait --for=condition=Ready pod/$OTEL_OBSERVER -n "$NAMESPACE" --timeout=120s
  kubectl wait --for=condition=Ready pod/$OTEL_UNSELECTED_OBSERVER \
    -n "$NAMESPACE" --timeout=120s
}

create_otlp_storage_observer() {
  cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: $OTEL_STORAGE_OBSERVER
  labels:
    app.kubernetes.io/name: curie
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: e2e-otel-storage-observer
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: storage-observer
      image: busybox:1.36.1
      command: ["sh", "-c", "sleep 3600"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: [ALL]}
      resources:
        requests: {cpu: 5m, memory: 8Mi}
        limits: {cpu: 25m, memory: 16Mi}
      volumeMounts:
        - name: collector-storage
          mountPath: /var/lib/otelcol
          readOnly: true
  volumes:
    - name: collector-storage
      persistentVolumeClaim:
        claimName: $COLLECTOR_PVC
        readOnly: true
EOF
  kubectl wait --for=condition=Ready pod/$OTEL_STORAGE_OBSERVER \
    -n "$NAMESPACE" --timeout=120s
}

# send_otlp_triplet <label> <count> <delay-seconds> <strict|best-effort>
send_otlp_triplet() {
  local label="$1" count="$2" delay="$3" mode="$4"
  local job="e2e-otlp-${label}"
  kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found >/dev/null
  cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  labels:
    app.kubernetes.io/name: curie-e2e-otel
    app.kubernetes.io/instance: $RELEASE
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: curie
        app.kubernetes.io/instance: $RELEASE
        app.kubernetes.io/component: e2e-otel-probe
    spec:
      restartPolicy: Never
      containers:
        - name: send
          image: curlimages/curl:8.12.1
          command:
            - sh
            - -c
            - |
              set -eu
              i=1
              while [ "\$i" -le "$count" ]; do
                for signal in traces logs metrics; do
                  sed 's/__LABEL__/$label/g' "/fixtures/\${signal}.json" > /tmp/payload.json
                  if [ "$mode" = strict ]; then
                    curl -fsS -H 'Content-Type: application/json' \
                      --data-binary @/tmp/payload.json \
                      "http://$OTEL_COLLECTOR_SERVICE:4318/v1/\${signal}" >/dev/null
                  else
                    curl -sS -H 'Content-Type: application/json' \
                      --data-binary @/tmp/payload.json \
                      "http://$OTEL_COLLECTOR_SERVICE:4318/v1/\${signal}" >/dev/null || true
                  fi
                done
                i=\$((i + 1))
                [ "$delay" = 0 ] || sleep "$delay"
              done
          volumeMounts:
            - {name: fixtures, mountPath: /fixtures, readOnly: true}
      volumes:
        - name: fixtures
          configMap: {name: $OTEL_FIXTURES}
EOF
  if ! kubectl wait --for=condition=complete job/$job -n "$NAMESPACE" --timeout=180s; then
    kubectl logs job/$job -n "$NAMESPACE" || true
    fail "OTLP fixture Job $job did not complete"
  fi
}

# build_bound_pod : read the installed SandboxTemplate, extract its podTemplate,
# and emit a bound `kind: Pod` manifest on stdout. This is the generic
# "render template -> bind bundle" step; any runtime assertion reuses the Pod.
build_bound_pod() {
  local template_json="$1"
  POD_NAME="$POD_NAME" NAMESPACE="$NAMESPACE" BUNDLE_REF="$BUNDLE_REF" \
    RUNNER_IMAGE="$RUNNER_IMAGE" python3 - "$template_json" <<'PY'
import json, os, sys

st = json.load(open(sys.argv[1]))
pod_tmpl = st["spec"]["podTemplate"]
spec = dict(pod_tmpl.get("spec", {}))

pod_name = os.environ["POD_NAME"]
namespace = os.environ["NAMESPACE"]
bundle_ref = os.environ["BUNDLE_REF"]
runner_image = os.environ.get("RUNNER_IMAGE", "")

# Bind the bundle: replace any CURIE_BUNDLE_REF env (drop valueFrom) with the
# seeded object key, in every container and initContainer that declares it.
def bind_ref(container):
    env = container.get("env")
    if not env:
        return
    for e in env:
        if e.get("name") == "CURIE_BUNDLE_REF":
            e.clear()
            e["name"] = "CURIE_BUNDLE_REF"
            e["value"] = bundle_ref

def bind_runner_bundle(container):
    if container.get("name") != "runner":
        return
    for e in container.get("env", []):
        if e.get("name") == "CURIE_PLUGIN_DIR":
            e.clear()
            e["name"] = "CURIE_PLUGIN_DIR"
            e["value"] = "/bundles/current"

for c in spec.get("initContainers", []):
    bind_ref(c)
for c in spec.get("containers", []):
    bind_ref(c)
    # The installed SandboxTemplate is intentionally warm/unbound and points
    # at /unused. This harness has just bound a real bundle, so bind the runner
    # to the extracted directory as well; otherwise the default exact command
    # exits before the runtime assertions and only --runner-image can pass.
    bind_runner_bundle(c)

# Use the default ServiceAccount (avoids a missing-SA scheduling failure).
spec.pop("serviceAccountName", None)
spec.pop("automountServiceAccountToken", None)
spec["restartPolicy"] = "Never"

# Optional runner-image override: swap ONLY the runner container's image for a
# trivially-runnable one. The #56 assertion depends on the runner's VIEW of the
# shared bundle mount, not the runner binary, so this is a safe fallback when the
# real runner image will not reach Running on the cluster.
if runner_image:
    for c in spec.get("containers", []):
        if c.get("name") == "runner":
            c["image"] = runner_image
            c["command"] = ["/bin/sh", "-c", "sleep 3600"]
            c.pop("readinessProbe", None)
            c.pop("livenessProbe", None)
            c.pop("ports", None)

pod = {
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {
        "name": pod_name,
        "namespace": namespace,
        "labels": pod_tmpl.get("metadata", {}).get("labels", {}),
    },
    "spec": spec,
}
print(json.dumps(pod))
PY
}

# wait_for_init_complete : block until BOTH init containers are terminated with
# reason=Completed and the runner container is state=running (not necessarily
# Ready -- /healthz readiness may never flip, but exec works on a Running
# container). Dumps diagnostics and fails on timeout.
wait_for_init_complete() {
  local timeout=180 waited=0 interval=4
  while true; do
    local pod_json
    pod_json="$(kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o json 2>/dev/null || echo '{}')"
    local ready
    ready="$(printf '%s' "$pod_json" | python3 -c '
import json, sys
p = json.load(sys.stdin)
st = p.get("status", {})
inits = st.get("initContainerStatuses", [])
conts = st.get("containerStatuses", [])
init_ok = len(inits) >= 2 and all(
    (c.get("state", {}).get("terminated", {}) or {}).get("reason") == "Completed"
    for c in inits
)
runner_running = any(
    c.get("name") == "runner" and "running" in c.get("state", {})
    for c in conts
)
print("yes" if (init_ok and runner_running) else "no")
' 2>/dev/null || echo "no")"
    if [[ "$ready" == "yes" ]]; then
      return 0
    fi
    if [[ "$waited" -ge "$timeout" ]]; then
      banner "DIAGNOSTICS (timeout after ${timeout}s)"
      kubectl describe pod "$POD_NAME" -n "$NAMESPACE" || true
      echo "--- bundle-fetch logs"
      kubectl logs "$POD_NAME" -n "$NAMESPACE" -c bundle-fetch || true
      echo "--- bundle-extract logs"
      kubectl logs "$POD_NAME" -n "$NAMESPACE" -c bundle-extract || true
      fail "bound sandbox did not reach (init Completed + runner Running) within ${timeout}s"
    fi
    sleep "$interval"
    waited=$((waited + interval))
  done
}

# run_assertions : exec the runner and evaluate the positive control + the #56
# security assertion. Sets global RESULT to "PASS" or "FAIL".
run_assertions() {
  RESULT="FAIL"

  # POSITIVE CONTROL: prove the bundle was actually fetched + extracted, so an
  # empty config.json result below is meaningful (not a no-op fetch).
  exec_echo "positive control: bundle manifest present" \
    -c runner -- sh -c 'find /bundles/current -name plugin.json'
  # Bundle present only if the exec succeeded AND the manifest path printed on
  # STDOUT (stderr like "Permission denied" must never count as present).
  if [[ "$EXEC_RC" -ne 0 || -z "${EXEC_OUT//[[:space:]]/}" ]]; then
    banner "DIAGNOSTICS (positive control failed)"
    kubectl logs "$POD_NAME" -n "$NAMESPACE" -c bundle-fetch || true
    kubectl logs "$POD_NAME" -n "$NAMESPACE" -c bundle-extract || true
    fail "bundle not provisioned (no plugin.json under /bundles/current); test inconclusive"
  fi

  # SECURITY ASSERTION (#56): the RustFS credential must not be readable off the
  # shared bundle volume from the runner's view.
  exec_echo "security: S3 client credential files on shared volume" \
    -c runner -- sh -c "find /bundles \\( -name config.json -o -name credentials \\)"
  local config_hits="$EXEC_OUT"
  exec_echo "security: persisted AWS credential fields on shared volume" \
    -c runner -- sh -c "grep -rEl 'aws_(access_key_id|secret_access_key)' /bundles 2>/dev/null || true"
  local cred_hits="$EXEC_OUT"

  local exposed=0
  if [[ -n "${config_hits//[[:space:]]/}" || -n "${cred_hits//[[:space:]]/}" ]]; then
    exposed=1
  fi

  echo
  if [[ "$EXPECT_VULNERABLE" -eq 1 ]]; then
    # Negative control: the credential SHOULD be exposed on an unfixed template.
    if [[ "$exposed" -eq 1 ]]; then
      echo "negative control: credential IS exposed on shared volume (expected on unfixed template)"
      RESULT="PASS"
    else
      echo "negative control: credential NOT exposed, but --expect-vulnerable expected it"
      RESULT="FAIL"
    fi
  else
    if [[ "$exposed" -eq 0 ]]; then
      echo "security assertion clean: no config.json and no cleartext credential on shared volume"
      RESULT="PASS"
    else
      echo "security assertion FAILED: RustFS credential is readable off the shared bundle volume"
      RESULT="FAIL"
    fi
  fi
}

# --------------------------------------------------------------------------
# 1. Fresh namespace + trimmed install
# --------------------------------------------------------------------------
banner "PRECHECK context=$CURRENT_CTX namespace=$NAMESPACE release=$RELEASE"

DEFAULT_STORAGE_CLASS="$(kubectl get storageclass -o json | python3 -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
defaults = []
for item in items:
    annotations = item.get("metadata", {}).get("annotations", {})
    if annotations.get("storageclass.kubernetes.io/is-default-class") == "true" or annotations.get("storageclass.beta.kubernetes.io/is-default-class") == "true":
        defaults.append(item.get("metadata", {}).get("name", ""))
print("\n".join(name for name in defaults if name))
')"
if [[ -z "$DEFAULT_STORAGE_CLASS" ]]; then
  fail "no default StorageClass is installed; durable Collector PVC runtime proof cannot run"
fi
if [[ "$DEFAULT_STORAGE_CLASS" == *$'\n'* ]]; then
  fail "multiple default StorageClasses detected (${DEFAULT_STORAGE_CLASS//$'\n'/, }); choose exactly one before running durable Collector proof"
fi
echo "default StorageClass: $DEFAULT_STORAGE_CLASS"

if kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  # Only reclaim a namespace WE created. An unlabeled pre-existing namespace
  # (e.g. `default`, or someone else's) must never be deleted by this harness.
  if ns_is_owned "$NAMESPACE"; then
    banner "namespace $NAMESPACE already exists (harness-owned) -- cleaning up before run"
    helm uninstall "$RELEASE" -n "$NAMESPACE" --no-hooks >/dev/null 2>&1 || true
    kubectl delete ns "$NAMESPACE" --wait=true --timeout=120s >/dev/null 2>&1 || true
  else
    fail "namespace $NAMESPACE already exists and is NOT owned by this harness (missing label ${OWNED_LABEL}=true). Remove it manually or pick another --namespace."
  fi
fi

# Pre-create the namespace stamped with the ownership label so teardown/cleanup
# can only ever delete a namespace this script created.
create_owned_ns "$NAMESPACE"

banner "CREATE private OTLP sink and observer"
create_otlp_probe_resources

OTEL_OVERLAY="$E2E_TMP/otel-durability-values.yaml"
cat > "$OTEL_OVERLAY" <<EOF
# PriorityClasses are cluster-scoped. Give this task-owned release private
# names so a concurrent or long-lived Curie install cannot be adopted or
# disturbed by the throwaway runtime proof.
priorityClasses:
  platform:
    name: $RELEASE-curie-platform
  sandbox:
    name: $RELEASE-curie-sandbox
otelCollector:
  deploy: true
  debugExporter:
    enabled: false
  persistence:
    enabled: true
    size: $OTEL_PVC_SIZE
  resources:
    requests: {cpu: 25m, memory: 48Mi}
    limits: {cpu: 200m, memory: $OTEL_MEMORY_LIMIT}
  extraExporters:
    $OTEL_EXPORTER:
      endpoint: http://$OTEL_SINK:4318
      retry_on_failure:
        enabled: true
        initial_interval: 1s
        max_interval: 2s
        # Leave enough retry horizon for a deliberately sustained outage plus
        # a slow shared-cluster PVC detach/reattach. Queue overflow, not retry
        # expiry, is the independent bounded-loss control below.
        max_elapsed_time: 600s
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: $OTEL_QUEUE_SIZE
        num_consumers: 1
  extraPipelineExporters: [$OTEL_EXPORTER]
  extraLogPipelineExporters: [$OTEL_EXPORTER]
  extraMetricPipelineExporters: [$OTEL_EXPORTER]
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - podSelector:
          matchLabels:
            app.kubernetes.io/name: curie
            app.kubernetes.io/instance: $RELEASE
            app.kubernetes.io/component: e2e-otel-observer
EOF

banner "INSTALL trimmed chart"
install_chart() {
  helm install "$RELEASE" "$CHART_PATH" \
    -n "$NAMESPACE" --no-hooks \
    -f "$CHART_PATH/values-e2e-nogvisor.yaml" \
    -f "$CHART_PATH/values-e2e-harness.yaml" \
    -f "$OTEL_OVERLAY"
}
# k8scratch is shared and slightly flaky: a spurious "namespaces not found" at
# install is usually API churn, so retry ONCE before failing.
if ! install_chart; then
  echo "helm install failed once (likely transient API churn on shared node); retrying in 5s..."
  sleep 5
  # Recreate the labeled namespace before retrying; only ever delete our own.
  if ns_is_owned "$NAMESPACE"; then
    kubectl delete ns "$NAMESPACE" --wait=true --timeout=60s >/dev/null 2>&1 || true
  fi
  create_owned_ns "$NAMESPACE"
  create_otlp_probe_resources
  install_chart || fail "helm install failed twice"
fi

banner "WAIT durable OTel Collector Running"
if ! kubectl rollout status deployment/$OTEL_COLLECTOR_DEPLOYMENT \
    -n "$NAMESPACE" --timeout=180s; then
  kubectl describe deployment/$OTEL_COLLECTOR_DEPLOYMENT -n "$NAMESPACE" || true
  kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=otel-collector || true
  fail "chart OTel Collector did not become Ready"
fi

banner "ASSERT Collector self-metrics ingress policy"
assert_collector_metrics_network_policy

COLLECTOR_PVC="$(kubectl get deployment "$OTEL_COLLECTOR_DEPLOYMENT" -n "$NAMESPACE" -o json | python3 -c '
import json, sys
pod = json.load(sys.stdin)["spec"]["template"]["spec"]
claims = [
    volume.get("persistentVolumeClaim", {}).get("claimName")
    for volume in pod.get("volumes", [])
    if volume.get("persistentVolumeClaim", {}).get("claimName")
]
if len(claims) != 1:
    raise SystemExit(f"expected one Collector PVC volume, found {claims!r}")
print(claims[0])
')"
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/$COLLECTOR_PVC \
  -n "$NAMESPACE" --timeout=120s
create_otlp_storage_observer
PVC_REQUEST="$(kubectl get pvc "$COLLECTOR_PVC" -n "$NAMESPACE" -o jsonpath='{.spec.resources.requests.storage}')"
PVC_CAPACITY="$(kubectl get pvc "$COLLECTOR_PVC" -n "$NAMESPACE" -o jsonpath='{.status.capacity.storage}')"
[[ "$PVC_REQUEST" == "$OTEL_PVC_SIZE" ]] || \
  fail "Collector disk bound is $PVC_REQUEST, expected task limit $OTEL_PVC_SIZE"
[[ -n "$PVC_CAPACITY" ]] || fail "Collector PVC $COLLECTOR_PVC has no finite bound capacity"

COLLECTOR_MEMORY_LIMIT="$(kubectl get deployment "$OTEL_COLLECTOR_DEPLOYMENT" -n "$NAMESPACE" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="otel-collector")].resources.limits.memory}')"
[[ "$COLLECTOR_MEMORY_LIMIT" == "$OTEL_MEMORY_LIMIT" ]] || \
  fail "Collector memory bound is $COLLECTOR_MEMORY_LIMIT, expected $OTEL_MEMORY_LIMIT"
echo "bounded Collector resources: memory=$COLLECTOR_MEMORY_LIMIT disk-request=$PVC_REQUEST disk-capacity=$PVC_CAPACITY"

# --------------------------------------------------------------------------
# 2. Wait for RustFS Running. Gate on the pod, not Helm release status.
# --------------------------------------------------------------------------
banner "WAIT RustFS Running"
if ! kubectl wait --for=condition=Ready pod \
    -l app.kubernetes.io/component=rustfs \
    -n "$NAMESPACE" --timeout=180s; then
  kubectl get pods -n "$NAMESPACE" || true
  kubectl describe pod -l app.kubernetes.io/component=rustfs -n "$NAMESPACE" || true
  fail "RustFS pod did not become Ready"
fi

# --------------------------------------------------------------------------
# 3. Seed a real bundle into RustFS
# --------------------------------------------------------------------------
banner "SEED bundle into RustFS"
# Build a VALID tar.gz (bundle-extract runs `set -eu; tar -xzf`, so a malformed
# archive fails the pod). Layout: myplugin/.claude-plugin/plugin.json.
SEED_DIR="$(mktemp -d /tmp/e2e-bundle.XXXXXX)"
mkdir -p "$SEED_DIR/myplugin/.claude-plugin"
cat > "$SEED_DIR/myplugin/.claude-plugin/plugin.json" <<'JSON'
{"name":"e2e-probe","version":"0.0.0"}
JSON
tar -czf "$SEED_DIR/probe.tgz" -C "$SEED_DIR" myplugin
PROBE_B64="$(base64 -w0 "$SEED_DIR/probe.tgz" 2>/dev/null || base64 "$SEED_DIR/probe.tgz" | tr -d '\n')"

# ConfigMap carrying the archive plus a one shot AWS CLI Job that creates the
# bucket and uploads the object using path addressing.
cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: e2e-bundle-seed
binaryData:
  probe.tgz: $PROBE_B64
---
apiVersion: batch/v1
kind: Job
metadata:
  name: e2e-bundle-seed
spec:
  backoffLimit: 3
  template:
    metadata:
      labels:
        app.kubernetes.io/name: curie
        app.kubernetes.io/instance: $RELEASE
    spec:
      restartPolicy: Never
      containers:
        - name: seed
          image: amazon/aws-cli:2.32.6
          imagePullPolicy: IfNotPresent
          command:
            - /bin/sh
            - -c
            - |
              set -eu
              mkdir -p /tmp/aws
              aws configure set default.s3.addressing_style path
              endpoint="http://$RUSTFS_SVC:9000"
              aws --endpoint-url "\$endpoint" s3api head-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1 || \
                aws --endpoint-url "\$endpoint" s3api create-bucket --bucket "$RUSTFS_BUCKET"
              aws --endpoint-url "\$endpoint" s3 cp /data/probe.tgz "s3://$RUSTFS_BUCKET/$BUNDLE_REF"
              echo "seeded $BUNDLE_REF"
          env:
            - name: AWS_ACCESS_KEY_ID
              value: $RUSTFS_ACCESS_KEY
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: $SECRET_NAME
                  key: rustfsSecretKey
            - name: AWS_DEFAULT_REGION
              value: us-east-1
            - name: AWS_CONFIG_FILE
              value: /tmp/aws/config
          volumeMounts:
            - name: bundle
              mountPath: /data
      volumes:
        - name: bundle
          configMap:
            name: e2e-bundle-seed
EOF

if ! kubectl wait --for=condition=complete job/e2e-bundle-seed \
    -n "$NAMESPACE" --timeout=120s; then
  kubectl logs job/e2e-bundle-seed -n "$NAMESPACE" || true
  fail "bundle seed Job did not complete"
fi
echo "bundle seeded: $BUNDLE_REF"

# --------------------------------------------------------------------------
# 4. Render + apply the bound sandbox Pod
# --------------------------------------------------------------------------
banner "RENDER bound sandbox Pod from SandboxTemplate $SANDBOX_TEMPLATE"
if [[ -n "$RUNNER_IMAGE" ]]; then
  echo "runner image override: $RUNNER_IMAGE"
else
  echo "runner image: real (from SandboxTemplate)"
fi
TEMPLATE_JSON="$(mktemp /tmp/e2e-sandboxtemplate.XXXXXX.json)"
kubectl get sandboxtemplate "$SANDBOX_TEMPLATE" -n "$NAMESPACE" -o json > "$TEMPLATE_JSON"
POD_JSON="$(mktemp /tmp/e2e-bound-pod.XXXXXX.json)"
build_bound_pod "$TEMPLATE_JSON" > "$POD_JSON"
kubectl apply -n "$NAMESPACE" -f "$POD_JSON"

banner "WAIT init pair Completed + runner Running"
wait_for_init_complete
echo "bound sandbox ready: init containers Completed, runner Running"

# --------------------------------------------------------------------------
# 5. Assertions
# --------------------------------------------------------------------------
banner "ASSERT runtime security (#56)"
run_assertions

# --------------------------------------------------------------------------
# 6. Collector reception, durable outage/restart recovery, and overflow loss
# --------------------------------------------------------------------------
banner "ASSERT OTel healthy reception and delivery"
send_otlp_triplet healthy 1 0 strict
wait_metrics_positive "$OTEL_COLLECTOR_SERVICE" "" "${OTELCOL_RECEIVER_METRICS[@]}"
wait_metrics_positive "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" "${OTELCOL_SENT_METRICS[@]}"
wait_metrics_positive "$OTEL_SINK" "" "${OTELCOL_RECEIVER_METRICS[@]}"
assert_metrics_zero "$OTEL_COLLECTOR_SERVICE" "" "${OTELCOL_REFUSED_METRICS[@]}"
assert_metrics_zero "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" \
  "${OTELCOL_FAILED_METRICS[@]}"
echo "healthy path: all three signals accepted and sent; refused and send-failed stayed observable at zero"

banner "ASSERT backend outage queues and reports failure"
SINK_POD="$(kubectl get pod -n "$NAMESPACE" -l app="$OTEL_SINK" -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$SINK_POD" ]] || fail "private OTLP sink has no pod before outage control"
kubectl scale deployment/$OTEL_SINK -n "$NAMESPACE" --replicas=0
kubectl wait --for=delete pod/$SINK_POD -n "$NAMESPACE" --timeout=120s
# Occupy the sole consumer with a distinct in-flight retry, then enqueue the
# uniquely named outage sample behind it. Queue size excludes the in-flight
# request, so this makes durable persistence observable before restart.
send_otlp_triplet priming 1 0 strict
OUTAGE_COLLECTOR_POD="$(kubectl get pod -n "$NAMESPACE" \
  -l app.kubernetes.io/component=otel-collector -o jsonpath='{.items[0].metadata.name}')"
wait_collector_retry_log "$OUTAGE_COLLECTOR_POD"
send_otlp_triplet outage 1 0 strict
wait_metrics_present "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" "${OTELCOL_FAILED_METRICS[@]}"
wait_metrics_present "$OTEL_COLLECTOR_SERVICE" "" "${OTELCOL_REFUSED_METRICS[@]}"
wait_metrics_positive "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" "$OTELCOL_QUEUE_SIZE_METRIC"
assert_sustained_outage_bounded "$OUTAGE_COLLECTOR_POD"
echo "outage path: per-signal refused/failure counters remain visible, stderr reports retry, and the bounded persistent queue is non-empty"

banner "ASSERT Collector restart retains the queued signals on its PVC"
OLD_COLLECTOR_POD="$(kubectl get pod -n "$NAMESPACE" \
  -l app.kubernetes.io/component=otel-collector -o jsonpath='{.items[0].metadata.name}')"
OLD_COLLECTOR_UID="$(kubectl get pod "$OLD_COLLECTOR_POD" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')"
# The storage observer mounts the same ReadWriteOnce claim read-only. Remove it
# before replacement so single-node and multi-node provisioners alike can
# detach/reattach the Collector claim without a second live pod holding it.
kubectl delete pod "$OTEL_STORAGE_OBSERVER" -n "$NAMESPACE" \
  --wait=true --timeout=120s
kubectl delete pod "$OLD_COLLECTOR_POD" -n "$NAMESPACE" --wait=true --timeout=180s
kubectl rollout status deployment/$OTEL_COLLECTOR_DEPLOYMENT -n "$NAMESPACE" --timeout=180s
NEW_COLLECTOR_POD="$(kubectl get pod -n "$NAMESPACE" \
  -l app.kubernetes.io/component=otel-collector -o jsonpath='{.items[0].metadata.name}')"
NEW_COLLECTOR_UID="$(kubectl get pod "$NEW_COLLECTOR_POD" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')"
[[ "$NEW_COLLECTOR_UID" != "$OLD_COLLECTOR_UID" ]] || \
  fail "Collector restart control reused pod UID $OLD_COLLECTOR_UID"
NEW_COLLECTOR_PVC="$(kubectl get pod "$NEW_COLLECTOR_POD" -n "$NAMESPACE" -o json | python3 -c '
import json, sys
pod = json.load(sys.stdin)["spec"]
claims = [
    volume.get("persistentVolumeClaim", {}).get("claimName")
    for volume in pod.get("volumes", [])
    if volume.get("persistentVolumeClaim", {}).get("claimName")
]
if len(claims) != 1:
    raise SystemExit(f"expected one Collector PVC after restart, found {claims!r}")
print(claims[0])
')"
[[ "$NEW_COLLECTOR_PVC" == "$COLLECTOR_PVC" ]] || \
  fail "Collector restart changed durable claim from $COLLECTOR_PVC to $NEW_COLLECTOR_PVC"
wait_metrics_positive "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" "$OTELCOL_QUEUE_SIZE_METRIC"
echo "restart path: new pod UID retained claim $COLLECTOR_PVC and its non-empty queue"

banner "ASSERT queued outage sample recovers exactly once"
kubectl scale deployment/$OTEL_SINK -n "$NAMESPACE" --replicas=1
kubectl rollout status deployment/$OTEL_SINK -n "$NAMESPACE" --timeout=120s
wait_metrics_positive "$OTEL_SINK" "" "${OTELCOL_RECEIVER_METRICS[@]}"
# Sink self-metrics reset when its outage pod was removed. Count the unique
# outage sample in its detailed stderr receipt ledger so an independently
# recovered priming request cannot disguise a duplicate queued delivery.
sleep 5
RECOVERY_SINK_POD="$(kubectl get pod -n "$NAMESPACE" -l app="$OTEL_SINK" \
  -o jsonpath='{.items[0].metadata.name}')"
RECOVERY_LOGS="$(kubectl logs "$RECOVERY_SINK_POD" -n "$NAMESPACE")"
for marker in trace-outage-probe log-outage-probe metric.outage.probe; do
  recovered="$(awk -v marker="$marker" 'index($0, marker) { count++ } END { print count + 0 }' \
    <<<"$RECOVERY_LOGS")"
  [[ "$recovered" -eq 1 ]] || \
    fail "$marker appeared $recovered times at the recovered sink, expected exactly once"
done
wait_metric_equals "$OTEL_COLLECTOR_SERVICE" "$OTELCOL_QUEUE_SIZE_METRIC" "$OTEL_EXPORTER" 0
echo "recovery path: uniquely named queued trace, log record, and metric point delivered once; queue drained"

banner "ASSERT tiny persistent queue overflows loudly and loses bounded excess"
SINK_POD="$(kubectl get pod -n "$NAMESPACE" -l app="$OTEL_SINK" -o jsonpath='{.items[0].metadata.name}')"
kubectl scale deployment/$OTEL_SINK -n "$NAMESPACE" --replicas=0
kubectl wait --for=delete pod/$SINK_POD -n "$NAMESPACE" --timeout=120s
# One-second spacing forces separate batch exports. With one request retrying
# and queue_size=2, twelve samples cannot all fit and enqueue_failed_* must move.
OVERFLOW_SAMPLES=12
send_otlp_triplet overflow "$OVERFLOW_SAMPLES" 1 best-effort
wait_metrics_positive "$OTEL_COLLECTOR_SERVICE" "$OTEL_EXPORTER" "${OTELCOL_ENQUEUE_FAILED_METRICS[@]}"
wait_metric_equals "$OTEL_COLLECTOR_SERVICE" "$OTELCOL_QUEUE_CAPACITY_METRIC" "$OTEL_EXPORTER" "$OTEL_QUEUE_CAPACITY_TOTAL"
assert_metric_between "$OTEL_COLLECTOR_SERVICE" "$OTELCOL_QUEUE_SIZE_METRIC" "$OTEL_EXPORTER" 0 "$OTEL_QUEUE_CAPACITY_TOTAL"

kubectl scale deployment/$OTEL_SINK -n "$NAMESPACE" --replicas=1
kubectl rollout status deployment/$OTEL_SINK -n "$NAMESPACE" --timeout=120s
wait_metrics_positive "$OTEL_SINK" "" "${OTELCOL_RECEIVER_METRICS[@]}"
sleep 8
metrics_snapshot "$OTEL_SINK"
for metric in "${OTELCOL_RECEIVER_METRICS[@]}"; do
  delivered="$(metric_value "$metric" 2>/dev/null || echo missing)"
  [[ "$delivered" != missing ]] || fail "overflow recovery sink omitted $metric"
  python3 -c '
import sys
value, emitted = float(sys.argv[1]), float(sys.argv[2])
raise SystemExit(0 if 0 < value < emitted else 1)
' "$delivered" "$OVERFLOW_SAMPLES" || \
    fail "$metric delivered $delivered of $OVERFLOW_SAMPLES overflow samples; expected explicit bounded loss"
done
echo "overflow path: enqueue_failed metrics moved, per-signal capacity stayed $OTEL_QUEUE_SIZE, and excess was observably lost"

# --------------------------------------------------------------------------
# Verdict
# --------------------------------------------------------------------------
echo
if [[ "$EXPECT_VULNERABLE" -eq 1 ]]; then
  echo "mode: negative-control (--expect-vulnerable)"
else
  echo "mode: default (expect secure)"
fi
if [[ "$RESULT" == "PASS" ]]; then
  echo "PASS"
  exit 0
else
  echo "FAIL"
  exit 1
fi
