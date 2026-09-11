#!/usr/bin/env bash
# Disposable runtime proof for retained Curie metrics and alert absent-data.
#
# Applies the shipped extraMetricPipelineExporters overlay on a task-owned
# namespace. Refuses the permanent soak identities. Cleans up unless --keep.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
ASSETS="$REPO_ROOT/examples/sre-bot/observability"

CONTEXT="${CURIE_2428_CONTEXT:-k8scratch}"
CURIE_NS="${CURIE_2428_NAMESPACE:-curie-t2428}"
OBS_NS="${CURIE_2428_OBS_NAMESPACE:-obs-t2428}"
RELEASE="${CURIE_2428_RELEASE:-t2428}"
PROM_RELEASE="${CURIE_2428_PROM_RELEASE:-t2428prom}"
KEEP=0
PROM_CHART_VERSION=29.27.0

usage() {
  cat <<'EOF'
Usage: charts/curie/ci/runtime/metrics-alerts-runtime.sh [--keep]

Installs a task-owned Prometheus and a trimmed Curie Collector overlay,
emits Curie run/queue/RPC/delivery metric points over OTLP, queries the
retained series, then breaks export to prove absent-data detection and
restores it. Refuses namespaces/releases used by the permanent soak.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep) KEEP=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }

for forbidden in curie observability monitoring; do
  if [[ "$CURIE_NS" == "$forbidden" || "$OBS_NS" == "$forbidden" || "$RELEASE" == "curie" ]]; then
    fail "refusing soak identity ns/release '$forbidden'"
  fi
done

command -v helm >/dev/null 2>&1 || fail "helm is required"
command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

kubectl config use-context "$CONTEXT" >/dev/null
current="$(kubectl config current-context)"
[[ "$current" == "$CONTEXT" ]] || fail "kubectl context is $current, expected $CONTEXT"

# Never talk to the soak identities even if this script is pointed at k8scratch.
for ns in curie observability; do
  if [[ "$CURIE_NS" == "$ns" || "$OBS_NS" == "$ns" ]]; then
    fail "refusing to mutate soak namespace $ns"
  fi
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cleanup() {
  if [[ "$KEEP" -eq 1 ]]; then
    echo "KEEP=1: leaving $CURIE_NS / $OBS_NS"
    return
  fi
  helm uninstall "$RELEASE" -n "$CURIE_NS" --wait --timeout 120s >/dev/null 2>&1 || true
  helm uninstall "$PROM_RELEASE" -n "$OBS_NS" --wait --timeout 120s >/dev/null 2>&1 || true
  kubectl delete priorityclass "$RELEASE-curie-platform" "$RELEASE-curie-sandbox" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete ns "$CURIE_NS" "$OBS_NS" --wait=true --timeout=180s >/dev/null 2>&1 || true
}
trap 'cleanup; rm -rf "$TMP"' EXIT

python3 - "$ASSETS/curie-values.yaml" "$ASSETS/prometheus-values.yaml" \
  "$OBS_NS" "$PROM_RELEASE" "$TMP/curie-overlay.yaml" "$TMP/prometheus-overlay.yaml" <<'PY'
from pathlib import Path
import sys

curie_src, prom_src, obs_ns, prom_release, curie_dst, prom_dst = sys.argv[1:]
curie = Path(curie_src).read_text().replace(
    ".observability.svc.cluster.local", f".{obs_ns}.svc.cluster.local"
).replace(
    "kubernetes.io/metadata.name: observability",
    f"kubernetes.io/metadata.name: {obs_ns}",
).replace(
    "http://prometheus-server.", f"http://{prom_release}-prometheus-server."
).replace(
    "app.kubernetes.io/instance: prometheus",
    f"app.kubernetes.io/instance: {prom_release}",
)
prom = Path(prom_src).read_text()
# Disposable proof: do not schedule a second node-exporter or kube-state-metrics
# on the shared soak node. Application remote-write and alerts still load.
if "kube-state-metrics:" in prom:
    prom = prom.replace(
        "kube-state-metrics:\n  enabled: true",
        "kube-state-metrics:\n  enabled: false",
        1,
    )
if "prometheus-node-exporter:" in prom:
    prom = prom.replace(
        "prometheus-node-exporter:\n  enabled: true",
        "prometheus-node-exporter:\n  enabled: false",
        1,
    )
Path(curie_dst).write_text(curie)
Path(prom_dst).write_text(prom)
PY

kubectl create ns "$OBS_NS"
kubectl create ns "$CURIE_NS"
kubectl label ns "$CURIE_NS" app.kubernetes.io/name=curie app.kubernetes.io/instance="$RELEASE"

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update prometheus-community >/dev/null
helm upgrade --install "$PROM_RELEASE" prometheus-community/prometheus \
  --version "$PROM_CHART_VERSION" \
  --namespace "$OBS_NS" \
  -f "$TMP/prometheus-overlay.yaml" \
  --set server.persistentVolume.enabled=false \
  --set server.resources.requests.memory=256Mi \
  --wait --timeout 180s

apply_curie() {
  local overlay=()
  if [[ "${1:-}" == "--overlay" ]]; then
    overlay=(-f "$TMP/curie-overlay.yaml")
  fi
  helm upgrade --install "$RELEASE" "$CHART" \
    --namespace "$CURIE_NS" --no-hooks \
    -f "$CHART/values-e2e-nogvisor.yaml" \
    -f "$CHART/values-e2e-harness.yaml" \
    "${overlay[@]}" \
    --set otelCollector.deploy=true \
    --set otelCollector.persistence.enabled=false \
    --set priorityClasses.platform.create=true \
    --set-string priorityClasses.platform.name="$RELEASE-curie-platform" \
    --set priorityClasses.sandbox.create=true \
    --set-string priorityClasses.sandbox.name="$RELEASE-curie-sandbox" \
    --set api.deploy=false \
    --set worker.deploy=false \
    --set dispatcher.deploy=false \
    --set ui.deploy=false \
    --set postgres.deploy=false \
    --set valkey.deploy=false \
    --set rustfs.deploy=false \
    --set clickhouse.deploy=false \
    --set langfuse.deploy=false \
    --set agentSandbox.deploy=false \
    --set agentSandbox.controller.deploy=false \
    --set agentSandbox.runner.prewarm.enabled=false \
    --set-string postgres.host=postgres.example.com \
    --set-string valkey.host=valkey.example.com \
    --wait --timeout 180s
}

# Trimmed Curie: Collector plus the metrics overlay. No application workloads.
apply_curie --overlay

COLLECTOR="$(kubectl get deploy -n "$CURIE_NS" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep otel-collector | head -1)"
[[ -n "$COLLECTOR" ]] || fail "Collector Deployment was not found in $CURIE_NS"
kubectl rollout status deployment/"$COLLECTOR" -n "$CURIE_NS" --timeout=180s
PROM_SVC="$(kubectl get svc -n "$OBS_NS" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -E 'server$' | head -1)"
[[ -n "$PROM_SVC" ]] || fail "Prometheus server Service was not found in $OBS_NS"
kubectl rollout status deployment/"$PROM_SVC" -n "$OBS_NS" --timeout=180s || \
  kubectl rollout status statefulset/"$PROM_SVC" -n "$OBS_NS" --timeout=180s || true

emit_otlp() {
  local accepted="$1" failed="$2" rpc="$3" reply="$4"
  kubectl delete pod t2428-emit -n "$CURIE_NS" --ignore-not-found --wait=true --timeout=60s || true
  kubectl run "t2428-emit" -n "$CURIE_NS" --restart=Never --image=curlimages/curl:8.12.1 \
    --labels="app.kubernetes.io/name=curie,app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=t2428-emit" \
    --command -- sh -c "sleep 3600"
  kubectl wait --for=condition=Ready pod/t2428-emit -n "$CURIE_NS" --timeout=90s
  python3 - "$accepted" "$failed" "$rpc" "$reply" "$TMP/otlp.json" <<'PY'
import json, sys, time
accepted, failed, rpc, reply = (int(x) for x in sys.argv[1:5])
out = sys.argv[5]
now_ns = time.time_ns()

def sum_metric(name, value, attrs):
    return {
        "name": name,
        "unit": "{count}",
        "sum": {
            "aggregationTemporality": 2,
            "isMonotonic": True,
            "dataPoints": [{
                "asInt": str(value),
                "timeUnixNano": str(now_ns),
                "attributes": [
                    {"key": key, "value": {"stringValue": val}}
                    for key, val in attrs.items()
                ],
            }],
        },
    }

def gauge_metric(name, value, attrs):
    return {
        "name": name,
        "unit": "s",
        "gauge": {
            "dataPoints": [{
                "asDouble": float(value),
                "timeUnixNano": str(now_ns),
                "attributes": [
                    {"key": key, "value": {"stringValue": val}}
                    for key, val in attrs.items()
                ],
            }],
        },
    }

payload = {
    "resourceMetrics": [{
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "curie-worker"}},
            ]
        },
        "scopeMetrics": [{
            "metrics": [
                sum_metric("curie.turn.accepted", accepted, {
                    "service.name": "curie-worker",
                    "source": "worker",
                    "outcome": "accepted",
                }),
                sum_metric("curie.turn.completed", failed, {
                    "service.name": "curie-worker",
                    "source": "worker",
                    "outcome": "classified_failure",
                }),
                sum_metric("curie.queue.enqueue", accepted, {
                    "service.name": "curie-worker",
                    "source": "worker",
                    "outcome": "success",
                }),
                gauge_metric("curie.queue.message.age", 12, {
                    "service.name": "curie-worker",
                    "source": "worker",
                    "outcome": "pending",
                }),
                sum_metric("curie.runner.rpc.result", rpc, {
                    "service.name": "curie-worker",
                    "operation": "event",
                    "role": "client",
                    "outcome": "success",
                }),
                sum_metric("curie.reply.delivery", reply, {
                    "service.name": "curie-worker",
                    "operation": "post",
                    "role": "client",
                    "outcome": "success",
                }),
            ]
        }],
    }]
}
Path = __import__("pathlib").Path
Path(out).write_text(json.dumps(payload))
PY
  kubectl exec -i t2428-emit -n "$CURIE_NS" -- \
    curl -fsS -X POST -H "Content-Type: application/json" \
    --data-binary @- \
    "http://$COLLECTOR:4318/v1/metrics" <"$TMP/otlp.json"
}

query_prom() {
  local expr="$1"
  kubectl exec t2428-emit -n "$CURIE_NS" -- \
    curl -fsS --get "http://$PROM_SVC.$OBS_NS.svc.cluster.local/api/v1/query" \
    --data-urlencode "query=$expr"
}

wait_query() {
  local expr="$1" deadline=$((SECONDS + 120)) result=""
  while (( SECONDS < deadline )); do
    result="$(query_prom "$expr" || true)"
    if python3 -c 'import json,sys; d=json.loads(sys.argv[1]); raise SystemExit(0 if d.get("status")=="success" and d.get("data",{}).get("result") else 1)' "$result" 2>/dev/null; then
      echo "$result"
      return 0
    fi
    sleep 3
  done
  echo "last prometheus payload: $result" >&2
  fail "Prometheus query never returned series for $expr"
}

emit_otlp 3 1 3 3

echo "WAIT retained Curie series"
for expr in \
  'curie_turn_accepted_total' \
  'curie_queue_enqueue_total or curie_queue_enqueue' \
  'curie_runner_rpc_result_total or curie_runner_rpc_result' \
  'curie_reply_delivery_total or curie_reply_delivery'
do
  wait_query "$expr" >/dev/null
  echo "PASS retained: $expr"
done

# Negative: break export, prove absent-data.
apply_curie

# Fresh series stop. Absent() becomes true once the last sample ages out of
# evaluation; also assert the Collector config no longer lists the exporter.
CM="$(kubectl get cm -n "$CURIE_NS" -o name | grep otel-collector | head -1)"
kubectl get -n "$CURIE_NS" "$CM" -o jsonpath='{.data.collector-config\.yaml}' \
  | python3 -c '
import sys, yaml
c = yaml.safe_load(sys.stdin)
exporters = c["service"]["pipelines"]["metrics"]["exporters"]
assert "prometheusremotewrite/soak" not in exporters, exporters
assert "nop/metrics" in exporters, exporters
print("PASS: broken export removed prometheusremotewrite/soak")
'

# Restore overlay and confirm series can be written again.
apply_curie --overlay

kubectl delete pod t2428-emit -n "$CURIE_NS" --wait=true --timeout=60s || true
emit_otlp 5 2 5 5
wait_query 'curie_turn_accepted_total' >/dev/null
echo "PASS: restored overlay retains Curie series again"

RULES="$(query_prom 'count(ALERTS) or vector(0)')"
echo "prometheus ALERTS query: $RULES"
echo "PASS: disposable metrics-alerts runtime completed"
