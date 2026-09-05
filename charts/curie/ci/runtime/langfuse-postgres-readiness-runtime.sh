#!/usr/bin/env bash
# Runtime regression for #1853. It lives below charts/curie/ci/runtime because
# chart-check discovers only top-level assertion scripts, while this script
# creates and removes a real Helm release. Its nested chart-CI path still lets
# curie dev verify-fix-pin route it as a chart runtime selector.
set -euo pipefail

NAMESPACE="${LANGFUSE_POSTGRES_NAMESPACE:-}"
RELEASE="${LANGFUSE_POSTGRES_RELEASE:-}"
POSTGRES_IMAGE="${LANGFUSE_POSTGRES_IMAGE:-}"
CHART=""
OBSERVE_SECONDS="${LANGFUSE_POSTGRES_OBSERVE_SECONDS:-20}"
POLL_SECONDS=2
NAMESPACE_CREATED=0
CLEANUP_STARTED=0
FORCE=0
HELM_INSTALL_PID=0
INVALID_PASSWORD="curie-readiness-intentional-invalid-password"
TMP_DIR="$(mktemp -d)"

usage() {
  cat <<'EOF'
Usage: bash charts/curie/ci/runtime/langfuse-postgres-readiness-runtime.sh \
  --namespace <unique-ns> --release <unique-release> \
  --postgres-image <distinct-delayed-image> [--chart <path>] [--force]

Installs the real chart into a namespace that must not already exist. The
Postgres image must be a caller-built, uniquely tagged image from
postgres-readiness-delay.Dockerfile and must already be available to the
cluster. LANGFUSE_POSTGRES_NAMESPACE, LANGFUSE_POSTGRES_RELEASE, and
LANGFUSE_POSTGRES_IMAGE provide equivalent defaults for verify-fix-pin. The
harness normally accepts only kind-*, k3d-*, and minikube contexts and always
tears its namespace down. --force overrides only the context-name check; an
existing namespace is still refused.
EOF
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || {
    echo "missing value for $1" >&2
    usage >&2
    exit 2
  }
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) need_value "$@"; NAMESPACE="$2"; shift 2 ;;
    --release) need_value "$@"; RELEASE="$2"; shift 2 ;;
    --postgres-image) need_value "$@"; POSTGRES_IMAGE="$2"; shift 2 ;;
    --chart) need_value "$@"; CHART="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$NAMESPACE" && -n "$RELEASE" && -n "$POSTGRES_IMAGE" ]] || {
  usage >&2
  exit 2
}
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#NAMESPACE} -le 63 ]] || {
  echo "namespace must be a DNS label of at most 63 characters" >&2
  exit 2
}
[[ "$RELEASE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#RELEASE} -le 53 ]] || {
  echo "release must be a lowercase Helm name of at most 53 characters" >&2
  exit 2
}
# The chart default (charts/curie/values.yaml postgres.image) is the one image
# this fixture must NOT be: the whole point is a build that starts late (#2319).
CHART_DEFAULT_POSTGRES_IMAGE="postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
[[ "$POSTGRES_IMAGE" != "$CHART_DEFAULT_POSTGRES_IMAGE" && "$POSTGRES_IMAGE" != *:latest ]] || {
  echo "--postgres-image must be a distinct, non-latest delayed fixture tag" >&2
  exit 2
}
[[ "$OBSERVE_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  echo "LANGFUSE_POSTGRES_OBSERVE_SECONDS must be a positive integer" >&2
  exit 2
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$CHART" ]]; then
  CHART="$(cd "$SCRIPT_DIR/../.." && pwd)"
elif [[ "$CHART" != /* ]]; then
  CHART="$(cd "$CHART" && pwd)"
fi

banner() { echo; echo "== $* =="; }
fail() { echo "FAIL: $*" >&2; exit 1; }

sanitize() {
  sed -E \
    -e 's#(postgres(ql)?://[^:/@[:space:]]+:)[^@[:space:]]+@#\1<redacted>@#g' \
    -e 's#(([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd]|[Tt][Oo][Kk][Ee][Nn]|[Ss][Ee][Cc][Rr][Ee][Tt]|[Aa][Pp][Ii][_-]?[Kk][Ee][Yy]|[Aa][Cc][Cc][Ee][Ss][Ss][_-]?[Kk][Ee][Yy])=)[^[:space:],;]+#\1<redacted>#g' \
    -e "s/${INVALID_PASSWORD}/<redacted>/g"
}

component_pod() {
  local component="$1" excluded_uid="${2:-}" pods_json
  if ! pods_json="$(kubectl get pods -n "$NAMESPACE" \
    -l "app.kubernetes.io/component=$component" -o json)"; then
    echo "kubectl failed listing component=$component pods in namespace $NAMESPACE" >&2
    return 2
  fi
  printf '%s' "$pods_json" | python3 /dev/fd/3 "$excluded_uid" 3<<'PY'
import json
import sys

excluded = sys.argv[1]
items = json.load(sys.stdin).get("items", [])
items.sort(key=lambda item: item.get("metadata", {}).get("creationTimestamp", ""), reverse=True)
for item in items:
    meta = item.get("metadata", {})
    if meta.get("deletionTimestamp") or meta.get("uid") == excluded:
        continue
    print(f"{meta.get('name', '')}|{meta.get('uid', '')}")
    break
PY
}

wait_for_component_pod() {
  local component="$1" excluded_uid="${2:-}" timeout="${3:-240}" waited=0 found rc
  while (( waited < timeout )); do
    set +e
    found="$(component_pod "$component" "$excluded_uid")"
    rc=$?
    set -e
    (( rc == 0 )) || return "$rc"
    if [[ "$found" == *"|"* && -n "${found%%|*}" && -n "${found#*|}" ]]; then
      printf '%s\n' "$found"
      return 0
    fi
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
  return 1
}

component_deployment() {
  local pod="$1" replica_set
  if ! replica_set="$(kubectl get pod "$pod" -n "$NAMESPACE" \
    -o jsonpath='{.metadata.ownerReferences[?(@.kind=="ReplicaSet")].name}')"; then
    echo "kubectl failed reading owner of pod $NAMESPACE/$pod" >&2
    return 2
  fi
  [[ -n "$replica_set" ]] || return 1
  if ! kubectl get replicaset "$replica_set" -n "$NAMESPACE" \
    -o jsonpath='{.metadata.ownerReferences[?(@.kind=="Deployment")].name}'; then
    echo "kubectl failed reading owner of ReplicaSet $NAMESPACE/$replica_set" >&2
    return 2
  fi
}

pod_snapshot() {
  local pod="$1" pod_json
  if ! pod_json="$(kubectl get pod "$pod" -n "$NAMESPACE" -o json)"; then
    echo "kubectl failed fetching pod snapshot for $NAMESPACE/$pod" >&2
    return 2
  fi
  printf '%s' "$pod_json" | python3 /dev/fd/3 3<<'PY'
import json
import sys

pod = json.load(sys.stdin)
spec = pod.get("spec", {})
status = pod.get("status", {})

def container_status(statuses, name):
    entry = next((item for item in statuses or [] if item.get("name") == name), {})
    state = entry.get("state", {})
    state_name = next((key for key in ("waiting", "running", "terminated") if key in state), "missing")
    terminated = state.get("terminated", {}) or entry.get("lastState", {}).get("terminated", {})
    exit_code = terminated.get("exitCode", "")
    return str(entry.get("restartCount", "")), state_name, str(exit_code), str(entry.get("ready", False)).lower()

init_names = {item.get("name") for item in spec.get("initContainers", [])}
init_restart, init_state, init_exit, _ = container_status(
    status.get("initContainerStatuses"), "wait-for-postgres"
)
web_restart, web_state, web_exit, web_ready = container_status(
    status.get("containerStatuses"), "langfuse-web"
)
fields = [
    "yes" if "wait-for-postgres" in init_names else "no",
    init_restart,
    init_state,
    init_exit,
    web_restart,
    web_state,
    web_exit,
    web_ready,
]
print("|".join(fields))
PY
}

read_snapshot() {
  local pod="$1" snapshot
  if ! snapshot="$(pod_snapshot "$pod")"; then
    fail "could not read pod status for $NAMESPACE/$pod"
  fi
  IFS='|' read -r INIT_PRESENT INIT_RESTART INIT_STATE INIT_EXIT \
    WEB_RESTART WEB_STATE WEB_EXIT WEB_READY <<<"$snapshot"
}

pod_ready() {
  local pod="$1" ready
  if ! ready="$(kubectl get pod "$pod" -n "$NAMESPACE" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"; then
    echo "kubectl failed reading Ready condition for $NAMESPACE/$pod" >&2
    return 2
  fi
  printf '%s' "$ready"
}

assert_no_backoff() {
  local pod="$1" count events_json
  if ! events_json="$(kubectl get events -n "$NAMESPACE" -o json)"; then
    echo "kubectl failed listing events in namespace $NAMESPACE while checking pod $pod" >&2
    return 2
  fi
  count="$(printf '%s' "$events_json" | python3 /dev/fd/3 "$pod" 3<<'PY'
import json
import sys

pod = sys.argv[1]
events = json.load(sys.stdin).get("items", [])
print(sum(
    1 for event in events
    if event.get("reason") == "BackOff"
    and event.get("involvedObject", {}).get("kind") == "Pod"
    and event.get("involvedObject", {}).get("name") == pod
))
PY
)"
  [[ "$count" == "0" ]] || fail "pod $pod emitted $count BackOff event(s)"
}

safe_pod_logs() {
  local pod="$1" container="$2"
  echo "--- $pod/$container current logs (sanitized)"
  kubectl logs "$pod" -n "$NAMESPACE" -c "$container" --tail=120 2>&1 | sanitize || true
  echo "--- $pod/$container previous logs (sanitized)"
  kubectl logs "$pod" -n "$NAMESPACE" -c "$container" --previous --tail=120 2>&1 | sanitize || true
}

diagnostics() {
  banner "DIAGNOSTICS (credentials redacted)"
  if [[ -n "${HELM_INSTALL_LOG:-}" && -f "$HELM_INSTALL_LOG" ]]; then
    echo "--- helm install output (sanitized)"
    tail -120 "$HELM_INSTALL_LOG" | sanitize || true
  fi
  kubectl get pods -n "$NAMESPACE" -o wide 2>&1 | sanitize || true
  kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>&1 | tail -80 | sanitize || true
  local pod
  pod="$(component_pod langfuse-web | cut -d'|' -f1 || true)"
  [[ -z "$pod" ]] || {
    kubectl describe pod "$pod" -n "$NAMESPACE" 2>&1 | sanitize || true
    safe_pod_logs "$pod" langfuse-web
    safe_pod_logs "$pod" wait-for-postgres
  }
  pod="$(component_pod langfuse-worker | cut -d'|' -f1 || true)"
  [[ -z "$pod" ]] || safe_pod_logs "$pod" langfuse-worker
}

cleanup() {
  local rc="${1:-$?}" cleanup_failed=0
  (( CLEANUP_STARTED == 0 )) || return
  CLEANUP_STARTED=1
  trap - EXIT INT TERM
  set +e
  if (( rc != 0 && NAMESPACE_CREATED == 1 )); then
    diagnostics
  fi
  if (( HELM_INSTALL_PID > 0 )) && kill -0 "$HELM_INSTALL_PID" >/dev/null 2>&1; then
    kill "$HELM_INSTALL_PID" >/dev/null 2>&1 || true
    wait "$HELM_INSTALL_PID" >/dev/null 2>&1 || true
  fi
  if (( NAMESPACE_CREATED == 1 )); then
    banner "TEARDOWN namespace=$NAMESPACE release=$RELEASE"
    helm uninstall "$RELEASE" -n "$NAMESPACE" --no-hooks >/dev/null 2>&1 || true
    kubectl delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1
    if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
      echo "FAIL: disposable namespace $NAMESPACE still exists after teardown" >&2
      cleanup_failed=1
    else
      echo "teardown confirmed: namespace $NAMESPACE is absent"
    fi
  fi
  rm -rf "$TMP_DIR"
  if (( cleanup_failed == 1 && rc == 0 )); then
    rc=1
  fi
  exit "$rc"
}
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM

CURRENT_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
[[ -n "$CURRENT_CONTEXT" ]] || fail "kubectl has no current context"
[[ "$CURRENT_CONTEXT" != "k8scratch" ]] || fail "refusing shared k8scratch even with --force"
case "$CURRENT_CONTEXT" in
  kind-*|k3d-*|minikube|minikube-*) ;;
  *)
    (( FORCE == 1 )) || fail "context '$CURRENT_CONTEXT' is not an explicitly disposable kind-*/k3d-*/minikube context (override with --force)"
    ;;
esac

banner "PRECHECK context=$CURRENT_CONTEXT namespace=$NAMESPACE release=$RELEASE image=$POSTGRES_IMAGE"
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  fail "namespace $NAMESPACE already exists; choose a new isolated namespace"
fi
if kubectl create namespace "$NAMESPACE" >/dev/null; then
  NAMESPACE_CREATED=1
else
  fail "could not atomically create namespace $NAMESPACE"
fi
kubectl label namespace "$NAMESPACE" curie-readiness-harness=owned >/dev/null

CHART_VALUES=(
  --set-string postgres.image="$POSTGRES_IMAGE"
  --set global.imagePullPolicy=IfNotPresent
  --set security.allowDevDefaults=true
  --set postgres.persistence.enabled=false
  --set valkey.persistence.enabled=false
  --set clickhouse.persistence.enabled=false
  --set rustfs.persistence.enabled=false
  --set api.deploy=false
  --set dispatcher.deploy=false
  --set worker.deploy=false
  --set ui.deploy=false
  --set otelCollector.deploy=false
  --set inference.deploy=false
  --set agentSandbox.deploy=false
  --set agentSandbox.controller.deploy=false
  --set langfuse.modelPricing.enabled=false
  --set preflights.avxCheck.enabled=false
  --set preflights.networkPolicyProbe.enabled=false
  --set preflights.controllerReady.enabled=false
  --set priorityClasses.platform.create=false
  --set-string priorityClasses.platform.name=
  --set priorityClasses.sandbox.create=false
  --set-string priorityClasses.sandbox.name=
)

banner "INSTALL real chart consumer with delayed Postgres"
HELM_INSTALL_LOG="$TMP_DIR/helm-install.log"
helm install "$RELEASE" "$CHART" -n "$NAMESPACE" --skip-crds "${CHART_VALUES[@]}" \
  >"$HELM_INSTALL_LOG" 2>&1 &
HELM_INSTALL_PID=$!

WEB_REF="$(wait_for_component_pod langfuse-web "" 300)" || fail "Langfuse web pod was not created"
WEB_POD="${WEB_REF%%|*}"
WEB_UID="${WEB_REF#*|}"
POSTGRES_REF="$(wait_for_component_pod postgres "" 180)" || fail "Postgres pod was not created"
POSTGRES_POD="${POSTGRES_REF%%|*}"

banner "ASSERT bounded gate is running while Postgres is unavailable"
waited=0
while (( waited < 180 )); do
  read_snapshot "$WEB_POD"
  if [[ "$INIT_PRESENT" == "no" ]]; then
    fail "live Langfuse web pod has no wait-for-postgres init container"
  fi
  if [[ "$INIT_STATE" == "running" && "$(pod_ready "$POSTGRES_POD")" != "True" ]]; then
    break
  fi
  sleep "$POLL_SECONDS"
  waited=$((waited + POLL_SECONDS))
done
[[ "$INIT_STATE" == "running" ]] || fail "wait-for-postgres never reached Running before Postgres readiness"
[[ "$(pod_ready "$POSTGRES_POD")" != "True" ]] || fail "delayed fixture became ready before the observation window"

MIN_PRE_READY_SAMPLES=3
PRE_READY_SAMPLES=0
OBSERVE_STARTED=$SECONDS
while (( PRE_READY_SAMPLES < MIN_PRE_READY_SAMPLES )); do
  (( SECONDS - OBSERVE_STARTED < OBSERVE_SECONDS )) || \
    fail "pre-ready sampling exceeded ${OBSERVE_SECONDS}s after $PRE_READY_SAMPLES samples"
  current_ref="$(component_pod langfuse-web)"
  [[ "${current_ref#*|}" == "$WEB_UID" ]] || fail "Langfuse web pod was replaced before Postgres readiness"
  if [[ "$(pod_ready "$POSTGRES_POD")" == "True" ]]; then
    fail "delayed fixture became ready after only $PRE_READY_SAMPLES pre-ready samples"
  fi
  read_snapshot "$WEB_POD"
  [[ "$INIT_PRESENT" == "yes" && "$INIT_STATE" == "running" ]] || fail "readiness init stopped waiting before Postgres was ready"
  [[ "${INIT_RESTART:-0}" == "0" ]] || fail "readiness init restarted before Postgres readiness"
  [[ "${WEB_RESTART:-0}" == "0" ]] || fail "Langfuse web restarted before Postgres readiness"
  [[ "$WEB_STATE" == "waiting" || "$WEB_STATE" == "missing" ]] || fail "Langfuse web started before its readiness init completed"
  PRE_READY_SAMPLES=$((PRE_READY_SAMPLES + 1))
  echo "pre-ready sample $PRE_READY_SAMPLES: init=running initRestarts=0 web=${WEB_STATE} webRestarts=0"
  if (( PRE_READY_SAMPLES < MIN_PRE_READY_SAMPLES )); then
    sleep "$POLL_SECONDS"
  fi
done
assert_no_backoff "$WEB_POD"
echo "pre-ready observation: samples=$PRE_READY_SAMPLES BackOff=0"

if ! wait "$HELM_INSTALL_PID"; then
  tail -120 "$HELM_INSTALL_LOG" | sanitize >&2
  HELM_INSTALL_PID=0
  fail "helm install failed while the readiness window was being observed"
fi
HELM_INSTALL_PID=0
echo "helm install and normal post-install hooks completed"

banner "WAIT for Postgres and Langfuse web readiness"
kubectl wait pod "$POSTGRES_POD" -n "$NAMESPACE" --for=condition=Ready --timeout=300s
POSTGRES_RESTART="$(kubectl get pod "$POSTGRES_POD" -n "$NAMESPACE" \
  -o jsonpath='{.status.containerStatuses[?(@.name=="postgres")].restartCount}')" || \
  fail "kubectl failed reading Postgres restartCount for $NAMESPACE/$POSTGRES_POD"
[[ "$POSTGRES_RESTART" == "0" ]] || fail "delayed Postgres restarted $POSTGRES_RESTART time(s); fixture exceeded its liveness budget"
WEB_DEPLOYMENT="$(component_deployment "$WEB_POD")"
[[ -n "$WEB_DEPLOYMENT" ]] || fail "Langfuse web Deployment not found"
kubectl rollout status deployment/"$WEB_DEPLOYMENT" -n "$NAMESPACE" --timeout=360s
current_ref="$(component_pod langfuse-web)"
[[ "${current_ref#*|}" == "$WEB_UID" ]] || fail "Langfuse web pod was replaced while recovering"
read_snapshot "$WEB_POD"
[[ "$INIT_STATE" == "terminated" && "$INIT_EXIT" == "0" && "${INIT_RESTART:-0}" == "0" ]] || \
  fail "readiness init did not complete once with exit 0"
[[ "$WEB_READY" == "true" && "${WEB_RESTART:-0}" == "0" ]] || \
  fail "Langfuse web did not become Ready with zero restarts"
assert_no_backoff "$WEB_POD"
echo "positive: postgres=Ready postgresRestarts=0 initExit=0 initRestarts=0 web=Ready webRestarts=0 BackOff=0"

WORKER_REF="$(component_pod langfuse-worker || true)"
if [[ "$WORKER_REF" == *"|"* ]]; then
  WORKER_POD="${WORKER_REF%%|*}"
  WORKER_RESTART="$(kubectl get pod "$WORKER_POD" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[?(@.name=="langfuse-worker")].restartCount}' 2>/dev/null || true)"
  WORKER_REASON="$(kubectl get pod "$WORKER_POD" -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[?(@.name=="langfuse-worker")].state.waiting.reason}' 2>/dev/null || true)"
  echo "informational sibling: langfuse-worker restarts=${WORKER_RESTART:-unknown} waitingReason=${WORKER_REASON:-none}"
fi

banner "NEGATIVE invalid credentials pass readiness and fail in real Langfuse web"
SECRET_NAME="$(kubectl get deployment "$WEB_DEPLOYMENT" -n "$NAMESPACE" -o json |
  python3 /dev/fd/3 3<<'PY'
import json
import sys

deployment = json.load(sys.stdin)
containers = deployment["spec"]["template"]["spec"]["containers"]
web = next(container for container in containers if container.get("name") == "langfuse-web")
password = next(item for item in web.get("env", []) if item.get("name") == "POSTGRES_PASSWORD")
print(password["valueFrom"]["secretKeyRef"]["name"])
PY
)"
helm upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" --reuse-values \
  --set-string postgres.auth.password="$INVALID_PASSWORD"
EXPECTED_SECRET="$INVALID_PASSWORD" kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o json |
  EXPECTED_SECRET="$INVALID_PASSWORD" python3 /dev/fd/3 3<<'PY'
import base64
import json
import os
import sys

secret = json.load(sys.stdin)
actual = base64.b64decode(secret["data"]["postgresPassword"]).decode()
if actual != os.environ["EXPECTED_SECRET"]:
    print("rotated Postgres Secret did not contain the intentional invalid credential", file=sys.stderr)
    sys.exit(1)
print("negative armed: Postgres Secret changed (value redacted), running database retained its original credential")
PY

kubectl delete pod "$WEB_POD" -n "$NAMESPACE" --wait=true >/dev/null
NEGATIVE_REF="$(wait_for_component_pod langfuse-web "$WEB_UID" 180)" || fail "replacement Langfuse web pod was not created"
NEGATIVE_POD="${NEGATIVE_REF%%|*}"

waited=0
while (( waited < 180 )); do
  read_snapshot "$NEGATIVE_POD"
  [[ "$INIT_PRESENT" == "yes" ]] || fail "negative pod has no readiness init"
  [[ "${INIT_RESTART:-0}" == "0" ]] || fail "negative readiness init restarted"
  if [[ "$INIT_STATE" == "terminated" && "$INIT_EXIT" == "0" ]]; then
    break
  fi
  [[ -z "$INIT_EXIT" || "$INIT_EXIT" == "0" ]] || fail "negative readiness init exited nonzero instead of handing off"
  sleep "$POLL_SECONDS"
  waited=$((waited + POLL_SECONDS))
done
[[ "$INIT_STATE" == "terminated" && "$INIT_EXIT" == "0" ]] || fail "invalid credentials remained hidden behind the readiness init"

waited=0
while (( waited < 180 )); do
  read_snapshot "$NEGATIVE_POD"
  if [[ "${WEB_RESTART:-0}" =~ ^[1-9][0-9]*$ && -n "$WEB_EXIT" && "$WEB_EXIT" != "0" ]]; then
    break
  fi
  sleep "$POLL_SECONDS"
  waited=$((waited + POLL_SECONDS))
done
[[ "${WEB_RESTART:-0}" =~ ^[1-9][0-9]*$ && -n "$WEB_EXIT" && "$WEB_EXIT" != "0" ]] || \
  fail "real Langfuse web did not exit nonzero after the invalid credential was supplied"
[[ "$WEB_READY" != "true" ]] || fail "invalid-credential Langfuse web unexpectedly became Ready"

AUTH_LOG="$TMP_DIR/langfuse-auth.log"
kubectl logs "$NEGATIVE_POD" -n "$NAMESPACE" -c langfuse-web >"$AUTH_LOG" 2>&1 || true
kubectl logs "$NEGATIVE_POD" -n "$NAMESPACE" -c langfuse-web --previous >>"$AUTH_LOG" 2>&1 || true
if ! grep -Eiq 'P1000|authentication failed|password authentication failed|database credentials.+not valid|provided database credentials' "$AUTH_LOG"; then
  tail -80 "$AUTH_LOG" | sanitize >&2
  fail "Langfuse exited nonzero, but its previous log did not identify an authentication/migration failure"
fi
grep -Ei 'P1000|authentication failed|password authentication failed|database credentials.+not valid|provided database credentials' "$AUTH_LOG" |
  tail -5 | sanitize
echo "negative: initExit=0 initRestarts=0 webExit=$WEB_EXIT webRestarts=$WEB_RESTART authFailure=observed"

banner "NEGATIVE unreachable Postgres exhausts the bounded readiness gate"
EXHAUSTION_OLD_UID="${NEGATIVE_REF#*|}"
helm upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" --reuse-values \
  --set postgres.deploy=false \
  --set-string postgres.host=postgres-readiness.invalid \
  --set langfuse.web.postgresReadiness.attempts=2 \
  --set langfuse.web.postgresReadiness.intervalSeconds=1 \
  --set langfuse.web.postgresReadiness.probeTimeoutSeconds=1

EXHAUSTION_REF="$(wait_for_component_pod langfuse-web "$EXHAUSTION_OLD_UID" 30 || true)"
if [[ "$EXHAUSTION_REF" != *"|"* ]]; then
  kubectl delete pod "$NEGATIVE_POD" -n "$NAMESPACE" --wait=true >/dev/null 2>&1 || true
  EXHAUSTION_REF="$(wait_for_component_pod langfuse-web "$EXHAUSTION_OLD_UID" 120)" || \
    fail "fresh Langfuse web pod was not created for readiness exhaustion"
fi
EXHAUSTION_POD="${EXHAUSTION_REF%%|*}"

waited=0
while (( waited < 45 )); do
  read_snapshot "$EXHAUSTION_POD"
  [[ "$INIT_PRESENT" == "yes" ]] || fail "exhaustion pod has no readiness init"
  [[ "$WEB_STATE" == "waiting" || "$WEB_STATE" == "missing" ]] || \
    fail "Langfuse web started while its readiness gate was exhausting"
  [[ "${WEB_RESTART:-0}" == "0" && -z "$WEB_EXIT" && "$WEB_READY" != "true" ]] || \
    fail "Langfuse web ran before the unreachable-Postgres gate completed"
  if [[ -n "$INIT_EXIT" && "$INIT_EXIT" != "0" ]]; then
    break
  fi
  sleep 1
  waited=$((waited + 1))
done
[[ -n "$INIT_EXIT" && "$INIT_EXIT" != "0" ]] || \
  fail "wait-for-postgres did not exit nonzero within the 45s exhaustion deadline"
[[ "$WEB_STATE" == "waiting" || "$WEB_STATE" == "missing" ]] || \
  fail "Langfuse web started after readiness exhaustion"
[[ "${WEB_RESTART:-0}" == "0" && -z "$WEB_EXIT" && "$WEB_READY" != "true" ]] || \
  fail "Langfuse web process started during readiness exhaustion"

EXHAUSTION_LOG="$TMP_DIR/postgres-readiness-exhaustion.log"
kubectl logs "$EXHAUSTION_POD" -n "$NAMESPACE" -c wait-for-postgres >"$EXHAUSTION_LOG" 2>&1 || true
kubectl logs "$EXHAUSTION_POD" -n "$NAMESPACE" -c wait-for-postgres --previous >>"$EXHAUSTION_LOG" 2>&1 || true
if ! grep -Eq '^Postgres readiness exhausted for .+ after 2 attempts: pg_isready=[0-3] ' "$EXHAUSTION_LOG"; then
  tail -40 "$EXHAUSTION_LOG" | sanitize >&2
  fail "readiness init exited nonzero without the credential-free exhaustion diagnostic"
fi
grep -E '^Postgres readiness exhausted for .+ after 2 attempts: pg_isready=[0-3] ' "$EXHAUSTION_LOG" | tail -1 | sanitize
echo "exhaustion negative: initExit=$INIT_EXIT webState=$WEB_STATE webRestarts=0 webStarted=false"

banner "PASS delayed Postgres caused no Langfuse web restart/BackOff; readiness recovered; invalid credentials remained terminal; unreachable Postgres exhausted the bounded gate"
