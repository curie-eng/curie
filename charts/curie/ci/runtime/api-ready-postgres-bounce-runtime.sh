#!/usr/bin/env bash
# Runtime regression for reopened #1582. Proves on an isolated cluster that
# GET /ready fails in the same window GET /agents 500s while Postgres cannot
# serve list-agents, that GET /health stays shallow, that a longer outage past
# the 120s readiness cutoff withdraws the API from Service endpoints, and that
# recovery keeps the seeded agent without a crash loop.
#
# Lives under charts/curie/ci/runtime because chart-check discovers only
# top-level assertion scripts, while this script creates and removes a real
# Helm release.
set -euo pipefail

KUBE_CONTEXT="${API_READY_CONTEXT:-k8scratch}"
NAMESPACE="${API_READY_NAMESPACE:-test-1582-ready-postgres}"
RELEASE="${API_READY_RELEASE:-curie1582}"
API_IMAGE="${API_READY_API_IMAGE:-}"
CHART=""
FORCE=0
NAMESPACE_CREATED=0
CLEANUP_STARTED=0
TMP_DIR="$(mktemp -d)"
API_KEY="curie-dev-key"
AGENT_NAME="acme-bot"
CHANNEL_ADDRESS="C0EXAMPLE1"
PLATFORM_PRIORITY_CLASS=""
SANDBOX_PRIORITY_CLASS=""
BOUNCE_SECONDS=45
LONG_OUTAGE_SECONDS=130
POLL_SECONDS=1

usage() {
  cat <<'EOF'
Usage: bash charts/curie/ci/runtime/api-ready-postgres-bounce-runtime.sh \
  --api-image <imported-image> [--context k8scratch] \
  [--namespace test-1582-ready-postgres] [--release curie1582] \
  [--chart <path>] [--force]

Installs a trimmed chart slice (API + Postgres + Valkey + RustFS) into a
namespace that must not already exist. The API image must already be present
on the cluster. The harness tears the namespace down on exit.
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
    --context) need_value "$@"; KUBE_CONTEXT="$2"; shift 2 ;;
    --namespace) need_value "$@"; NAMESPACE="$2"; shift 2 ;;
    --release) need_value "$@"; RELEASE="$2"; shift 2 ;;
    --api-image) need_value "$@"; API_IMAGE="$2"; shift 2 ;;
    --chart) need_value "$@"; CHART="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$API_IMAGE" ]] || {
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

case "$NAMESPACE" in
  curie|curie-email*)
    echo "refusing namespace $NAMESPACE" >&2
    exit 2
    ;;
esac
[[ "$NAMESPACE" != *ProdCurietechAi* && "$NAMESPACE" != *StagingCurietechAi* ]] || {
  echo "refusing namespace $NAMESPACE" >&2
  exit 2
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$CHART" ]]; then
  CHART="$(cd "$SCRIPT_DIR/../.." && pwd)"
elif [[ "$CHART" != /* ]]; then
  CHART="$(cd "$CHART" && pwd)"
fi

if [[ "$RELEASE" == *curie* ]]; then
  FULLNAME="$RELEASE"
else
  FULLNAME="$RELEASE-curie"
fi
PLATFORM_PRIORITY_CLASS="${NAMESPACE}-platform"
SANDBOX_PRIORITY_CLASS="${NAMESPACE}-sandbox"
POSTGRES_STS="$FULLNAME-postgres"
API_DEPLOY="$FULLNAME-api"
API_SERVICE="$FULLNAME-api"

kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }
hm() { helm --kube-context "$KUBE_CONTEXT" "$@"; }

banner() { echo; echo "== $* =="; }
fail() { echo "FAIL: $*" >&2; exit 1; }

sanitize() {
  sed -E \
    -e 's#(postgres(ql)?://[^:/@[:space:]]+:)[^@[:space:]]+@#\1<redacted>@#g' \
    -e 's#(([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd]|[Tt][Oo][Kk][Ee][Nn]|[Ss][Ee][Cc][Rr][Ee][Tt]|[Aa][Pp][Ii][_-]?[Kk][Ee][Yy]|[Aa][Cc][Cc][Ee][Ss][Ss][_-]?[Kk][Ee][Yy])=)[^[:space:],;]+#\1<redacted>#g'
}

split_image() {
  local image="$1"
  if [[ "$image" == *@sha256:* ]]; then
    echo "digest-pinned --api-image is not supported; pass repository:tag" >&2
    exit 2
  fi
  API_REPOSITORY="${image%:*}"
  API_TAG="${image##*:}"
  [[ "$API_REPOSITORY" != "$image" && -n "$API_TAG" ]] || {
    echo "--api-image must be repository:tag" >&2
    exit 2
  }
}

api_pod() {
  kc get pods -n "$NAMESPACE" -l "app.kubernetes.io/component=api" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

# Prints one HTTP status, or 000 on transport failure.
# python -c (not stdin) because kubectl exec without -i drops a heredoc.
http_status() {
  local path="$1" pod key status
  pod="$(api_pod)"
  [[ -n "$pod" ]] || { echo 000; return 0; }
  key=""
  if [[ "$path" == /agents ]]; then
    key="$API_KEY"
  fi
  status="$(kc exec -n "$NAMESPACE" "$pod" -c api -- python -c '
import sys, urllib.error, urllib.request
path, key = sys.argv[1], sys.argv[2]
req = urllib.request.Request("http://127.0.0.1:8000" + path)
if key:
    req.add_header("X-API-Key", key)
try:
    with urllib.request.urlopen(req, timeout=4) as resp:
        print(resp.status)
except urllib.error.HTTPError as exc:
    print(exc.code)
except Exception:
    print("000")
' "$path" "$key" 2>/dev/null | tail -n 1)"
  [[ -n "$status" ]] || status=000
  echo "$status"
}

http_body() {
  local path="$1" pod
  pod="$(api_pod)"
  [[ -n "$pod" ]] || return 1
  kc exec -n "$NAMESPACE" "$pod" -c api -- python -c '
import sys, urllib.request
path, key = sys.argv[1], sys.argv[2]
req = urllib.request.Request("http://127.0.0.1:8000" + path, headers={"X-API-Key": key})
with urllib.request.urlopen(req, timeout=5) as resp:
    sys.stdout.write(resp.read().decode())
' "$path" "$API_KEY"
}

create_agent() {
  local pod
  pod="$(api_pod)"
  [[ -n "$pod" ]] || fail "API pod missing while seeding $AGENT_NAME"
  kc exec -n "$NAMESPACE" "$pod" -c api -- python -c '
import json, sys, urllib.error, urllib.request
key, name, address = sys.argv[1], sys.argv[2], sys.argv[3]
body = json.dumps({"name": name, "channel": {"kind": "slack", "address": address}}).encode()
req = urllib.request.Request(
    "http://127.0.0.1:8000/agents",
    data=body,
    method="POST",
    headers={"X-API-Key": key, "Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(resp.status)
except urllib.error.HTTPError as exc:
    sys.stderr.write(exc.read().decode())
    raise SystemExit(1)
' "$API_KEY" "$AGENT_NAME" "$CHANNEL_ADDRESS"
}

api_ready_condition() {
  local pod
  pod="$(api_pod)"
  [[ -n "$pod" ]] || { echo ""; return 0; }
  kc get pod "$pod" -n "$NAMESPACE" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

api_restart_count() {
  local pod
  pod="$(api_pod)"
  [[ -n "$pod" ]] || { echo missing; return 0; }
  kc get pod "$pod" -n "$NAMESPACE" \
    -o jsonpath='{.status.containerStatuses[?(@.name=="api")].restartCount}' 2>/dev/null || echo missing
}

ready_endpoint_count() {
  local json
  json="$(kc get endpoints "$API_SERVICE" -n "$NAMESPACE" -o json 2>/dev/null || echo '{}')"
  printf '%s' "$json" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
count = 0
for subset in doc.get("subsets") or []:
    count += len(subset.get("addresses") or [])
print(count)
'
}

assert_no_backoff() {
  local pod="$1" count events_json
  [[ -n "$pod" ]] || return 0
  events_json="$(kc get events -n "$NAMESPACE" -o json)"
  count="$(printf '%s' "$events_json" | python3 -c '
import json, sys
pod = sys.argv[1]
events = json.load(sys.stdin).get("items", [])
print(sum(
    1 for event in events
    if event.get("reason") == "BackOff"
    and event.get("involvedObject", {}).get("kind") == "Pod"
    and event.get("involvedObject", {}).get("name") == pod
))
' "$pod")"
  [[ "$count" == "0" ]] || fail "pod $pod emitted $count BackOff event(s)"
}

diagnostics() {
  banner "DIAGNOSTICS (credentials redacted)"
  if [[ -n "${HELM_INSTALL_LOG:-}" && -f "$HELM_INSTALL_LOG" ]]; then
    echo "--- helm install output (sanitized)"
    tail -120 "$HELM_INSTALL_LOG" | sanitize || true
  fi
  kc get pods -n "$NAMESPACE" -o wide 2>&1 | sanitize || true
  kc get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>&1 | tail -80 | sanitize || true
  local pod
  pod="$(api_pod)"
  [[ -z "$pod" ]] || {
    kc describe pod "$pod" -n "$NAMESPACE" 2>&1 | sanitize || true
    kc logs "$pod" -n "$NAMESPACE" -c api --tail=80 2>&1 | sanitize || true
  }
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
  if (( NAMESPACE_CREATED == 1 )); then
    banner "TEARDOWN namespace=$NAMESPACE release=$RELEASE"
    hm uninstall "$RELEASE" -n "$NAMESPACE" --no-hooks >/dev/null 2>&1 || true
    kc delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1
    if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
      echo "FAIL: disposable namespace $NAMESPACE still exists after teardown" >&2
      cleanup_failed=1
    else
      echo "teardown confirmed: namespace $NAMESPACE is absent"
    fi
    kc delete priorityclass "$PLATFORM_PRIORITY_CLASS" --ignore-not-found >/dev/null 2>&1 || true
    kc delete priorityclass "$SANDBOX_PRIORITY_CLASS" --ignore-not-found >/dev/null 2>&1 || true
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

case "$KUBE_CONTEXT" in
  *ProdCurietechAi*|*StagingCurietechAi*)
    fail "refusing context $KUBE_CONTEXT"
    ;;
  k8scratch|k8|kind-*|k3d-*|minikube|minikube-*) ;;
  *)
    (( FORCE == 1 )) || fail "context '$KUBE_CONTEXT' is not k8scratch/k8/kind-*/k3d-*/minikube (override with --force)"
    ;;
esac

split_image "$API_IMAGE"

banner "PRECHECK context=$KUBE_CONTEXT namespace=$NAMESPACE release=$RELEASE image=$API_IMAGE"
if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
  fail "namespace $NAMESPACE already exists; choose a new isolated namespace"
fi
if kc create namespace "$NAMESPACE" >/dev/null; then
  NAMESPACE_CREATED=1
else
  fail "could not atomically create namespace $NAMESPACE"
fi
kc label namespace "$NAMESPACE" curie-readiness-harness=owned >/dev/null

CHART_VALUES=(
  --set security.allowDevDefaults=true
  --set global.imagePullPolicy=IfNotPresent
  --set-string api.image.repository="$API_REPOSITORY"
  --set-string api.image.tag="$API_TAG"
  --set api.image.pullPolicy=Never
  --set api.deploy=true
  --set dispatcher.deploy=false
  --set worker.deploy=false
  --set ui.deploy=false
  --set langfuse.deploy=false
  --set-string langfuse.host=langfuse.example.com
  --set clickhouse.deploy=false
  --set otelCollector.deploy=false
  --set inference.deploy=false
  --set agentSandbox.deploy=false
  --set agentSandbox.controller.deploy=false
  --set mailAdapter.deploy=false
  --set postgres.persistence.enabled=true
  --set valkey.persistence.enabled=false
  --set rustfs.persistence.enabled=false
  --set clickhouse.persistence.enabled=false
  --set preflights.avxCheck.enabled=false
  --set preflights.networkPolicyProbe.enabled=false
  --set preflights.controllerReady.enabled=false
  --set priorityClasses.platform.create=true
  --set-string priorityClasses.platform.name="$PLATFORM_PRIORITY_CLASS"
  --set priorityClasses.sandbox.create=true
  --set-string priorityClasses.sandbox.name="$SANDBOX_PRIORITY_CLASS"
)

banner "INSTALL trimmed API + Postgres slice"
HELM_INSTALL_LOG="$TMP_DIR/helm-install.log"
if ! hm install "$RELEASE" "$CHART" -n "$NAMESPACE" --skip-crds --no-hooks \
  "${CHART_VALUES[@]}" >"$HELM_INSTALL_LOG" 2>&1; then
  cat "$HELM_INSTALL_LOG" | sanitize
  fail "helm install failed"
fi

banner "WAIT for postgres and API"
kc rollout status "statefulset/$POSTGRES_STS" -n "$NAMESPACE" --timeout=240s
kc rollout status "deployment/$API_DEPLOY" -n "$NAMESPACE" --timeout=300s

waited=0
while (( waited < 180 )); do
  if [[ "$(http_status /ready)" == "200" && "$(http_status /health)" == "200" ]]; then
    break
  fi
  sleep 2
  waited=$((waited + 2))
done
[[ "$(http_status /ready)" == "200" ]] || fail "GET /ready never became 200 after install"
[[ "$(http_status /health)" == "200" ]] || fail "GET /health is not 200 after install"

banner "SEED $AGENT_NAME"
create_agent
agents_body="$(http_body /agents)"
[[ "$agents_body" == *"$AGENT_NAME"* ]] || fail "seeded agent $AGENT_NAME missing from GET /agents"

BASELINE_RESTARTS="$(api_restart_count)"
BASELINE_POD="$(api_pod)"
echo "baseline api pod=$BASELINE_POD restarts=$BASELINE_RESTARTS ready=$(http_status /ready) agents=$(http_status /agents) health=$(http_status /health)"

banner "PHASE 1 postgres StatefulSet restart"
kc rollout restart "statefulset/$POSTGRES_STS" -n "$NAMESPACE"
bounce_end=$((SECONDS + BOUNCE_SECONDS))
bounce_agents_500=0
bounce_ready_503_with_agents_500=0
while (( SECONDS < bounce_end )); do
  ready_s="$(http_status /ready)"
  health_s="$(http_status /health)"
  agents_s="$(http_status /agents)"
  echo "bounce sample ready=$ready_s health=$health_s agents=$agents_s"
  if [[ "$health_s" != "200" && "$health_s" != "000" ]]; then
    fail "GET /health left 200 during bounce (status=$health_s)"
  fi
  if [[ "$agents_s" == "500" ]]; then
    bounce_agents_500=$((bounce_agents_500 + 1))
    if [[ "$ready_s" == "200" ]]; then
      ready_again="$(http_status /ready)"
      if [[ "$ready_again" == "200" ]]; then
        fail "GET /ready stayed 200 while GET /agents was 500"
      fi
      ready_s="$ready_again"
    fi
    if [[ "$ready_s" == "503" ]]; then
      bounce_ready_503_with_agents_500=$((bounce_ready_503_with_agents_500 + 1))
    fi
  fi
  sleep "$POLL_SECONDS"
done
kc rollout status "statefulset/$POSTGRES_STS" -n "$NAMESPACE" --timeout=180s
echo "bounce agents500=$bounce_agents_500 ready503_with_agents500=$bounce_ready_503_with_agents_500"
(( bounce_ready_503_with_agents_500 > 0 )) || fail "postgres restart produced no GET /agents 500 with GET /ready 503 pair"


banner "PHASE 2 scale postgres to 0 past readiness cutoff"
kc scale "statefulset/$POSTGRES_STS" -n "$NAMESPACE" --replicas=0
first_503=""
hold_agents_500=0
while true; do
  ready_s="$(http_status /ready)"
  health_s="$(http_status /health)"
  agents_s="$(http_status /agents)"
  echo "outage sample ready=$ready_s health=$health_s agents=$agents_s"
  [[ "$health_s" == "200" ]] || fail "GET /health left 200 during long outage (status=$health_s)"
  if [[ "$agents_s" == "500" && "$ready_s" == "200" ]]; then
    ready_again="$(http_status /ready)"
    if [[ "$ready_again" == "200" ]]; then
      fail "GET /ready stayed 200 while GET /agents was 500 during long outage"
    fi
    ready_s="$ready_again"
  fi
  if [[ "$agents_s" == "500" && "$ready_s" == "503" ]]; then
    hold_agents_500=$((hold_agents_500 + 1))
    if [[ -z "$first_503" ]]; then
      first_503=$SECONDS
    fi
  fi
  if [[ -n "$first_503" && $((SECONDS - first_503)) -ge $LONG_OUTAGE_SECONDS ]]; then
    break
  fi
  if [[ -z "$first_503" && $SECONDS -gt 180 ]]; then
    fail "GET /ready never 503d after scaling postgres to 0"
  fi
  sleep "$POLL_SECONDS"
done
(( hold_agents_500 > 0 )) || fail "long outage never showed GET /agents 500 with GET /ready 503"

[[ "$(api_ready_condition)" != "True" ]] || fail "API pod stayed Ready after ${LONG_OUTAGE_SECONDS}s of GET /ready 503"
ready_eps="$(ready_endpoint_count)"
[[ "$ready_eps" == "0" ]] || fail "API Service still has $ready_eps ready endpoint(s) during the outage"
[[ "$(api_restart_count)" == "$BASELINE_RESTARTS" ]] || fail "API restartCount moved during the outage"
assert_no_backoff "$(api_pod)"
echo "long outage: Ready!=True endpoints=$ready_eps restarts=$(api_restart_count) health=200"

banner "RECOVERY scale postgres to 1"
kc scale "statefulset/$POSTGRES_STS" -n "$NAMESPACE" --replicas=1
kc rollout status "statefulset/$POSTGRES_STS" -n "$NAMESPACE" --timeout=240s
waited=0
while (( waited < 180 )); do
  if [[ "$(http_status /ready)" == "200" && "$(http_status /agents)" == "200" ]]; then
    break
  fi
  sleep 2
  waited=$((waited + 2))
done
[[ "$(http_status /ready)" == "200" ]] || fail "GET /ready did not recover to 200"
[[ "$(http_status /health)" == "200" ]] || fail "GET /health is not 200 after recovery"
[[ "$(http_status /agents)" == "200" ]] || fail "GET /agents did not recover to 200"
recovered_body="$(http_body /agents)"
[[ "$recovered_body" == *"$AGENT_NAME"* ]] || fail "agent $AGENT_NAME missing after recovery"
[[ "$(api_restart_count)" == "$BASELINE_RESTARTS" ]] || fail "API restartCount moved during recovery"
assert_no_backoff "$(api_pod)"

echo "PASS: /ready 503d with /agents 500, /health stayed 200, endpoints emptied after the 120s cutoff, $AGENT_NAME survived, restartCount=$BASELINE_RESTARTS"
