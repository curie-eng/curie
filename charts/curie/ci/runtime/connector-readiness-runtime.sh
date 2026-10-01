#!/usr/bin/env bash
# Cluster pin for the connector readiness probe (#3338, follow-up to #3058 /
# PR #3335). The probe is rendered by plugin_format.connector_render and until
# now was pinned only by a unit test on the rendered manifest. The defect was
# found on a real cluster, so this script exercises the exact rendered objects
# on one:
#
#   1. Render a hosted connector whose container never listens on its port
#      (`sh -c sleep infinity`) and apply all four objects. Its container
#      reaches Running with zero restarts, and the Deployment must report
#      0 available replicas for a sustained window: without the probe, Ready
#      meant only "the process started" and a never-binding server stayed
#      Available (#3058).
#   2. Restore the connector by re-rendering and re-applying it with a server
#      that binds after a fixed delay (`sleep 45 && exec httpd -f -p 8080`).
#      The Deployment must stay 0-available through the whole pre-bind window
#      and turn Available only after the server binds.
#   3. The ingress NetworkPolicy is applied before the Deployment and is never
#      removed: its UID is recorded up front and re-asserted at the end. The
#      kubelet probe passing through it is then proven NON-vacuously, the way
#      scripts/check-netpol-enforcement.sh does it: a pod wearing the exact
#      runner-sandbox labels the policy admits can reach the connector, and an
#      unlabelled pod cannot (if it can, the CNI is not enforcing, the probe
#      leg is vacuous, and the script fails instead of passing silently).
#
# Lives under charts/curie/ci/runtime because chart-check discovers only
# top-level assertion scripts and runs them with no cluster; this script needs
# a real one. Runnable directly:
#
#   bash charts/curie/ci/runtime/connector-readiness-runtime.sh \
#     [--context k8scratch] [--namespace test-3338-connector-readiness] \
#     [--release c3338] [--force]
#
# The context must be a disposable cluster (k8scratch, k8, kind-*, k3d-*,
# minikube); --force overrides only that check, never the fresh-namespace one.
# The default context is k8scratch. CI and every other caller pass
# CONNECTOR_READINESS_CONTEXT or --context explicitly. The Calico kind job
# uses kind-curie-e2e.
# The deny leg additionally needs a CNI that enforces NetworkPolicy (kind:
# disable the default CNI and install Calico; k3s/k3d enforce by default),
# which is also what the chart's own preflights.networkPolicyProbe requires.
set -euo pipefail

KUBE_CONTEXT="${CONNECTOR_READINESS_CONTEXT:-k8scratch}"
NAMESPACE="${CONNECTOR_READINESS_NAMESPACE:-test-3338-connector-readiness}"
RELEASE="${CONNECTOR_READINESS_RELEASE:-c3338}"
CONNECTOR_IMAGE="${CONNECTOR_READINESS_CONNECTOR_IMAGE:-busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662}"
PROBE_IMAGE="${CONNECTOR_READINESS_PROBE_IMAGE:-curlimages/curl:8.10.1@sha256:d9b4541e214bcd85196d6e92e2753ac6d0ea699f0af5741f8c6cccbfcf00ef4b}"
CONTROL_IMAGE="${CONNECTOR_READINESS_CONTROL_IMAGE:-hashicorp/http-echo:1.0@sha256:fcb75f691c8b0414d670ae570240cbf95502cc18a9ba57e982ecac589760a186}"
FORCE=0
NAMESPACE_CREATED=0
CLEANUP_STARTED=0

# Fixture identity. app_name is the chart default (`curie`): it is what the
# rendered sandbox_selector keys on, so the sandbox-labelled probe pod below
# wears exactly the labels a real install's ingress NetworkPolicy admits.
AGENT="acme"
CONNECTOR="probe"
APP_NAME="curie"
PORT=8080

# The restore server sleeps BIND_DELAY seconds before binding, which is what
# makes "Available only after the server binds" provable rather than hoped for:
# nothing can pass a TCP probe on a port that does not exist yet.
BIND_DELAY=45
# The kubelet probe runs at initialDelaySeconds 2 + periodSeconds 10; 15s after
# the container starts is past the first failures, so a still-Available
# deployment at that point is the probe doing nothing (#3058).
PROBE_SETTLE_SECONDS=15
NEGATIVE_SECONDS=30
POLL_SECONDS=5
CONTROL_POD="connector-readiness-control-target"
OUTSIDE_POD="connector-readiness-outside"
SANDBOX_POD="connector-readiness-sandbox"

TMP_DIR="$(mktemp -d)"

usage() {
  cat <<'EOF'
Usage: bash charts/curie/ci/runtime/connector-readiness-runtime.sh \
  [--context k8scratch] [--namespace test-3338-connector-readiness] \
  [--release c3338] [--connector-image <image>] [--probe-image <image>] \
  [--control-image <image>] [--force]

Renders a real connector (plugin_format.connector_render) whose container
never listens, applies it to an isolated namespace, and asserts the Deployment
reports 0 available replicas; then restores it with a server that binds after
a delay and asserts it turns Available only after the bind. The ingress
NetworkPolicy stays applied throughout, and its enforcement is proven
non-vacuously (sandbox-labelled probe allowed in, unlabelled probe denied).

Requires: kubectl, uv, python3, a disposable cluster context, and a CNI that
enforces NetworkPolicy for the deny leg. The namespace must not already
exist; it is created and torn down by this script.
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
    --connector-image) need_value "$@"; CONNECTOR_IMAGE="$2"; shift 2 ;;
    --probe-image) need_value "$@"; PROBE_IMAGE="$2"; shift 2 ;;
    --control-image) need_value "$@"; CONTROL_IMAGE="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
  esac
done

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
# runtime/ sits four levels below the repo root (charts/curie/ci/runtime).
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

required_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is missing: $1" >&2
    exit 2
  }
}
required_command kubectl
required_command uv
required_command python3

kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }

banner() { echo; echo "== $* =="; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# The rendered object name. Cross-checked against what the renderer actually
# produced, so this script cannot drift from connector_render's naming (whose
# truncation/digest rules are not a plain concatenation at long names).
OBJECT_NAME="${RELEASE}-${AGENT}-mcp-${CONNECTOR}"
DEPLOYMENT="$OBJECT_NAME"
SERVICE="$OBJECT_NAME"
EGRESS_POLICY="${OBJECT_NAME}-allow"
INGRESS_POLICY="${OBJECT_NAME}-allow-ingress"

diagnostics() {
  banner "DIAGNOSTICS"
  kc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o wide 2>&1 || true
  kc get pods -n "$NAMESPACE" -o wide 2>&1 || true
  kc get networkpolicy -n "$NAMESPACE" 2>&1 || true
  kc get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>&1 | tail -40 || true
  kc describe deployment "$DEPLOYMENT" -n "$NAMESPACE" 2>&1 | tail -40 || true
  local pod
  pod="$(connector_pod)"
  [[ -z "$pod" ]] || {
    kc describe pod "$pod" -n "$NAMESPACE" 2>&1 | tail -60 || true
    kc logs "$pod" -n "$NAMESPACE" -c server --tail=40 2>&1 || true
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
    banner "TEARDOWN namespace=$NAMESPACE"
    kc delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
    if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
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

case "$KUBE_CONTEXT" in
  *ProdCurietechAi*|*StagingCurietechAi*)
    fail "refusing context $KUBE_CONTEXT"
    ;;
  k8scratch|k8|kind-*|k3d-*|minikube|minikube-*) ;;
  *)
    (( FORCE == 1 )) || fail "context '$KUBE_CONTEXT' is not k8scratch/k8/kind-*/k3d-*/minikube (override with --force)"
    ;;
esac

# ---------------------------------------------------------------------------
# Render + apply helpers. The manifests come from the real renderer, the same
# code path the API and the worker's reconcile loop use, so the Deployment
# under test is byte-for-byte the one a bundle gets -- probe included.
# ---------------------------------------------------------------------------

# The embedded render program, written to the temp dir once. Kept as a file
# (not a stdin heredoc at run time) so the failure output can name it.
RENDER_PROGRAM="$TMP_DIR/render_connector.py"
cat >"$RENDER_PROGRAM" <<'PY'
"""Render this test's connector fixtures with the production renderer.

Prints the rendered Deployment name on stdout and writes one JSON file per
object into the output directory. Fails loudly if the fixture would
crash-loop (busybox has CMD ["sh"] and no ENTRYPOINT, and render_deployment
renders only `args`, never `command`, so the fixture must start its own
shell) or if the renderer produced no readiness probe on the connector port
(the exact #3058 defect this script exists to pin).
"""

import json
import sys
from pathlib import Path

from plugin_format.connectors import ConnectorSpec
from plugin_format.connector_render import (
    render_deployment,
    render_ingress_networkpolicy,
    render_networkpolicy,
    render_service,
)

out_dir = Path(sys.argv[1])
args = json.loads(sys.argv[2])
namespace = sys.argv[3]
release = sys.argv[4]
agent = sys.argv[5]
connector = sys.argv[6]
app_name = sys.argv[7]
image = sys.argv[8]
port = int(sys.argv[9])
expected_name = sys.argv[10]

if args[:2] != ["sh", "-c"]:
    raise SystemExit(
        f"fixture args must start a shell (['sh', '-c', ...]), got {args!r}: "
        "busybox has CMD ['sh'] and no ENTRYPOINT, and render_deployment "
        "renders only `args`, so any other shape execs a nonexistent program "
        "and crash-loops instead of never listening"
    )

spec = ConnectorSpec(image=image, args=args, port=port)
# render() refuses a hosted connector with no caller key. This fixture pins
# the server container's readiness probe, and this cluster step does not load
# the worker image the proxy runs from, so it renders that server through the
# same helpers render() calls, without a proxy sidecar.
secret_name = f"{release}-{agent}-connector-secrets"
objects = [
    render_service(release, agent, connector, spec, None),
    render_deployment(
        release, agent, namespace, connector, spec, secret_name, None
    ),
    render_networkpolicy(release, agent, app_name, connector, spec, None),
    render_ingress_networkpolicy(
        release, agent, app_name, connector, spec, None
    ),
]

deployment = next(o for o in objects if o["kind"] == "Deployment")
if deployment["metadata"]["name"] != expected_name:
    raise SystemExit(
        f"rendered Deployment is named {deployment['metadata']['name']!r}, "
        f"expected {expected_name!r}"
    )

(container,) = deployment["spec"]["template"]["spec"]["containers"]
probe = container.get("readinessProbe")
if not probe or probe.get("tcpSocket") != {"port": "http"}:
    raise SystemExit(
        f"rendered connector has no tcpSocket readiness probe on port "
        f"'http' (#3058): {json.dumps(container.get('readinessProbe'))}"
    )
if container.get("ports") != [{"name": "http", "containerPort": port}]:
    raise SystemExit(
        f"probe port 'http' does not name the connector port {port}: "
        f"{json.dumps(container.get('ports'))}"
    )

for index, obj in enumerate(objects):
    (out_dir / f"{index:02d}-{obj['kind'].lower()}.json").write_text(
        json.dumps(obj), encoding="utf-8"
    )
print(deployment["metadata"]["name"])
PY

# render_connector <out-subdir> <fixture-args-json>
render_connector() {
  local out_sub="$1" args_json="$2" out_dir
  out_dir="$TMP_DIR/$out_sub"
  mkdir -p "$out_dir"
  (cd "$REPO_ROOT" && uv run --python 3.13 python "$RENDER_PROGRAM" \
    "$out_dir" "$args_json" "$NAMESPACE" "$RELEASE" "$AGENT" "$CONNECTOR" \
    "$APP_NAME" "$CONNECTOR_IMAGE" "$PORT" "$OBJECT_NAME") \
    || fail "rendering the connector fixtures failed"
}

# The reconciler's writer identity (apps/worker/src/curie_worker/
# connector_k8s.py FIELD_MANAGER), so the objects here are owned the way a
# deploy owns them and a second apply updates rather than conflicts.
apply_objects() {
  local out_sub="$1" f
  for f in "$TMP_DIR/$out_sub"/*.json; do
    kc apply --server-side --field-manager=curie-connector-reconciler \
      --force-conflicts -n "$NAMESPACE" -f "$f" >/dev/null \
      || fail "kubectl apply failed for $f"
  done
}

# ---------------------------------------------------------------------------
# Small read helpers, each normalizing "absent" into a comparable value.
# ---------------------------------------------------------------------------

deployment_available() {
  local v
  v="$(kc get deployment "$DEPLOYMENT" -n "$NAMESPACE" \
    -o jsonpath='{.status.availableReplicas}' 2>/dev/null || true)"
  echo "${v:-0}"
}

deployment_condition() {
  local type="$1"
  kc get deployment "$DEPLOYMENT" -n "$NAMESPACE" \
    -o jsonpath="{.status.conditions[?(@.type==\"$type\")].status}" 2>/dev/null || true
}

connector_pod() {
  kc get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=$OBJECT_NAME" \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | head -1
}

pod_jsonpath() {
  local pod="$1" path="$2"
  kc get pod "$pod" -n "$NAMESPACE" -o jsonpath="$path" 2>/dev/null || true
}

container_running() {
  local pod="$1"
  [[ -n "$(pod_jsonpath "$pod" \
    '{.status.containerStatuses[?(@.name=="server")].state.running.startedAt}')" ]]
}

restart_count() {
  local pod="$1" v
  v="$(pod_jsonpath "$pod" \
    '{.status.containerStatuses[?(@.name=="server")].restartCount}')"
  echo "${v:-0}"
}

pod_ready_condition() {
  pod_jsonpath "$1" '{.status.conditions[?(@.type=="Ready")].status}'
}

ready_endpoint_count() {
  local json
  # A failed read is not zero ready addresses. Treating it as 0 would let the
  # never-listens and pre-bind legs pass without seeing the Endpoints object.
  # Callers must be plain assignments. A failed read inside if, &&, or local
  # would be ignored, and set -e would not stop the script.
  if ! json="$(kc get endpoints "$SERVICE" -n "$NAMESPACE" -o json)"; then
    fail "could not read Endpoints $SERVICE"
  fi
  printf '%s' "$json" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
print(sum(len(subset.get("addresses") or []) for subset in doc.get("subsets") or []))
'
}

epoch_of() {
  python3 -c '
import datetime, sys
stamp = sys.argv[1]
if stamp.endswith("Z"):
    stamp = stamp[:-1] + "+00:00"
print(int(datetime.datetime.fromisoformat(stamp).timestamp()))
' "$1"
}

wait_until() {
  local timeout="$1" desc="$2" waited=0
  shift 2
  while (( waited < timeout )); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
  fail "timed out after ${timeout}s waiting for $desc"
}

pod_exists() {
  [[ -n "$(connector_pod)" ]]
}

deployment_exists() {
  kc get deployment "$DEPLOYMENT" -n "$NAMESPACE" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# Preconditions: a fresh, isolated namespace on a disposable context.
# ---------------------------------------------------------------------------

banner "PRECHECK context=$KUBE_CONTEXT namespace=$NAMESPACE release=$RELEASE"
if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
  fail "namespace $NAMESPACE already exists; choose a new isolated namespace"
fi
if kc create namespace "$NAMESPACE" >/dev/null; then
  NAMESPACE_CREATED=1
else
  fail "could not atomically create namespace $NAMESPACE"
fi
kc label namespace "$NAMESPACE" curie-connector-readiness-harness=owned >/dev/null

# ---------------------------------------------------------------------------
# Phase 1: a connector whose container never listens.
# ---------------------------------------------------------------------------

banner "RENDER + APPLY the never-listening connector (sleep infinity)"
render_connector negative '["sh","-c","sleep infinity"]'
apply_objects negative
wait_until 120 "the connector Deployment to exist" deployment_exists
[[ -n "$(kc get service "$SERVICE" -n "$NAMESPACE" -o name 2>/dev/null || true)" ]] \
  || fail "connector Service $SERVICE did not apply"

# Both rendered policies must be live before the Deployment is judged, and the
# ingress one must never leave: its UID is the "stays in place" evidence.
for policy in "$EGRESS_POLICY" "$INGRESS_POLICY"; do
  kc get networkpolicy "$policy" -n "$NAMESPACE" >/dev/null 2>&1 \
    || fail "NetworkPolicy $policy did not apply"
done
[[ "$(kc get networkpolicy "$INGRESS_POLICY" -n "$NAMESPACE" \
  -o jsonpath='{.spec.policyTypes}' 2>/dev/null || true)" == *Ingress* ]] \
  || fail "$INGRESS_POLICY does not declare policyTypes Ingress"
INGRESS_POLICY_UID="$(kc get networkpolicy "$INGRESS_POLICY" -n "$NAMESPACE" \
  -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
[[ -n "$INGRESS_POLICY_UID" ]] \
  || fail "could not read the UID of $INGRESS_POLICY"

banner "WAIT for the never-listening container to be Running"
wait_until 180 "the connector pod to exist" pod_exists
CONNECTOR_POD="$(connector_pod)"
wait_until 180 "container server in $CONNECTOR_POD to be Running" \
  container_running "$CONNECTOR_POD"
[[ "$(restart_count "$CONNECTOR_POD")" == "0" ]] \
  || fail "the never-listening container restarted (crash loop, not a probe pin)"

banner "NEGATIVE: the Deployment must report 0 available replicas"
# The container runs, never listens, and must stay out of the Service. Sleep
# past the probe's first windows, then hold the assertion for a sustained
# window: one green sample proves nothing about a probe that never fires.
sleep "$PROBE_SETTLE_SECONDS"
negative_samples=0
negative_end=$((SECONDS + NEGATIVE_SECONDS))
while (( SECONDS < negative_end )); do
  available="$(deployment_available)"
  condition="$(deployment_condition Available)"
  ready="$(pod_ready_condition "$CONNECTOR_POD")"
  endpoints="$(ready_endpoint_count)"
  restarts="$(restart_count "$CONNECTOR_POD")"
  echo "negative sample available=$available condition=$condition podReady=${ready:-unset} readyEndpoints=$endpoints restarts=$restarts"
  [[ "$available" == "0" ]] \
    || fail "connector Deployment reported $available available replicas while its container never listens (#3058 regression)"
  [[ "$condition" == "False" ]] \
    || fail "connector Deployment condition Available is '$condition' while its container never listens"
  [[ "$ready" != "True" ]] \
    || fail "connector pod is Ready while its container never listens"
  (( endpoints == 0 )) \
    || fail "connector Service has $endpoints ready endpoint(s) while no container ever listened"
  [[ "$restarts" == "0" ]] \
    || fail "the never-listening container restarted during the negative window"
  negative_samples=$((negative_samples + 1))
  sleep "$POLL_SECONDS"
done
(( negative_samples > 0 )) \
  || fail "the negative window took no samples; PROBE_SETTLE_SECONDS ($PROBE_SETTLE_SECONDS) consumed NEGATIVE_SECONDS ($NEGATIVE_SECONDS)"
echo "negative window held: $negative_samples samples, always 0 available, 0 ready endpoints"

# ---------------------------------------------------------------------------
# Phase 2: restore the connector with a server that binds after a delay.
# ---------------------------------------------------------------------------

OLD_POD="$CONNECTOR_POD"
banner "RESTORE: re-render and re-apply with a server that binds after ${BIND_DELAY}s"
render_connector restore "[\"sh\",\"-c\",\"sleep $BIND_DELAY && exec httpd -f -p $PORT\"]"
apply_objects restore

restore_pod() {
  kc get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=$OBJECT_NAME" \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
    | tr ' ' '\n' | grep -v -x "$OLD_POD" | head -1
}
restore_pod_exists() {
  [[ -n "$(restore_pod)" ]]
}
wait_until 120 "the restored connector pod to exist" restore_pod_exists
NEW_POD="$(restore_pod)"
[[ -n "$NEW_POD" && "$NEW_POD" != "$OLD_POD" ]] \
  || fail "could not identify the restored pod (old=$OLD_POD)"
wait_until 120 "container server in $NEW_POD to be Running" container_running "$NEW_POD"
[[ "$(restart_count "$NEW_POD")" == "0" ]] \
  || fail "the restored container restarted before binding; increase BIND_DELAY or fix the fixture"

STARTED_AT="$(pod_jsonpath "$NEW_POD" \
  '{.status.containerStatuses[?(@.name=="server")].state.running.startedAt}')"
[[ -n "$STARTED_AT" ]] || fail "could not read the restored container start time"
T0="$(epoch_of "$STARTED_AT")"

banner "PRE-BIND: must stay unavailable until the server binds"
# Poll budget comes from the server sleep (startedAt + BIND_DELAY), counted
# with this shell's SECONDS. Do not compare date +%s to the kubelet clock:
# skew either ends the window before a sample or holds it open past the bind.
# The window can start a few seconds after the kubelet start. The Ready
# timestamp check below is what proves the probe did not pass early.
prebind_window=$(( BIND_DELAY - POLL_SECONDS ))
prebind_mark=$SECONDS
prebind_samples=0
prebind_polls=$(( prebind_window / POLL_SECONDS ))
(( prebind_polls > 0 )) \
  || fail "pre-bind poll budget is empty (BIND_DELAY=$BIND_DELAY POLL_SECONDS=$POLL_SECONDS)"
while (( prebind_samples < prebind_polls && SECONDS - prebind_mark < prebind_window )); do
  available="$(deployment_available)"
  ready="$(pod_ready_condition "$NEW_POD")"
  endpoints="$(ready_endpoint_count)"
  echo "pre-bind sample available=$available newPodReady=${ready:-unset} readyEndpoints=$endpoints"
  [[ "$available" == "0" ]] \
    || fail "connector Deployment reported $available available replicas before the server could have bound"
  [[ "$ready" != "True" ]] \
    || fail "restored pod became Ready before the server could have bound (startedAt=$STARTED_AT)"
  (( endpoints == 0 )) \
    || fail "connector Service gained ready endpoints before the server could have bound"
  prebind_samples=$((prebind_samples + 1))
  remaining=$(( prebind_window - (SECONDS - prebind_mark) ))
  if (( prebind_samples < prebind_polls && remaining > 0 )); then
    if (( remaining < POLL_SECONDS )); then
      sleep "$remaining"
    else
      sleep "$POLL_SECONDS"
    fi
  fi
done
(( prebind_samples > 0 )) \
  || fail "the pre-bind window took no samples"

banner "BIND: rollout must reach Available only now"
kc rollout status "deployment/$DEPLOYMENT" -n "$NAMESPACE" --timeout=180s \
  || fail "the restored connector Deployment never became Available"
[[ "$(deployment_available)" == "1" ]] \
  || fail "connector Deployment is Available but reports $(deployment_available) available replicas"
[[ "$(deployment_condition Available)" == "True" ]] \
  || fail "connector Deployment condition Available is not True after the bind"
bound_endpoints="$(ready_endpoint_count)"
(( bound_endpoints > 0 )) \
  || fail "connector Service has no ready endpoints after the rollout"

READY_AT="$(pod_jsonpath "$NEW_POD" \
  '{.status.conditions[?(@.type=="Ready")].lastTransitionTime}')"
[[ -n "$READY_AT" ]] || fail "could not read the restored pod's Ready transition time"
READY_EPOCH="$(epoch_of "$READY_AT")"
# Both stamps come from the kubelet, so host clock skew is not in this
# comparison. Three seconds covers timestamp granularity. The server cannot
# have bound before T0 + BIND_DELAY, so an earlier Ready transition means
# the probe passed pre-bind.
(( READY_EPOCH >= T0 + BIND_DELAY - 3 )) \
  || fail "restored pod became Ready at $READY_AT, before the server could have bound (start=$STARTED_AT)"
echo "bind ordering held: Ready at $READY_AT, bind earliest $STARTED_AT + ${BIND_DELAY}s"

[[ "$(kc get networkpolicy "$INGRESS_POLICY" -n "$NAMESPACE" \
  -o jsonpath='{.metadata.uid}' 2>/dev/null || true)" == "$INGRESS_POLICY_UID" ]] \
  || fail "$INGRESS_POLICY was removed or recreated during the run; it must stay in place"

# ---------------------------------------------------------------------------
# Phase 3: the kubelet probe got through the ingress NetworkPolicy, proven
# non-vacuously. Runs only now, with the connector Ready and its Service
# holding a ready endpoint, so a denial below can only mean policy.
# ---------------------------------------------------------------------------

banner "NETWORKPOLICY legs (probe pods)"
CLUSTER_IP="$(kc get service "$SERVICE" -n "$NAMESPACE" \
  -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)"
[[ -n "$CLUSTER_IP" ]] || fail "could not read the connector Service ClusterIP"

# Numeric uids are required. runAsNonRoot with a named image user makes the
# kubelet refuse the pod (it cannot prove the name is not root). Observed
# 2026-09-28: hashicorp/http-echo:1.0 is uid 65532; curlimages/curl:8.10.1
# is uid 100 (curl_user) gid 101 (curl_group).
kc apply -n "$NAMESPACE" -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $CONTROL_POD
  labels:
    app.kubernetes.io/name: connector-readiness-control-target
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: listener
      image: $CONTROL_IMAGE
      args: ["-listen=:8000", "-text=reachable"]
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
---
apiVersion: v1
kind: Pod
metadata:
  name: $OUTSIDE_POD
  labels:
    app.kubernetes.io/name: connector-readiness-outside
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 100
    runAsGroup: 101
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
---
apiVersion: v1
kind: Pod
metadata:
  name: $SANDBOX_POD
  labels:
    app.kubernetes.io/name: $APP_NAME
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
    curietech.ai/agent: $AGENT
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 100
    runAsGroup: 101
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
YAML

kc wait --for=condition=Ready -n "$NAMESPACE" \
  "pod/$CONTROL_POD" "pod/$OUTSIDE_POD" "pod/$SANDBOX_POD" --timeout=180s >/dev/null \
  || fail "the probe pods did not become ready"

# Read the sandbox labels back off the live object: a mutating webhook that
# strips them would silently widen nothing and narrow the allowed leg into a
# second copy of the deny leg.
SANDBOX_LABELS="$(kc get pod "$SANDBOX_POD" -n "$NAMESPACE" \
  -o jsonpath='{.metadata.labels.app\.kubernetes\.io/name} {.metadata.labels.app\.kubernetes\.io/instance} {.metadata.labels.app\.kubernetes\.io/component} {.metadata.labels.curietech\.ai/agent}' 2>/dev/null || true)"
[[ "$SANDBOX_LABELS" == "$APP_NAME $RELEASE runner-sandbox $AGENT" ]] \
  || fail "the sandbox-labelled probe is not wearing the owning-agent sandbox labels (read back: '$SANDBOX_LABELS')"

CONTROL_IP="$(kc get pod "$CONTROL_POD" -n "$NAMESPACE" \
  -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
[[ -n "$CONTROL_IP" ]] || fail "could not read the control listener's pod IP"

# Positive control for the prober itself: the unlabelled pod must reach a
# listener no ingress policy selects, or every denial below is the client
# failing, not the policy.
prober_rc=0
kc exec -n "$NAMESPACE" "$OUTSIDE_POD" -c probe -- \
  curl -s -m 8 -o /dev/null "http://$CONTROL_IP:8000/" || prober_rc=$?
(( prober_rc == 0 )) \
  || fail "the unlabelled probe pod cannot reach the control listener at $CONTROL_IP:8000 (curl exit $prober_rc); its later denial of the connector would be vacuous"

# The allowed direction: a pod with exactly the labels the ingress policy
# admits reaches the connector. The kubelet's readiness probe takes a
# different, guaranteed path -- node -> pod, admitted by API contract with no
# from-rule at all -- while this leg is an explicitly allowed pod peer. What
# the pair proves: the policy is enforced (the deny below) AND admits the
# labelled peer, so the connector's never-Ready state in phase 1 was the
# probe, not the policy blocking the kubelet.
allowed_rc=0
kc exec -n "$NAMESPACE" "$SANDBOX_POD" -c probe -- \
  curl -s -m 8 -o /dev/null "http://$CLUSTER_IP:$PORT/" || allowed_rc=$?
(( allowed_rc == 0 )) \
  || fail "the sandbox-labelled probe cannot reach the connector at $CLUSTER_IP:$PORT (curl exit $allowed_rc); the connector is Ready, so its egress rule or the ingress rule is not matching"

# The deny: an unlabelled pod in the same namespace must NOT reach it. If it
# can, the CNI is not evaluating NetworkPolicy and this whole leg -- including
# "the kubelet probe gets through the policy" -- is vacuous.
denied_rc=0
kc exec -n "$NAMESPACE" "$OUTSIDE_POD" -c probe -- \
  curl -s -m 8 -o /dev/null "http://$CLUSTER_IP:$PORT/" || denied_rc=$?
if (( denied_rc == 0 )); then
  fail "the unlabelled probe reached the connector at $CLUSTER_IP:$PORT.

The ingress NetworkPolicy is applied but not being evaluated, so this run
certifies nothing about the kubelet probe getting through it.

  kind      needs 'disableDefaultCNI: true' plus a policy-enforcing CNI;
            the default kindnet implements no NetworkPolicy controller.
  minikube  needs 'minikube start --cni=calico'.
  k3s/k3d   enforce by default via kube-router.

Re-run against a cluster whose CNI enforces."
fi
echo "  ok  unlabelled probe denied the connector (curl exit $denied_rc; timeout is a drop, refusal is an RST, both are denials with the prober proven above)"

echo
echo "PASS: never-listening connector held 0 available replicas ($negative_samples samples); restored connector became Available only after the bind ($prebind_samples pre-bind samples); the ingress NetworkPolicy stayed in place (uid $INGRESS_POLICY_UID) and the kubelet probe got through it on an enforcing CNI"
