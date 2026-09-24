#!/usr/bin/env bash
# Nightly SRE demo e2e (#2246, #2854).
#
# One driver for observations toward five demo assertions on a kind cluster with
# the pinned upstream kubernetes-mcp-server and a live provider. Turns start
# with `curie cluster message`.
# Approvals resolve through `curie cluster approvals` and an operator
# principal on routes bound to an explicit `approvers.users` list. No Slack
# app, bot, user token, or channel is required.
#
# Phases (CURIE_SRE_DEMO_PHASE, or the first argument):
#   prereqs  Check the live provider. A missing credential writes
#            SKIPPED plus the reason to GITHUB_STEP_SUMMARY, sets ready=false
#            on GITHUB_OUTPUT, and exits 0 for pull-request inventory only.
#            That is the documented skip, not a green that proved the five
#            assertions. Required scheduled, dispatch and release-candidate
#            runs fail closed on missing setup (CURIE_SRE_DEMO_REQUIRED=1).
#   run      Drive the five assertions against an already-installed kind
#            release. Refuses unless CURIE_SRE_DEMO_ALLOW_LIVE=1 so a
#            laptop invocation cannot touch a cluster. Missing credentials
#            in this phase fail closed (exit 1); skipping is the prereqs
#            phase's job.
#
# The five assertions, each with a negative control:
#   1. read (namespaces_list) replies and creates no approval record
#   2. approval-gated resources_scale 1 to 2: one pending naming only that
#      tool, replicas stay 1/1 until approve, then 2/2; audit principal_kind
#      is operator
#   3. one-shot re-arm: a second scale creates a new pending approval; the
#      first grant is not reused; replicas stay 2/2
#   4. configuration_view is absent from the catalog; namespaces_list is present
#   5. RBAC ceiling: an approved scale of the platform API is forbidden and
#      leaves replicas unchanged
#
# Pin: ghcr.io/containers/kubernetes-mcp-server@sha256:6d650f4bd6ac303ad82713c997e73a2d001602f9bf17392c9b9a0e30e29c6423
# (examples/sre-bot/connectors.yaml). Do not float this to latest.
#
# Required env for a live run:
#   CURIE_BIN, CURIE_CREDENTIALS
#   CURIE_SRE_DEMO_ALLOW_LIVE=1
# Optional: CURIE_MODEL, CURIE_NAMESPACE (default curie), CURIE_RELEASE
# (default curie), CURIE_SRE_DEMO_AGENT (default sre-bot),
# CURIE_SRE_DEMO_OPERATOR (default U0EXAMPLE1),
# CURIE_SRE_DEMO_RESOLUTION_CHANNEL (default C0LOCALDEV),
# CURIE_SRE_DEMO_TIMEOUT_SECS (default 300), CURIE_SRE_DEMO_EVIDENCE_DIR
# (private parent directory in which raw row logs are retained; otherwise
# removed on exit). CURIE_SRE_DEMO_RESULTS_FILE retains fixed row/status JSON
# only.

set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PHASE="${CURIE_SRE_DEMO_PHASE:-${1:-}}"
NAMESPACE="${CURIE_NAMESPACE:-curie}"
RELEASE="${CURIE_RELEASE:-curie}"
AGENT="${CURIE_SRE_DEMO_AGENT:-sre-bot}"
OPERATOR_SUBJECT="${CURIE_SRE_DEMO_OPERATOR:-U0EXAMPLE1}"
RESOLUTION_CHANNEL="${CURIE_SRE_DEMO_RESOLUTION_CHANNEL:-C0LOCALDEV}"
TIMEOUT_SECS="${CURIE_SRE_DEMO_TIMEOUT_SECS:-300}"
DEMO_NS="sre-demo"
DEMO_DEPLOY="sre-demo-app"
# Keep in lockstep with examples/sre-bot/connectors.yaml.
K8S_MCP_DIGEST="sha256:6d650f4bd6ac303ad82713c997e73a2d001602f9bf17392c9b9a0e30e29c6423"
K8S_MCP_IMAGE="ghcr.io/containers/kubernetes-mcp-server@${K8S_MCP_DIGEST}"
SCALE_APPROVAL_ID=""
MCP_FORWARD_PID=""
PROBE_NS=""
PROBE_NS_CREATED=0
TURN_PID=""
API_FORWARD_PID=""

if [[ -z "$PHASE" ]]; then
  if [[ "${CURIE_SRE_DEMO_ALLOW_LIVE:-}" == "1" ]]; then
    PHASE=run
  else
    PHASE=prereqs
  fi
fi

write_summary() {
  local body="$1"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$body" >>"$GITHUB_STEP_SUMMARY"
  fi
  printf '%s\n' "$body" >&2
}

write_output() {
  local key="$1"
  local value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "$key" "$value" >>"$GITHUB_OUTPUT"
  fi
}

missing_prereqs() {
  local missing=()
  [[ -n "${CURIE_CREDENTIALS:-}" ]] || missing+=("CURIE_CREDENTIALS (live provider)")
  if ((${#missing[@]})); then
    printf '%s\n' "${missing[@]}"
  fi
}

phase_prereqs() {
  local missing
  missing="$(missing_prereqs || true)"
  if [[ -n "$missing" ]]; then
    write_summary "$(cat <<EOF
### SRE demo e2e SKIPPED

The five CLI assertions did not run. Missing prerequisite(s):

$(printf '%s\n' "$missing" | sed 's/^/- /')

Provision OPENROUTER_API_KEY as CURIE_CREDENTIALS, then re-run this workflow
from workflow_dispatch.
No Slack app is required.

Assertions not executed: namespaces_list read; resources_scale approval; re-arm;
configuration_view denial; RBAC ceiling.
EOF
)"
    write_output ready false
    write_output skip_reason "missing live provider"
    echo "sre-demo-e2e: acceptance BLOCKED (prerequisites missing)" >&2
    if [[ "${CURIE_SRE_DEMO_REQUIRED:-0}" == "1" ]]; then
      exit 1
    fi
    exit 0
  fi
  write_summary "### SRE demo e2e prerequisites ready

The live provider credential is present. The live job may run the five
assertions on kind through cluster message and an operator principal."
  write_output ready true
  echo "sre-demo-e2e: prerequisites ready" >&2
}

curie_bin() {
  if [[ -n "${CURIE_BIN:-}" ]]; then
    printf '%s' "$CURIE_BIN"
    return
  fi
  if command -v curie >/dev/null 2>&1; then
    command -v curie
    return
  fi
  echo "CURIE_BIN is unset and curie is not on PATH" >&2
  exit 1
}

json_get() {
  python3 -c 'import json,sys; data=json.load(sys.stdin)
path=sys.argv[1].split(".")
cur=data
for key in path:
    if isinstance(cur, list):
        cur=cur[int(key)]
    else:
        cur=cur[key]
if cur is None:
    sys.exit(1)
if isinstance(cur,(dict,list)):
    json.dump(cur, sys.stdout)
else:
    print(cur)' "$1"
}

spec_replicas_of() {
  local ns="$1" name="$2"
  kubectl get deploy "$name" -n "$ns" -o jsonpath='{.spec.replicas}'
}

wait_replicas() {
  local ns="$1" name="$2" want="$3" timeout="${4:-180}"
  local i
  for i in $(seq 1 "$timeout"); do
    if kubectl get deploy "$name" -n "$ns" -o json | python3 -c '
import json,sys
d=json.load(sys.stdin); want=int(sys.argv[1]); s=d.get("status", {})
generation=d.get("metadata", {}).get("generation")
ready=(isinstance(generation,int) and s.get("observedGeneration",-1)>=generation
       and d.get("spec",{}).get("replicas")==want
       and all(s.get(k,0)==want for k in
               ("replicas","updatedReplicas","readyReplicas","availableReplicas"))
       and s.get("unavailableReplicas",0)==0)
sys.exit(0 if ready else 1)' "$want"; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for observed generation and all desired replicas ready/available" >&2
  return 1
}

connector_deployment() {
  kubectl get deploy -n "$NAMESPACE" -o json | python3 -c '
import json,sys
rows=[r["metadata"]["name"] for r in json.load(sys.stdin)["items"]
      if any(c.get("image")==sys.argv[1] for c in r["spec"]["template"]["spec"]["containers"])]
if len(rows)!=1: raise SystemExit("expected exactly one pinned connector deployment")
print(rows[0])' "$K8S_MCP_IMAGE"
}

build_kubeconfig() {
  kubectl wait --namespace "$NAMESPACE" \
    --for=jsonpath='{.data.token}' secret/sre-bot-kubernetes-token \
    --timeout=120s >/dev/null
  python3 - "$NAMESPACE" <<'PY'
import base64, json, subprocess, sys
namespace = sys.argv[1]
raw = subprocess.check_output(
    ["kubectl", "get", "secret", "sre-bot-kubernetes-token", "-n", namespace, "-o", "json"]
)
secret = json.loads(raw)
data = secret["data"]
ca = data["ca.crt"]
token = base64.b64decode(data["token"]).decode("utf-8")
if not token.strip():
    raise SystemExit("sre-bot-kubernetes-token is empty")
config = {
    "apiVersion": "v1",
    "kind": "Config",
    "clusters": [{
        "name": "in-cluster",
        "cluster": {
            "server": "https://kubernetes.default.svc",
            "certificate-authority-data": ca,
        },
    }],
    "users": [{"name": "sre-bot-kubernetes", "user": {"token": token}}],
    "contexts": [{
        "name": "sre-bot-kubernetes",
        "context": {"cluster": "in-cluster", "user": "sre-bot-kubernetes"},
    }],
    "current-context": "sre-bot-kubernetes",
}
sys.stdout.write(json.dumps(config))
PY
}

build_denied_upgrade_kubeconfig() {
  # The demo tests the Kubernetes connector, but the source bundle also declares
  # self-upgrade. Give that unused connector a distinct identity with no role
  # binding; never reuse the reader token or grant upgrade permissions here.
  local identity="sre-demo-upgrade-denied" token
  kubectl -n "$NAMESPACE" create serviceaccount "$identity" >/dev/null
  local verb resource
  for permission in 'create jobs' 'get secrets'; do
    read -r verb resource <<< "$permission"
    if [[ "$(kubectl auth can-i --as="system:serviceaccount:${NAMESPACE}:${identity}" "$verb" "$resource" -n "$NAMESPACE")" != "no" ]]; then
      echo "unexpected upgrade grant for ${identity}: ${permission}" >&2
      return 1
    fi
  done
  token="$(kubectl -n "$NAMESPACE" create token "$identity" --duration=1h)"
  SRE_DEMO_DENIED_TOKEN="$token" SRE_DEMO_DENIED_IDENTITY="$identity" python3 -c '
import json, os, sys
config = json.load(sys.stdin)
identity = os.environ["SRE_DEMO_DENIED_IDENTITY"]
config["users"] = [{"name": identity, "user": {"token": os.environ["SRE_DEMO_DENIED_TOKEN"]}}]
config["contexts"] = [{"name": identity, "context": {"cluster": "in-cluster", "user": identity}}]
config["current-context"] = identity
json.dump(config, sys.stdout)
'
}

ensure_demo_workload() {
  kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEMO_DEPLOY}
  namespace: ${DEMO_NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${DEMO_DEPLOY}
  template:
    metadata:
      labels:
        app: ${DEMO_DEPLOY}
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
          resources:
            requests:
              cpu: 1m
              memory: 8Mi
            limits:
              cpu: 10m
              memory: 16Mi
EOF
  kubectl rollout status deploy/"$DEMO_DEPLOY" -n "$DEMO_NS" --timeout=120s
}

run_cluster() {
  local bin subcommand
  bin="$(curie_bin)"
  subcommand="$1"
  shift
  "$bin" --json cluster "$subcommand" --namespace "$NAMESPACE" --release "$RELEASE" "$@"
}

cluster_turn() {
  local text="$1"
  shift
  run_cluster message --timeout-secs "$TIMEOUT_SECS" --chart "$ROOT/charts/curie" "$@" "$text"
}

turn_is_reply() {
  python3 -c 'import json,sys
d=json.load(sys.stdin)
if d.get("timed_out") or d.get("awaiting_approval") or d.get("status")=="enqueued":
    sys.exit(1)
if not d.get("finalized"):
    sys.exit(1)
if d.get("reply") is None:
    sys.exit(1)
print(d["reply"])'
}

turn_thread() {
  python3 -c 'import json,sys
d=json.load(sys.stdin)
thread=d.get("thread")
if not thread:
    raise SystemExit("cluster message JSON omitted thread")
print(thread)'
}

list_pending() {
  run_cluster approvals "$AGENT" --list
}

thread_pending() {
  list_pending | python3 -c 'import json,sys
d=json.load(sys.stdin)
if d.get("truncated",True): raise SystemExit("approval list incomplete")
rows=[r for r in d["pending"] if r.get("conversation_id")==sys.argv[1]
      and r.get("status")=="pending"]
json.dump(rows,sys.stdout)' "$1"
}

pending_for_tool() {
  list_pending | python3 -c 'import json,sys
d=json.load(sys.stdin)
if d.get("truncated",True): raise SystemExit("approval list incomplete")
tool=sys.argv[1]
rows=[r for r in d["pending"] if r.get("status")=="pending"
      and r.get("granted_tool")==tool]
if len(rows)>1: sys.exit(2)
if not rows: sys.exit(1)
print(rows[0]["id"])' "$1"
}

wait_pending_tool() {
  local tool="$1"
  local timeout="${2:-180}"
  local deadline=$((SECONDS + timeout))
  local id
  while (( SECONDS < deadline )); do
    if id="$(pending_for_tool "$tool")"; then
      printf '%s' "$id"
      return 0
    else
      local result=$?
      [[ "$result" == 1 ]] || return 1
    fi
    sleep 2
  done
  echo "no pending approval named ${tool} appeared" >&2
  return 1
}

wait_scale_pending() {
  wait_pending_tool "mcp__kubernetes__resources_scale" "${1:-180}"
}

mint_operator_principal() {
  local payload token subject
  payload="$(run_cluster approvals "$AGENT" --mint-operator-principal "$OPERATOR_SUBJECT")"
  subject="$(printf '%s' "$payload" | json_get operator_principal.subject)"
  token="$(printf '%s' "$payload" | json_get operator_principal.token)"
  [[ "$subject" == "$OPERATOR_SUBJECT" ]] || {
    echo "operator principal subject did not match ${OPERATOR_SUBJECT}" >&2
    return 1
  }
  [[ -n "$token" ]] || {
    echo "operator principal token is missing" >&2
    return 1
  }
  printf '%s' "$token"
}

bind_operator_route() {
  run_cluster approvals "$AGENT" \
    --route-resolution "sre-approvals=${RESOLUTION_CHANNEL}" \
    --route-approvers "sre-approvals=users:${OPERATOR_SUBJECT}" >/dev/null
}

discover_release_secret() {
  kubectl get secret -n "$NAMESPACE" \
    -l "app.kubernetes.io/instance=${RELEASE}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
    | python3 -c 'import sys
names=[n.strip() for n in sys.stdin if n.strip()]
match=[n for n in names if n.endswith("-secrets") and not n.endswith("-connector-secrets")]
if len(match)!=1:
    raise SystemExit("expected exactly one release secrets object")
print(match[0])'
}

audit_is_operator() {
  python3 -c 'import json,os,sys
rows=json.load(sys.stdin)
operator=os.environ["CURIE_SRE_AUDIT_OPERATOR"]
if not isinstance(rows, list) or not rows:
    raise SystemExit("approval audit is empty")
authorized=[row for row in rows if row.get("authorized") is True]
if not authorized:
    raise SystemExit("approval audit has no authorized attempt")
last=authorized[-1]
if last.get("principal_kind")!="operator":
    raise SystemExit("approval was not resolved by an operator principal")
if last.get("actor")!=operator:
    raise SystemExit("approval audit actor did not match the operator subject")'
}

assert_operator_audit() {
  local approval_id="$1"
  local secret port
  secret="$(discover_release_secret)"
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
  kubectl port-forward --address=127.0.0.1 -n "$NAMESPACE" "svc/${RELEASE}-api" "${port}:80" >/dev/null 2>&1 &
  API_FORWARD_PID=$!
  local i
  for i in $(seq 1 20); do
    kill -0 "$API_FORWARD_PID" 2>/dev/null || {
      echo "API port-forward exited before the audit check" >&2
      API_FORWARD_PID=""
      return 1
    }
    if python3 -c 'import socket,sys; socket.create_connection(("127.0.0.1",int(sys.argv[1])),timeout=.2).close()' "$port" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  # Decode the key server-side so the plaintext is never a process argument.
  local result=0
  CURIE_SRE_AUDIT_SECRET="$secret" \
  CURIE_SRE_AUDIT_NAMESPACE="$NAMESPACE" \
  CURIE_SRE_AUDIT_PORT="$port" \
  CURIE_SRE_AUDIT_ID="$approval_id" \
  CURIE_SRE_AUDIT_OPERATOR="$OPERATOR_SUBJECT" \
  python3 - <<'PY' | CURIE_SRE_AUDIT_OPERATOR="$OPERATOR_SUBJECT" audit_is_operator || result=$?
import json, os, subprocess, sys, urllib.request
secret = os.environ["CURIE_SRE_AUDIT_SECRET"]
namespace = os.environ["CURIE_SRE_AUDIT_NAMESPACE"]
port = os.environ["CURIE_SRE_AUDIT_PORT"]
approval_id = os.environ["CURIE_SRE_AUDIT_ID"]
key = subprocess.check_output(
    [
        "kubectl", "-n", namespace, "get", "secret", secret,
        "-o", 'go-template={{ index .data "apiKey" | base64decode }}',
    ],
    text=True,
).strip()
if not key:
    raise SystemExit("release apiKey is empty")
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/approvals/{approval_id}/audit",
    headers={"X-API-Key": key},
    method="GET",
)
with urllib.request.urlopen(req, timeout=30) as resp:
    json.dump(json.load(resp), sys.stdout)
PY
  if [[ -n "$API_FORWARD_PID" ]]; then
    kill "$API_FORWARD_PID" 2>/dev/null || true
    wait "$API_FORWARD_PID" 2>/dev/null || true
    API_FORWARD_PID=""
  fi
  return "$result"
}

resolve_approval() {
  local id="$1"
  shift
  run_cluster approvals "$AGENT" --resolve "$id" "$@"
}

approve() {
  local id="$1" payload
  payload="$(resolve_approval "$id")"
  printf '%s' "$payload" | python3 -c 'import json,sys
d=json.load(sys.stdin).get("resolved") or {}
if d.get("status")!="approved":
    raise SystemExit("resolve did not approve the row")'
  assert_operator_audit "$id"
}

reject() {
  local id="$1" payload
  payload="$(resolve_approval "$id" --reject)"
  printf '%s' "$payload" | python3 -c 'import json,sys
d=json.load(sys.stdin).get("resolved") or {}
if d.get("status")!="rejected":
    raise SystemExit("resolve did not reject the row")'
  assert_operator_audit "$id"
}

stop_turn() {
  if [[ -n "${TURN_PID:-}" ]]; then
    kill "$TURN_PID" 2>/dev/null || true
    wait "$TURN_PID" 2>/dev/null || true
    TURN_PID=""
  fi
}

drive_gated_turn() {
  local text="$1" tool="$2" decision="$3" before_resolve="${4:-}"
  shift $(( $# >= 4 ? 4 : 3 ))
  local out="${evidence_dir:-$HOME}/gated-turn.out"
  local err="${evidence_dir:-$HOME}/gated-turn.err"
  : >"$out"
  : >"$err"
  local bin
  bin="$(curie_bin)"
  "$bin" --json cluster message --namespace "$NAMESPACE" --release "$RELEASE" \
    --chart "$ROOT/charts/curie" --timeout-secs "$TIMEOUT_SECS" \
    "$@" "$text" >"$out" 2>"$err" &
  TURN_PID=$!
  local id
  if ! id="$(wait_pending_tool "$tool" 180)"; then
    stop_turn
    echo "gated turn did not produce a pending ${tool} approval" >&2
    return 1
  fi
  if [[ -n "$before_resolve" ]]; then
    "$before_resolve" || {
      local held=$?
      stop_turn
      return "$held"
    }
  fi
  case "$decision" in
    approve) approve "$id" ;;
    reject) reject "$id" ;;
    *)
      stop_turn
      echo "drive_gated_turn decision must be approve or reject" >&2
      return 1
      ;;
  esac
  local status=0
  set +e
  wait "$TURN_PID"
  status=$?
  set -e
  TURN_PID=""
  if (( status != 0 )); then
    echo "cluster message exited ${status} after ${decision}" >&2
    return 1
  fi
  printf '%s' "$id"
}

phase_run() {
  if [[ "${CURIE_SRE_DEMO_ALLOW_LIVE:-}" != "1" ]]; then
    echo "PHASE=run refuses to start without CURIE_SRE_DEMO_ALLOW_LIVE=1 (this guard keeps a laptop invocation from touching a cluster)." >&2
    exit 1
  fi
  if [[ -z "${CURIE_CREDENTIALS:-}" ]]; then
    echo "PHASE=run is missing CURIE_CREDENTIALS (fail closed; skipping is the prereqs phase)." >&2
    exit 1
  fi
  local bin
  bin="$(curie_bin)"
  kubectl apply -f "$ROOT/examples/sre-bot/manifests/kubernetes-access.yaml"
  local kubeconfig upgrade_kubeconfig
  kubeconfig="$(build_kubeconfig)"
  upgrade_kubeconfig="$(printf '%s' "$kubeconfig" | build_denied_upgrade_kubeconfig)"
  ensure_demo_workload

  export K8S_KUBECONFIG="$kubeconfig"
  export SELF_UPGRADE_KUBECONFIG="$upgrade_kubeconfig"

  set +e
  "$bin" cluster deploy \
    --plugin-dir "$ROOT/examples/sre-bot" \
    --chart "$ROOT/charts/curie" \
    --namespace "$NAMESPACE" \
    --release "$RELEASE" \
    --secret K8S_KUBECONFIG \
    --secret SELF_UPGRADE_KUBECONFIG
  local first_deploy=$?
  set -e
  if [[ "$first_deploy" != 0 && "$first_deploy" != 2 ]]; then
    echo "initial cluster deploy failed with exit ${first_deploy}" >&2
    exit 1
  fi
  bind_operator_route
  "$bin" cluster deploy \
    --plugin-dir "$ROOT/examples/sre-bot" \
    --chart "$ROOT/charts/curie" \
    --namespace "$NAMESPACE" \
    --release "$RELEASE" \
    --secret K8S_KUBECONFIG \
    --secret SELF_UPGRADE_KUBECONFIG

  local waited deployment
  waited=0
  while [[ $waited -lt 90 ]]; do
    if deployment="$(connector_deployment 2>/dev/null)"; then
      break
    fi
    sleep 2
    waited=$((waited + 1))
  done
  if ! deployment="$(connector_deployment)"; then
    echo "the pinned kubernetes-mcp-server connector did not become ready" >&2
    exit 1
  fi

  kubectl rollout status "deploy/$deployment" -n "$NAMESPACE" --timeout=180s

  local token
  token="$(mint_operator_principal)"
  export CURIE_APPROVAL_PRINCIPAL_TOKEN="$token"

  if [[ -n "${CURIE_SRE_DEMO_EVIDENCE_DIR:-}" ]]; then
    mkdir -p "$CURIE_SRE_DEMO_EVIDENCE_DIR"
    evidence_dir="$(mktemp -d "$CURIE_SRE_DEMO_EVIDENCE_DIR/sre-demo.XXXXXX")"
  else
    evidence_dir="$(mktemp -d)"
  fi
  trap 'stop_turn; [[ -n "${API_FORWARD_PID:-}" ]] && kill "$API_FORWARD_PID" 2>/dev/null || true; [[ "$PROBE_NS_CREATED" == 0 ]] || kubectl delete namespace "$PROBE_NS" --wait=false >/dev/null 2>&1; [[ -n "${CURIE_SRE_DEMO_EVIDENCE_DIR:-}" ]] || rm -rf "$evidence_dir"' EXIT
  PROBE_NS="sre-e2e-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
  kubectl create namespace "$PROBE_NS" >/dev/null
  PROBE_NS_CREATED=1
  OBSERVATION_FAILURES=0
  run_assertion read assert_read
  run_assertion scale assert_scale
  run_assertion rearm assert_rearm
  run_assertion configuration-denial assert_configuration_denial
  run_assertion rbac-ceiling assert_rbac_ceiling
  if (( OBSERVATION_FAILURES )); then
    write_summary "SRE demo acceptance incomplete. BLOCKED rows are unproved and do not count as passes. See each row above."
    return 1
  fi
}

run_assertion() {
  local row="$1" function="$2" result status
  # Do not put the function in an if/|| condition: Bash would disable errexit
  # throughout it and a failed check could fall through to a passing echo.
  set +e
  (set -e; "$function") >"$evidence_dir/$row.log" 2>&1
  result=$?
  set -e
  case "$result" in
    0) status=PASS; write_summary "- $row: PASS (only the named assertion)." ;;
    3) status=BLOCKED; write_summary "- $row: BLOCKED. $(block_reason "$row")"
       OBSERVATION_FAILURES=$((OBSERVATION_FAILURES + 1)) ;;
    *) status=FAILED; write_summary "- $row: FAILED. Raw diagnostics were kept private during execution."
       OBSERVATION_FAILURES=$((OBSERVATION_FAILURES + 1)) ;;
  esac
  if [[ -n "${CURIE_SRE_DEMO_RESULTS_FILE:-}" ]]; then
    # Only fixed row/status enums are public artifacts. Raw cluster diagnostics
    # may contain deployment identifiers or credentials.
    python3 - "$row" "$status" >>"$CURIE_SRE_DEMO_RESULTS_FILE" <<'PYOUTCOME'
import json,sys
row,status=sys.argv[1:]
assert row in {"read","scale","rearm","configuration-denial","rbac-ceiling"}
assert status in {"PASS","BLOCKED","FAILED"}
print(json.dumps({"row":row,"status":status},sort_keys=True))
PYOUTCOME
  fi
}

# A case, not an associative array: bash 3.2, which macOS ships, has none.
block_reason() {
  case "$1" in
    read) echo "cluster message did not return a finalized reply that named every observed namespace." ;;
    scale) echo "Pending tool, held replicas, operator-principal resolve, and post-approve replica change are required." ;;
    rearm) echo "Requires a completed operator grant followed by a new request whose pending row is distinct; the preceding grant path is blocked." ;;
    configuration-denial) echo "The real connector MCP endpoint could not be reached." ;;
    rbac-ceiling) echo "Requires operator-principal approval and an explicit forbidden tool result, with the platform deployment unchanged." ;;
  esac
}

workload_specs() {
  kubectl get deploy,statefulset,daemonset -A -o json | python3 -c '
import json,sys
rows=[[r["kind"],r["metadata"]["namespace"],r["metadata"]["name"],r["spec"]]
      for r in json.load(sys.stdin)["items"]]
print(json.dumps(sorted(rows,key=lambda r:r[:3]),sort_keys=True))'
}

assert_read() {
  local before after posted ts replies namespaces
  before="$(workload_specs)"
  namespaces="$(kubectl get ns -o json)"
  posted="$(cluster_turn "List all current Kubernetes namespaces using namespaces_list. Include every namespace name in your answer. Do not scale or mutate anything.")"
  ts="$(printf '%s' "$posted" | turn_thread)"
  replies="$(printf '%s' "$posted" | turn_is_reply)"
  printf '%s' "$replies" | EXPECTED_NAMESPACES="$namespaces" python3 -c '
import json,os,re,sys
names={r["metadata"]["name"] for r in json.loads(os.environ["EXPECTED_NAMESPACES"])["items"]}
words=set(re.findall(r"[a-z0-9][a-z0-9-]*",sys.stdin.read()))
if not names or not names<=words: raise SystemExit("reply omitted observed namespace data")'
  thread_pending "$ts" | python3 -c 'import json,sys; sys.exit(bool(json.load(sys.stdin)))'
  after="$(workload_specs)"
  [[ "$before" == "$after" ]] || { echo "read changed workload specifications" >&2; return 1; }
}

assert_scale() {
  local id
  wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 1
  hold_scale_at_one() { wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 1; }
  id="$(drive_gated_turn \
    "Scale the ${DEMO_DEPLOY} Deployment in namespace ${DEMO_NS} from 1 replica to 2 using resources_scale. Request approval first." \
    "mcp__kubernetes__resources_scale" \
    approve \
    hold_scale_at_one)"
  wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 2
  [[ -n "$id" ]] || return 1
  SCALE_APPROVAL_ID="$id"
}

assert_rearm() {
  local id
  [[ -n "$SCALE_APPROVAL_ID" ]] || {
    echo "re-arm requires the scale approval id from the preceding grant" >&2
    return 1
  }
  wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 2
  hold_scale_at_two() { wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 2; }
  id="$(drive_gated_turn \
    "Scale the ${DEMO_DEPLOY} Deployment in namespace ${DEMO_NS} from 2 replicas to 3 using resources_scale. Request a fresh approval; do not reuse a prior grant." \
    "mcp__kubernetes__resources_scale" \
    reject \
    hold_scale_at_two)"
  wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 2
  [[ -n "$id" ]] || return 1
  [[ "$id" != "$SCALE_APPROVAL_ID" ]] || {
    echo "re-arm reused the preceding scale approval id" >&2
    return 1
  }
}

assert_configuration_denial() {
  local deployment port
  deployment="$(connector_deployment)"
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
  kubectl port-forward --address=127.0.0.1 -n "$NAMESPACE" "deploy/$deployment" "$port:8000" >/dev/null 2>&1 &
  MCP_FORWARD_PID=$!
  trap 'kill "$MCP_FORWARD_PID" 2>/dev/null || true; wait "$MCP_FORWARD_PID" 2>/dev/null || true' EXIT
  local i
  for i in $(seq 1 20); do
    kill -0 "$MCP_FORWARD_PID" 2>/dev/null || return 3
    if python3 -c 'import socket,sys; socket.create_connection(("127.0.0.1",int(sys.argv[1])),timeout=.2).close()' "$port" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  # Use the same MCP SDK transport as the runner, not container command-line args.
  uv run --locked --package curie-runner --project "$ROOT" python "$ROOT/cli/scripts/sre-demo-mcp-probe.py" \
    "http://127.0.0.1:$port/mcp" "$PROBE_NS"
}

assert_rbac_ceiling() {
  local before want id
  before="$(spec_replicas_of "$NAMESPACE" "${RELEASE}-api")"
  [[ -n "$before" ]] || return 1
  want=$((before + 1))
  hold_platform_replicas() {
    [[ "$(spec_replicas_of "$NAMESPACE" "${RELEASE}-api")" == "$before" ]]
  }
  id="$(drive_gated_turn \
    "Use Kubernetes resources_scale to scale the ${RELEASE}-api Deployment in namespace ${NAMESPACE} from ${before} replica(s) to ${want}. Request approval first. Do not use any other mutation tool." \
    "mcp__kubernetes__resources_scale" \
    approve \
    hold_platform_replicas)"
  [[ "$(spec_replicas_of "$NAMESPACE" "${RELEASE}-api")" == "$before" ]] || {
    echo "platform API replica count changed after an approved scale" >&2
    return 1
  }
  wait_replicas "$DEMO_NS" "$DEMO_DEPLOY" 2
  [[ -n "$id" ]] || return 1
}

case "$PHASE" in
  prereqs) phase_prereqs ;;
  run) phase_run ;;
  *)
    echo "usage: cli/scripts/sre-demo-e2e.sh [prereqs|run]" >&2
    exit 2
    ;;
esac
