#!/usr/bin/env bash
# QA4-02: rollout-free first message plus dead-consumer PEL recovery.
#
# This deployed test drives the public CLI, worker Deployment, a namespaced
# SandboxTemplate/SandboxClaim, Valkey PEL, and the reply stub. It never mutates
# the shared agent-sandbox controller.
#
# This is intentionally a CI-owned script rather than another public `curie
# dev` verb: it requires a preinstalled disposable release plus test-only live
# Deployment/SandboxTemplate mutations for the recovery phase. `curie dev e2e-ladder` remains the
# stable contributor-facing cluster surface and CI owns this narrow phase.
#
# Optional red-on-revert knobs (normal CI leaves these unset):
#   CURIE_E2E_BASE_REF               diagnostic source ref (default origin/main)
#   CURIE_E2E_PRE_FIX_CLI_BIN        pre-fix CLI used only by phase 1
#   CURIE_E2E_PRE_FIX_WORKER_IMAGE   pre-fix worker repository for phase 2
#   CURIE_E2E_PRE_FIX_WORKER_TAG     pre-fix worker tag for phase 2
# A pre-fix phase must make this script non-zero at its named assertion.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${CURIE_E2E_NAMESPACE:-curie}"
RELEASE="${CURIE_E2E_RELEASE:-curie}"
BASE_REF="${CURIE_E2E_BASE_REF:-origin/main}"
PRE_FIX_CLI_BIN="${CURIE_E2E_PRE_FIX_CLI_BIN:-}"
PRE_FIX_WORKER_IMAGE="${CURIE_E2E_PRE_FIX_WORKER_IMAGE:-}"
PRE_FIX_WORKER_TAG="${CURIE_E2E_PRE_FIX_WORKER_TAG:-}"
STREAM="curie:runs"
GROUP="curie-workers"
WORKER_DEPLOYMENT="${RELEASE}-worker"
VALKEY_STATEFULSET="${RELEASE}-valkey"
SANDBOX_TEMPLATE="${CURIE_E2E_SANDBOX_TEMPLATE:-${RELEASE}-runner}"
CONTROLLER_NAMESPACE="agent-sandbox-system"
CONTROLLER_DEPLOYMENT="agent-sandbox-controller"
GATE_CONFIGMAP="${RELEASE}-rollout-recovery-gate"
SANDBOX_HOLD_KEY="curie-e2e.invalid/sandbox-rollout-recovery"
SANDBOX_HOLD_VALUE="blocked"
STAGING_BUDGET_SECONDS=120
MESSAGE_TIMEOUT_SECONDS=300
RECOVERY_BUDGET_SECONDS=180
WORKDIR="$(mktemp -d)"
ORIGINAL_TEMPLATE_FILE="$WORKDIR/sandbox-template.original.json"
CLAIMS_BEFORE_FILE="$WORKDIR/claims.before"
SANDBOXES_BEFORE_FILE="$WORKDIR/sandboxes.before"
FIRST_PID=""
SECOND_PID=""
TRANSFER_WATCHER_PID=""
BLOCKED_CLAIM=""
DEPLOYMENT_PATCHED=0
TEMPLATE_PATCHED=0
ORIGINAL_REVISION=""

if [[ -z "${CURIE_BIN:-}" || ! -x "${CURIE_BIN:-}" ]]; then
    echo "error: CURIE_BIN must name an executable curie binary" >&2
    exit 1
fi
if [[ -z "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
    echo "error: CURIE_E2E_LISTEN_HOST must name the pod-reachable callback host" >&2
    exit 1
fi
if [[ -n "$PRE_FIX_CLI_BIN" && ! -x "$PRE_FIX_CLI_BIN" ]]; then
    echo "error: CURIE_E2E_PRE_FIX_CLI_BIN must name an executable pre-fix CLI" >&2
    exit 1
fi
if [[ -n "$PRE_FIX_WORKER_IMAGE" || -n "$PRE_FIX_WORKER_TAG" ]]; then
    if [[ -z "$PRE_FIX_WORKER_IMAGE" || -z "$PRE_FIX_WORKER_TAG" ]]; then
        echo "error: CURIE_E2E_PRE_FIX_WORKER_IMAGE and CURIE_E2E_PRE_FIX_WORKER_TAG must be set together" >&2
        exit 1
    fi
fi
BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"
PHASE1_BIN="$BIN"
if [[ -n "$PRE_FIX_CLI_BIN" ]]; then
    PHASE1_BIN="$(cd "$(dirname "$PRE_FIX_CLI_BIN")" && pwd)/$(basename "$PRE_FIX_CLI_BIN")"
fi

stop_pid() {
    local pid="$1"
    [[ -n "$pid" ]] || return 0
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

worker_pods_json() {
    kubectl -n "$NAMESPACE" get pods \
        -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker" -o json
}

restore_sandbox_template() {
    local current patch expected actual
    (( TEMPLATE_PATCHED )) || return 0
    current="$(kubectl -n "$NAMESPACE" get sandboxtemplate "$SANDBOX_TEMPLATE" -o json)"
    patch="$(python3 - "$ORIGINAL_TEMPLATE_FILE" "$current" <<'PY'
import json, pathlib, sys
original = json.loads(pathlib.Path(sys.argv[1]).read_text())
current = json.loads(sys.argv[2])
path = "/spec/podTemplate/spec/nodeSelector"
original_spec = original["spec"]["podTemplate"]["spec"]
current_spec = current["spec"]["podTemplate"]["spec"]
if "nodeSelector" in original_spec:
    op = "replace" if "nodeSelector" in current_spec else "add"
    print(json.dumps([{"op": op, "path": path, "value": original_spec["nodeSelector"]}]))
elif "nodeSelector" in current_spec:
    print(json.dumps([{"op": "remove", "path": path}]))
else:
    print("[]")
PY
)"
    if [[ "$patch" != "[]" ]]; then
        kubectl -n "$NAMESPACE" patch sandboxtemplate "$SANDBOX_TEMPLATE" \
            --type=json -p "$patch" >/dev/null
    fi
    expected="$(python3 - "$ORIGINAL_TEMPLATE_FILE" <<'PY'
import json, pathlib, sys
spec = json.loads(pathlib.Path(sys.argv[1]).read_text())["spec"]["podTemplate"]["spec"]
print(json.dumps({"present": "nodeSelector" in spec, "value": spec.get("nodeSelector")}, sort_keys=True))
PY
)"
    actual="$(kubectl -n "$NAMESPACE" get sandboxtemplate "$SANDBOX_TEMPLATE" -o json | python3 -c '
import json,sys
spec=json.load(sys.stdin)["spec"]["podTemplate"]["spec"]
print(json.dumps({"present": "nodeSelector" in spec, "value": spec.get("nodeSelector")}, sort_keys=True))
')"
    [[ "$actual" == "$expected" ]] || {
        echo "error: SandboxTemplate nodeSelector did not restore exactly" >&2
        return 1
    }
    TEMPLATE_PATCHED=0
}

restore_worker_deployment() {
    local pod
    (( DEPLOYMENT_PATCHED )) || return 0
    [[ -n "$ORIGINAL_REVISION" ]] || {
        echo "error: cannot restore worker Deployment without its original revision" >&2
        return 1
    }
    # Make rollback safe even when failure happened before the gatekeeper was
    # started. Touching is immediate; opening the ConfigMap is the fallback for
    # a pod that is not exec-able yet and will see the projected update later.
    kubectl -n "$NAMESPACE" patch configmap "$GATE_CONFIGMAP" --type=merge \
        -p '{"data":{"ready":"open","stop":"open"}}' >/dev/null 2>&1 || true
    while read -r pod; do
        kubectl -n "$NAMESPACE" exec "$pod" -- \
            touch /tmp/curie-e2e-ready /tmp/curie-e2e-stop >/dev/null 2>&1 || true
    done < <(kubectl -n "$NAMESPACE" get pods \
        -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=worker" \
        -o name 2>/dev/null | sed 's#^pod/##')
    kubectl -n "$NAMESPACE" rollout undo "deployment/$WORKER_DEPLOYMENT" \
        --to-revision="$ORIGINAL_REVISION" >/dev/null
    kubectl -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" \
        --timeout=300s >/dev/null
    DEPLOYMENT_PATCHED=0
}

assert_release_healthy() {
    local component
    for component in api worker ui; do
        kubectl -n "$NAMESPACE" rollout status "deployment/${RELEASE}-${component}" \
            --timeout=300s >/dev/null
    done
    kubectl -n "$NAMESPACE" rollout status "statefulset/$VALKEY_STATEFULSET" \
        --timeout=300s >/dev/null
    kubectl -n "$NAMESPACE" rollout status "daemonset/${RELEASE}-runner-prewarm" \
        --timeout=300s >/dev/null
    # Read-only health proof: this harness never mutates the shared controller.
    kubectl -n "$CONTROLLER_NAMESPACE" rollout status \
        "deployment/$CONTROLLER_DEPLOYMENT" --timeout=300s >/dev/null
    # A Deployment can report complete while the replaced pod is still serving
    # its termination grace period.  Give that pod the same bounded opportunity
    # to disappear that the product gives it before making the final hygiene
    # assertion below.
    local terminating_deadline=$((SECONDS + 60))
    while (( SECONDS < terminating_deadline )); do
        if ! worker_pods_json | python3 -c '
import json,sys
pods=json.load(sys.stdin).get("items", [])
raise SystemExit(0 if any(p.get("metadata",{}).get("deletionTimestamp") for p in pods) else 1)
'; then
            break
        fi
        sleep 1
    done
    worker_pods_json | python3 -c '
import json,sys
pods=json.load(sys.stdin).get("items", [])
if not pods:
    raise SystemExit("worker release has no pods")
for pod in pods:
    name=pod.get("metadata",{}).get("name")
    if pod.get("metadata",{}).get("deletionTimestamp"):
        raise SystemExit("terminating worker remains: {}".format(name))
    if not any(c.get("type") == "Ready" and c.get("status") == "True"
               for c in pod.get("status",{}).get("conditions",[])):
        raise SystemExit("worker not Ready: {}".format(name))
'
    echo "release health: app rollouts, Valkey, runner prewarm, and shared controller are Ready"
}

cleanup() {
    local code=$?
    trap - EXIT INT TERM
    set +e
    stop_pid "$FIRST_PID"
    stop_pid "$SECOND_PID"
    stop_pid "$TRANSFER_WATCHER_PID"
    restore_sandbox_template || true
    if [[ -n "$BLOCKED_CLAIM" ]]; then
        kubectl -n "$NAMESPACE" delete sandboxclaim "$BLOCKED_CLAIM" \
            --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
    restore_worker_deployment || true
    kubectl -n "$NAMESPACE" delete configmap "$GATE_CONFIGMAP" \
        --ignore-not-found >/dev/null 2>&1 || true
    rm -rf -- "$WORKDIR"
    exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

valkey_json() {
    # The secret expands only inside the pod; it never enters host argv/state.
    # shellcheck disable=SC2016
    kubectl -n "$NAMESPACE" exec "statefulset/$VALKEY_STATEFULSET" -- \
        sh -c 'REDISCLI_AUTH="$VALKEY_PASSWORD" exec valkey-cli --json "$@"' sh "$@"
}

json_int() { python3 -c 'import json,sys; print(int(json.load(sys.stdin)))'; }
pending_rows() { valkey_json XPENDING "$STREAM" "$GROUP" - + 100; }
xinfo_consumers() { valkey_json XINFO CONSUMERS "$STREAM" "$GROUP"; }

consumer_info_pending() {
    local consumer="$1"
    xinfo_consumers | python3 -c '
import json,sys
consumer=sys.argv[1]
for row in json.load(sys.stdin):
    data=row if isinstance(row,dict) else dict(zip(row[0::2],row[1::2]))
    if data.get("name") == consumer:
        print(int(data.get("pending",-1)))
        break
else:
    print("missing")
' "$consumer"
}

consumer_for_pod() {
    local pod="$1"
    xinfo_consumers | python3 -c '
import json,sys
pod=sys.argv[1]
names=[]
for row in json.load(sys.stdin):
    data=row if isinstance(row,dict) else dict(zip(row[0::2],row[1::2]))
    name=data.get("name")
    if isinstance(name,str) and name.startswith(pod + "-"):
        names.append(name)
if len(names) == 1:
    print(names[0])
elif len(names) > 1:
    raise SystemExit("multiple XINFO consumers match pod {}: {}".format(pod,names))
' "$pod"
}

wait_consumer_for_pod() {
    local pod="$1" budget="$2" started=$SECONDS consumer
    while (( SECONDS - started < budget )); do
        consumer="$(consumer_for_pod "$pod")"
        if [[ -n "$consumer" ]]; then
            printf '%s\n' "$consumer"
            return 0
        fi
        sleep 0.25
    done
    echo "error: XINFO never reported a consumer belonging to worker pod $pod" >&2
    xinfo_consumers >&2 || true
    return 1
}

pending_for_consumer() {
    local consumer="$1"
    pending_rows | python3 -c '
import json,sys
consumer=sys.argv[1]
rows=[r for r in json.load(sys.stdin) if len(r)>=4 and r[1] == consumer]
if len(rows) == 1:
    row=rows[0]
    print("{}\t{}\t{}\t{}".format(row[0],row[1],int(row[2]),int(row[3])))
elif len(rows) > 1:
    raise SystemExit("consumer {} owns multiple unexpected PEL rows: {}".format(consumer,rows))
' "$consumer"
}

assert_pel_empty() {
    local label="$1" old_consumer="$2" rows total old info_old
    rows="$(pending_rows)"
    total="$(printf '%s' "$rows" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"
    old="$(printf '%s' "$rows" | python3 -c '
import json,sys
name=sys.argv[1]
print(sum(1 for row in json.load(sys.stdin) if len(row)>1 and row[1] == name))
' "$old_consumer")"
    info_old="$(consumer_info_pending "$old_consumer")"
    if [[ "$total" != "0" || "$old" != "0" || \
          ( "$info_old" != "0" && "$info_old" != "missing" ) ]]; then
        echo "$label: PEL not empty (total=$total, old=$old, xinfo=$info_old)" >&2
        printf '%s\n' "$rows" >&2
        xinfo_consumers >&2 || true
        return 1
    fi
    echo "$label: XPENDING empty; XINFO reports $old_consumer pending=0 or absent"
}

ready_worker() {
    worker_pods_json | python3 -c '
import json,sys
for pod in json.load(sys.stdin).get("items",[]):
    if pod.get("metadata",{}).get("deletionTimestamp"):
        continue
    if any(c.get("type") == "Ready" and c.get("status") == "True"
           for c in pod.get("status",{}).get("conditions",[])):
        print(pod["metadata"]["name"])
        break
'
}

wait_ready_worker_other_than() {
    local old="$1" budget="$2" started=$SECONDS pod
    while (( SECONDS - started < budget )); do
        pod="$(worker_pods_json | python3 -c '
import json,sys
old=sys.argv[1]
for pod in json.load(sys.stdin).get("items",[]):
    name=pod["metadata"]["name"]
    if name == old or pod["metadata"].get("deletionTimestamp"):
        continue
    if any(c.get("type") == "Ready" and c.get("status") == "True"
           for c in pod.get("status",{}).get("conditions",[])):
        print(name); break
' "$old")"
        if [[ -n "$pod" ]]; then printf '%s\n' "$pod"; return 0; fi
        sleep 0.25
    done
    echo "error: no Ready replacement worker appeared for $old" >&2
    return 1
}

wait_worker_pod_gone() {
    local pod="$1" budget="$2" started=$SECONDS resource
    while true; do
        if ! resource="$(kubectl -n "$NAMESPACE" get pod "$pod" \
            --ignore-not-found -o name)"; then
            echo "error: could not observe worker pod $pod during rollout" >&2
            return 1
        fi
        [[ -n "$resource" ]] || return 0
        if (( SECONDS - started >= budget )); then
            echo "error: worker pod $pod remained after its stop gate opened" >&2
            return 1
        fi
        sleep 0.25
    done
}

worker_generation() {
    kubectl -n "$NAMESPACE" get deployment "$WORKER_DEPLOYMENT" \
        -o jsonpath='{.metadata.generation}'
}

worker_observed_generation() {
    kubectl -n "$NAMESPACE" get deployment "$WORKER_DEPLOYMENT" \
        -o jsonpath='{.status.observedGeneration}'
}

worker_pod_identity() {
    worker_pods_json | python3 -c '
import json,sys
pods=[]
for pod in json.load(sys.stdin).get("items",[]):
    meta=pod.get("metadata",{})
    ready=any(c.get("type") == "Ready" and c.get("status") == "True"
              for c in pod.get("status",{}).get("conditions",[]))
    pods.append((meta.get("name"),meta.get("uid"),meta.get("deletionTimestamp"),ready))
print(json.dumps(sorted(pods),separators=(",",":")))
'
}

assert_worker_identity_unchanged() {
    local label="$1" generation="$2" observed="$3" pods="$4"
    local actual_generation actual_observed actual_pods
    actual_generation="$(worker_generation)"
    actual_observed="$(worker_observed_generation)"
    actual_pods="$(worker_pod_identity)"
    if [[ "$actual_generation" != "$generation" || \
          "$actual_observed" != "$observed" || "$actual_pods" != "$pods" ]]; then
        echo "error: $label mutated worker identity" >&2
        echo "expected generation=$generation observedGeneration=$observed pods=$pods" >&2
        echo "actual generation=$actual_generation observedGeneration=$actual_observed pods=$actual_pods" >&2
        return 1
    fi
}

wait_exec_touch() {
    local pod="$1" path="$2" budget="$3" started=$SECONDS
    while (( SECONDS - started < budget )); do
        if kubectl -n "$NAMESPACE" exec "$pod" -- touch "$path" >/dev/null 2>&1; then return 0; fi
        sleep 0.25
    done
    echo "error: worker pod $pod never accepted gate file $path" >&2
    return 1
}

wait_for_pid() {
    local pid="$1" budget="$2" started=$SECONDS
    while kill -0 "$pid" 2>/dev/null; do
        if (( SECONDS - started >= budget )); then return 124; fi
        sleep 0.25
    done
    wait "$pid"
}

assert_reply() {
    local label="$1" path="$2"
    python3 - "$label" "$path" <<'PY'
import json, pathlib, sys
label,path=sys.argv[1:]
raw=pathlib.Path(path).read_text()
try:
    value=json.loads(raw)
except Exception as exc:
    raise SystemExit(f"{label}: unparseable CLI JSON: {exc}: {raw[:500]}")
if value.get("finalized") is not True:
    raise SystemExit(f"{label}: turn did not finalize: {value}")
reply=value.get("reply")
if not isinstance(reply,str) or not reply.strip():
    raise SystemExit(f"{label}: finalized reply was empty: {value}")
print(f"{label}: same CLI process returned a finalized nonempty reply")
PY
}

snapshot_resource_names() {
    local resource="$1" path="$2"
    kubectl -n "$NAMESPACE" get "$resource" -o json | python3 -c '
import json,sys
for item in json.load(sys.stdin).get("items",[]):
    print(item["metadata"]["name"])
' >"$path"
}

new_resource_name() {
    local resource="$1" baseline="$2" exclude="${3:-}"
    kubectl -n "$NAMESPACE" get "$resource" -o json | python3 -c '
import json,pathlib,sys
baseline=set(pathlib.Path(sys.argv[1]).read_text().splitlines())
exclude=sys.argv[2]
items=[item for item in json.load(sys.stdin).get("items",[])
       if item["metadata"]["name"] not in baseline
       and item["metadata"]["name"] != exclude]
if items:
    items.sort(key=lambda item: item["metadata"].get("creationTimestamp",""))
    print(items[-1]["metadata"]["name"])
' "$baseline" "$exclude"
}

patch_sandbox_template_unschedulable() {
    local current patch
    kubectl -n "$NAMESPACE" get sandboxtemplate "$SANDBOX_TEMPLATE" -o json \
        >"$ORIGINAL_TEMPLATE_FILE"
    current="$(<"$ORIGINAL_TEMPLATE_FILE")"
    patch="$(python3 - "$current" "$SANDBOX_HOLD_KEY" "$SANDBOX_HOLD_VALUE" <<'PY'
import json,sys
obj=json.loads(sys.argv[1]); key,value=sys.argv[2:]
spec=obj["spec"]["podTemplate"]["spec"]
selector=dict(spec.get("nodeSelector") or {}); selector[key]=value
op="replace" if "nodeSelector" in spec else "add"
print(json.dumps([{"op":op,"path":"/spec/podTemplate/spec/nodeSelector","value":selector}]))
PY
)"
    kubectl -n "$NAMESPACE" patch sandboxtemplate "$SANDBOX_TEMPLATE" \
        --type=json -p "$patch" >/dev/null
    TEMPLATE_PATCHED=1
}

assert_unschedulable_sandbox_pod() {
    local sandbox="$1"
    kubectl -n "$NAMESPACE" get pod "$sandbox" -o json | python3 -c '
import json,sys
pod=json.load(sys.stdin); key,value=sys.argv[1:]
selector=pod.get("spec",{}).get("nodeSelector",{})
if selector.get(key) != value:
    raise SystemExit("sandbox pod lacks test-only nodeSelector: {}".format(selector))
if pod.get("spec",{}).get("nodeName"):
    raise SystemExit("sandbox unexpectedly scheduled on {}".format(pod["spec"]["nodeName"]))
if pod.get("status",{}).get("phase") != "Pending":
    raise SystemExit("sandbox phase is not Pending: {}".format(pod.get("status",{}).get("phase")))
' "$SANDBOX_HOLD_KEY" "$SANDBOX_HOLD_VALUE"
}

watch_pel_transfer() {
    local entry_id="$1" old_consumer="$2" old_deliveries="$3"
    # Poll beside Valkey so a fast completion cannot hide the transient row.
    # shellcheck disable=SC2016
    kubectl -n "$NAMESPACE" exec "statefulset/$VALKEY_STATEFULSET" -- sh -c '
set -eu
stream=$1; group=$2; entry_id=$3; old_consumer=$4; old_deliveries=$5
deadline=$(( $(date +%s) + 190 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    raw="$(REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli --raw XPENDING \
        "$stream" "$group" "$entry_id" "$entry_id" 1 2>/dev/null || true)"
    # shellcheck disable=SC2086
    set -- $raw
    if [ "$#" -ge 4 ] && [ "$1" = "$entry_id" ] && \
       [ "$2" != "$old_consumer" ] && [ "$4" -gt "$old_deliveries" ]; then
        printf "%s\t%s\t%s\t%s\n" "$1" "$2" "$3" "$4"
        exit 0
    fi
    sleep 0.05
done
exit 124
' sh "$STREAM" "$GROUP" "$entry_id" "$old_consumer" "$old_deliveries"
}

claim_created_after() {
    local claim="$1" instant="$2"
    kubectl -n "$NAMESPACE" get sandboxclaim "$claim" -o json | python3 -c '
import datetime,json,sys
created=json.load(sys.stdin)["metadata"]["creationTimestamp"]
instant=sys.argv[1]
parse=lambda value: datetime.datetime.fromisoformat(value.replace("Z","+00:00"))
if parse(created) < parse(instant):
    raise SystemExit("claim {} predates termination {}".format(created,instant))
' "$instant"
}

echo "red-on-revert base ref: $BASE_REF"
if [[ -n "$PRE_FIX_CLI_BIN" ]]; then echo "phase 1 uses pre-fix CLI $PHASE1_BIN"; fi
if [[ -n "$PRE_FIX_WORKER_IMAGE" ]]; then
    echo "phase 2 uses pre-fix worker ${PRE_FIX_WORKER_IMAGE}:${PRE_FIX_WORKER_TAG}"
fi

echo "=== deploy examples/weather through the real cluster CLI ==="
cp -a "$REPO_ROOT/examples/weather" "$WORKDIR/weather"
"$BIN" --json cluster deploy --plugin-dir "$WORKDIR/weather" \
    --namespace "$NAMESPACE" --release "$RELEASE" >"$WORKDIR/deploy.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["deployment"]["status"] == "active"' \
    "$WORKDIR/deploy.json"

ORIGINAL_REVISION="$(kubectl -n "$NAMESPACE" get deployment "$WORKER_DEPLOYMENT" \
    -o jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}')"

echo "=== install TEST-ONLY deterministic worker rollout gate ==="
kubectl -n "$NAMESPACE" create configmap "$GATE_CONFIGMAP" \
    --from-literal=ready=open --from-literal=stop=open >/dev/null
kubectl -n "$NAMESPACE" patch deployment "$WORKER_DEPLOYMENT" --type=strategic -p "$(python3 - "$GATE_CONFIGMAP" <<'PY'
import json,sys
name=sys.argv[1]
print(json.dumps({"spec":{"template":{"spec":{
    "volumes":[{"name":"rollout-recovery-gate","configMap":{"name":name}}],
    "containers":[{"name":"worker","command":["/bin/sh","-ec"],
        "args":["exec python -m curie_worker"],
        "volumeMounts":[{"name":"rollout-recovery-gate","mountPath":"/var/run/curie-e2e-gate","readOnly":True}],
        "readinessProbe":{"exec":{"command":["/bin/sh","-ec","test -e /tmp/curie-e2e-ready || grep -qx open /var/run/curie-e2e-gate/ready"]},"initialDelaySeconds":1,"periodSeconds":1,"timeoutSeconds":1,"failureThreshold":1},
        "lifecycle":{"preStop":{"exec":{"command":["/bin/sh","-ec","until test -e /tmp/curie-e2e-stop || grep -qx open /var/run/curie-e2e-gate/stop; do sleep 0.2; done"]}}}
    }]}}}}))
PY
)" >/dev/null
DEPLOYMENT_PATCHED=1
kubectl -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" --timeout=300s
# A completed rollout can still have a terminating predecessor. Settle that
# setup transition before choosing the consumer and freezing the full identity
# invariant for the first message; keep every pod in the later comparisons.
assert_release_healthy
OLD_POD="$(ready_worker)"
[[ -n "$OLD_POD" ]] || { echo "error: no Ready baseline worker" >&2; exit 1; }
OLD_CONSUMER="$(wait_consumer_for_pod "$OLD_POD" 60)"
wait_exec_touch "$OLD_POD" /tmp/curie-e2e-ready 30
kubectl -n "$NAMESPACE" patch configmap "$GATE_CONFIGMAP" --type=merge \
    -p '{"data":{"ready":"hold","stop":"hold"}}' >/dev/null
for _ in $(seq 1 360); do
    if kubectl -n "$NAMESPACE" exec "$OLD_POD" -- \
        grep -qx hold /var/run/curie-e2e-gate/ready 2>/dev/null; then break; fi
    sleep 0.5
done
kubectl -n "$NAMESPACE" exec "$OLD_POD" -- grep -qx hold /var/run/curie-e2e-gate/ready
BASE_GENERATION="$(worker_generation)"
BASE_OBSERVED_GENERATION="$(worker_observed_generation)"
BASE_POD_IDENTITY="$(worker_pod_identity)"

echo "=== first invocation: reply without worker rollout or pod replacement ==="
"$PHASE1_BIN" --json cluster message "What is the weather?" \
    --namespace "$NAMESPACE" --release "$RELEASE" \
    --listen-host "$CURIE_E2E_LISTEN_HOST" --timeout-secs "$MESSAGE_TIMEOUT_SECONDS" \
    >"$WORKDIR/first.json" 2>"$WORKDIR/first.err" &
FIRST_PID=$!
FIRST_STARTED=$SECONDS
while kill -0 "$FIRST_PID" 2>/dev/null; do
    if (( SECONDS - FIRST_STARTED >= MESSAGE_TIMEOUT_SECONDS )); then
        cat "$WORKDIR/first.err" >&2 || true
        echo "error: first cluster message exceeded ${MESSAGE_TIMEOUT_SECONDS}s" >&2
        exit 1
    fi
    assert_worker_identity_unchanged "first invocation in flight" \
        "$BASE_GENERATION" "$BASE_OBSERVED_GENERATION" "$BASE_POD_IDENTITY"
    sleep 0.25
done
if wait_for_pid "$FIRST_PID" "$MESSAGE_TIMEOUT_SECONDS"; then
    :
else
    status=$?
    cat "$WORKDIR/first.err" >&2 || true
    echo "error: first cluster message failed or exceeded ${MESSAGE_TIMEOUT_SECONDS}s (status $status)" >&2
    exit 1
fi
FIRST_PID=""
assert_worker_identity_unchanged "completed first invocation" \
    "$BASE_GENERATION" "$BASE_OBSERVED_GENERATION" "$BASE_POD_IDENTITY"
assert_reply "first invocation" "$WORKDIR/first.json"
assert_pel_empty "first invocation" "$OLD_CONSUMER"
echo "first invocation kept worker generation=$BASE_GENERATION observedGeneration=$BASE_OBSERVED_GENERATION pods=$BASE_POD_IDENTITY"

echo "=== deliberate termination: recover a real PEL entry promptly ==="
kubectl -n "$NAMESPACE" patch configmap "$GATE_CONFIGMAP" --type=merge \
    -p '{"data":{"ready":"open","stop":"open"}}' >/dev/null
# The projected ConfigMap can lag behind the Deployment update. Open the old
# pod's stop gate directly so it cannot keep consuming during preStop after the
# intentional rollout, then wait for that exact pod to disappear before
# selecting the recovery consumer.
wait_exec_touch "$OLD_POD" /tmp/curie-e2e-stop 30
TRUST_ORIGIN="http://${CURIE_E2E_LISTEN_HOST}"
kubectl -n "$NAMESPACE" set env "deployment/$WORKER_DEPLOYMENT" \
    "CURIE_SLACK_TRUSTED_ORIGINS=$TRUST_ORIGIN" >/dev/null
kubectl -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" --timeout=300s >/dev/null
wait_worker_pod_gone "$OLD_POD" 120
if [[ -n "$PRE_FIX_WORKER_IMAGE" ]]; then
    kubectl -n "$NAMESPACE" set image "deployment/$WORKER_DEPLOYMENT" \
        "worker=${PRE_FIX_WORKER_IMAGE}:${PRE_FIX_WORKER_TAG}" >/dev/null
    kubectl -n "$NAMESPACE" rollout status "deployment/$WORKER_DEPLOYMENT" --timeout=300s >/dev/null
fi
RECOVERY_POD="$(ready_worker)"
[[ -n "$RECOVERY_POD" ]] || { echo "error: no Ready worker for termination phase" >&2; exit 1; }
RECOVERY_CONSUMER="$(wait_consumer_for_pod "$RECOVERY_POD" 60)"
patch_sandbox_template_unschedulable
snapshot_resource_names sandboxclaims "$CLAIMS_BEFORE_FILE"
snapshot_resource_names sandboxes "$SANDBOXES_BEFORE_FILE"

SECOND_STARTED=$SECONDS
"$BIN" --json cluster message "Give me a fresh weather reply after recovery." \
    --namespace "$NAMESPACE" --release "$RELEASE" \
    --listen-host "$CURIE_E2E_LISTEN_HOST" --timeout-secs "$MESSAGE_TIMEOUT_SECONDS" \
    >"$WORKDIR/second.json" 2>"$WORKDIR/second.err" &
SECOND_PID=$!
STAGING_STARTED=$SECONDS
PEL_ROW=""
BLOCKED_SANDBOX=""
while (( SECONDS - STAGING_STARTED < STAGING_BUDGET_SECONDS )); do
    PEL_ROW="$(pending_for_consumer "$RECOVERY_CONSUMER")"
    BLOCKED_CLAIM="$(new_resource_name sandboxclaims "$CLAIMS_BEFORE_FILE")"
    BLOCKED_SANDBOX="$(new_resource_name sandboxes "$SANDBOXES_BEFORE_FILE")"
    if [[ -n "$PEL_ROW" && -n "$BLOCKED_CLAIM" && -n "$BLOCKED_SANDBOX" ]] && \
       kubectl -n "$NAMESPACE" get pod "$BLOCKED_SANDBOX" >/dev/null 2>&1 && \
       assert_unschedulable_sandbox_pod "$BLOCKED_SANDBOX" 2>/dev/null; then
        break
    fi
    kill -0 "$SECOND_PID" 2>/dev/null || break
    sleep 0.25
done
STAGING_SECONDS=$((SECONDS - STAGING_STARTED))
if [[ -z "$PEL_ROW" || -z "$BLOCKED_CLAIM" || -z "$BLOCKED_SANDBOX" ]]; then
    cat "$WORKDIR/second.err" >&2 || true
    echo "error: staging did not produce a consumer-owned PEL row plus unschedulable SandboxClaim/Sandbox within ${STAGING_BUDGET_SECONDS}s" >&2
    pending_rows >&2 || true
    kubectl -n "$NAMESPACE" get sandboxclaims,sandboxes,pods -o wide >&2 || true
    exit 1
fi
assert_unschedulable_sandbox_pod "$BLOCKED_SANDBOX"
if ! kill -0 "$SECOND_PID" 2>/dev/null; then
    if wait "$SECOND_PID"; then status=0; else status=$?; fi
    SECOND_PID=""
    cat "$WORKDIR/second.err" >&2 || true
    cat "$WORKDIR/second.json" >&2 || true
    echo "error: phase 2 CLI returned early (status $status) while its real Sandbox remained unschedulable; a capacity reply or early acknowledgement is not recovery proof" >&2
    exit 1
fi
IFS=$'\t' read -r ENTRY_ID ENTRY_OWNER ENTRY_IDLE ENTRY_DELIVERIES <<<"$PEL_ROW"
[[ "$ENTRY_OWNER" == "$RECOVERY_CONSUMER" && "$ENTRY_DELIVERIES" == "1" ]] || {
    echo "error: staged PEL row is not delivery 1 owned by $RECOVERY_CONSUMER: $PEL_ROW" >&2
    exit 1
}
[[ "$(consumer_info_pending "$RECOVERY_CONSUMER")" == "1" ]] || {
    echo "error: XINFO does not report one pending entry for exact consumer $RECOVERY_CONSUMER" >&2
    exit 1
}
(( STAGING_SECONDS <= STAGING_BUDGET_SECONDS )) || {
    echo "error: staging took ${STAGING_SECONDS}s, above ${STAGING_BUDGET_SECONDS}s" >&2
    exit 1
}
echo "staged in ${STAGING_SECONDS}s: entry $ENTRY_ID idle=${ENTRY_IDLE}ms delivery=$ENTRY_DELIVERIES belongs to $RECOVERY_CONSUMER while Sandbox $BLOCKED_SANDBOX is Pending"

watch_pel_transfer "$ENTRY_ID" "$RECOVERY_CONSUMER" "$ENTRY_DELIVERIES" \
    >"$WORKDIR/transfer.tsv" &
TRANSFER_WATCHER_PID=$!
TERMINATED_RFC3339="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TERMINATED_AT=$SECONDS
kubectl -n "$NAMESPACE" delete pod "$RECOVERY_POD" \
    --force --grace-period=0 --wait=true --timeout=60s >/dev/null
restore_sandbox_template
HEARTBEAT_KEY="${STREAM}:consumer-heartbeat:${GROUP}:${RECOVERY_CONSUMER}"
ABSENCE_WAIT_STARTED=$SECONDS
while [[ "$(valkey_json EXISTS "$HEARTBEAT_KEY" | json_int)" != "0" ]]; do
    (( SECONDS - ABSENCE_WAIT_STARTED < 60 )) || {
        echo "error: terminated consumer alive lease remained present for 60s" >&2
        exit 1
    }
    sleep 0.1
done
FIRST_ABSENCE_AT=$SECONDS
echo "first confirmed alive-lease absence observed ${FIRST_ABSENCE_AT}s after harness start; prompt claim still requires the independent second observation"
REPLACEMENT_POD="$(wait_ready_worker_other_than "$RECOVERY_POD" 120)"
REPLACEMENT_CONSUMER="$(wait_consumer_for_pod "$REPLACEMENT_POD" 60)"

remaining=$((RECOVERY_BUDGET_SECONDS - (SECONDS - TERMINATED_AT)))
if (( remaining <= 0 )) || ! wait_for_pid "$TRANSFER_WATCHER_PID" "$remaining"; then
    stop_pid "$TRANSFER_WATCHER_PID"
    TRANSFER_WATCHER_PID=""
    if [[ -n "$PRE_FIX_WORKER_IMAGE" ]]; then
        echo "error: phase 2 red-on-revert assertion failed against $BASE_REF: pre-fix worker ${PRE_FIX_WORKER_IMAGE}:${PRE_FIX_WORKER_TAG} did not transfer the PEL entry within ${RECOVERY_BUDGET_SECONDS}s" >&2
    else
        echo "error: PEL ownership did not transfer with incremented times_delivered within ${RECOVERY_BUDGET_SECONDS}s" >&2
    fi
    exit 1
fi
TRANSFER_WATCHER_PID=""
IFS=$'\t' read -r TRANSFER_ID TRANSFER_OWNER TRANSFER_IDLE TRANSFER_DELIVERIES \
    <"$WORKDIR/transfer.tsv"
[[ "$TRANSFER_ID" == "$ENTRY_ID" && "$TRANSFER_OWNER" == "$REPLACEMENT_CONSUMER" && \
   "$TRANSFER_DELIVERIES" -gt "$ENTRY_DELIVERIES" ]] || {
    echo "error: transfer does not match replacement consumer or increment times_delivered: $(<"$WORKDIR/transfer.tsv")" >&2
    echo "replacement pod=$REPLACEMENT_POD consumer=$REPLACEMENT_CONSUMER" >&2
    exit 1
}
echo "observed PEL transfer before clear: owner $RECOVERY_CONSUMER -> $TRANSFER_OWNER, idle reset to ${TRANSFER_IDLE}ms, times_delivered $ENTRY_DELIVERIES -> $TRANSFER_DELIVERIES"
DETECT_TO_TRANSFER_SECONDS=$((SECONDS - FIRST_ABSENCE_AT))
echo "detect-to-transfer=${DETECT_TO_TRANSFER_SECONDS}s from first confirmed lease absence"

REOPENED_CLAIM=""
while (( SECONDS - TERMINATED_AT < RECOVERY_BUDGET_SECONDS )); do
    REOPENED_CLAIM="$(new_resource_name sandboxclaims "$CLAIMS_BEFORE_FILE" "$BLOCKED_CLAIM")"
    [[ -n "$REOPENED_CLAIM" ]] && break
    sleep 0.25
done
[[ -n "$REOPENED_CLAIM" ]] || {
    echo "error: replacement delivery never opened a fresh post-termination SandboxClaim" >&2
    exit 1
}
claim_created_after "$REOPENED_CLAIM" "$TERMINATED_RFC3339"
echo "post-termination reopen transition: replacement created SandboxClaim $REOPENED_CLAIM"

remaining=$((RECOVERY_BUDGET_SECONDS - (SECONDS - TERMINATED_AT)))
status=0
if (( remaining <= 0 )); then
    status=124
elif wait_for_pid "$SECOND_PID" "$remaining"; then
    :
else
    status=$?
fi
if (( status != 0 )); then
    cat "$WORKDIR/second.err" >&2 || true
    echo "error: terminated-consumer message failed or exceeded ${RECOVERY_BUDGET_SECONDS}s after termination (status $status)" >&2
    exit 1
fi
SECOND_PID=""
RECOVERY_SECONDS=$((SECONDS - TERMINATED_AT))
TOTAL_SECONDS=$((SECONDS - SECOND_STARTED))
(( RECOVERY_SECONDS < RECOVERY_BUDGET_SECONDS )) || {
    echo "error: recovery took ${RECOVERY_SECONDS}s after termination, not <${RECOVERY_BUDGET_SECONDS}s" >&2
    exit 1
}
assert_reply "deliberate termination" "$WORKDIR/second.json"
assert_pel_empty "deliberate termination" "$RECOVERY_CONSUMER"
echo "deliberate termination recovered in ${RECOVERY_SECONDS}s (${TOTAL_SECONDS}s total), below ${RECOVERY_BUDGET_SECONDS}s and far below 900s"

kubectl -n "$NAMESPACE" delete sandboxclaim "$BLOCKED_CLAIM" \
    --ignore-not-found --wait=false >/dev/null
BLOCKED_CLAIM=""
restore_worker_deployment
kubectl -n "$NAMESPACE" delete configmap "$GATE_CONFIGMAP" --ignore-not-found >/dev/null
assert_release_healthy
echo "QA4-02 ROLLOUT-FREE/PEL RECOVERY PASS"
