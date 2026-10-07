#!/usr/bin/env bash
# Issue #4005: execute the actual controller-ready Helm hook in an owned kind
# cluster. Keep the rendered SandboxWarmPool at zero replicas: its successful
# reconciliation establishes the durable metric without starting runner pods.
# Issue #4197: an upgrade that does not restart a controller which restarted
# once long before must pass on the stable serving leader and stay deployed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
CLUSTER=test-4005-controller-preflight
CONTEXT="kind-$CLUSTER"
NAMESPACE=test-4005-controller-preflight
RELEASE=curie-controller-preflight
CONTROLLER_NS=agent-sandbox-system
DEPLOY=agent-sandbox-controller
READ_ROLE=agent-sandbox-controller-networkpolicies-read
LEASE=a3317529.agent-sandbox.x-k8s.io
HOOK="$RELEASE-preflight-controller"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for command in kind docker kubectl helm python3; do
  command -v "$command" >/dev/null || fail "required command is unavailable: $command"
done
python3 -c 'import yaml' || fail "PyYAML is required for the exact negative-control post-renderer"
clusters="$(kind get clusters)" || fail "cannot inventory kind clusters"
if grep -Fxq "$CLUSTER" <<< "$clusters"; then
  fail "refusing a pre-existing cluster named $CLUSTER"
fi
owned_containers="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER")" \
  || fail "cannot inventory kind node ownership"
[[ -z "$owned_containers" ]] || fail "refusing pre-existing kind node containers for $CLUSTER"

mkdir -p "$REPO_ROOT/.projects/4005-controller-preflight"
TMP="$(mktemp -d "$REPO_ROOT/.projects/4005-controller-preflight/run.XXXXXX")"
export KUBECONFIG="$TMP/kubeconfig"
cluster_owned=0
scenario_passed=0

cleanup() {
  local rc=$? cleanup_failed=0 remaining_clusters remaining_containers
  trap - EXIT INT TERM
  set +e
  if [[ "$cluster_owned" -eq 1 ]]; then
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" \
      || cleanup_failed=1
    if remaining_clusters="$(kind get clusters)"; then
      if grep -Fxq "$CLUSTER" <<< "$remaining_clusters"; then
        cleanup_failed=1
      fi
    else
      cleanup_failed=1
    fi
    if remaining_containers="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$CLUSTER")"; then
      [[ -z "$remaining_containers" ]] || cleanup_failed=1
    else
      cleanup_failed=1
    fi
    if [[ "$cleanup_failed" -eq 0 ]]; then
      echo "Cleanup confirmed: owned kind cluster and node containers are gone, including all namespace and cluster resources."
    else
      echo "FAIL: could not confirm deletion of owned kind cluster $CLUSTER" >&2
      [[ "$rc" -ne 0 ]] || rc=1
    fi
  fi
  rm -rf "$TMP"
  if [[ "$rc" -eq 0 && "$scenario_passed" -eq 1 ]]; then
    echo "PASS: fresh install passes, a rotated-log upgrade passes from successful reconcile metrics without replacing or restarting the controller, an upgrade over a long-serving leader with an old restart passes and stays deployed, and an RBAC crash-loop fails the actual upgrade hook."
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Exercise the release archive generated from this candidate, with exactly the
# same package for the fresh install and both upgrades.
CANDIDATE_CHART="$TMP/$(python3 - "$CHART/Chart.yaml" <<'PY'
import sys

import yaml

with open(sys.argv[1]) as source:
    chart = yaml.safe_load(source)
print(f"{chart['name']}-{chart['version']}.tgz")
PY
)"
helm package "$CHART" --kube-context "$CONTEXT" --destination "$TMP" \
  > "$TMP/package.log" 2>&1 || fail "candidate release chart packaging failed"
[[ -f "$CANDIDATE_CHART" ]] || fail "packaging did not create the expected candidate chart archive"

# Register ownership before creation so even a partial startup is cleaned up.
# The private kubeconfig and explicit context keep every operation on this run.
cluster_owned=1
kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" --wait 120s
kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

values=(
  -f "$CHART/values-e2e-nogvisor.yaml"
  -f "$CHART/values-e2e-harness.yaml"
  --set agentSandbox.deploy=true
  --set agentSandbox.controller.deploy=true
  --set agentSandbox.warmPool.replicas=0
  --set rustfs.deploy=false
  --set-string rustfs.host=object-store.example.com
  --set-string 'rustfs.egress[0].cidr=192.0.2.0/24'
  --set-string 'rustfs.egress[0].ports[0].protocol=TCP'
  --set 'rustfs.egress[0].ports[0].port=443'
  --set security.checkDefaultCredentials=false
)

echo "Installing the packaged candidate chart with its actual hooks enabled."
if ! helm install "$RELEASE" "$CANDIDATE_CHART" --kube-context "$CONTEXT" \
    --namespace "$NAMESPACE" "${values[@]}" --timeout 300s; then
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK" || true
  fail "fresh install did not pass the actual controller-ready hook"
fi
install_logs="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK")"
grep -q 'RESULT: PASS' <<< "$install_logs" \
  || fail "fresh install hook did not report RESULT: PASS"
printf '%s\n' "$install_logs"

pod_names="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pods \
  -l "app=$DEPLOY" -o jsonpath='{.items[*].metadata.name}')"
read -r -a pods <<< "$pod_names"
[[ "${#pods[@]}" -eq 1 ]] || fail "expected exactly one controller pod, got: $pod_names"
pod="${pods[0]}"
uid="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.metadata.uid}')"
restarts="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
[[ "$restarts" == 0 ]] || fail "fresh controller has unexpected restartCount=$restarts"

successful_reconciles() {
  local metrics
  metrics="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get \
    --raw "/api/v1/namespaces/$CONTROLLER_NS/pods/$pod:8080/proxy/metrics")" || return 1
  # v0.5.0 pins controller-runtime v0.23.3, whose Prometheus CounterVec has
  # controller/result labels and increments result="success" after reconcile:
  # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/go.mod
  # https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.3/pkg/internal/controller/metrics/metrics.go
  # https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.3/pkg/internal/controller/controller.go
  python3 -c '
import re
import sys

total = 0.0
for line in sys.stdin:
    sample = re.fullmatch(r"controller_runtime_reconcile_total\{([^}]*)\}\s+(\S+)\s*", line.strip())
    if sample and re.search(r"(?:^|,)result=\"success\"(?:,|$)", sample[1]):
        total += float(sample[2])
print(total)
' <<< "$metrics"
}

metrics_ready=0
for ((attempt=0; attempt<90; attempt++)); do
  if total="$(successful_reconciles 2>/dev/null)" \
      && awk -v total="$total" 'BEGIN { exit !(total > 0) }'; then
    metrics_ready=1
    break
  fi
  sleep 2
done
[[ "$metrics_ready" -eq 1 ]] || fail "controller never exported a positive successful-reconcile counter"
echo "Controller before rotation: uid=$uid restartCount=$restarts successful_reconciles=$total"

# Truncate the actual CRI container log on the owned kind node. The controller
# process, Deployment, Pod UID, and restart count all remain unchanged.
node="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.spec.nodeName}')"
container_id="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].containerID}')"
container_id="${container_id#containerd://}"
log_path="$(docker exec "$node" crictl inspect "$container_id" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["status"]["logPath"])')"
[[ "$log_path" == /var/log/pods/"${CONTROLLER_NS}_${pod}_${uid}/$DEPLOY/"*.log ]] \
  || fail "CRI log path is outside the recorded controller pod"
docker exec "$node" sh -c 'test -f "$1" && truncate -s 0 "$1"' sh "$log_path"
rotated_logs="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" logs "$pod")"
startup_count="$(grep -c 'Starting workers' <<< "$rotated_logs" || true)"
[[ "$startup_count" == 0 ]] || fail "actual controller log still contains Starting workers after rotation"
echo "Controller after rotation: Starting workers count=$startup_count"

if ! helm upgrade "$RELEASE" "$CANDIDATE_CHART" --kube-context "$CONTEXT" \
    --namespace "$NAMESPACE" "${values[@]}" --timeout 300s; then
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK" || true
  fail "healthy rotated-log controller upgrade failed the actual controller-ready hook"
fi
upgrade_logs="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK")"
# The leader may already have served past stableLeaderSeconds on a slow
# runner, in which case the Lease signal (checked first) passes instead.
grep -Eqi 'RESULT: PASS.*(reconcile|rollout complete and serving)' <<< "$upgrade_logs" \
  || fail "rotated-log upgrade did not report a metrics- or leader-based RESULT: PASS"
printf '%s\n' "$upgrade_logs"
after_uid="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.metadata.uid}')"
after_restarts="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
[[ "$after_uid" == "$uid" && "$after_restarts" == "$restarts" ]] \
  || fail "rotated-log upgrade changed controller identity or restart count"
echo "Controller after upgrade: uid=$after_uid restartCount=$after_restarts"

# Issue #4197: restart the controller container once (same pod, so the
# upgrade below does not replace it), let the new process lead past
# stableLeaderSeconds, then upgrade. Before the fix the restart alone failed
# the hook on every later upgrade that did not restart the controller.
stable_leader_seconds=180
container_id="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].containerID}')"
docker exec "$node" crictl stop "${container_id#containerd://}" >/dev/null
restarted=0
for ((attempt=0; attempt<60; attempt++)); do
  restarts="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  ready="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].ready}')"
  if [[ "$restarts" =~ ^[0-9]+$ && "$restarts" -gt "$after_restarts" && "$ready" == true ]]; then
    restarted=1
    break
  fi
  sleep 2
done
[[ "$restarted" -eq 1 ]] || fail "controller container did not restart in place"
leader_held=0
for ((attempt=0; attempt<150; attempt++)); do
  read -r holder acquired renewed < <(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get lease "$LEASE" \
    -o jsonpath='{.spec.holderIdentity} {.spec.acquireTime} {.spec.renewTime}{"\n"}')
  held="$(python3 - "$acquired" "$renewed" <<'PY'
import sys
from datetime import datetime

a, b = (datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ") for t in sys.argv[1:3])
print(int((b - a).total_seconds()))
PY
)"
  if [[ "${holder%%_*}" == "$pod" && "$held" -ge $((stable_leader_seconds + 10)) ]]; then
    leader_held=1
    break
  fi
  sleep 2
done
[[ "$leader_held" -eq 1 ]] || fail "restarted controller did not lead past ${stable_leader_seconds}s"
echo "Controller before leader upgrade: restartCount=$restarts holder=$holder held=${held}s"

if ! helm upgrade "$RELEASE" "$CANDIDATE_CHART" --kube-context "$CONTEXT" \
    --namespace "$NAMESPACE" "${values[@]}" --timeout 300s; then
  kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK" || true
  fail "upgrade over a long-serving controller with an old restart failed the controller-ready hook"
fi
leader_logs="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK")"
grep -q 'RESULT: PASS.*rollout complete and serving' <<< "$leader_logs" \
  || fail "upgrade over a long-serving controller did not pass on the stable-leader signal"
printf '%s\n' "$leader_logs"
release_status="$(helm status "$RELEASE" --kube-context "$CONTEXT" --namespace "$NAMESPACE" -o json \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["info"]["status"])')"
[[ "$release_status" == deployed ]] || fail "release is $release_status after a healthy no-restart upgrade"
leader_uid="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.metadata.uid}')"
leader_restarts="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
[[ "$leader_uid" == "$uid" && "$leader_restarts" == "$restarts" ]] \
  || fail "leader upgrade changed controller identity or restart count"
echo "Controller after leader upgrade: uid=$leader_uid restartCount=$leader_restarts release=$release_status"

# Remove only the NetworkPolicy read grant and restart its controller. The
# negative upgrade's post-renderer removes that same one ClusterRole so Helm
# cannot repair the deliberate fault before the real hook observes it.
cat > "$TMP/drop-read-role.py" <<'PY'
#!/usr/bin/env python3
import re
import sys

import yaml

removed = 0
for document in re.split(r"(?m)^---[ \t]*\r?\n", sys.stdin.read()):
    parsed = yaml.safe_load(document)
    if parsed and parsed.get("kind") == "ClusterRole" and parsed.get("metadata", {}).get("name") == "agent-sandbox-controller-networkpolicies-read":
        removed += 1
    else:
        sys.stdout.write("---\n" + document)
if removed != 1:
    sys.exit("expected exactly one controller NetworkPolicy read ClusterRole")
PY
chmod +x "$TMP/drop-read-role.py"
kubectl --context "$CONTEXT" delete clusterrole "$READ_ROLE"
kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" delete pod "$pod" --wait=true

crash_loop=0
for ((attempt=0; attempt<100; attempt++)); do
  negative_pod="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pods \
    -l "app=$DEPLOY" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$negative_pod" ]]; then
    negative_logs="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" logs "$negative_pod" 2>/dev/null || true)"
    previous_logs="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" logs "$negative_pod" --previous 2>/dev/null || true)"
    negative_restarts="$(kubectl --context "$CONTEXT" -n "$CONTROLLER_NS" get pod "$negative_pod" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || true)"
    if [[ "$negative_restarts" =~ ^[0-9]+$ && "$negative_restarts" -gt 0 ]] \
        && printf '%s\n%s\n' "$negative_logs" "$previous_logs" | grep -Eq 'networkpolicies.*[Ff]orbidden'; then
      crash_loop=1
      break
    fi
  fi
  sleep 2
done
[[ "$crash_loop" -eq 1 ]] || fail "removed NetworkPolicy read grant did not produce the expected RBAC crash-loop signature"
echo "Negative controller: restartCount=$negative_restarts, forbidden-networkpolicies signature observed."

if helm upgrade "$RELEASE" "$CANDIDATE_CHART" --kube-context "$CONTEXT" \
    --namespace "$NAMESPACE" "${values[@]}" --timeout 300s \
    --post-renderer "$TMP/drop-read-role.py"; then
  fail "upgrade accepted a controller whose NetworkPolicy informer cannot sync"
fi
negative_hook_logs="$(kubectl --context "$CONTEXT" -n "$NAMESPACE" logs "job/$HOOK")"
grep -q 'RESULT: FAIL' <<< "$negative_hook_logs" \
  && grep -q 'forbidden-networkpolicies logged' <<< "$negative_hook_logs" \
  || fail "failed upgrade hook did not classify the missing NetworkPolicy read grant as RBAC"
if kubectl --context "$CONTEXT" get clusterrole "$READ_ROLE" >/dev/null 2>&1; then
  fail "negative upgrade restored the deliberately removed NetworkPolicy read grant"
fi
printf '%s\n' "$negative_hook_logs"
scenario_passed=1
