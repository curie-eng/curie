#!/usr/bin/env bash
# #4017 cluster proof: a plain cluster down removes the release and its owned
# namespaces, and a ConfigMap finalizer makes that down exit 3 within the
# bound while leaving the finalizer in place (#707, #767, #768). The CLI
# carries the delete bound. This script does not.
#
# Usage:
#   bash cli/scripts/cluster-down-namespace-bound.sh [--keep]
#   bash cli/scripts/cluster-down-namespace-bound.sh --self-test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KIND_CLUSTER=test-4017-cluster-down
NS=test-4017-down
RELEASE=t4017
KUBECONFIG="$REPO_ROOT/.projects/4017/kubeconfig"
CURIE_BIN="${CURIE_BIN:-/home/theconnman/.cargo/shared-targets/agentos/debug/curie}"
KEEP=0
SELF_TEST=0
CREATED=0
DOWN_SECONDS=0

for arg in "$@"; do
  case "$arg" in
    --self-test) SELF_TEST=1 ;;
    --keep) KEEP=1 ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

self_test() {
  local needle missing=0
  for needle in \
    "example.com/hold" \
    "330" \
    "test-4017-cluster-down" \
    "test-4017-down" \
    "t4017" \
    "kind delete"
  do
    if ! grep -q -F -- "$needle" "$0"; then
      echo "self-test: $0 does not contain $needle" >&2
      missing=1
    fi
  done
  exit "$missing"
}

if [[ "$SELF_TEST" -eq 1 ]]; then
  self_test
fi

case "$KIND_CLUSTER" in
  k8|Prod*|Staging*)
    echo "refusing context $KIND_CLUSTER" >&2
    exit 1
    ;;
esac

if [[ ! -x "$CURIE_BIN" ]]; then
  echo "cluster up is missing: $CURIE_BIN is not executable" >&2
  exit 1
fi
if ! "$CURIE_BIN" cluster up --help >/dev/null 2>&1; then
  echo "cluster up is missing from $CURIE_BIN" >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is missing" >&2
  exit 1
fi
if ! command -v kind >/dev/null 2>&1; then
  echo "kind is missing" >&2
  exit 1
fi

mkdir -p "$(dirname "$KUBECONFIG")"
rm -f "$KUBECONFIG"
export KUBECONFIG

cleanup() {
  local status=$?
  if [[ "$KEEP" -eq 0 && "$CREATED" -eq 1 ]]; then
    kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT

if kind get clusters 2>/dev/null | grep -Fxq "$KIND_CLUSTER"; then
  echo "kind cluster $KIND_CLUSTER already exists; refusing to adopt or delete it" >&2
  exit 1
fi

CREATED=1
kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG" --wait 180s
kubectl --kubeconfig "$KUBECONFIG" config rename-context "kind-${KIND_CLUSTER}" "$KIND_CLUSTER"
current="$(kubectl --kubeconfig "$KUBECONFIG" config current-context)"
if [[ "$current" != "$KIND_CLUSTER" ]]; then
  echo "isolated kubeconfig current context is $current, expected $KIND_CLUSTER" >&2
  exit 1
fi
case "$current" in
  k8|Prod*|Staging*)
    echo "refusing context $current" >&2
    exit 1
    ;;
esac

LOG_DIR="$(mktemp -d)"

run_down() {
  local out="$1"
  local start end rc
  start="$(date +%s)"
  set +e
  # An external watchdog so a missing sweep bound fails this scenario instead
  # of leaving the process waiting. 330s is the acceptance ceiling.
  timeout 330 "$CURIE_BIN" cluster down \
    --context "$KIND_CLUSTER" \
    --namespace "$NS" \
    --release "$RELEASE" \
    --yes \
    >"$out" 2>&1
  rc=$?
  set -e
  end="$(date +%s)"
  DOWN_SECONDS="$((end - start))"
  return "$rc"
}

plain_install() {
  # `cluster up` has no values-file flag. On a kind node with no RuntimeClass
  # it infers security.gvisor.mode=off itself.
  "$CURIE_BIN" cluster up \
    --context "$KIND_CLUSTER" \
    --namespace "$NS" \
    --release "$RELEASE" \
    --chart "$REPO_ROOT/charts/curie" \
    --dev \
    --fake-model \
    --no-expose
}

assert_release_gone() {
  if kubectl --context "$KIND_CLUSTER" get namespace "$NS" >/dev/null 2>&1; then
    echo "namespace $NS is still present" >&2
    exit 1
  fi
  local owned claims
  owned="$(kubectl --context "$KIND_CLUSTER" get namespace \
    -l "curietech.ai/created-by=${RELEASE},curietech.ai/created-in=${NS}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
  if [[ -n "$owned" ]]; then
    echo "owned namespaces remain: $owned" >&2
    exit 1
  fi
  claims="$(kubectl --context "$KIND_CLUSTER" get pv \
    -o jsonpath='{range .items[*]}{.spec.claimRef.namespace}{"\n"}{end}')"
  if printf '%s\n' "$claims" | grep -Fxq -- "$NS"; then
    echo "a PersistentVolume claimRef.namespace is still $NS" >&2
    exit 1
  fi
}

plain_install
if ! run_down "$LOG_DIR/plain-down.txt"; then
  echo "cluster down after a plain install failed" >&2
  cat "$LOG_DIR/plain-down.txt" >&2
  exit 1
fi
assert_release_gone

plain_install
kubectl --context "$KIND_CLUSTER" -n "$NS" create configmap hold --from-literal=keep=yes
kubectl --context "$KIND_CLUSTER" -n "$NS" patch configmap hold --type=json \
  --patch='[{"op":"add","path":"/metadata/finalizers","value":["example.com/hold"]}]'

stuck_rc=0
run_down "$LOG_DIR/stuck-down.txt" || stuck_rc=$?
echo "held cluster down exited ${stuck_rc} in ${DOWN_SECONDS}s"
if [[ "$stuck_rc" -ne 3 ]]; then
  echo "expected exit 3 when a finalizer holds the namespace, got $stuck_rc after ${DOWN_SECONDS}s" >&2
  cat "$LOG_DIR/stuck-down.txt" >&2
  exit 1
fi
if [[ "$DOWN_SECONDS" -gt 330 ]]; then
  echo "cluster down took ${DOWN_SECONDS}s, which is over the 330s bound" >&2
  exit 1
fi
if ! grep -q hold "$LOG_DIR/stuck-down.txt"; then
  echo "cluster down output did not name hold" >&2
  cat "$LOG_DIR/stuck-down.txt" >&2
  exit 1
fi
phase="$(kubectl --context "$KIND_CLUSTER" get namespace "$NS" -o jsonpath='{.status.phase}')"
if [[ "$phase" != "Terminating" ]]; then
  echo "namespace $NS phase is $phase, expected Terminating" >&2
  exit 1
fi
finalizers="$(kubectl --context "$KIND_CLUSTER" -n "$NS" get configmap hold -o jsonpath='{.metadata.finalizers}')"
case "$finalizers" in
  *example.com/hold*) ;;
  *)
    echo "configmap hold finalizers no longer contain example.com/hold: $finalizers" >&2
    exit 1
    ;;
esac

kubectl --context "$KIND_CLUSTER" -n "$NS" patch configmap hold --type=json \
  --patch='[{"op":"replace","path":"/metadata/finalizers","value":[]}]'
if ! run_down "$LOG_DIR/cleared-down.txt"; then
  echo "cluster down after the test removed the finalizer failed" >&2
  cat "$LOG_DIR/cleared-down.txt" >&2
  exit 1
fi
assert_release_gone

echo "PASS cluster down namespace bound ($NS / $RELEASE)"
