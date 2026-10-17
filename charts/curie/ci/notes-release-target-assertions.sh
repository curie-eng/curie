#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$repo_root"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for command in helm kind docker kubectl python3; do
  command -v "$command" >/dev/null || fail "required command is unavailable: $command"
done

work_dir="$(mktemp -d)"
cluster_owned=0
cleanup() {
  local status=$? cleanup_failed=0 remaining_clusters remaining_nodes
  trap - EXIT
  trap '' HUP INT TERM
  set +e
  if (( cluster_owned )); then
    kind delete cluster --name "$cluster" --kubeconfig "$KUBECONFIG" || cleanup_failed=1
    if remaining_clusters="$(kind get clusters)"; then
      if [[ $'\n'"$remaining_clusters"$'\n' == *$'\n'"$cluster"$'\n'* ]]; then
        cleanup_failed=1
      fi
    else
      cleanup_failed=1
    fi
    if remaining_nodes="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$cluster")"; then
      [[ -z "$remaining_nodes" ]] || cleanup_failed=1
    else
      cleanup_failed=1
    fi
    if (( cleanup_failed )); then
      echo "FAIL: could not confirm deletion of owned kind cluster and node containers" >&2
      [[ "$status" -ne 0 ]] || status=1
    else
      echo "Cleanup confirmed: owned kind cluster and node containers are gone."
    fi
  fi
  rm -rf "$work_dir" || status=1
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

export KUBECONFIG="$work_dir/kubeconfig"
export KIND_EXPERIMENTAL_PROVIDER=docker
cluster="test-4695-notes-$$-$(python3 -c 'import time; print(time.time_ns())')"
context="kind-$cluster"
clusters="$(kind get clusters)" || fail "cannot inventory kind clusters"
[[ $'\n'"$clusters"$'\n' != *$'\n'"$cluster"$'\n'* ]] || fail "kind cluster name is occupied"
nodes="$(docker ps -aq --filter "label=io.x-k8s.kind.cluster=$cluster")" \
  || fail "cannot inventory kind node containers"
[[ -z "$nodes" ]] || fail "kind node containers already exist for this cluster name"

cluster_owned=1
kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --wait 120s \
  --image kindest/node:v1.31.0@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93

kubectl --kubeconfig "$KUBECONFIG" --context "$context" apply --server-side -f charts/curie/crds
kubectl --kubeconfig "$KUBECONFIG" --context "$context" wait --for=condition=Established --timeout=120s \
  crd/sandboxes.agents.x-k8s.io \
  crd/sandboxclaims.extensions.agents.x-k8s.io \
  crd/sandboxtemplates.extensions.agents.x-k8s.io \
  crd/sandboxwarmpools.extensions.agents.x-k8s.io

# Helm v3.16.4 checks reachability before handling a client dry run:
# https://github.com/helm/helm/blob/v3.16.4/pkg/action/install.go
rendered="$(helm install --dry-run=client demo charts/curie -n demo --kube-context "$context")"

printf '%s\n' "$rendered" | python3 -c '
import re
import sys


def require(condition, message):
    if not condition:
        raise SystemExit(message)


rendered = sys.stdin.read()
parts = re.split(r"(?m)^NOTES:[ \t]*$", rendered, maxsplit=1)
require(len(parts) == 2, "Helm install output has no NOTES section")
notes = parts[1]
section = re.search(
    r"(?ms)^Enable a real model and a Slack workspace:[ \t]*\n(.*?)(?=^\S|\Z)",
    notes,
)
require(section is not None, "NOTES have no model and Slack setup section")
setup = section.group(1)
require(setup.strip(), "The model and Slack setup section is empty")
plain_notes = re.sub(r"\s+", " ", notes.replace("`", ""))
plain_setup = re.sub(r"\s+", " ", setup.replace("`", ""))

require(
    "verbs default to" not in plain_notes.lower(),
    "NOTES still claim the verbs default to the installed release and namespace",
)
require(
    "charts/curie" not in setup,
    "Model and Slack setup still requires a source-checkout chart path",
)
require(
    "Or do both (model + Slack) in one helm upgrade" not in plain_setup,
    "Model and Slack setup still includes the redundant Helm upgrade block",
)
require(
    re.search(
        r"(?m)^[ \t]*curie cluster message --namespace demo --release demo(?:[ \t]|$)",
        setup,
    ),
    "The cluster message example does not target namespace demo and release demo",
)
require(
    re.search(
        r"\b(?:every|each|all) curie cluster (?:verb|command)s? "
        r"defaults? to (?:release(?: and namespace|/namespace) curie"
        r"|release curie and namespace curie)(?![\w/-])",
        plain_setup,
        flags=re.IGNORECASE,
    ),
    "NOTES must explain that every curie cluster verb defaults to release and namespace curie",
)
target = r"--namespace demo --release demo"
each_call = r"\b(?:each|every) (?:call|command|invocation)\b"
require(
    re.search(
        rf"{target}[^.!?]*{each_call}|{each_call}[^.!?]*{target}",
        plain_setup,
        flags=re.IGNORECASE,
    ),
    "NOTES must explain that this release needs namespace and release flags on each call",
)

print("PASS: install NOTES explain CLI defaults and target demo explicitly")
'
