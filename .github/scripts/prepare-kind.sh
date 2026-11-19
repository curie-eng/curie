#!/usr/bin/env bash
set -euo pipefail

[[ $# == 2 && -n "$1" && -f "$2" ]] || {
  echo 'usage: prepare-kind.sh CLUSTER CONFIG' >&2
  exit 2
}
cluster="$1"
config="$2"
context="kind-$cluster"
image='kindest/node:v1.31.4@sha256:2cb39f7295fe7eafee0842b1052a599a4fb0f8bcf3f83d96c7f4864c357c6c30'
directory="$(mktemp -d)"
owned=0
cleanup() {
  local status=$?
  trap - EXIT
  rm -rf "$directory"
  if (( status != 0 && owned )); then
    kind delete cluster --name "$cluster" || status=1
  fi
  exit "$status"
}
trap cleanup EXIT
kind get clusters >"$directory/clusters"
if grep -Fxq -- "$cluster" "$directory/clusters"; then
  echo 'refusing to replace an existing cluster' >&2
  exit 1
fi
# The node's containerd does not read the host daemon's registry-mirrors, so
# without this every docker.io image the chart names is pulled anonymously
# from Docker Hub and hits its shared-IP rate limit (429). mirror.gcr.io is
# Google's public Docker Hub cache; Docker Hub stays the fallback on a miss.
if grep -q '^containerdConfigPatches:' "$config"; then
  echo 'refusing a kind config that already sets containerdConfigPatches' >&2
  exit 1
fi
effective="$directory/kind.yaml"
cat "$config" - >"$effective" <<'EOF'
containerdConfigPatches:
  - |
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
      endpoint = ["https://mirror.gcr.io", "https://registry-1.docker.io"]
EOF
bash "$(dirname "${BASH_SOURCE[0]}")/pull-image.sh" "$image"
manifest="$directory/calico.yaml"
curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 -o "$manifest" \
  https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml
echo "be59408bf990e96276f631d2f9285c2a0f9802194c0ad1cecdb6d9c52623a1c8  $manifest" \
  | sha256sum --check --strict
ready() {
  kind create cluster --name "$cluster" --config "$effective" --image "$image" --wait 0s || return
  kubectl --context "$context" apply -f "$manifest" || return
  kubectl --context "$context" -n kube-system rollout status daemonset/calico-node --timeout=300s || return
  kubectl --context "$context" wait --for=condition=Ready node --all --timeout=300s
}
for attempt in 1 2; do
  owned=1
  if ready; then
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
      printf 'owned_cluster=%s\n' "$cluster" >> "$GITHUB_OUTPUT"
    fi
    exit 0
  fi
  kubectl --context "$context" get pods -A -o wide >&2 || true
  if (( attempt == 2 )); then
    echo 'cluster setup failed after one recreate' >&2
    exit 1
  fi
  kind delete cluster --name "$cluster"
  owned=0
done
