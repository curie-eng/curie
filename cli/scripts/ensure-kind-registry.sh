#!/usr/bin/env bash
# Recreate the local registry container helm/kind-action addresses as
# kind-registry. The action's post cleanup always runs `docker rm -f` on that
# name. When the container was never created, that line is "No such container:
# kind-registry". Create it when missing, start it when stopped, and leave a
# running container alone.
set -euo pipefail

name="${KIND_REGISTRY_NAME:-kind-registry}"
image="${KIND_REGISTRY_IMAGE:-mirror.gcr.io/library/registry:2}"
port="${KIND_REGISTRY_PORT:-5000}"

delay="${KIND_REGISTRY_RETRY_DELAY:-15}"

start_registry() {
  docker run -d --restart=always \
    --name "$name" \
    --network bridge \
    -p "127.0.0.1:${port}:5000" \
    "$image"
}

if docker inspect "$name" >/dev/null 2>&1; then
  running="$(docker inspect -f '{{.State.Running}}' "$name")"
  if [[ "$running" != "true" ]]; then
    docker start "$name"
  fi
else
  # docker run pulls the image. A transient registry or network failure should
  # not fail the kind job before its own retry. A second failure is real.
  if ! start_registry; then
    docker rm -f "$name" >/dev/null 2>&1 || true
    echo "kind-registry image pull failed on attempt 1 of 2, retrying in ${delay} seconds" >&2
    sleep "$delay"
    start_registry
  fi
fi
