#!/bin/sh
# Starts the curie-authz plugin, then the stock dind entrypoint with
# `--authorization-plugin=curie-authz --userns-remap=default` placed right
# after `dockerd`, before every caller argument, so no value-consuming caller
# flag (`--label`, `-H`, ...) can swallow them. A daemon without the plugin or
# without user namespace remapping is the unprotected daemon ADR 0217 decision
# 4 rules out. Caller duplicates of the injected flags are harmless.
#
# dockerd runs through `curie-docker-authz cgroup-isolate`, which gives it a
# private cgroup namespace with cgroup2 remounted. containerd gives a
# privileged container none of its own (a Kubernetes privileged pod), and a
# cgroup parent chosen through BuildKit's /grpc, which the plugin cannot read,
# would otherwise resolve against the node's cgroup hierarchy.
set -eu

# dockerd discovers the plugin by this default path; the override is for tests.
socket=${CURIE_AUTHZ_SOCKET:-/run/docker/plugins/curie-authz.sock}

case "${1:-}" in
  dockerd) shift ;;
  -*) ;;
  # Not a daemon start (a shell, a client command): nothing to protect.
  *) exec "$@" ;;
esac

# A later --userns-remap (empty, another user, or the two-argument form) would
# override the injected one, so only the exact duplicate is accepted. A caller
# config file would replace /etc/docker/daemon.json and with it the BuildKit
# entitlements turned off there.
for arg in "$@"; do
  case "$arg" in
    --userns-remap=default) ;;
    --userns-remap | --userns-remap=*)
      echo "curie-docker-daemon: refusing dockerd argument $arg: user namespace remapping is fixed to --userns-remap=default" >&2
      exit 1
      ;;
    --config-file | --config-file=*)
      echo "curie-docker-daemon: refusing dockerd argument $arg: the daemon config is fixed to /etc/docker/daemon.json" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$(dirname "$socket")"
curie-docker-authz --socket "$socket" &

# dockerd fails to start if the plugin socket is not there yet.
i=0
while [ ! -S "$socket" ]; do
  i=$((i + 1))
  if [ "$i" -gt 100 ]; then
    echo "curie-docker-daemon: $socket did not appear within 10 s" >&2
    exit 1
  fi
  sleep 0.1
done

exec curie-docker-authz cgroup-isolate -- \
  dockerd-entrypoint.sh dockerd --authorization-plugin=curie-authz --userns-remap=default "$@"
