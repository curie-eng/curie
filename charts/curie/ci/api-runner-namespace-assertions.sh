#!/usr/bin/env bash
#
# Structural assertion for the api's RUNNER_NAMESPACE (#3075): the Logs tab
# lists runner pods in the namespace the worker claims sandboxes in, which is
# the release namespace. Without it the api falls back to "curie" and any
# other namespace gets a 403 listing pods.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }

assert_runner_namespace() {
  local case_id="$1" namespace="$2"
  local out
  if ! out="$(helm template curie "$CHART" -n "$namespace" -s templates/api.yaml -s templates/worker.yaml 2>&1)"; then
    fail "$case_id" "helm template failed
$(head -3 <<<"$out")"
  fi

  CASE_ID="$case_id" python3 - "$out" "$namespace" <<'PY' || exit 1
import os
import sys

import yaml

case_id = os.environ["CASE_ID"]
expected = sys.argv[2]
documents = [doc for doc in yaml.safe_load_all(sys.argv[1]) if doc]


def env_of(component: str, name: str) -> list[str]:
    values = []
    for doc in documents:
        if doc.get("kind") != "Deployment":
            continue
        if doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") != component:
            continue
        for container in doc["spec"]["template"]["spec"]["containers"]:
            for env in container.get("env", []):
                if env.get("name") == name:
                    values.append(env.get("value"))
    return values


api = env_of("api", "RUNNER_NAMESPACE")
if api != [expected]:
    print(f"FAIL [{case_id}] api RUNNER_NAMESPACE {api!r}, expected [{expected!r}]", file=sys.stderr)
    sys.exit(1)
worker = env_of("worker", "CURIE_NAMESPACE")
if worker != api:
    print(
        f"FAIL [{case_id}] api RUNNER_NAMESPACE {api!r} differs from worker CURIE_NAMESPACE {worker!r}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
}

assert_runner_namespace a curie
assert_runner_namespace b factory-e2e

echo "api-runner-namespace-assertions: both assertions passed"
