#!/usr/bin/env bash
#
# Render-assertion test for the installation-wide model auth mode (ADR 0268).
# One chart value, factory.modelAuthMode (service or personal, default
# service), renders FACTORY_MODEL_AUTH_MODE into BOTH the api and the worker
# container from one helper, so the two cannot drift. The variable is
# chart-owned: setting it through api.extraEnv or worker.extraEnv is refused,
# even with an equal value. Personal mode needs the Kubernetes sandbox
# substrate, so a personal render whose worker.extraEnv selects another
# CURIE_SANDBOX_SUBSTRATE is refused before the worker could refuse to start.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

render() {
  helm template rel "$CHART" -f "$CHART/values-dev.yaml" "$@"
}

check() {
  # $1: label; $2: the mode both containers must carry. Reads a render on stdin.
  python3 -c '
import sys

import yaml

label, expected = sys.argv[1], sys.argv[2]

def fail(message):
    sys.exit(f"FAIL [{label}]: {message}")

def containers(value):
    if isinstance(value, dict):
        if "name" in value and "env" in value and "image" in value:
            yield value
        for child in value.values():
            yield from containers(child)
    elif isinstance(value, list):
        for child in value:
            yield from containers(child)

found = {}
for doc in yaml.safe_load_all(sys.stdin):
    if not doc or doc.get("kind") != "Deployment":
        continue
    for container in containers(doc.get("spec", {})):
        workload = container["name"]
        if workload not in ("api", "worker"):
            continue
        entries = [e for e in container["env"] if e.get("name") == "FACTORY_MODEL_AUTH_MODE"]
        if len(entries) != 1:
            fail(f"{workload} carries {len(entries)} FACTORY_MODEL_AUTH_MODE entries, want 1")
        found[workload] = entries[0]

for workload in ("api", "worker"):
    if workload not in found:
        fail(f"no {workload} container rendered")
    entry = found[workload]
    if entry != {"name": "FACTORY_MODEL_AUTH_MODE", "value": expected}:
        fail(f"{workload} FACTORY_MODEL_AUTH_MODE is {entry!r}, want plain value {expected!r}")
api_entry, worker_entry = found["api"], found["worker"]
if api_entry != worker_entry:
    fail(f"api and worker disagree: {api_entry!r} vs {worker_entry!r}")
print(f"  ok: {label}: api and worker both carry FACTORY_MODEL_AUTH_MODE={expected}")
' "$@"
}

refused() {
  # $1: label; $2: required text in the error; remaining: render args.
  local label="$1" needle="$2"
  shift 2
  local out
  if out=$("$@" 2>&1 >/dev/null); then
    echo "FAIL: $label rendered; it must be refused" >&2
    exit 1
  fi
  if ! grep -qF -- "$needle" <<<"$out"; then
    echo "FAIL: $label was refused without naming '$needle':" >&2
    echo "$out" >&2
    exit 1
  fi
  echo "  ok: $label refused"
}

echo "=== default and explicit modes ==="
render | check "default" service
render --set factory.modelAuthMode=service | check "explicit service" service
render --set factory.modelAuthMode=personal | check "personal" personal

echo "=== unknown values ==="
# Helm words the schema refusal differently across versions, so require only
# that it names the key.
refused "unknown mode" "modelAuthMode" \
  render --set factory.modelAuthMode=shared
# The template guard holds on its own when a render skips the schema.
refused "unknown mode without the schema" \
  "factory.modelAuthMode must be service or personal" \
  render --skip-schema-validation --set factory.modelAuthMode=shared

echo "=== the variable is chart-owned ==="
for workload in api worker; do
  for value in personal service; do
    refused "$workload.extraEnv FACTORY_MODEL_AUTH_MODE=$value" \
      "$workload.extraEnv contains chart-owned environment variable FACTORY_MODEL_AUTH_MODE; remove it from extraEnv and configure factory.modelAuthMode instead" \
      render --set factory.modelAuthMode="$value" \
        --set "$workload.extraEnv[0].name=FACTORY_MODEL_AUTH_MODE" \
        --set-string "$workload.extraEnv[0].value=$value"
  done
done

# The api and the worker read settings case-insensitively, so a case variant
# of the name would override the chart value just the same.
for workload in api worker; do
  refused "$workload.extraEnv factory_model_auth_mode" \
    "$workload.extraEnv sets factory_model_auth_mode, which the $workload reads as FACTORY_MODEL_AUTH_MODE; remove it from extraEnv and configure factory.modelAuthMode instead" \
    render --set "$workload.extraEnv[0].name=factory_model_auth_mode" \
      --set-string "$workload.extraEnv[0].value=personal"
done

echo "=== personal mode needs the Kubernetes substrate ==="
substrate() {
  printf -- '--set\nworker.extraEnv[0].name=CURIE_SANDBOX_SUBSTRATE\n--set-string\nworker.extraEnv[0].value=%s\n' "$1"
}
mapfile -t docker_substrate < <(substrate docker)
mapfile -t kube_substrate < <(substrate kubernetes)
mapfile -t padded_substrate < <(substrate " Kubernetes ")
mapfile -t empty_substrate < <(substrate "")
from_config_map=(
  --set "worker.extraEnv[0].name=CURIE_SANDBOX_SUBSTRATE"
  --set "worker.extraEnv[0].valueFrom.configMapKeyRef.name=acme-substrate"
  --set "worker.extraEnv[0].valueFrom.configMapKeyRef.key=substrate"
)
refused "personal with CURIE_SANDBOX_SUBSTRATE=docker" \
  "factory.modelAuthMode=personal needs the Kubernetes sandbox substrate, but worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE=docker" \
  render --set factory.modelAuthMode=personal "${docker_substrate[@]}"
render --set factory.modelAuthMode=service "${docker_substrate[@]}" \
  | check "service with CURIE_SANDBOX_SUBSTRATE=docker" service
render --set factory.modelAuthMode=personal "${kube_substrate[@]}" \
  | check "personal with CURIE_SANDBOX_SUBSTRATE=kubernetes" personal
render --set factory.modelAuthMode=personal "${padded_substrate[@]}" \
  | check "personal with CURIE_SANDBOX_SUBSTRATE=' Kubernetes '" personal
# A valueFrom entry resolves only at runtime, to anything; an empty value is a
# worker startup error. Both are refused in personal mode only.
refused "personal with CURIE_SANDBOX_SUBSTRATE from a ConfigMap" \
  "worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE from valueFrom, which cannot be verified before the worker starts" \
  render --set factory.modelAuthMode=personal "${from_config_map[@]}"
refused "personal with an empty CURIE_SANDBOX_SUBSTRATE" \
  "worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE to an empty value" \
  render --set factory.modelAuthMode=personal "${empty_substrate[@]}"
render --set factory.modelAuthMode=service "${from_config_map[@]}" \
  | check "service with CURIE_SANDBOX_SUBSTRATE from a ConfigMap" service
render --set factory.modelAuthMode=service "${empty_substrate[@]}" \
  | check "service with an empty CURIE_SANDBOX_SUBSTRATE" service

echo "factory model auth mode assertions passed"
