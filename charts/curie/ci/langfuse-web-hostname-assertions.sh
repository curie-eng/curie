#!/usr/bin/env bash
#
# Render-assertion test for the Langfuse web port-forward fix.
#
# Langfuse's server binds to the HOSTNAME environment variable, and Kubernetes
# sets HOSTNAME to the pod name. Left alone, langfuse-web listens on the pod IP
# only, so `kubectl port-forward` (which dials loopback inside the pod) is
# refused and `curie cluster access` cannot reach Langfuse on a ClusterIP
# install.
#
# This asserts that helm template renders HOSTNAME=0.0.0.0 on the langfuse-web
# container, and that a render without it, or with another value, is rejected.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RENDER="$TMP/langfuse.yaml"
CHECKER="$TMP/check.py"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

echo "=== Rendering templates/langfuse.yaml ==="
helm template curie "$CHART" --show-only templates/langfuse.yaml >"$RENDER"

cat >"$CHECKER" <<'PY'
import pathlib
import sys

import yaml

path = sys.argv[1]
web = None
for document in yaml.safe_load_all(pathlib.Path(path).read_text()):
    if not document or document.get("kind") != "Deployment":
        continue
    if document.get("metadata", {}).get("name", "").endswith("-langfuse-web"):
        web = document
        break
if web is None:
    raise SystemExit("no Deployment ending in '-langfuse-web' rendered")

containers = web["spec"]["template"]["spec"]["containers"]
container = next((c for c in containers if c.get("name") == "langfuse-web"), None)
if container is None:
    raise SystemExit("langfuse-web Deployment has no langfuse-web container")

values = [e.get("value") for e in container.get("env", []) if e.get("name") == "HOSTNAME"]
if values != ["0.0.0.0"]:
    raise SystemExit(
        "the langfuse-web container must set HOSTNAME=0.0.0.0 exactly once so "
        f"kubectl port-forward reaches it; found HOSTNAME values {values!r}"
    )
print("langfuse-web container renders HOSTNAME=0.0.0.0")
PY

echo "=== Assertion: langfuse-web renders HOSTNAME=0.0.0.0 ==="
python3 "$CHECKER" "$RENDER"

mutate() {
  local dest="$1"
  local mode="$2"
  python3 - "$RENDER" "$dest" "$mode" <<'PY'
import pathlib
import sys

import yaml

source, dest, mode = sys.argv[1], sys.argv[2], sys.argv[3]
documents = list(yaml.safe_load_all(pathlib.Path(source).read_text()))
mutated = False
for document in documents:
    if not document or document.get("kind") != "Deployment":
        continue
    if not document.get("metadata", {}).get("name", "").endswith("-langfuse-web"):
        continue
    for container in document["spec"]["template"]["spec"]["containers"]:
        if container.get("name") != "langfuse-web":
            continue
        env = container["env"]
        if mode == "drop":
            container["env"] = [e for e in env if e.get("name") != "HOSTNAME"]
        else:
            for e in env:
                if e.get("name") == "HOSTNAME":
                    e["value"] = "127.0.0.1"
        mutated = True
if not mutated:
    raise SystemExit("mutant setup failed: no langfuse-web container to change")
with pathlib.Path(dest).open("w") as handle:
    yaml.safe_dump_all(documents, handle)
PY
}

reject() {
  local mode="$1"
  local mutant="$TMP/langfuse-${mode}.yaml"
  echo "=== Negative: HOSTNAME ${mode} is rejected ==="
  mutate "$mutant" "$mode"
  local output=""
  if output="$(python3 "$CHECKER" "$mutant" 2>&1)"; then
    fail "the HOSTNAME ${mode} mutant passed the contract"
  fi
  if [[ "$output" != *"must set HOSTNAME=0.0.0.0"* ]]; then
    fail "the HOSTNAME ${mode} mutant failed unexpectedly: $output"
  fi
  echo "negative: HOSTNAME ${mode} is rejected"
}

reject drop
reject wrong

echo "PASS: langfuse-web renders HOSTNAME=0.0.0.0 and a render without it is refused"
