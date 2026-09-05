#!/usr/bin/env bash
# Fast render contract for the Langfuse web Postgres readiness gate (#1853).
# The executable live delayed-start regression is nested under ci/runtime so
# chart-check keeps discovering only the fast top-level assertion scripts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# The readiness init defaults to postgres.image (langfuse.web.postgresReadiness
# .image is empty by default), so read the pin out of values.yaml instead of
# repeating it here -- a literal would silently drift from the #2319 pin.
POSTGRES_IMAGE="$(python3 -c '
import sys

import yaml

values = yaml.safe_load(open(sys.argv[1])) or {}
image = ((values.get("postgres") or {}).get("image") or "").strip()
if not image:
    raise SystemExit("values.yaml postgres.image is empty")
print(image)
' "$CHART/values.yaml")"

render() {
  local name="$1"
  shift
  if ! helm template curie "$CHART" --show-only templates/langfuse.yaml "$@" >"$TMP/$name.yaml"; then
    fail "$name render failed"
  fi
}

render default
render disabled --set langfuse.web.postgresReadiness.enabled=false
render blank-security --set-json 'langfuse.web.containerSecurityContext=null'
render root-web-security \
  --set langfuse.web.containerSecurityContext.runAsNonRoot=false \
  --set langfuse.web.containerSecurityContext.runAsUser=0
render tuned \
  --set-string langfuse.web.postgresReadiness.image=registry.example.com/postgres-readiness:test \
  --set langfuse.web.postgresReadiness.attempts=7 \
  --set langfuse.web.postgresReadiness.intervalSeconds=3 \
  --set langfuse.web.postgresReadiness.probeTimeoutSeconds=4

cat >"$TMP/assert.py" <<'PY'
import sys
import yaml

path, mode, expected_image, attempts, interval, timeout = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(path)) if doc]

def deployment_with_container(name):
    matches = []
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        containers = doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if any(container.get("name") == name for container in containers):
            matches.append(doc)
    if len(matches) != 1:
        raise SystemExit(f"expected one Deployment containing {name}, got {len(matches)}")
    return matches[0]

web = deployment_with_container("langfuse-web")
worker = deployment_with_container("langfuse-worker")
web_spec = web["spec"]["template"]["spec"]
worker_spec = worker["spec"]["template"]["spec"]
web_inits = web_spec.get("initContainers", [])
worker_waits = [item for item in worker_spec.get("initContainers", []) if item.get("name") == "wait-for-postgres"]
if worker_waits:
    raise SystemExit("wait-for-postgres must be absent from the Langfuse worker Deployment")

waits = [item for item in web_inits if item.get("name") == "wait-for-postgres"]
if mode == "disabled":
    if waits:
        raise SystemExit("wait-for-postgres rendered while postgresReadiness.enabled=false")
    print("  ok: enabled=false removes the web readiness init and worker remains unchanged")
    raise SystemExit(0)

if len(waits) != 1:
    raise SystemExit(f"web must render exactly one wait-for-postgres init, got {len(waits)}")
wait = waits[0]
if not web_inits or web_inits[0].get("name") != "wait-for-postgres":
    raise SystemExit("wait-for-postgres must be the first web init container")
if wait.get("image") != expected_image:
    raise SystemExit(f"wait-for-postgres image is {wait.get('image')!r}, expected {expected_image!r}")

web_main = next(item for item in web_spec["containers"] if item.get("name") == "langfuse-web")
wait_sc = wait.get("securityContext") or {}
web_sc = web_main.get("securityContext") or {}
if mode not in ("blank-security", "root-web-security") and wait_sc != web_sc:
    raise SystemExit(f"wait-for-postgres securityContext differs from langfuse-web: {wait_sc!r} != {web_sc!r}")
uid = wait_sc.get("runAsUser")
if type(uid) is not int or uid < 1:
    raise SystemExit(f"wait-for-postgres runAsUser must be a numeric non-root uid, got {uid!r}")
if wait_sc.get("runAsNonRoot") is not True:
    raise SystemExit("wait-for-postgres must retain runAsNonRoot=true")
if mode == "blank-security" and (uid != 1001 or web_sc):
    raise SystemExit(f"blank web securityContext must leave the init at its 1001 non-root floor, got wait={wait_sc!r} web={web_sc!r}")
if mode == "root-web-security":
    if uid != 1001 or web_sc.get("runAsUser") != 0 or web_sc.get("runAsNonRoot") is not False:
        raise SystemExit(f"root web override must leave the readiness init at its 1001 non-root floor, got wait={wait_sc!r} web={web_sc!r}")

env_items = wait.get("env", [])
env = {item.get("name"): item for item in env_items}
expected_names = {
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_USER",
    "POSTGRES_DATABASE",
    "POSTGRES_READINESS_ATTEMPTS",
    "POSTGRES_READINESS_INTERVAL_SECONDS",
    "POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS",
}
if set(env) != expected_names:
    raise SystemExit(f"wait-for-postgres env names are {sorted(env)}, expected {sorted(expected_names)}")
for name, item in env.items():
    if "valueFrom" in item:
        raise SystemExit(f"credential-free readiness env {name} unexpectedly uses valueFrom")
    upper = name.upper()
    if any(token in upper for token in ("PASSWORD", "TOKEN", "SECRET", "CREDENTIAL")):
        raise SystemExit(f"credential-like env {name} reached wait-for-postgres")

expected_tuning = {
    "POSTGRES_READINESS_ATTEMPTS": attempts,
    "POSTGRES_READINESS_INTERVAL_SECONDS": interval,
    "POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS": timeout,
}
for name, expected in expected_tuning.items():
    actual = str(env[name].get("value"))
    if actual != expected:
        raise SystemExit(f"{name} rendered {actual!r}, expected {expected!r}")

command = "\n".join(str(part) for part in wait.get("command", []))
for needle in ("pg_isready", 'while [ "$attempt" -le "$POSTGRES_READINESS_ATTEMPTS" ]', "Postgres readiness exhausted", "exit 1"):
    if needle not in command:
        raise SystemExit(f"wait-for-postgres command is missing bounded-gate contract {needle!r}")

print(f"  ok: {mode} web init is first, credential-free, hardened, image={expected_image}, tuning={attempts}/{interval}/{timeout}")
PY

python3 "$TMP/assert.py" "$TMP/default.yaml" default "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/tuned.yaml" tuned registry.example.com/postgres-readiness:test 7 3 4
python3 "$TMP/assert.py" "$TMP/blank-security.yaml" blank-security "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/root-web-security.yaml" root-web-security "$POSTGRES_IMAGE" 60 2 2
python3 "$TMP/assert.py" "$TMP/disabled.yaml" disabled ignored 0 0 0

assert_refused() {
  local field="$1" flag="$2" value="$3" output rc
  set +e
  output="$(helm template curie "$CHART" --show-only templates/langfuse.yaml \
    "$flag" "langfuse.web.postgresReadiness.${field}=${value}" 2>&1)"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "invalid ${field}=${value} rendered successfully"
  [[ "$output" == *"langfuse.web.postgresReadiness.${field}"* ]] || \
    fail "invalid ${field}=${value} failed without naming the knob: $output"
}

for field in attempts intervalSeconds probeTimeoutSeconds; do
  assert_refused "$field" --set 0
  assert_refused "$field" --set -1
  assert_refused "$field" --set-json 2.5
done
echo "  ok: every bounded tuning knob rejects zero, negative, and fractional values"

# Red-on-revert control: remove the init from a real default render, then prove
# the same consumer assertion rejects it for the intended reason.
python3 - "$TMP/default.yaml" "$TMP/missing-init.yaml" <<'PY'
import sys
import yaml

source, target = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if doc]
for doc in docs:
    if doc.get("kind") != "Deployment":
        continue
    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    if any(item.get("name") == "langfuse-web" for item in spec.get("containers", [])):
        spec["initContainers"] = [
            item for item in spec.get("initContainers", [])
            if item.get("name") != "wait-for-postgres"
        ]
with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

set +e
negative_output="$(python3 "$TMP/assert.py" "$TMP/missing-init.yaml" negative "$POSTGRES_IMAGE" 60 2 2 2>&1)"
negative_rc=$?
set -e
[[ "$negative_rc" -ne 0 ]] || fail "negative control passed after wait-for-postgres was removed"
[[ "$negative_output" == *"wait-for-postgres"* ]] || fail "negative control failed for an unrelated reason: $negative_output"
echo "  ok: removing the rendered web readiness init is rejected"

python3 - "$TMP/default.yaml" "$TMP/unbounded-init.yaml" <<'PY'
import sys
import yaml

source, target = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(open(source)) if doc]
for doc in docs:
    if doc.get("kind") != "Deployment":
        continue
    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    for item in spec.get("initContainers", []):
        if item.get("name") == "wait-for-postgres":
            item["command"][-1] = item["command"][-1].replace(
                'while [ "$attempt" -le "$POSTGRES_READINESS_ATTEMPTS" ]; do',
                "while true; do",
            )
with open(target, "w") as output:
    yaml.safe_dump_all(docs, output)
PY

set +e
negative_output="$(python3 "$TMP/assert.py" "$TMP/unbounded-init.yaml" negative "$POSTGRES_IMAGE" 60 2 2 2>&1)"
negative_rc=$?
set -e
[[ "$negative_rc" -ne 0 ]] || fail "negative control passed after the readiness loop was made unbounded"
[[ "$negative_output" == *"bounded-gate contract"* ]] || fail "unbounded-loop control failed for an unrelated reason: $negative_output"
echo "  ok: stripping the loop bound from the rendered init is rejected"

echo "PASS: Langfuse web Postgres readiness render contract and red negative control"
