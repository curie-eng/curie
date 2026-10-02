#!/usr/bin/env bash
#
# Render-assertion test for worker.maxConcurrency (issue #760). How many turns
# one worker runs at once was a constructor default of 16 that no deployment
# could change: the worker entry point passed nothing and the chart had no
# value. worker.maxConcurrency now renders as CURIE_WORKER_MAX_CONCURRENCY, and
# NOTES prints replicas x maxConcurrency next to the sandbox quota ceiling so an
# operator can compare the turns the workers will start with the sandboxes the
# quota will admit.
#
#   1. DEFAULT: the worker container carries CURIE_WORKER_MAX_CONCURRENCY
#      exactly once, at 16.
#   2. OVERRIDE: worker.maxConcurrency=4 reaches the worker env as 4.
#   3. BOUNDS, accept side: 1 and 256 (the schema's own bounds) render.
#   4. BOUNDS, refuse side: 0, 257, a fractional 4.5 and a --set-string "4"
#      are refused by helm itself, naming maxConcurrency. The worker's own
#      config refuses the same range at boot; its twin is
#      test_max_concurrency_refuses_out_of_range in
#      apps/worker/tests/test_config.py.
#   5. RESERVED: a worker.extraEnv entry named CURIE_WORKER_MAX_CONCURRENCY
#      refuses the render and points at worker.maxConcurrency, as every
#      chart-owned worker env does (files/reserved-env.yaml), rather than
#      rendering a second copy Kubernetes would reject on patch.
#   6. NOTES: with lookup stubbed to a fixture node, worker.replicas=2 and
#      worker.maxConcurrency=4 print "2 x 4 = 8" beside the quota ceiling, and
#      the shipped defaults print "1 x 16 = 16".
#   7. NOTES, negative: worker.deploy=false prints no turn-slot line, and a
#      plain render (lookup empty, as under helm template) prints none either.
#   8. RETAINED: a chart copy whose own values.yaml lacks maxConcurrency
#      renders the worker env as 16; an explicit override still renders as 4.
#   9. NOTES, retained: the same missing key prints "1 x 16 = 16".
#
# Refusals in 4 come from helm's bundled JSON-Schema validator, whose wording
# differs across helm versions, so only the exit status and the bare key name
# are asserted (see ci/worker-ttl-bounds-assertions.sh).
#
# Renders to --output-dir and reads the file, never a stdout pipe (a piped
# `helm template` has been observed to truncate silently while exiting 0).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local chart="$1" name="$2"
  shift 2
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  mkdir -p "$RENDER_DIR"
  helm template rel "$chart" --output-dir "$RENDER_DIR" "$@" >"$TMP/$name.log" 2>&1 \
    || fail "helm template failed for render '$name': $(tail -n 5 "$TMP/$name.log")"
}

# Print the worker container's CURIE_WORKER_MAX_CONCURRENCY values, one per line.
worker_env() {
  python3 - "$RENDER_DIR" <<'PY'
import pathlib
import sys

import yaml

root = pathlib.Path(sys.argv[1])
manifest = next(root.rglob("templates/worker.yaml"), None)
if manifest is None:
    raise SystemExit(f"{root}: no worker.yaml rendered")
found = []
for doc in yaml.safe_load_all(manifest.read_text()):
    if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
        continue
    for container in doc["spec"]["template"]["spec"]["containers"]:
        if container["name"] != "worker":
            continue
        for env in container.get("env") or []:
            if env.get("name") == "CURIE_WORKER_MAX_CONCURRENCY":
                found.append(str(env.get("value")))
if not found:
    raise SystemExit(f"{manifest}: worker container has no CURIE_WORKER_MAX_CONCURRENCY")
print("\n".join(found))
PY
}

expect_env() {
  local want="$1" got
  got="$(worker_env)" || fail "could not read the worker env (see the message above)"
  [[ "$got" == "$want" ]] \
    || fail "worker CURIE_WORKER_MAX_CONCURRENCY rendered as [$got], expected exactly one [$want]"
}

refuse() {
  local name="$1"
  shift
  if helm template rel "$CHART" "$@" >"$TMP/$name.out" 2>&1; then
    fail "render '$name' was accepted; expected helm to refuse it"
  fi
  grep -qF maxConcurrency "$TMP/$name.out" \
    || fail "render '$name' refused without naming maxConcurrency: $(tail -n 5 "$TMP/$name.out")"
  echo "  ok: '$name' refused, naming maxConcurrency"
}

echo "=== Assertion 1: the default worker env carries 16 exactly once ==="
render "$CHART" default
expect_env 16
echo "  ok: CURIE_WORKER_MAX_CONCURRENCY=16"

echo "=== Assertion 2: worker.maxConcurrency=4 reaches the worker env ==="
render "$CHART" four --set worker.maxConcurrency=4
expect_env 4
echo "  ok: CURIE_WORKER_MAX_CONCURRENCY=4"

echo "=== Assertion 3: both schema bounds render ==="
for value in 1 256; do
  render "$CHART" "bound-$value" --set "worker.maxConcurrency=$value"
  expect_env "$value"
  echo "  ok: worker.maxConcurrency=$value renders"
done

echo "=== Assertion 4: out-of-range and non-integer values are refused ==="
refuse zero --set worker.maxConcurrency=0
refuse over --set worker.maxConcurrency=257
refuse fraction --set worker.maxConcurrency=4.5
refuse string --set-string worker.maxConcurrency=4

echo "=== Assertion 5: an extraEnv copy of the env is refused, naming the value key ==="
cat >"$TMP/extra.yaml" <<'EOF'
worker:
  extraEnv:
    - name: CURIE_WORKER_MAX_CONCURRENCY
      value: "4"
EOF
if helm template rel "$CHART" -f "$TMP/extra.yaml" >"$TMP/extra.out" 2>&1; then
  fail "a worker.extraEnv CURIE_WORKER_MAX_CONCURRENCY was accepted"
fi
grep -qF "CURIE_WORKER_MAX_CONCURRENCY" "$TMP/extra.out" && grep -qF "worker.maxConcurrency" "$TMP/extra.out" \
  || fail "the extraEnv refusal does not name the env and worker.maxConcurrency: $(tail -n 5 "$TMP/extra.out")"
echo "  ok: refused, pointing at worker.maxConcurrency"

echo "=== Assertions 6 and 7: NOTES prints replicas x maxConcurrency beside the ceiling ==="
# lookup is empty under helm template, so a chart copy swaps NOTES' two lookup
# calls for one fixture node with room to spare and no pods, and renders NOTES
# through a test-only ConfigMap.
STUB="$TMP/stub"
cp -R "$CHART" "$STUB"
mkdir -p "$STUB/capacity-fixtures"
cat >"$STUB/capacity-fixtures/node.yaml" <<'EOF'
nodes:
  - metadata: { name: a, labels: {} }
    spec: {}
    status: { allocatable: { cpu: "32", memory: 128Gi } }
pods: []
EOF
python3 - "$CHART/templates/NOTES.txt" "$STUB/NOTES.txt" <<'PY'
import sys

src, dst = sys.argv[1:]
text = open(src).read()
node = '(lookup "v1" "Node" "" "")'
pod = '(lookup "v1" "Pod" "" "")'
assert text.count(node) == 1 and text.count(pod) == 1, "NOTES lookup calls changed shape"
fixture = '(dict "items" (.Files.Get "capacity-fixtures/node.yaml" | fromYaml).{})'
open(dst, "w").write(text.replace(node, fixture.format("nodes")).replace(pod, fixture.format("pods")))
PY
cat >"$STUB/templates/notes-check.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF
notes() { cat "$RENDER_DIR/curie/templates/notes-check.yaml"; }

render "$STUB" notes-scaled --set worker.replicas=2 --set worker.maxConcurrency=4
notes | grep -q "Sandbox capacity" || fail "6 stubbed NOTES did not reach the capacity block"
notes | grep -qF "worker.replicas 2 x worker.maxConcurrency 4 = 8 concurrent turns; the sandbox quota admits 8" \
  || fail "6 NOTES does not print 2 x 4 = 8 beside the quota ceiling: $(notes | grep -i 'turn' || true)"
echo "  ok: 2 x 4 = 8 beside a quota ceiling of 8"

render "$STUB" notes-default
notes | grep -qF "worker.replicas 1 x worker.maxConcurrency 16 = 16 concurrent turns; the sandbox quota admits 8" \
  || fail "6 NOTES does not print the shipped 1 x 16 = 16: $(notes | grep -i 'turn' || true)"
echo "  ok: shipped defaults print 1 x 16 = 16"

render "$STUB" notes-no-worker --set worker.deploy=false
notes | grep -q "Sandbox capacity" || fail "7 stubbed NOTES did not reach the capacity block"
if notes | grep -q "concurrent turns"; then
  fail "7 worker.deploy=false still printed a worker turn-slot line"
fi
echo "  ok: worker.deploy=false prints no turn-slot line"

PLAIN="$TMP/plain"
cp -R "$CHART" "$PLAIN"
cp "$CHART/templates/NOTES.txt" "$PLAIN/NOTES.txt"
cp "$STUB/templates/notes-check.yaml" "$PLAIN/templates/notes-check.yaml"
render "$PLAIN" notes-plain
if notes | grep -q "concurrent turns"; then
  fail "7 a render with no lookup data printed a worker turn-slot line"
fi
echo "  ok: no lookup data, no turn-slot line"

echo "=== Assertion 8: retained values missing maxConcurrency use 16 and preserve overrides ==="
# Removing the key from the copied chart's defaults reproduces the values
# available to a release upgrade that retains values from before the key existed.
RETAINED="$TMP/retained"
cp -R "$CHART" "$RETAINED"
python3 - "$RETAINED/values.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
line = "  maxConcurrency: 16\n"
assert text.count(line) == 1, "worker.maxConcurrency default changed shape"
path.write_text(text.replace(line, ""))
PY
render "$RETAINED" retained-default
expect_env 16
echo "  ok: missing worker.maxConcurrency renders CURIE_WORKER_MAX_CONCURRENCY=16"

render "$RETAINED" retained-four --set worker.maxConcurrency=4
expect_env 4
echo "  ok: an explicit worker.maxConcurrency=4 still renders as 4"

echo "=== Assertion 9: NOTES uses 16 when retained values lack maxConcurrency ==="
cp "$RETAINED/values.yaml" "$STUB/values.yaml"
render "$STUB" notes-retained
notes | grep -qF "worker.replicas 1 x worker.maxConcurrency 16 = 16 concurrent turns; the sandbox quota admits 8" \
  || fail "9 NOTES does not print the retained default 1 x 16 = 16: $(notes | grep -i 'turn' || true)"
echo "  ok: retained defaults print 1 x 16 = 16"

echo
echo "PASS: worker.maxConcurrency reaches the worker env within its bounds, and NOTES prints replicas x maxConcurrency beside the sandbox quota ceiling."
