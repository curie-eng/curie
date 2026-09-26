#!/usr/bin/env bash
#
# Render-assertion tests for issue #2949: the sandbox ResourceQuota is an
# admission ceiling, not a scheduling guarantee. NOTES.txt compares the
# quota's ceiling (in sandboxes) with what fits on the nodes, via the pure
# helpers in templates/_capacity.tpl.
#
# `lookup` is empty under `helm template`, so this copies the chart to a temp
# dir and adds a test-only template that calls curie.sandboxCapacity with
# fixture Node/Pod objects, and curie.sandboxCapacity.notes with numbers.
#
#   1. The issue's scenario: 3 nodes, 5.79 cpu allocatable, 2.56 requested by
#      non-sandbox pods, 1 cpu per sandbox -> fits 3; quota ceiling 8.
#   2. A cordoned node is excluded.
#   3. A NoSchedule-tainted node is excluded unless tolerated.
#   4. Sandbox-class pods and Succeeded pods are not subtracted.
#   5. Quantity parsing: "1500m", "0.5", "2" cpu; Gi/Mi/plain memory.
#   8. Shipped defaults: the ceiling reads the real values.yaml requests,
#      limits and hard, and is 8 (limitsCpu 8 / 1 cpu limit), not 42.
#   9. Exponent memory quantities parse; an unsupported form fails loudly.
#  10. NOTES with lookup stubbed to fixture Nodes/Pods renders the capacity
#      block under --set placement=null (placement read via the helper); with
#      resourceQuota.capacityReport=false, lookup is replaced by `fail` and the
#      render still succeeds, so no lookup is evaluated.
#   6. The warning renders when ceiling > fits and not when ceiling <= fits
#      (negative control on the same define NOTES.txt calls).
#
# Renders to --output-dir and reads the file, never a stdout pipe (see the
# sibling scripts). Fails loudly, naming the assertion.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

TEST_CHART="$TMP/chart"
cp -a "$CHART" "$TEST_CHART"
mkdir -p "$TEST_CHART/capacity-fixtures"

# Three workers at 1930m each (5.79 total). Non-sandbox requests total 2.56:
# 850m + 850m + 860m, expressed through containers, initContainers, and
# "0.5"/"1500m"-style quantities. Free: 1080m, 1080m, 1070m -> 1 each.
cat >"$TEST_CHART/capacity-fixtures/scenario.yaml" <<'YAML'
perSandbox: { cpu: "1", memory: 1Gi }
hard: { requestsCpu: "8", requestsMemory: 16Gi, sandboxPodCount: "50" }
nodes:
  - metadata: { name: a, labels: { pool: sandbox } }
    spec: {}
    status: { allocatable: { cpu: 1930m, memory: 8Gi } }
  - metadata: { name: b, labels: { pool: sandbox } }
    spec: {}
    status: { allocatable: { cpu: 1930m, memory: 8Gi } }
  - metadata: { name: c, labels: { pool: sandbox } }
    spec: {}
    status: { allocatable: { cpu: "1.93", memory: "8589934592" } }
pods:
  # a: 350m + 0.5 = 850m
  - spec:
      nodeName: a
      containers:
        - resources: { requests: { cpu: 350m, memory: 256Mi } }
        - resources: { requests: { cpu: "0.5", memory: 256Mi } }
    status: { phase: Running }
  # b: containers sum 100m, initContainer 850m wins
  - spec:
      nodeName: b
      initContainers:
        - resources: { requests: { cpu: 850m, memory: 64Mi } }
      containers:
        - resources: { requests: { cpu: 100m, memory: 64Mi } }
    status: { phase: Running }
  # c: 860m
  - spec:
      nodeName: c
      containers:
        - resources: { requests: { cpu: 860m, memory: 1Gi } }
    status: { phase: Pending }
  # Existing sandboxes: not subtracted (they are part of "fits concurrently").
  - spec:
      nodeName: a
      priorityClassName: curie-sandbox
      containers:
        - resources: { requests: { cpu: "1", memory: 1Gi } }
    status: { phase: Running }
  # Finished pods: not subtracted.
  - spec:
      nodeName: b
      containers:
        - resources: { requests: { cpu: 1500m, memory: 1Gi } }
    status: { phase: Succeeded }
  - spec:
      nodeName: c
      containers:
        - resources: { requests: { cpu: "2", memory: 1Gi } }
    status: { phase: Failed }
  # Unbound pod: not subtracted.
  - spec:
      containers:
        - resources: { requests: { cpu: "4" } }
    status: { phase: Pending }
YAML

cat >"$TEST_CHART/capacity-fixtures/extra-nodes.yaml" <<'YAML'
cordoned:
  metadata: { name: d, labels: { pool: sandbox } }
  spec: { unschedulable: true }
  status: { allocatable: { cpu: "16", memory: 64Gi } }
tainted:
  metadata: { name: e, labels: { pool: sandbox } }
  spec:
    taints:
      - { key: dedicated, value: gpu, effect: NoSchedule }
  status: { allocatable: { cpu: "4", memory: 64Gi } }
prefer:
  metadata: { name: f, labels: { pool: sandbox } }
  spec:
    taints:
      - { key: soft, value: x, effect: PreferNoSchedule }
  status: { allocatable: { cpu: "2", memory: 64Gi } }
otherpool:
  metadata: { name: g, labels: { pool: platform } }
  spec: {}
  status: { allocatable: { cpu: "8", memory: 64Gi } }
YAML

cat >"$TEST_CHART/templates/zz-capacity-check.yaml" <<'EOF2'
{{- $s := .Files.Get "capacity-fixtures/scenario.yaml" | fromYaml }}
{{- $x := .Files.Get "capacity-fixtures/extra-nodes.yaml" | fromYaml }}
{{- $base := dict "pods" $s.pods "perSandbox" $s.perSandbox "sandboxPriorityClass" "curie-sandbox" "tolerations" list "nodeSelector" dict "hard" $s.hard }}
{{- $tolEq := list (dict "key" "dedicated" "operator" "Equal" "value" "gpu" "effect" "NoSchedule") }}
{{- $tolWrong := list (dict "key" "dedicated" "operator" "Equal" "value" "cpu") }}
{{- $tolAll := list (dict "operator" "Exists") }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: capacity-check
data:
  scenario: {{ include "curie.sandboxCapacity" (merge (dict "nodes" $s.nodes) $base) | quote }}
  cordoned: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.cordoned)) $base) | quote }}
  tainted: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.tainted)) $base) | quote }}
  taintedWrongValue: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.tainted) "tolerations" $tolWrong) $base) | quote }}
  taintedTolerated: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.tainted) "tolerations" $tolEq) $base) | quote }}
  taintedExistsAll: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.tainted) "tolerations" $tolAll) $base) | quote }}
  preferNoSchedule: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.prefer)) $base) | quote }}
  nodeSelector: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.otherpool) "nodeSelector" (dict "pool" "sandbox")) $base) | quote }}
  noSelector: {{ include "curie.sandboxCapacity" (merge (dict "nodes" (append $s.nodes $x.otherpool)) $base) | quote }}
  memBound: {{ include "curie.sandboxCapacity" (merge (dict "nodes" $s.nodes "perSandbox" (dict "cpu" "100m" "memory" "3Gi")) $base) | quote }}
  ceilingPods: {{ include "curie.sandboxCapacity" (merge (dict "nodes" $s.nodes "hard" (dict "requestsCpu" "8" "requestsMemory" "16Gi" "sandboxPodCount" "5")) $base) | quote }}
  ceilingMem: {{ include "curie.sandboxCapacity" (merge (dict "nodes" $s.nodes "perSandbox" (dict "cpu" "500m" "memory" "1536Mi")) $base) | quote }}
  cpu1500m: {{ include "curie.capacity.cpuMillis" "1500m" | quote }}
  cpuHalf: {{ include "curie.capacity.cpuMillis" "0.5" | quote }}
  cpu2: {{ include "curie.capacity.cpuMillis" "2" | quote }}
  cpuInt: {{ include "curie.capacity.cpuMillis" 3 | quote }}
  memGi: {{ include "curie.capacity.memBytes" "8Gi" | quote }}
  memMi: {{ include "curie.capacity.memBytes" "192Mi" | quote }}
  memG: {{ include "curie.capacity.memBytes" "1G" | quote }}
  memHalfGi: {{ include "curie.capacity.memBytes" "1.5Gi" | quote }}
  memPlain: {{ include "curie.capacity.memBytes" "1024" | quote }}
  memExp: {{ include "curie.capacity.memBytes" "1e9" | quote }}
  memExpUpper: {{ include "curie.capacity.memBytes" "1E3" | quote }}
  memExpFrac: {{ include "curie.capacity.memBytes" "1.5e3" | quote }}
  memMilli: {{ include "curie.capacity.memBytes" "1500m" | quote }}
  memMilliLarge: {{ include "curie.capacity.memBytes" "2000000m" | quote }}
  memMilliSub1: {{ include "curie.capacity.memBytes" "500m" | quote }}
  defaults: {{ include "curie.sandboxCapacity" (dict "nodes" $s.nodes "pods" list "perSandbox" .Values.agentSandbox.runner.resources.requests "perSandboxLimits" .Values.agentSandbox.runner.resources.limits "sandboxPriorityClass" "curie-sandbox" "hard" .Values.resourceQuota.hard) | quote }}
  notesOver: |
{{- include "curie.sandboxCapacity.notes" (dict "fits" 3 "ceiling" 8 "cpu" "1" "memory" "1Gi" "fullname" "curie" "namespace" "ns") | nindent 4 }}
  notesEqual: |
{{- include "curie.sandboxCapacity.notes" (dict "fits" 8 "ceiling" 8 "cpu" "1" "memory" "1Gi" "fullname" "curie" "namespace" "ns") | nindent 4 }}
  notesUnder: |
{{- include "curie.sandboxCapacity.notes" (dict "fits" 20 "ceiling" 8 "cpu" "1" "memory" "1Gi" "fullname" "curie" "namespace" "ns") | nindent 4 }}
EOF2

helm template curie "$TEST_CHART" --output-dir "$TMP/out" >/dev/null
OUT="$TMP/out/curie/templates/zz-capacity-check.yaml"
[ -s "$OUT" ] || fail "test template did not render"

python3 - "$OUT" <<'PY'
import json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))["data"]
fails = []
def cap(k): return json.loads(d[k])
def check(name, got, want):
    if got != want:
        fails.append(f"{name}: got {got!r}, want {want!r}")

s = cap("scenario")
check("1 issue scenario fits", s["fits"], 3)
check("1 issue scenario ceiling", s["ceiling"], 8)
check("1 issue scenario nodes", s["nodes"], 3)
check("2 cordoned node excluded", cap("cordoned")["fits"], 3)
check("3 tainted node excluded", cap("tainted")["fits"], 3)
check("3 taint with wrong value not tolerated", cap("taintedWrongValue")["fits"], 3)
check("3 tainted node tolerated (Equal)", cap("taintedTolerated")["fits"], 7)
check("3 tainted node tolerated (Exists, empty key)", cap("taintedExistsAll")["fits"], 7)
check("3 PreferNoSchedule does not exclude", cap("preferNoSchedule")["fits"], 5)
check("3 nodeSelector excludes other pool", cap("nodeSelector")["fits"], 3)
check("3 control: no nodeSelector includes other pool", cap("noSelector")["fits"], 11)
# memory: node a free 8Gi-512Mi=7.5Gi -> 2; b 8Gi-64Mi -> 2; c 7Gi -> 2
check("5 memory-bound fits", cap("memBound")["fits"], 6)
check("ceiling bound by pods", cap("ceilingPods")["ceiling"], 5)
# 16Gi / 1.5Gi = 10.67 -> 10; cpu 8/0.5 = 16
check("ceiling bound by memory", cap("ceilingMem")["ceiling"], 10)
check("5 cpu 1500m", d["cpu1500m"], "1500")
check("5 cpu 0.5", d["cpuHalf"], "500")
check("5 cpu 2", d["cpu2"], "2000")
check("5 cpu int", d["cpuInt"], "3000")
check("5 mem Gi", d["memGi"], str(8 * 2**30))
check("5 mem Mi", d["memMi"], str(192 * 2**20))
check("5 mem G", d["memG"], "1000000000")
check("5 mem 1.5Gi", d["memHalfGi"], str(int(1.5 * 2**30)))
check("5 mem plain", d["memPlain"], "1024")
check("9 mem 1e9", d["memExp"], "1000000000")
check("9 mem 1E3", d["memExpUpper"], "1000")
check("9 mem 1.5e3", d["memExpFrac"], "1500")
check("9 mem 1500m", d["memMilli"], "1")
check("9 mem 2000000m", d["memMilliLarge"], "2000")
check("9 mem 500m rounds down to 0", d["memMilliSub1"], "0")
check("8 shipped defaults ceiling (limitsCpu binds)", cap("defaults")["ceiling"], 8)

over, eq, under = d["notesOver"], d["notesEqual"], d["notesUnder"]
if "!! WARNING" not in over or "admits 8 sandboxes" not in over or "fit only 3" not in over:
    fails.append("6 warning missing when ceiling > fits:\n" + over)
for sig in ("exceeded quota", "describe resourcequota curie-sandbox-quota -n ns",
            "reason=FailedScheduling -n ns", "lower resourceQuota.hard"):
    if sig not in over:
        fails.append(f"6 warning lacks {sig!r}")
for name, text in (("equal", eq), ("under", under)):
    if "WARNING" in text:
        fails.append(f"6 negative control: warning rendered when ceiling <= fits ({name})")
if "admits 8" not in under or "fit 20" not in under:
    fails.append("6 informative line missing both numbers:\n" + under)

if fails:
    print("ASSERTION FAILED:\n  " + "\n  ".join(fails), file=sys.stderr)
    sys.exit(1)
PY

# The published chart must render NOTES cleanly when lookup is empty
# (helm template / dry-run): the capacity block prints nothing.
NOTES_CHART="$TMP/notes-chart"
cp -a "$CHART" "$NOTES_CHART"
cp "$CHART/templates/NOTES.txt" "$NOTES_CHART/NOTES.txt"
cat >"$NOTES_CHART/templates/notes-check.yaml" <<'EOF2'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF2
helm template curie "$NOTES_CHART" --output-dir "$TMP/notes-out" >/dev/null
NOTES_OUT="$TMP/notes-out/curie/templates/notes-check.yaml"
[ -s "$NOTES_OUT" ] || fail "7 NOTES did not render"
grep -q "Kernel isolation" "$NOTES_OUT" || fail "7 NOTES render lacks the sandbox section (probe is not reaching it)"
if grep -q "Sandbox capacity" "$NOTES_OUT"; then
  fail "7 NOTES printed a capacity block with no nodes from lookup"
fi

# 9. An unsupported memory quantity fails rendering loudly, naming it.
BAD_CHART="$TMP/bad-chart"
cp -a "$CHART" "$BAD_CHART"
cat >"$BAD_CHART/templates/zz-bad.yaml" <<'EOF2'
apiVersion: v1
kind: ConfigMap
metadata:
  name: bad
data:
  v: {{ include "curie.capacity.memBytes" "12Qi" | quote }}
EOF2
if helm template curie "$BAD_CHART" --output-dir "$TMP/bad-out" >"$TMP/bad.log" 2>&1; then
  fail "9 unsupported memory quantity rendered instead of failing"
fi
grep -q 'unsupported memory quantity \\"12Qi\\"\|unsupported memory quantity "12Qi"' "$TMP/bad.log" \
  || fail "9 failure message does not name the quantity: $(cat "$TMP/bad.log")"

# 10. NOTES with lookup stubbed. Each lookup call is swapped for fixture data
# (or for `fail`), so the lookup-dependent block really executes offline
# instead of short-circuiting on an empty lookup.
stub_notes() { # $1 dest chart, $2 node expr, $3 pod expr
  cp -a "$TEST_CHART" "$1"
  rm -f "$1/templates/zz-capacity-check.yaml"
  python3 -c '
import sys
src, dst, node, pod = sys.argv[1:]
t = open(src).read()
n1, n2 = "(lookup \"v1\" \"Node\" \"\" \"\")", "(lookup \"v1\" \"Pod\" \"\" \"\")"
assert t.count(n1) == 1 and t.count(n2) == 1, "NOTES lookup calls changed shape"
open(dst, "w").write(t.replace(n1, node).replace(n2, pod))
' "$CHART/templates/NOTES.txt" "$1/NOTES.txt" "$2" "$3"
  cat >"$1/templates/notes-check.yaml" <<'EOF2'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF2
}
FIXN='(dict "items" (.Files.Get "capacity-fixtures/scenario.yaml" | fromYaml).nodes)'
FIXP='(dict "items" (.Files.Get "capacity-fixtures/scenario.yaml" | fromYaml).pods)'
stub_notes "$TMP/stub" "$FIXN" "$FIXP"
helm template curie "$TMP/stub" --set placement=null --output-dir "$TMP/stub-out" >"$TMP/stub.log" 2>&1 \
  || fail "10 NOTES failed to render with placement=null: $(cat "$TMP/stub.log")"
grep -q "Sandbox capacity" "$TMP/stub-out/curie/templates/notes-check.yaml" \
  || fail "10 stubbed NOTES did not reach the capacity block"
grep -q "admits 8[;s ]" "$TMP/stub-out/curie/templates/notes-check.yaml" \
  || fail "10 stubbed NOTES ceiling is not the defaults' 8"

STUBFAIL='(fail "lookup evaluated with capacityReport=false")'
stub_notes "$TMP/stub-off" "$STUBFAIL" "$STUBFAIL"
helm template curie "$TMP/stub-off" --set resourceQuota.capacityReport=false --output-dir "$TMP/stub-off-out" >"$TMP/off.log" 2>&1 \
  || fail "10 capacityReport=false still evaluated a lookup: $(cat "$TMP/off.log")"
if grep -q "Sandbox capacity" "$TMP/stub-off-out/curie/templates/notes-check.yaml"; then
  fail "10 capacityReport=false still printed the capacity block"
fi
# Negative control: the same fail stub with the report on must fail.
if helm template curie "$TMP/stub-off" --output-dir "$TMP/stub-on-out" >/dev/null 2>&1; then
  fail "10 negative control: fail stub did not fire with capacityReport=true"
fi

echo "OK: sandbox capacity fit assertions passed (#2949)"
