#!/usr/bin/env bash
#
# Render-assertion test for the compute-plane PodDisruptionBudgets (issue
# #1574). The chart's PDB template covered only the four backing stores, so the
# api, worker, ui and dispatcher Deployments could not be protected from a
# voluntary drain even by opting in. Each now carries an optional
# `<component>.podDisruptionBudget` block, off by default.
#
# A budget is only worth anything if it selects exactly the pods it names. A
# selector that drifts to another component's labels, or drops the component
# label and matches every pod in the release, still renders, still passes
# kubeconform, and still shows up in `kubectl get pdb`. The checker below reads
# the selector against every rendered pod template, so it is the check that
# catches that drift, and assertion 3 proves it does by feeding it one.
#
#   1. DEFAULT render: no PodDisruptionBudget of any kind.
#   2. POSITIVE, worker: enabled with replicas 2 renders exactly one PDB, named
#      after the worker Deployment, with the default maxUnavailable: 1 and no
#      minAvailable, whose selector matches the worker Deployment's pod
#      template and no other rendered pod template.
#   3. NEGATIVE, selector mismatch: the assertion 2 manifest with the PDB's
#      component selector rewritten to `api`, and again with the component
#      label removed (which matches every pod in the release), must both FAIL
#      the same checker.
#   4. POSITIVE, api + ui + worker together: one PDB each, each selecting only
#      its own Deployment.
#   5. NEGATIVE, blocking budget without acknowledgement: minAvailable equal to
#      replicas, minAvailable above replicas, and maxUnavailable: 0 each refuse
#      the render and name the key and `allowBlockingDrain`.
#   6. POSITIVE, acknowledged blocking budget: the same minAvailable: 1 at
#      replicas 1 with allowBlockingDrain: true renders minAvailable: 1 and
#      drops maxUnavailable (a policy/v1 PDB carries exactly one of the two).
#   7. POSITIVE, non-blocking minAvailable: minAvailable 1 at replicas 2 renders
#      with no acknowledgement.
#   8. DISPATCHER: it is one replica with strategy Recreate by design (#2944),
#      so only an acknowledged blocking budget is accepted. The default
#      maxUnavailable: 1 is refused, an unacknowledged minAvailable: 1 is
#      refused, and minAvailable: 1 with allowBlockingDrain: true renders a PDB
#      selecting only the dispatcher pods.
#   9. SKIP WHEN NOT DEPLOYED: an enabled budget on a component that does not
#      render (worker.deploy=false, or a dispatcher with no Slack tokens)
#      renders no PDB, matching the backing-store budgets.
#  10. SCHEMA: a negative maxUnavailable, a string minAvailable and a string
#      `enabled` are refused by helm itself.
#  11. BACKING STORES UNCHANGED: postgres.podDisruptionBudget.enabled still
#      renders its minAvailable: 1 budget selecting only the postgres pods.
#
# Refusal assertions check only helm's exit status and bare key names. The
# refusal text in 5 and 8 is chart-owned `fail` output, so asserting its key
# names is stable; the schema refusals in 10 come from helm's bundled
# validator, whose wording differs across helm versions, so only the key name
# is asserted there (see ci/worker-ttl-bounds-assertions.sh).
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

SLACK=(--set dispatcher.slack.appToken=xapp-assert --set dispatcher.slack.botToken=xoxb-assert)

# Render to a directory rather than a stdout pipe: a piped `helm template` has
# been observed to truncate silently while still exiting 0.
render() {
  local name="$1"
  shift
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  mkdir -p "$RENDER_DIR"
  helm template rel "$CHART" --output-dir "$RENDER_DIR" "$@" >/dev/null \
    || fail "helm template failed for render '$name'"
}

# Expect a refused render whose combined output names every given token.
refuse() {
  local name="$1"
  shift
  local tokens=()
  while [[ "$1" != "--" ]]; do
    tokens+=("$1")
    shift
  done
  shift
  local out="$TMP/$name.out"
  if helm template rel "$CHART" "$@" >"$out" 2>&1; then
    fail "render '$name' was accepted; expected a refusal naming ${tokens[*]}"
  fi
  local token
  for token in "${tokens[@]}"; do
    grep -qF -- "$token" "$out" \
      || fail "render '$name' refused without naming '$token': $(tail -n 5 "$out")"
  done
  echo "  ok: '$name' refused, naming ${tokens[*]}"
}

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
"""PodDisruptionBudget checker.

argv[1] = a helm --output-dir tree (or one manifest file)
argv[2] = JSON object {component: {budget field: value}}; {} expects no PDB.

Every rendered PodDisruptionBudget must be one of the expected components.
Each expected component has exactly one PDB, named after that component's
workload, whose spec is exactly the selector plus the expected budget fields,
and whose selector matches that workload's pod template and no other rendered
pod template. Exits 1 with a message on the first violation.
"""
import json
import pathlib
import sys

import yaml

PATH = pathlib.Path(sys.argv[1])
EXPECTED = json.loads(sys.argv[2])
COMPONENT = "app.kubernetes.io/component"
WORKLOADS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"}


def die(message):
    raise SystemExit(f"{PATH}: {message}")


def load_docs(path):
    files = sorted(path.rglob("*.yaml")) if path.is_dir() else [path]
    docs = []
    for file_path in files:
        for doc in yaml.safe_load_all(file_path.read_text()):
            if isinstance(doc, dict):
                docs.append(doc)
    return docs


def pod_templates(docs):
    """(kind/name, pod labels) for every pod the render can create."""
    found = []
    for doc in docs:
        kind = doc.get("kind")
        name = f"{kind}/{(doc.get('metadata') or {}).get('name')}"
        spec = doc.get("spec") or {}
        if kind == "Pod":
            labels = (doc.get("metadata") or {}).get("labels")
        elif kind == "CronJob":
            labels = (
                (((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {})
                .get("metadata", {})
                .get("labels")
            )
        elif kind in WORKLOADS:
            labels = ((spec.get("template") or {}).get("metadata") or {}).get("labels")
        elif "podTemplate" in spec:
            labels = ((spec.get("podTemplate") or {}).get("metadata") or {}).get("labels")
        else:
            continue
        found.append((kind, name, labels or {}))
    return found


def selects(match_labels, labels):
    return all(labels.get(key) == value for key, value in match_labels.items())


docs = load_docs(PATH)
pdbs = [doc for doc in docs if doc.get("kind") == "PodDisruptionBudget"]
templates = pod_templates(docs)

if len(pdbs) != len(EXPECTED):
    names = sorted(doc["metadata"]["name"] for doc in pdbs)
    die(f"expected {len(EXPECTED)} PodDisruptionBudget(s) for {sorted(EXPECTED)}, got {names}")

for component, budget in sorted(EXPECTED.items()):
    owners = [
        (kind, name, labels)
        for kind, name, labels in templates
        if kind in ("Deployment", "StatefulSet") and labels.get(COMPONENT) == component
    ]
    if len(owners) != 1:
        die(f"expected one {component} Deployment or StatefulSet, got {[n for _, n, _ in owners]}")
    owner_kind, owner_name, owner_labels = owners[0]
    workload = owner_name.split("/", 1)[1]
    mine = [doc for doc in pdbs if doc["metadata"]["name"] == workload]
    if len(mine) != 1:
        die(f"expected one PodDisruptionBudget named {workload}, got {len(mine)}")
    spec = mine[0].get("spec") or {}
    fields = {key: value for key, value in spec.items() if key != "selector"}
    if fields != budget:
        die(f"{workload} budget is {fields}, expected exactly {budget}")
    match_labels = (spec.get("selector") or {}).get("matchLabels") or {}
    if set(spec.get("selector") or {}) != {"matchLabels"} or not match_labels:
        die(f"{workload} selector must be a non-empty matchLabels only: {spec.get('selector')}")
    if not selects(match_labels, owner_labels):
        die(f"{workload} selector {match_labels} does not select its own {owner_name} pods {owner_labels}")
    strays = [name for _, name, labels in templates if name != owner_name and selects(match_labels, labels)]
    if strays:
        die(f"{workload} selector {match_labels} also selects {strays}")
    print(f"  ok: {workload} {fields} selects only {owner_name}")

if not EXPECTED:
    print("  ok: no PodDisruptionBudget rendered")
PY

check() { python3 "$CHECKER" "$@"; }

echo "=== Assertion 1: the default render has no PodDisruptionBudget ==="
render default
check "$RENDER_DIR" '{}' || fail "the default render carries a PodDisruptionBudget"

echo "=== Assertion 2: worker budget at replicas 2 selects exactly the worker pods ==="
render worker --set worker.replicas=2 --set worker.podDisruptionBudget.enabled=true
WORKER_DIR="$RENDER_DIR"
check "$WORKER_DIR" '{"worker": {"maxUnavailable": 1}}' \
  || fail "the worker budget does not select exactly the worker pods"

echo "=== Assertion 3: a selector that drifts off the worker fails the same checker ==="
PDB_FILE="$(grep -rl '^kind: PodDisruptionBudget' "$WORKER_DIR")"
[[ -n "$PDB_FILE" ]] || fail "no rendered PodDisruptionBudget file to mutate"
for mutation in api drop; do
  MUTANT="$TMP/mutant-$mutation"
  cp -R "$WORKER_DIR" "$MUTANT"
  python3 - "${PDB_FILE/#$WORKER_DIR/$MUTANT}" "$mutation" <<'PY'
import pathlib
import sys

import yaml

path, mutation = pathlib.Path(sys.argv[1]), sys.argv[2]
docs = [doc for doc in yaml.safe_load_all(path.read_text()) if isinstance(doc, dict)]
for doc in docs:
    if doc.get("kind") == "PodDisruptionBudget":
        labels = doc["spec"]["selector"]["matchLabels"]
        if mutation == "api":
            labels["app.kubernetes.io/component"] = "api"
        else:
            del labels["app.kubernetes.io/component"]
path.write_text(yaml.safe_dump_all(docs))
PY
  if check "$MUTANT" '{"worker": {"maxUnavailable": 1}}' 2>"$TMP/mutant-$mutation.err"; then
    fail "a worker PDB whose selector was mutated ($mutation) still passed the checker"
  fi
  echo "  ok: selector mutation '$mutation' fails the checker: $(cat "$TMP/mutant-$mutation.err")"
done

echo "=== Assertion 4: api, ui and worker budgets each select only their own pods ==="
render three \
  --set api.replicas=2 --set api.podDisruptionBudget.enabled=true \
  --set ui.replicas=2 --set ui.podDisruptionBudget.enabled=true \
  --set worker.replicas=2 --set worker.podDisruptionBudget.enabled=true
check "$RENDER_DIR" \
  '{"api": {"maxUnavailable": 1}, "ui": {"maxUnavailable": 1}, "worker": {"maxUnavailable": 1}}' \
  || fail "the api, ui and worker budgets do not each select only their own pods"

echo "=== Assertion 5: an unacknowledged blocking budget refuses the render ==="
refuse min-equals-replicas worker.podDisruptionBudget allowBlockingDrain -- \
  --set worker.podDisruptionBudget.enabled=true --set worker.podDisruptionBudget.minAvailable=1
refuse min-above-replicas api.podDisruptionBudget allowBlockingDrain -- \
  --set api.replicas=2 --set api.podDisruptionBudget.enabled=true \
  --set api.podDisruptionBudget.minAvailable=3
refuse max-zero ui.podDisruptionBudget allowBlockingDrain -- \
  --set ui.podDisruptionBudget.enabled=true --set ui.podDisruptionBudget.maxUnavailable=0

echo "=== Assertion 6: an acknowledged blocking budget renders minAvailable only ==="
render acknowledged \
  --set worker.podDisruptionBudget.enabled=true \
  --set worker.podDisruptionBudget.minAvailable=1 \
  --set worker.podDisruptionBudget.allowBlockingDrain=true
check "$RENDER_DIR" '{"worker": {"minAvailable": 1}}' \
  || fail "the acknowledged blocking worker budget did not render minAvailable: 1 alone"

echo "=== Assertion 7: minAvailable below replicas needs no acknowledgement ==="
render min-below \
  --set api.replicas=2 --set api.podDisruptionBudget.enabled=true \
  --set api.podDisruptionBudget.minAvailable=1
check "$RENDER_DIR" '{"api": {"minAvailable": 1}}' \
  || fail "a non-blocking api minAvailable budget did not render"

echo "=== Assertion 8: the dispatcher accepts only an acknowledged blocking budget ==="
refuse dispatcher-default dispatcher.podDisruptionBudget Recreate -- \
  "${SLACK[@]}" --set dispatcher.podDisruptionBudget.enabled=true
refuse dispatcher-unacknowledged dispatcher.podDisruptionBudget allowBlockingDrain -- \
  "${SLACK[@]}" --set dispatcher.podDisruptionBudget.enabled=true \
  --set dispatcher.podDisruptionBudget.minAvailable=1
render dispatcher "${SLACK[@]}" \
  --set dispatcher.podDisruptionBudget.enabled=true \
  --set dispatcher.podDisruptionBudget.minAvailable=1 \
  --set dispatcher.podDisruptionBudget.allowBlockingDrain=true
check "$RENDER_DIR" '{"dispatcher": {"minAvailable": 1}}' \
  || fail "the acknowledged dispatcher budget does not select exactly the dispatcher pods"

echo "=== Assertion 9: an enabled budget on a component that does not render is skipped ==="
render worker-off --set worker.deploy=false --set worker.podDisruptionBudget.enabled=true
check "$RENDER_DIR" '{}' || fail "worker.deploy=false still rendered a worker budget"
render dispatcher-off \
  --set dispatcher.podDisruptionBudget.enabled=true \
  --set dispatcher.podDisruptionBudget.minAvailable=1 \
  --set dispatcher.podDisruptionBudget.allowBlockingDrain=true
check "$RENDER_DIR" '{}' || fail "a token-less install still rendered a dispatcher budget"

echo "=== Assertion 10: the schema refuses malformed budget values ==="
refuse schema-negative podDisruptionBudget maxUnavailable -- --set worker.podDisruptionBudget.maxUnavailable=-1
refuse schema-string-min podDisruptionBudget minAvailable -- --set-string api.podDisruptionBudget.minAvailable=1
refuse schema-string-enabled podDisruptionBudget enabled -- --set-string ui.podDisruptionBudget.enabled=yes

echo "=== Assertion 11: backing-store budgets are unchanged ==="
render postgres --set postgres.podDisruptionBudget.enabled=true
check "$RENDER_DIR" '{"postgres": {"minAvailable": 1}}' \
  || fail "the postgres budget changed shape"

echo
echo "PASS: compute-plane PodDisruptionBudgets select exactly their own pods, refuse unacknowledged blocking budgets, and keep the dispatcher single-replica."
