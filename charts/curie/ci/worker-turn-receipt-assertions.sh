#!/usr/bin/env bash
#
# Render-assertion test for the install's turn receipt mode (ADR-0180, #3462).
#
# `worker.turnReceipt` decides whether a turn's reply ends with the
# `_What I changed:_` receipt. The worker refuses any mode but `all`,
# `failures` and `off` at config load, so a value the chart let through would
# install green and then crash-loop every worker. Six assertions:
#
#   (a) DEFAULT: the worker Deployment carries CURIE_TURN_RECEIPT=all exactly
#       once, so an install that sets nothing keeps the ADR-0117 receipt.
#   (b) EACH MODE: failures, off and an explicit all reach the worker env
#       verbatim.
#   (c) NEGATIVE, schema: a mode outside the three (wrong case, a near miss,
#       another word, empty, a number, a boolean) fails the render, and the
#       output names the knob.
#   (d) NEGATIVE, explicit null: Helm drops a nil key before schema
#       validation, so no schema can see it; the template's own `required`
#       refuses it and names the knob.
#   (e) RESERVED: worker.extraEnv cannot set CURIE_TURN_RECEIPT beside the
#       value; the chart-owned refusal names worker.turnReceipt.
#   (f) ONE CONSUMER: across the whole render the name appears on the worker
#       container and nowhere else, so no sandbox template or other service
#       grows a copy the worker does not read.
#
# Schema wording is not asserted, only helm's exit status and the bare knob
# name, which is the one token every helm version prints (the reasoning is in
# the header of worker-ttl-bounds-assertions.sh). The (d) and (e) refusals are
# chart-owned text, so they are asserted by what they name.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }
render() { helm template curie "$CHART" "$@" 2>&1; }

# Reads every container env in the render by NAME rather than grepping, and
# reports where CURIE_TURN_RECEIPT appears. Read from a bare heredoc, not a
# command substitution, so macOS's bash 3.2 does not scan the Python for
# quotes (see worker-ttl-bounds-assertions.sh). `read -d ''` returns 1 at end
# of input, hence `|| true`.
IFS= read -r -d '' RECEIPT_ENV_PY <<'PY' || true
import sys, yaml

path, expected = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(open(path)) if d]
found = []


def walk(node, where):
    if isinstance(node, dict):
        env = node.get("env")
        if "name" in node and isinstance(env, list):
            for entry in env:
                if isinstance(entry, dict) and entry.get("name") == "CURIE_TURN_RECEIPT":
                    found.append((where, node["name"], entry))
        for child in node.values():
            walk(child, where)
    elif isinstance(node, list):
        for child in node:
            walk(child, where)


for doc in docs:
    labels = (doc.get("metadata") or {}).get("labels") or {}
    where = f"{doc.get('kind')}/{(doc.get('metadata') or {}).get('name')}"
    walk(doc, (where, labels.get("app.kubernetes.io/component")))

if len(found) != 1:
    raise SystemExit(f"CURIE_TURN_RECEIPT appears {len(found)} times, expected once: {found}")
(where, component), container, entry = found[0]
if not where.startswith("Deployment/") or component != "worker" or container != "worker":
    raise SystemExit(
        f"CURIE_TURN_RECEIPT is on {where} container {container!r} "
        f"(component {component!r}), expected the worker Deployment's worker container"
    )
if entry != {"name": "CURIE_TURN_RECEIPT", "value": expected}:
    raise SystemExit(f"CURIE_TURN_RECEIPT rendered {entry!r}, expected value {expected!r}")
PY

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Renders into a file and asserts the one CURIE_TURN_RECEIPT entry. A failed
# render fails the assertion, so a broken schema cannot pass a content check
# vacuously.
assert_mode() {
  local letter="$1" expected="$2" out="$TMP/$1-$2.yaml"
  shift 2
  if ! helm template curie "$CHART" "$@" >"$out" 2>&1; then
    fail "$letter" "the render FAILED; it must succeed
  $(head -5 "$out")"
  fi
  local msg
  if ! msg="$(python3 -c "$RECEIPT_ENV_PY" "$out" "$expected" 2>&1)"; then
    fail "$letter" "$msg"
  fi
}

# Asserts a render fails AND that its output names what it must. Captured
# rather than piped: helm exits non-zero here by design, and under
# `set -o pipefail` a pipeline would fail even when the check succeeds.
assert_refused() {
  local letter="$1" needle="$2"
  shift 2
  local out
  if out="$(render "$@")"; then
    fail "$letter" "helm accepted $* -- it must refuse it"
  fi
  grep -qF -- "$needle" <<<"$out" \
    || fail "$letter" "the refusal of $* does not name $needle
  $(head -5 <<<"$out")"
}

# (a) + (f) The shipped default, on the worker container only.
assert_mode a all

# (b) Every mode the worker accepts reaches it verbatim.
for mode in all failures off; do
  assert_mode b "$mode" --set "worker.turnReceipt=$mode"
done

# (c) The schema admits exactly the three lowercase spellings.
for bad in ALL Off failure none quiet; do
  assert_refused c turnReceipt --set "worker.turnReceipt=$bad"
done
assert_refused c turnReceipt --set-string "worker.turnReceipt="
assert_refused c turnReceipt --set "worker.turnReceipt=1"
assert_refused c turnReceipt --set "worker.turnReceipt=false"

# (d) An explicit null never reaches the schema; the template refuses it.
assert_refused d worker.turnReceipt --set "worker.turnReceipt=null"

# (e) The name is chart owned, so extraEnv cannot shadow the value.
assert_refused e CURIE_TURN_RECEIPT \
  --set "worker.extraEnv[0].name=CURIE_TURN_RECEIPT" \
  --set-string "worker.extraEnv[0].value=off"
assert_refused e worker.turnReceipt \
  --set "worker.extraEnv[0].name=CURIE_TURN_RECEIPT" \
  --set-string "worker.extraEnv[0].value=off"

echo "worker-turn-receipt-assertions: all six assertions passed"
