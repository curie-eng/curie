#!/usr/bin/env bash
#
# Render-assertion test for issue #2321. A nil
# worker.publication.githubHttpsCidrs makes the range in publication-owner.yaml
# emit nothing, collapsing the GitHub HTTPS egress rule to an empty `to:`,
# which in NetworkPolicy means every destination on TCP 443. The publication
# job is tokenless; that isolation is the control that makes it safe.
#
# values.schema.json minItems:1 catches `[]` and not nil: Helm drops a
# null-valued key during coalescing before schema validation runs, the same
# gap #2008 recorded for placement.
#
# This pins:
#
#   1. The default render, and the values-dev overlay, never emit a
#      NetworkPolicy egress rule whose `to:` is missing or empty.
#   2. `--set worker.publication.githubHttpsCidrs=null` fails the render and
#      names worker.publication.githubHttpsCidrs, rather than shipping
#      allow-all on 443.
#   3. An explicit empty list is still refused (schema path), naming
#      githubHttpsCidrs, a second independent refusal of "no ranges".
#   4. Disabling publication still renders when the key is nulled: the guard
#      is scoped to the job that would otherwise fail open, not a blanket
#      required key.
#   5. Negative control: strip the fail guard, render with the key nulled,
#      and require the empty-`to:` checker to reject the result. An assert
#      that has never been shown to fail is not pinning anything.
#   6. Negative control: an empty NetworkPolicyPeer (`to: [{}]`) is the
#      same allow-all form and must also be rejected.
#
# Runnable locally and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RELEASE=curie
NIL_KEY='worker.publication.githubHttpsCidrs'

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

CHECKER="$TMP/empty_to.py"
cat > "$CHECKER" <<'PY'
import pathlib
import sys

import yaml

root = pathlib.Path(sys.argv[1])
hits = []
policies = 0

PEER_FIELDS = ("ipBlock", "namespaceSelector", "podSelector")


def peer_is_empty(peer):
    if not isinstance(peer, dict):
        return True
    return not any(peer.get(field) for field in PEER_FIELDS)


def dest_is_allow_all(dest):
    if dest in (None, [], {}):
        return True
    if isinstance(dest, list) and any(peer_is_empty(peer) for peer in dest):
        return True
    return False


for path in sorted(root.rglob("*.yaml")):
    text = path.read_text()
    if not text.strip():
        continue
    for document in yaml.safe_load_all(text):
        if not isinstance(document, dict) or document.get("kind") != "NetworkPolicy":
            continue
        policies += 1
        meta = document.get("metadata") or {}
        name = meta.get("name")
        namespace = meta.get("namespace")
        egress = (document.get("spec") or {}).get("egress") or []
        for index, rule in enumerate(egress):
            if not isinstance(rule, dict):
                hits.append((str(path), name, namespace, index, rule))
                continue
            if "to" not in rule:
                hits.append((str(path), name, namespace, index, "missing to"))
                continue
            dest = rule["to"]
            if dest_is_allow_all(dest):
                hits.append((str(path), name, namespace, index, dest))

if hits:
    sys.stderr.write(
        "NetworkPolicy egress rule with empty or missing to: (allow-all destinations)\n"
    )
    for path, name, namespace, index, dest in hits:
        sys.stderr.write(
            "  %s name=%r namespace=%r rule[%s] to=%r\n"
            % (path, name, namespace, index, dest)
        )
    sys.exit(1)

if policies < 1:
    sys.stderr.write("%s: Helm wrote no NetworkPolicy documents\n" % root)
    sys.exit(1)

print("ok: %d NetworkPolicy object(s) with no empty egress to:" % policies)
PY

assert_no_empty_to() {
  local label="$1"
  local rendered="$2"
  python3 "$CHECKER" "$rendered" \
    || fail "$label rendered a NetworkPolicy egress rule with an empty to:"
}

render_dir() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --output-dir "$out" "$@" >/dev/null \
    || fail "$name: helm template failed: $*"
  printf '%s\n' "$out"
}

must_fail_naming() {
  local label="$1"
  local needle="$2"
  shift 2
  local out="$TMP/${label}.txt"
  set +e
  helm template "$RELEASE" "$CHART" "$@" >"$out" 2>&1
  local rc=$?
  set -e
  [ "$rc" -ne 0 ] || fail "$label rendered successfully; this configuration must fail closed"
  grep -q "$needle" "$out" \
    || fail "$label failed without naming $needle; output was: $(cat "$out")"
  echo "ok: $label is refused at render, naming $needle"
}

echo "=== Assertion 1: default render has no empty NetworkPolicy egress to: ==="
DEFAULT_OUT="$(render_dir default)"
assert_no_empty_to "default render" "$DEFAULT_OUT"

echo "=== Assertion 2: values-dev overlay has no empty NetworkPolicy egress to: ==="
DEV_OUT="$(render_dir values-dev -f "$CHART/values-dev.yaml")"
assert_no_empty_to "values-dev overlay" "$DEV_OUT"

echo "=== Assertion 3: nil githubHttpsCidrs fails the render, naming the value ==="
must_fail_naming \
  "nil-githubHttpsCidrs" \
  "$NIL_KEY" \
  --set worker.publication.githubHttpsCidrs=null

echo "=== Assertion 4: empty-list githubHttpsCidrs is refused, naming the key ==="
EMPTY_LIST="$TMP/empty-list.yaml"
cat > "$EMPTY_LIST" <<'EOF'
worker:
  publication:
    githubHttpsCidrs: []
EOF
must_fail_naming \
  "empty-list-githubHttpsCidrs" \
  "githubHttpsCidrs" \
  -f "$EMPTY_LIST"

echo "=== Assertion 5: publication disabled still renders when the key is nulled ==="
DISABLED_OUT="$(render_dir publication-disabled \
  --set worker.publication.enabled=false \
  --set worker.publication.githubHttpsCidrs=null)"
assert_no_empty_to "publication-disabled render" "$DISABLED_OUT"
if grep -R -q 'component: publication' "$DISABLED_OUT/curie/templates/publication-owner.yaml" 2>/dev/null; then
  fail "publication.enabled=false still rendered a publication NetworkPolicy"
fi
echo "ok: publication.enabled=false with a nulled CIDR list still renders"

echo "=== Assertion 6 negative control: empty-to checker rejects a nulled CIDR list ==="
MUTANT="$TMP/mutant-chart"
cp -a "$CHART" "$MUTANT"
python3 - "$MUTANT/templates/publication-owner.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = path.read_text().splitlines(keepends=True)
out = []
stripped = 0
index = 0
while index < len(lines):
    if "if not .Values.worker.publication.githubHttpsCidrs" in lines[index]:
        block = "".join(lines[index:index + 3])
        if "fail" in block and "end" in block:
            index += 3
            stripped += 1
            continue
    out.append(lines[index])
    index += 1
if stripped > 1:
    raise SystemExit("negative control found more than one githubHttpsCidrs fail guard")
path.write_text("".join(out))
PY
MUTANT_OUT="$TMP/mutant-null"
mkdir -p "$MUTANT_OUT"
set +e
helm template "$RELEASE" "$MUTANT" \
  --set worker.publication.githubHttpsCidrs=null \
  --output-dir "$MUTANT_OUT" >/dev/null 2>"$TMP/mutant-null.err"
mutant_rc=$?
set -e
[ "$mutant_rc" -eq 0 ] \
  || fail "negative-control mutant must render so the empty-to checker can reject it; helm said: $(cat "$TMP/mutant-null.err")"
if python3 "$CHECKER" "$MUTANT_OUT" >/dev/null 2>"$TMP/mutant-check.err"; then
  fail "negative control did not fire: a nulled githubHttpsCidrs list passed the empty-to checker"
fi
if ! grep -q "empty or missing to" "$TMP/mutant-check.err"; then
  fail "empty-to negative control failed unexpectedly: $(cat "$TMP/mutant-check.err")"
fi
echo "ok: a nulled CIDR list is rejected by the empty-to checker (the assert can fail)"

echo "=== Assertion 7 negative control: empty NetworkPolicyPeer to: [{}] is rejected ==="
EMPTY_PEER="$TMP/empty-peer"
mkdir -p "$EMPTY_PEER"
python3 - "$DEFAULT_OUT" "$EMPTY_PEER" <<'PY'
import pathlib
import shutil
import sys

import yaml

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
mutated = False
for path in sorted(src.rglob("*.yaml")):
    rel = path.relative_to(src)
    target = dst / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    text = path.read_text()
    if mutated or "kind: NetworkPolicy" not in text:
        shutil.copy2(path, target)
        continue
    documents = list(yaml.safe_load_all(text))
    rewritten = []
    for document in documents:
        rewritten.append(document)
        if mutated or not isinstance(document, dict) or document.get("kind") != "NetworkPolicy":
            continue
        egress = (document.get("spec") or {}).get("egress") or []
        if not egress:
            continue
        egress[0]["to"] = [{}]
        mutated = True
    target.write_text("".join("---\n" + yaml.safe_dump(doc) for doc in rewritten if doc))
if not mutated:
    raise SystemExit("negative control could not find a NetworkPolicy egress rule to mutate")
PY
if python3 "$CHECKER" "$EMPTY_PEER" >/dev/null 2>"$TMP/empty-peer-check.err"; then
  fail "negative control did not fire: to: [{}] passed the empty-to checker"
fi
if ! grep -q "empty or missing to" "$TMP/empty-peer-check.err"; then
  fail "empty-peer negative control failed unexpectedly: $(cat "$TMP/empty-peer-check.err")"
fi
echo "ok: to: [{}] is rejected by the empty-to checker (the assert can fail)"

echo
echo "PASS: no rendered NetworkPolicy egress rule has an empty to:; a nil or empty worker.publication.githubHttpsCidrs fails closed naming the value; disabling publication still renders; the empty-to checker is proven to fire."
