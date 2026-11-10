#!/usr/bin/env bash
#
# Render-assertion test for issues #2321 and #4397. The publication Job's
# HTTPS egress renders from the factory code host entries: each entry's own
# egress, or the chart's built-in GitHub set for a github entry with none, and
# the built-in set when no code host is configured. If that list came out
# empty, a range over it would collapse the rule to an empty `to:`, which in
# NetworkPolicy means every destination on that port. The publication job is
# tokenless; that isolation is the control that makes it safe.
#
# This pins:
#
#   1. The default render, and the values-dev overlay, never emit a
#      NetworkPolicy egress rule whose `to:` is missing or empty.
#   2. A `--reuse-values` release with no factory key, and a release with a
#      per-agent forge sandbox policy, emit no empty `to:` either.
#   3. A code host that is not github and names no egress leaves publication
#      nothing to reach: the render fails naming egress rather than shipping
#      allow-all.
#   4. Disabling publication still renders with that same code host: the
#      guard is scoped to the job that would otherwise fail open, not a
#      blanket required key.
#   5. Negative control: strip the egress fail guard from
#      publication-owner.yaml, render the same values, and require the render
#      to succeed (the refusal is that guard's) and, when it emits an HTTPS
#      rule, the empty-`to:` checker to reject it.
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
# A code host the chart has no built-in set for, with no egress of its own.
NO_EGRESS_HOST='factory.codeHosts=[{"name":"gl","kind":"gitlab","host":"gitlab.internal.example","apiUrl":"https://gitlab.internal.example/api/v4","credential":{"type":"static_token","existingSecret":"gl","existingSecretKey":"token","neverExpires":true}}]'

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

echo "=== Assertion 3: no-factory and forge sandbox renders have no empty to: ==="
NULL_OUT="$(render_dir factory-null --set-json 'factory=null')"
assert_no_empty_to "factory=null (--reuse-values) render" "$NULL_OUT"
FORGE_OUT="$(render_dir forge-sandbox -f "$CHART/values-dev.yaml" \
  --set agentSandbox.deploy=true --set agentSandbox.controller.deploy=false \
  --set 'factory.sandboxEgress.dark-factory=true')"
assert_no_empty_to "forge sandbox render" "$FORGE_OUT"
grep -R -q 'allow-forge-egress' "$FORGE_OUT" \
  || fail "factory.sandboxEgress.dark-factory=true rendered no forge egress policy"
echo "ok: the built-in GitHub set fills publication with no factory key, and the forge sandbox policy has peers"

echo "=== Assertion 4: a code host with no egress and no built-in set fails the render ==="
must_fail_naming "no-egress-code-host" "egress" --set-json "$NO_EGRESS_HOST"

echo "=== Assertion 5: publication disabled still renders with that code host ==="
DISABLED_OUT="$(render_dir publication-disabled \
  --set worker.publication.enabled=false \
  --set-json "$NO_EGRESS_HOST")"
assert_no_empty_to "publication-disabled render" "$DISABLED_OUT"
if grep -R -q 'component: publication' "$DISABLED_OUT/curie/templates/publication-owner.yaml" 2>/dev/null; then
  fail "publication.enabled=false still rendered a publication NetworkPolicy"
fi
echo "ok: publication.enabled=false with a code host that has no egress still renders"

echo "=== Assertion 6 negative control: the refusal is publication-owner.yaml's empty-list guard ==="
MUTANT="$TMP/mutant-chart"
cp -a "$CHART" "$MUTANT"
python3 - "$MUTANT/templates/publication-owner.yaml" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
# Every fail in this template that refuses missing egress; replace the call and
# keep the surrounding if/end so the template still parses.
pattern = re.compile(r'\{\{-?\s*fail\s+(\(printf\s+)?"[^"]*egress[^"]*"[^}]*\}\}')
mutated, count = pattern.subn("{{- /* guard stripped by the negative control */ -}}", text)
if count < 1:
    raise SystemExit("negative control found no egress fail guard in publication-owner.yaml")
path.write_text(mutated)
PY
MUTANT_OUT="$TMP/mutant-null"
mkdir -p "$MUTANT_OUT"
set +e
helm template "$RELEASE" "$MUTANT" \
  --set-json "$NO_EGRESS_HOST" \
  --output-dir "$MUTANT_OUT" >/dev/null 2>"$TMP/mutant-null.err"
mutant_rc=$?
set -e
[ "$mutant_rc" -eq 0 ] \
  || fail "with the publication-owner.yaml guard stripped the render still fails, so the refusal is not that guard's: $(cat "$TMP/mutant-null.err")"
if python3 - "$MUTANT_OUT" <<'PY'
import pathlib
import sys

import yaml

for path in pathlib.Path(sys.argv[1]).rglob("publication-owner.yaml"):
    for doc in yaml.safe_load_all(path.read_text()):
        if isinstance(doc, dict) and doc.get("kind") == "NetworkPolicy":
            for rule in (doc.get("spec") or {}).get("egress") or []:
                if {p.get("port") for p in rule.get("ports") or []} != {53}:
                    sys.exit(0)
sys.exit(1)
PY
then
  if python3 "$CHECKER" "$MUTANT_OUT" >/dev/null 2>"$TMP/mutant-check.err"; then
    fail "negative control did not fire: an empty code host egress list passed the empty-to checker"
  fi
  grep -q "empty or missing to" "$TMP/mutant-check.err" \
    || fail "empty-to negative control failed unexpectedly: $(cat "$TMP/mutant-check.err")"
  echo "ok: without the guard the empty list renders an allow-all to:, which the checker rejects"
else
  echo "ok: without the guard the render succeeds (the refusal is the guard's) and emits no HTTPS rule"
fi

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
echo "PASS: no rendered NetworkPolicy egress rule has an empty to:; a code host with no egress fails closed naming egress; disabling publication still renders; the empty-to checker is proven to fire."
