#!/usr/bin/env bash
#
# Render-assertion tests for the tenant capacity ceiling (#758, ADR-0059
# decision 4): the ResourceQuota and LimitRange that complete ADR-0008's
# namespace-per-tenant boundary with a bound on CONSUMPTION, not just
# reachability.
#
#   1. Default install renders the ResourceQuota scoped (via scopeSelector) to
#      the sandbox PriorityClass name, with the published hard ceiling values.
#   2. Default install renders the LimitRange with the published per-container
#      default/defaultRequest, and -- structurally -- with no min/max entry.
#      Only default/defaultRequest can ever ADD a value where a container
#      leaves one unset; min/max are enforced against a container's OWN
#      declared values too, so accidentally introducing one later could reject
#      an already-configured control-plane pod at admission time. This assert
#      is the regression net for that invariant.
#   3. The sandbox PriorityClass name is operator-overridable, so the #759
#      coordination point (this chart guesses "curie-sandbox") has a real
#      escape hatch if the two PRs' constants land different.
#   4. The quota's hard ceilings are operator-overridable (ADR-0059 decision 6).
#   5. `resourceQuota.enabled: false` / `limitRange.enabled: false` each
#      suppress just their own object.
#   6. `agentSandbox.deploy: false` suppresses BOTH (no sandbox pods, no
#      capacity envelope to bound).
#   7. The N=1 self-host render (published default values.yaml -- the actual
#      self-host topology this ADR describes: agentSandbox.deploy defaults
#      true and every first-party service shares this one release namespace;
#      values-dev.yaml is a DIFFERENT, offline scratch-cluster profile that
#      turns agentSandbox.deploy off and is not the topology in question here.
#      Dispatcher tokens are set so every control-plane Deployment renders, and
#      the opt-in inference Deployment is turned on so it is rendered too --
#      #2329 was invisible to this assertion for exactly as long as it was
#      not): every control-plane container still declares its OWN cpu+memory
#      request+limit. That is what keeps the LimitRange's defaults (assertion
#      2) from ever engaging for the control plane -- a default only fills a
#      dimension a container leaves unset, and this proves none of them do,
#      for cpu/memory. If a future container lands here without its own
#      cpu/memory, it would silently start inheriting this chart's generic
#      sandbox-shaped LimitRange default instead of an intentional value --
#      this assertion is the tripwire.
#
# NOTE ON `--output-dir`: mirrors the sibling scripts in this directory --
# render to a directory and read the written file, never a stdout pipe (a
# piped `helm template` has been observed to truncate silently while still
# exiting 0 in this environment).
#
# Fails loudly, naming the assertion. Runnable locally and from CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

# Render to a directory; echo the path (possibly absent) of a named template.
render_file() {
  # $1 = subdir name, $2 = template file under templates/, rest = extra helm args
  local name="$1" tmpl="$2"
  shift 2
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template curie "$CHART" --output-dir "$out" "$@" >/dev/null
  echo "$out/curie/templates/$tmpl"
}

PYCHECK="$TMP/check.py"
cat > "$PYCHECK" <<'PY'
"""Structural assertions over one rendered manifest, dispatched by subcommand.

argv[1] = subcommand, argv[2] = manifest/dir path, argv[3:] = subcommand args.
Exits 0 and prints nothing on success; exits 1 with a message on failure.
"""
import sys
import pathlib
import yaml


def load_one(path, kind):
    docs = [d for d in yaml.safe_load_all(pathlib.Path(path).read_text()) if d]
    matches = [d for d in docs if d.get("kind") == kind]
    if not matches:
        sys.exit(f"no {kind} document found in {path}")
    if len(matches) > 1:
        sys.exit(f"expected exactly one {kind} in {path}, found {len(matches)}")
    return matches[0]


cmd = sys.argv[1]

if cmd == "quota-scope":
    path, expect_name = sys.argv[2], sys.argv[3]
    doc = load_one(path, "ResourceQuota")
    exprs = (doc.get("spec") or {}).get("scopeSelector", {}).get("matchExpressions", [])
    if len(exprs) != 1:
        sys.exit(f"expected exactly one scopeSelector matchExpression, got {len(exprs)}: {exprs}")
    e = exprs[0]
    if e.get("scopeName") != "PriorityClass":
        sys.exit(f"scopeName is {e.get('scopeName')!r}, expected 'PriorityClass'")
    if e.get("operator") != "In":
        sys.exit(f"operator is {e.get('operator')!r}, expected 'In'")
    if e.get("values") != [expect_name]:
        sys.exit(f"scopeSelector values are {e.get('values')!r}, expected [{expect_name!r}]")

elif cmd == "quota-hard":
    path = sys.argv[2]
    expected = dict(arg.split("=", 1) for arg in sys.argv[3:])
    doc = load_one(path, "ResourceQuota")
    hard = (doc.get("spec") or {}).get("hard", {})
    for key, want in expected.items():
        got = str(hard.get(key))
        if got != want:
            sys.exit(f"hard[{key!r}] is {got!r}, expected {want!r} (full hard: {hard})")

elif cmd == "limitrange-shape":
    path = sys.argv[2]
    expected = dict(arg.split("=", 1) for arg in sys.argv[3:])
    doc = load_one(path, "LimitRange")
    limits = (doc.get("spec") or {}).get("limits", [])
    if len(limits) != 1:
        sys.exit(f"expected exactly one limits entry, got {len(limits)}: {limits}")
    entry = limits[0]
    if entry.get("type") != "Container":
        sys.exit(f"limits[0].type is {entry.get('type')!r}, expected 'Container'")
    if "min" in entry or "max" in entry:
        sys.exit(
            "limits[0] declares a 'min' or 'max' -- these are enforced against a "
            "container's OWN declared resources (not just undeclared ones) and "
            "could reject an already-configured control-plane pod at admission "
            f"time; only default/defaultRequest are safe here. Got: {entry}"
        )
    for dotted, want in expected.items():
        section, key = dotted.split(".", 1)
        got = str((entry.get(section) or {}).get(key))
        if got != want:
            sys.exit(f"{dotted} is {got!r}, expected {want!r} (full entry: {entry})")

elif cmd == "controlplane-resources":
    # Every container (non-init) across every Deployment/StatefulSet document
    # under a rendered chart dir declares its OWN cpu+memory request+limit, so
    # the LimitRange's default/defaultRequest can never engage for it. Scoped
    # to the RELEASE namespace only -- the vendored agent-sandbox controller
    # Deployment hardcodes `namespace: agent-sandbox-system` (a different,
    # cluster-scoped namespace this chart's own tenant LimitRange never
    # reaches), so a doc with an explicit, DIFFERENT namespace is out of scope
    # here by construction, not a control-plane pod this assertion governs.
    root = pathlib.Path(sys.argv[2])
    release_ns = sys.argv[3]
    dims = ("requests.cpu", "requests.memory", "limits.cpu", "limits.memory")
    missing = []
    checked = 0
    for p in sorted(root.rglob("*.yaml")):
        for doc in yaml.safe_load_all(p.read_text()):
            if not isinstance(doc, dict) or doc.get("kind") not in ("Deployment", "StatefulSet"):
                continue
            doc_ns = doc.get("metadata", {}).get("namespace")
            if doc_ns and doc_ns != release_ns:
                continue
            pod_spec = (((doc.get("spec") or {}).get("template") or {}).get("spec")) or {}
            name = doc.get("metadata", {}).get("name")
            for c in pod_spec.get("containers") or []:
                checked += 1
                res = c.get("resources") or {}
                for dotted in dims:
                    section, key = dotted.split(".", 1)
                    if (res.get(section) or {}).get(key) is None:
                        missing.append(f"{doc['kind']}/{name} container {c.get('name')!r} missing {dotted}")
    if checked < 5:
        sys.exit(f"only checked {checked} containers across Deployment/StatefulSet; "
                  "expected the full control plane to be rendered (dispatcher tokens set?)")
    if missing:
        sys.exit(
            "control-plane container(s) leave cpu/memory undeclared, which means the "
            "tenant LimitRange's default/defaultRequest WOULD engage for them (a "
            "capacity change, not a no-op) on a live cluster:\n  " + "\n  ".join(missing)
        )

else:
    sys.exit(f"unknown subcommand {cmd!r}")
PY

check() { python3 "$PYCHECK" "$@" || fail "$*"; }

echo "=== Assertion 1: default install scopes the ResourceQuota to the sandbox PriorityClass ==="
default_quota="$(render_file default tenant-resourcequota.yaml)"
[ -f "$default_quota" ] || fail "default install: tenant-resourcequota.yaml did not render"
check quota-scope "$default_quota" curie-sandbox
echo "  ok: scopeSelector keys on PriorityClass=curie-sandbox"
check quota-hard "$default_quota" \
  requests.cpu=4 requests.memory=8Gi \
  limits.cpu=8 limits.memory=16Gi pods=50
echo "  ok: published hard ceiling values render as documented"

# Regression guard (the kind cluster rung, #692, is the only place a real apply
# runs; this catches the class cheaply at render time): a PriorityClass-scoped
# ResourceQuota may ONLY constrain cpu/memory/pods. Listing ephemeral-storage
# (or any other resource) makes the API server reject the object at admission
# with "unsupported scope applied to resource" -- invisible to `helm template`.
# Ignore comment lines (the template explains the omission in a `#` comment that
# renders into the output); flag only a real resource KEY.
if grep -vE '^[[:space:]]*#' "$default_quota" | grep -q 'ephemeral-storage'; then
  fail "scoped ResourceQuota lists ephemeral-storage -- a PriorityClass scope forbids it; the API server will reject the install. Constrain per-pod disk via the LimitRange/pod limits instead."
fi
echo "  ok: scoped quota constrains only cpu/memory/pods (no admission-invalid resource)"

echo "=== Assertion 2: default install's LimitRange carries the published defaults, no min/max ==="
# Reuses assertion 1's "default" render (--output-dir renders every template in
# one pass, not just the one path returned), so no second `helm template` call.
default_limitrange="$TMP/default/curie/templates/tenant-limitrange.yaml"
[ -f "$default_limitrange" ] || fail "default install: tenant-limitrange.yaml did not render"
check limitrange-shape "$default_limitrange" \
  defaultRequest.cpu=50m defaultRequest.memory=128Mi defaultRequest.ephemeral-storage=256Mi \
  default.cpu=1 default.memory=1Gi default.ephemeral-storage=2Gi
echo "  ok: default/defaultRequest match values.yaml, no min/max present"

echo "=== Assertion 3: sandboxPriorityClassName is operator-overridable ==="
overridden_quota="$(render_file override-name tenant-resourcequota.yaml --set resourceQuota.sandboxPriorityClassName=custom-sandbox-priority)"
check quota-scope "$overridden_quota" custom-sandbox-priority
echo "  ok: overriding resourceQuota.sandboxPriorityClassName is honored (the #759 coordination escape hatch)"

echo "=== Assertion 4: quota hard ceilings are operator-overridable ==="
overridden_hard="$(render_file override-hard tenant-resourcequota.yaml \
  --set resourceQuota.hard.limitsMemory=32Gi --set resourceQuota.hard.sandboxPodCount=200)"
check quota-hard "$overridden_hard" limits.memory=32Gi pods=200
echo "  ok: resourceQuota.hard.* overrides are honored"

echo "=== Assertion 5: each object's own 'enabled' flag suppresses only itself ==="
out="$TMP/quota-off"
mkdir -p "$out"
helm template curie "$CHART" --output-dir "$out" --set resourceQuota.enabled=false >/dev/null
[ -f "$out/curie/templates/tenant-resourcequota.yaml" ] && fail "resourceQuota.enabled=false: ResourceQuota still rendered"
[ -f "$out/curie/templates/tenant-limitrange.yaml" ] || fail "resourceQuota.enabled=false: LimitRange should still render (independent toggle)"
echo "  ok: resourceQuota.enabled=false suppresses only the ResourceQuota"

out="$TMP/limitrange-off"
mkdir -p "$out"
helm template curie "$CHART" --output-dir "$out" --set limitRange.enabled=false >/dev/null
[ -f "$out/curie/templates/tenant-limitrange.yaml" ] && fail "limitRange.enabled=false: LimitRange still rendered"
[ -f "$out/curie/templates/tenant-resourcequota.yaml" ] || fail "limitRange.enabled=false: ResourceQuota should still render (independent toggle)"
echo "  ok: limitRange.enabled=false suppresses only the LimitRange"

echo "=== Assertion 6: agentSandbox.deploy=false suppresses both ==="
out="$TMP/no-sandbox"
mkdir -p "$out"
helm template curie "$CHART" --output-dir "$out" --set agentSandbox.deploy=false >/dev/null
[ -f "$out/curie/templates/tenant-resourcequota.yaml" ] && fail "agentSandbox.deploy=false: ResourceQuota still rendered"
[ -f "$out/curie/templates/tenant-limitrange.yaml" ] && fail "agentSandbox.deploy=false: LimitRange still rendered"
echo "  ok: agentSandbox.deploy=false renders neither object"

echo "=== Assertion 7: N=1 self-host render leaves the control plane's own cpu/memory untouched ==="
# Published DEFAULT values (no -f overlay): agentSandbox.deploy defaults true
# and every first-party service shares this one release namespace -- the
# actual self-host N=1 topology ADR-0059 describes. values-dev.yaml is a
# different, offline scratch-cluster profile that turns agentSandbox.deploy
# OFF entirely (see its own comments), so it is deliberately NOT used here.
selfhost="$TMP/selfhost"
mkdir -p "$selfhost"
RELEASE_NS="curie-selfhost-assert"
# inference.deploy is opt-in and OFF by default, so the published defaults alone
# render no Ollama Deployment -- which is precisely how #2329 stayed invisible
# here: the one control-plane container that declared no resources at all was
# the one container this render never produced. Turn it on (persistence too,
# which inference.yaml `fail`s without when pullModel is true) so the assertion
# covers it. Any opt-in workload added later belongs here for the same reason.
helm template curie "$CHART" --namespace "$RELEASE_NS" --output-dir "$selfhost" \
  --set dispatcher.slack.appToken=xapp-assert --set dispatcher.slack.botToken=xoxb-assert \
  --set inference.deploy=true --set inference.persistence.enabled=true >/dev/null
[ -f "$selfhost/curie/templates/tenant-resourcequota.yaml" ] || fail "N=1 self-host render: ResourceQuota did not render"
[ -f "$selfhost/curie/templates/tenant-limitrange.yaml" ] || fail "N=1 self-host render: LimitRange did not render"
check controlplane-resources "$selfhost/curie/templates" "$RELEASE_NS"
echo "  ok: every control-plane Deployment/StatefulSet container still declares its own cpu+memory request+limit"

echo
echo "PASS: tenant ResourceQuota scopes to the sandbox PriorityClass with the documented (and overridable) hard ceilings; the LimitRange ships safe default-only per-container defaults; both toggle independently and with agentSandbox.deploy; and the N=1 self-host control plane remains fully self-declared on cpu/memory."
