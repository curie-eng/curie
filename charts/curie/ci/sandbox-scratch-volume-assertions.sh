#!/usr/bin/env bash
#
# Render-assertion test for issue #5158. The factory sandbox workspace and
# the home writable volume can be generic ephemeral volumes. The default
# render stays emptyDir at the profile sizes. /tmp stays emptyDir either
# way. The pod annotation curie.ai/sandbox-scratch-size-keys names the
# values key that won and whether that volume may use the ephemeral type.
#
# The factory fixture enables factory and agent acme-factory, which selects
# the factory profile. writablePaths order in values.yaml is /tmp then
# /home/runner, so writable-0 is /tmp and writable-1 is /home/runner. The
# generic template on the same render stays on the standard profile. The
# plan's default sizes are checked on both templates.
#
# 1. Default render. On the factory template, workspace and writable-1 are
#    emptyDir 15Gi and writable-0 is emptyDir 4Gi. The stamp maps workspace
#    to agentSandbox.resourceProfiles.factory.workspaceSizeLimit (ephemeral
#    true), writable-0 to factory.tmpSizeLimit (ephemeral false), and
#    writable-1 to factory.homeSizeLimit (ephemeral true). The generic
#    template keeps the standard sizes and the standard profile keys.
# 2. The same fixture with factory scratchVolume.type=ephemeral, once with
#    no storageClassName and once with storageClassName=fast. A per-agent
#    workspace of 24Gi, and writablePathSizeLimit=2Gi on every writable path.
# 3. type=bogus fails the render and the failure names that type. An explicit
#    empty type fails the same way and names the empty value. A null extra
#    profile entry still renders today's emptyDir.
# 4. A factory workspace size of 0Gi with type=ephemeral still fails, and
#    the failure names the workspace size key.
# 5. runner-resources-admission.yaml still contains !has(v.hostPath) and
#    does not require has(v.emptyDir). The check reads the template source.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURE="$SCRIPT_DIR/fixtures/factory-resource-profile.yaml"
TPL=templates/agent-sandbox.yaml
POLICY="$CHART/templates/runner-resources-admission.yaml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

[ -f "$FIXTURE" ] || fail "missing fixture $FIXTURE"
[ -f "$POLICY" ] || fail "missing $POLICY"

# Positive renders must succeed. helm template exits non-zero into fail.
render() {
  local out="$1"
  shift
  helm template rel "$CHART" --show-only "$TPL" -f "$FIXTURE" "$@" >"$out"
}

run_check() {
  local mode="$1"
  local yaml="$2"
  local out
  if ! out="$(python3 "$CHECK" "$mode" "$yaml" 2>&1)"; then
    fail "$out"
  fi
  echo "$out"
}

# An expected helm failure must not abort the script. Capture the exit code
# and look for the needle in stdout or stderr.
expect_render_fail() {
  local label="$1"
  local needle="$2"
  shift 2
  local out="$TMP/fail-${label}.out"
  local err="$TMP/fail-${label}.err"
  local code=0
  helm template rel "$CHART" --show-only "$TPL" -f "$FIXTURE" "$@" >"$out" 2>"$err" || code=$?
  if [[ "$code" -eq 0 ]]; then
    fail "${label}: helm template exited 0; expected a failure containing: ${needle}"
  fi
  if ! grep -qF -- "$needle" "$err" && ! grep -qF -- "$needle" "$out"; then
    echo "--- stdout ---" >&2
    cat "$out" >&2
    echo "--- stderr ---" >&2
    cat "$err" >&2
    fail "${label}: helm template exited ${code} but the output does not contain: ${needle}"
  fi
  echo "  ok: ${label} (helm exit ${code})"
}

CHECK="$TMP/check.py"
cat >"$CHECK" <<'PY'
"""Check scratch volume kind, size, storage class, and the size-key stamp.

Usage: check.py <mode> <render.yaml>

The stamp is parsed as JSON and compared as objects, so key order in the
annotation does not matter. SandboxTemplate selection matches the resource
profile assertions: generic is the one name without an -agent- segment, and
acme-factory is the one name matching *-agent-acme-factory-runner.
"""
import fnmatch
import json
import sys

import yaml

ANN = "curie.ai/sandbox-scratch-size-keys"
FACTORY_WS = "agentSandbox.resourceProfiles.factory.workspaceSizeLimit"
FACTORY_TMP = "agentSandbox.resourceProfiles.factory.tmpSizeLimit"
FACTORY_HOME = "agentSandbox.resourceProfiles.factory.homeSizeLimit"
STANDARD_WS = "agentSandbox.resourceProfiles.standard.workspaceSizeLimit"
STANDARD_TMP = "agentSandbox.resourceProfiles.standard.tmpSizeLimit"
STANDARD_HOME = "agentSandbox.resourceProfiles.standard.homeSizeLimit"
WRITABLE_KEY = "agentSandbox.runner.hardening.writablePathSizeLimit"
AGENT_WS = "agentSandbox.workspaceSizeLimits.acme-factory"

MOUNTS = {"writable-0": "/tmp", "writable-1": "/home/runner"}


def stamp(ws_key, tmp_key, home_key):
    return {
        "workspace": {"key": ws_key, "ephemeral": True},
        "writable-0": {"key": tmp_key, "ephemeral": False},
        "writable-1": {"key": home_key, "ephemeral": True},
    }


def empty(*items):
    """items is name, size, name, size, ... for emptyDir volumes."""
    out = {}
    for i in range(0, len(items), 2):
        out[items[i]] = ("emptyDir", items[i + 1])
    return out


def claim(size, storage_class):
    return ("ephemeral", size, storage_class)


# factory: volumes and stamp for the acme-factory template.
# generic: the same for the standard-profile template.
# An ephemeral tuple is (kind, size, storageClassName or None). None means
# the storageClassName field must be absent, not rendered as an empty string.
CASES = {
    "default": {
        "factory": {
            "volumes": empty("workspace", "15Gi", "writable-0", "4Gi", "writable-1", "15Gi"),
            "stamp": stamp(FACTORY_WS, FACTORY_TMP, FACTORY_HOME),
        },
        "generic": {
            "volumes": empty("workspace", "1Gi", "writable-0", "512Mi", "writable-1", "512Mi"),
            "stamp": stamp(STANDARD_WS, STANDARD_TMP, STANDARD_HOME),
        },
    },
    "ephemeral": {
        "factory": {
            "volumes": {
                "workspace": claim("15Gi", None),
                "writable-0": ("emptyDir", "4Gi"),
                "writable-1": claim("15Gi", None),
            },
            "stamp": stamp(FACTORY_WS, FACTORY_TMP, FACTORY_HOME),
        },
        "generic": {
            "volumes": empty("workspace", "1Gi", "writable-0", "512Mi", "writable-1", "512Mi"),
            "stamp": stamp(STANDARD_WS, STANDARD_TMP, STANDARD_HOME),
        },
    },
    "ephemeral-class": {
        "factory": {
            "volumes": {
                "workspace": claim("15Gi", "fast"),
                "writable-0": ("emptyDir", "4Gi"),
                "writable-1": claim("15Gi", "fast"),
            },
            "stamp": stamp(FACTORY_WS, FACTORY_TMP, FACTORY_HOME),
        },
        "generic": {
            "volumes": empty("workspace", "1Gi", "writable-0", "512Mi", "writable-1", "512Mi"),
            "stamp": stamp(STANDARD_WS, STANDARD_TMP, STANDARD_HOME),
        },
    },
    "workspace-24gi": {
        "factory": {
            "volumes": {
                "workspace": claim("24Gi", None),
                "writable-0": ("emptyDir", "4Gi"),
                "writable-1": claim("15Gi", None),
            },
            "stamp": stamp(AGENT_WS, FACTORY_TMP, FACTORY_HOME),
        },
        "generic": {
            "volumes": empty("workspace", "1Gi", "writable-0", "512Mi", "writable-1", "512Mi"),
            "stamp": stamp(STANDARD_WS, STANDARD_TMP, STANDARD_HOME),
        },
    },
    "writable-override": {
        "factory": {
            "volumes": {
                "workspace": claim("15Gi", None),
                "writable-0": ("emptyDir", "2Gi"),
                "writable-1": claim("2Gi", None),
            },
            "stamp": stamp(FACTORY_WS, WRITABLE_KEY, WRITABLE_KEY),
        },
        "generic": {
            "volumes": empty("workspace", "1Gi", "writable-0", "2Gi", "writable-1", "2Gi"),
            "stamp": stamp(STANDARD_WS, WRITABLE_KEY, WRITABLE_KEY),
        },
    },
}


def load(path):
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    templates = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxTemplate"}
    if not templates:
        sys.exit(f"no SandboxTemplate rendered in {path}")
    return templates


def find(templates, key):
    if key == "generic":
        hits = [n for n in templates if "-agent-" not in n]
    else:
        hits = [n for n in templates if fnmatch.fnmatchcase(n, f"*-agent-{key}-runner")]
    if len(hits) != 1:
        sys.exit(f"want one {key} SandboxTemplate, got {sorted(templates)}")
    return templates[hits[0]]


def assert_agent_set(templates):
    agents = sorted(
        n.split("-agent-", 1)[1][: -len("-runner")]
        for n in templates
        if "-agent-" in n
    )
    if agents != ["acme-factory"]:
        sys.exit(f"per-agent templates {agents} != ['acme-factory']")


def pod_spec(tpl):
    return tpl["spec"]["podTemplate"]["spec"]


def volumes(tpl):
    return {v["name"]: v for v in pod_spec(tpl).get("volumes") or []}


def check_mounts(tpl):
    label = tpl["metadata"]["name"]
    spec = pod_spec(tpl)
    runners = [c for c in spec.get("containers") or [] if c.get("name") == "runner"]
    if len(runners) != 1:
        sys.exit(f"{label}: want one runner container, got {len(runners)}")
    mounts = {m.get("name"): m.get("mountPath") for m in runners[0].get("volumeMounts") or []}
    for name, path in MOUNTS.items():
        if mounts.get(name) != path:
            sys.exit(f"{label}: {name} mountPath {mounts.get(name)!r}, want {path!r}")


def check_volume(label, name, vol, expect):
    kind, size = expect[0], expect[1]
    if vol is None:
        sys.exit(f"{label}: missing volume {name}")
    if kind == "emptyDir":
        if "ephemeral" in vol or "emptyDir" not in vol:
            sys.exit(f"{label}: volume {name} keys {sorted(vol)}, want emptyDir sizeLimit {size}")
        got = vol["emptyDir"].get("sizeLimit")
        if str(got) != size:
            sys.exit(f"{label}: volume {name} sizeLimit {got!r}, want {size!r}")
        return
    if kind != "ephemeral":
        sys.exit(f"{label}: bad expected kind {kind}")
    storage_class = expect[2] if len(expect) > 2 else None
    if "emptyDir" in vol or "ephemeral" not in vol:
        sys.exit(f"{label}: volume {name} keys {sorted(vol)}, want a generic ephemeral volume of {size}")
    spec = ((vol.get("ephemeral") or {}).get("volumeClaimTemplate") or {}).get("spec") or {}
    if spec.get("accessModes") != ["ReadWriteOnce"]:
        sys.exit(f"{label}: volume {name} accessModes {spec.get('accessModes')!r}, want ['ReadWriteOnce']")
    got = ((spec.get("resources") or {}).get("requests") or {}).get("storage")
    if str(got) != size:
        sys.exit(f"{label}: volume {name} requests.storage {got!r}, want {size!r}")
    if storage_class is None:
        if "storageClassName" in spec:
            sys.exit(
                f"{label}: volume {name} has storageClassName={spec.get('storageClassName')!r}; "
                "an absent class must omit the field"
            )
    elif spec.get("storageClassName") != storage_class:
        sys.exit(
            f"{label}: volume {name} storageClassName {spec.get('storageClassName')!r}, want {storage_class!r}"
        )


def check_stamp(tpl, want):
    label = tpl["metadata"]["name"]
    meta = tpl["spec"]["podTemplate"].get("metadata") or {}
    raw = (meta.get("annotations") or {}).get(ANN)
    if not isinstance(raw, str) or not raw.strip():
        sys.exit(f"{label}: annotation {ANN} must be a JSON string, got {raw!r}")
    try:
        got = json.loads(raw)
    except json.JSONDecodeError as exc:
        sys.exit(f"{label}: annotation {ANN} is not JSON ({exc})")
    if not isinstance(got, dict):
        sys.exit(f"{label}: annotation {ANN} must be a JSON object, got {type(got).__name__}")
    if set(got) != set(want):
        sys.exit(f"{label}: scratch stamp volumes {sorted(got)} != {sorted(want)}")
    for name, entry in want.items():
        g = got[name]
        if not isinstance(g, dict) or set(g) != {"key", "ephemeral"}:
            sys.exit(f"{label}: stamp {name} is {g!r}, want {entry!r}")
        if g.get("key") != entry["key"] or type(g.get("ephemeral")) is not bool or g.get("ephemeral") is not entry["ephemeral"]:
            sys.exit(f"{label}: stamp {name} is {g!r}, want {entry!r}")


def check(path, mode):
    if mode not in CASES:
        sys.exit(f"unknown mode {mode}")
    templates = load(path)
    assert_agent_set(templates)
    for key, expect in (("acme-factory", CASES[mode]["factory"]), ("generic", CASES[mode]["generic"])):
        tpl = find(templates, key if key == "generic" else "acme-factory")
        label = tpl["metadata"]["name"]
        check_mounts(tpl)
        vols = volumes(tpl)
        for name, vol_expect in expect["volumes"].items():
            check_volume(label, name, vols.get(name), vol_expect)
        check_stamp(tpl, expect["stamp"])
    print(f"  ok: {mode}: acme-factory and generic scratch volumes and stamps")


check(sys.argv[2], sys.argv[1])
PY

echo "=== 1. Default factory fixture: emptyDir sizes and scratch stamp ==="
render "$TMP/default.yaml" || fail "default fixture render"
run_check default "$TMP/default.yaml"

echo "=== 2. scratchVolume.type=ephemeral, no storageClassName ==="
render "$TMP/ephemeral.yaml" \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=ephemeral \
  || fail "ephemeral render"
run_check ephemeral "$TMP/ephemeral.yaml"

echo "=== 2. scratchVolume.type=ephemeral, storageClassName=fast ==="
render "$TMP/ephemeral-class.yaml" \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=ephemeral \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.storageClassName=fast \
  || fail "ephemeral storageClassName render"
run_check ephemeral-class "$TMP/ephemeral-class.yaml"

echo "=== 2. ephemeral plus workspaceSizeLimits.acme-factory=24Gi ==="
render "$TMP/ws24.yaml" \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=ephemeral \
  --set-string agentSandbox.workspaceSizeLimits.acme-factory=24Gi \
  || fail "workspaceSizeLimits render"
run_check workspace-24gi "$TMP/ws24.yaml"

echo "=== 2. ephemeral plus writablePathSizeLimit=2Gi ==="
render "$TMP/writable.yaml" \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=ephemeral \
  --set-string agentSandbox.runner.hardening.writablePathSizeLimit=2Gi \
  || fail "writablePathSizeLimit render"
run_check writable-override "$TMP/writable.yaml"

echo "=== 3. Unknown scratchVolume.type ==="
expect_render_fail unknown-type \
  'agentSandbox.resourceProfiles.factory.scratchVolume.type "bogus" is not emptyDir or ephemeral' \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=bogus

echo "=== 3. Explicit empty scratchVolume.type ==="
cat >"$TMP/empty-type.yaml" <<'YAML'
agentSandbox:
  resourceProfiles:
    factory:
      scratchVolume:
        type: ""
YAML
expect_render_fail empty-type \
  'agentSandbox.resourceProfiles.factory.scratchVolume.type "" is not emptyDir or ephemeral' \
  -f "$TMP/empty-type.yaml"

echo "=== 3. A null profile entry still renders today's emptyDir ==="
cat >"$TMP/null-profile.yaml" <<'YAML'
agentSandbox:
  resourceProfiles:
    unused: null
YAML
render "$TMP/null-profile-out.yaml" -f "$TMP/null-profile.yaml" || fail "null profile entry render"
run_check default "$TMP/null-profile-out.yaml"

echo "=== 4. Zero workspace size while type=ephemeral ==="
expect_render_fail zero-workspace \
  'agentSandbox.resourceProfiles.factory.workspaceSizeLimit' \
  --set agentSandbox.resourceProfiles.factory.scratchVolume.type=ephemeral \
  --set-string agentSandbox.resourceProfiles.factory.workspaceSizeLimit=0Gi

echo "=== 5. runner-resources admission still rejects hostPath only ==="
if ! grep -qF '!has(v.hostPath)' "$POLICY"; then
  fail "runner-resources-admission.yaml does not contain !has(v.hostPath)"
fi
if grep -qF 'has(v.emptyDir)' "$POLICY"; then
  fail "runner-resources-admission.yaml requires has(v.emptyDir)"
fi
echo "  ok: runner-resources admission contains !has(v.hostPath) and does not require has(v.emptyDir)"

echo
echo "PASS: factory scratch volumes stay emptyDir at the profile sizes, ephemeral claims use the resolved size, unknown type, an empty string, and a zero size fail the render, a null profile entry still renders, and runner admission still allows a non-emptyDir volume."
