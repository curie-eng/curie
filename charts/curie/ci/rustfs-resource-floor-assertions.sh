#!/usr/bin/env bash
#
# Render-assertion test for issue #2706. Measured rustfs/rustfs:1.0.0-beta.12
# under ordinary Langfuse traffic: 1120Mi working set, 425m CPU. The shipped
# default was limits 500m / 512Mi, so every in-chart rustfs install sat in a
# silent OOMKill loop.
#
# Presence is not the defect, the CEILING is (same lesson as inference
# assertion 5 / #2329, and Tempo #2059). An operator may raise further; these
# pins are FLOORS, not exact values.
#
# OOMKill is kubelet cgroup SIGKILL of a healthy process. HTTP /health returns
# 200 between kills, which is why the pod reports Ready. Tightening probe
# thresholds cannot observe SIGKILL, so probes in templates/rustfs.yaml are
# out of scope. The observable is lastState.terminated.reason=OOMKilled /
# restart count.
#
#   1. DEFAULT render (no overlay): rustfs StatefulSet container `rustfs` has
#      memory limit >= 1680Mi (1120 * 1.5), memory request >= 1120Mi, cpu
#      limit >= 1000m (parse 1 / 1000m / 1.0), cpu limit is NOT 500m and NOT
#      425m, and request memory <= limit memory.
#   2. NEGATIVE: helm template --set rustfs.resources.limits.memory=512Mi
#      (other rustfs.resources kept so helm accepts a complete map) must make
#      assertion 1 fail. Invoke the same checker against that mutant and
#      require non-zero. This is the reproduction of the shipped bug.
#   3. values-dev overlay: memory limit >= 1680Mi and cpu limit >= 1000m.
#      Do NOT require the 1120Mi request floor; the scratch profile keeps
#      128Mi so the 2496 Mi request sum still packs on a 4 GB node.
#   4. rustfs.deploy=false renders no rustfs StatefulSet.
#
# NOTE ON `--output-dir`: do not use `--show-only` (it failed in this
# environment even when the template exists). Render to a directory and read
# the written file; never a stdout pipe (a piped `helm template` has been
# observed to truncate silently while still exiting 0).
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local name="$1"
  shift
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  mkdir -p "$RENDER_DIR"
  helm template rel "$CHART" --output-dir "$RENDER_DIR" "$@" >/dev/null \
    || fail "helm template failed for render '$name'"
}

manifest_for() {
  local manifest
  manifest="$(find "$1" -type f -path "*/templates/rustfs.yaml" -print -quit)"
  [[ -n "$manifest" ]] || fail "RustFS template was not written to $1"
  printf '%s\n' "$manifest"
}

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
"""Floor checker for the rustfs StatefulSet container.

argv[1] = mode (default-floor | overlay-floor | no-sts)
argv[2] = rendered rustfs.yaml, or a helm --output-dir tree for no-sts.

Exits 0 on success; exits 1 with a message on failure. Assertion 2 invokes
default-floor against the 512Mi mutant and requires that non-zero.
"""
import pathlib
import sys

import yaml

MODE = sys.argv[1]
PATH = pathlib.Path(sys.argv[2])

# 1120Mi measured working set; 1680Mi is 1120 * 1.5, the headroom floor
# (values.yaml then ceils that to 2Gi).
FLOOR_MEM_LIMIT = 1680 * 1024 ** 2
FLOOR_MEM_REQUEST = 1120 * 1024 ** 2
FLOOR_CPU_MILLI = 1000
# Shipped default (500m) and measured draw (425m). Do not pin at either.
FORBIDDEN_CPU_MILLI = (425, 500)


def die(message):
    raise SystemExit(message)


def load_docs(path):
    files = sorted(path.rglob("*.yaml")) if path.is_dir() else [path]
    docs = []
    for file_path in files:
        if not file_path.is_file():
            continue
        for doc in yaml.safe_load_all(file_path.read_text()):
            if isinstance(doc, dict):
                docs.append(doc)
    return docs


def to_bytes(value):
    text = str(value).strip()
    for suffix, mult in (
        ("Ki", 1024),
        ("Mi", 1024 ** 2),
        ("Gi", 1024 ** 3),
        ("Ti", 1024 ** 4),
        ("K", 10 ** 3),
        ("M", 10 ** 6),
        ("G", 10 ** 9),
        ("T", 10 ** 12),
    ):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * mult)
    return int(float(text))


def to_milli(value):
    text = str(value).strip()
    if text.endswith("m"):
        return int(round(float(text[:-1])))
    return int(round(float(text) * 1000))


def rustfs_containers(docs):
    found = []
    for doc in docs:
        if doc.get("kind") != "StatefulSet":
            continue
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
        for container in spec.get("containers") or []:
            if container.get("name") == "rustfs":
                found.append(container)
    return found


docs = load_docs(PATH)
containers = rustfs_containers(docs)

if MODE == "no-sts":
    if containers:
        die(
            f"{PATH}: rustfs.deploy=false still rendered a rustfs "
            f"StatefulSet container"
        )
    print("  ok: rustfs.deploy=false renders no rustfs StatefulSet")
    sys.exit(0)

if len(containers) != 1:
    die(
        f"{PATH}: expected exactly one rustfs StatefulSet container, "
        f"got {len(containers)}"
    )

resources = containers[0].get("resources") or {}
requests = resources.get("requests") or {}
limits = resources.get("limits") or {}

if limits.get("memory") is None:
    die(f"{PATH}: rustfs container leaves limits.memory undeclared")
if limits.get("cpu") is None:
    die(f"{PATH}: rustfs container leaves limits.cpu undeclared")

mem_lim = to_bytes(limits["memory"])
cpu_lim = to_milli(limits["cpu"])

if mem_lim < FLOOR_MEM_LIMIT:
    die(
        f"{PATH}: rustfs limits.memory is {limits['memory']}, below the "
        f"#2706 floor of 1680Mi (1120Mi * 1.5). Presence is not the defect, "
        f"the CEILING is."
    )
if cpu_lim < FLOOR_CPU_MILLI:
    die(
        f"{PATH}: rustfs limits.cpu is {limits['cpu']}, below 1000m "
        f"(parse 1 / 1000m / 1.0)"
    )

if MODE == "default-floor":
    if cpu_lim in FORBIDDEN_CPU_MILLI:
        die(
            f"{PATH}: rustfs limits.cpu is {limits['cpu']} ({cpu_lim}m); "
            f"must not be 500m (the shipped default) or 425m (the measured draw)"
        )
    if requests.get("memory") is None:
        die(f"{PATH}: rustfs container leaves requests.memory undeclared")
    mem_req = to_bytes(requests["memory"])
    if mem_req < FLOOR_MEM_REQUEST:
        die(
            f"{PATH}: rustfs requests.memory is {requests['memory']}, below "
            f"the measured 1120Mi working set; the scheduler packs against "
            f"the request"
        )
    if mem_req > mem_lim:
        die(
            f"{PATH}: rustfs requests.memory ({requests['memory']}) exceeds "
            f"limits.memory ({limits['memory']}); the pod is unschedulable"
        )
    print(
        f"  ok: default rustfs requests.memory={requests['memory']} "
        f"limits.memory={limits['memory']} limits.cpu={limits['cpu']} "
        f"(floors 1120Mi / 1680Mi / 1000m)"
    )
elif MODE == "overlay-floor":
    print(
        f"  ok: values-dev rustfs limits.memory={limits['memory']} "
        f"limits.cpu={limits['cpu']} "
        f"(floors 1680Mi / 1000m; request overcommit allowed)"
    )
else:
    die(f"unknown mode {MODE!r}")
PY

echo "=== Assertion 1: default rustfs container clears the #2706 floor ==="
render default
DEFAULT="$(manifest_for "$RENDER_DIR")"
python3 "$CHECKER" default-floor "$DEFAULT" \
  || fail "default rustfs resources are below the #2706 floor"

echo "=== Assertion 2: a 512Mi memory limit fails assertion 1 (the shipped bug) ==="
# Keep the rest of rustfs.resources so helm accepts a complete map; only the
# memory ceiling is the mutant. Presence of cpu/memory is not the defect.
render mutant \
  --set rustfs.resources.requests.cpu=250m \
  --set rustfs.resources.requests.memory=1280Mi \
  --set rustfs.resources.limits.cpu=1 \
  --set rustfs.resources.limits.memory=512Mi
MUTANT="$(manifest_for "$RENDER_DIR")"
if python3 "$CHECKER" default-floor "$MUTANT"; then
  fail "512Mi memory limit still passed the #2706 floor checker; presence is not the defect, the CEILING is"
fi
echo "  ok: 512Mi mutant fails the same checker (reproduces the shipped OOMKill ceiling)"

echo "=== Assertion 3: values-dev overlay keeps the limit floor, not the request floor ==="
render dev -f "$CHART/values-dev.yaml"
DEV="$(manifest_for "$RENDER_DIR")"
python3 "$CHECKER" overlay-floor "$DEV" \
  || fail "values-dev rustfs limits are below the #2706 floor"

echo "=== Assertion 4: rustfs.deploy=false renders no rustfs StatefulSet ==="
cat > "$TMP/byo.yaml" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: 192.0.2.10/32
      ports: [{ protocol: TCP, port: 443 }]
EOF
render off -f "$TMP/byo.yaml"
python3 "$CHECKER" no-sts "$RENDER_DIR" \
  || fail "rustfs.deploy=false still rendered a rustfs StatefulSet"

echo
echo "PASS: rustfs resource floors cover the #2706 OOMKill ceiling (default, 512Mi mutant, values-dev overlay, deploy=false)."
