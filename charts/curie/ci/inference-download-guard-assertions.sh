#!/usr/bin/env bash
#
# Render-assertion test for issue #1779. Enabling cluster-local inference with
# the shipped pullModel=true and persistence.enabled=false values must fail
# closed: the postStart hook would otherwise download model weights into an
# emptyDir and repeat that implicit download whenever the pod is replaced.
#
# Proves all supported branches of the guard:
#
#   1. inference.deploy=true with the remaining defaults is refused with one
#      exact, actionable message naming both supported recoveries.
#   2. Durable persistence keeps the model pull and mounts the generated PVC.
#   3. A pre-provisioned model (pullModel=false) may use ephemeral storage, but
#      renders neither a postStart hook nor an `ollama pull` command.
#   4. inference.deploy=false remains unaffected by inference value defaults.
#   5. The same container is sized above the namespace LimitRange default and
#      carries a securityContext (#2329). tenant-capacity-assertions.sh proves
#      only that cpu/memory are PRESENT, which a regression setting
#      `limits.memory: 1Gi` would still satisfy while restoring the exact bug:
#      presence is not the defect, the CEILING is. So the numeric relationship
#      to that default is pinned here, where the Deployment is already rendered.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

TPL=templates/inference.yaml
REFUSAL='inference.deploy=true with inference.pullModel=true would implicitly download model weights into an ephemeral emptyDir; enable durable storage with inference.persistence.enabled=true, or pre-provision the model and set inference.pullModel=false.'

fail() { echo "FAIL: $*" >&2; exit 1; }

expect_default_pull_refused() {
  local stdout="$TMP/default-pull.stdout"
  local stderr="$TMP/default-pull.stderr"

  if helm template rel "$CHART" --show-only "$TPL" \
    --set inference.deploy=true > "$stdout" 2> "$stderr"; then
    fail "inference.deploy=true with the shipped ephemeral pull defaults must be refused."
  fi
  if ! grep -Fq -- "$REFUSAL" "$stdout" "$stderr"; then
    fail "unsafe inference defaults failed without the required recovery message. Expected exact message: $REFUSAL"
  fi
  echo "  ok: implicit download to emptyDir was refused with both recoveries"
}

render_inference() {
  local output="$1"
  shift

  if ! helm template rel "$CHART" --show-only "$TPL" "$@" > "$output"; then
    fail "supported inference values failed to render: $*"
  fi
}

assert_inference_shape() {
  local output="$1"
  local expected="$2"

  python3 - "$output" "$expected" <<'PY'
import sys

import yaml


path, expected = sys.argv[1:]
with open(path, encoding="utf-8") as rendered:
    text = rendered.read()
docs = [doc for doc in yaml.safe_load_all(text) if doc]


def one(kind):
    matches = [doc for doc in docs if doc.get("kind") == kind]
    if len(matches) != 1:
        raise SystemExit(
            f"{path}: expected exactly one {kind}, got {len(matches)}"
        )
    return matches[0]


deployment = one("Deployment")
pod_spec = deployment["spec"]["template"]["spec"]
containers = {
    container["name"]: container for container in pod_spec.get("containers") or []
}
if "ollama" not in containers:
    raise SystemExit(f"{path}: inference Deployment has no ollama container")
ollama = containers["ollama"]

volumes = {volume["name"]: volume for volume in pod_spec.get("volumes") or []}
if "ollama-data" not in volumes:
    raise SystemExit(f"{path}: inference Deployment has no ollama-data volume")
data_volume = volumes["ollama-data"]

if expected == "persistent-pull":
    pvc = one("PersistentVolumeClaim")
    claim_name = data_volume.get("persistentVolumeClaim", {}).get("claimName")
    if claim_name != pvc["metadata"]["name"]:
        raise SystemExit(
            f"{path}: ollama-data does not mount the rendered PVC; "
            f"claimName={claim_name!r}, pvc={pvc['metadata']['name']!r}"
        )
    if "emptyDir" in data_volume:
        raise SystemExit(f"{path}: durable inference also rendered an emptyDir")
    post_start = (
        (ollama.get("lifecycle") or {}).get("postStart") or {}
    ).get("exec") or {}
    command = post_start.get("command") or []
    if not any("ollama pull" in str(part) for part in command):
        raise SystemExit(
            f"{path}: durable inference did not retain its postStart ollama pull"
        )
    print("  ok: durable inference renders a PVC mount and postStart model pull")
elif expected == "ephemeral-no-pull":
    pvcs = [doc for doc in docs if doc.get("kind") == "PersistentVolumeClaim"]
    if pvcs:
        raise SystemExit(f"{path}: pullModel=false ephemeral inference rendered a PVC")
    if "emptyDir" not in data_volume:
        raise SystemExit(f"{path}: ephemeral inference did not render an emptyDir")
    if "persistentVolumeClaim" in data_volume:
        raise SystemExit(f"{path}: ephemeral inference also rendered a PVC mount")
    post_start = (ollama.get("lifecycle") or {}).get("postStart")
    if post_start is not None:
        raise SystemExit(
            f"{path}: pullModel=false still rendered a postStart hook: {post_start!r}"
        )
    if "ollama pull" in text:
        raise SystemExit(f"{path}: pullModel=false still rendered an ollama pull")
    print("  ok: no-pull inference renders emptyDir without postStart or ollama pull")
else:
    raise SystemExit(f"unknown expected inference shape: {expected}")
PY
}

assert_inference_disabled() {
  local output="$1"

  python3 - "$output" <<'PY'
import sys

import yaml


path = sys.argv[1]
with open(path, encoding="utf-8") as rendered:
    docs = [doc for doc in yaml.safe_load_all(rendered) if doc]

inference_resources = []
for doc in docs:
    metadata = doc.get("metadata") or {}
    labels = metadata.get("labels") or {}
    if labels.get("app.kubernetes.io/component") == "inference":
        inference_resources.append(f"{doc.get('kind')}/{metadata.get('name')}")

if inference_resources:
    raise SystemExit(
        f"{path}: inference.deploy=false rendered inference resources: "
        f"{inference_resources}"
    )
print("  ok: inference.deploy=false renders no inference resources")
PY
}

echo "=== Assertion 1: unsafe default local-model pull is refused ==="
expect_default_pull_refused

echo "=== Assertion 2: durable persistence permits the explicit model pull ==="
PERSISTENT="$TMP/persistent-pull.yaml"
render_inference "$PERSISTENT" \
  --set inference.deploy=true \
  --set inference.persistence.enabled=true
assert_inference_shape "$PERSISTENT" persistent-pull

echo "=== Assertion 3: pre-provisioned/no-pull permits ephemeral storage ==="
NO_PULL="$TMP/ephemeral-no-pull.yaml"
render_inference "$NO_PULL" \
  --set inference.deploy=true \
  --set inference.pullModel=false
assert_inference_shape "$NO_PULL" ephemeral-no-pull

echo "=== Assertion 4: inference.deploy=false is unaffected ==="
DISABLED="$TMP/disabled.yaml"
if ! helm template rel "$CHART" \
  --set inference.deploy=false \
  --set inference.pullModel=true \
  --set inference.persistence.enabled=false > "$DISABLED"; then
  fail "inference.deploy=false must not activate the download guard."
fi
assert_inference_disabled "$DISABLED"

echo "=== Assertion 5: the inference container is sized above the LimitRange default and hardened (#2329) ==="
# The chart's namespace LimitRange (tenant-limitrange.yaml, on by default) fills
# `default.memory: 1Gi` / `default.cpu: "1"` into any container that declares
# none. Ollama serving the chart's default qwen3:4b needs several times that, so
# a container that merely "declares something" is not enough -- the memory limit
# has to clear the LimitRange default it would otherwise inherit, and the memory
# REQUEST has to cover the resident weights or the scheduler packs the pod onto a
# node that cannot hold them.
LIMITRANGE_DEFAULT_MEMORY_BYTES=$((1024 * 1024 * 1024))  # 1Gi, values.yaml limitRange.container.default.memory
python3 - "$PERSISTENT" "$LIMITRANGE_DEFAULT_MEMORY_BYTES" <<'PYCHK' || fail "inference container capacity/hardening shape (#2329)"
import sys, yaml

path, lr_default = sys.argv[1], int(sys.argv[2])
UNITS = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
         "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}


def to_bytes(v):
    v = str(v)
    for suffix, mult in UNITS.items():
        if v.endswith(suffix):
            return float(v[: -len(suffix)]) * mult
    return float(v)


doc = next(d for d in yaml.safe_load_all(open(path))
           if isinstance(d, dict) and d.get("kind") == "Deployment")
c = next(x for x in doc["spec"]["template"]["spec"]["containers"] if x["name"] == "ollama")
res = c.get("resources") or {}

for section in ("requests", "limits"):
    for dim in ("cpu", "memory"):
        if (res.get(section) or {}).get(dim) is None:
            sys.exit(f"inference container leaves {section}.{dim} undeclared, so the tenant "
                     f"LimitRange default would engage for it (#2329)")

lim = to_bytes(res["limits"]["memory"])
if lim <= lr_default:
    sys.exit(f"inference limits.memory is {res['limits']['memory']}, which does not exceed the "
             f"tenant LimitRange default of 1Gi -- the ceiling is then itself the bug, and the "
             f"model server OOMKills on first inference exactly as in #2329")

req = to_bytes(res["requests"]["memory"])
if req <= lr_default:
    sys.exit(f"inference requests.memory is {res['requests']['memory']}; the scheduler packs on "
             f"the REQUEST, so it must cover the resident model weights (the chart's default "
             f"model is larger than the 1Gi LimitRange default)")
if req > lim:
    sys.exit(f"inference requests.memory ({res['requests']['memory']}) exceeds its own "
             f"limits.memory ({res['limits']['memory']}); the pod is unschedulable")

if not c.get("securityContext"):
    sys.exit("inference container renders no securityContext, so it runs with "
             "allowPrivilegeEscalation and the default capability set and is rejected at "
             "admission on a namespace enforcing Pod Security Standards baseline (#2329)")
sc = c["securityContext"]
if sc.get("allowPrivilegeEscalation") is not False:
    sys.exit(f"inference securityContext must deny privilege escalation; got {sc}")
if (sc.get("seccompProfile") or {}).get("type") != "RuntimeDefault":
    sys.exit(f"inference securityContext must pin the RuntimeDefault seccomp profile; got {sc}")

print(f"  ok: requests.memory={res['requests']['memory']} limits.memory={res['limits']['memory']} "
      f"(both above the 1Gi LimitRange default), securityContext denies escalation "
      f"and pins RuntimeDefault")
PYCHK

echo
echo "PASS: cluster inference refuses implicit emptyDir downloads, accepts both explicit recovery paths, and ships a container sized above the tenant LimitRange default with a hardened securityContext."
