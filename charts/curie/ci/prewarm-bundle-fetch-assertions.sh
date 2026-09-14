#!/usr/bin/env bash
#
# Render-assertion: every bundle-fetch init image on the SandboxTemplate is a
# container image on the runner-prewarm DaemonSet.
#
# A sandbox pod is cold-created per thread. The runner image is IfNotPresent
# plus this DaemonSet so a ~380MB pull cannot land inside the 90s claim window
# (live incident 2026-07-06). The same window also runs the bundle-fetch and
# bundle-extract init containers, whose default fetchImage is ~418MB. If those
# images are rendered on the sandbox and absent from the prewarm DaemonSet, a
# fresh or image-GCed node cold-pulls them on the first claim -- the same
# hazard, a different image.
#
# The two sides share agentSandbox.runner.bundleFetch.fetchImage and
# extractImage (values, not hardcoded refs). Extra prewarm containers render
# only while agentSandbox.deploy, runner.prewarm.enabled, and
# bundleFetch.enabled are all true.
#
# Proves:
#   (a) Default values: SandboxTemplate init containers bundle-fetch and
#       bundle-extract carry the values.yaml fetchImage and extractImage, and
#       bundle-extract uses the registry-verified exact default digest in both
#       the SandboxTemplate and runner-prewarm DaemonSet.
#   (b) Default values: those same images are container images on the
#       runner-prewarm DaemonSet (one sleep container per image).
#   (c) Cross-object invariant: every bundle-fetch / bundle-extract init image
#       on the SandboxTemplate is in the prewarm DaemonSet container image
#       set. Compares the two rendered objects, not a value against itself.
#   (d) Overriding fetchImage / extractImage propagates to BOTH the sandbox
#       init pair AND the prewarm DaemonSet.
#   (e) bundleFetch.enabled=false omits the init pair from the sandbox AND
#       omits those extra sleep containers from the prewarm DaemonSet.
#   (f) NEGATIVE: a rendered prewarm DaemonSet that drops any sandbox
#       bundle-fetch image fails the same checker the default render uses.
#   (g) NEGATIVE: removing or changing the default extract digest on both
#       rendered consumers fails the exact pin checker.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

SANDBOX_TPL=templates/agent-sandbox.yaml
PREWARM_TPL=templates/runner-prewarm.yaml

fail() { echo "FAIL: $*" >&2; exit 1; }

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import sys
import yaml

BUNDLE_INIT_NAMES = ("bundle-fetch", "bundle-extract")
# Registry verification on 2026 09 14 used docker buildx imagetools inspect.
# Both busybox:1.36.1 and busybox:1.36 resolved to this exact digest.
EXPECTED_DEFAULT_EXTRACT_IMAGE = (
    "busybox:1.36.1@sha256:"
    "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)


def load_docs(path):
    docs = []
    with open(path) as fh:
        for doc in yaml.safe_load_all(fh):
            if doc:
                docs.append(doc)
    return docs


def find_kind(docs, kind):
    for doc in docs:
        if doc.get("kind") == kind:
            return doc
    return None


def sandbox_bundle_images(docs):
    tmpl = find_kind(docs, "SandboxTemplate")
    if tmpl is None:
        raise SystemExit("no SandboxTemplate rendered")
    spec = tmpl["spec"]["podTemplate"]["spec"]
    images = []
    for container in spec.get("initContainers") or []:
        if container.get("name") in BUNDLE_INIT_NAMES:
            image = container.get("image")
            if not image:
                raise SystemExit(
                    f"SandboxTemplate init container {container.get('name')!r} "
                    "has an empty image"
                )
            images.append((container["name"], image))
    return images


def prewarm_containers(docs):
    ds = find_kind(docs, "DaemonSet")
    if ds is None:
        return None
    spec = ds["spec"]["template"]["spec"]
    containers = spec.get("containers") or []
    if not containers:
        raise SystemExit("runner-prewarm DaemonSet has no containers")
    return containers


def missing_from_prewarm(bundle_images, containers):
    present = {c.get("image") for c in containers}
    missing = []
    for name, image in bundle_images:
        if image not in present:
            missing.append((name, image))
    return missing, present


def require_covered(label, bundle_images, containers):
    if containers is None:
        raise SystemExit(
            f"{label}: SandboxTemplate renders bundle-fetch images "
            f"{[i for _, i in bundle_images]} but the runner-prewarm "
            "DaemonSet did not render"
        )
    missing, present = missing_from_prewarm(bundle_images, containers)
    if missing:
        detail = ", ".join(f"{name}={image}" for name, image in missing)
        raise SystemExit(
            f"{label}: bundle-fetch init image(s) rendered on the "
            f"SandboxTemplate but absent from the runner-prewarm "
            f"DaemonSet: {detail}. prewarm images={sorted(present)}"
        )
    print(
        f"  ok: {label}: {len(bundle_images)} sandbox bundle-fetch "
        f"image(s) present on the prewarm DaemonSet"
    )


def main(argv):
    mode = argv[1]
    if mode == "exact-extract-pin":
        sandbox = dict(sandbox_bundle_images(load_docs(argv[2])))
        prewarm = {
            c.get("name"): c.get("image")
            for c in (prewarm_containers(load_docs(argv[3])) or [])
        }
        actual = {
            "SandboxTemplate bundle-extract": sandbox.get("bundle-extract"),
            "DaemonSet prewarm-bundle-extract": prewarm.get("prewarm-bundle-extract"),
        }
        wrong = {
            surface: image
            for surface, image in actual.items()
            if image != EXPECTED_DEFAULT_EXTRACT_IMAGE
        }
        if wrong:
            raise SystemExit(
                f"exact-pin: expected {EXPECTED_DEFAULT_EXTRACT_IMAGE!r}; got {wrong}"
            )
        print("  ok: exact default extract pin is rendered on both consumers")
        return
    if mode == "covered":
        sandbox_docs = load_docs(argv[2])
        prewarm_docs = load_docs(argv[3])
        bundle_images = sandbox_bundle_images(sandbox_docs)
        if not bundle_images:
            raise SystemExit(
                "covered: expected bundle-fetch init containers on the "
                "SandboxTemplate, found none"
            )
        require_covered("default" if len(argv) < 5 else argv[4], bundle_images, prewarm_containers(prewarm_docs))
        return
    if mode == "absent":
        sandbox_docs = load_docs(argv[2])
        prewarm_docs = load_docs(argv[3])
        bundle_images = sandbox_bundle_images(sandbox_docs)
        if bundle_images:
            raise SystemExit(
                "absent: SandboxTemplate still renders bundle-fetch init "
                f"containers {bundle_images} with bundleFetch.enabled=false"
            )
        containers = prewarm_containers(prewarm_docs)
        if containers is None:
            raise SystemExit("absent: runner-prewarm DaemonSet did not render")
        extra = [c["name"] for c in containers if c.get("name") != "prewarm"]
        if extra:
            raise SystemExit(
                "absent: prewarm DaemonSet still has extra containers "
                f"{extra} with bundleFetch.enabled=false"
            )
        print("  ok: bundleFetch.enabled=false omits extra prewarm containers")
        return
    if mode == "mutant-missing":
        sandbox_docs = load_docs(argv[2])
        prewarm_docs = load_docs(argv[3])
        bundle_images = sandbox_bundle_images(sandbox_docs)
        containers = prewarm_containers(prewarm_docs)
        if containers is None:
            raise SystemExit("mutant-missing: prewarm DaemonSet did not render")
        drop = {image for _, image in bundle_images}
        stripped = [c for c in containers if c.get("image") not in drop]
        missing, _present = missing_from_prewarm(bundle_images, stripped)
        if not missing:
            raise SystemExit(
                "mutant-missing: stripping sandbox bundle-fetch images "
                "from the prewarm DaemonSet did not produce a gap; the "
                "checker would not catch a missing prewarm container"
            )
        print(
            "  ok: mutant with sandbox bundle-fetch images dropped from "
            "prewarm is rejected"
        )
        return
    raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main(sys.argv)
PY

echo "=== (a) default sandbox init images match values.yaml ==="
helm template rel "$CHART" --show-only "$SANDBOX_TPL" > "$TMP/default-sandbox.yaml"
helm template rel "$CHART" --show-only "$PREWARM_TPL" > "$TMP/default-prewarm.yaml"

python3 "$CHECKER" exact-extract-pin "$TMP/default-sandbox.yaml" "$TMP/default-prewarm.yaml" \
  || fail "default bundle extract image is not the exact registry-verified digest"

python3 - "$CHART/values.yaml" "$TMP/default-sandbox.yaml" <<'PY' || fail "default sandbox init images did not match values.yaml"
import sys, yaml

values = yaml.safe_load(open(sys.argv[1]))
bf = values["agentSandbox"]["runner"]["bundleFetch"]
expected = {
    "bundle-fetch": bf["fetchImage"],
    "bundle-extract": bf["extractImage"],
}

docs = [d for d in yaml.safe_load_all(open(sys.argv[2])) if d]
tmpl = next(d for d in docs if d.get("kind") == "SandboxTemplate")
inits = {
    c["name"]: c.get("image")
    for c in (tmpl["spec"]["podTemplate"]["spec"].get("initContainers") or [])
    if c.get("name") in expected
}
missing = set(expected) - set(inits)
if missing:
    raise SystemExit(f"sandbox missing init containers {sorted(missing)}")
for name, image in expected.items():
    if inits[name] != image:
        raise SystemExit(
            f"{name}: sandbox image {inits[name]!r} != values {image!r}"
        )
print(f"  ok: bundle-fetch={expected['bundle-fetch']}")
print(f"  ok: bundle-extract={expected['bundle-extract']}")
PY

echo "=== (b/c) default prewarm DaemonSet covers those sandbox images ==="
python3 "$CHECKER" covered "$TMP/default-sandbox.yaml" "$TMP/default-prewarm.yaml" "default" \
  || fail "default render: a bundle-fetch image is on the sandbox and absent from prewarm"

python3 - "$TMP/default-sandbox.yaml" "$TMP/default-prewarm.yaml" <<'PY' || fail "default prewarm extra containers are not sleep-infinity sidecars with the sandbox init pull policy"
import sys, yaml

def docs(path):
    return [d for d in yaml.safe_load_all(open(path)) if d]

sandbox = next(d for d in docs(sys.argv[1]) if d.get("kind") == "SandboxTemplate")
ds = next(d for d in docs(sys.argv[2]) if d.get("kind") == "DaemonSet")
inits = {
    c["name"]: c
    for c in sandbox["spec"]["podTemplate"]["spec"].get("initContainers") or []
}
init_policy = inits["bundle-fetch"].get("imagePullPolicy")
if init_policy != inits["bundle-extract"].get("imagePullPolicy"):
    raise SystemExit("sandbox bundle-fetch and bundle-extract pull policies differ")
containers = ds["spec"]["template"]["spec"]["containers"]
names = [c["name"] for c in containers]
if "prewarm" not in names:
    raise SystemExit(f"runner prewarm container missing; got {names}")
extras = [c for c in containers if c["name"] != "prewarm"]
if len(extras) != 2:
    raise SystemExit(
        f"expected two extra prewarm containers (fetch + extract), got {names}"
    )
expected_names = {"prewarm-bundle-fetch", "prewarm-bundle-extract"}
got_names = {c["name"] for c in extras}
if got_names != expected_names:
    raise SystemExit(f"extra prewarm container names {got_names} != {expected_names}")
for c in extras:
    if c.get("command") != ["sleep", "infinity"]:
        raise SystemExit(
            f"{c['name']}: command {c.get('command')!r} != ['sleep', 'infinity']"
        )
    if c.get("imagePullPolicy") != init_policy:
        raise SystemExit(
            f"{c['name']}: imagePullPolicy {c.get('imagePullPolicy')!r} != "
            f"sandbox init {init_policy!r}"
        )
print(f"  ok: extra sleep containers {sorted(got_names)} policy={init_policy}")
PY

echo "=== (d) fetchImage/extractImage override reaches sandbox AND prewarm ==="
helm template rel "$CHART" --show-only "$SANDBOX_TPL" \
  --set agentSandbox.runner.bundleFetch.fetchImage=example.com/fetch:1.2.3 \
  --set agentSandbox.runner.bundleFetch.extractImage=example.com/extract:4.5.6 \
  > "$TMP/override-sandbox.yaml"
helm template rel "$CHART" --show-only "$PREWARM_TPL" \
  --set agentSandbox.runner.bundleFetch.fetchImage=example.com/fetch:1.2.3 \
  --set agentSandbox.runner.bundleFetch.extractImage=example.com/extract:4.5.6 \
  > "$TMP/override-prewarm.yaml"
python3 "$CHECKER" covered "$TMP/override-sandbox.yaml" "$TMP/override-prewarm.yaml" "override" \
  || fail "override: sandbox bundle-fetch images not covered by prewarm"
python3 - "$TMP/override-sandbox.yaml" "$TMP/override-prewarm.yaml" <<'PY' || fail "override did not propagate both image refs"
import sys, yaml

def docs(path):
    return [d for d in yaml.safe_load_all(open(path)) if d]

sandbox = next(d for d in docs(sys.argv[1]) if d.get("kind") == "SandboxTemplate")
prewarm = next(d for d in docs(sys.argv[2]) if d.get("kind") == "DaemonSet")
inits = {
    c["name"]: c["image"]
    for c in sandbox["spec"]["podTemplate"]["spec"].get("initContainers") or []
}
images = {c["image"] for c in prewarm["spec"]["template"]["spec"]["containers"]}
if inits.get("bundle-fetch") != "example.com/fetch:1.2.3":
    raise SystemExit(f"sandbox fetch {inits.get('bundle-fetch')!r}")
if inits.get("bundle-extract") != "example.com/extract:4.5.6":
    raise SystemExit(f"sandbox extract {inits.get('bundle-extract')!r}")
if "example.com/fetch:1.2.3" not in images:
    raise SystemExit(f"prewarm missing overridden fetchImage; images={sorted(images)}")
if "example.com/extract:4.5.6" not in images:
    raise SystemExit(f"prewarm missing overridden extractImage; images={sorted(images)}")
print("  ok: override reached sandbox init containers and prewarm DaemonSet")
PY

echo "=== (e) bundleFetch.enabled=false omits extra prewarm containers ==="
helm template rel "$CHART" --show-only "$SANDBOX_TPL" \
  --set agentSandbox.runner.bundleFetch.enabled=false \
  > "$TMP/disabled-sandbox.yaml"
helm template rel "$CHART" --show-only "$PREWARM_TPL" \
  --set agentSandbox.runner.bundleFetch.enabled=false \
  > "$TMP/disabled-prewarm.yaml"
python3 "$CHECKER" absent "$TMP/disabled-sandbox.yaml" "$TMP/disabled-prewarm.yaml" \
  || fail "bundleFetch.enabled=false still prewarms or still renders the init pair"

echo "=== (f) NEGATIVE: dropping sandbox bundle-fetch images from prewarm fails ==="
python3 "$CHECKER" mutant-missing "$TMP/default-sandbox.yaml" "$TMP/default-prewarm.yaml" \
  || fail "mutant-missing checker did not reject a prewarm gap"

echo "=== (g) NEGATIVE: missing and wrong default extract digests fail ==="
for mutant in "busybox:1.36.1" "busybox:1.36.1@sha256:0000000000000000000000000000000000000000000000000000000000000000"; do
  sed -E "s|busybox:1\.36\.1@sha256:[a-f0-9]{64}|$mutant|g" "$TMP/default-sandbox.yaml" > "$TMP/mutant-sandbox.yaml"
  sed -E "s|busybox:1\.36\.1@sha256:[a-f0-9]{64}|$mutant|g" "$TMP/default-prewarm.yaml" > "$TMP/mutant-prewarm.yaml"
  if python3 "$CHECKER" exact-extract-pin "$TMP/mutant-sandbox.yaml" "$TMP/mutant-prewarm.yaml" >/dev/null 2>&1; then
    fail "mutant $mutant passed the exact pin checker"
  fi
done
echo "  ok: missing and wrong digest mutants are rejected"

echo "=== (h) prewarm.imagePullPolicy=Never still leaves extras on the sandbox init policy ==="
helm template rel "$CHART" --show-only "$SANDBOX_TPL" \
  --set agentSandbox.runner.prewarm.imagePullPolicy=Never \
  > "$TMP/never-sandbox.yaml"
helm template rel "$CHART" --show-only "$PREWARM_TPL" \
  --set agentSandbox.runner.prewarm.imagePullPolicy=Never \
  > "$TMP/never-prewarm.yaml"
python3 "$CHECKER" covered "$TMP/never-sandbox.yaml" "$TMP/never-prewarm.yaml" "never-prewarm" \
  || fail "Never override: sandbox bundle-fetch images not covered by prewarm"
python3 - "$TMP/never-sandbox.yaml" "$TMP/never-prewarm.yaml" <<'PY' || fail "Never override applied the runner-import policy to bundle-fetch images"
import sys, yaml

def docs(path):
    return [d for d in yaml.safe_load_all(open(path)) if d]

sandbox = next(d for d in docs(sys.argv[1]) if d.get("kind") == "SandboxTemplate")
ds = next(d for d in docs(sys.argv[2]) if d.get("kind") == "DaemonSet")
inits = {
    c["name"]: c
    for c in sandbox["spec"]["podTemplate"]["spec"].get("initContainers") or []
}
init_policy = inits["bundle-fetch"].get("imagePullPolicy")
by_name = {c["name"]: c for c in ds["spec"]["template"]["spec"]["containers"]}
runner = by_name.get("prewarm")
if runner is None:
    raise SystemExit("runner prewarm container missing under Never override")
if runner.get("imagePullPolicy") != "Never":
    raise SystemExit(
        f"runner prewarm imagePullPolicy {runner.get('imagePullPolicy')!r} != Never"
    )
for name in ("prewarm-bundle-fetch", "prewarm-bundle-extract"):
    extra = by_name.get(name)
    if extra is None:
        raise SystemExit(f"{name} missing under Never override")
    if extra.get("imagePullPolicy") != init_policy:
        raise SystemExit(
            f"{name}: imagePullPolicy {extra.get('imagePullPolicy')!r} != "
            f"sandbox init {init_policy!r} (cluster CI sets prewarm Never for "
            "an imported runner; extras must still be pullable)"
        )
    if extra.get("imagePullPolicy") == "Never":
        raise SystemExit(
            f"{name} inherited Never; aws-cli/busybox are not kind-loaded"
        )
print(
    f"  ok: runner policy=Never; extras policy={init_policy} matching sandbox init"
)
PY

echo
echo "PASS: every SandboxTemplate bundle-fetch init image is a container image on the runner-prewarm DaemonSet; an override hits both sides; bundleFetch.enabled=false omits the extra containers; a missing prewarm image is refused; extras keep the sandbox init pull policy when prewarm.imagePullPolicy=Never."
