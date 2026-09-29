#!/usr/bin/env bash
#
# Render-assertion: a bundle's layered runner image (agentSandbox.runnerImages,
# ADR-0173, issue #3217) reaches that agent's SandboxTemplate only, and the
# runner prewarm DaemonSet pulls every distinct runner digest the release
# renders, so a sandbox cold boot never pulls an image.
#
# Proves:
#   (a) Default values: no per-agent template, and the prewarm DaemonSet holds
#       only the platform runner container plus the bundle-fetch pair.
#   (b) runnerImages.factory set, connectorSecrets for acme, attachments on
#       so attachments-init renders: the factory
#       template renders the digest in every runner-image container; the
#       generic and acme templates keep the platform image.
#   (c) Cross-object invariant: every runner-image container image across all
#       rendered SandboxTemplates is in the prewarm image set; two agents with
#       the same digest add exactly one prewarm container.
#   (d) The worker's CURIE_AGENT_SANDBOX_POOLS names factory.
#   (e) NEGATIVE: a tag value and an invalid agent name fail render, naming
#       agentSandbox.runnerImages.<agent>.
#   (f) NEGATIVE: the invariant checker fails on a prewarm DaemonSet that drops
#       the digest container.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

DIGEST_A="ghcr.io/acme/factory-runner@sha256:$(printf 'a%.0s' $(seq 64))"
DIGEST_B="ghcr.io/acme/other-runner@sha256:$(printf 'b%.0s' $(seq 64))"


CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import sys
import yaml

mode, path = sys.argv[1], sys.argv[2]
args = sys.argv[3:]
RUNNER = {"runner", "workspace-init", "attachments-init"}

docs = [d for d in yaml.safe_load_all(open(path)) if d]
templates = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxTemplate"}
prewarm = [d for d in docs if d.get("kind") == "DaemonSet" and d["metadata"]["name"].endswith("-runner-prewarm")]
if len(prewarm) != 1:
    sys.exit(f"expected one runner-prewarm DaemonSet, got {len(prewarm)}")
pw_containers = prewarm[0]["spec"]["template"]["spec"]["containers"]
pw_images = {c["image"] for c in pw_containers}
platform = next(c["image"] for c in pw_containers if c["name"] == "prewarm")


def runner_images(tpl):
    spec = tpl["spec"]["podTemplate"]["spec"]
    out = {}
    for c in spec.get("initContainers", []) + spec.get("containers", []):
        if c["name"] in RUNNER:
            out[c["name"]] = c["image"]
    if "runner" not in out:
        sys.exit(f"{tpl['metadata']['name']}: no runner container")
    return out


def by_suffix(suffix):
    hits = [t for n, t in templates.items() if n.endswith(suffix)]
    return hits[0] if hits else None


# Invariant (c): every runner-image container image is prewarmed.
for name, tpl in templates.items():
    for cname, image in runner_images(tpl).items():
        if image not in pw_images:
            sys.exit(f"{name}/{cname} image {image} is not in the prewarm set {sorted(pw_images)}")

if mode == "default":
    agent_tpls = [n for n in templates if "-agent-" in n]
    if agent_tpls:
        sys.exit(f"default render has per-agent templates: {agent_tpls}")
    names = sorted(c["name"] for c in pw_containers)
    if names != sorted(["prewarm", "prewarm-bundle-fetch", "prewarm-bundle-extract"]):
        sys.exit(f"default prewarm containers changed: {names}")
elif mode == "layered":
    digest, expected_extra = args[0], int(args[1])
    factory = by_suffix("-agent-factory-runner")
    acme = by_suffix("-agent-acme-runner")
    generic = [t for n, t in templates.items() if "-agent-" not in n]
    if factory is None or acme is None or len(generic) != 1:
        sys.exit(f"missing templates, got {sorted(templates)}")
    imgs = runner_images(factory)
    if set(imgs) != RUNNER:
        sys.exit(f"factory template runner-image containers {sorted(imgs)} != {sorted(RUNNER)}")
    for cname, image in imgs.items():
        if image != digest:
            sys.exit(f"factory {cname} renders {image}, want {digest}")
    for tpl in (acme, generic[0]):
        for cname, image in runner_images(tpl).items():
            if image != platform:
                sys.exit(f"{tpl['metadata']['name']}/{cname} renders {image}, want platform {platform}")
    extra = [c for c in pw_containers if c["name"].startswith("prewarm-agent-")]
    if len(extra) != expected_extra:
        sys.exit(f"want {expected_extra} layered prewarm containers, got {[c['name'] for c in extra]}")
    base = next(c for c in pw_containers if c["name"] == "prewarm")
    for c in extra:
        if c["imagePullPolicy"] != base["imagePullPolicy"] or c.get("resources") != base.get("resources"):
            sys.exit(f"{c['name']} pull policy or resources differ from the platform prewarm container")
        if c["command"] != ["sleep", "infinity"]:
            sys.exit(f"{c['name']} is not a sleep container")
    worker = next(d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-worker") and "langfuse" not in d["metadata"]["name"])
    env = {e["name"]: e.get("value") for c in worker["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
    pools = (env.get("CURIE_AGENT_SANDBOX_POOLS") or "").split(",")
    if "factory" not in pools:
        sys.exit(f"CURIE_AGENT_SANDBOX_POOLS={env.get('CURIE_AGENT_SANDBOX_POOLS')!r} lacks factory")
print(f"ok: {mode}")
PY

render() { helm template t "$CHART" "$@"; }

# (a)
render >"$TMP/default.yaml" || fail "default render"
python3 "$CHECKER" default "$TMP/default.yaml" || fail "(a) default render"

# (b) (c) (d)
LAYERED=(--set-string "agentSandbox.runnerImages.factory=$DIGEST_A"
         --set-string "agentSandbox.connectorSecrets.acme.API_TOKEN=acme-secret"
         --set worker.attachments.enabled=true)
render "${LAYERED[@]}" >"$TMP/layered.yaml" || fail "layered render"
python3 "$CHECKER" layered "$TMP/layered.yaml" "$DIGEST_A" 1 || fail "(b)(c)(d) layered render"

# (c) two agents sharing a digest add one container; a second digest adds one more.
render "${LAYERED[@]}" --set-string "agentSandbox.runnerImages.zeta=$DIGEST_A" >"$TMP/shared.yaml" || fail "shared render"
python3 "$CHECKER" layered "$TMP/shared.yaml" "$DIGEST_A" 1 || fail "(c) shared digest"
render "${LAYERED[@]}" --set-string "agentSandbox.runnerImages.zeta=$DIGEST_B" >"$TMP/two.yaml" || fail "two digest render"
python3 "$CHECKER" layered "$TMP/two.yaml" "$DIGEST_A" 2 || fail "(c) two digests"

# (e)
expect_render_fail() {
  local want="$1"; shift
  if render "$@" >"$TMP/neg.out" 2>&1; then fail "render succeeded, expected failure naming $want"; fi
  grep -q "$want" "$TMP/neg.out" || { cat "$TMP/neg.out" >&2; fail "failure does not name $want"; }
  grep -q "curie build" "$TMP/neg.out" || [ "$want" = "agentSandbox.runnerImages.Bad_Name" ] || fail "failure does not mention curie build"
}
expect_render_fail "agentSandbox.runnerImages.factory" --set-string "agentSandbox.runnerImages.factory=ghcr.io/acme/factory-runner:1.0"
expect_render_fail "agentSandbox.runnerImages.factory" --set-string "agentSandbox.runnerImages.factory=ghcr.io/acme/factory-runner:1.0@sha256:abc"
expect_render_fail "agentSandbox.runnerImages.Bad_Name" --set-string "agentSandbox.runnerImages.Bad_Name=$DIGEST_A"

# (f) the invariant checker must reject a prewarm that drops the digest container.
python3 - "$TMP/layered.yaml" "$TMP/mutated.yaml" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
for d in docs:
    if d.get("kind") == "DaemonSet" and d["metadata"]["name"].endswith("-runner-prewarm"):
        spec = d["spec"]["template"]["spec"]
        spec["containers"] = [c for c in spec["containers"] if not c["name"].startswith("prewarm-agent-")]
yaml.safe_dump_all(docs, open(sys.argv[2], "w"))
PY
if python3 "$CHECKER" layered "$TMP/mutated.yaml" "$DIGEST_A" 1 >"$TMP/mut.out" 2>&1; then
  fail "(f) checker accepted a prewarm without the layered digest"
fi
grep -q "not in the prewarm set" "$TMP/mut.out" || { cat "$TMP/mut.out" >&2; fail "(f) wrong failure"; }

echo "PASS: runner-images assertions"
