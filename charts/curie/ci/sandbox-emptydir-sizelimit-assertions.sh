#!/usr/bin/env bash
#
# Render-assertion test for issue #756 (ADR-0059 decision 2). Every writable
# `emptyDir` volume in the sandbox pod -- the shared `bundles` volume, the
# init only `aws-config` volume, and one volume per `hardening.writablePaths`
# entry -- must carry an explicit `sizeLimit` so a pod that overruns is
# evicted on its own account instead of exhausting node disk and taking every
# co-scheduled pod down with it (node-wide `DiskPressure`). This test pins
# that every emptyDir volume rendered in the SandboxTemplate carries a
# non-empty sizeLimit, on both the default `writablePaths` list and a
# lengthened one, so the mechanism is proven generic rather than hardcoded to
# `/tmp` and `/home/runner`.
#
# The same script also pins the per-agent workspace ceiling
# (agentSandbox.workspaceSizeLimits, issue #3523); see that section below.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

TPL=templates/agent-sandbox.yaml

fail() { echo "FAIL: $*" >&2; exit 1; }

DEFAULT="$TMP/default.yaml"
EXTRA_PATH="$TMP/extra-path.yaml"

echo "=== Rendering SandboxTemplate (defaults: bundles, aws-config, /tmp, /home/runner) ==="
helm template rel "$CHART" --show-only "$TPL" > "$DEFAULT"

echo "=== Rendering SandboxTemplate (a third writablePaths entry appended) ==="
helm template rel "$CHART" --show-only "$TPL" \
  --set agentSandbox.runner.hardening.writablePaths[0]=/tmp \
  --set agentSandbox.runner.hardening.writablePaths[1]=/home/runner \
  --set agentSandbox.runner.hardening.writablePaths[2]=/var/scratch \
  --set agentSandbox.runner.hardening.writablePathSizeLimit=256Mi > "$EXTRA_PATH"

ASSERT_PY="$TMP/assert.py"
cat > "$ASSERT_PY" <<'PY'
import sys, yaml


def sandbox_template(path):
    for doc in yaml.safe_load_all(open(path)):
        if doc and doc.get("kind") == "SandboxTemplate":
            return doc
    raise SystemExit(f"no SandboxTemplate rendered in {path}")


def emptydir_volumes(path):
    tmpl = sandbox_template(path)
    spec = tmpl["spec"]["podTemplate"]["spec"]
    return [v for v in (spec.get("volumes") or []) if "emptyDir" in v]


def check(path, expected_names, expected_size_limit=None):
    volumes = emptydir_volumes(path)
    names = {v["name"] for v in volumes}
    missing = set(expected_names) - names
    if missing:
        raise SystemExit(
            f"{path}: expected emptyDir volumes {sorted(missing)} not rendered "
            f"(got {sorted(names)})"
        )
    unset = []
    for v in volumes:
        limit = (v.get("emptyDir") or {}).get("sizeLimit")
        if not limit:
            unset.append(v["name"])
    if unset:
        raise SystemExit(
            f"{path}: emptyDir volume(s) {sorted(unset)} have no sizeLimit set "
            "(ADR-0059 decision 2 requires one on every writable emptyDir)"
        )
    if expected_size_limit is not None:
        wrong = {
            v["name"]: v["emptyDir"]["sizeLimit"]
            for v in volumes
            if v["name"].startswith("writable-")
            and v["emptyDir"]["sizeLimit"] != expected_size_limit
        }
        if wrong:
            raise SystemExit(
                f"{path}: writablePaths volume(s) did not honor the overridden "
                f"writablePathSizeLimit={expected_size_limit!r}: {wrong}"
            )
    print(f"  ok: {sorted(names)} all carry a non-empty sizeLimit")


# Default render: bundles, aws-config, and one writable-N per default
# writablePaths entry (/tmp, /home/runner -> writable-0, writable-1).
check(sys.argv[1], {"bundles", "aws-config", "writable-0", "writable-1"})

# A third writablePaths entry must ALSO get a sizeLimit -- proves the
# mechanism is generic over the list, not hardcoded to two hand-picked paths
# -- and the overridden writablePathSizeLimit must apply to every one of them.
check(
    sys.argv[2],
    {"bundles", "aws-config", "writable-0", "writable-1", "writable-2"},
    expected_size_limit="256Mi",
)
PY

if ! out="$(python3 "$ASSERT_PY" "$DEFAULT" "$EXTRA_PATH" 2>&1)"; then
  fail "$out"
fi
echo "$out"


# Per-agent workspace ceiling (agentSandbox.workspaceSizeLimits, issue #3523).
# A build-heavy agent (the dark factory compiling cli/) outgrew a 10Gi
# workspace emptyDir and its pod was evicted; the chart must let one agent
# carry a larger workspace without enlarging every sandbox.
# Proves:
#   (a) Default values: no per-agent template, and the generic template's
#       workspace emptyDir carries the chart default sizeLimit (1Gi).
#   (b) workspaceSizeLimits.factory=24Gi alone renders a factory template whose
#       workspace sizeLimit is 24Gi; the generic template and a
#       connectorSecrets-only agent (acme) keep the default. Every other
#       emptyDir on the factory template keeps its own limit.
#   (c) The worker's CURIE_AGENT_SANDBOX_POOLS names factory, so its claims go
#       to the larger pool.
#   (d) An agent listed in both runnerImages and workspaceSizeLimits renders
#       exactly one template carrying both the digest and the larger workspace.
#   (e) runner.workspace.sizeLimit still moves the default for every other
#       agent while the listed agent keeps its own value.
#   (f) NEGATIVE: a non-binary or malformed quantity, an invalid agent name,
#       and a limit set while runner.workspace.enabled is false all fail
#       render, naming agentSandbox.workspaceSizeLimits.<agent>.
#   (g) NEGATIVE: the checker rejects a render whose factory template lost the
#       override.

echo
echo "=== Per-agent workspace size limits (#3523) ==="
DIGEST="ghcr.io/acme/factory-runner@sha256:$(printf 'a%.0s' $(seq 64))"

CHECKER="$TMP/ws-check.py"
cat >"$CHECKER" <<'PY'
import sys
import yaml

path = sys.argv[1]
# Each remaining arg is <template-suffix>=<expected workspace sizeLimit>;
# "generic" names the template without an -agent- segment.
expected = dict(a.split("=", 1) for a in sys.argv[2:] if not a.startswith("pools:") and not a.startswith("image:"))
pools = next((a[len("pools:"):] for a in sys.argv[2:] if a.startswith("pools:")), None)
image = next((a[len("image:"):] for a in sys.argv[2:] if a.startswith("image:")), None)

docs = [d for d in yaml.safe_load_all(open(path)) if d]
templates = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxTemplate"}


def volumes(tpl):
    return {v["name"]: v for v in tpl["spec"]["podTemplate"]["spec"].get("volumes", [])}


def find(key):
    if key == "generic":
        hits = [t for n, t in templates.items() if "-agent-" not in n]
    else:
        hits = [t for n, t in templates.items() if n.endswith(f"-agent-{key}-runner")]
    if len(hits) != 1:
        sys.exit(f"want one {key} template, got {sorted(templates)}")
    return hits[0]


agent_tpls = sorted(n for n in templates if "-agent-" in n)
want_agents = sorted(k for k in expected if k != "generic")
got_agents = sorted(n.split("-agent-", 1)[1][: -len("-runner")] for n in agent_tpls)
if got_agents != want_agents:
    sys.exit(f"per-agent templates {got_agents} != expected {want_agents}")

generic_vols = volumes(find("generic"))
for key, limit in expected.items():
    tpl = find(key)
    vols = volumes(tpl)
    ws = vols.get("workspace")
    if ws is None:
        sys.exit(f"{tpl['metadata']['name']}: no workspace volume")
    got = ws["emptyDir"].get("sizeLimit")
    if got != limit:
        sys.exit(f"{tpl['metadata']['name']}: workspace sizeLimit {got!r}, want {limit!r}")
    # Only the workspace moves: every other emptyDir matches the generic one.
    for name, vol in vols.items():
        if name != "workspace" and vol != generic_vols.get(name):
            sys.exit(f"{tpl['metadata']['name']}: volume {name} differs from the generic template")

if image is not None:
    agent, digest = image.split("=", 1)
    runner = next(c for c in find(agent)["spec"]["podTemplate"]["spec"]["containers"] if c["name"] == "runner")
    if runner["image"] != digest:
        sys.exit(f"{agent} runner image {runner['image']}, want {digest}")

if pools is not None:
    worker = next(
        d for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-worker")
        and "langfuse" not in d["metadata"]["name"]
    )
    env = {e["name"]: e.get("value") for c in worker["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
    if env.get("CURIE_AGENT_SANDBOX_POOLS") != pools:
        sys.exit(f"CURIE_AGENT_SANDBOX_POOLS={env.get('CURIE_AGENT_SANDBOX_POOLS')!r}, want {pools!r}")
print("ok")
PY

render() { helm template t "$CHART" "$@"; }

# (a)
render >"$TMP/ws-default.yaml" || fail "default render"
python3 "$CHECKER" "$TMP/ws-default.yaml" generic=1Gi pools: || fail "(a) default render"

# (b) (c)
SIZED=(--set-string "agentSandbox.workspaceSizeLimits.factory=24Gi"
       --set-string "agentSandbox.connectorSecrets.acme.API_TOKEN=acme-secret")
render "${SIZED[@]}" >"$TMP/sized.yaml" || fail "sized render"
python3 "$CHECKER" "$TMP/sized.yaml" generic=1Gi factory=24Gi acme=1Gi pools:acme,factory || fail "(b)(c) sized render"

# (d)
render "${SIZED[@]}" --set-string "agentSandbox.runnerImages.factory=$DIGEST" >"$TMP/both.yaml" || fail "both render"
python3 "$CHECKER" "$TMP/both.yaml" generic=1Gi factory=24Gi acme=1Gi pools:acme,factory "image:factory=$DIGEST" \
  || fail "(d) runnerImages plus workspaceSizeLimits"

# (e)
render "${SIZED[@]}" --set-string agentSandbox.runner.workspace.sizeLimit=2Gi >"$TMP/moved.yaml" || fail "moved render"
python3 "$CHECKER" "$TMP/moved.yaml" generic=2Gi factory=24Gi acme=2Gi pools:acme,factory || fail "(e) moved default"

# (f)
expect_render_fail() {
  local want="$1"; shift
  if render "$@" >"$TMP/ws-neg.out" 2>&1; then fail "render succeeded, expected failure naming $want"; fi
  grep -q "$want" "$TMP/ws-neg.out" || { cat "$TMP/ws-neg.out" >&2; fail "failure does not name $want"; }
}
expect_render_fail "agentSandbox.workspaceSizeLimits.factory" --set-string "agentSandbox.workspaceSizeLimits.factory=24GB"
expect_render_fail "agentSandbox.workspaceSizeLimits.factory" --set-string "agentSandbox.workspaceSizeLimits.factory=0Gi"
expect_render_fail "agentSandbox.workspaceSizeLimits.factory" --set-string "agentSandbox.workspaceSizeLimits.factory=lots"
expect_render_fail "agentSandbox.workspaceSizeLimits.Bad_Name" --set-string "agentSandbox.workspaceSizeLimits.Bad_Name=24Gi"
expect_render_fail "runner.workspace.enabled is false" \
  --set-string "agentSandbox.workspaceSizeLimits.factory=24Gi" --set agentSandbox.runner.workspace.enabled=false

# (g) the checker must reject a factory template that lost the override.
python3 - "$TMP/sized.yaml" "$TMP/ws-mutated.yaml" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
for d in docs:
    if d.get("kind") == "SandboxTemplate" and d["metadata"]["name"].endswith("-agent-factory-runner"):
        for v in d["spec"]["podTemplate"]["spec"]["volumes"]:
            if v["name"] == "workspace":
                v["emptyDir"]["sizeLimit"] = "1Gi"
yaml.safe_dump_all(docs, open(sys.argv[2], "w"))
PY
if python3 "$CHECKER" "$TMP/ws-mutated.yaml" generic=1Gi factory=24Gi acme=1Gi >"$TMP/ws-mut.out" 2>&1; then
  fail "(g) checker accepted a factory template without the override"
fi
grep -q "workspace sizeLimit '1Gi', want '24Gi'" "$TMP/ws-mut.out" || { cat "$TMP/ws-mut.out" >&2; fail "(g) wrong failure"; }

echo
echo "PASS: every emptyDir volume in the rendered sandbox pod (bundles, aws-config, and one per hardening.writablePaths entry, including a lengthened list) carries an explicit, operator-overridable sizeLimit."
echo "PASS: agentSandbox.workspaceSizeLimits sets one agent's workspace sizeLimit and routes its claims to that pool."
