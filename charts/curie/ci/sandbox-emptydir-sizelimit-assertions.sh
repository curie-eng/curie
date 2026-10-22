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
# (agentSandbox.workspaceSizeLimits, issue #3523) and the sandbox resource
# profiles that give factory agents a complete larger envelope (#4889); see
# those sections below.
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


# Sandbox resource profiles (#4889, ADR-0059). A factory agent builds Rust and
# links large binaries; the generic envelope (768Mi memory, 1Gi workspace,
# 512Mi home and /tmp) OOMKills and evicts it. The chart selects a complete
# `factory` envelope for each factory.sandboxEgress agent while factory.enabled
# is true, keeps every other template on `standard`, and lets an operator move
# any single field.
# Proves, on rendered SandboxTemplates (runner container AND every init
# container resources, plus the workspace emptyDir and each writable-N emptyDir
# keyed by its mount path):
#   (h) the shared factory fixture renders the acme-factory template with the
#       full factory envelope (8Gi/8Gi memory, 50m/2 cpu, 10Gi/20Gi
#       ephemeral-storage, workspace 15Gi, /home/runner 15Gi, /tmp 4Gi) while
#       the generic template on the SAME render keeps the standard envelope,
#       and the worker routes acme-factory to its own pool. Base charts render
#       768Mi and a 1Gi workspace there, so this is the Fix pin assertion.
#   (i) non-factory control: the same sandboxEgress with factory.enabled=false
#       keeps acme-factory on standard; the default render stays standard.
#   (j) an explicit generic resource field applies to every template, and
#       resource precedence is per pair: a generic memory limit alone gives the
#       factory agent the standard memory request plus that limit while cpu and
#       ephemeral-storage keep the factory pair.
#   (k) an explicit generic writablePathSizeLimit sets every writable volume on
#       every template.
#   (l) an explicit generic workspace sizeLimit applies to every template, and
#       workspaceSizeLimits.<agent> still wins for that agent only.
#   (m) a profile field moves only the templates on that profile.
#   (n) agentResourceProfiles opts a factory agent out to standard, and opts a
#       non-factory agent in to factory with its own template and pool, equal
#       to the derived factory template.
#   (o) a repository runner template of the factory agent carries the factory
#       envelope.
#   (p) NOTES prints every envelope that differs from the generic one, one
#       line each, with every effective limit named separately.
#   (q) NEGATIVE: an unknown explicit profile, an invalid agent name, an
#       unresolved profile field and a request above its limit each refuse the
#       render naming the key, each next to a liveness case that renders; and
#       the checker rejects a factory template that lost its workspace.

echo
echo "=== Sandbox resource profiles (#4889) ==="
FIXTURE="$SCRIPT_DIR/fixtures/factory-resource-profile.yaml"
[ -f "$FIXTURE" ] || fail "missing fixture $FIXTURE"

ENVCHECK="$TMP/env-check.py"
cat >"$ENVCHECK" <<'PY'
"""Assert complete sandbox envelopes on rendered SandboxTemplates.

Usage: env-check.py <render.yaml> <spec>...
  <key>=<STANDARD|FACTORY>   the template's expected base envelope. <key> is
                             `generic` (the one template without an -agent-
                             segment) or the text between `-agent-` and
                             `-runner` (fnmatch globs allowed).
  <key>.<resource>=<req>/<lim>  override one pair (cpu, memory,
                             ephemeral-storage).
  <key>.workspace=<size>     override the workspace sizeLimit.
  <key>.<path>=<size>        override the writable volume mounted at <path>.
  <key>.writable=<size>      every writable volume of that template.
  pool:<agent> / nopool:<agent>  CURIE_AGENT_SANDBOX_POOLS membership.
  agents:<a,b>               the exact set of per-agent templates.
  same:<a>,<b>               two templates carry identical resources/volumes.
"""
import fnmatch
import sys

import yaml

ENVELOPES = {
    "STANDARD": {
        "cpu": ("50m", "1"), "memory": ("192Mi", "768Mi"),
        "ephemeral-storage": ("512Mi", "4Gi"),
        "workspace": "1Gi", "home": "512Mi", "tmp": "512Mi", "other": "512Mi",
    },
    "FACTORY": {
        "cpu": ("50m", "2"), "memory": ("8Gi", "8Gi"),
        "ephemeral-storage": ("10Gi", "20Gi"),
        "workspace": "15Gi", "home": "15Gi", "tmp": "4Gi", "other": "512Mi",
    },
}
RESOURCES = ("cpu", "memory", "ephemeral-storage")

path, specs = sys.argv[1], sys.argv[2:]
docs = [d for d in yaml.safe_load_all(open(path)) if d]
templates = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxTemplate"}
errors = []


def find(key):
    if key == "generic":
        hits = [n for n in templates if "-agent-" not in n]
    else:
        hits = [n for n in templates if fnmatch.fnmatchcase(n, f"*-agent-{key}-runner")]
    if len(hits) != 1:
        sys.exit(f"want one {key} SandboxTemplate, got {sorted(templates)}")
    return hits[0]


def pod(name):
    return templates[name]["spec"]["podTemplate"]["spec"]


def norm(value):
    return None if value is None else str(value)


def shape(name):
    spec = pod(name)
    runner = next(c for c in spec["containers"] if c["name"] == "runner")
    vols = {v["name"]: v for v in spec.get("volumes", [])}
    mounts = {m["name"]: m["mountPath"] for m in runner.get("volumeMounts", [])}
    containers = [runner] + list(spec.get("initContainers") or [])
    res = {c["name"]: c.get("resources") or {} for c in containers}
    writable = {}
    for vname, vol in vols.items():
        if vname.startswith("writable-"):
            if vname not in mounts:
                sys.exit(f"{name}: {vname} is not mounted on the runner")
            writable[mounts[vname]] = norm((vol.get("emptyDir") or {}).get("sizeLimit"))
    ws = vols.get("workspace")
    workspace = norm((ws or {}).get("emptyDir", {}).get("sizeLimit")) if ws else None
    return res, workspace, writable


expect = {}
pools_in, pools_out, agents, same = [], [], None, []
for spec in specs:
    if spec.startswith("pool:"):
        pools_in.append(spec[5:])
    elif spec.startswith("nopool:"):
        pools_out.append(spec[7:])
    elif spec.startswith("agents:"):
        agents = sorted(a for a in spec[7:].split(",") if a)
    elif spec.startswith("same:"):
        same.append(spec[5:].split(",", 1))
    else:
        lhs, value = spec.split("=", 1)
        if "." not in lhs:
            expect.setdefault(lhs, {}).update(ENVELOPES[value])
        else:
            key, field = lhs.split(".", 1)
            if key not in expect:
                sys.exit(f"spec {spec!r}: declare {key}=<STANDARD|FACTORY> first")
            if field in RESOURCES:
                expect[key][field] = tuple(value.split("/", 1))
            elif field == "writable":
                expect[key]["_writable"] = value
            elif field.startswith("/"):
                expect[key].setdefault("_paths", {})[field] = value
            else:
                expect[key][field] = value

for key, want in expect.items():
    name = find(key)
    res, workspace, writable = shape(name)
    want_res = {
        "requests": {r: want[r][0] for r in RESOURCES},
        "limits": {r: want[r][1] for r in RESOURCES},
    }
    for cname, got in res.items():
        got = {side: {k: norm(v) for k, v in (got.get(side) or {}).items()} for side in ("requests", "limits")}
        if got != want_res:
            errors.append(f"{key}: container {cname} resources {got}, want {want_res}")
    if workspace != want["workspace"]:
        errors.append(f"{key}: volume workspace sizeLimit {workspace!r}, want {want['workspace']!r}")
    for mount, got in sorted(writable.items()):
        paths = want.get("_paths", {})
        if mount in paths:
            limit = paths[mount]
        elif "_writable" in want:
            limit = want["_writable"]
        elif mount == "/home/runner":
            limit = want["home"]
        elif mount == "/tmp":
            limit = want["tmp"]
        else:
            limit = want["other"]
        if got != limit:
            errors.append(f"{key}: writable volume at {mount} sizeLimit {got!r}, want {limit!r}")
    for mount in want.get("_paths", {}):
        if mount not in writable:
            errors.append(f"{key}: no writable volume mounted at {mount} (got {sorted(writable)})")

if agents is not None:
    got_agents = sorted(n.split("-agent-", 1)[1][: -len("-runner")] for n in templates if "-agent-" in n)
    if got_agents != agents:
        errors.append(f"per-agent templates {got_agents} != expected {agents}")

for a, b in same:
    if shape(find(a)) != shape(find(b)):
        errors.append(f"templates {a} and {b} differ: {shape(find(a))} vs {shape(find(b))}")

if pools_in or pools_out:
    worker = next(
        d for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-worker")
        and "langfuse" not in d["metadata"]["name"]
    )
    env = {e["name"]: e.get("value") for c in worker["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
    pools = [p for p in (env.get("CURIE_AGENT_SANDBOX_POOLS") or "").split(",") if p]
    for agent in pools_in:
        if agent not in pools:
            errors.append(f"CURIE_AGENT_SANDBOX_POOLS {pools} does not route {agent} to its pool")
    for agent in pools_out:
        if agent in pools:
            errors.append(f"CURIE_AGENT_SANDBOX_POOLS {pools} unexpectedly lists {agent}")

if errors:
    sys.exit("\n".join(errors))
print("ok")
PY

envcheck() { # envcheck <label> <render> <spec>...
  local label="$1" out="$2"; shift 2
  python3 "$ENVCHECK" "$out" "$@" >"$TMP/envcheck.out" 2>&1 || { cat "$TMP/envcheck.out" >&2; fail "$label"; }
}

# (h) Fix pin: the derived factory selection.
render -f "$FIXTURE" >"$TMP/fp.yaml" || fail "(h) factory fixture render"
envcheck "(h) factory agent carries the factory envelope; generic stays standard" "$TMP/fp.yaml" \
  generic=STANDARD acme-factory=FACTORY agents:acme-factory pool:acme-factory
echo "  ok: (h) factory agent carries the full factory envelope; generic template stays standard"

# (i) non-factory control.
render -f "$FIXTURE" --set factory.enabled=false >"$TMP/fp-off.yaml" || fail "(i) factory disabled render"
envcheck "(i) factory.enabled=false keeps acme-factory standard" "$TMP/fp-off.yaml" \
  generic=STANDARD acme-factory=STANDARD
envcheck "(i) default render stays standard" "$TMP/ws-default.yaml" generic=STANDARD agents:
echo "  ok: (i) without factory.enabled every template is standard"

# (j) explicit generic resource fields, and per-pair precedence (A1).
render -f "$FIXTURE" --set-string agentSandbox.runner.resources.limits.cpu=3 >"$TMP/fp-cpu.yaml" \
  || fail "(j) generic cpu limit render"
envcheck "(j) generic cpu limit applies to both templates" "$TMP/fp-cpu.yaml" \
  generic=STANDARD generic.cpu=50m/3 acme-factory=FACTORY acme-factory.cpu=50m/3
render -f "$FIXTURE" --set-string agentSandbox.runner.resources.limits.memory=3Gi >"$TMP/fp-mem.yaml" \
  || fail "(j) generic memory limit render (an operator who set only limits.memory must keep rendering)"
envcheck "(j) generic memory limit takes the whole memory pair from the generic side" "$TMP/fp-mem.yaml" \
  generic=STANDARD generic.memory=192Mi/3Gi acme-factory=FACTORY acme-factory.memory=192Mi/3Gi
render -f "$FIXTURE" --set-string agentSandbox.runner.resources.requests.ephemeral-storage=1Gi >"$TMP/fp-eph.yaml" \
  || fail "(j) generic ephemeral-storage request render"
envcheck "(j) generic ephemeral-storage request takes the standard limit" "$TMP/fp-eph.yaml" \
  generic=STANDARD generic.ephemeral-storage=1Gi/4Gi acme-factory=FACTORY acme-factory.ephemeral-storage=1Gi/4Gi
echo "  ok: (j) explicit generic resource fields apply per pair to every template"

# (k) explicit generic writablePathSizeLimit.
render -f "$FIXTURE" --set-string agentSandbox.runner.hardening.writablePathSizeLimit=2Gi >"$TMP/fp-wp.yaml" \
  || fail "(k) generic writablePathSizeLimit render"
envcheck "(k) generic writablePathSizeLimit sets every writable volume" "$TMP/fp-wp.yaml" \
  generic=STANDARD generic.writable=2Gi acme-factory=FACTORY acme-factory.writable=2Gi
render -f "$FIXTURE" \
  --set 'agentSandbox.runner.hardening.writablePaths[0]=/tmp' \
  --set 'agentSandbox.runner.hardening.writablePaths[1]=/home/runner' \
  --set 'agentSandbox.runner.hardening.writablePaths[2]=/var/scratch' >"$TMP/fp-3p.yaml" \
  || fail "(k) third writable path render"
envcheck "(k) a third writable path takes the profile writablePathSizeLimit" "$TMP/fp-3p.yaml" \
  generic=STANDARD generic./var/scratch=512Mi acme-factory=FACTORY acme-factory./var/scratch=512Mi
echo "  ok: (k) writable volume limits follow the profile per path, and one explicit value sets them all"

# (l) explicit generic workspace, then a per-agent workspace on top.
render -f "$FIXTURE" --set-string agentSandbox.runner.workspace.sizeLimit=2Gi >"$TMP/fp-ws.yaml" \
  || fail "(l) generic workspace render"
envcheck "(l) generic workspace sizeLimit applies to every template" "$TMP/fp-ws.yaml" \
  generic=STANDARD generic.workspace=2Gi acme-factory=FACTORY acme-factory.workspace=2Gi
render -f "$FIXTURE" --set-string agentSandbox.runner.workspace.sizeLimit=2Gi \
  --set-string agentSandbox.workspaceSizeLimits.acme-factory=18Gi >"$TMP/fp-ws18.yaml" \
  || fail "(l) per-agent workspace render"
envcheck "(l) workspaceSizeLimits.<agent> wins for that agent only" "$TMP/fp-ws18.yaml" \
  generic=STANDARD generic.workspace=2Gi acme-factory=FACTORY acme-factory.workspace=18Gi
render -f "$FIXTURE" --set-string agentSandbox.workspaceSizeLimits.acme-factory=18Gi >"$TMP/fp-18.yaml" \
  || fail "(l) per-agent workspace over the profile render"
envcheck "(l) workspaceSizeLimits.<agent> beats the factory profile" "$TMP/fp-18.yaml" \
  generic=STANDARD acme-factory=FACTORY acme-factory.workspace=18Gi
echo "  ok: (l) workspace precedence: per-agent > explicit generic > profile"

# (m) profile fields move only their own profile.
render -f "$FIXTURE" --set-string agentSandbox.resourceProfiles.factory.tmpSizeLimit=6Gi >"$TMP/fp-tmp.yaml" \
  || fail "(m) factory tmpSizeLimit render"
envcheck "(m) resourceProfiles.factory.tmpSizeLimit moves the factory /tmp only" "$TMP/fp-tmp.yaml" \
  generic=STANDARD acme-factory=FACTORY acme-factory./tmp=6Gi
render -f "$FIXTURE" --set-string agentSandbox.resourceProfiles.standard.resources.limits.memory=1Gi >"$TMP/fp-std.yaml" \
  || fail "(m) standard memory limit render"
envcheck "(m) resourceProfiles.standard moves the generic only" "$TMP/fp-std.yaml" \
  generic=STANDARD generic.memory=192Mi/1Gi acme-factory=FACTORY
echo "  ok: (m) a profile field moves only the templates on that profile"

# (n) explicit selection: opt-out and opt-in.
render -f "$FIXTURE" --set-string agentSandbox.agentResourceProfiles.acme-factory=standard >"$TMP/fp-out.yaml" \
  || fail "(n) opt-out render"
envcheck "(n) agentResourceProfiles.<factory agent>=standard opts out" "$TMP/fp-out.yaml" \
  generic=STANDARD acme-factory=STANDARD pool:acme-factory
render --set-string agentSandbox.agentResourceProfiles.acme-other=factory >"$TMP/fp-in.yaml" \
  || fail "(n) opt-in render"
envcheck "(n) agentResourceProfiles.<agent>=factory opts a non-factory agent in" "$TMP/fp-in.yaml" \
  generic=STANDARD acme-other=FACTORY agents:acme-other pool:acme-other
python3 - "$TMP/fp.yaml" "$TMP/fp-in.yaml" "$TMP/fp-same.yaml" <<'PY' || fail "(n) could not merge renders"
import sys, yaml
derived = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
optin = [d for d in yaml.safe_load_all(open(sys.argv[2])) if d]
pick = lambda docs, suffix: [d for d in docs if d.get("kind") == "SandboxTemplate" and d["metadata"]["name"].endswith(suffix)]
yaml.safe_dump_all(pick(derived, "-agent-acme-factory-runner") + pick(optin, "-agent-acme-other-runner"),
                   open(sys.argv[3], "w"))
PY
envcheck "(n) the opt-in envelope equals the derived factory envelope" "$TMP/fp-same.yaml" \
  same:acme-factory,acme-other
echo "  ok: (n) agentResourceProfiles opts out to standard and opts in to factory"

# (o) repository runner pools of the factory agent.
python3 - "$FIXTURE" "$TMP/fp-repo-values.yaml" <<'PY' || fail "(o) could not write repo runner values"
import sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
digest = lambda c: "sha256:" + c * 64
doc["factory"]["bindings"] = [{
    "name": "acme-api", "tracker": "github", "trackerScopeId": "812345678",
    "repos": [{
        "alias": "api", "codeHost": "github", "projectId": "812345678",
        "path": "acme-corp/api", "bases": ["main"], "defaultBase": "main",
        "runner": {
            "tools": ["uv"], "image": "registry.example.com/acme/runner@" + digest("a"),
            "base": digest("d"),
            "platformDigests": {"linux/amd64": digest("b"), "linux/arm64": digest("c")},
            "dockerfile": "Dockerfile.runner",
            "probes": [{"name": "check", "command": ["uv", "--version"]}],
        },
    }],
}]
yaml.safe_dump(doc, open(sys.argv[2], "w"), sort_keys=False)
PY
render -f "$TMP/fp-repo-values.yaml" >"$TMP/fp-repo.yaml" || fail "(o) repository runner render"
envcheck "(o) the factory agent's repository runner template carries the factory envelope" "$TMP/fp-repo.yaml" \
  generic=STANDARD acme-factory=FACTORY 'acme-factory-img-*=FACTORY'
echo "  ok: (o) repository runner templates inherit the factory envelope"

# (p) NOTES. helm template does not render NOTES.txt, so a copy of the chart
# renders it through tpl, the way sandbox-capacity-fit-assertions.sh does.
# The envelope block needs no lookup, so it prints under helm template.
NOTES_CHART="$TMP/notes-chart"
cp -a "$CHART" "$NOTES_CHART"
cp "$CHART/templates/NOTES.txt" "$NOTES_CHART/NOTES.txt"
cat >"$NOTES_CHART/templates/notes-check.yaml" <<'EOF2'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF2
NOTESCHECK="$TMP/notes-check.py"
cat >"$NOTESCHECK" <<'PY'
"""notes-check.py <render.yaml> <who>=<envelope>... | absent:<agent>

<who> is `generic` or `agent <name> (profile <p>)`; the line must read exactly
`- <who>: <envelope>` after leading whitespace, under a header line containing
`Sandbox resource envelopes`."""
import sys

import yaml

docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
notes = next(d["data"]["notes"] for d in docs if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "notes-check")
lines = notes.splitlines()
start = next((i for i, line in enumerate(lines) if "Sandbox resource envelopes" in line), None)
if start is None:
    sys.exit("NOTES has no 'Sandbox resource envelopes' block")
block = []
for line in lines[start + 1:]:
    if not line.strip().startswith("- "):
        break
    block.append(line.strip()[2:])
errors = []
for spec in sys.argv[2:]:
    if spec.startswith("absent:"):
        agent = spec[len("absent:"):]
        if any(line.startswith(f"agent {agent} ") for line in block):
            errors.append(f"agent {agent} has the generic envelope but is listed: {block}")
        continue
    who, want = spec.split("=", 1)
    got = [line[len(who) + 2:] for line in block if line.startswith(f"{who}: ")]
    if got != [want]:
        errors.append(f"NOTES line for {who!r}: got {got}, want [{want!r}] (block: {block})")
if errors:
    sys.exit("\n".join(errors))
print("ok")
PY
STD_LINE='memory 192Mi/768Mi, cpu 50m/1, ephemeral-storage 512Mi/4Gi, workspace 1Gi, home 512Mi, tmp 512Mi'
FACTORY_LINE='memory 8Gi/8Gi, cpu 50m/2, ephemeral-storage 10Gi/20Gi, workspace 15Gi, home 15Gi, tmp 4Gi'
helm template t "$NOTES_CHART" --show-only templates/notes-check.yaml -f "$FIXTURE" \
  --set-string agentSandbox.agentResourceProfiles.acme-big=standard \
  --set-string agentSandbox.workspaceSizeLimits.acme-big=18Gi \
  --set-string agentSandbox.connectorSecrets.acme-plain.API_TOKEN=acme-secret \
  >"$TMP/notes.yaml" 2>"$TMP/notes.err" || { cat "$TMP/notes.err" >&2; fail "(p) NOTES render"; }
python3 "$NOTESCHECK" "$TMP/notes.yaml" \
  "generic (profile standard)=$STD_LINE" \
  "agent acme-factory (profile factory)=$FACTORY_LINE" \
  "agent acme-big (profile standard)=${STD_LINE/workspace 1Gi/workspace 18Gi}" \
  absent:acme-plain >"$TMP/notes.out" 2>&1 || { cat "$TMP/notes.out" >&2; fail "(p) NOTES envelope block"; }
echo "  ok: (p) NOTES names every effective envelope that differs from the generic one"

# (q) refusals, each beside a liveness case that renders.
render -f "$FIXTURE" --set-string agentSandbox.agentResourceProfiles.acme-factory=factory >/dev/null \
  || fail "(q) liveness: an explicit known profile must render"
expect_render_fail "agentSandbox.agentResourceProfiles.acme-factory" -f "$FIXTURE" \
  --set-string agentSandbox.agentResourceProfiles.acme-factory=huge
render --set-string agentSandbox.agentResourceProfiles.acme-good=standard >/dev/null \
  || fail "(q) liveness: a valid agent name must render"
expect_render_fail "agentSandbox.agentResourceProfiles.Bad_Name" \
  --set-string agentSandbox.agentResourceProfiles.Bad_Name=factory
# A field no explicit key supplies must resolve from the profile; one an
# explicit key supplies need not (A4).
render -f "$FIXTURE" --set agentSandbox.resourceProfiles.factory.homeSizeLimit=null \
  --set-string agentSandbox.runner.hardening.writablePathSizeLimit=2Gi >/dev/null \
  || fail "(q) liveness: an explicit writablePathSizeLimit covers a missing profile homeSizeLimit"
expect_render_fail "agentSandbox.resourceProfiles.factory.homeSizeLimit" -f "$FIXTURE" \
  --set agentSandbox.resourceProfiles.factory.homeSizeLimit=null
render -f "$FIXTURE" --set-string agentSandbox.resourceProfiles.factory.resources.requests.memory=6Gi >/dev/null \
  || fail "(q) liveness: a profile request below its limit must render"
expect_render_fail "agentSandbox.resourceProfiles.factory.resources.requests.memory" -f "$FIXTURE" \
  --set-string agentSandbox.resourceProfiles.factory.resources.requests.memory=9Gi
render --set-string agentSandbox.runner.resources.requests.memory=1Gi \
  --set-string agentSandbox.runner.resources.limits.memory=1Gi >/dev/null \
  || fail "(q) liveness: an equal generic memory pair must render"
expect_render_fail "agentSandbox.runner.resources.requests.memory" \
  --set-string agentSandbox.runner.resources.requests.memory=2Gi \
  --set-string agentSandbox.runner.resources.limits.memory=1Gi
expect_render_fail "agentSandbox.runner.resources.requests.ephemeral-storage" -f "$FIXTURE" \
  --set-string agentSandbox.runner.resources.requests.ephemeral-storage=5Gi
# The checker must reject a factory template that lost its workspace.
python3 - "$TMP/fp.yaml" "$TMP/fp-mutated.yaml" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
for d in docs:
    if d.get("kind") == "SandboxTemplate" and d["metadata"]["name"].endswith("-agent-acme-factory-runner"):
        for v in d["spec"]["podTemplate"]["spec"]["volumes"]:
            if v["name"] == "workspace":
                v["emptyDir"]["sizeLimit"] = "1Gi"
yaml.safe_dump_all(docs, open(sys.argv[2], "w"))
PY
if python3 "$ENVCHECK" "$TMP/fp-mutated.yaml" generic=STANDARD acme-factory=FACTORY >"$TMP/fp-mut.out" 2>&1; then
  fail "(q) checker accepted a factory template whose workspace fell back to 1Gi"
fi
grep -q "acme-factory: volume workspace sizeLimit '1Gi', want '15Gi'" "$TMP/fp-mut.out" \
  || { cat "$TMP/fp-mut.out" >&2; fail "(q) checker failed for the wrong reason"; }
echo "  ok: (q) unknown profile, bad agent name, unresolved field and request above limit refuse the render"

echo
echo "PASS: every emptyDir volume in the rendered sandbox pod (bundles, aws-config, and one per hardening.writablePaths entry, including a lengthened list) carries an explicit, operator-overridable sizeLimit."
echo "PASS: agentSandbox.workspaceSizeLimits sets one agent's workspace sizeLimit and routes its claims to that pool."
echo "PASS: a factory agent gets the complete factory sandbox envelope (resources on every container, workspace, home, tmp), the generic template stays standard, every explicit override wins per field or pair, and NOTES names every envelope (#4889)."
