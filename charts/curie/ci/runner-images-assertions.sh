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
#   (g) Repository runner images (ADR 0216) share one template and pool per
#       factory agent and index digest, preserve the agent's sandbox settings,
#       reach admission and prewarm, and appear in the worker's repository map.
#   (h) NEGATIVE: ambiguous factory agents, long pool names, digest-prefix
#       collisions, conflicting image metadata and a disabled substrate fail.
#   (i) Per-agent runnerEnv creates a platform-image pool, reaches only that
#       agent's runner containers, and survives repository-template inheritance.
#   (j) NEGATIVE: malformed runnerEnv maps, agent and variable names, non-string
#       values, reserved names and duplicate runner env names fail render.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

CLUSTER=0
case "${1:-}" in
  "") [ "$#" -eq 0 ] || { echo "Usage: $0 [--cluster]" >&2; exit 2; } ;;
  --cluster) [ "$#" -eq 1 ] || { echo "Usage: $0 [--cluster]" >&2; exit 2; }; CLUSTER=1 ;;
  *) echo "Usage: $0 [--cluster]" >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_SCHEMA="$CHART/../../apps/worker/schema/repo-runner-pools.schema.json"
DAEMON_VECTOR="$CHART/../../tests/vectors/daemon-config.json"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

DIGEST_A="ghcr.io/acme/factory-runner@sha256:$(printf 'a%.0s' $(seq 64))"
DIGEST_B="ghcr.io/acme/other-runner@sha256:$(printf 'b%.0s' $(seq 64))"


CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import copy
import json
import re
import sys
from collections import Counter
import jsonschema
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


def worker_env():
    worker = next(d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-worker") and "langfuse" not in d["metadata"]["name"])
    return {e["name"]: e.get("value") for c in worker["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}


def assert_runner_env(values):
    configured = values["agentSandbox"]["runnerEnv"]
    custom_names = {name for env in configured.values() for name in env}
    global_env = {entry["name"]: entry["value"] for entry in values["agentSandbox"].get("runner", {}).get("extraEnv", [])}
    for name, tpl in templates.items():
        pod = tpl["spec"]["podTemplate"]
        agent = pod["metadata"]["labels"].get("curietech.ai/agent", "")
        expected = configured.get(agent, {})
        for container in pod["spec"].get("initContainers", []) + pod["spec"]["containers"]:
            entries = container.get("env", [])
            names = [entry["name"] for entry in entries]
            if len(names) != len(set(names)):
                sys.exit(f"{name}/{container['name']}: duplicate environment names")
            actual = {entry["name"]: entry.get("value") for entry in entries if entry["name"] in custom_names}
            want = expected if container["name"] == "runner" else {}
            if actual != want:
                sys.exit(f"{name}/{container['name']}: runnerEnv {actual!r}, want {want!r}")
            global_actual = {entry["name"]: entry.get("value") for entry in entries if entry["name"] in global_env}
            if global_actual != (global_env if container["name"] == "runner" else {}):
                sys.exit(f"{name}/{container['name']}: global extraEnv was lost or leaked")


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
    env = worker_env()
    pools = (env.get("CURIE_AGENT_SANDBOX_POOLS") or "").split(",")
    if "factory" not in pools:
        sys.exit(f"CURIE_AGENT_SANDBOX_POOLS={env.get('CURIE_AGENT_SANDBOX_POOLS')!r} lacks factory")
elif mode == "no-repositories":
    if any("-img-" in name for name in templates):
        sys.exit("repositories without runner blocks rendered image-specific templates")
    if "CURIE_REPO_RUNNER_POOLS" in worker_env():
        sys.exit("repositories without runner blocks rendered CURIE_REPO_RUNNER_POOLS")
elif mode == "runner-env":
    values = json.load(open(args[0]))
    configured = values["agentSandbox"]["runnerEnv"]
    assert_runner_env(values)
    pools = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxWarmPool"}
    if len(templates) != len(configured) + 1:
        sys.exit("runnerEnv-only agents must each have one template beside the generic template")
    for agent in configured:
        tpl = by_suffix(f"-agent-{agent}-runner")
        if tpl is None or set(runner_images(tpl).values()) != {platform}:
            sys.exit(f"runnerEnv-only agent {agent} must use the platform image")
        name = tpl["metadata"]["name"]
        if pools.get(name + "-pool", {}).get("spec", {}).get("sandboxTemplateRef", {}).get("name") != name:
            sys.exit(f"runnerEnv-only agent {agent} has no matching warm pool")
    actual_agents = set((worker_env().get("CURIE_AGENT_SANDBOX_POOLS") or "").split(","))
    if actual_agents != set(configured):
        sys.exit(f"worker agent pools {actual_agents} differ from runnerEnv agents {set(configured)}")
    if any(c["name"].startswith("prewarm-agent-") for c in pw_containers):
        sys.exit("runnerEnv-only agents must not add duplicate platform prewarm images")
elif mode == "repositories":
    values = json.load(open(args[0]))
    daemon = json.load(open(args[2]))["daemon"]
    assert_runner_env(values)
    repos = values["factory"]["bindings"][0]["repos"]
    agent = "factory"
    default = by_suffix(f"-agent-{agent}-runner")
    generic = [t for n, t in templates.items() if "-agent-" not in n]
    if default is None or len(generic) != 1:
        sys.exit(f"missing default runner templates: {sorted(templates)}")
    prefix = default["metadata"]["name"].removesuffix(f"-agent-{agent}-runner")
    agent_image = values["agentSandbox"]["runnerImages"][agent]
    if set(runner_images(default)) != RUNNER or set(runner_images(default).values()) != {agent_image}:
        sys.exit("repository image replaced the agent's default runner image")
    if set(runner_images(generic[0]).values()) != {platform}:
        sys.exit("repository image replaced the generic runner image")
    pools = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxWarmPool"}
    default_pool = pools.get(default["metadata"]["name"] + "-pool")
    if default_pool is None:
        sys.exit("agent default runner has no warm pool")
    expected_names = {default["metadata"]["name"], generic[0]["metadata"]["name"]}
    expected_repo_pools = {}
    for repo in repos:
        runner = repo.get("runner")
        if runner is None:
            continue
        index = runner["image"].rsplit("@", 1)[1]
        suffix = index.removeprefix("sha256:")[:12]
        name = f"{prefix}-agent-{agent}-img-{suffix}-runner"
        pool_name = name + "-pool"
        expected_names.add(name)
        tpl, pool = templates.get(name), pools.get(pool_name)
        if tpl is None or pool is None or pool["spec"].get("sandboxTemplateRef", {}).get("name") != name:
            sys.exit(f"missing repository runner template/pool pair {name}/{pool_name}")
        if len(name) > 63 or len(pool_name) > 63:
            sys.exit(f"repository runner resource name exceeds 63 characters: {pool_name}")
        if set(runner_images(tpl)) != RUNNER or set(runner_images(tpl).values()) != {runner["image"]}:
            sys.exit(f"{name}: not every runner-image container uses the repository image")
        expected_pod = copy.deepcopy(default["spec"]["podTemplate"])
        for c in expected_pod["spec"].get("initContainers", []) + expected_pod["spec"].get("containers", []):
            if c["name"] in RUNNER:
                c["image"] = runner["image"]
        if tpl["spec"]["podTemplate"] != expected_pod:
            sys.exit(f"{name}: repository template lost agent labels, secrets, workspace or sandbox settings")
        if tpl["spec"]["podTemplate"]["metadata"]["labels"].get("curietech.ai/agent") != agent:
            sys.exit(f"{name}: repository runner is not labeled for the factory agent")
        expected_pool = copy.deepcopy(default_pool["spec"])
        expected_pool["sandboxTemplateRef"]["name"] = name
        if pool["spec"] != expected_pool:
            sys.exit(f"{pool_name}: repository pool does not inherit warmPool settings")
        docker = repo.get("docker", {})
        enabled = docker.get("enabled", False)
        expected_repo_pools[repo["path"]] = {
            "pool": pool_name,
            "digests": [index, runner["platformDigests"]["linux/amd64"], runner["platformDigests"]["linux/arm64"]],
            "base": runner["base"],
            "docker": enabled,
            "egress": docker.get("egressAllowlist", []) if enabled else [],
        }
        if enabled:
            expected_daemon = copy.deepcopy(daemon)
            expected_daemon["namespace"] = f"default-{prefix}-docker"
            expected_daemon["image"] = values["factory"]["docker"]["daemonImage"]
            expected_repo_pools[repo["path"]]["daemon"] = expected_daemon
    if set(templates) != expected_names:
        sys.exit(f"repository digest templates were not deduplicated: {sorted(templates)}")
    if set(pools) != {name + "-pool" for name in expected_names}:
        sys.exit(f"repository warm pools were not deduplicated: {sorted(pools)}")
    probe = next(d for d in docs if d.get("kind") == "Job" and d["metadata"]["name"].endswith("-security-probe"))
    probe_env = {e["name"]: e.get("value") for c in probe["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
    if Counter((probe_env.get("RUNNER_TEMPLATES") or "").split()) != Counter({name: 1 for name in expected_names}):
        sys.exit("security probe RUNNER_TEMPLATES must include every distinct repository runner template")
    spec = default["spec"]["podTemplate"]["spec"]
    runner = next(c for c in spec["containers"] if c["name"] == "runner")
    secrets = {e["name"]: e.get("valueFrom", {}).get("secretKeyRef") for e in runner.get("env", [])}
    secret_name = prefix + "-agent-factory-connector-secrets"
    if secrets.get("API_TOKEN", {}).get("name") != secret_name:
        sys.exit("factory connector secret is not present in the inherited runner settings")
    secret = next(d for d in docs if d.get("kind") == "Secret" and d["metadata"]["name"] == secret_name)
    if secret.get("stringData", {}).get("API_TOKEN") != "acme-secret":
        sys.exit("factory connector secret value did not reach its chart-owned Secret")
    workspace = next(v for v in spec["volumes"] if v["name"] == "workspace")
    if workspace.get("emptyDir", {}).get("sizeLimit") != "24Gi":
        sys.exit("factory workspace size limit did not reach the inherited runner settings")
    if default_pool["spec"].get("replicas") != 2 or default_pool["spec"].get("updateStrategy", {}).get("type") != "OnReplenish":
        sys.exit("custom warmPool values did not reach repository pools")
    expected_images = {c["image"] for tpl in templates.values() for c in tpl["spec"]["podTemplate"]["spec"].get("initContainers", []) + tpl["spec"]["podTemplate"]["spec"]["containers"]}
    if Counter(c["image"] for c in pw_containers) != Counter({image: 1 for image in expected_images}):
        sys.exit("prewarm must include every distinct rendered image exactly once")
    policy = next(d for d in docs if d.get("kind") == "ValidatingAdmissionPolicy" and d["metadata"]["name"].endswith("-runner-resources"))
    expression = next(v["expression"] for v in policy["spec"]["validations"] if "c.image in" in v["expression"])
    image_lists = re.findall(r"c\.image in (\[[^]]*\])", expression)
    if len(image_lists) != 2:
        sys.exit("runner admission must constrain container and init-container images")
    allowed = {image for images in image_lists for image in re.findall(r"'([^']+)'", images)}
    if allowed != expected_images:
        sys.exit(f"runner admission image set differs from rendered images: {sorted(allowed ^ expected_images)}")
    env = worker_env()
    repo_pools = json.loads(env.get("CURIE_REPO_RUNNER_POOLS") or "{}")
    if repo_pools != expected_repo_pools:
        sys.exit(f"worker repository pool map changed: {env.get('CURIE_REPO_RUNNER_POOLS')}")
    jsonschema.validate(repo_pools, json.load(open(args[1])))
    if agent not in (env.get("CURIE_AGENT_SANDBOX_POOLS") or "").split(","):
        sys.exit("repository pools dropped the factory's default agent pool")
print(f"ok: {mode}")
PY

render() { helm template t "$CHART" "$@"; }

# (a)
render >"$TMP/default.yaml" || fail "default render"
python3 "$CHECKER" default "$TMP/default.yaml" || fail "(a) default render"

# (i) Environment alone selects an agent pool, with string values preserved
# verbatim and no copy on another agent, the generic runner or init containers.
cat >"$TMP/runner-env-values.json" <<'JSON'
{"agentSandbox":{"runner":{"extraEnv":[{"name":"ACME_GLOBAL","value":"global"}]},"runnerEnv":{"factory":{"CLAUDE_CODE_MAX_OUTPUT_TOKENS":"16000","ACME_ZERO":"0","ACME_EMPTY":"","ACME_FALSE":"false","_ACME_TEXT":"first line\nsecond line"},"acme":{"ACME_OTHER":"17"}}},"worker":{"attachments":{"enabled":true}}}
JSON
render -f "$TMP/runner-env-values.json" >"$TMP/runner-env.yaml" || fail "runnerEnv-only render"
python3 "$CHECKER" runner-env "$TMP/runner-env.yaml" "$TMP/runner-env-values.json" || fail "(i) runnerEnv-only pools and isolation"

# (j) Every configured value must be a string, and no name can replace a
# credential, a platform env entry, a connector secret or global runner extraEnv.
expect_runner_env_fail() {
  local want="$1"; shift
  if render "$@" >"$TMP/env-neg.out" 2>&1; then fail "runnerEnv render succeeded, expected refusal naming $want"; fi
  grep -qF "$want" "$TMP/env-neg.out" || { cat "$TMP/env-neg.out" >&2; fail "runnerEnv refusal does not name $want"; }
}
expect_runner_env_fail "agentSandbox.runnerEnv" --set-string 'agentSandbox.runnerEnv=invalid'
for invalid in '"invalid"' '[]' '16000' 'true'; do
  expect_runner_env_fail "agentSandbox.runnerEnv" --set-json "agentSandbox.runnerEnv={\"factory\":$invalid}"
done
for invalid in '[]' '16000' 'true'; do
  expect_runner_env_fail "agentSandbox.runnerEnv" --set-json "agentSandbox.runnerEnv={\"factory\":{\"ACME_VALUE\":$invalid}}"
done
# A string is a valid leaf, unlike a string used as the agent's env map.
render --set-string 'agentSandbox.runnerEnv.factory.ACME_VALUE=invalid' >"$TMP/env-string.yaml" || fail "string runnerEnv value"
expect_runner_env_fail "agentSandbox.runnerEnv" --set-json 'agentSandbox.runnerEnv={"factory":{"ACME_VALUE":{}}}'
for agent in 'Bad_Name' '-factory' 'factory-' "$(printf 'a%.0s' {1..41})"; do
  expect_runner_env_fail "agentSandbox.runnerEnv" --set-json "agentSandbox.runnerEnv={\"$agent\":{\"ACME_VALUE\":\"1\"}}"
done
for name in '1VALUE' 'BAD-NAME' 'BAD.NAME' 'lowercase' 'Mixed_CASE' ''; do
  expect_runner_env_fail "agentSandbox.runnerEnv" --set-json "agentSandbox.runnerEnv={\"factory\":{\"$name\":\"1\"}}"
done
for name in CURIE_FUTURE ANTHROPIC_BASE_URL ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_AUTH_TOKEN HTTPS_PROXY HTTP_PROXY NODE_EXTRA_CA_CERTS ANTHROPIC_CUSTOM_HEADERS OTEL_EXPORTER_OTLP_ENDPOINT OTEL_FUTURE; do
  expect_runner_env_fail "agentSandbox.runnerEnv" --set-string "agentSandbox.runnerEnv.factory.$name=placeholder"
done
for name in HOME GIT_CONFIG_COUNT SNAPSHOT_SEALING_KEY SNAPSHOT_SEALING_KEYS_RETAINED; do
  expect_runner_env_fail "$name" --set-string "agentSandbox.runnerEnv.factory.$name=placeholder"
done
expect_runner_env_fail "API_TOKEN" --set-string 'agentSandbox.runnerEnv.factory.API_TOKEN=placeholder' --set-string 'agentSandbox.connectorSecrets.factory.API_TOKEN=placeholder'
expect_runner_env_fail "ACME_MODE" --set-string 'agentSandbox.runnerEnv.factory.ACME_MODE=agent' --set-json 'agentSandbox.runner.extraEnv=[{"name":"ACME_MODE","value":"global"}]'

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
expect_render_fail "agentSandbox.runnerImages.factory" --set-string "agentSandbox.runnerImages.factory=null"
expect_render_fail "agentSandbox.runnerImages.Bad_Name" --set-string "agentSandbox.runnerImages.Bad_Name=$DIGEST_A"

# A retained values file that simply omits a stale agent does not render that
# digest. The sibling layer and the connector secret stay. A null map entry is
# not this case: the string "null" above is still refused.
cat >"$TMP/layered-values.json" <<EOF
{"agentSandbox":{"runnerImages":{"factory":"$DIGEST_A","acme-other":"$DIGEST_B"},"connectorSecrets":{"factory":{"API_TOKEN":"acme-secret"}}}}
EOF
render -f "$TMP/layered-values.json" >"$TMP/both-values.yaml" || fail "values render"
grep -q "$DIGEST_A" "$TMP/both-values.yaml" || fail "factory digest missing from values render"
grep -q "$DIGEST_B" "$TMP/both-values.yaml" || fail "sibling digest missing from values render"
python3 - <<PY
import json
path = "$TMP/layered-values.json"
doc = json.load(open(path))
del doc["agentSandbox"]["runnerImages"]["factory"]
json.dump(doc, open(path, "w"))
PY
render -f "$TMP/layered-values.json" >"$TMP/omitted.yaml" || fail "omitted key render"
if grep -q "$DIGEST_A" "$TMP/omitted.yaml"; then fail "omitted factory digest still rendered"; fi
grep -q "$DIGEST_B" "$TMP/omitted.yaml" || fail "sibling digest dropped when factory was omitted"
grep -q "acme-secret" "$TMP/omitted.yaml" || fail "connector secret dropped when factory was omitted"

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

# (g) Absent and empty repository runner configuration preserve the exact
# rendered text of the affected workloads, including their Secret checksums.
# Use reproducible dev credentials: sealed fresh installs deliberately mint
# distinct Secret content, which now participates in the worker checksum.
# Keep the full comparison rather than stripping annotations or credentials.
UNCHANGED=(--set security.allowDevDefaults=true
           --show-only templates/agent-sandbox.yaml
           --show-only templates/worker.yaml
           --show-only templates/runner-prewarm.yaml
           --show-only templates/runner-resources-admission.yaml
           --show-only templates/security-probe.yaml)
render "${UNCHANGED[@]}" >"$TMP/no-repo-default.yaml" || fail "no repository baseline"
render "${UNCHANGED[@]}" --set-json 'agentSandbox.runnerEnv={}' >"$TMP/no-env-empty.yaml" || fail "empty runnerEnv configuration"
cmp -s "$TMP/no-repo-default.yaml" "$TMP/no-env-empty.yaml" || fail "empty runnerEnv changed existing workloads"
render "${UNCHANGED[@]}" --set-json 'factory.bindings=[]' --set-json 'factory.sandboxEgress={}' >"$TMP/no-repo-empty.yaml" || fail "empty repository configuration"
cmp -s "$TMP/no-repo-default.yaml" "$TMP/no-repo-empty.yaml" || fail "empty repository configuration changed existing workloads"
render "${UNCHANGED[@]}" --set-json 'factory=null' >"$TMP/no-repo-absent.yaml" || fail "absent factory configuration"
cmp -s "$TMP/no-repo-default.yaml" "$TMP/no-repo-absent.yaml" || fail "absent factory configuration changed existing workloads"

python3 - "$TMP/repository-values.json" "$TMP/no-runner-values.json" <<'PY'
import copy, json, sys

digest = lambda c: "sha256:" + c * 64

def runner(index, amd64, arm64, tools):
    return {
        "tools": tools,
        "image": "registry.example.com/acme/repository-runner@" + digest(index),
        "base": digest("d"),
        "platformDigests": {"linux/amd64": digest(amd64), "linux/arm64": digest(arm64)},
    }

shared = runner("a", "e", "f", ["uv", "docker-cli"])
values = {
    "agentSandbox": {
        "runner": {"digest": digest("d")},
        "runnerImages": {"factory": "registry.example.com/acme/agent-runner@" + digest("c")},
        "runnerEnv": {"factory": {"CLAUDE_CODE_MAX_OUTPUT_TOKENS": "16000", "ACME_ZERO": "0", "ACME_EMPTY": ""}},
        "connectorSecrets": {"factory": {"API_TOKEN": "acme-secret"}},
        "workspaceSizeLimits": {"factory": "24Gi"},
        "warmPool": {"replicas": 2, "updateStrategy": "OnReplenish"},
    },
    "worker": {"attachments": {"enabled": True}},
    "factory": {
        "docker": {"daemonImage": "registry.example.com/acme/curie-docker-daemon@" + digest("7")},
        "sandboxEgress": {"factory": True, "unused": False},
        "bindings": [{"name": "acme", "repos": [
            {"path": "acme-corp/api", "runner": copy.deepcopy(shared), "docker": {
                "enabled": True,
                "egressAllowlist": [{"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]}],
            }},
            {"path": "acme-corp/ui", "runner": copy.deepcopy(shared)},
            {"path": "acme-corp/tools", "runner": runner("b", "1", "2", ["pnpm"]), "docker": {
                "enabled": False,
                "egressAllowlist": [{"cidr": "198.51.100.0/24", "ports": [{"protocol": "TCP", "port": 443}]}],
            }},
            {"path": "acme-corp/default"},
        ]}],
    },
}
json.dump(values, open(sys.argv[1], "w"))
without_runners = copy.deepcopy(values)
for repo in without_runners["factory"]["bindings"][0]["repos"]:
    repo.pop("runner", None)
    repo.pop("docker", None)
json.dump(without_runners, open(sys.argv[2], "w"))
PY
render -f "$TMP/no-runner-values.json" >"$TMP/no-runners.yaml" || fail "repositories without runner blocks"
python3 "$CHECKER" no-repositories "$TMP/no-runners.yaml" || fail "no repository runner output"
render -f "$TMP/repository-values.json" >"$TMP/repositories.yaml" || fail "repository runners render"
python3 "$CHECKER" repositories "$TMP/repositories.yaml" "$TMP/repository-values.json" "$REPO_SCHEMA" "$DAEMON_VECTOR" || fail "(g) repository runner render"

# (h) A runner declaration cannot guess which agent to use or silently alias a
# distinct digest or image metadata onto an existing pool.
expect_repository_fail() {
  local want="$1"; shift
  if render -f "$TMP/repository-values.json" "$@" >"$TMP/repo-neg.out" 2>&1; then
    fail "repository render succeeded, expected failure naming $want"
  fi
  grep -qF "$want" "$TMP/repo-neg.out" || { cat "$TMP/repo-neg.out" >&2; fail "repository refusal does not name $want"; }
}
expect_repository_fail "factory.sandboxEgress" --set-json 'factory.sandboxEgress={"factory":false}'
expect_repository_fail "factory.sandboxEgress" --set-json 'factory.sandboxEgress={"factory":true,"other":true}'
expect_repository_fail "agentSandbox.deploy" --set agentSandbox.deploy=false
expect_repository_fail "worker.warmPool" --set-string 'worker.warmPool=acme-custom-pool'
# The template fits at 60 characters here; its pool reaches 65, so checking
# only the template would accept an invalid Kubernetes resource name.
expect_repository_fail "63" --set-string 'fullnameOverride=acme-installation-long'
render -f "$TMP/repository-values.json" --set-string 'fullnameOverride=acme-installation-ok' >"$TMP/repo-name-boundary.yaml" || fail "63-character repository pool name"
python3 "$CHECKER" repositories "$TMP/repo-name-boundary.yaml" "$TMP/repository-values.json" "$REPO_SCHEMA" "$DAEMON_VECTOR" || fail "valid repository name boundary"
COLLISION="sha256:$(printf 'a%.0s' {1..12})$(printf 'b%.0s' {1..52})"
expect_repository_fail "collision" --set-string "factory.bindings[0].repos[2].runner.image=registry.example.com/acme/repository-runner@$COLLISION"
expect_repository_fail "runner" --set-string "factory.bindings[0].repos[1].runner.base=sha256:$(printf 'e%.0s' {1..64})"
expect_repository_fail "runner" --set-json "factory.bindings[0].repos[1].runner.platformDigests={\"linux/amd64\":\"sha256:$(printf '1%.0s' {1..64})\",\"linux/arm64\":\"sha256:$(printf 'f%.0s' {1..64})\"}"
expect_repository_fail "acme-corp/api" --set-string 'factory.bindings[0].repos[2].path=acme-corp/api'
expect_repository_fail "collision" --set-string 'agentSandbox.poolAgents[0]=factory-img-aaaaaaaaaaaa'
python3 - "$TMP/repository-values.json" "$TMP/mixed-runner-first.json" "$TMP/mixed-no-runner-first.json" <<'PY'
import copy, json, sys
values = json.load(open(sys.argv[1]))
repos = values["factory"]["bindings"][0]["repos"]
repos[-1]["path"] = repos[0]["path"]
json.dump(values, open(sys.argv[2], "w"))
reversed_values = copy.deepcopy(values)
reversed_values["factory"]["bindings"][0]["repos"].reverse()
json.dump(reversed_values, open(sys.argv[3], "w"))
PY
for mixed in "$TMP/mixed-runner-first.json" "$TMP/mixed-no-runner-first.json"; do
  expect_repository_fail "acme-corp/api" -f "$mixed"
done

# The same factory agent declarations remain valid without repository runners.
render -f "$TMP/no-runner-values.json" --set-json 'factory.sandboxEgress={"factory":true,"other":true}' >"$TMP/normal-multiple.yaml" || fail "normal multi-agent configuration"
python3 "$CHECKER" no-repositories "$TMP/normal-multiple.yaml" || fail "normal multi-agent render"

if (( CLUSTER )); then
  [ -n "${CURIE_BIN:-}" ] && [[ "$CURIE_BIN" = /* ]] && [ -x "$CURIE_BIN" ] || fail "--cluster requires CURIE_BIN naming the absolute changed CLI executable"
  REAL_KUBECTL=$(command -v kubectl) || fail "kubectl is required for --cluster"
  REAL_HELM=$(command -v helm) || fail "helm is required for --cluster"
  [[ "$REAL_KUBECTL" = /* && "$REAL_HELM" = /* ]] || fail "cluster proof requires absolute helm and kubectl executables"
  NS=test-factory-tools-chart-repo-templates
  RELEASE=ft-tools
  PREFIX=ft-tools-curie
  NAMESPACE_OWNED=0
  INSTALL_STARTED=0
  CLUSTER_OBJECTS="$TMP/cluster-objects"
  : >"$CLUSTER_OBJECTS"

  cleanup_cluster() {
    local result=$? cleanup_failed=0 kind name object
    trap - EXIT INT TERM
    if (( NAMESPACE_OWNED )); then
      if (( INSTALL_STARTED )); then
        helm --kube-context k8 uninstall "$RELEASE" --namespace "$NS" --no-hooks --ignore-not-found >"$TMP/uninstall.out" 2>&1 || cleanup_failed=1
        while read -r kind name; do
          [ -n "$kind" ] || continue
          object=$(kubectl --context k8 get "$kind" "$name" --ignore-not-found -o json) || { cleanup_failed=1; continue; }
          [ -n "$object" ] || continue
          if ! python3 -c 'import json,sys; d=json.load(sys.stdin)["metadata"]; assert d.get("labels",{}).get("app.kubernetes.io/instance")==sys.argv[1]; assert d.get("annotations",{}).get("meta.helm.sh/release-name")==sys.argv[1]; assert d.get("annotations",{}).get("meta.helm.sh/release-namespace")==sys.argv[2]' "$RELEASE" "$NS" <<<"$object"; then
            echo "FAIL: cleanup refused an unowned $kind/$name" >&2
            cleanup_failed=1
            continue
          fi
          kubectl --context k8 delete "$kind" "$name" --wait=true --timeout=60s >/dev/null || cleanup_failed=1
        done <"$CLUSTER_OBJECTS"
      fi
      object=$(kubectl --context k8 get namespace "$NS" --ignore-not-found -o json) || cleanup_failed=1
      if [ -n "$object" ]; then
        if python3 -c 'import json,sys; d=json.load(sys.stdin)["metadata"]; assert d.get("labels",{}).get("owner")=="factory-tools"; assert d.get("labels",{}).get("app.kubernetes.io/instance")=="ft-tools"' <<<"$object"; then
          kubectl --context k8 delete namespace "$NS" --wait=true --timeout=60s >/dev/null || cleanup_failed=1
        else
          echo "FAIL: cleanup refused an unowned namespace" >&2
          cleanup_failed=1
        fi
      fi
      object=$(kubectl --context k8 get namespace "$NS" --ignore-not-found -o name) || cleanup_failed=1
      [ -z "$object" ] || cleanup_failed=1
      if (( INSTALL_STARTED )); then
        while read -r kind name; do
          [ -n "$kind" ] || continue
          object=$(kubectl --context k8 get "$kind" "$name" --ignore-not-found -o name) || { cleanup_failed=1; continue; }
          [ -z "$object" ] || cleanup_failed=1
        done <"$CLUSTER_OBJECTS"
      fi
    fi
    rm -rf "$TMP"
    if (( cleanup_failed )); then
      echo "FAIL: owned cluster test resources were not fully removed" >&2
      exit 1
    fi
    if (( result == 0 )); then
      echo "PASS: installed repository runner images, admission and cleanup"
    fi
    exit "$result"
  }
  trap cleanup_cluster EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  existing=$(kubectl --context k8 get namespace "$NS" --ignore-not-found -o name) || fail "cluster namespace precondition"
  [ -z "$existing" ] || fail "cluster test namespace already exists"
  kubectl --context k8 create -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
  labels:
    owner: factory-tools
    app.kubernetes.io/instance: $RELEASE
EOF
  NAMESPACE_OWNED=1
  CLUSTER_VALUES=(-f "$CHART/values-external.yaml" -f "$TMP/repository-values.json"
                  --set agentSandbox.controller.deploy=false
                  --set priorityClasses.platform.create=false --set-string priorityClasses.platform.name=
                  --set priorityClasses.sandbox.create=false --set-string priorityClasses.sandbox.name=
                  --set resourceQuota.enabled=false
                  --set api.deploy=false --set dispatcher.deploy=false --set ui.deploy=false
                  --set mailAdapter.deploy=false --set inference.deploy=false
                  --set langfuse.deploy=false --set-string langfuse.host=langfuse.example.com
                  --set otelCollector.deploy=false --set otelCollector.telemetryDisabled=true
                  --set worker.deploy=true --set worker.replicas=0 --set worker.publication.enabled=false
                  --set agentSandbox.warmPool.replicas=0 --set agentSandbox.runner.prewarm.enabled=false
                  --set security.gvisor.mode=off --set agentSandbox.runner.fakeModel=true)
  helm template "$RELEASE" "$CHART" --namespace "$NS" "${CLUSTER_VALUES[@]}" >"$TMP/cluster-render.yaml" || fail "cluster fixture render"
  python3 - "$TMP/cluster-render.yaml" "$CLUSTER_OBJECTS" <<'PY'
import sys, yaml
cluster_kinds = {"ClusterRole", "ClusterRoleBinding", "PriorityClass", "RuntimeClass", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"}
with open(sys.argv[2], "w") as out:
    for doc in yaml.safe_load_all(open(sys.argv[1])):
        if doc and doc.get("kind") in cluster_kinds and "helm.sh/hook" not in doc.get("metadata", {}).get("annotations", {}):
            out.write(f"{doc['kind']} {doc['metadata']['name']}\n")
PY
  while read -r kind name; do
    existing=$(kubectl --context k8 get "$kind" "$name" --ignore-not-found -o name) || fail "cluster object precondition"
    [ -z "$existing" ] || fail "cluster resource $kind/$name already exists"
  done <"$CLUSTER_OBJECTS"
  INSTALL_STARTED=1
  helm --kube-context k8 install "$RELEASE" "$CHART" --namespace "$NS" --skip-crds --no-hooks "${CLUSTER_VALUES[@]}" >"$TMP/install.out" 2>&1 || { cat "$TMP/install.out" >&2; fail "cluster fixture install"; }
  helm --kube-context k8 get values "$RELEASE" --namespace "$NS" -o json >"$TMP/installed-values.json" || fail "installed values readback"
  kubectl --context k8 --namespace "$NS" get sandboxtemplates.extensions.agents.x-k8s.io,sandboxwarmpools.extensions.agents.x-k8s.io,deployment -o json >"$TMP/installed.json" || fail "installed object readback"
  python3 - "$TMP/installed.json" "$TMP/repository-values.json" "$REPO_SCHEMA" "$TMP" "$NS" "$RELEASE" "$DAEMON_VECTOR" <<'PY'
import copy, json, pathlib, sys
import jsonschema
docs = json.load(open(sys.argv[1]))["items"]
values = json.load(open(sys.argv[2]))
prefix = "ft-tools-curie"
templates = {d["metadata"]["name"]: d for d in docs if d["kind"] == "SandboxTemplate"}
pools = {d["metadata"]["name"]: d for d in docs if d["kind"] == "SandboxWarmPool"}
names = {prefix + "-runner", prefix + "-agent-factory-runner"}
expected = {}
daemon = json.load(open(sys.argv[7]))["daemon"]
for repo in values["factory"]["bindings"][0]["repos"]:
    runner = repo.get("runner")
    if not runner:
        continue
    index = runner["image"].rsplit("@", 1)[1]
    name = f"{prefix}-agent-factory-img-{index.removeprefix('sha256:')[:12]}-runner"
    names.add(name)
    docker = repo.get("docker", {})
    enabled = docker.get("enabled", False)
    expected[repo["path"]] = {"pool": name + "-pool", "digests": [index, runner["platformDigests"]["linux/amd64"], runner["platformDigests"]["linux/arm64"]], "base": runner["base"], "docker": enabled, "egress": docker.get("egressAllowlist", []) if enabled else []}
    if enabled:
        expected_daemon = copy.deepcopy(daemon)
        expected_daemon["namespace"] = f"{sys.argv[5]}-{prefix}-docker"
        expected_daemon["selectorLabels"]["app.kubernetes.io/instance"] = sys.argv[6]
        expected_daemon["image"] = values["factory"]["docker"]["daemonImage"]
        expected[repo["path"]]["daemon"] = expected_daemon
assert set(templates) == names, "installed repository templates differ from the render"
assert set(pools) == {name + "-pool" for name in names}, "installed repository pools differ from the render"
for name in names:
    assert pools[name + "-pool"]["spec"]["sandboxTemplateRef"]["name"] == name
    assert pools[name + "-pool"]["spec"]["replicas"] == 0
    pod = templates[name]["spec"]["podTemplate"]
    expected_env = values["agentSandbox"]["runnerEnv"].get(pod["metadata"]["labels"].get("curietech.ai/agent", ""), {})
    custom_names = {key for configured in values["agentSandbox"]["runnerEnv"].values() for key in configured}
    for container in pod["spec"].get("initContainers", []) + pod["spec"]["containers"]:
        entries = container.get("env", [])
        actual_env = {entry["name"]: entry.get("value") for entry in entries if entry["name"] in custom_names}
        assert actual_env == (expected_env if container["name"] == "runner" else {}), f"installed {name}/{container['name']} lost or leaked runnerEnv"
worker = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == prefix + "-worker")
assert worker["spec"]["replicas"] == 0
env = {e["name"]: e.get("value") for c in worker["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
actual = json.loads(env["CURIE_REPO_RUNNER_POOLS"])
assert actual == expected, "installed worker repository pool map differs from the binding"
jsonschema.validate(actual, json.load(open(sys.argv[3])))
repository_templates = sorted(name for name in names if "-img-" in name)
for i, name in enumerate(repository_templates):
    doc = copy.deepcopy(templates[name])
    doc["metadata"] = {"name": f"repo-{i}-resources", "labels": {"app.kubernetes.io/instance": "ft-tools", "curietech.ai/sandbox-claim": "chart-proof"}}
    doc.pop("status", None)
    json.dump(doc, open(pathlib.Path(sys.argv[4]) / f"admit-{i}.json", "w"))
    if i == 0:
        bad = copy.deepcopy(doc)
        bad["metadata"]["name"] = "repo-unknown-resources"
        bad["spec"]["podTemplate"]["spec"]["containers"][0]["image"] = "registry.example.com/acme/unknown@sha256:" + "9" * 64
        json.dump(bad, open(pathlib.Path(sys.argv[4]) / "admit-unknown.json", "w"))
PY
  IMPERSONATE="system:serviceaccount:$NS:$PREFIX-worker"
  image_policy_enforced=0
  admission_deadline=$((SECONDS + 60))
  while (( SECONDS < admission_deadline )); do
    if kubectl --context k8 --namespace "$NS" --as="$IMPERSONATE" create -f "$TMP/admit-unknown.json" >"$TMP/admission-negative.out" 2>&1; then
      # Admission can become active after install. Remove only this proof's
      # accepted template before retrying the same unknown image.
      kubectl --context k8 --namespace "$NS" delete sandboxtemplate repo-unknown-resources --wait=true --timeout=15s >/dev/null || fail "transient admission probe cleanup"
    elif grep -qF 'runner templates may run only the images' "$TMP/admission-negative.out"; then
      image_policy_enforced=1
      break
    fi
    sleep 2
  done
  (( image_policy_enforced )) || { cat "$TMP/admission-negative.out" >&2; fail "runner image admission did not deny the unknown image within 60 seconds"; }
  for fixture in "$TMP"/admit-[0-9].json; do
    kubectl --context k8 --namespace "$NS" --as="$IMPERSONATE" create -f "$fixture" >/dev/null || fail "runner admission refused a rendered repository image"
  done

  # The stored values still require both repository pools, but the intentional
  # post-renderer removes just those four objects from this owned release.
  # The changed CLI must refuse that inventory before attempting an upgrade.
  python3 - "$TMP" "$REAL_KUBECTL" "$REAL_HELM" <<'PY'
import pathlib, shlex, sys
tmp = pathlib.Path(sys.argv[1])
bin_dir = tmp / "cluster-bin"
bin_dir.mkdir()
for command, executable, context in (("kubectl", sys.argv[2], "--context"), ("helm", sys.argv[3], "--kube-context")):
    wrapper = bin_dir / command
    wrapper.write_text("#!/usr/bin/env bash\nexec " + shlex.quote(executable) + " " + context + ' k8 "$@"\n')
    wrapper.chmod(0o755)
post_renderer = tmp / "drop-repository-pairs.py"
post_renderer.write_text('''#!/usr/bin/env python3
import sys, yaml
docs = [doc for doc in yaml.safe_load_all(sys.stdin) if doc]
names = {"ft-tools-curie-agent-factory-img-aaaaaaaaaaaa-runner", "ft-tools-curie-agent-factory-img-bbbbbbbbbbbb-runner"}
names |= {name + "-pool" for name in tuple(names)}
def remove(doc):
    return doc.get("kind") in {"SandboxTemplate", "SandboxWarmPool"} and doc.get("metadata", {}).get("name") in names
assert sum(remove(doc) for doc in docs) == 4, "post-renderer did not find exactly two repository pairs"
yaml.safe_dump_all([doc for doc in docs if not remove(doc)], sys.stdout, sort_keys=False)
''')
post_renderer.chmod(0o755)
PY
  helm --kube-context k8 upgrade "$RELEASE" "$CHART" --namespace "$NS" --skip-crds --no-hooks "${CLUSTER_VALUES[@]}" --post-renderer "$TMP/drop-repository-pairs.py" >"$TMP/broken-inventory-upgrade.out" 2>&1 || { cat "$TMP/broken-inventory-upgrade.out" >&2; fail "owned incomplete inventory setup"; }
  helm --kube-context k8 get values "$RELEASE" --namespace "$NS" -o json >"$TMP/incomplete-values.json" || fail "incomplete inventory values readback"
  python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))==json.load(open(sys.argv[2])), "post-renderer changed stored Helm values"' "$TMP/installed-values.json" "$TMP/incomplete-values.json" || fail "runner requirements were not retained"
  helm --kube-context k8 history "$RELEASE" --namespace "$NS" -o json >"$TMP/history-before-cli.json" || fail "CLI baseline revision readback"
  python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))[-1]["status"]=="deployed", "CLI baseline release is not deployed"' "$TMP/history-before-cli.json" || fail "CLI baseline release state"
  if PATH="$TMP/cluster-bin:$PATH" "$CURIE_BIN" cluster github-app --disconnect --namespace "$NS" --release "$RELEASE" --chart "$CHART" >"$TMP/cli-inventory-negative.out" 2>&1; then
    fail "changed CLI accepted a stored manifest missing repository pools"
  fi
  grep -qF 'inventory' "$TMP/cli-inventory-negative.out" || { cat "$TMP/cli-inventory-negative.out" >&2; fail "changed CLI did not name the inventory refusal"; }
  grep -qF "$PREFIX-agent-factory-img-aaaaaaaaaaaa-runner" "$TMP/cli-inventory-negative.out" || { cat "$TMP/cli-inventory-negative.out" >&2; fail "changed CLI did not name the missing repository template"; }
  helm --kube-context k8 history "$RELEASE" --namespace "$NS" -o json >"$TMP/history-after-cli.json" || fail "CLI final revision readback"
  python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))==json.load(open(sys.argv[2])), "CLI changed Helm history before rejecting the missing repository inventory"' "$TMP/history-before-cli.json" "$TMP/history-after-cli.json" || fail "changed CLI mutated the release before refusal"
  helm --kube-context k8 get manifest "$RELEASE" --namespace "$NS" >"$TMP/manifest-after-cli.yaml" || fail "CLI final manifest readback"
  python3 - "$TMP/manifest-after-cli.yaml" <<'PY'
import sys, yaml
docs = [doc for doc in yaml.safe_load_all(open(sys.argv[1])) if doc]
names = {doc["metadata"]["name"] for doc in docs if doc.get("kind") in {"SandboxTemplate", "SandboxWarmPool"}}
assert names == {"ft-tools-curie-runner", "ft-tools-curie-runner-pool", "ft-tools-curie-agent-factory-runner", "ft-tools-curie-agent-factory-runner-pool"}, "CLI changed the intentionally incomplete desired sandbox inventory"
PY
  kubectl --context k8 --namespace "$NS" get pods -o json >"$TMP/pods.json" || fail "stackless fixture pod check"
  python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["items"]==[], "stackless admission proof started a pod"' "$TMP/pods.json" || fail "stackless fixture spawned pods"
fi

echo "PASS: runner-images assertions"
