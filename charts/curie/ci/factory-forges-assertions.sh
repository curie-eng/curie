#!/usr/bin/env bash
#
# Render-assertion test for issue #4397 (AC 3). The factory is configured by
# one top-level `factory:` block. The api receives FACTORY_ENABLED,
# FACTORY_INTAKE, FACTORY_CI_WAIT_S and one CURIE_FORGES JSON document
# (snake_case, the shape of curie_api.forges.config.ForgesConfig) whose
# credentials are file paths into one read-only projected Secret volume, never
# key material. The removed api.* factory keys are named only by
# files/legacy-factory-keys.yaml: this script reads them from there and spells
# none of them, and a release that still sets one is refused with a message
# that names factory.* and `curie cluster factory`.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
GUARD="$CHART/files/legacy-factory-keys.yaml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

[ -f "$GUARD" ] || { echo "FAIL: $GUARD is missing" >&2; exit 1; }

cat >"$WORK/factory.yaml" <<'YAML'
api:
  githubAppId: "12345"
  githubRepoAllowlist: ["acme-corp/*"]
factory:
  enabled: true
  intake: poll
  cardBaseUrl: https://curie.example.com
  ciWaitSeconds: 1500
  codeHosts:
    - name: github
      kind: github
      host: github.com
      apiUrl: https://api.github.com
      credential:
        type: github_app
        appId: "12345"
        existingSecret: curie-factory-app
        existingSecretKey: privateKey
  trackers:
    - name: github
      kind: github
      host: github.com
      apiUrl: https://api.github.com
      label: factory
      mention: curie-bot
      pollIntervalSeconds: 30
      credential:
        type: github_app
        appId: "12345"
        existingSecret: curie-factory-app
        existingSecretKey: privateKey
  bindings:
    - name: acme-api
      tracker: github
      trackerScopeId: "812345678"
      repos:
        - alias: api
          codeHost: github
          projectId: "812345678"
          path: acme-corp/api
          bases: [main, next]
          defaultBase: next
          ci:
            required:
              key: python-ci
              paths: [apps/api, packages]
              pendingKeyPrefix: "python-ci / shard"
            metadataRerunKeys: [pr-body, "status:docs"]
YAML

render() {
  helm template rel "$CHART" -f "$CHART/values-dev.yaml" "$@"
}

render_chart() {
  local chart="$1"
  shift
  helm template rel "$chart" -f "$CHART/values-dev.yaml" "$@"
}

check() {
  python3 -c '
import json, sys, yaml

mode, guard_path = sys.argv[1], sys.argv[2]
MOUNT = "/etc/curie/forges"
SECRET_FIELDS = {"private_key", "client_secret", "token"}
# The env prefix of the removed settings, built so this file names no removed env.
OLD_ENV_PREFIX = "GITHUB_" + "FACTORY_"

def fail(msg):
    sys.stderr.write(f"FAIL [{mode}]: {msg}\n")
    sys.exit(1)

raw = sys.stdin.read()
docs = [d for d in yaml.safe_load_all(raw) if d]
legacy_leaves = [key.split(".")[-1] for key in yaml.safe_load(open(guard_path))]

api = next(
    (d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-curie-api")),
    None,
) or fail("no api Deployment rendered")
spec = api["spec"]["template"]["spec"]
container = next(c for c in spec["containers"] if c["name"] == "api")
env = {e["name"]: e for e in container.get("env", []) or []}

for doc in docs:
    for c in (doc.get("spec", {}).get("template", {}).get("spec", {}) or {}).get("containers", []) or []:
        if c.get("name") == "worker" and any(e["name"] == "CURIE_REPO_RUNNER_POOLS" for e in c.get("env", []) or []):
            fail("schema-only repository blocks must not render CURIE_REPO_RUNNER_POOLS")
        stale = [e["name"] for e in c.get("env", []) or [] if e["name"].startswith(OLD_ENV_PREFIX)]
        if stale:
            fail("%s/%s still renders %s" % (doc["metadata"]["name"], c["name"], stale))
for leaf in legacy_leaves:
    if leaf in raw:
        fail(f"the render still names the removed key {leaf}")

def value(name):
    entry = env.get(name) or fail(f"api env {name} is not rendered")
    if "value" not in entry:
        fail(f"api env {name} must be a plain value, got {entry}")
    return entry["value"]

forges = json.loads(value("CURIE_FORGES") or "{}")
volumes = [v for v in spec.get("volumes", []) or [] if "projected" in v and any(
    "secret" in s for s in v["projected"].get("sources", []))]
mounts = [m for m in container.get("volumeMounts", []) or [] if m["mountPath"] == MOUNT]

if mode == "default":
    if value("FACTORY_ENABLED") != "false":
        fail("the factory must default off")
    if forges.get("code_hosts") or forges.get("trackers") or forges.get("bindings"):
        fail(f"no factory config must render no forges, got {forges}")
    if mounts:
        fail(f"no credential must mount nothing at {MOUNT}, got {mounts}")
    print("  ok: default render has the factory off and no forge credentials")
    sys.exit(0)

if (value("FACTORY_ENABLED"), value("FACTORY_INTAKE"), value("FACTORY_CI_WAIT_S")) != ("true", "poll", "1500"):
    fail("FACTORY_ENABLED / FACTORY_INTAKE / FACTORY_CI_WAIT_S do not carry factory.*")

def walk(node, path="$"):
    if isinstance(node, dict):
        for key, child in node.items():
            yield path, key, child
            yield from walk(child, f"{path}.{key}")
    elif isinstance(node, list):
        for index, child in enumerate(node):
            yield from walk(child, f"{path}[{index}]")

for path, key, child in walk(forges):
    if key in SECRET_FIELDS:
        fail(f"CURIE_FORGES carries secret material at {path}.{key}")
    if key == "egress" or key != key.lower():
        fail(f"CURIE_FORGES must be explicit snake_case fields, got {path}.{key}")
if "BEGIN" in value("CURIE_FORGES"):
    fail("CURIE_FORGES carries key material")

(code_host,) = forges["code_hosts"]
(tracker,) = forges["trackers"]
(binding,) = forges["bindings"]
if (code_host["name"], code_host["kind"], code_host["host"], code_host["api_url"]) != (
    "github", "github", "github.com", "https://api.github.com"):
    fail(f"code host entry {code_host}")
if (tracker["label"], tracker["mention"], tracker["poll_interval_s"]) != ("factory", "curie-bot", 30):
    fail(f"tracker entry {tracker}")
if forges.get("card_base_url") != "https://curie.example.com":
    fail("card_base_url does not carry factory.cardBaseUrl")
(repo,) = binding["repos"]
if (binding["tracker_scope_id"], repo["project_id"], repo["code_host"]) != ("812345678", "812345678", "github"):
    fail(f"binding identity must stay strings, got {binding}")
if (repo["bases"], repo["default_base"]) != (["main", "next"], "next"):
    fail(f"bases {repo}")
if repo["ci"] != {
    "required": {"key": "python-ci", "paths": ["apps/api", "packages"], "pending_key_prefix": "python-ci / shard"},
    "metadata_rerun_keys": ["pr-body", "status:docs"],
}:
    fail("ci policy %s" % (repo["ci"],))

if mode == "tools":
    digest = lambda c: "sha256:" + c * 64
    expected_runner = {
        "tools": ["uv", "rust", "pnpm", "build-essential", "docker-cli"],
        "image": "registry.example.com/acme/runner@" + digest("a"), "base": digest("d"),
        "platform_digests": {"linux/amd64": digest("b"), "linux/arm64": digest("c")},
        "dockerfile": "Dockerfile.runner",
        "probes": [{"name": "check", "command": ["check", "", "--version"]}],
    }
    if repo.get("runner") != expected_runner:
        fail("runner fields, platform keys or probe argv changed in CURIE_FORGES")
    expected_docker = {"enabled": True, "egress_allowlist": [
        {"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]},
    ]}
    if repo.get("docker") != expected_docker:
        fail("Docker declaration changed in CURIE_FORGES")
elif "runner" in repo or "docker" in repo:
    fail("absent repository runner/Docker blocks must stay absent")

if len(volumes) != 1:
    fail(f"expected one projected Secret volume for forge credentials, got {volumes}")
volume = volumes[0]
if volume["projected"].get("defaultMode") != 0o440:
    fail("forge credential files must be 0440, got %s" % volume["projected"].get("defaultMode"))
if len(mounts) != 1 or mounts[0].get("readOnly") is not True or mounts[0]["name"] != volume["name"]:
    fail(f"the credential volume must mount once, read-only, at {MOUNT}: {mounts}")
projected = {}
for source in volume["projected"]["sources"]:
    secret = source.get("secret")
    if secret is None:
        continue
    for item in secret.get("items", []):
        projected[MOUNT + "/" + item["path"]] = (secret["name"], item["key"])
for entry in (code_host, tracker):
    credential = entry["credential"]
    if (credential["type"], credential["app_id"]) != ("github_app", "12345"):
        fail(f"credential {credential}")
    path = credential.get("private_key_file") or fail("%s names no private_key_file" % entry["name"])
    if projected.get(path) != ("curie-factory-app", "privateKey"):
        fail(f"{path} is not projected from curie-factory-app/privateKey: {projected}")
print("  ok: factory block renders FACTORY_* env, CURIE_FORGES and a read-only credential mount")
' "$1" "$GUARD"
}

refused() {
  # $1: label; $2: required text in the error; remaining: render args.
  local label="$1" needle="$2"
  shift 2
  local out
  if out=$("$@" 2>&1 >/dev/null); then
    echo "FAIL: $label rendered; it must be refused" >&2
    exit 1
  fi
  if ! grep -qF -- "$needle" <<<"$out"; then
    echo "FAIL: $label was refused without naming '$needle':" >&2
    echo "$out" >&2
    exit 1
  fi
}

# Liveness and shape ------------------------------------------------------------
render | check default
render -f "$WORK/factory.yaml" | check factory

# Repository tool declarations remain data; no pool env or runtime is added.
python3 - "$WORK/factory.yaml" "$WORK/tools.yaml" <<'PY'
import sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
repo = doc["factory"]["bindings"][0]["repos"][0]
digest = lambda c: "sha256:" + c * 64
repo["runner"] = {
    "tools": ["uv", "rust", "pnpm", "build-essential", "docker-cli"],
    "image": "registry.example.com/acme/runner@" + digest("a"), "base": digest("d"),
    "platformDigests": {"linux/amd64": digest("b"), "linux/arm64": digest("c")},
    "dockerfile": "Dockerfile.runner",
    "probes": [{"name": "check", "command": ["check", "", "--version"]}],
}
repo["docker"] = {"enabled": True, "egressAllowlist": [
    {"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]},
]}
yaml.safe_dump(doc, open(sys.argv[2], "w"), sort_keys=False)
PY
render -f "$WORK/tools.yaml" | check tools
PREFIX='factory.bindings[0].repos[0]'
DIGEST="$(python3 -c 'print("sha256:" + "a" * 64)')"
for image in "registry.example.com:5000/acme/runner@$DIGEST" "registry.example.com:5000/acme/runner:v1@$DIGEST"; do
  render -f "$WORK/tools.yaml" --set-string "$PREFIX.runner.image=$image" >/dev/null
done
render -f "$WORK/factory.yaml" --set-json "$PREFIX.docker={\"enabled\":false}" >/dev/null
render -f "$WORK/factory.yaml" --set-json "$PREFIX.docker={\"enabled\":false,\"egressAllowlist\":[{\"cidr\":\"203.0.113.0/24\",\"ports\":[{\"protocol\":\"TCP\",\"port\":443}]}]}" >/dev/null
refused "tag-only repository image" "runner" render -f "$WORK/tools.yaml" \
  --set-string "$PREFIX.runner.image=registry.example.com/acme/runner:latest"
refused "unknown catalog tool" "tools" render -f "$WORK/tools.yaml" \
  --set-json "$PREFIX.runner.tools=[\"unknown-tool\"]"
refused "empty probe argv" "command" render -f "$WORK/tools.yaml" \
  --set-json "$PREFIX.runner.probes=[{\"name\":\"check\",\"command\":[]}]"
refused "blank probe executable" "command" render -f "$WORK/tools.yaml" \
  --set-json "$PREFIX.runner.probes=[{\"name\":\"check\",\"command\":[\" \t\"]}]"
refused "Docker enabled without CLI" "runner" render -f "$WORK/tools.yaml" \
  --set-json "$PREFIX.runner.tools=[\"uv\"]"
refused "Docker enabled without runner" "runner" render -f "$WORK/factory.yaml" \
  --set-json "$PREFIX.docker={\"enabled\":true}"
echo "  ok: tool blocks preserve platform keys and argv; invalid declarations are refused"

# `--reuse-values` from a release that predates the block: no `factory` key.
render --set-json 'factory=null' >/dev/null
echo "  ok: a render with no factory key renders"

# The removed keys, read from the guard file -------------------------------------
mapfile -t LEGACY < <(python3 -c 'import sys, yaml; print("\n".join(yaml.safe_load(open(sys.argv[1]))))' "$GUARD")
[ "${#LEGACY[@]}" -ge 1 ] || { echo "FAIL: the guard file lists no key" >&2; exit 1; }

# A value no old chart default can equal, chosen by the leaf's shape.
nondefault() {
  case "$1" in
    *Enabled) echo 'true' ;;
    *Seconds) echo '7' ;;
    *Ci | *Bases) echo '{"acme-corp/api": {"probe": ["x"]}}' ;;
    *) echo '"probe-nondefault"' ;;
  esac
}

nulls=()
for key in "${LEGACY[@]}"; do
  refused "legacy key $key set" "factory." render -f "$WORK/factory.yaml" --set-json "$key=$(nondefault "${key##*.}")"
  refused "legacy key $key set" "curie cluster factory" render --set-json "$key=$(nondefault "${key##*.}")"
  nulls+=(--set-json "$key=null")
done
echo "  ok: each of ${#LEGACY[@]} legacy keys set to a non-default value is refused naming factory.* and curie cluster factory"

# What `curie cluster factory` writes: every legacy key null, beside the block.
render -f "$WORK/factory.yaml" "${nulls[@]}" | check factory
echo "  ok: legacy keys written as null pass the guard"

# Incomplete factory config ------------------------------------------------------
refused "factory.enabled with no tracker" "factory." \
  render -f "$WORK/factory.yaml" --set-json 'factory.trackers=[]'
refused "factory.enabled with no code host" "factory." \
  render -f "$WORK/factory.yaml" --set-json 'factory.codeHosts=[]'
refused "a credential with no existingSecret" "existingSecret" \
  render -f "$WORK/factory.yaml" --set 'factory.codeHosts[0].credential.existingSecret='
refused "a factory App id other than api.githubAppId" "appId" \
  render -f "$WORK/factory.yaml" --set-string 'api.githubAppId=99999'
refused "an intake outside poll and webhook" "intake" \
  render -f "$WORK/factory.yaml" --set 'factory.intake=push'
refused "ciWaitSeconds above 10800" "ciWaitSeconds" \
  render -f "$WORK/factory.yaml" --set 'factory.ciWaitSeconds=10801'
echo "  ok: incomplete or inconsistent factory config is refused"
render -f "$WORK/factory.yaml" --set 'factory.enabled=false' --set-json 'factory.trackers=[]' >/dev/null
echo "  ok: a disabled factory needs no tracker"

# Negative control: the refusal comes from the guard file, not a hard-coded key.
cp -R "$CHART" "$WORK/chart"
echo '[]' >"$WORK/chart/files/legacy-factory-keys.yaml"
probe="${LEGACY[0]}"
if ! render_chart "$WORK/chart" --set-json "$probe=$(nondefault "${probe##*.}")" >/dev/null 2>&1; then
  echo "FAIL: with an empty guard file $probe is still refused; the template must read the guard file" >&2
  exit 1
fi
echo "  ok: negative control (empty guard file) no longer refuses $probe"

# Egress (#4397 AC 6, decision 6) ----------------------------------------------
# One source: each factory.codeHosts[] / factory.trackers[] entry's `egress`
# feeds both the publication Job policy (code hosts) and the per-agent sandbox
# policy (code hosts plus trackers) of every factory.sandboxEgress.<agent>. A
# github entry with no egress gets the chart's built-in GitHub set, pinned in
# ci/fixtures/factory-github-builtin-egress.yaml. Forge entries may name RFC
# 1918 addresses and any concrete TCP port (self-managed GitLab, Bitbucket DC).
BUILTIN="$SCRIPT_DIR/fixtures/factory-github-builtin-egress.yaml"
[ -f "$BUILTIN" ] || { echo "FAIL: $BUILTIN is missing" >&2; exit 1; }
SANDBOX=(--set agentSandbox.deploy=true --set agentSandbox.controller.deploy=false)

# The same factory block, with operator egress on both entries: a private
# self-managed host on 8443 for the code host (with the hostname it was
# resolved from), and a /8 for the tracker that strictly contains an RFC 1918
# range, so the sandbox policy must carry the private carve-out.
python3 - "$WORK/factory.yaml" "$WORK/egress.yaml" <<'PY'
import sys, yaml
src, dst = sys.argv[1:]
doc = yaml.safe_load(open(src))
doc["factory"]["codeHosts"][0]["egress"] = [
    {"cidr": "10.20.0.5/32", "ports": [{"protocol": "TCP", "port": 8443}], "host": "git.internal.example"},
]
doc["factory"]["trackers"][0]["egress"] = [
    {"cidr": "172.0.0.0/8", "ports": [{"protocol": "TCP", "port": 443}]},
]
yaml.safe_dump(doc, open(dst, "w"), sort_keys=False)
PY

EGRESS_CHECK="$WORK/egress_check.py"
cat >"$EGRESS_CHECK" <<'PY'
import ipaddress, json, sys, yaml

label, spec_text, builtin_path = sys.argv[1], sys.argv[2], sys.argv[3]
spec = json.loads(spec_text)
builtin_doc = yaml.safe_load(open(builtin_path))
BUILTIN = {(c, builtin_doc["port"]) for c in builtin_doc["cidrs"]}


def fail(msg):
    sys.stderr.write(f"FAIL [{label}]: {msg}\n")
    sys.exit(1)


def expand(pairs):
    if pairs == "builtin":
        return set(BUILTIN)
    return {(c, p) for c, p in pairs}


raw = sys.stdin.read()
docs = [d for d in yaml.safe_load_all(raw) if d]
if "githubHttpsCidrs" in raw:
    fail("the render still names githubHttpsCidrs")


def labels(doc):
    return (doc.get("metadata") or {}).get("labels") or {}


def ip_rules(policy):
    """(cidr, port) pairs and per-cidr excepts of every non-DNS egress rule."""
    pairs, excepts = set(), {}
    for index, rule in enumerate(policy["spec"].get("egress") or []):
        ports = rule.get("ports") or []
        if {p.get("port") for p in ports} == {53}:
            continue
        peers = rule.get("to") or fail(f"{policy['metadata']['name']} rule {index} has an empty to:")
        if not ports:
            fail(f"{policy['metadata']['name']} rule {index} names no port (every port allowed)")
        for peer in peers:
            block = peer.get("ipBlock") or fail(f"{policy['metadata']['name']} rule {index} has a non-ipBlock peer {peer}")
            for port in ports:
                if "endPort" in port:
                    fail(f"{policy['metadata']['name']} renders an endPort range")
                pairs.add((block["cidr"], port["port"]))
            excepts[block["cidr"]] = block.get("except") or []
    return pairs, excepts


publications = [
    d for d in docs
    if d.get("kind") == "NetworkPolicy" and labels(d).get("app.kubernetes.io/component") == "publication"
    and any((r.get("ports") or []) and {p.get("port") for p in r["ports"]} != {53} for r in d["spec"].get("egress") or [])
]
if "publication" in spec:
    if len(publications) != 1:
        fail(f"expected one publication egress NetworkPolicy, got {[p['metadata']['name'] for p in publications]}")
    got, excepts = ip_rules(publications[0])
    pub = spec["publication"]
    if not isinstance(pub, dict):
        pub = {"pairs": pub}
    want = expand(pub["pairs"])
    if got != want:
        fail(f"publication egress: missing {sorted(want - got)}, unexpected {sorted(got - want)}")
    # The publication Job carves exactly what the sandbox policy carves
    # (curie.egress.ipBlockExcept): an entry's own except plus private ranges.
    for cidr, needed in (pub.get("excepts") or {}).items():
        if set(excepts.get(cidr, [])) != set(needed):
            fail(f"publication {cidr} must except exactly {sorted(needed)}, got {excepts.get(cidr)}")
    for cidr, carved in excepts.items():
        if carved and cidr not in (pub.get("excepts") or {}):
            fail(f"publication {cidr} has unexpected excepts {carved}")
    print(f"  ok [{label}]: publication Job egress is exactly {len(want)} (cidr, port) pairs with their excepts")

forge_policies = {
    d["metadata"]["name"]: d for d in docs
    if d.get("kind") == "NetworkPolicy" and d["metadata"]["name"].endswith("-allow-forge-egress")
}
sandbox = spec.get("sandbox", {})
expected_names = {f"rel-curie-agent-{agent}-allow-forge-egress" for agent in sandbox}
if set(forge_policies) != expected_names:
    fail(f"forge sandbox policies {sorted(forge_policies)} != {sorted(expected_names)}")
pools_env = None
for d in docs:
    if d.get("kind") == "Deployment" and d["metadata"]["name"] == "rel-curie-worker":
        for c in d["spec"]["template"]["spec"]["containers"]:
            for e in c.get("env") or []:
                if e["name"] == "CURIE_AGENT_SANDBOX_POOLS":
                    pools_env = e.get("value", "")
templates = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "SandboxTemplate"}
for agent, want_spec in sandbox.items():
    policy = forge_policies[f"rel-curie-agent-{agent}-allow-forge-egress"]
    selector = policy["spec"]["podSelector"].get("matchLabels") or {}
    if selector.get("curietech.ai/agent") != agent or selector.get("app.kubernetes.io/component") != "runner-sandbox":
        fail(f"{agent} forge policy selector {selector} does not select that agent's runner sandboxes")
    if policy["spec"].get("policyTypes") != ["Egress"]:
        fail(f"{agent} forge policy must be egress-only, got {policy['spec'].get('policyTypes')}")
    got, excepts = ip_rules(policy)
    want = expand(want_spec["pairs"])
    if got != want:
        fail(f"{agent} sandbox forge egress: missing {sorted(want - got)}, unexpected {sorted(got - want)}")
    for cidr, needed in (want_spec.get("excepts") or {}).items():
        if not set(needed) <= set(excepts.get(cidr, [])):
            fail(f"{agent} {cidr} must except {needed} (curie.egress.ipBlockRules), got {excepts.get(cidr)}")
    for cidr, carved in excepts.items():
        net = ipaddress.ip_network(cidr)
        for e in carved:
            if not ipaddress.ip_network(e).subnet_of(net):
                fail(f"{agent} {cidr} excepts {e}, which is outside it (Kubernetes rejects the policy)")
    template = templates.get(f"rel-curie-agent-{agent}-runner") or fail(
        f"{agent} has no per-agent SandboxTemplate, so no pod carries curietech.ai/agent: {agent}; templates {sorted(templates)}")
    pod_labels = template["spec"]["podTemplate"]["metadata"].get("labels") or {}
    if pod_labels.get("curietech.ai/agent") != agent:
        fail(f"{agent} SandboxTemplate pods do not carry curietech.ai/agent: {pod_labels}")
    if pools_env is None or agent not in pools_env.split(","):
        fail(f"{agent} is not in the worker's CURIE_AGENT_SANDBOX_POOLS ({pools_env!r}); its claims would use the shared pool")
    print(f"  ok [{label}]: {agent} is enrolled in the sandbox pool and its forge policy carries {len(want)} pairs")
PY

egress() {
  # $1: label; $2: JSON expectations; remaining: render args.
  local label="$1" spec="$2"
  shift 2
  render "$@" | python3 "$EGRESS_CHECK" "$label" "$spec" "$BUILTIN"
}

# Publication without the factory still targets the install's GitHub (remote-dev).
egress "no factory block" '{"publication": "builtin"}'
egress "factory key absent (--reuse-values)" '{"publication": "builtin"}' --set-json 'factory=null'
# A github code host with no egress gets the built-in set; the tracker too.
egress "github entries with no egress" '{"publication": "builtin"}' -f "$WORK/factory.yaml"
egress "sandboxEgress, github entries with no egress" \
  '{"publication": "builtin", "sandbox": {"dark-factory": {"pairs": "builtin"}}}' \
  -f "$WORK/factory.yaml" "${SANDBOX[@]}" --set 'factory.sandboxEgress.dark-factory=true'
# Operator egress replaces the built-in set; one source feeds both policies.
egress "operator egress on code host and tracker" \
  '{"publication": [["10.20.0.5/32", 8443]], "sandbox": {"dark-factory": {"pairs": [["10.20.0.5/32", 8443], ["172.0.0.0/8", 443]], "excepts": {"172.0.0.0/8": ["172.16.0.0/12"]}}}}' \
  -f "$WORK/egress.yaml" "${SANDBOX[@]}" --set 'factory.sandboxEgress.dark-factory=true'
# A broad code host route keeps its operator except and gains the private
# carve-out on the publication Job policy exactly as on the sandbox policy.
egress "code host egress with an operator except" \
  '{"publication": {"pairs": [["172.0.0.0/8", 443]], "excepts": {"172.0.0.0/8": ["172.99.0.0/16", "172.16.0.0/12"]}}, "sandbox": {"dark-factory": {"pairs": [["172.0.0.0/8", 443]], "excepts": {"172.0.0.0/8": ["172.99.0.0/16", "172.16.0.0/12"]}}}}' \
  -f "$WORK/egress.yaml" "${SANDBOX[@]}" --set 'factory.sandboxEgress.dark-factory=true' \
  --set-json 'factory.codeHosts[0].egress=[{"cidr":"172.0.0.0/8","ports":[{"protocol":"TCP","port":443}],"except":["172.99.0.0/16"]}]' \
  --set-json 'factory.trackers[0].egress=[{"cidr":"172.0.0.0/8","ports":[{"protocol":"TCP","port":443}],"except":["172.99.0.0/16"]}]'
# The egress fields are chart-only: CURIE_FORGES still carries no egress or host.
render -f "$WORK/egress.yaml" | check factory
# Two agents get one policy each; an agent set to false gets none.
egress "two agents, one false" \
  '{"publication": [["10.20.0.5/32", 8443]], "sandbox": {"dark-factory": {"pairs": [["10.20.0.5/32", 8443], ["172.0.0.0/8", 443]]}, "dark-factory-fast": {"pairs": [["10.20.0.5/32", 8443], ["172.0.0.0/8", 443]]}}}' \
  -f "$WORK/egress.yaml" "${SANDBOX[@]}" --set 'factory.sandboxEgress.dark-factory=true' \
  --set 'factory.sandboxEgress.dark-factory-fast=true' --set 'factory.sandboxEgress.dark-factory-off=false'
# A platform-runner agent (no runnerImages entry, nothing else naming it) is
# enrolled by sandboxEgress alone: the check above already requires its
# per-agent SandboxTemplate and pool env entry.
echo "  ok: one egress source feeds publication and per-agent sandbox policies"

# Refusals (each has a rendering liveness case above) ----------------------------
refused "a code host egress default route" "default or equivalently broad route" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"0.0.0.0/0","ports":[{"port":443}]}]'
refused "a tracker egress default route" "default or equivalently broad route" \
  render -f "$WORK/egress.yaml" --set-json 'factory.trackers[0].egress=[{"cidr":"0.0.0.0/0","ports":[{"port":443}]}]'
refused "a code host egress covering cloud metadata" "metadata" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"169.254.0.0/16","ports":[{"port":443}]}]'
refused "a code host egress IPv4 /7" "per-family floor" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"2.0.0.0/7","ports":[{"port":443}]}]'
refused "an operator IPv6 egress shorter than /32" "per-family floor" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"2001:db8::/31","ports":[{"port":443}]}]'
refused "a code host egress with endPort" "endPort" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"10.20.0.5/32","ports":[{"port":8443,"endPort":8450}]}]'
refused "a code host egress with no port" "port" \
  render -f "$WORK/egress.yaml" --set-json 'factory.codeHosts[0].egress=[{"cidr":"10.20.0.5/32","ports":[{"protocol":"TCP"}]}]'
refused "an invalid sandboxEgress agent name" "sandboxEgress" \
  render -f "$WORK/factory.yaml" "${SANDBOX[@]}" --set 'factory.sandboxEgress.Dark_Factory=true'
refused "a sandboxEgress agent name over 40 characters" "sandboxEgress" \
  render -f "$WORK/factory.yaml" "${SANDBOX[@]}" --set "factory.sandboxEgress.$(printf 'a%.0s' {1..41})=true"
# A code host that is not github and names no egress leaves publication nothing
# to reach; the empty-list fail stays (networkpolicy-empty-to-assertions.sh
# holds its negative control).
refused "a non-github code host with no egress" "egress" \
  render --set-json 'factory.codeHosts=[{"name":"gl","kind":"gitlab","host":"gitlab.internal.example","apiUrl":"https://gitlab.internal.example/api/v4","credential":{"type":"static_token","existingSecret":"gl","existingSecretKey":"token","neverExpires":true}}]'
egress "a non-github code host with its own egress" '{"publication": [["10.30.0.9/32", 443]]}' \
  --set-json 'factory.codeHosts=[{"name":"gl","kind":"gitlab","host":"gitlab.internal.example","apiUrl":"https://gitlab.internal.example/api/v4","credential":{"type":"static_token","existingSecret":"gl","existingSecretKey":"token","neverExpires":true},"egress":[{"cidr":"10.30.0.9/32","ports":[{"port":443}]}]}]'
echo "  ok: route-shaped, metadata, endPort and portless forge egress and bad agent names are refused"

# The removed list is gone from every chart source, not only from the render.
for source in "$CHART/values.yaml" "$CHART/values.schema.json" "$CHART"/templates/*; do
  if grep -qF 'githubHttpsCidrs' "$source"; then
    echo "FAIL: $source still names githubHttpsCidrs" >&2
    exit 1
  fi
done
echo "  ok: worker.publication.githubHttpsCidrs is gone from values, schema and templates"

echo "factory-forges-assertions: all assertions passed"
