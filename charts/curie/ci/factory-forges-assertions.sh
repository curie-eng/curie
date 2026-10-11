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

echo "factory-forges-assertions: all assertions passed"
