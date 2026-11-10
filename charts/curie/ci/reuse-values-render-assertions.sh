#!/usr/bin/env bash
#
# The candidate chart must render on top of a PREVIOUS release's values, the
# way `helm upgrade --reuse-values` hands them to it (#3813).
#
# `--reuse-values` does not merge the new chart's values.yaml at all. Helm
# rebuilds the release's values from the OLD chart's values.yaml coalesced with
# the operator's stored user-supplied values, and renders the NEW templates
# against that (pkg/action/upgrade.go, reuseValues: `chart.Values = oldVals`).
# So a template that dereferences a top level key the old values.yaml never had
# evaluates nil and the upgrade dies. That shipped three times: `placement`
# (#2008), `langfuse.eventUpload` in v0.10.2, and `connectorCaller` in v0.11.0
# (#3505, guarded by #3544). Every other render assertion here renders against
# the candidate's own values.yaml, where every key exists, so none of them can
# see this class.
#
# This gate reproduces that render offline: copy the candidate chart, replace
# its values.yaml with the released chart's, and `helm template` it with the
# operator's values on top. Nothing comes from the candidate's values.yaml. The
# candidate's values.schema.json stays in place, because Helm validates the
# upgraded values against the new chart's schema too.
#
# Two released sources, both taken from the published chart artifact:
#
#   * the newest stable release at or below the candidate's chart version,
#     resolved at run time and verified against that release's checksums.txt.
#     This is the upgrade every operator on the current release runs next.
#   * v0.10.3, pinned by digest: the last release before `connectorCaller`
#     existed, i.e. the upgrade source that #3505 broke. A key only goes
#     missing relative to a release that predates it, so the newest release
#     alone cannot hold the negative control below.
#
# Each source renders under three operator profiles: no user values, the
# release's own values-dev.yaml, and ci/fixtures/reuse-values-operator.yaml.
# Each profile is first rendered with the RELEASED chart, so a profile the
# release itself rejects fails here instead of quietly testing nothing.
#
# On the newest source it also proves the sandbox resource profiles (#4889):
# released values keep each released template envelope, and the factory agent
# matches the released factory profile (factory_env, with explicit cpu and
# workspace overrides). Candidate values give a factory agent the full factory
# profile, and explicit overrides win on both.
#
# Negative control: reverting #3544's nil guard on `connectorCaller` in a copy
# of the candidate must make the v0.10.3 render fail with a nil dereference.
# If it renders, this gate cannot see the bug class and fails.
#
# Needs helm, python3, and gh (authenticated, or GH_TOKEN) for the release
# lookup. Network access is required; no cluster is touched.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
OPERATOR_VALUES="$SCRIPT_DIR/fixtures/reuse-values-operator.yaml"
REPO_SLUG="curie-eng/curie"

# The upgrade source #3505 broke. Bump only together with a negative control
# that still fails against the new pin.
REGRESSION_VERSION="0.10.3"
REGRESSION_SHA256="7f2a8582d51d49867870b3bd2179277e673f8aecaf81f6927ba219b5072a5089"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for tool in helm python3 gh; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
[[ -f "$OPERATOR_VALUES" ]] || fail "operator values fixture is missing: $OPERATOR_VALUES"

chart_version() {
  helm show chart "$1" | awk '$1 == "version:" {print $2}'
}

CANDIDATE_VERSION="$(chart_version "$CHART")"
[[ -n "$CANDIDATE_VERSION" ]] || fail "could not read the candidate chart version"

# --- resolve the newest stable release at or below the candidate -------------
LATEST_VERSION="$(gh release list --repo "$REPO_SLUG" --exclude-drafts \
    --exclude-pre-releases --limit 200 --json tagName --jq '.[].tagName' \
  | python3 -c '
import re, sys

def key(v):
    return tuple(int(p) for p in v.split("."))

candidate = key(sys.argv[1].split("-")[0])
versions = []
for line in sys.stdin:
    m = re.fullmatch(r"v(\d+\.\d+\.\d+)", line.strip())
    if m and key(m.group(1)) <= candidate:
        versions.append(m.group(1))
if versions:
    print(max(versions, key=key))
' "$CANDIDATE_VERSION")"
[[ -n "$LATEST_VERSION" ]] || fail "no stable release at or below candidate $CANDIDATE_VERSION"
echo "OK: candidate chart $CANDIDATE_VERSION; newest stable release at or below it is $LATEST_VERSION"

fetch_release() {
  # fetch_release <version> [pinned sha256]
  local version="$1" pinned="${2:-}" dir="$TMP/released-$1"
  local tgz="curie-$version.tgz" want
  mkdir -p "$dir"
  gh release download "v$version" --repo "$REPO_SLUG" --pattern "$tgz" --dir "$dir" >&2
  if [[ -n "$pinned" ]]; then
    want="$pinned"
  else
    gh release download "v$version" --repo "$REPO_SLUG" --pattern checksums.txt --dir "$dir" >&2
    want="$(awk -v f="$tgz" '$2 == f {print $1}' "$dir/checksums.txt")"
    [[ -n "$want" ]] || fail "v$version checksums.txt does not list $tgz"
  fi
  echo "$want  $dir/$tgz" | sha256sum -c - >&2 || fail "v$version chart artifact digest mismatch"
  tar -xzf "$dir/$tgz" -C "$dir"
  [[ "$(chart_version "$dir/curie")" == "$version" ]] \
    || fail "v$version artifact does not carry chart version $version"
}

reuse_chart() {
  # reuse_chart <version> <dest>: the candidate chart with ONLY the released
  # values.yaml, which is what --reuse-values renders the new templates with.
  local version="$1" dest="$2"
  rm -rf "$dest"
  cp -R "$CHART" "$dest"
  cp "$TMP/released-$version/curie/values.yaml" "$dest/values.yaml"
  # Right after a release the candidate's values.yaml can still equal it. The
  # render is then the ordinary one; the pinned source and the negative control
  # below are what keep the gate from being vacuous.
  if cmp -s "$dest/values.yaml" "$CHART/values.yaml"; then
    echo "NOTE: v$version values.yaml is identical to the candidate's" >&2
  fi
}

render() {
  # render <install|upgrade> <chart> <out> [profile]
  # `upgrade` sets .Release.IsUpgrade, so the upgrade-only template branches
  # (installation identity, schema migrate, upgrade drain) render as they do
  # under `helm upgrade`. The released chart is rendered as the install that
  # first stored the operator values.
  local mode="$1" chart="$2" out="$3" profile="${4:-}"
  local args=(template acme "$chart")
  [[ "$mode" == upgrade ]] && args+=(--is-upgrade)
  [[ -n "$profile" ]] && args+=(-f "$profile")
  helm "${args[@]}" > "$out" 2> "$out.err"
}

assert_source() {
  local version="$1"
  local released="$TMP/released-$version/curie"
  local reuse="$TMP/reuse-$version"
  reuse_chart "$version" "$reuse"

  local -a labels=("no operator values" "released values-dev.yaml" "operator fixture")
  local -a profiles=("" "$released/values-dev.yaml" "$OPERATOR_VALUES")
  local i label profile out
  for i in "${!labels[@]}"; do
    label="${labels[$i]}"
    profile="${profiles[$i]}"
    out="$TMP/$version-$i"
    if ! render install "$released" "$out.released.yaml" "$profile"; then
      echo "FAIL: v$version itself refuses the '$label' profile, so it is not a" >&2
      echo "      values set any v$version install could hold:" >&2
      sed 's/^/    /' "$out.released.yaml.err" >&2
      exit 1
    fi
    if ! render upgrade "$reuse" "$out.reuse.yaml" "$profile"; then
      echo "FAIL: upgrading a v$version release with --reuse-values ('$label') fails:" >&2
      echo "      the candidate templates do not render on v$version's values.yaml." >&2
      echo "      Guard any top level key v$version does not define, for example" >&2
      echo "      (get (.Values.<key> | default dict) \"<field>\"), as #3544 did." >&2
      sed 's/^/    /' "$out.reuse.yaml.err" >&2
      exit 1
    fi
    echo "OK: v$version -> candidate --reuse-values render passes ($label)"
  done

  if [[ "$version" == "$LATEST_VERSION" ]]; then
    # @spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md
    # The candidate's values.yaml is replaced with the actual released chart's
    # values above. This proves an old release with no new key retains the
    # connector peer, rather than being masked by candidate defaults.
    local valkey_out="$TMP/$version-valkey-reuse.yaml"
    render upgrade "$reuse" "$valkey_out"
    python3 - "$valkey_out" <<'PY'
import sys, yaml
docs = [doc for doc in yaml.safe_load_all(open(sys.argv[1])) if doc]
policy = next(doc for doc in docs if doc.get("kind") == "NetworkPolicy"
              and doc.get("metadata", {}).get("name", "").endswith("-valkey-allow-app-ingress"))
peer = next(peer for rule in policy["spec"]["ingress"]
            for peer in rule.get("from", [])
            if peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "mcp-connector")
assert any(port.get("port") == 6379 for rule in policy["spec"]["ingress"]
           if any(peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "mcp-connector"
                  for peer in rule.get("from", [])) for port in rule.get("ports", []))
PY
    echo "OK: v$version released values retain the MCP Valkey peer under --reuse-values"
  fi
}

fetch_release "$LATEST_VERSION"
if [[ "$LATEST_VERSION" != "$REGRESSION_VERSION" ]]; then
  fetch_release "$REGRESSION_VERSION" "$REGRESSION_SHA256"
fi

assert_source "$LATEST_VERSION"
[[ "$LATEST_VERSION" == "$REGRESSION_VERSION" ]] || assert_source "$REGRESSION_VERSION"

# A valid layered runner binding from a previous release must render on that
# release and on the candidate. Removing the key is the clear. The string
# "null" stays a refused nonempty digest (#3849).
LAYER_DIGEST="ghcr.io/acme/acme-bot-runner@sha256:$(printf 'a%.0s' $(seq 1 64))"
OTHER_DIGEST="ghcr.io/acme/acme-other-runner@sha256:$(printf 'b%.0s' $(seq 1 64))"
cat > "$TMP/layered-binding.json" <<EOF
{"agentSandbox":{"runnerImages":{"acme-bot":"$LAYER_DIGEST","acme-other":"$OTHER_DIGEST"},"connectorSecrets":{"acme-bot":{"API_TOKEN":"acme-secret"}}}}
EOF
if ! helm template acme "$TMP/released-$LATEST_VERSION/curie" -f "$TMP/layered-binding.json" >"$TMP/layered-released.yaml" 2>"$TMP/layered-released.err"; then
  echo "FAIL: v$LATEST_VERSION refuses a valid layered runner binding:" >&2
  sed 's/^/    /' "$TMP/layered-released.err" >&2
  exit 1
fi
grep -q "$LAYER_DIGEST" "$TMP/layered-released.yaml" || fail "v$LATEST_VERSION did not render the layered digest"
if ! helm template acme "$CHART" --is-upgrade -f "$TMP/layered-binding.json" >"$TMP/layered-candidate.yaml" 2>"$TMP/layered-candidate.err"; then
  echo "FAIL: candidate refuses a valid layered binding carried from v$LATEST_VERSION:" >&2
  sed 's/^/    /' "$TMP/layered-candidate.err" >&2
  exit 1
fi
python3 - "$TMP/layered-binding.json" <<'PY'
import json, sys
path = sys.argv[1]
doc = json.load(open(path))
del doc["agentSandbox"]["runnerImages"]["acme-bot"]
json.dump(doc, open(path, "w"))
PY
if ! helm template acme "$CHART" --is-upgrade -f "$TMP/layered-binding.json" >"$TMP/layered-omitted.yaml" 2>"$TMP/layered-omitted.err"; then
  echo "FAIL: candidate refuses retained values once the stale layer key is removed:" >&2
  sed 's/^/    /' "$TMP/layered-omitted.err" >&2
  exit 1
fi
if grep -q "$LAYER_DIGEST" "$TMP/layered-omitted.yaml"; then
  fail "omitted acme-bot digest is still rendered"
fi
grep -q "$OTHER_DIGEST" "$TMP/layered-omitted.yaml" || fail "sibling layer was dropped with the stale key"
grep -q "acme-secret" "$TMP/layered-omitted.yaml" || fail "connector secret was dropped with the stale layer"
NEW_DIGEST="ghcr.io/acme/acme-bot-runner@sha256:$(printf 'c%.0s' $(seq 1 64))"
python3 - "$TMP/layered-binding.json" "$NEW_DIGEST" <<'PY'
import json, sys
path, digest = sys.argv[1], sys.argv[2]
doc = json.load(open(path))
doc["agentSandbox"]["runnerImages"]["acme-bot"] = digest
json.dump(doc, open(path, "w"))
PY
if ! helm template acme "$CHART" --is-upgrade -f "$TMP/layered-binding.json" >"$TMP/layered-redeploy.yaml" 2>"$TMP/layered-redeploy.err"; then
  echo "FAIL: candidate refuses a rebuilt layer digest on the upgraded chart:" >&2
  sed 's/^/    /' "$TMP/layered-redeploy.err" >&2
  exit 1
fi
grep -q "$NEW_DIGEST" "$TMP/layered-redeploy.yaml" || fail "rebuilt layer digest was not rendered"
if grep -q "$LAYER_DIGEST" "$TMP/layered-redeploy.yaml"; then
  fail "the stale layer digest survived the rebuild"
fi
if helm template acme "$CHART" --is-upgrade --set-string "agentSandbox.runnerImages.acme-bot=null" >"$TMP/layered-null.yaml" 2>"$TMP/layered-null.err"; then
  fail "the string null was accepted as a runner digest"
fi
grep -q "agentSandbox.runnerImages.acme-bot" "$TMP/layered-null.err" || fail "null digest refusal does not name the binding"
echo "OK: v$LATEST_VERSION layered binding upgrades when the stale key is removed"

# --- sandbox resource profiles across the upgrade paths (#4889) ---------------
# The candidate adds agentSandbox.resourceProfiles / agentResourceProfiles and
# leaves the generic runner.resources, workspace.sizeLimit and
# writablePathSizeLimit unset so the selected profile fills them. The current
# release already stores those profiles, so under --reuse-values the factory
# agent resolves the released factory envelope. Proves, on the newest release's
# values:
#   * released values (helm upgrade --reuse-values from that release): the
#     generic template renders byte for byte what the released chart renders,
#     and the factory agent matches the released acme-factory template
#     (factory_env, including init containers and emptyDir size limits);
#     explicit per-agent workspace and cpu overrides still win, both against
#     the released override render and as factory_env with cpu limit 3 and
#     workspace 18Gi.
#   * candidate values (helm upgrade --reset-then-reuse-values, curie cluster
#     upgrade, and a later --reuse-values once the release runs the candidate):
#     the factory agent gets the full factory profile, explicit overrides win,
#     and a release without the factory keeps only the standard generic
#     template (non-factory control).
PROFILE_FIXTURE="$SCRIPT_DIR/fixtures/factory-resource-profile.yaml"
[[ -f "$PROFILE_FIXTURE" ]] || fail "factory resource profile fixture is missing: $PROFILE_FIXTURE"
python3 - "$PROFILE_FIXTURE" "$TMP/profile-overrides.json" <<'PY'
import json, sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
doc["agentSandbox"] = {
    "workspaceSizeLimits": {"acme-factory": "18Gi"},
    "runner": {"resources": {"limits": {"cpu": "3"}}},
}
json.dump(doc, open(sys.argv[2], "w"))
PY
PROFILE_SHAPE="$TMP/profile-shape.py"
cat > "$PROFILE_SHAPE" <<'PY'
"""profile-shape.py <render> <key> (<want-json> | same <render2> <key2>)

<key> is `generic` or an agent name; `none` asserts the render has no
per-agent SandboxTemplate. <want-json> maps cpu/memory/ephemeral-storage to
[request, limit] and workspace, /tmp, /home/runner to sizeLimits. `same`
compares the runner and init container resources and every emptyDir volume
with another render's template."""
import json
import sys

import yaml


def templates(path):
    return {d["metadata"]["name"]: d for d in yaml.safe_load_all(open(path))
            if d and d.get("kind") == "SandboxTemplate"}


def find(path, key):
    names = templates(path)
    if key == "generic":
        hits = [n for n in names if "-agent-" not in n]
    else:
        hits = [n for n in names if n.endswith(f"-agent-{key}-runner")]
    if len(hits) != 1:
        sys.exit(f"{path}: want one {key} SandboxTemplate, got {sorted(names)}")
    return names[hits[0]]["spec"]["podTemplate"]["spec"]


def shape(spec):
    runner = next(c for c in spec["containers"] if c["name"] == "runner")
    mounts = {m["name"]: m["mountPath"] for m in runner.get("volumeMounts", [])}
    res = {c["name"]: {side: {k: str(v) for k, v in (c.get("resources") or {}).get(side, {}).items()}
                       for side in ("requests", "limits")}
           for c in [runner] + list(spec.get("initContainers") or [])}
    vols = {}
    for v in spec.get("volumes", []):
        if "emptyDir" in v:
            label = mounts.get(v["name"], v["name"]) if v["name"].startswith("writable-") else v["name"]
            vols[label] = str(v["emptyDir"].get("sizeLimit"))
    return res, vols


path, key = sys.argv[1], sys.argv[2]
if key == "none":
    agents = sorted(n for n in templates(path) if "-agent-" in n)
    if agents:
        sys.exit(f"{path}: expected no per-agent SandboxTemplate, got {agents}")
    raise SystemExit(0)
res, vols = shape(find(path, key))
if sys.argv[3] == "same":
    other = shape(find(sys.argv[4], sys.argv[5]))
    if (res, vols) != other:
        sys.exit(f"{key} in {path} differs from {sys.argv[5]} in {sys.argv[4]}:\n  {(res, vols)}\n  {other}")
    raise SystemExit(0)
want = json.loads(sys.argv[3])
errors = []
for name, got in res.items():
    for r in ("cpu", "memory", "ephemeral-storage"):
        pair = [got["requests"].get(r), got["limits"].get(r)]
        if pair != want[r]:
            errors.append(f"{key}: container {name} {r} {pair}, want {want[r]}")
for label in ("workspace", "/tmp", "/home/runner"):
    if vols.get(label) != want[label]:
        errors.append(f"{key}: volume {label} sizeLimit {vols.get(label)!r}, want {want[label]!r}")
if errors:
    sys.exit("\n".join(errors))
PY
shape_is() { # shape_is <label> <render> <key> <want-json | same render2 key2 | ->
  local label="$1"; shift
  python3 "$PROFILE_SHAPE" "$@" > "$TMP/profile-shape.out" 2>&1 \
    || { sed 's/^/    /' "$TMP/profile-shape.out" >&2; fail "$label"; }
}
std_env() { # std_env [cpu-limit] [workspace]
  printf '{"cpu":["50m","%s"],"memory":["192Mi","768Mi"],"ephemeral-storage":["512Mi","4Gi"],"workspace":"%s","/tmp":"512Mi","/home/runner":"512Mi"}' \
    "${1:-1}" "${2:-1Gi}"
}
factory_env() { # factory_env [cpu-limit] [workspace]
  printf '{"cpu":["50m","%s"],"memory":["8Gi","8Gi"],"ephemeral-storage":["10Gi","20Gi"],"workspace":"%s","/tmp":"4Gi","/home/runner":"15Gi"}' \
    "${1:-2}" "${2:-15Gi}"
}
profile_render() { # profile_render <label> <mode> <chart> <out> [values]
  local label="$1"; shift
  render "$@" || { sed 's/^/    /' "$3.err" >&2; fail "$label refused to render"; }
}

released="$TMP/released-$LATEST_VERSION/curie"
profile_reuse="$TMP/profile-reuse-$LATEST_VERSION"
reuse_chart "$LATEST_VERSION" "$profile_reuse"
profile_render "v$LATEST_VERSION factory fixture" install "$released" "$TMP/profile-released.yaml" "$PROFILE_FIXTURE"
profile_render "v$LATEST_VERSION factory fixture with overrides" install "$released" "$TMP/profile-released-ovr.yaml" "$TMP/profile-overrides.json"

# Released values (helm upgrade --reuse-values from v$LATEST_VERSION).
profile_render "--reuse-values from v$LATEST_VERSION with the factory fixture" \
  upgrade "$profile_reuse" "$TMP/profile-reuse.yaml" "$PROFILE_FIXTURE"
shape_is "released values: the generic template changed under --reuse-values" \
  "$TMP/profile-reuse.yaml" generic same "$TMP/profile-released.yaml" generic
shape_is "released values: the factory agent envelope changed under --reuse-values" \
  "$TMP/profile-reuse.yaml" acme-factory same "$TMP/profile-released.yaml" acme-factory
shape_is "released values: the factory agent envelope is not the factory profile" \
  "$TMP/profile-reuse.yaml" acme-factory "$(factory_env)"
profile_render "--reuse-values from v$LATEST_VERSION with explicit overrides" \
  upgrade "$profile_reuse" "$TMP/profile-reuse-ovr.yaml" "$TMP/profile-overrides.json"
shape_is "released values: explicit workspace and cpu overrides lost" \
  "$TMP/profile-reuse-ovr.yaml" acme-factory same "$TMP/profile-released-ovr.yaml" acme-factory
shape_is "released values: explicit overrides are not the factory profile" \
  "$TMP/profile-reuse-ovr.yaml" acme-factory "$(factory_env 3 18Gi)"
shape_is "released values: generic template changed under explicit overrides" \
  "$TMP/profile-reuse-ovr.yaml" generic same "$TMP/profile-released-ovr.yaml" generic
echo "OK: v$LATEST_VERSION --reuse-values keeps each released template envelope, the factory profile, and explicit overrides"

# Candidate values (--reset-then-reuse-values, curie cluster upgrade).
profile_render "candidate upgrade with the factory fixture" upgrade "$CHART" "$TMP/profile-cand.yaml" "$PROFILE_FIXTURE"
shape_is "candidate values: factory agent lacks the factory profile" \
  "$TMP/profile-cand.yaml" acme-factory "$(factory_env)"
shape_is "candidate values: generic template is not standard" "$TMP/profile-cand.yaml" generic "$(std_env)"
profile_render "candidate upgrade with explicit overrides" upgrade "$CHART" "$TMP/profile-cand-ovr.yaml" "$TMP/profile-overrides.json"
shape_is "candidate values: explicit overrides did not win over the factory profile" \
  "$TMP/profile-cand-ovr.yaml" acme-factory "$(factory_env 3 18Gi)"
shape_is "candidate values: generic template lost the explicit cpu limit" \
  "$TMP/profile-cand-ovr.yaml" generic "$(std_env 3)"
profile_render "candidate upgrade without the factory" upgrade "$CHART" "$TMP/profile-cand-none.yaml"
shape_is "candidate values (non-factory control): generic template is not standard" \
  "$TMP/profile-cand-none.yaml" generic "$(std_env)"
shape_is "candidate values (non-factory control): a per-agent template appeared" \
  "$TMP/profile-cand-none.yaml" none

# Candidate to candidate (--reuse-values once the release runs the candidate):
# Helm reuses the candidate's own values.yaml plus the stored user values.
profile_c2c="$TMP/profile-c2c"
rm -rf "$profile_c2c"
cp -R "$CHART" "$profile_c2c"
profile_render "candidate to candidate --reuse-values" upgrade "$profile_c2c" "$TMP/profile-c2c.yaml" "$TMP/profile-overrides.json"
shape_is "candidate to candidate --reuse-values lost the factory envelope or an override" \
  "$TMP/profile-c2c.yaml" acme-factory "$(factory_env 3 18Gi)"
echo "OK: candidate values give the factory agent the full factory profile with explicit overrides; a release without the factory stays standard"

# --- API memory limit from the resource profile (#4939) -----------------------
# The candidate leaves api.resources.limits.memory unset so the selected
# profile supplies it: agentSandbox.resourceProfiles.factory.apiMemoryLimit
# (2Gi) when factory.enabled, else standard (512Mi). An explicit value, even
# 512Mi, wins everywhere. A release made by an older chart stores 512Mi in its
# values.yaml, which reads as explicit under --reuse-values, so it keeps it.
# The schema-migrate Job runs the API image with the API's resources and must
# carry the same limit. CPU and the memory request never move.
API_MEMORY="$TMP/api-memory.py"
cat > "$API_MEMORY" <<'PY'
"""api-memory.py <render> <want-limit>: the API Deployment's api container has
memory limit <want-limit>, request 256Mi, cpu 100m/"1"; the schema-migrate Job's
container has the same resources."""
import sys

import yaml

path, want = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(open(path)) if d]


def container(kind, suffix, name):
    hits = [d for d in docs if d.get("kind") == kind
            and d["metadata"]["name"].endswith(suffix)]
    if len(hits) != 1:
        sys.exit(f"{path}: want one {kind} *{suffix}, got {len(hits)}")
    spec = hits[0]["spec"]["template"]["spec"]
    match = [c for c in spec["containers"] if c["name"] == name]
    if len(match) != 1:
        sys.exit(f"{path}: {kind} *{suffix} has no {name} container")
    res = match[0].get("resources") or {}
    return {side: {k: str(v) for k, v in (res.get(side) or {}).items()}
            for side in ("requests", "limits")}


api = container("Deployment", "-api", "api")
expected = {"requests": {"cpu": "100m", "memory": "256Mi"},
            "limits": {"cpu": "1", "memory": want}}
if api != expected:
    sys.exit(f"{path}: API container resources {api}, want {expected}")
migrate = container("Job", "-schema-migrate", "schema-migrate")
if migrate != api:
    sys.exit(f"{path}: schema-migrate resources {migrate} differ from the API's {api}")
PY
api_memory_is() { # api_memory_is <label> <render> <want-limit>
  local label="$1"
  python3 "$API_MEMORY" "$2" "$3" > "$TMP/api-memory.out" 2>&1 \
    || { sed 's/^/    /' "$TMP/api-memory.out" >&2; fail "$label"; }
}
api_values() { # api_values <out-json> <memory> [base-yaml]: base plus an explicit API memory limit
  python3 - "$1" "$2" "${3:-}" <<'PY'
import json, sys, yaml
out, memory, base = sys.argv[1], sys.argv[2], sys.argv[3]
doc = yaml.safe_load(open(base)) if base else {}
doc.setdefault("api", {}).setdefault("resources", {}).setdefault("limits", {})["memory"] = memory
json.dump(doc, open(out, "w"))
PY
}
api_values "$TMP/api-factory-512.json" 512Mi "$PROFILE_FIXTURE"
api_values "$TMP/api-plain-3gi.json" 3Gi

# Candidate values.
api_memory_is "candidate values: factory API memory limit is not the factory profile's 2Gi" \
  "$TMP/profile-cand.yaml" 2Gi
api_memory_is "candidate values (non-factory control): API memory limit is not the standard 512Mi" \
  "$TMP/profile-cand-none.yaml" 512Mi
profile_render "candidate upgrade, factory with explicit API memory 512Mi" \
  upgrade "$CHART" "$TMP/api-cand-factory-512.yaml" "$TMP/api-factory-512.json"
api_memory_is "candidate values: an explicit 512Mi API memory limit lost to the factory profile" \
  "$TMP/api-cand-factory-512.yaml" 512Mi
profile_render "candidate upgrade, non-factory with explicit API memory 3Gi" \
  upgrade "$CHART" "$TMP/api-cand-plain-3gi.yaml" "$TMP/api-plain-3gi.json"
api_memory_is "candidate values (non-factory): an explicit 3Gi API memory limit lost to the standard profile" \
  "$TMP/api-cand-plain-3gi.yaml" 3Gi

# Released values (helm upgrade --reuse-values from v$LATEST_VERSION): the
# released values.yaml's memory limit reads as explicit and is retained, so
# the API renders what the released chart rendered.
released_api_memory="$(python3 - "$released/values.yaml" <<'PY'
import sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
print(((((doc.get("api") or {}).get("resources") or {}).get("limits") or {}).get("memory")) or "")
PY
)"
if [[ -n "$released_api_memory" ]]; then
  [[ "$released_api_memory" == 512Mi ]] \
    || fail "v$LATEST_VERSION values.yaml carries API memory $released_api_memory; this gate expects the 512Mi older charts shipped"
  api_memory_is "released values: --reuse-values from v$LATEST_VERSION did not keep the released API memory limit" \
    "$TMP/profile-reuse.yaml" "$released_api_memory"
else
  # The release already resolves the limit from its profile catalog.
  api_memory_is "released values: --reuse-values from v$LATEST_VERSION changed the API memory limit" \
    "$TMP/profile-reuse.yaml" 2Gi
fi

# Candidate to candidate (--reuse-values once the release runs the candidate).
profile_render "candidate to candidate --reuse-values with the factory fixture" \
  upgrade "$profile_c2c" "$TMP/api-c2c-factory.yaml" "$PROFILE_FIXTURE"
api_memory_is "candidate to candidate --reuse-values: factory API memory limit is not 2Gi" \
  "$TMP/api-c2c-factory.yaml" 2Gi
profile_render "candidate to candidate --reuse-values with explicit API memory 512Mi" \
  upgrade "$profile_c2c" "$TMP/api-c2c-factory-512.yaml" "$TMP/api-factory-512.json"
api_memory_is "candidate to candidate --reuse-values: the explicit 512Mi API memory limit was lost" \
  "$TMP/api-c2c-factory-512.yaml" 512Mi
echo "OK: API memory limit is 2Gi on a factory install, 512Mi otherwise, an explicit value wins, and released values keep ${released_api_memory:-their profile limit}; schema-migrate matches the API"

# --- negative control: revert #3544's connectorCaller guard ------------------
# #3544 turned `.Values.connectorCaller.<field>` into
# `(get (.Values.connectorCaller | default dict) "<field>")`. Put the direct
# dereference back in a copy and the v0.10.3 render must die on it.
if grep -q '^connectorCaller:' "$TMP/released-$REGRESSION_VERSION/curie/values.yaml"; then
  fail "v$REGRESSION_VERSION already defines connectorCaller, so it cannot hold the negative control"
fi
control="$TMP/control"
reuse_chart "$REGRESSION_VERSION" "$control"
reverted="$(python3 - "$control/templates" <<'PY'
import pathlib, re, sys

guard = re.compile(r'\(get \(\.Values\.connectorCaller \| default dict\) "([A-Za-z]+)"\)')
count = 0
for path in pathlib.Path(sys.argv[1]).rglob("*"):
    if path.suffix not in {".yaml", ".tpl"}:
        continue
    text = path.read_text()
    new, n = guard.subn(r".Values.connectorCaller.\1", text)
    if n:
        path.write_text(new)
        count += n
print(count)
PY
)"
[[ "$reverted" -gt 0 ]] \
  || fail "negative control is inert: no #3544 connectorCaller guard was found to revert"
if render upgrade "$control" "$TMP/control.yaml"; then
  fail "negative control rendered: with #3544's connectorCaller guard reverted ($reverted sites), the v$REGRESSION_VERSION --reuse-values render still passed, so this gate cannot see a missing top level key"
fi
grep -Eq 'nil pointer evaluating .*\.(existingSecret|verifyKeyKey|previousVerifyKey|signingKey|verifyKey)' "$TMP/control.yaml.err" \
  || { sed 's/^/    /' "$TMP/control.yaml.err" >&2; fail "negative control failed for a reason other than the nil connectorCaller dereference"; }
echo "OK: negative control: reverting #3544's connectorCaller guard ($reverted sites) fails the v$REGRESSION_VERSION render:"
sed -n '1p' "$TMP/control.yaml.err" | sed 's/^/    /'

printf '%s\n' "Reuse-values render assertions passed for v$LATEST_VERSION and v$REGRESSION_VERSION."
