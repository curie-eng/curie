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
