#!/usr/bin/env bash
# Assert fullname-derived Service, StatefulSet and install or upgrade Job names
# fail at render once they pass 63 characters, and that fitting renders match
# a base chart when asked.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$CHART/../.." && pwd)"
WIDE="$CHART/ci/fixtures/release-name-length-wide.yaml"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if ! command -v helm >/dev/null 2>&1; then
  echo "error: helm is required" >&2
  exit 1
fi

fail() { echo "FAIL $*" >&2; exit 1; }

render_to() {
  local dest="$1"
  shift
  if helm template "$@" >"$dest" 2>"$dest.err"; then
    return 0
  fi
  return 1
}

# Prints kind, name, suffix for checked objects. Args: yaml-file fullname
inventory() {
  python3 - "$1" "$2" <<'PY'
import sys
from pathlib import Path
import yaml
path, fullname = sys.argv[1], sys.argv[2]
prefix = fullname + "-"
events = {"pre-install", "post-install", "pre-upgrade", "post-upgrade"}
docs = [d for d in yaml.safe_load_all(Path(path).read_text()) if isinstance(d, dict)]
for doc in docs:
    kind = doc.get("kind")
    if kind not in {"Service", "StatefulSet", "Job"}:
        continue
    meta = doc.get("metadata") or {}
    name = meta.get("name") or ""
    if not name.startswith(prefix):
        continue
    if kind == "Job":
        hook = ((meta.get("annotations") or {}).get("helm.sh/hook") or "").strip()
        if hook:
            tokens = {part.strip() for part in hook.split(",") if part.strip()}
            if tokens.isdisjoint(events):
                continue
    print(f"{kind}\t{name}\t{name[len(fullname):]}")
PY
}

assert_pass_bounds() {
  local yaml="$1" fullname="$2"
  python3 - "$yaml" "$fullname" <<'PY'
import sys
from pathlib import Path
import yaml
path, fullname = sys.argv[1], sys.argv[2]
prefix = fullname + "-"
events = {"pre-install", "post-install", "pre-upgrade", "post-upgrade"}
docs = [d for d in yaml.safe_load_all(Path(path).read_text()) if isinstance(d, dict)]
checked = []
fixed = set()
for doc in docs:
    kind = doc.get("kind")
    meta = doc.get("metadata") or {}
    name = meta.get("name") or ""
    if kind == "Service" and name in {"agent-sandbox-controller", "agent-sandbox-webhook-service"}:
        fixed.add(name)
    if kind not in {"Service", "StatefulSet", "Job"} or not name.startswith(prefix):
        continue
    if kind == "Job":
        hook = ((meta.get("annotations") or {}).get("helm.sh/hook") or "").strip()
        if hook:
            tokens = {part.strip() for part in hook.split(",") if part.strip()}
            if tokens.isdisjoint(events):
                continue
    checked.append((kind, name, name[len(fullname):]))
missing = {"agent-sandbox-controller", "agent-sandbox-webhook-service"} - fixed
if missing:
    raise SystemExit("missing fixed Services: " + ", ".join(sorted(missing)))
if not checked:
    raise SystemExit("no checked names")
if any(len(name) > 63 for _, name, _ in checked):
    raise SystemExit("checked name over 63 in a passing render")
exact = [item for item in checked if len(item[1]) == 63]
if not exact:
    raise SystemExit("no checked name is exactly 63")
for kind, name, suffix in exact:
    print(f"{kind}\t{suffix}")
PY
}

# Cumulative helm --set args live in SETS.
SETS=()

disable_for() {
  local suffix="$1"
  case "$suffix" in
    -mail-persistence-preflight)
      SETS+=(--set mailAdapter.persistence.existingClaim=null)
      ;;
    -langfuse-model-pricing)
      SETS+=(--set langfuse.modelPricing.enabled=false)
      ;;
    -grafana-token-updater)
      SETS+=(--set grafanaConnector.enabled=false)
      ;;
    -upgrade-drain-release)
      SETS+=(--set worker.upgradeDrain.enabled=false)
      ;;
    -preflight-controller)
      SETS+=(--set preflights.controllerReady.enabled=false)
      ;;
    -preflight-gvisor)
      SETS+=(--set security.gvisorPreflight.enabled=false)
      ;;
    -otel-collector)
      SETS+=(--set otelCollector.deploy=false)
      ;;
    -schema-migrate)
      SETS+=(--set api.migrate.enabled=false)
      ;;
    -preflight-avx)
      SETS+=(--set preflights.avxCheck.enabled=false)
      ;;
    -langfuse-web)
      SETS+=(--set langfuse.deploy=false --set langfuse.host=https://langfuse.example.test)
      ;;
    -mail-adapter)
      SETS+=(--set mailAdapter.deploy=false)
      ;;
    -rustfs-init)
      SETS+=(--set rustfs.deploy=false --set rustfs.host=rustfs.example.test --set rustfs.port=443 --set security.networkPolicy.enabled=false)
      ;;
    -clickhouse)
      SETS+=(--set clickhouse.deploy=false --set clickhouse.host=clickhouse.example.test)
      ;;
    -inference)
      SETS+=(--set inference.deploy=false)
      ;;
    -postgres)
      SETS+=(--set postgres.deploy=false --set postgres.host=postgres.example.test)
      ;;
    -valkey)
      SETS+=(--set valkey.deploy=false --set valkey.host=valkey.example.test)
      ;;
    -api)
      SETS+=(--set api.deploy=false --set ui.apiBaseUrl=https://api.example.test)
      ;;
    -ui)
      SETS+=(--set ui.deploy=false)
      ;;
    *)
      fail "no disable values for suffix $suffix"
      ;;
  esac
}

repeat_char() {
  python3 -c 'import sys; print(sys.argv[1] * int(sys.argv[2]))' "$1" "$2"
}

run_ladder() {
  : >"$TMP/exact63"
  local step=0
  while true; do
    step=$((step + 1))
    if (( step > 19 )); then
      fail "peel ladder did not empty the inventory in 18 steps"
    fi
    render_to "$TMP/probe.yaml" probe "$CHART" -n curie-qa20261017 -f "$WIDE" --set fullnameOverride=q --is-upgrade "${SETS[@]}" \
      || fail "probe render failed at step $step: $(cat "$TMP/probe.yaml.err")"
    mapfile -t rows < <(inventory "$TMP/probe.yaml" q)
    if (( ${#rows[@]} == 0 )); then
      if (( step != 19 )); then
        : # empty inventory ends the ladder; step already incremented past the last object
      fi
      break
    fi
    local max=0
    for row in "${rows[@]}"; do
      IFS=$'\t' read -r rkind rname rsuffix <<<"$row"
      local len=${#rsuffix}
      if (( len > max )); then
        max=$len
      fi
    done
    local fit=$((63 - max))
    local over=$((64 - max))
    if (( fit < 1 )); then
      fail "step $step suffix length $max leaves no room for a 63 character name"
    fi
    local fit_name over_name
    fit_name="$(repeat_char a "$fit")"
    over_name="$(repeat_char b "$over")"
    local suffix="" kind=""
    for mode in install upgrade; do
      local upgrade_flag=()
      if [[ "$mode" == upgrade ]]; then
        upgrade_flag=(--is-upgrade)
      fi
      render_to "$TMP/fit.yaml" fit "$CHART" -n curie-qa20261017 -f "$WIDE" --set "fullnameOverride=$fit_name" "${upgrade_flag[@]}" "${SETS[@]}" \
        || fail "step $step $mode fit render failed: $(cat "$TMP/fit.yaml.err")"
      assert_pass_bounds "$TMP/fit.yaml" "$fit_name" >>"$TMP/exact63"
      render_to "$TMP/over.yaml" over "$CHART" -n curie-qa20261017 -f "$WIDE" --set "fullnameOverride=$over_name" "${upgrade_flag[@]}" "${SETS[@]}" \
        && fail "step $step $mode over-limit render succeeded"
      local err parsed_kind parsed_name parsed_suffix
      err="$(cat "$TMP/over.yaml.err")"
      parsed_kind="$(python3 -c 'import re,sys; m=re.search(r"(Service|StatefulSet|Job) name \"([^\"]+)\" is 64 characters", sys.stdin.read()); print(m.group(1) if m else "")' <<<"$err")"
      parsed_name="$(python3 -c 'import re,sys; m=re.search(r"(Service|StatefulSet|Job) name \"([^\"]+)\" is 64 characters", sys.stdin.read()); print(m.group(2) if m else "")' <<<"$err")"
      [[ -n "$parsed_kind" ]] || fail "step $step $mode stderr did not name a 64 character object: $err"
      parsed_suffix="${parsed_name#"$over_name"}"
      [[ ${#parsed_suffix} -eq $max ]] || fail "step $step $mode named $parsed_name whose suffix is not length $max"
      if [[ -z "$suffix" ]]; then
        suffix="$parsed_suffix"
        kind="$parsed_kind"
      elif [[ "$suffix" != "$parsed_suffix" || "$kind" != "$parsed_kind" ]]; then
        fail "step $step modes named different objects: $kind $suffix and $parsed_kind $parsed_suffix"
      fi
    done
    if (( step == 1 )); then
      printf '%s\n' "${rows[@]}" >"$TMP/first-inventory"
    fi
    disable_for "$suffix"
  done
  if (( step != 19 )); then
    fail "expected 18 peel steps before an empty inventory, got $((step - 1))"
  fi
  python3 - "$TMP/first-inventory" "$TMP/exact63" <<'PY'
import sys
from pathlib import Path
first = Path(sys.argv[1]).read_text().splitlines()
exact = {tuple(line.split("\t")) for line in Path(sys.argv[2]).read_text().splitlines() if line}
exempt = {
    ("Job", "-upgrade-drain"),
    ("Job", "-upgrade-drain-attest"),
    ("Job", "-upgrade-drain-hold"),
    ("Service", "-rustfs"),
    ("StatefulSet", "-rustfs"),
}
missing = []
for row in first:
    kind, _name, suffix = row.split("\t")
    key = (kind, suffix)
    if key in exempt:
        continue
    if key not in exact:
        missing.append(f"{kind} {suffix}")
if missing:
    raise SystemExit("first inventory objects never rendered at 63: " + ", ".join(missing))
print(f"peel ladder passed with {len(first)} first-step objects")
PY
}

run_cases() {
  local rel="qa-longname-tenant-production-eu-west-1-blue01-abcdef"
  if render_to "$TMP/long.yaml" "$rel" "$CHART" -n curie-qa20261017; then
    fail "53 character release rendered"
  fi
  local err
  err="$(cat "$TMP/long.yaml.err")"
  grep -q 'Job name "qa-longname-tenant-production-eu-west-1-blue01-abcdef-curie-upgrade-drain" is 73 characters' <<<"$err" \
    || fail "long release stderr missing the 73 character Job: $err"
  grep -q 'at most 63' <<<"$err" || fail "long release stderr missing the limit"

  local forty_one="qa-release-name-length-forty-one-ab"
  local forty_two="qa-release-name-length-forty-two-abc"
  for mode in install upgrade; do
    local flag=()
    [[ "$mode" == upgrade ]] && flag=(--is-upgrade)
    render_to "$TMP/one.yaml" "$forty_one" "$CHART" --set langfuse.modelPricing.enabled=false "${flag[@]}" \
      || fail "41 character fullname failed in $mode: $(cat "$TMP/one.yaml.err")"
    if render_to "$TMP/two.yaml" "$forty_two" "$CHART" --set langfuse.modelPricing.enabled=false "${flag[@]}"; then
      fail "42 character fullname rendered in $mode"
    fi
    err="$(cat "$TMP/two.yaml.err")"
    grep -q 'Job name ".*-upgrade-drain-release" is 64 characters' <<<"$err" \
      || fail "42 character fullname stderr missing the drain release Job in $mode: $err"
  done

  local decision
  decision="curie-$(repeat_char a 43)"
  local decision_sets=(
    --set otelCollector.deploy=false
    --set api.migrate.enabled=false
    --set preflights.controllerReady.enabled=false
    --set worker.upgradeDrain.enabled=false
    --set langfuse.modelPricing.enabled=false
  )
  for mode in install upgrade; do
    local flag=()
    [[ "$mode" == upgrade ]] && flag=(--is-upgrade)
    render_to "$TMP/decision.yaml" "$decision" "$CHART" "${decision_sets[@]}" "${flag[@]}" \
      || fail "decision 3 release failed in $mode: $(cat "$TMP/decision.yaml.err")"
    python3 - "$TMP/decision.yaml" "$decision" <<'PY'
import sys
from pathlib import Path
import yaml
path, release = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(Path(path).read_text()) if isinstance(d, dict)]
job_name = release + "-security-probe"
jobs = [
    d for d in docs
    if d.get("kind") == "Job" and (d.get("metadata") or {}).get("name") == job_name
]
if len(jobs) != 1:
    raise SystemExit(f"expected one {job_name} Job, found {len(jobs)}")
hook = ((jobs[0].get("metadata") or {}).get("annotations") or {}).get("helm.sh/hook")
if hook != "test":
    raise SystemExit(f"security probe hook is {hook!r}")
if len(job_name) != 64:
    raise SystemExit(f"security probe name length is {len(job_name)}")
long_netpol = [
    (d.get("metadata") or {}).get("name")
    for d in docs
    if d.get("kind") == "NetworkPolicy" and len((d.get("metadata") or {}).get("name") or "") > 63
]
if not long_netpol:
    raise SystemExit("no NetworkPolicy name longer than 63")
print("decision 3 release kept the test Job and a long NetworkPolicy")
PY
  done
  run_ladder
  echo "release name length assertions passed"
}

mask_render() {
  python3 - "$1" <<'PY'
import sys
from pathlib import Path
import yaml
docs = list(yaml.safe_load_all(Path(sys.argv[1]).read_text()))
def walk(node):
    if isinstance(node, dict):
        if node.get("kind") == "Secret":
            for field in ("data", "stringData"):
                values = node.get(field)
                if isinstance(values, dict):
                    for key in values:
                        values[key] = "MASKED"
        annotations = node.get("annotations")
        if isinstance(annotations, dict):
            for key in list(annotations):
                if str(key).startswith("checksum/"):
                    annotations[key] = "MASKED"
        if node.get("name") == "CURIE_INSTALLATION_ID" and "value" in node:
            node["value"] = "MASKED"
        for value in node.values():
            walk(value)
    elif isinstance(node, list):
        for item in node:
            walk(item)
for doc in docs:
    walk(doc)
yaml.safe_dump_all(docs, sys.stdout, sort_keys=True)
PY
}

run_compare() {
  local ref="$1"
  git -C "$ROOT" archive --format=tar "$ref" charts/curie | tar -x -C "$TMP"
  local base="$TMP/charts/curie"
  [[ -f "$base/Chart.yaml" ]] || fail "base chart missing at $ref"
  local cases=(
    "curie||"
    "curie|-f|$WIDE"
    "curie-qa20261017||"
    "curie-qa20261017|-f|$WIDE"
    "qa-release-name-length-forty-one-ab|--set|langfuse.modelPricing.enabled=false"
    "DECISION||"
  )
  local decision
  decision="curie-$(repeat_char a 43)"
  local n=0
  for spec in "${cases[@]}"; do
    IFS='|' read -r rel flag extra <<<"$spec"
    local args=()
    if [[ "$rel" == DECISION ]]; then
      rel="$decision"
      args=(
        --set otelCollector.deploy=false
        --set api.migrate.enabled=false
        --set preflights.controllerReady.enabled=false
        --set worker.upgradeDrain.enabled=false
        --set langfuse.modelPricing.enabled=false
      )
    elif [[ -n "$flag" ]]; then
      args=("$flag" "$extra")
    fi
    for mode in install upgrade; do
      n=$((n + 1))
      local upgrade_flag=()
      [[ "$mode" == upgrade ]] && upgrade_flag=(--is-upgrade)
      render_to "$TMP/cand.yaml" "$rel" "$CHART" "${args[@]}" "${upgrade_flag[@]}" \
        || fail "candidate compare render $n failed: $(cat "$TMP/cand.yaml.err")"
      render_to "$TMP/base.yaml" "$rel" "$base" "${args[@]}" "${upgrade_flag[@]}" \
        || fail "base compare render $n failed: $(cat "$TMP/base.yaml.err")"
      mask_render "$TMP/cand.yaml" >"$TMP/cand.mask"
      mask_render "$TMP/base.yaml" >"$TMP/base.mask"
      if ! cmp -s "$TMP/cand.mask" "$TMP/base.mask"; then
        diff -u "$TMP/base.mask" "$TMP/cand.mask" | head -80 >&2 || true
        fail "comparison $n differs for $rel $mode"
      fi
      echo "comparison $n identical: $rel $mode"
    done
  done
  if (( n != 12 )); then
    fail "expected 12 comparisons, ran $n"
  fi
}

if [[ "${1:-}" == "--compare-base" ]]; then
  [[ -n "${2:-}" ]] || fail "--compare-base requires a git ref"
  run_compare "$2"
else
  [[ $# -eq 0 ]] || fail "unknown arguments: $*"
  run_cases
fi
rm -rf "$TMP"
trap - EXIT
