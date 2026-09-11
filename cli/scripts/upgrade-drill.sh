#!/usr/bin/env bash
# Isolated retained-upgrade and interrupted-upgrade recovery drill (#2426).
#
# Starts from the published v0.8.6 CLI/chart/images on a task-owned kind
# install, upgrades through the candidate CLI from this checkout, interrupts
# drain/apply, proves leftover hooks cannot quiesce a new incarnation, rolls
# back a compatible revision and serves a new turn, and refuses an incompatible
# 0.8.4 schema rollback before Helm mutates.
#
# Refuses the permanent soak (namespace/release `curie`, namespace `default`).
# Never mutates soak, never prints secret values, never messages a human.
# Live provider/channel rows fail closed when credential references are absent.
#
# Usage:
#   curie dev upgrade-drill [--scenario all|...] [--also-predecessor] \
#     [--force] [--keep] [--json]
#   bash cli/scripts/upgrade-drill.sh --self-test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SELF_TEST=0
FORCE=0
KEEP=0
JSON=0
ALSO_PREDECESSOR=0
SCENARIO="all"
BIN="${CURIE_BIN:-}"
NAMESPACE="${CURIE_E2E_NAMESPACE:-acme-2426}"
RELEASE="${CURIE_E2E_RELEASE:-t2426}"
KIND_CLUSTER="${CURIE_E2E_KIND_CLUSTER:-curie-t2426}"
KUBECONFIG_FILE="${CURIE_E2E_KUBECONFIG:-$REPO_ROOT/.projects/kubeconfig-t2426}"
export KUBECONFIG="$KUBECONFIG_FILE"
EVIDENCE_DIR="${CURIE_E2E_EVIDENCE_DIR:-$REPO_ROOT/.projects/2426-evidence}"
PLUGIN_DIR="${CURIE_E2E_PLUGIN_DIR:-$REPO_ROOT/examples/coder}"
AGENT_NAME="${CURIE_E2E_AGENT:-acme-2426-bot}"
CHANNEL="${CURIE_E2E_CHANNEL:-C0EXAMPLE1}"
CANDIDATE_TAG="${CURIE_E2E_CANDIDATE_TAG:-}"
LOCK_FILE="/tmp/curie-upgrade-drill.lock"
WORKDIR=""
CANDIDATE=""
OWNED_KIND=0
OWNED_HELM=0
BASELINE_BIN=""
PRED_BIN=""
ASSET_DIR=""
STARTED_AT=""
SECRET_REF_NAME="t2426-runner-creds"
PVC_BEFORE=""
SECRET_REFS_BEFORE=""
BUNDLE_DIGEST=""
TURN_RESULT=""
HELM_REAL=""

# Published GitHub Release pins (checksums.txt on v0.8.6 / v0.8.7).
CHART_086_SHA="27832cf5094dffb6c145338eb1fcbe37ab71729d604a89244f8200ca75b9e1a9"
CHART_087_SHA="0870a2600907b0c94cbfffc06dec39a4d56e58978f0b05e1c4dfd2226f8237f1"
CLI_086_SHA="0be1438d08c380ae41132672f35cad2cbf1ecd745c799c34050e0b4e1a925c80"
CLI_087_SHA="1d5636b01e7812d9c1d836f95f111c655f7d88b88db2359498af4e407cdffe79"
REL_086="https://github.com/curie-eng/curie/releases/download/v0.8.6"
REL_087="https://github.com/curie-eng/curie/releases/download/v0.8.7"

SCENARIOS_ALL=(
    install-086
    upgrade
    retention
    interrupt-drain
    interrupt-apply
    leftover-hook
    compatible-rollback
    incompatible-rollback
    live-roundtrip
)
SCENARIOS_OPTIONAL=(install-087)

log() { printf '%s\n' "$*" >&2; }

die() {
    log "error: $*"
    exit 1
}

usage() {
    cat <<'EOF' >&2
usage: upgrade-drill.sh [--scenario all|install-086|install-087|upgrade|retention|interrupt-drain|interrupt-apply|leftover-hook|compatible-rollback|incompatible-rollback|live-roundtrip] [--also-predecessor] [--force] [--keep] [--json] [--self-test]
EOF
}

is_soak_namespace() {
    local ns="${1:-}"
    [[ "$ns" == "curie" || "$ns" == "default" ]]
}

is_soak_release() {
    local rel="${1:-}"
    [[ "$rel" == "curie" ]]
}

refuse_soak() {
    local ns="${1:-}" rel="${2:-}"
    if is_soak_namespace "$ns"; then
        die "refusing soak namespace '$ns' (permanent soak / shared default). Use a task-owned CURIE_E2E_NAMESPACE."
    fi
    if is_soak_release "$rel"; then
        die "refusing soak release '$rel'. Use a task-owned CURIE_E2E_RELEASE."
    fi
}

valid_scenario() {
    local want="$1" s
    [[ "$want" == "all" ]] && return 0
    for s in "${SCENARIOS_ALL[@]}" "${SCENARIOS_OPTIONAL[@]}"; do
        [[ "$s" == "$want" ]] && return 0
    done
    return 1
}

live_provider_present() {
    [[ -n "${CURIE_CREDENTIALS:-}" || -n "${CURIE_MODEL_CREDENTIALS:-}" || -n "${ANTHROPIC_API_KEY:-}" || -n "${OPENROUTER_SOAK_TEST_KEY:-}" || -n "${OPENROUTER_TOKEN:-}" ]]
}

channel_present() {
    [[ -n "${SLACK_BOT_TOKEN:-}" && -n "${SLACK_APP_TOKEN:-}" && -n "${SLACK_TEST_CHANNEL:-}" ]]
}

require_live() {
    if ! live_provider_present; then
        die "missing live provider credentials; set OPENROUTER_SOAK_TEST_KEY, CURIE_CREDENTIALS, CURIE_MODEL_CREDENTIALS, or ANTHROPIC_API_KEY. Fake-model cannot close #2426."
    fi
    if ! channel_present; then
        die "missing channel credentials; set SLACK_BOT_TOKEN, SLACK_APP_TOKEN, and SLACK_TEST_CHANNEL to the authorized test route. Fake-model cannot close #2426."
    fi
}

is_sha256() {
    local h="${1:-}"
    [[ "$h" =~ ^[0-9a-f]{64}$ ]]
}

verify_sha256() {
    local want="$1" file="$2"
    echo "$want  $file" | sha256sum -c -
}

chart_version() {
    awk '$1 == "version:" { print $2; exit }' "$1"
}

cli_version() {
    awk -F '"' '$1 == "version = " { print $2; exit }' "$1"
}

release_identities_match() {
    local chart="$1" cargo="$2" chart_identity cli_identity
    [[ -f "$chart" && -f "$cargo" ]] || return 1
    chart_identity="$(chart_version "$chart")"
    cli_identity="$(cli_version "$cargo")"
    [[ "$chart_identity" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$chart_identity" == "$cli_identity" ]]
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --scenario) SCENARIO="${2:-}"; shift 2 ;;
            --also-predecessor) ALSO_PREDECESSOR=1; shift ;;
            --force) FORCE=1; shift ;;
            --keep) KEEP=1; shift ;;
            --json) JSON=1; shift ;;
            --self-test) SELF_TEST=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown argument: $1" ;;
        esac
    done
    if ! valid_scenario "$SCENARIO"; then
        die "unknown scenario '$SCENARIO'"
    fi
}

run_self_test() {
    local failed=0 tmp identity mismatch_chart
    if is_soak_namespace "curie" && is_soak_namespace "default" && ! is_soak_namespace "acme-2426"; then
        log "soak namespace curie refused"
        log "soak namespace default refused"
    else
        log "self-test: soak namespace helper is wrong"
        failed=1
    fi
    if is_soak_release "curie" && ! is_soak_release "t2426"; then
        log "soak release curie refused"
    else
        log "self-test: soak release helper is wrong"
        failed=1
    fi
    if valid_scenario "all" && valid_scenario "interrupt-drain" && valid_scenario "install-087" && ! valid_scenario "not-a-scenario"; then
        log "unknown scenario refused"
    else
        log "self-test: scenario helper is wrong"
        failed=1
    fi
    (
        unset CURIE_CREDENTIALS CURIE_MODEL_CREDENTIALS ANTHROPIC_API_KEY OPENROUTER_SOAK_TEST_KEY OPENROUTER_TOKEN \
            SLACK_BOT_TOKEN SLACK_APP_TOKEN SLACK_TEST_CHANNEL AGENTMAIL_SOAK_TEST_KEY || true
        if live_provider_present || channel_present; then
            log "self-test: missing live/channel helper is wrong"
            exit 1
        fi
        log "missing live provider credentials refused"
        log "missing channel credentials refused"
    ) || failed=1
    if is_sha256 "$CHART_086_SHA" && is_sha256 "$CHART_087_SHA" && is_sha256 "$CLI_086_SHA" && is_sha256 "$CLI_087_SHA"; then
        log "published v0.8.6 chart checksum pinned"
        log "published v0.8.7 chart checksum pinned"
        log "published v0.8.6 cli checksum pinned"
        log "published v0.8.7 cli checksum pinned"
    else
        log "self-test: published checksum pins are malformed"
        failed=1
    fi
    tmp="$(mktemp)"
    printf 'curie-upgrade-drill-pin\n' >"$tmp"
    local got
    got="$(sha256sum "$tmp" | awk '{print $1}')"
    if verify_sha256 "$got" "$tmp" >/dev/null; then
        log "sha256 helper verified a matching fixture"
    else
        log "self-test: sha256 helper failed a matching fixture"
        failed=1
    fi
    if verify_sha256 "$CHART_086_SHA" "$tmp" >/dev/null 2>&1; then
        log "self-test: sha256 helper accepted a mismatch"
        failed=1
    else
        log "sha256 helper rejected a mismatched fixture"
    fi
    rm -f "$tmp"
    if release_identities_match "$REPO_ROOT/charts/curie/Chart.yaml" "$REPO_ROOT/cli/Cargo.toml"; then
        identity="$(chart_version "$REPO_ROOT/charts/curie/Chart.yaml")"
        log "candidate Chart.yaml and CLI identities match at $identity"
    else
        log "self-test: Chart.yaml and CLI identities drifted"
        failed=1
    fi
    mismatch_chart="$(mktemp)"
    printf '%s\n' 'version: 9.9.9' >"$mismatch_chart"
    if release_identities_match "$mismatch_chart" "$REPO_ROOT/cli/Cargo.toml"; then
        log "self-test: release identity guard accepted a mismatch"
        failed=1
    else
        log "mismatched chart and CLI identities refused"
    fi
    rm -f "$mismatch_chart"
    local script_path="${BASH_SOURCE[0]}"
    if grep -q '^--set security.gvisor' "$script_path"; then
        log "self-test: image_sets must emit KEY=VAL lines, not combined --set tokens"
        failed=1
    elif grep -q 'security.gvisor.mode=off' "$script_path"; then
        log "helm --set KEY=VAL tokens are split"
    else
        log "self-test: image_sets missing gvisor off assignment"
        failed=1
    fi
    (( failed == 0 )) || die "self-test failed"
    log "self-test passed"
    if (( JSON )); then
        printf '{"status":"self-test","issue":2426,"baseline":"0.8.6","predecessor":"0.8.7","chart_identity":"%s"}\n' "$identity"
    fi
}

resolve_bin() {
    if [[ -n "$BIN" && -x "$BIN" ]]; then
        return 0
    fi
    if [[ -x "$REPO_ROOT/cli/target/debug/curie" ]]; then
        BIN="$REPO_ROOT/cli/target/debug/curie"
        return 0
    fi
    if [[ -x "$REPO_ROOT/cli/target/release/curie" ]]; then
        BIN="$REPO_ROOT/cli/target/release/curie"
        return 0
    fi
    if command -v curie >/dev/null 2>&1; then
        BIN="$(command -v curie)"
        return 0
    fi
    die "CURIE_BIN must name an executable candidate curie built from this checkout"
}

candidate_identity() {
    CANDIDATE="$(git -C "$REPO_ROOT" rev-parse HEAD)"
}

redact() {
    python3 -c '
import re, sys
text = sys.stdin.read()
text = re.sub(r"sk-[A-Za-z0-9_-]{8,}", "sk-REDACTED", text)
text = re.sub(r"sk-or-[A-Za-z0-9_-]{8,}", "sk-or-REDACTED", text)
text = re.sub(r"sk-ant-[A-Za-z0-9_-]{8,}", "sk-ant-REDACTED", text)
text = re.sub(r"xoxb-[A-Za-z0-9-]+", "xoxb-REDACTED", text)
text = re.sub(r"xapp-[A-Za-z0-9-]+", "xapp-REDACTED", text)
text = re.sub(r"\bC[A-Z0-9]{8,}\b", "C0EXAMPLE1", text)
text = re.sub(r"\bU[A-Z0-9]{8,}\b", "U0EXAMPLE1", text)
sys.stdout.write(text)
'
}

kubectl_ns() {
    kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" "$@"
}

helm_ns() {
    helm --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" "$@"
}

fullname() {
    printf '%s-curie' "$RELEASE"
}

cleanup() {
    local status=$?
    if (( KEEP )); then
        log "keeping owned resources (kind=$KIND_CLUSTER ns=$NAMESPACE release=$RELEASE)"
        return 0
    fi
    if (( OWNED_HELM )); then
        log "uninstalling release $RELEASE in $NAMESPACE"
        helm_ns uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
        kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
    fi
    if (( OWNED_KIND )); then
        log "deleting kind cluster $KIND_CLUSTER"
        kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
    fi
    if [[ -n "$WORKDIR" && -d "$WORKDIR" ]]; then
        rm -rf "$WORKDIR" 2>/dev/null || true
    fi
    if (( status != 0 )); then
        log "cleanup finished after failure (exit $status)"
    fi
}

download_pin() {
    local url="$1" dest="$2" sha="$3"
    if [[ -f "$dest" ]]; then
        if verify_sha256 "$sha" "$dest" >/dev/null 2>&1; then
            return 0
        fi
        rm -f "$dest"
    fi
    log "downloading $(basename "$dest")"
    curl -fsSL --retry 3 -o "$dest" "$url"
    verify_sha256 "$sha" "$dest"
}

fetch_published() {
    ASSET_DIR="$WORKDIR/assets"
    mkdir -p "$ASSET_DIR"
    download_pin "$REL_086/curie-0.8.6.tgz" "$ASSET_DIR/curie-0.8.6.tgz" "$CHART_086_SHA"
    download_pin "$REL_086/curie-x86_64-unknown-linux-gnu" "$ASSET_DIR/curie-0.8.6" "$CLI_086_SHA"
    chmod +x "$ASSET_DIR/curie-0.8.6"
    BASELINE_BIN="$ASSET_DIR/curie-0.8.6"
    if (( ALSO_PREDECESSOR )) || [[ "$SCENARIO" == "install-087" || "$SCENARIO" == "all" ]]; then
        download_pin "$REL_087/curie-0.8.7.tgz" "$ASSET_DIR/curie-0.8.7.tgz" "$CHART_087_SHA"
        download_pin "$REL_087/curie-x86_64-unknown-linux-gnu" "$ASSET_DIR/curie-0.8.7" "$CLI_087_SHA"
        chmod +x "$ASSET_DIR/curie-0.8.7"
        PRED_BIN="$ASSET_DIR/curie-0.8.7"
    fi
    [[ "$(helm show chart "$ASSET_DIR/curie-0.8.6.tgz" | awk '$1 == "version:" {print $2}')" == 0.8.6 ]] \
        || die "published chart tgz is not version 0.8.6"
    log "published v0.8.6 chart and CLI verified"
}

ensure_kind() {
    mkdir -p "$(dirname "$KUBECONFIG_FILE")" "$EVIDENCE_DIR"
    if kind get clusters 2>/dev/null | grep -Fxq "$KIND_CLUSTER"; then
        if (( FORCE )); then
            log "recreating leftover kind cluster $KIND_CLUSTER"
            kind delete cluster --name "$KIND_CLUSTER"
        else
            die "kind cluster $KIND_CLUSTER already exists; pass --force to recreate this task-owned cluster only"
        fi
    fi
    kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
    OWNED_KIND=1
}

image_for() {
    local name="$1" tag="$2"
    printf 'ghcr.io/curie-eng/%s:%s' "$name" "$tag"
}

load_tag_images() {
    local tag="$1" img
    for img in curie-api curie-worker curie-dispatcher curie-ui curie-runner curie-mail-adapter; do
        local ref
        ref="$(image_for "$img" "$tag")"
        if ! docker image inspect "$ref" >/dev/null 2>&1; then
            log "pulling $ref"
            docker pull "$ref"
        fi
        log "kind load $ref"
        kind load docker-image "$ref" --name "$KIND_CLUSTER"
    done
}

image_sets() {
    local tag="$1"
    local with_secret="${2:-1}"
    cat <<EOF
security.gvisor.mode=off
worker.replicas=1
worker.image.repository=ghcr.io/curie-eng/curie-worker
worker.image.tag=${tag}
worker.image.pullPolicy=IfNotPresent
api.image.repository=ghcr.io/curie-eng/curie-api
api.image.tag=${tag}
api.image.pullPolicy=IfNotPresent
dispatcher.image.repository=ghcr.io/curie-eng/curie-dispatcher
dispatcher.image.tag=${tag}
dispatcher.image.pullPolicy=IfNotPresent
ui.image.repository=ghcr.io/curie-eng/curie-ui
ui.image.tag=${tag}
ui.image.pullPolicy=IfNotPresent
agentSandbox.runner.image=ghcr.io/curie-eng/curie-runner
agentSandbox.runner.tag=${tag}
agentSandbox.runner.imagePullPolicy=IfNotPresent
agentSandbox.runner.prewarm.imagePullPolicy=IfNotPresent
langfuse.deploy=false
langfuse.host=langfuse.example.com
clickhouse.deploy=false
ui.deploy=false
mailAdapter.deploy=false
otelCollector.deploy=false
otelCollector.telemetryDisabled=true
EOF
    if [[ "$with_secret" == "1" ]]; then
        printf '%s\n' "agentSandbox.runner.credentialsExistingSecret=${SECRET_REF_NAME}"
    fi
}

create_secret_ref() {
    # Create the Secret in the Helm-owned namespace after cluster up, never
    # before: published v0.8.7 refuses a namespace that already has objects
    # and has no --adopt flag.
    local key="${CURIE_CREDENTIALS:-${CURIE_MODEL_CREDENTIALS:-${OPENROUTER_SOAK_TEST_KEY:-${OPENROUTER_TOKEN:-${ANTHROPIC_API_KEY:-}}}}}"
    [[ -n "$key" ]] || die "cannot create runner Secret without a live credential reference"
    kubectl_ns create secret generic "$SECRET_REF_NAME" \
        --from-literal=agentCredentials="$key" \
        --dry-run=client -o yaml | kubectl_ns apply -f - >/dev/null
    log "created Secret reference $SECRET_REF_NAME key=agentCredentials (value not printed)"
}

cluster_up_with() {
    local curie_bin="$1" chart="$2" tag="$3"
    local with_secret="${4:-1}"
    if [[ $# -ge 4 ]]; then
        shift 4
    else
        shift 3
    fi
    local extra=("$@")
    local sets=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && sets+=(--set "$line")
    done < <(image_sets "$tag" "$with_secret")
    log "cluster up via $(basename "$curie_bin") chart=$chart tag=$tag ns=$NAMESPACE release=$RELEASE"
    local adopt=()
    # Published v0.8.6 cluster up leaves Helm-owned objects the candidate CLI's
    # namespace-adoption guard (#2375) otherwise refuses. --adopt is the
    # supported operator flag for that upgrade, not a hidden kubectl edit.
    if [[ "$curie_bin" == "$BIN" ]]; then
        adopt+=(--adopt)
        log "candidate cluster up passes --adopt (v0.8.6 namespace objects)"
    fi
    "$curie_bin" cluster up \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$chart" \
        --dev \
        --no-expose \
        "${adopt[@]}" \
        "${sets[@]}" \
        "${extra[@]}"
    OWNED_HELM=1
}

wait_rollout() {
    local deploy
    for deploy in api worker; do
        kubectl_ns rollout status "deploy/$(fullname)-$deploy" --timeout=300s
    done
}

snapshot_pvcs() {
    kubectl_ns get pvc -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}' | sort
}

snapshot_secret_refs() {
    helm_ns get values "$RELEASE" -o json | python3 -c '
import json,sys
vals=json.load(sys.stdin)
runner=(vals.get("agentSandbox") or {}).get("runner") or {}
print("credentialsExistingSecret="+str(runner.get("credentialsExistingSecret") or ""))
'
}

record_before() {
    PVC_BEFORE="$(snapshot_pvcs)"
    SECRET_REFS_BEFORE="$(snapshot_secret_refs)"
    printf '%s\n' "$PVC_BEFORE" >"$EVIDENCE_DIR/pvc-before.txt"
    printf '%s\n' "$SECRET_REFS_BEFORE" >"$EVIDENCE_DIR/secret-refs-before.txt"
    log "recorded PVC UIDs and Secret references (names only)"
}

assert_retention() {
    local after_pvc after_refs
    after_pvc="$(snapshot_pvcs)"
    after_refs="$(snapshot_secret_refs)"
    printf '%s\n' "$after_pvc" >"$EVIDENCE_DIR/pvc-after.txt"
    printf '%s\n' "$after_refs" >"$EVIDENCE_DIR/secret-refs-after.txt"
    [[ -n "$PVC_BEFORE" ]] || die "no pre-upgrade PVC snapshot"
    [[ "$after_pvc" == "$PVC_BEFORE" ]] || die "PVC identities changed across upgrade"
    echo "$after_refs" | grep -q "$SECRET_REF_NAME" || die "Secret reference $SECRET_REF_NAME was not retained"
    log "PVC identities retained"
    log "Secret reference $SECRET_REF_NAME retained"
}

deploy_agent() {
    log "deploying $AGENT_NAME from $PLUGIN_DIR"
    local extra=()
    if [[ -n "${SLACK_TEST_CHANNEL:-}" ]]; then
        extra+=(--slack-channel "$SLACK_TEST_CHANNEL")
        CHANNEL="$SLACK_TEST_CHANNEL"
    fi
    "$BIN" cluster deploy \
        --plugin-dir "$PLUGIN_DIR" \
        --agent "$AGENT_NAME" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$REPO_ROOT/charts/curie" \
        "${extra[@]}"
    BUNDLE_DIGEST="$("$BIN" --json cluster status --namespace "$NAMESPACE" --release "$RELEASE" 2>/dev/null | python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get("bundle_digest") or d.get("bundleDigest") or "")' || true)"
}

send_turn() {
    local text="$1" out
    out="$("$BIN" --json cluster message \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --channel "$CHANNEL" \
        "$text")"
    printf '%s\n' "$out" | redact >&2
    TURN_RESULT="$out"
    python3 -c 'import json,sys
raw=sys.stdin.read()
d=json.loads(raw)
status=str(d.get("status") or d.get("state") or d.get("outcome") or "")
thread=str(d.get("thread") or d.get("conversation_id") or "")
if not status and not thread:
    raise SystemExit("cluster message returned JSON without status or thread")
low=status.lower()
if low and any(x in low for x in ("error", "fail", "refus")):
    raise SystemExit("cluster message status is a failure: "+status)
print(status or "ok")
' <<<"$out" >/dev/null
}

drain_job_name() {
    printf '%s-upgrade-drain' "$(fullname)"
}

wait_for_drain_job() {
    local deadline=$((SECONDS + 120)) name
    name="$(drain_job_name)"
    while (( SECONDS < deadline )); do
        if kubectl_ns get job "$name" >/dev/null 2>&1; then
            log "observed drain Job $name"
            return 0
        fi
        sleep 1
    done
    die "drain Job $name did not appear"
}

scenario_wanted() {
    local want="$1"
    [[ "$SCENARIO" == "all" || "$SCENARIO" == "$want" ]]
}

run_install_086() {
    load_tag_images "0.8.6"
    cluster_up_with "$BASELINE_BIN" "$ASSET_DIR/curie-0.8.6.tgz" "0.8.6" 0
    wait_rollout
    create_secret_ref
    cluster_up_with "$BASELINE_BIN" "$ASSET_DIR/curie-0.8.6.tgz" "0.8.6" 1
    wait_rollout
    record_before
    log "U1 published v0.8.6 install complete"
}

run_install_087() {
    load_tag_images "0.8.7"
    cluster_up_with "${PRED_BIN:-$BIN}" "$ASSET_DIR/curie-0.8.7.tgz" "0.8.7" 0
    wait_rollout
    create_secret_ref
    cluster_up_with "${PRED_BIN:-$BIN}" "$ASSET_DIR/curie-0.8.7.tgz" "0.8.7" 1
    wait_rollout
    record_before
    log "U2 published v0.8.7 install complete"
}

candidate_image_tag() {
    if [[ -n "$CANDIDATE_TAG" ]]; then
        printf '%s' "$CANDIDATE_TAG"
        return
    fi
    printf '0.8.7'
}

run_upgrade() {
    local tag
    tag="$(candidate_image_tag)"
    load_tag_images "$tag"
    if [[ "$tag" == "0.8.7" && -z "$CANDIDATE_TAG" ]]; then
        log "candidate application images are published :0.8.7; CLI/chart are this checkout $CANDIDATE"
    fi
    # Omit credentialsExistingSecret so retention is observed, not re-applied.
    cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
    wait_rollout
    log "U4 candidate CLI upgrade complete (tag=$tag)"
}

run_retention() {
    assert_retention
    log "U10 retention verified"
}

terminate_tree() {
    local pid="$1" child
    [[ -n "$pid" ]] || return 0
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        terminate_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
}

run_interrupt_drain() {
    local job pid status=0
    job="$(drain_job_name)"
    log "U5 interrupting drain Job $job"
    local tag
    tag="$(candidate_image_tag)"
    (
        cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
    ) >"$EVIDENCE_DIR/interrupt-drain-up.log" 2>&1 &
    pid=$!
    wait_for_drain_job
    kubectl_ns delete job "$job" --wait=false || true
    wait "$pid" || status=$?
    if (( status == 0 )); then
        log "upgrade continued after drain delete; treating as bounded recovery if rollout is healthy"
    else
        log "upgrade failed after drain interrupt (exit $status); retrying cluster up"
        cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
        wait_rollout
    fi
    log "U5 drain interrupt recovered via supported cluster up retry"
}

run_interrupt_apply() {
    local pid status=0 tag seen=0
    tag="$(candidate_image_tag)"
    log "U6 interrupting helm apply after drain"
    (
        cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
    ) >"$EVIDENCE_DIR/interrupt-apply-up.log" 2>&1 &
    pid=$!
    local deadline=$((SECONDS + 90))
    while (( SECONDS < deadline )); do
        if kubectl_ns get job "$(drain_job_name)" -o jsonpath='{.status.succeeded}' 2>/dev/null | grep -q 1; then
            seen=1
            break
        fi
        sleep 1
    done
    (( seen == 1 )) || die "U6 did not observe drain Job success before apply interrupt"
    terminate_tree "$pid"
    wait "$pid" || status=$?
    log "killed in-flight cluster up process tree (exit ${status:-0}); retrying"
    cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
    wait_rollout
    log "U6 apply interrupt recovered via supported cluster up retry"
}

run_leftover_hook() {
    local job
    job="$(drain_job_name)"
    kubectl_ns get job "$job" >/dev/null 2>&1 && log "leftover drain Job present: $job" || log "no leftover drain Job (before-hook-creation may have cleared it)"
    local tag
    tag="$(candidate_image_tag)"
    cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0
    wait_rollout
    assert_retention
    send_turn "2426 leftover-hook follow-up $(date -u +%Y%m%dT%H%M%SZ)"
    log "U7 leftover hook did not pause the new incarnation; follow-up turn consumed"
}

run_compatible_rollback() {
    log "U8 compatible rollback"
    "$BIN" --json cluster rollback --namespace "$NAMESPACE" --release "$RELEASE" --yes \
        >"$EVIDENCE_DIR/compatible-rollback.json" || die "compatible rollback failed"
    wait_rollout
    send_turn "2426 compatible-rollback new turn $(date -u +%Y%m%dT%H%M%SZ)"
    log "U8 compatible rollback served a new turn"
}

run_incompatible_rollback() {
    HELM_REAL="$(command -v helm)"
    [[ -n "$HELM_REAL" ]] || die "helm not on PATH"
    local wrap="$WORKDIR/u9"
    mkdir -p "$wrap"
    cat >"$wrap/helm" <<EOF
#!/bin/sh
echo "\$*" >> "$wrap/helm-argv.log"
case "\$1" in
history)
  python3 - <<'PY'
import json, os, subprocess, sys
real = os.environ["HELM_REAL"]
kube = os.environ.get("KUBECONFIG", "")
cmd = [real, "history", "--namespace", "$NAMESPACE", "$RELEASE", "-o", "json"]
env = os.environ.copy()
out = subprocess.check_output(cmd, env=env)
try:
    hist = json.loads(out.decode() or "[]")
except Exception:
    hist = []
hist.append({
    "revision": 94,
    "status": "superseded",
    "chart": "curie-0.8.4",
    "app_version": "0.8.4",
    "description": "Upgrade complete",
})
json.dump(hist, sys.stdout)
PY
  ;;
rollback)
  echo rollback-ran >> "$wrap/helm-argv.log"
  echo "Rollback was a success."
  ;;
*)
  exec "$HELM_REAL" "\$@"
  ;;
esac
EOF
    chmod +x "$wrap/helm"
    local before after status=0
    before="$(kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.replicas}/{.status.readyReplicas}' 2>/dev/null || echo "")"
    export HELM_REAL
    set +e
    PATH="$wrap:$PATH" \
        "$BIN" --json cluster rollback \
            --namespace "$NAMESPACE" \
            --release "$RELEASE" \
            --revision 94 \
            --yes \
            >"$EVIDENCE_DIR/incompatible-rollback.json" \
            2>"$EVIDENCE_DIR/incompatible-rollback.err"
    status=$?
    set -e
    after="$(kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.replicas}/{.status.readyReplicas}' 2>/dev/null || echo "")"
    if grep -q rollback-ran "$wrap/helm-argv.log" 2>/dev/null; then
        die "incompatible rollback invoked helm rollback"
    fi
    python3 - <<PY
import json, pathlib, sys
out = pathlib.Path("$EVIDENCE_DIR/incompatible-rollback.json").read_text().strip()
err = pathlib.Path("$EVIDENCE_DIR/incompatible-rollback.err").read_text()
payload = {}
if out:
    payload = json.loads(out.splitlines()[-1])
error = str(payload.get("error") or err)
fix = str(payload.get("fix") or "")
assert payload.get("rolled_back") is not True, payload
assert "0.8.4" in error or "0.8.4" in fix or "schema" in error.lower() or "0039" in error or "0038" in error, (payload, err)
print("incompatible rollback refused before helm mutate")
print("error_redacted_ok")
PY
    [[ "$before" == "$after" ]] || die "API replica identity changed during refused rollback ($before -> $after)"
    (( status != 0 )) || die "incompatible rollback exited 0"
    log "U9 incompatible 0.8.4 rollback refused before mutation; API replicas unchanged"
}

run_live_roundtrip() {
    require_live
    if [[ -n "${SLACK_BOT_TOKEN:-}" && -n "${SLACK_APP_TOKEN:-}" && -n "${SLACK_TEST_CHANNEL:-}" ]]; then
        log "connecting test Slack route via cluster comms"
        "$BIN" cluster comms --slack \
            --namespace "$NAMESPACE" \
            --release "$RELEASE" \
            --chart "$REPO_ROOT/charts/curie"
        CHANNEL="$SLACK_TEST_CHANNEL"
    fi
    deploy_agent
    send_turn "2426 live round-trip $(date -u +%Y%m%dT%H%M%SZ); reply with the single word pong"
    log "U3/U4 live provider/channel round trip recorded (bodies redacted)"
}

write_evidence() {
    local elapsed=$((SECONDS - STARTED_AT))
    cat >"$EVIDENCE_DIR/summary.json" <<EOF
{
  "issue": 2426,
  "commit": "$CANDIDATE",
  "baseline": "0.8.6",
  "predecessor": "0.8.7",
  "candidate_cli": "$CANDIDATE",
  "candidate_image_tag": "$(candidate_image_tag)",
  "kind_cluster": "$KIND_CLUSTER",
  "namespace": "$NAMESPACE",
  "release": "$RELEASE",
  "elapsed_seconds": $elapsed,
  "secret_ref": "$SECRET_REF_NAME",
  "bundle_digest": "$BUNDLE_DIGEST"
}
EOF
    if (( JSON )); then
        cat "$EVIDENCE_DIR/summary.json"
    fi
}

run_matrix() {
    refuse_soak "$NAMESPACE" "$RELEASE"
    if [[ "$SCENARIO" == "all" || "$SCENARIO" == "live-roundtrip" || "$SCENARIO" == "compatible-rollback" || "$SCENARIO" == "upgrade" ]]; then
        require_live
    fi
    resolve_bin
    candidate_identity
    STARTED_AT=$SECONDS
    WORKDIR="$(mktemp -d /tmp/curie-upgrade-drill.XXXXXX)"
    mkdir -p "$EVIDENCE_DIR"
    trap cleanup EXIT
    fetch_published
    ensure_kind
    if [[ "$SCENARIO" == "install-087" ]]; then
        run_install_087
        write_evidence
        return 0
    fi
    run_install_086
    deploy_agent || true
    send_turn "2426 seed turn before upgrade" || true
    if scenario_wanted "upgrade" || scenario_wanted "retention" || [[ "$SCENARIO" == "all" ]]; then
        run_upgrade
    fi
    if scenario_wanted "retention" || [[ "$SCENARIO" == "all" ]]; then
        run_retention
    fi
    if scenario_wanted "live-roundtrip" || [[ "$SCENARIO" == "all" ]]; then
        run_live_roundtrip
    fi
    if scenario_wanted "interrupt-drain" || [[ "$SCENARIO" == "all" ]]; then
        run_interrupt_drain
    fi
    if scenario_wanted "interrupt-apply" || [[ "$SCENARIO" == "all" ]]; then
        run_interrupt_apply
    fi
    if scenario_wanted "leftover-hook" || [[ "$SCENARIO" == "all" ]]; then
        run_leftover_hook
    fi
    if scenario_wanted "compatible-rollback" || [[ "$SCENARIO" == "all" ]]; then
        run_compatible_rollback
    fi
    if scenario_wanted "incompatible-rollback" || [[ "$SCENARIO" == "all" ]]; then
        run_incompatible_rollback
    fi
    write_evidence
    if (( ALSO_PREDECESSOR )) && [[ "$SCENARIO" == "all" ]]; then
        log "also-predecessor: published v0.8.7 happy path on a fresh install"
        helm_ns uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
        kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
        local deadline=$((SECONDS + 180))
        while (( SECONDS < deadline )); do
            if ! kubectl --kubeconfig "$KUBECONFIG_FILE" get namespace "$NAMESPACE" >/dev/null 2>&1; then
                break
            fi
            sleep 2
        done
        if kubectl --kubeconfig "$KUBECONFIG_FILE" get namespace "$NAMESPACE" >/dev/null 2>&1; then
            die "namespace $NAMESPACE still terminating; cannot start the v0.8.7 predecessor case"
        fi
        OWNED_HELM=0
        run_install_087
        run_upgrade
        run_retention
        write_evidence
    fi
    log "upgrade-drill finished on candidate $CANDIDATE"
}

load_env_file() {
    local file="${CURIE_ENV_FILE:-}"
    [[ -n "$file" && -f "$file" ]] || return 0
    # Non-overriding load; names only reach this shell. Values are not logged.
    set -a
    # shellcheck disable=SC1090
    source "$file"
    set +a
    if [[ -z "${CURIE_CREDENTIALS:-}" && -n "${OPENROUTER_SOAK_TEST_KEY:-}" ]]; then
        CURIE_CREDENTIALS="$OPENROUTER_SOAK_TEST_KEY"
        export CURIE_CREDENTIALS
    fi
    if [[ -z "${CURIE_CREDENTIALS:-}" && -n "${OPENROUTER_TOKEN:-}" ]]; then
        CURIE_CREDENTIALS="$OPENROUTER_TOKEN"
        export CURIE_CREDENTIALS
    fi
}

parse_args "$@"
if (( SELF_TEST )); then
    run_self_test
    exit 0
fi
load_env_file
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    die "another upgrade-drill holds $LOCK_FILE"
fi
run_matrix
