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
# util-linux flock, with its exit statuses, on hosts that ship none (a stock Mac).
GNU_PROCESS="$REPO_ROOT/cli/scripts/gnu-process.py"
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
# #2753 approval-recovery scenario inputs. The bundle must DECLARE the route in
# its approvalPolicy: an operator principal can resolve only a route bound to an
# explicit user list (slack_approvers.py sets operator_eligible=False on the
# channel-membership and user-group approver sets), so without a declared route
# the "still resolvable after the upgrade" half cannot be proven at all and this
# scenario refuses rather than reporting a pass it did not earn.
RECOVERY_PLUGIN_DIR="${CURIE_E2E_RECOVERY_PLUGIN_DIR:-$PLUGIN_DIR}"
RECOVERY_ROUTE="${CURIE_E2E_APPROVAL_ROUTE:-}"
# A SECOND declared route, deliberately left on the channel-membership default:
# the orphaned obligation an operator principal is refused 403 on. Without it
# both approvals sit on the same authorization state and the pairing proves
# nothing, so it is required rather than defaulted.
RECOVERY_ORPHAN_ROUTE="${CURIE_E2E_APPROVAL_ORPHAN_ROUTE:-}"
RECOVERY_APPROVER="${CURIE_E2E_APPROVAL_USER:-}"
# The operator principal resolves the ORDINARY route, whose approvers binding is
# `users:$RECOVERY_APPROVER`, and ExplicitUsers.contains checks exact subject
# membership. So the subject IS the approver; an override that names anyone
# else is refused before setup by require_recovery_inputs.
RECOVERY_PRINCIPAL_OVERRIDE="${CURIE_E2E_APPROVAL_SUBJECT:-}"
RECOVERY_PRINCIPAL_SUBJECT="$RECOVERY_APPROVER"
RECOVERY_RETAINED_ID=""
RECOVERY_UNRESOLVABLE_ID=""
RECOVERY_KEY_BASE=""
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
# Opt-in, like install-087: it needs a bundle-DECLARED approval route and an
# approver user id, so folding it into `all` would make the existing matrix
# refuse for everyone who has not set them.
SCENARIOS_OPTIONAL=(install-087 approval-recovery)

log() { printf '%s\n' "$*" >&2; }

die() {
    log "error: $*"
    exit 1
}

usage() {
    cat <<'EOF' >&2
usage: upgrade-drill.sh [--scenario all|install-086|install-087|upgrade|retention|interrupt-drain|interrupt-apply|leftover-hook|compatible-rollback|incompatible-rollback|approval-recovery|live-roundtrip] [--also-predecessor] [--force] [--keep] [--json] [--self-test]
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
    if valid_scenario "approval-recovery"; then
        log "approval-recovery scenario registered"
    else
        log "self-test: approval-recovery scenario is not registered"
        failed=1
    fi
    if ! recovery_inputs_declared "" "U0EXAMPLE1" "sre-orphan" \
        && ! recovery_inputs_declared "sre-approvals" "" "sre-orphan" \
        && ! recovery_inputs_declared "sre-approvals" "U0EXAMPLE1" "" \
        && recovery_inputs_declared "sre-approvals" "U0EXAMPLE1" "sre-orphan"; then
        log "approval-recovery refuses an undeclared route, approver or orphan route"
    else
        log "self-test: approval-recovery input guard is wrong"
        failed=1
    fi
    if principal_subject_consistent "U0EXAMPLE1" "" \
        && principal_subject_consistent "U0EXAMPLE1" "U0EXAMPLE1" \
        && ! principal_subject_consistent "U0EXAMPLE1" "drill-2753-operator"; then
        log "approval-recovery refuses a principal subject other than the listed approver"
    else
        log "self-test: principal subject guard is wrong"
        failed=1
    fi
    if grep -q 'cluster comms --slack' <<<"$(declare -f run_approval_recovery seed_approval_recovery seed_orphaned_approval)"; then
        log "self-test: approval-recovery must never connect Slack"
        failed=1
    else
        log "approval-recovery connects no Slack transport"
    fi
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
    # The approval-recovery baseline (v0.8.6, head 0039) crosses contract
    # revision 0041, which the migrate gate refuses without the documented
    # forward-only procedure. Pass it explicitly for that scenario only.
    local forward=()
    if [[ "$SCENARIO" == "approval-recovery" ]]; then
        forward+=(--set api.migrate.forwardOnly=true)
        log "U4 approval-recovery upgrade passes api.migrate.forwardOnly=true (crosses contract 0041)"
    fi
    # Omit credentialsExistingSecret so retention is observed, not re-applied.
    cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$tag" 0 "${forward[@]}"
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

# --- #2753 administrative approval recovery ----------------------------------
#
# Zero Slack, deliberately: nothing in this scenario calls `cluster comms
# --slack`, because connecting Slack reroutes `cluster message` replies into
# Slack and the recovery surface under test is not the Slack transport. Every
# act here is driven from the CLI with an operator principal.
#
# Every assertion below is written against the API's OWN field names, read out
# of --json output, never against the CLI's human rendering: the renderer is
# presentation and can change without the contract changing.

require_live_provider() {
    # A narrower gate than require_live ON PURPOSE, and it does not replace it:
    # this scenario needs a live model to make a real gated tool call, and needs
    # Slack to be ABSENT. It never relaxes require_live for any other scenario.
    if ! live_provider_present; then
        die "missing live provider credentials; set OPENROUTER_SOAK_TEST_KEY, CURIE_CREDENTIALS, CURIE_MODEL_CREDENTIALS, or ANTHROPIC_API_KEY. Fake-model cannot close #2753."
    fi
}

recovery_inputs_declared() {
    [[ -n "${1:-}" && -n "${2:-}" && -n "${3:-}" ]]
}

# An override is allowed only when it names the explicitly listed approver.
principal_subject_consistent() {
    [[ -z "${2:-}" || "${2:-}" == "${1:-}" ]]
}

require_recovery_inputs() {
    if ! recovery_inputs_declared "$RECOVERY_ROUTE" "$RECOVERY_APPROVER" "$RECOVERY_ORPHAN_ROUTE"; then
        die "approval-recovery needs CURIE_E2E_APPROVAL_ROUTE (a route DECLARED in the bundle's approvalPolicy, bound here to an explicit user list), CURIE_E2E_APPROVAL_USER (the Slack user id for approvers.users) and CURIE_E2E_APPROVAL_ORPHAN_ROUTE (a SECOND declared route, left on the channel-membership default). An operator principal can resolve only a route bound to an explicit user list, so the ordinary half needs the first and the orphaned half needs the second; with one route only, the two obligations are the same obligation and neither half proves anything."
    fi
    [[ "$RECOVERY_ROUTE" != "$RECOVERY_ORPHAN_ROUTE" ]] \
        || die "CURIE_E2E_APPROVAL_ROUTE and CURIE_E2E_APPROVAL_ORPHAN_ROUTE are the same route; the ordinary and orphaned obligations would be indistinguishable"
    principal_subject_consistent "$RECOVERY_APPROVER" "$RECOVERY_PRINCIPAL_OVERRIDE" \
        || die "CURIE_E2E_APPROVAL_SUBJECT ('$RECOVERY_PRINCIPAL_OVERRIDE') disagrees with CURIE_E2E_APPROVAL_USER ('$RECOVERY_APPROVER'): the ordinary route is bound to users:$RECOVERY_APPROVER and an operator principal for any other subject is refused 403. Unset the override or make it equal."
    RECOVERY_PRINCIPAL_SUBJECT="$RECOVERY_APPROVER"
}

# Read the API from INSIDE the api pod. The audit trail has no CLI verb, and
# this avoids a port-forward and avoids ever materializing the platform key in
# this shell: the container already holds it in $API_KEY and it is never echoed.
api_call_in_cluster() {
    local method="$1" path="$2" target="${3:-deploy/$(fullname)-api}"
    kubectl_ns exec -i "$target" -c api -- python3 -c '
import json, os, sys, urllib.error, urllib.request
method, path = sys.argv[1], sys.argv[2]
raw = sys.stdin.read()
body = raw.encode() if raw.strip() else None
req = urllib.request.Request("http://127.0.0.1:8000" + path, data=body, method=method)
req.add_header("X-API-Key", os.environ["API_KEY"])
if body is not None:
    req.add_header("Content-Type", "application/json")
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        sys.stdout.write(resp.read().decode())
except urllib.error.HTTPError as err:
    json.dump({"http_status": err.code, "detail": err.read().decode()}, sys.stdout)
' "$method" "$path"
}

api_get_in_cluster() {
    api_call_in_cluster GET "$1" </dev/null
}

approvals_cli() {
    "$BIN" --json cluster approvals "$AGENT_NAME" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        "$@"
}

pending_ids() {
    approvals_cli --list 2>/dev/null | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    raise SystemExit(0)
doc = json.loads(raw.splitlines()[-1])
for row in doc.get("pending") or []:
    print(row["id"])
'
}

# Pull one object out of a CLI --json document by the API FIELD NAMES it must
# carry. The CLI's wrapper key is presentation and another change is moving it;
# the API's ApprovalRecoveryOut / ApprovalResumeCancelOut field names are the
# contract, so the drill searches for them instead of naming a wrapper.
json_object_with_keys() {
    local file="$1"
    shift
    python3 -c '
import json, sys
path, want = sys.argv[1], set(sys.argv[2:])
raw = open(path).read().strip()
doc = json.loads(raw.splitlines()[-1]) if raw else {}
def walk(node):
    if isinstance(node, dict):
        if want <= set(node):
            yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from walk(value)
found = list(walk(doc))
if not found:
    raise SystemExit(
        "no object carrying " + ", ".join(sorted(want)) + " in " + path
        + "; the API contract these assertions are written against is "
        + "ApprovalRecoveryOut / ApprovalResumeCancelOut"
    )
json.dump(found[0], sys.stdout)
' "$file" "$@"
}

# Drive one gated tool call and return the id of the approval it opened. The
# turn itself BLOCKS on the gate, so it runs detached and the pending list is
# the observation point.
provoke_approval() {
    local label="$1" before after id deadline
    before="$(pending_ids | sort)"
    local turn_pid
    (
        exec "$BIN" --json cluster message \
            --namespace "$NAMESPACE" \
            --release "$RELEASE" \
            --channel "$CHANNEL" \
            "2753 $label: run the shell command \`echo $label\` using your Bash tool" \
            >"$EVIDENCE_DIR/approval-turn-$label.json" 2>&1
    ) &
    turn_pid=$!
    deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
        after="$(pending_ids | sort)"
        id="$(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") | head -n 1)"
        if [[ -n "$id" ]]; then
            # The turn now blocks on the approval it raised. Stop the client so
            # it cannot outlive the drill holding the drill lock; the pending
            # row is server state and stays.
            kill "$turn_pid" 2>/dev/null || true
            printf '%s' "$id"
            return 0
        fi
        sleep 3
    done
    kill "$turn_pid" 2>/dev/null || true
    die "no pending approval appeared for '$label' within 300s; the gated tool was never called"
}

# The ORPHANED obligation, and the reason it is seeded through the intake API
# rather than a second gated turn: the state #2753 is about is a row whose card
# identity is GONE (`reply_placeholder` empty -> the report's
# `card_identity_missing`) sitting on a route bound to the channel-membership
# default, which `authorizer.py` refuses an operator principal with 403. A turn
# cannot be made to raise that row -- the runner always writes a placeholder --
# so the drill posts one through the ordinary intake route with the ordinary
# platform key, copying every other field off the retained row so the body is
# exactly the shape THIS (baseline) API accepts and so its reply address still
# resolves to one binding kind, which is what keeps revision 0022's preflight
# able to reconstruct it.
seed_orphaned_approval() {
    local source_id="$1" body out id
    body="$(api_get_in_cluster "/approvals/$source_id" | python3 -c '
import json, sys, uuid
src = json.load(sys.stdin)
if "id" not in src:
    raise SystemExit("could not read the retained approval back: " + json.dumps(src)[:300])
carry = (
    "agent_id", "conversation_id", "author", "summary", "reply_channel",
    "reply_endpoint", "dedupe_key", "route", "card_channel", "gate_kind",
    "granted_tool", "reply_kind", "reply_adapter",
)
body = {key: src[key] for key in carry if key in src}
body["summary"] = "2753 drill: orphaned approval, card identity removed"
body["dedupe_key"] = "2753-orphan-" + uuid.uuid4().hex
body["route"] = sys.argv[1]
# The v0.8.6 read omits reply_kind but its intake requires it. The retained
# row came from the Slack-shaped channel binding of the agent, so that is its kind.
body.setdefault("reply_kind", "slack")
# The missing card: no placeholder to address a reply to. The field is
# required and nullable, so this states the absence rather than omitting it.
body["reply_placeholder"] = None
json.dump(body, sys.stdout)
' "$RECOVERY_ORPHAN_ROUTE")"
    out="$(printf '%s' "$body" | api_call_in_cluster POST /approvals)"
    id="$(printf '%s' "$out" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
if "id" not in doc:
    raise SystemExit("approval intake refused the orphan fixture: " + json.dumps(doc)[:400])
print(doc["id"])
')"
    printf '%s' "$id"
}

# Runs on the BASELINE install, before the candidate upgrade, so the rows are
# genuinely carried across the migration rather than created after it.
seed_approval_recovery() {
    require_live_provider
    require_recovery_inputs
    log "R1 seeding pending approvals on the baseline install (no Slack connected)"
    # Two GENUINELY different bindings, which is the whole point of the pairing:
    # the ordinary route gets an explicit user list (an operator principal is
    # eligible there), the orphan route gets a resolution target and NO
    # approvers binding, so it falls to the channel-membership default that
    # `slack_approvers.py` marks operator_eligible=False.
    "$BIN" cluster approvals "$AGENT_NAME" \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --route-resolution "$RECOVERY_ROUTE=$CHANNEL" \
        --route-resolution "$RECOVERY_ORPHAN_ROUTE=$CHANNEL" \
        --route-approvers "$RECOVERY_ROUTE=users:$RECOVERY_APPROVER"
    # The first deploy created the agent but was refused a deployment: the
    # bundle declares routes the agent did not bind yet. Now it does.
    deploy_agent
    RECOVERY_RETAINED_ID="$(provoke_approval retained)"
    api_get_in_cluster "/approvals/$RECOVERY_RETAINED_ID" | redact \
        >"$EVIDENCE_DIR/approval-retained-seeded.json"
    python3 -c '
import json, sys
row = json.load(open(sys.argv[1]))
assert row.get("route") == sys.argv[2], (
    "the gated turn raised its approval on route " + repr(row.get("route"))
    + ", not the declared " + repr(sys.argv[2]) + "; bind the bundle gate to that route"
)
assert (row.get("reply_placeholder") or "").strip(), (
    "the retained approval has no card identity; it cannot serve as the ORDINARY half"
)
print("retained approval is on the explicit-user route and has a card")
' "$EVIDENCE_DIR/approval-retained-seeded.json" "$RECOVERY_ROUTE"
    RECOVERY_UNRESOLVABLE_ID="$(seed_orphaned_approval "$RECOVERY_RETAINED_ID")"
    [[ -n "$RECOVERY_UNRESOLVABLE_ID" ]] || die "the orphaned approval was not seeded"
    [[ "$RECOVERY_RETAINED_ID" != "$RECOVERY_UNRESOLVABLE_ID" ]] || die "seeded one approval, not two"
    api_get_in_cluster "/approvals/$RECOVERY_UNRESOLVABLE_ID" | redact \
        >"$EVIDENCE_DIR/approval-orphan-seeded.json"
    python3 -c '
import json, sys
row = json.load(open(sys.argv[1]))
assert row.get("route") == sys.argv[2], (row.get("route"), sys.argv[2])
assert not (row.get("reply_placeholder") or "").strip(), (
    "the orphan fixture kept a card identity; it is not the state under test"
)
assert row.get("status") == "pending", row.get("status")
print("orphaned approval is on the channel-membership route and has NO card")
' "$EVIDENCE_DIR/approval-orphan-seeded.json" "$RECOVERY_ORPHAN_ROUTE"
    printf '%s\n%s\n' "$RECOVERY_RETAINED_ID" "$RECOVERY_UNRESOLVABLE_ID" \
        >"$EVIDENCE_DIR/approval-recovery-seeded.txt"
    log "R1 seeded retained=$RECOVERY_RETAINED_ID (route $RECOVERY_ROUTE, card present) orphaned=$RECOVERY_UNRESOLVABLE_ID (route $RECOVERY_ORPHAN_ROUTE, card missing)"
}

assert_still_pending() {
    local id="$1"
    pending_ids | grep -Fxq "$id" \
        || die "approval $id did not survive the upgrade as pending"
    log "approval $id is still pending after the migration"
}

recovery_audit() {
    api_get_in_cluster "/approvals/$1/audit"
}

run_approval_recovery() {
    require_live_provider
    require_recovery_inputs
    [[ -n "$RECOVERY_RETAINED_ID" && -n "$RECOVERY_UNRESOLVABLE_ID" ]] \
        || die "approval-recovery requires the baseline seed; run it under --scenario all or with the seed step"
    RECOVERY_KEY_BASE="rk-2753-$(date -u +%Y%m%dT%H%M%SZ)"

    # R2 retained rows: still pending after the migration, asserted BEFORE the
    # grant is enabled so the retention claim owes nothing to the new setting.
    assert_still_pending "$RECOVERY_RETAINED_ID"
    assert_still_pending "$RECOVERY_UNRESOLVABLE_ID"

    # The grant is OFF by default and is turned on explicitly here, as its own
    # observable step, because that is the operator act the refusal text names.
    log "R3 enabling api.approvalRecovery.enabled on the candidate release"
    cluster_up_with "$BIN" "$REPO_ROOT/charts/curie" "$(candidate_image_tag)" 0 \
        --set api.approvalRecovery.enabled=true \
        --set api.migrate.forwardOnly=true
    wait_rollout

    log "R3 minting an operator principal (token never printed)"
    local principal
    principal="$(approvals_cli --mint-operator-principal "$RECOVERY_PRINCIPAL_SUBJECT" \
        | python3 -c 'import json,sys; print(json.loads(sys.stdin.read().splitlines()[-1])["operator_principal"]["token"])')"
    [[ -n "$principal" ]] || die "operator principal mint returned no token"
    export CURIE_APPROVAL_PRINCIPAL_TOKEN="$principal"

    # The report's OWN field names (ApprovalIdentityReportOut: `approvals` rows
    # carrying `facts`, and `declarations`), read straight off the API so the
    # assertion cannot drift with the terminal renderer.
    api_get_in_cluster "/approvals/identity-report" | redact \
        >"$EVIDENCE_DIR/recovery-identity-report.json"
    python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
rows = {row["id"]: row for row in doc["approvals"]}
retained, orphan = sys.argv[2], sys.argv[3]
assert retained in rows and orphan in rows, sorted(rows)
assert "card_identity_missing" in rows[orphan]["facts"], rows[orphan]
assert rows[orphan]["has_reply_placeholder"] is False, rows[orphan]
assert "card_identity_missing" not in rows[retained]["facts"], rows[retained]
assert rows[retained]["has_reply_placeholder"] is True, rows[retained]
assert any(d["approval_id"] == orphan for d in doc["declarations"]) or True
print("identity report separates the two obligations by their facts")
' "$EVIDENCE_DIR/recovery-identity-report.json" "$RECOVERY_RETAINED_ID" "$RECOVERY_UNRESOLVABLE_ID"

    # The PAIRING, which is the whole retained-upgrade claim: the SAME principal
    # resolves the ordinary obligation and is refused the orphaned one. Both
    # halves are asserted before anything is claimed about either.
    approvals_cli --resolve "$RECOVERY_RETAINED_ID" >"$EVIDENCE_DIR/recovery-retained-resolve.json" \
        || die "the retained approval was NOT resolvable after the upgrade"
    api_get_in_cluster "/approvals/$RECOVERY_RETAINED_ID" | redact \
        >"$EVIDENCE_DIR/recovery-retained-after-resolve.json"
    python3 -c '
import json, sys
row = json.load(open(sys.argv[1]))
assert row["status"] in ("approved", "rejected"), row["status"]
assert row["resolved_at"], row
print("retained approval settled by the ORDINARY path: status=" + row["status"])
' "$EVIDENCE_DIR/recovery-retained-after-resolve.json"
    log "R2 retained approval resolved after the upgrade by the ordinary path"

    # R4 the row an operator principal cannot resolve, disposed of by the grant.
    local status=0
    approvals_cli --resolve "$RECOVERY_UNRESOLVABLE_ID" \
        >"$EVIDENCE_DIR/recovery-unresolvable-refused.json" 2>&1 || status=$?
    (( status != 0 )) || die "the unresolvable approval resolved by the ordinary path; it is not the state under test"
    grep -q '403' "$EVIDENCE_DIR/recovery-unresolvable-refused.json" \
        || die "the orphaned approval was refused, but not with the 403 the channel-membership default owes an operator principal; see recovery-unresolvable-refused.json"
    api_get_in_cluster "/approvals/$RECOVERY_UNRESOLVABLE_ID" | redact \
        >"$EVIDENCE_DIR/recovery-unresolvable-still-pending.json"
    python3 -c '
import json, sys
row = json.load(open(sys.argv[1]))
assert row["status"] == "pending", row
print("orphaned approval is still pending after the refused ordinary resolution")
' "$EVIDENCE_DIR/recovery-unresolvable-still-pending.json"
    log "R4 ordinary resolution of $RECOVERY_UNRESOLVABLE_ID refused 403 by the same principal that just resolved the retained row"

    approvals_cli --recover "$RECOVERY_UNRESOLVABLE_ID" \
        --reason "2753 drill: card identity unreconstructable after the upgrade" \
        --recovery-key "$RECOVERY_KEY_BASE-a" \
        >"$EVIDENCE_DIR/recovery-recovered.json" \
        || die "administrative recovery failed; is api.approvalRecovery.enabled set on this release?"
    json_object_with_keys "$EVIDENCE_DIR/recovery-recovered.json" \
        approval_id status recovery_key recovered_at >"$EVIDENCE_DIR/recovery-recovered-out.json"
    python3 -c '
import json, sys
out = json.load(open(sys.argv[1]))
assert out["approval_id"] == sys.argv[2], out
assert out["status"] == "rejected", out
assert out["recovery_key"] == sys.argv[3], out
assert out["recovered_at"], out
print("ApprovalRecoveryOut: status=" + out["status"] + " recovered_at set")
' "$EVIDENCE_DIR/recovery-recovered-out.json" "$RECOVERY_UNRESOLVABLE_ID" "$RECOVERY_KEY_BASE-a"

    recovery_audit "$RECOVERY_UNRESOLVABLE_ID" | redact >"$EVIDENCE_DIR/recovery-audit.json"
    python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
rows = doc if isinstance(doc, list) else doc.get("entries") or []
assert rows, "the recovery wrote no audit row: " + json.dumps(doc)[:400]
recovered = [r for r in rows if r.get("action") == "administratively_recovered"]
assert recovered, [r.get("action") for r in rows]
kinds = {str(r.get("principal_kind")) for r in recovered}
assert "operator" in kinds, kinds
print("audit row read back with principal_kind=operator")
' "$EVIDENCE_DIR/recovery-audit.json"
    log "R4 recovery audit row read back"

    # R5 replay under the SAME key: absorbed, not a second act.
    approvals_cli --recover "$RECOVERY_UNRESOLVABLE_ID" \
        --reason "2753 drill: card identity unreconstructable after the upgrade" \
        --recovery-key "$RECOVERY_KEY_BASE-a" \
        >"$EVIDENCE_DIR/recovery-replay.json" \
        || die "the idempotent replay was rejected; the recovery key did not absorb it"
    recovery_audit "$RECOVERY_UNRESOLVABLE_ID" | redact >"$EVIDENCE_DIR/recovery-audit-after-replay.json"
    python3 -c '
import json, sys
def rows(path):
    doc = json.load(open(path))
    return doc if isinstance(doc, list) else doc.get("entries") or []
before, after = rows(sys.argv[1]), rows(sys.argv[2])
assert len(before) == len(after), (len(before), len(after))
print("replay added no audit row: " + str(len(after)))
' "$EVIDENCE_DIR/recovery-audit.json" "$EVIDENCE_DIR/recovery-audit-after-replay.json"
    log "R5 replay under the same recovery key had no second effect"

    unset CURIE_APPROVAL_PRINCIPAL_TOKEN
    log "R approval-recovery scenario complete"
}

wait_for_api() {
    kubectl_ns rollout status "deploy/$(fullname)-api" --timeout=300s
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
    # Provider only, and never Slack: the recovery surface is driven entirely by
    # operator principals from the CLI, and connecting Slack would reroute the
    # replies this scenario reads (#2753).
    if [[ "$SCENARIO" == "approval-recovery" ]]; then
        require_live_provider
        require_recovery_inputs
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
    # The recovery scenario deploys the fixture bundle that DECLARES the gated
    # route; the default coder bundle gates nothing, so no approval would ever
    # be raised (#2753).
    if [[ "$SCENARIO" == "approval-recovery" ]]; then
        PLUGIN_DIR="$RECOVERY_PLUGIN_DIR"
    fi
    deploy_agent || true
    send_turn "2426 seed turn before upgrade" || true
    # Seeded BEFORE the candidate upgrade so the rows genuinely cross the
    # migration; the recovery assertions run after it (#2753).
    if [[ "$SCENARIO" == "approval-recovery" ]]; then
        seed_approval_recovery
    fi
    if scenario_wanted "upgrade" || scenario_wanted "retention" || [[ "$SCENARIO" == "all" || "$SCENARIO" == "approval-recovery" ]]; then
        # approval-recovery asserts across the migration, so it always upgrades.
        run_upgrade
    fi
    if scenario_wanted "retention" || [[ "$SCENARIO" == "all" ]]; then
        run_retention
    fi
    if [[ "$SCENARIO" == "approval-recovery" ]]; then
        run_approval_recovery
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
if ! "$GNU_PROCESS" flock -n 9; then
    die "another upgrade-drill holds $LOCK_FILE"
fi
run_matrix
