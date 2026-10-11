#!/usr/bin/env bash
# Isolated cluster-upgrade matrix for the next-train verb (#2590).
#
# Upgrade rows use `curie cluster upgrade --yes --to ...`; rollback-to-serving
# injects its interrupted history with one raw Helm upgrade.
# Refuses the permanent soak (namespace/release `curie`, namespace `default`).
# Never mutates soak, never prints secret values, never messages a human.
#
# Usage:
#   curie dev cluster-upgrade-matrix [--scenario all|...] [--force] [--keep] [--json]
#   bash cli/scripts/cluster-upgrade-matrix.sh --list-shards [--json]
#   bash cli/scripts/cluster-upgrade-matrix.sh --shard s01 [--force] [--keep] [--json]
#   bash cli/scripts/cluster-upgrade-matrix.sh --self-test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# util-linux flock, with its exit statuses, on hosts that ship none (a stock Mac).
GNU_PROCESS="$REPO_ROOT/cli/scripts/gnu-process.py"
SELF_TEST=0
FORCE=0
KEEP=0
JSON=0
SCENARIO="all"
SCENARIO_SET=0
SHARD=""
SHARD_SETUP=0
SHARD_ITEMS=()
LIST_SHARDS=0
CURRENT_LABEL=""
CURRENT_NAME=""
CURRENT_PHASES=""
CURRENT_STARTED=""
BIN="${CURIE_BIN:-}"
NAMESPACE="${CURIE_E2E_NAMESPACE:-acme-2590}"
RELEASE="${CURIE_E2E_RELEASE:-t2590}"
KIND_CLUSTER="${CURIE_E2E_KIND_CLUSTER:-curie-t2590}"
KUBECONFIG_FILE="${CURIE_E2E_KUBECONFIG:-$REPO_ROOT/.projects/kubeconfig-t2590}"
export KUBECONFIG="$KUBECONFIG_FILE"
EVIDENCE_DIR="${CURIE_E2E_EVIDENCE_DIR:-$REPO_ROOT/.projects/2590-evidence}"
CANDIDATE_TAG="${CURIE_E2E_CANDIDATE_TAG:-}"
LOCK_FILE="/tmp/curie-cluster-upgrade-matrix.lock"
WORKDIR=""
CANDIDATE=""
OWNED_KIND=0
OWNED_HELM=0
ASSET_DIR=""
STARTED_AT=""
CHART_0100=""
CHART_0101=""
REV_088=""
REV_089=""
SENTINEL_ID="acme-2590"
FAIL_AT_HOOK=""
INTERRUPT_AFTER_HOOK=""
KUBECTL_CONTEXT_ARGS=()
HELM_CONTEXT_ARGS=()
APPLY_KILL_PID=""
APPLY_KILL_PIDS=()
APPLY_KILL_HELM_SEEN=0
ROLLBACK_SERVING_HELM_PID=""
RETIRE_RUNNER_ORIGINAL_ID=""
RETIRE_RUNNER_DERIVED_TAG=""
RETIRE_LAYER_TAG=""

CHART_088_SHA="88664c2f991bed7a3e4bc0513ae73bfcbac08077d99a69e7087138aa6f8f3af2"
CLI_088_SHA="dc0e1ab1b928522f1ca1c03e05d823e08218800c2d2a33d0d88af623972f6685"
REL_088="https://github.com/curie-eng/curie/releases/download/v0.8.8"
CHART_089_SHA="ee57017fe3009c35a4390b0c0555c44249ba98bba1d4f53f12aa3944b2bd5e5e"
CLI_089_SHA="b8f3a00bcbf0920ae61e55039aa6a9d48e4d302db88a8c97e8148905569c0e9a"
REL_089="https://github.com/curie-eng/curie/releases/download/v0.8.9"
# Published v0.8.8 ships alembic 0039. The candidate head is the checkout's
# schema_compat.json, so a rebase onto a newer next does not hard-code 0043.
PUBLISHED_HEAD="0039"
TARGET_HEAD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["schema_head"])' \
    "$REPO_ROOT/apps/api/src/curie_api/schema_compat.json")"
SUPPORTED_ROLLBACK_HEAD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["windows"]["0.10.0"]["schema_head"])' \
    "$REPO_ROOT/cli/src/application_schema_windows.json")"

SCENARIOS_ALL=(
    soak-refusal
    fresh-n
    n1-to-n-nonempty
    same-version
    fail-every-phase
    interrupt-resume
    n-to-n1
    guarded-rollback
    rollback-published-088
    rollback-published-089
    migration-crash
    converge-negative
    previous-serves
    apply-kill-takeover
    rollback-to-serving
    runner-layer-retire
)

MATRIX_PHASES=(plan validate drain_preflight checkpoint migrate apply converge canary commit)
# interrupt-resume runs a subset (issue #2733): plan/validate/drain_preflight
# interrupt before any mutation and converge/canary follow apply, so
# checkpoint, migrate, apply, commit are the distinct resume states.
# fail-every-phase keeps all nine. drain_preflight is a worker-reachability
# check, not the #2010 drain gate itself (issue #2830).
INTERRUPT_PHASES=(checkpoint migrate apply commit)
# Tag the kind node currently holds exclusively; "" when unknown. Any path
# that loads or untags app images on the node must clear it.
EXCLUSIVE_KIND_TAG=""

# Canonical shard manifest: `<id> <setup|nosetup> <scenario>[:<phase>+<phase>]...`.
# Scenario order inside a shard preserves the serial state chain. `setup`
# shards start from setup_nonempty_n (published 0.8.8 + sentinel, forward-only
# upgrade to 0.10.0). --list-shards, --shard and the self-test coverage gate all
# read SHARDS, so CI cannot run a manifest the gate did not check.
SHARDS_CANONICAL="s01 nosetup soak-refusal fresh-n n1-to-n-nonempty same-version rollback-published-088 migration-crash
s02 setup fail-every-phase:plan+validate+drain_preflight
s03 setup fail-every-phase:checkpoint+migrate+apply
s04 setup fail-every-phase:converge
s05 setup fail-every-phase:canary
s06 setup fail-every-phase:commit
s07 setup interrupt-resume:checkpoint+migrate
s08 setup interrupt-resume:apply+commit
s09 setup n-to-n1 guarded-rollback
s11 nosetup rollback-published-089
s13 setup converge-negative
s14 setup previous-serves
s15 setup runner-layer-retire
s16 setup apply-kill-takeover
s17 setup rollback-to-serving"
SHARDS="${CURIE_E2E_SHARDS_OVERRIDE:-$SHARDS_CANONICAL}"

log() { printf '%s\n' "$*" >&2; }

die() {
    log "error: $*"
    exit 1
}

usage() {
    cat <<'EOF' >&2
usage: cluster-upgrade-matrix.sh [--scenario all|soak-refusal|fresh-n|n1-to-n-nonempty|same-version|fail-every-phase|interrupt-resume|n-to-n1|guarded-rollback|rollback-published-088|rollback-published-089|migration-crash|converge-negative|previous-serves|apply-kill-takeover|rollback-to-serving|runner-layer-retire] [--shard <id>] [--list-shards] [--force] [--keep] [--json] [--self-test]
EOF
}

is_soak_namespace() {
    local ns="${1:-}"
    [[ "$ns" == "curie" || "$ns" == "default" ]]
}

is_soak_release() {
    local rel="${1:-}"
    [[ "$rel" == "curie" || "$rel" == "default" ]]
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
    for s in "${SCENARIOS_ALL[@]}"; do
        [[ "$s" == "$want" ]] && return 0
    done
    return 1
}

is_sha256() {
    local h="${1:-}"
    [[ "$h" =~ ^[0-9a-f]{64}$ ]]
}

# shard_manifest <check|json|lookup> <manifest> [shard-id]
# check: exit 0 when every scenario runs exactly once and each phased scenario
# covers its phase set exactly once across shards (fail-every-phase:
# MATRIX_PHASES, interrupt-resume: INTERRUPT_PHASES); else print
# "shard coverage failed: ..." and exit 2. json: check, then emit the manifest.
# lookup: check, then print setup|nosetup and one item per line; exit 3 when
# the id is unknown.
shard_manifest() {
    python3 -c '
import json, sys
mode, manifest = sys.argv[1], sys.argv[2]
want = sys.argv[3] if len(sys.argv) > 3 else ""
scenarios = sys.argv[4].split()
phases = sys.argv[5].split()
required = {"fail-every-phase": phases, "interrupt-resume": sys.argv[6].split()}
phased = ("fail-every-phase", "interrupt-resume")
errors, shards, ids = [], [], set()
for ln, line in enumerate(manifest.splitlines(), 1):
    toks = line.split()
    if not toks:
        continue
    if len(toks) < 3:
        errors.append(f"line {ln}: need <id> <setup|nosetup> <scenario>...")
        continue
    sid, setup, items = toks[0], toks[1], toks[2:]
    if setup not in ("setup", "nosetup"):
        errors.append(f"{sid}: setup flag must be setup or nosetup, got {setup}")
    if sid in ids:
        errors.append(f"duplicate shard id {sid}")
    ids.add(sid)
    entries = []
    for item in items:
        name, sep, ph = item.partition(":")
        plist = None
        if name not in scenarios:
            errors.append(f"{sid}: unknown scenario {name}")
        if name in phased:
            if not ph:
                errors.append(f"{sid}: phased scenario {name} must be split by phase")
            else:
                plist = ph.split("+")
                for p in plist:
                    if p not in phases:
                        errors.append(f"{sid}: unknown phase {p} for {name}")
        elif sep:
            errors.append(f"{sid}: scenario {name} is not phased")
        entries.append({"name": name, "phases": plist})
    shards.append({"id": sid, "setup": setup == "setup", "scenarios": entries})
for name in scenarios:
    hits = [e for s in shards for e in s["scenarios"] if e["name"] == name]
    if name in phased:
        got = [p for e in hits for p in (e["phases"] or [])]
        required_phases = required[name]
        for p in phases:
            n, need = got.count(p), (1 if p in required_phases else 0)
            if n != need:
                errors.append(f"{name} phase {p} runs {n} times, want {need}")
    elif len(hits) != 1:
        errors.append(f"scenario {name} runs {len(hits)} times, want 1")
if errors:
    for e in errors:
        print(f"shard coverage failed: {e}", file=sys.stderr)
    raise SystemExit(2)
if mode == "json":
    print(json.dumps({"shards": shards}))
elif mode == "lookup":
    for s in shards:
        if s["id"] == want:
            print("setup" if s["setup"] else "nosetup")
            for e in s["scenarios"]:
                print(e["name"] + (":" + "+".join(e["phases"]) if e["phases"] else ""))
            raise SystemExit(0)
    raise SystemExit(3)
' "$1" "$2" "${3:-}" "${SCENARIOS_ALL[*]}" "${MATRIX_PHASES[*]}" "${INTERRUPT_PHASES[*]}"
}

verify_sha256() {
    local want="$1" file="$2"
    echo "$want  $file" | sha256sum -c -
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --scenario) SCENARIO="${2:-}"; SCENARIO_SET=1; shift 2 ;;
            --shard) SHARD="${2:-}"; [[ -n "$SHARD" ]] || die "--shard needs an id"; shift 2 ;;
            --list-shards) LIST_SHARDS=1; shift ;;
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
    if [[ -n "$SHARD" ]]; then
        (( SCENARIO_SET == 0 )) || die "--shard and --scenario are mutually exclusive"
        (( LIST_SHARDS == 0 )) || die "--shard and --list-shards are mutually exclusive"
        local out rc=0 line
        out="$(shard_manifest lookup "$SHARDS" "$SHARD")" || rc=$?
        (( rc != 3 )) || die "unknown shard '$SHARD'"
        (( rc == 0 )) || die "shard manifest failed coverage; refusing --shard $SHARD"
        SHARD_ITEMS=()
        while IFS= read -r line; do
            [[ -n "$line" ]] || continue
            if [[ "$line" == setup ]]; then
                SHARD_SETUP=1
            elif [[ "$line" == nosetup ]]; then
                SHARD_SETUP=0
            else
                SHARD_ITEMS+=("$line")
            fi
        done <<<"$out"
    fi
}

run_self_test() {
    local failed=0 tmp script_path
    script_path="${BASH_SOURCE[0]}"
    if is_soak_namespace "curie" && is_soak_namespace "default" && ! is_soak_namespace "acme-2590"; then
        log "soak namespace curie refused"
        log "soak namespace default refused"
    else
        log "self-test: soak namespace helper is wrong"
        failed=1
    fi
    if is_soak_release "curie" && is_soak_release "default" && ! is_soak_release "t2590"; then
        log "soak release curie refused"
    else
        log "self-test: soak release helper is wrong"
        failed=1
    fi
    if valid_scenario "all" && valid_scenario "fresh-n" && valid_scenario "fail-every-phase" \
        && valid_scenario "runner-layer-retire" && ! valid_scenario "not-a-scenario"; then
        log "unknown scenario refused"
    else
        log "self-test: scenario helper is wrong"
        failed=1
    fi
    if is_sha256 "$CHART_088_SHA" && is_sha256 "$CLI_088_SHA"; then
        log "published v0.8.8 chart checksum pinned"
        log "published v0.8.8 cli checksum pinned"
    else
        log "self-test: published checksum pins are malformed"
        failed=1
    fi
    if is_sha256 "$CHART_089_SHA" && is_sha256 "$CLI_089_SHA"; then
        log "published v0.8.9 chart checksum pinned"
        log "published v0.8.9 cli checksum pinned"
    else
        log "self-test: published v0.8.9 checksum pins are malformed"
        failed=1
    fi
    if [[ "$PUBLISHED_HEAD" =~ ^[0-9]{4}$ && "$TARGET_HEAD" =~ ^[0-9]{4}$ && "$SUPPORTED_ROLLBACK_HEAD" =~ ^[0-9]{4}$ ]]; then
        log "schema heads published=$PUBLISHED_HEAD candidate=$TARGET_HEAD supported-rollback=$SUPPORTED_ROLLBACK_HEAD"
    else
        log "self-test: schema heads are malformed published='$PUBLISHED_HEAD' target='$TARGET_HEAD' supported-rollback='$SUPPORTED_ROLLBACK_HEAD'"
        failed=1
    fi
    tmp="$(mktemp)"
    printf 'curie-upgrade-matrix-pin\n' >"$tmp"
    local got
    got="$(sha256sum "$tmp" | awk '{print $1}')"
    if verify_sha256 "$got" "$tmp" >/dev/null; then
        log "sha256 helper verified a matching fixture"
    else
        log "self-test: sha256 helper failed a matching fixture"
        failed=1
    fi
    if verify_sha256 "$CHART_088_SHA" "$tmp" >/dev/null 2>&1; then
        log "self-test: sha256 helper accepted a mismatch"
        failed=1
    else
        log "sha256 helper rejected a mismatched fixture"
    fi
    rm -f "$tmp"
    if awk '/^resolve_bin\(\)/,/^}/' "$script_path" | grep -q 'command -v curie'; then
        log "self-test: resolve_bin falls back to PATH curie"
        failed=1
    else
        log "candidate binary is not PATH fallback"
    fi
    if grep -q '^--set security.gvisor' "$script_path"; then
        log "self-test: image_sets must emit KEY=VAL lines, not combined --set tokens"
        failed=1
    elif grep -q 'security.gvisor.mode=off' "$script_path"; then
        log "helm --set KEY=VAL tokens are split"
    else
        log "self-test: image_sets missing gvisor off assignment"
        failed=1
    fi
    if grep -q -- '--json cluster upgrade' "$script_path"; then
        log "cluster upgrade verb is the mutator"
    else
        log "self-test: mutator must be cluster upgrade"
        failed=1
    fi
    # Match the source's variable names without expanding them in this shell.
    # shellcheck disable=SC2016
    if awk '/^restore_n\(\)/,/^}/' "$script_path" | grep -q '"$status" == "in_progress" && "$target" == "0.10.0"'; then
        log "restore_n resumes leftover in_progress 0.10.0"
    else
        log "self-test: restore_n must resume leftover in_progress 0.10.0 instead of skipping"
        failed=1
    fi
    if awk '/^restore_n\(\)/,/^}/' "$script_path" | awk '
        /cluster_upgrade "0.10.0"/ { upgraded=1 }
        /clear_upgrade_checkpoint/ { if (upgraded) after=1 }
        END { exit after ? 0 : 1 }
    '; then
        log "restore_n clears leftover in_progress after restoring 0.10.0"
    else
        log "self-test: restore_n must clear the checkpoint after the 0.10.0 restore"
        failed=1
    fi
    if awk '/^run_n_to_n1\(\)/,/^}/' "$script_path" | grep -q '^[[:space:]]*restore_n$'; then
        log "n-to-n1 restores 0.10.0 through restore_n"
    else
        log "self-test: n-to-n1 must call restore_n so leftover in_progress 0.10.0 cannot refuse 0.10.1"
        failed=1
    fi
    if awk '/^restore_n\(\)/,/^}/' "$script_path" | grep -q 'helm_ns rollback'; then
        log "restore_n rolls back to 0.10.0 when a revision exists"
    else
        log "self-test: restore_n must helm rollback to 0.10.0 instead of a full upgrade wait"
        failed=1
    fi
    if awk '/^restore_n\(\)/,/^}/' "$script_path" | awk '
        /exclusive_kind_tag "0.10.0"/ { if (!rollback) before=1 }
        /helm_ns rollback/ { rollback=1 }
        END { exit (before && rollback) ? 0 : 1 }
    '; then
        log "restore_n loads exclusive 0.10.0 images before rollback"
    else
        log "self-test: restore_n must exclusive_kind_tag 0.10.0 before helm rollback (pullPolicy Never)"
        failed=1
    fi
    if awk '/^exclusive_kind_tag\(\)/,/^}/' "$script_path" | awk '
        index($0, "\"$EXCLUSIVE_KIND_TAG\" == \"$keep\"") { early=NR }
        /kind load docker-image/ { if (!load) load=NR }
        index($0, "EXCLUSIVE_KIND_TAG=\"$keep\"") { set=NR }
        /untag_kind_siblings/ { last_untag=NR }
        END { exit (early && load && early < load && set > last_untag) ? 0 : 1 }
    '; then
        log "exclusive_kind_tag skips a reload when the node already holds the tag"
    else
        log "self-test: exclusive_kind_tag must return early on EXCLUSIVE_KIND_TAG and set it after the final untag"
        failed=1
    fi
    local fn inval_ok=1
    for fn in load_tag_images untag_kind_siblings ensure_kind retag_candidate_versions; do
        if ! awk "/^${fn}\\(\\)/,/^}/" "$script_path" | grep -q 'EXCLUSIVE_KIND_TAG=""'; then
            log "self-test: $fn must invalidate EXCLUSIVE_KIND_TAG"
            inval_ok=0
            failed=1
        fi
    done
    if (( inval_ok )); then
        log "load_tag_images invalidates the exclusive kind tag"
    fi
    if awk '/^exclusive_kind_tag\(\)/,/^}/' "$script_path" | awk '
        /untag_kind_siblings/ { untag++ }
        /kind load docker-image/ { load=1 }
        END { exit (untag >= 2 && load) ? 0 : 1 }
    '; then
        log "exclusive_kind_tag untags siblings before and after load"
    else
        log "self-test: exclusive_kind_tag must untag siblings before and after kind load"
        failed=1
    fi
    if awk '/^run_guarded_rollback\(\)/,/^}/' "$script_path" | grep -q 'exclusive_kind_tag "0.10.0"'; then
        log "guarded rollback reloads exclusive 0.10.0 images"
    else
        log "self-test: guarded rollback must exclusive_kind_tag 0.10.0 before helm rollback"
        failed=1
    fi
    if awk '/^run_rollback_published_088\(\)/,/^}/' "$script_path" | grep -q 'load_tag_images "0.8.8"'; then
        log "published 0.8.8 rollback reloads 0.8.8 images"
    else
        log "self-test: rollback-published-088 must load 0.8.8 images before helm rollback"
        failed=1
    fi
    if awk '/^run_rollback_published_089\(\)/,/^}/' "$script_path" | grep -q 'run_guarded_rollback'; then
        log "published 0.8.9 refusal keeps the guarded rollback proof in one scenario"
    else
        log "self-test: rollback-published-089 must run the guarded rollback proof"
        failed=1
    fi
    if declare -F run_rollback_to_serving >/dev/null 2>&1 \
        && ! awk '/^run_rollback_to_serving\(\)/,/^}/' "$script_path" \
            | grep -E 'settle_helm_operation|recover_helm_lock|recover_killed_upgrade_ownership' >/dev/null; then
        log "rollback-to-serving leaves the pending revision for the CLI"
    else
        log "self-test: rollback-to-serving must leave the pending revision for the CLI without recovery helpers"
        failed=1
    fi
    if awk '/^run_migration_crash\(\)/,/^}/' "$script_path" | awk '
        /interrupt_schema_migrate$/ { interrupt=NR }
        /SECONDS \+ 120/ { if (interrupt) bound=NR }
        /terminate_tree "\$pid"/ { if (bound) kill=NR }
        /recover_killed_upgrade_ownership/ { if (kill) owner=NR }
        /recover_helm_lock/ { if (owner) lock=NR }
        /migration-crash-retry/ { if (lock) retry=NR }
        END { exit retry ? 0 : 1 }
    '; then
        log "migration-crash bounds the interrupted upgrade wait"
    else
        log "self-test: migration-crash must wait at most 120s for the interrupted upgrade, then terminate it and recover ownership and the helm lock before retrying"
        failed=1
    fi
    if awk '/^run_apply_kill_takeover\(\)/,/^}/' "$script_path" | awk '
        /cluster_upgrade|exclusive_kind_tag|settle_helm_operation|recover_killed_upgrade_ownership|recover_helm_lock|terminate_tree/ { forbidden=1 }
        /"\$BIN" --json cluster upgrade/ { direct++ }
        END { exit (!forbidden && direct >= 5) ? 0 : 1 }
    '; then
        log "apply-kill-takeover uses the binary without harness recovery"
    else
        log "self-test: apply-kill-takeover must call the binary directly for every upgrade and use no harness recovery helper"
        failed=1
    fi
    if awk '/^run_apply_kill_takeover\(\)/,/^}/' "$script_path" | awk '
        /SECONDS \+ 600/ { bound=NR }
        /running_hooks="\$\(running_release_hook_jobs\)"/ { poll=NR }
        /release hook Jobs still running after 600s/ { refusal=NR }
        /"\$BIN" --json cluster upgrade.*--dry-run --take-over/ { dry=NR }
        END { exit (bound && poll > bound && refusal > poll && dry > refusal) ? 0 : 1 }
    '; then
        log "apply-kill-takeover waits at most 600s for hooks before matching dry takeover"
    else
        log "self-test: apply-kill-takeover must wait at most 600s for no running hook Jobs before matching dry takeover"
        failed=1
    fi
    if awk '/^sigkill_tree\(\)/,/^}/' "$script_path" | awk '
        /pgrep -P/ { walks=1 }
        /sigkill_tree "\$child"/ { descendants=1 }
        /kill -KILL "\$pid"/ { killed=1 }
        END { exit (walks && descendants && killed) ? 0 : 1 }
    ' && awk '/^run_apply_kill_takeover\(\)/,/^}/' "$script_path" | awk '
        /sigkill_tree "\$pid"/ { found=1 }
        END { exit found ? 0 : 1 }
    '; then
        log "apply-kill-takeover sends SIGKILL to every descendant"
    else
        log "self-test: apply-kill-takeover must send SIGKILL to the binary and every descendant"
        failed=1
    fi
    if awk '/^prepare_takeover_context\(\)/,/^}/' "$script_path" | awk '
        /kubeconfig_is_named_kind/ { verifies=1 }
        /--kubeconfig "\$KUBECONFIG_FILE" config rename-context "kind-\$KIND_CLUSTER" k8/ { private=1 }
        /KUBECTL_CONTEXT_ARGS=\(--context k8\)/ { kubectl=1 }
        /HELM_CONTEXT_ARGS=\(--kube-context k8\)/ { helm=1 }
        END { exit (verifies && private && kubectl && helm) ? 0 : 1 }
    '; then
        log "apply-kill-takeover pins its private kind cluster to context k8"
    else
        log "self-test: apply-kill-takeover must verify its named kind cluster before selecting k8 in its private kubeconfig"
        failed=1
    fi
    if shard_manifest check "$SHARDS"; then
        log "shard manifest covers every scenario exactly once"
    else
        log "self-test: shard coverage failed for the active manifest"
        failed=1
    fi
    if [[ "$(shard_manifest lookup "$SHARDS_CANONICAL" s15)" == $'setup\nrunner-layer-retire' ]] \
        && [[ "$(scenario_fn runner-layer-retire)" == run_runner_layer_retire ]]; then
        log "runner-layer-retire runs in setup shard s15"
    else
        log "self-test: runner-layer-retire must run in setup shard s15"
        failed=1
    fi
    local mutated label
    for label in "dropped scenario" "duplicated scenario" "dropped phase" "duplicated phase" "dropped runner-layer-retire"; do
        case "$label" in
            "dropped scenario") mutated="${SHARDS_CANONICAL/ migration-crash/}" ;;
            "duplicated scenario") mutated="${SHARDS_CANONICAL/s11 nosetup rollback-published-089/s11 nosetup rollback-published-089 fresh-n}" ;;
            "dropped phase") mutated="${SHARDS_CANONICAL/interrupt-resume:checkpoint+migrate/interrupt-resume:checkpoint}" ;;
            "duplicated phase") mutated="${SHARDS_CANONICAL/fail-every-phase:converge/fail-every-phase:converge+plan}" ;;
            "dropped runner-layer-retire") mutated="${SHARDS_CANONICAL/s15 setup runner-layer-retire/}" ;;
        esac
        if [[ "$mutated" == "$SHARDS_CANONICAL" ]]; then
            log "self-test: $label negative control did not mutate the manifest"
            failed=1
        elif shard_manifest check "$mutated" 2>/dev/null; then
            log "self-test: shard coverage accepted a $label"
            failed=1
        else
            log "shard coverage refused a $label"
        fi
    done
    local claim_probe
    claim_probe="$(mktemp -d)"
    python3 - "$claim_probe" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
claim = {"metadata": {"name": "acme-target", "uid": "acme-claim-uid",
                      "labels": {"curietech.ai/agent": "acme-bot"}},
         "status": {"sandbox": {"name": "acme-sandbox"},
                    "conditions": [{"type": "Ready", "status": "True"}]}}
pod = {"metadata": {"name": "acme-sandbox", "labels": {"app.kubernetes.io/component": "runner-sandbox"}},
       "spec": {"containers": [{"name": "runner", "image": "acme-runner@sha256:target"}]},
       "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
for name, document in (("claim", claim), ("pod", pod)):
    (root / f"{name}.json").write_text(json.dumps(document))
for name, value in (("managed", "curie-worker"), ("deleting", "2026-01-01T00:00:00Z")):
    changed = json.loads(json.dumps(claim))
    if name == "managed":
        changed["metadata"]["labels"]["curietech.ai/managed-by"] = value
    else:
        changed["metadata"]["deletionTimestamp"] = value
    (root / f"{name}.json").write_text(json.dumps(changed))
(root / "pod-labels.json").write_text(json.dumps(pod["metadata"]["labels"]))
(root / "pod-labels-empty.json").write_text("")
labelled = dict(pod["metadata"]["labels"], **{"curietech.ai/agent": "acme-bot"})
(root / "pod-labels-agent.json").write_text(json.dumps(labelled))
pod["status"]["conditions"][0]["status"] = "False"
(root / "unready.json").write_text(json.dumps(pod))
unready_claim = json.loads(json.dumps(claim))
unready_claim["status"]["conditions"] = [{"type": "Ready", "status": "False",
                                          "reason": "DependenciesNotReady"}]
(root / "unready-claim.json").write_text(json.dumps(unready_claim))
unready_claim["metadata"]["labels"]["curietech.ai/managed-by"] = "curie-worker"
(root / "unready-managed.json").write_text(json.dumps(unready_claim))
PY
    if runner_layer_pod_lacks_agent_label "$claim_probe/pod-labels.json" \
        && runner_layer_pod_lacks_agent_label "$claim_probe/pod-labels-empty.json"; then
        log "runner-layer-retire accepts a target Pod with no agent label"
    else
        log "self-test: runner-layer-retire rejected a target Pod with no agent label"
        failed=1
    fi
    if runner_layer_pod_lacks_agent_label "$claim_probe/pod-labels-agent.json" >/dev/null 2>&1; then
        log "self-test: runner-layer-retire accepted the agent-labelled target Pod control"
        failed=1
    else
        log "runner-layer-retire refused the agent-labelled target Pod control"
    fi
    if [[ "$(runner_layer_claim_observation "$claim_probe/claim.json" "$claim_probe/pod.json" \
        acme-bot acme-runner@sha256:target acme-claim-uid ready)" == acme-claim-uid ]]; then
        log "runner-layer-retire accepts an unchanged Ready target claim"
    else
        log "self-test: runner-layer-retire rejected its Ready target claim"
        failed=1
    fi
    local control claim_file pod_file expected_image expected_uid
    for control in managed deleting unready old-image replaced-uid; do
        claim_file="$claim_probe/claim.json"
        pod_file="$claim_probe/pod.json"
        expected_image="acme-runner@sha256:target"
        expected_uid="acme-claim-uid"
        case "$control" in
            managed|deleting) claim_file="$claim_probe/$control.json" ;;
            unready) pod_file="$claim_probe/unready.json" ;;
            old-image) expected_image="acme-runner@sha256:old" ;;
            replaced-uid) expected_uid="acme-replacement-uid" ;;
        esac
        if runner_layer_claim_observation "$claim_file" "$pod_file" acme-bot \
            "$expected_image" "$expected_uid" ready >/dev/null 2>&1; then
            log "self-test: runner-layer-retire accepted the $control control"
            failed=1
        else
            log "runner-layer-retire refused the $control control"
        fi
    done
    if [[ "$(runner_layer_claim_observation "$claim_probe/unready-claim.json" "$claim_probe/unready.json" \
        acme-bot acme-runner@sha256:target "" bound)" == acme-claim-uid ]]; then
        log "runner-layer-retire accepts a bound but unready old claim on the layer image"
    else
        log "self-test: runner-layer-retire rejected a bound but unready old claim"
        failed=1
    fi
    for control in managed old-image; do
        claim_file="$claim_probe/unready-claim.json"
        expected_image="acme-runner@sha256:target"
        case "$control" in
            managed) claim_file="$claim_probe/unready-managed.json" ;;
            old-image) expected_image="acme-runner@sha256:old" ;;
        esac
        if runner_layer_claim_observation "$claim_file" "$claim_probe/unready.json" acme-bot \
            "$expected_image" "" bound >/dev/null 2>&1; then
            log "self-test: runner-layer-retire bound observation accepted the $control control"
            failed=1
        else
            log "runner-layer-retire bound observation refused the $control control"
        fi
    done
    rm -rf "$claim_probe"
    local saved_evidence="$EVIDENCE_DIR" saved_summary="${GITHUB_STEP_SUMMARY-}" timing_dir
    timing_dir="$(mktemp -d)"
    EVIDENCE_DIR="$timing_dir"
    GITHUB_STEP_SUMMARY=""
    run_scenario_timed self-test-probe all true
    if python3 -c '
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
ok = len(rows) == 1 and rows[0]["scenario"] == "self-test-probe" and rows[0]["phases"] == "all" \
    and rows[0]["outcome"] == "passed" and isinstance(rows[0]["elapsed_seconds"], int) and "shard" in rows[0]
raise SystemExit(0 if ok else 1)
' "$timing_dir/scenarios.jsonl" 2>/dev/null && [[ -z "$CURRENT_LABEL" ]]; then
        log "per-scenario timing recorded"
    else
        log "self-test: per-scenario timing row missing or malformed"
        failed=1
    fi
    rm -rf "$timing_dir"
    EVIDENCE_DIR="$saved_evidence"
    GITHUB_STEP_SUMMARY="$saved_summary"
    if awk '/^cluster_upgrade\(\)/,/^}/' "$script_path" | awk '
        /settle_helm_operation/ { if (!upgrade) settled=1 }
        /cluster upgrade --yes/ { upgrade=1 }
        END { exit (settled && upgrade) ? 0 : 1 }
    '; then
        log "cluster_upgrade settles helm before the next upgrade"
    else
        log "self-test: cluster_upgrade must settle a pending helm operation before the next upgrade"
        failed=1
    fi
    if ! declare -F settle_helm_operation >/dev/null 2>&1; then
        log "self-test: settle_helm_operation is missing"
        failed=1
    else
        local probe bindir state logf saved_ns saved_kube saved_wait
        probe="$(mktemp -d)"
        bindir="$probe/bin"
        state="$probe/state"
        logf="$probe/log"
        mkdir -p "$bindir" "$state"
        cat >"$bindir/kubectl" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" >>"$CURIE_SETTLE_LOG"
if [[ "$1" == "--kubeconfig" ]]; then
    shift 2
fi
if [[ "$1" == "--context" ]]; then
    shift 2
fi
if [[ "$1" == "config" ]]; then
    case "$2" in
        current-context) cat "$CURIE_SETTLE_STATE/current-context" ;;
        view) cat "$CURIE_SETTLE_STATE/server" ;;
        rename-context)
            [[ "$(cat "$CURIE_SETTLE_STATE/current-context")" == "$3" ]] || exit 1
            printf '%s' "$4" >"$CURIE_SETTLE_STATE/current-context"
            ;;
        *) exit 1 ;;
    esac
    exit 0
fi
if [[ "$1" == "get" && "$2" == "namespace" ]]; then
    if [[ -f "$CURIE_SETTLE_STATE/lookup_fail" ]]; then
        echo "The connection to the server was refused" >&2
        exit 1
    fi
    if [[ -s "$CURIE_SETTLE_STATE/phases" ]]; then
        phase="$(head -n 1 "$CURIE_SETTLE_STATE/phases")"
        tail -n +2 "$CURIE_SETTLE_STATE/phases" >"$CURIE_SETTLE_STATE/phases.next"
        mv "$CURIE_SETTLE_STATE/phases.next" "$CURIE_SETTLE_STATE/phases"
        if [[ "$phase" == "absent" ]]; then
            echo 'Error from server (NotFound): namespaces "acme-2590" not found' >&2
            exit 1
        fi
        printf '%s' "$phase"
        exit 0
    fi
    if [[ -f "$CURIE_SETTLE_STATE/phase" ]]; then
        cat "$CURIE_SETTLE_STATE/phase"
        exit 0
    fi
    echo 'Error from server (NotFound): namespaces "acme-2590" not found' >&2
    exit 1
fi
if [[ "$1" == "create" && "$2" == "namespace" ]]; then
    if [[ -f "$CURIE_SETTLE_STATE/create_fail" ]]; then
        echo "namespace create refused" >&2
        exit 1
    fi
    printf 'Active' >"$CURIE_SETTLE_STATE/phase"
    exit 0
fi
exit 0
EOF
        cat >"$bindir/helm" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" >>"$CURIE_SETTLE_LOG"
cmd=""
for arg in "$@"; do
    if [[ "$arg" == "status" || "$arg" == "rollback" ]]; then
        cmd="$arg"
    fi
done
if [[ "$cmd" == "status" ]]; then
    if [[ -f "$CURIE_SETTLE_STATE/rolled" ]]; then
        printf '%s\n' '{"info":{"status":"deployed"}}'
        exit 0
    fi
    if [[ -f "$CURIE_SETTLE_STATE/status_fail" ]]; then
        echo "The connection to the server was refused" >&2
        exit 1
    fi
    if [[ -s "$CURIE_SETTLE_STATE/statuses" ]]; then
        status="$(head -n 1 "$CURIE_SETTLE_STATE/statuses")"
        tail -n +2 "$CURIE_SETTLE_STATE/statuses" >"$CURIE_SETTLE_STATE/statuses.next"
        if [[ "$status" == "pending-upgrade" && ! -s "$CURIE_SETTLE_STATE/statuses.next" ]]; then
            rm -f "$CURIE_SETTLE_STATE/statuses.next"
            printf '%s\n' '{"info":{"status":"pending-upgrade"}}'
            exit 0
        fi
        mv "$CURIE_SETTLE_STATE/statuses.next" "$CURIE_SETTLE_STATE/statuses"
        if [[ "$status" == "missing" ]]; then
            echo "Error: release: not found" >&2
            exit 1
        fi
        printf '{"info":{"status":"%s"}}\n' "$status"
        exit 0
    fi
    printf '%s\n' '{"info":{"status":"deployed"}}'
    exit 0
fi
if [[ "$cmd" == "rollback" ]]; then
    printf '1' >"$CURIE_SETTLE_STATE/rolled"
    exit 0
fi
exit 0
EOF
        cat >"$bindir/kind" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" >>"$CURIE_SETTLE_LOG"
if [[ "$1" == get && "$2" == clusters ]]; then
    printf '%s\n' "$CURIE_SETTLE_KIND_CLUSTER"
elif [[ "$1" == get && "$2" == kubeconfig && "$3" == --name && "$4" == "$CURIE_SETTLE_KIND_CLUSTER" ]]; then
    printf 'apiVersion: v1\nkind: Config\ncurrent-context: kind-%s\nclusters:\n  - name: kind-%s\n    cluster:\n      server: %s\n' \
        "$CURIE_SETTLE_KIND_CLUSTER" "$CURIE_SETTLE_KIND_CLUSTER" "$(cat "$CURIE_SETTLE_STATE/kind-server")"
else
    exit 1
fi
EOF
        chmod +x "$bindir/kubectl" "$bindir/helm" "$bindir/kind"
        saved_ns="$NAMESPACE"
        saved_kube="$KUBECONFIG_FILE"
        saved_path="$PATH"
        saved_wait="${CURIE_HELM_SETTLE_WAIT_SECONDS-}"
        saved_poll="${CURIE_HELM_SETTLE_POLL_SECONDS-}"
        NAMESPACE="acme-2590"
        KUBECONFIG_FILE="$probe/kubeconfig"
        export CURIE_SETTLE_STATE="$state" CURIE_SETTLE_LOG="$logf"
        export PATH="$bindir:$PATH"
        export CURIE_HELM_SETTLE_WAIT_SECONDS=0 CURIE_HELM_SETTLE_POLL_SECONDS=0
        printf 'pending-upgrade\n' >"$state/statuses"
        if ! settle_helm_operation; then
            log "self-test: pending helm with a missing namespace did not settle"
            failed=1
        elif [[ "$(cat "$state/phase" 2>/dev/null)" != "Active" || ! -f "$state/rolled" ]]; then
            log "self-test: pending helm must create the namespace and roll back"
            failed=1
        elif ! grep -q 'create namespace acme-2590' "$logf"; then
            log "self-test: namespace create was not issued"
            failed=1
        else
            log "pending helm created the namespace and rolled back"
        fi
        rm -f "$state/rolled" "$state/phase" "$state/lookup_fail" "$state/status_fail" \
            "$state/create_fail" "$logf" "$state/phases" "$state/statuses"
        printf 'Active' >"$state/phase"
        printf 'deployed\n' >"$state/statuses"
        if ! settle_helm_operation; then
            log "self-test: a deployed release in an Active namespace was refused"
            failed=1
        elif [[ -f "$state/rolled" ]] || grep -q 'create namespace' "$logf"; then
            log "self-test: a healthy release must not create a namespace or roll back"
            failed=1
        else
            log "healthy release left the namespace and helm release alone"
        fi
        rm -f "$state/rolled" "$state/phase" "$logf" "$state/statuses"
        printf 'Terminating\nabsent\n' >"$state/phases"
        printf 'missing\n' >"$state/statuses"
        export CURIE_HELM_SETTLE_WAIT_SECONDS=5 CURIE_HELM_SETTLE_POLL_SECONDS=0
        if ! settle_helm_operation; then
            log "self-test: a terminating namespace that disappeared was refused"
            failed=1
        elif [[ -f "$state/rolled" || "$(cat "$state/phase" 2>/dev/null)" != "Active" ]]; then
            log "self-test: a finished terminating namespace must be created without a rollback"
            failed=1
        else
            log "terminating namespace was created after it disappeared"
        fi
        rm -f "$state/rolled" "$state/phase" "$logf" "$state/phases" "$state/statuses"
        printf 'Active' >"$state/phase"
        printf 'pending-upgrade\ndeployed\n' >"$state/statuses"
        if ! settle_helm_operation; then
            log "self-test: a pending upgrade that finished was refused"
            failed=1
        elif [[ -f "$state/rolled" ]]; then
            log "self-test: a pending upgrade that finished inside the wait was rolled back"
            failed=1
        else
            log "pending upgrade that finished inside the wait was left in place"
        fi
        rm -f "$state/rolled" "$logf" "$state/statuses"
        printf 'Active' >"$state/phase"
        touch "$state/lookup_fail"
        if settle_helm_operation; then
            log "self-test: a failed namespace lookup was treated as settled"
            failed=1
        elif grep -q 'create namespace' "$logf"; then
            log "self-test: a failed namespace lookup created a namespace"
            failed=1
        else
            log "failed namespace lookup stopped the next upgrade"
        fi
        rm -f "$state/lookup_fail" "$state/rolled" "$logf"
        touch "$state/status_fail"
        if settle_helm_operation; then
            log "self-test: a failed helm status was treated as settled"
            failed=1
        elif [[ -f "$state/rolled" ]]; then
            log "self-test: a failed helm status rolled the release back"
            failed=1
        else
            log "failed helm status stopped the next upgrade"
        fi
        rm -f "$state/status_fail" "$state/rolled" "$logf" "$state/statuses"
        printf 'Active' >"$state/phase"
        printf 'deployed\n' >"$state/statuses"
        local errexit_out
        errexit_out="$probe/errexit.out"
        (
            set +e
            settle_helm_operation
            false
            printf 'continued:%s\n' "$?"
        ) >"$errexit_out"
        if [[ "$(cat "$errexit_out")" != "continued:1" ]]; then
            log "self-test: settlement turned errexit back on before the expected upgrade failure"
            failed=1
        else
            log "settlement left a disabled errexit disabled"
        fi
        rm -f "$state/lookup_fail" "$logf"
        touch "$state/lookup_fail"
        errexit_out="$probe/errexit-on.out"
        set +e
        (
            set -e
            settle_helm_operation
            echo "next:$-:$?" >&2
            printf 'continued\n'
        ) >"$errexit_out"
        set -e
        if grep -q continued "$errexit_out"; then
            log "self-test: a settle failure did not stop a caller that has errexit on"
            failed=1
        else
            log "a settle failure still stops a caller that has errexit on"
        fi
        export CURIE_SETTLE_KIND_CLUSTER=curie-matrix-adopted
        printf 'kind-%s' "$CURIE_SETTLE_KIND_CLUSTER" >"$state/current-context"
        printf 'https://127.0.0.1:6443' >"$state/server"
        cp "$state/server" "$state/kind-server"
        : >"$logf"
        if (
            KIND_CLUSTER="$CURIE_SETTLE_KIND_CLUSTER"
            EVIDENCE_DIR="$probe/evidence"
            FORCE=0
            OWNED_KIND=1
            ensure_kind
            (( OWNED_KIND == 0 )) || die "self-test: ensure_kind did not adopt the named cluster"
            prepare_takeover_context
            [[ "$(cat "$state/current-context")" == k8 ]] || die "self-test: adopted kind context was not renamed"
            [[ "${KUBECTL_CONTEXT_ARGS[*]}" == '--context k8' && "${HELM_CONTEXT_ARGS[*]}" == '--kube-context k8' ]] \
                || die "self-test: adopted kind context was not passed to both clients"
            (( OWNED_KIND == 0 )) || die "self-test: context preparation changed cluster cleanup ownership"
        ); then
            if grep -Fq -- "--kubeconfig $KUBECONFIG_FILE config rename-context kind-$CURIE_SETTLE_KIND_CLUSTER k8" "$logf"; then
                log "adopted named kind cluster prepared private context k8"
            else
                log "self-test: adopted kind context preparation did not select the private kubeconfig"
                failed=1
            fi
        else
            log "self-test: a verified adopted named kind cluster could not prepare context k8"
            failed=1
        fi
        printf 'other-context' >"$state/current-context"
        : >"$logf"
        if (
            KIND_CLUSTER="$CURIE_SETTLE_KIND_CLUSTER"
            OWNED_KIND=0
            prepare_takeover_context
        ); then
            log "self-test: an invalid adopted kind kubeconfig was accepted"
            failed=1
        elif grep -Fq 'rename-context' "$logf" || [[ "$(cat "$state/current-context")" != other-context ]]; then
            log "self-test: invalid kind kubeconfig was changed before refusal"
            failed=1
        else
            log "invalid kind kubeconfig refused before selecting k8"
        fi
        printf 'kind-%s' "$CURIE_SETTLE_KIND_CLUSTER" >"$state/current-context"
        printf 'https://127.0.0.1:7443' >"$state/server"
        : >"$logf"
        if (
            KIND_CLUSTER="$CURIE_SETTLE_KIND_CLUSTER"
            OWNED_KIND=0
            prepare_takeover_context
        ); then
            log "self-test: a mismatched adopted kind server was accepted"
            failed=1
        elif grep -Fq 'rename-context' "$logf"; then
            log "self-test: mismatched kind server was renamed before refusal"
            failed=1
        else
            log "mismatched kind server refused before selecting k8"
        fi
        NAMESPACE="$saved_ns"
        KUBECONFIG_FILE="$saved_kube"
        export PATH="$saved_path"
        if [[ -n "$saved_wait" ]]; then
            export CURIE_HELM_SETTLE_WAIT_SECONDS="$saved_wait"
        else
            unset CURIE_HELM_SETTLE_WAIT_SECONDS
        fi
        if [[ -n "$saved_poll" ]]; then
            export CURIE_HELM_SETTLE_POLL_SECONDS="$saved_poll"
        else
            unset CURIE_HELM_SETTLE_POLL_SECONDS
        fi
        unset CURIE_SETTLE_STATE CURIE_SETTLE_LOG CURIE_SETTLE_KIND_CLUSTER
        rm -rf "$probe"
    fi
    (( failed == 0 )) || die "self-test failed"
    log "self-test passed"
    if (( JSON )); then
        printf '{"status":"self-test","issue":2590,"published":"0.8.8","n":"0.10.0","n1":"0.10.1"}\n'
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
    die "CURIE_BIN must name an executable candidate curie built from this checkout"
}

candidate_identity() {
    CANDIDATE="$(git -C "$REPO_ROOT" rev-parse HEAD)"
}

kubectl_ns() {
    kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" -n "$NAMESPACE" "$@"
}

helm_ns() {
    helm --kubeconfig "$KUBECONFIG_FILE" "${HELM_CONTEXT_ARGS[@]}" -n "$NAMESPACE" "$@"
}

# Prints the phase and returns 0 when the namespace exists, 2 when it is
# absent, and 1 when kubectl itself fails. Absence is NotFound only. A
# connection error must not look like a missing namespace. Status is captured
# in an AND/OR list so this does not change the caller's errexit setting.
lookup_namespace() {
    local err rc out
    err="$(mktemp)"
    out="$(kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" get namespace "$NAMESPACE" \
        -o jsonpath='{.status.phase}' 2>"$err")" && rc=0 || rc=$?
    if (( rc == 0 )); then
        printf '%s' "$out"
        rm -f "$err"
        return 0
    fi
    if grep -qi 'NotFound' "$err"; then
        rm -f "$err"
        return 2
    fi
    log "namespace lookup failed: $(tr '\n' ' ' <"$err")"
    rm -f "$err"
    return 1
}

# Prints the Helm status. Returns 0 and an empty string when the release is
# absent. Returns 1 when helm itself fails, including a missing namespace.
read_helm_settle_status() {
    local err rc raw
    err="$(mktemp)"
    raw="$(helm_ns status "$RELEASE" -o json 2>"$err")" && rc=0 || rc=$?
    if (( rc != 0 )); then
        if grep -qi 'release: not found' "$err"; then
            rm -f "$err"
            return 0
        fi
        log "helm status failed: $(tr '\n' ' ' <"$err")"
        rm -f "$err"
        return 1
    fi
    rm -f "$err"
    printf '%s' "$raw" | python3 -c 'import json,sys
try:
    print((json.load(sys.stdin).get("info") or {}).get("status") or "")
except Exception:
    raise SystemExit(1)'
}

# Helm upgrade does not create the release namespace. A shard that assumes
# acme-2590 still exists fails the next step when the previous operation
# removed it. A Terminating namespace that finishes inside the wait is created
# again. A lookup or create failure is returned to the caller.
ensure_namespace() {
    local phase rc wait_s deadline poll err
    wait_s="${CURIE_HELM_SETTLE_WAIT_SECONDS:-30}"
    poll="${CURIE_HELM_SETTLE_POLL_SECONDS:-1}"
    deadline=$((SECONDS + wait_s))
    while true; do
        phase="$(lookup_namespace)" && rc=0 || rc=$?
        if (( rc == 1 )); then
            return 1
        fi
        if (( rc == 0 )) && [[ "$phase" == "Active" ]]; then
            return 0
        fi
        if (( rc == 2 )); then
            break
        fi
        if (( SECONDS >= deadline )); then
            log "namespace $NAMESPACE is ${phase:-unknown}; refusing to use it"
            return 1
        fi
        sleep "$poll"
    done
    log "creating namespace $NAMESPACE"
    err="$(mktemp)"
    kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" create namespace "$NAMESPACE" 2>"$err" && rc=0 || rc=$?
    if (( rc != 0 )); then
        log "namespace create failed: $(tr '\n' ' ' <"$err")"
        rm -f "$err"
        return 1
    fi
    rm -f "$err"
}

# A killed `helm upgrade` leaves the release pending, and the next upgrade
# then fails with "another operation is in progress" (run 36125059536, s14).
# Poll while a live operation can finish, then roll back a stuck pending
# release. A deployed release returns without a rollback. Helm and kubectl
# failures return to the caller instead of looking like a settled release.
settle_helm_operation() {
    local wait_s deadline poll st rc
    ensure_namespace || return $?
    wait_s="${CURIE_HELM_SETTLE_WAIT_SECONDS:-30}"
    poll="${CURIE_HELM_SETTLE_POLL_SECONDS:-1}"
    deadline=$((SECONDS + wait_s))
    st=""
    while true; do
        st="$(read_helm_settle_status)" && rc=0 || rc=$?
        if (( rc != 0 )); then
            return "$rc"
        fi
        case "$st" in
            pending-upgrade|pending-rollback|pending-install|uninstalling)
                if (( SECONDS >= deadline )); then
                    break
                fi
                sleep "$poll"
                ;;
            *)
                return 0
                ;;
        esac
    done
    log "helm status stayed ${st:-unknown}; rolling back so the next upgrade can start"
    recover_helm_lock
    deadline=$((SECONDS + wait_s))
    while true; do
        st="$(read_helm_settle_status)" && rc=0 || rc=$?
        if (( rc != 0 )); then
            return "$rc"
        fi
        case "$st" in
            pending-upgrade|pending-rollback|pending-install|uninstalling)
                if (( SECONDS >= deadline )); then
                    log "helm status stayed $st after rollback"
                    return 1
                fi
                sleep "$poll"
                ;;
            *)
                return 0
                ;;
        esac
    done
}

fullname() {
    printf '%s-curie' "$RELEASE"
}

# record_timing_row <label> <scenario> <phases> <outcome> <elapsed>
record_timing_row() {
    local label="$1" name="$2" phases="$3" outcome="$4" elapsed="$5"
    log "$label phases=$phases outcome=$outcome elapsed_seconds=$elapsed"
    mkdir -p "$EVIDENCE_DIR" 2>/dev/null || true
    python3 -c '
import json, sys
print(json.dumps({"scenario": sys.argv[1], "phases": sys.argv[2], "outcome": sys.argv[3],
                  "elapsed_seconds": int(sys.argv[4]), "shard": sys.argv[5] or None}))
' "$name" "$phases" "$outcome" "$elapsed" "$SHARD" >>"$EVIDENCE_DIR/scenarios.jsonl" 2>/dev/null || true
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        printf '| %s | %s | %s | %s | %s |\n' "${SHARD:-serial}" "$name" "$phases" "$outcome" "$elapsed" \
            >>"$GITHUB_STEP_SUMMARY" 2>/dev/null || true
    fi
}

# run_timed <label> <scenario> <phases|all> <fn...>
# No if/|| around the call: set -e must stay live inside the scenario. A
# failure exits the script and cleanup() records the failed row from
# CURRENT_LABEL.
run_timed() {
    CURRENT_LABEL="$1"
    CURRENT_NAME="$2"
    CURRENT_PHASES="$3"
    shift 3
    CURRENT_STARTED=$SECONDS
    "$@"
    record_timing_row "$CURRENT_LABEL" "$CURRENT_NAME" "$CURRENT_PHASES" passed $((SECONDS - CURRENT_STARTED))
    CURRENT_LABEL=""
}

run_scenario_timed() {
    local name="$1" phases="$2"
    shift 2
    run_timed "scenario=$name" "$name" "$phases" "$@"
}

cleanup() {
    local status=$? restore_failed=0
    restore_retire_runner_tags || restore_failed=1
    if [[ -n "$APPLY_KILL_PID" ]]; then
        sigkill_tree "$APPLY_KILL_PID"
        wait "$APPLY_KILL_PID" 2>/dev/null || true
        APPLY_KILL_PID=""
    fi
    if [[ -n "$ROLLBACK_SERVING_HELM_PID" ]]; then
        kill -9 "$ROLLBACK_SERVING_HELM_PID" 2>/dev/null || true
        wait "$ROLLBACK_SERVING_HELM_PID" 2>/dev/null || true
        ROLLBACK_SERVING_HELM_PID=""
    fi
    if [[ -n "$CURRENT_LABEL" ]]; then
        record_timing_row "$CURRENT_LABEL" "$CURRENT_NAME" "$CURRENT_PHASES" failed $((SECONDS - CURRENT_STARTED))
        CURRENT_LABEL=""
    fi
    if (( KEEP )); then
        log "keeping owned resources (kind=$KIND_CLUSTER ns=$NAMESPACE release=$RELEASE)"
        (( restore_failed == 0 )) || exit 1
        return 0
    fi
    if (( OWNED_HELM )); then
        refuse_soak "$NAMESPACE" "$RELEASE"
        log "uninstalling release $RELEASE in $NAMESPACE"
        helm_ns uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
        kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
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
    if (( restore_failed )); then
        log "runner-layer-retire could not restore its host image tags"
        exit 1
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
    # --retry alone does not retry a connection reset (curl exit 35). That
    # reset dropped the 0.8.9 binary on upgrade shard s03.
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 -o "$dest" "$url"
    verify_sha256 "$sha" "$dest"
}

fetch_published() {
    ASSET_DIR="$WORKDIR/assets"
    mkdir -p "$ASSET_DIR"
    download_pin "$REL_088/curie-0.8.8.tgz" "$ASSET_DIR/curie-0.8.8.tgz" "$CHART_088_SHA"
    download_pin "$REL_088/curie-x86_64-unknown-linux-gnu" "$ASSET_DIR/curie-0.8.8" "$CLI_088_SHA"
    chmod +x "$ASSET_DIR/curie-0.8.8"
    download_pin "$REL_089/curie-0.8.9.tgz" "$ASSET_DIR/curie-0.8.9.tgz" "$CHART_089_SHA"
    download_pin "$REL_089/curie-x86_64-unknown-linux-gnu" "$ASSET_DIR/curie-0.8.9" "$CLI_089_SHA"
    chmod +x "$ASSET_DIR/curie-0.8.9"
    [[ "$(helm show chart "$ASSET_DIR/curie-0.8.8.tgz" | awk '$1 == "version:" {print $2}')" == 0.8.8 ]] \
        || die "published chart tgz is not version 0.8.8"
    [[ "$(helm show chart "$ASSET_DIR/curie-0.8.9.tgz" | awk '$1 == "version:" {print $2}')" == 0.8.9 ]] \
        || die "published chart tgz is not version 0.8.9"
    if tar -tzf "$ASSET_DIR/curie-0.8.8.tgz" | grep -Eq 'templates/schema-compat.yaml|files/schema-compat.json'; then
        die "published v0.8.8 chart unexpectedly carries schema compatibility metadata"
    fi
    if tar -tzf "$ASSET_DIR/curie-0.8.9.tgz" | grep -Eq 'templates/schema-compat.yaml|files/schema-compat.json'; then
        die "published v0.8.9 chart unexpectedly carries schema compatibility metadata"
    fi
    log "published v0.8.8 and v0.8.9 charts and CLIs verified without schema compatibility metadata"
}

package_n_charts() {
    mkdir -p "$WORKDIR/charts"
    helm package "$REPO_ROOT/charts/curie" --version 0.10.0 --app-version 0.10.0 -d "$WORKDIR/charts" >/dev/null
    helm package "$REPO_ROOT/charts/curie" --version 0.10.1 --app-version 0.10.1 -d "$WORKDIR/charts" >/dev/null
    CHART_0100="$WORKDIR/charts/curie-0.10.0.tgz"
    CHART_0101="$WORKDIR/charts/curie-0.10.1.tgz"
    [[ -f "$CHART_0100" && -f "$CHART_0101" ]] || die "helm package did not write 0.10.0/0.10.1 archives"
    [[ "$(helm show chart "$CHART_0100" | awk '$1 == "version:" {print $2}')" == 0.10.0 ]] \
        || die "packaged 0.10.0 chart version mismatch"
    [[ "$(helm show chart "$CHART_0101" | awk '$1 == "version:" {print $2}')" == 0.10.1 ]] \
        || die "packaged 0.10.1 chart version mismatch"
    log "packaged N=0.10.0 and N+1=0.10.1 from this checkout"
}

kubeconfig_is_named_kind() {
    local ctx server expected
    ctx="$(kubectl --kubeconfig "$KUBECONFIG_FILE" config current-context 2>/dev/null || true)"
    [[ "$ctx" == "kind-${KIND_CLUSTER}" ]] || return 1
    server="$(kubectl --kubeconfig "$KUBECONFIG_FILE" config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
    expected="$(kind get kubeconfig --name "$KIND_CLUSTER" 2>/dev/null | awk '/server:/{print $2; exit}')"
    [[ -n "$server" && -n "$expected" && "$server" == "$expected" ]]
}

ensure_kind() {
    EXCLUSIVE_KIND_TAG=""
    mkdir -p "$(dirname "$KUBECONFIG_FILE")" "$EVIDENCE_DIR"
    if kind get clusters 2>/dev/null | grep -Fxq "$KIND_CLUSTER"; then
        if (( FORCE )); then
            log "recreating leftover kind cluster $KIND_CLUSTER"
            kind delete cluster --name "$KIND_CLUSTER"
        else
            log "adopting existing kind cluster $KIND_CLUSTER"
            kind get kubeconfig --name "$KIND_CLUSTER" >"$KUBECONFIG_FILE"
            kubeconfig_is_named_kind \
                || die "kind cluster $KIND_CLUSTER exists but kubeconfig $KUBECONFIG_FILE is not that cluster; pass --force to recreate this task-owned cluster only"
            OWNED_KIND=0
            return 0
        fi
    fi
    kind create cluster --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
    kubeconfig_is_named_kind || die "created kind cluster $KIND_CLUSTER but kubeconfig does not select it"
    OWNED_KIND=1
}

prepare_takeover_context() {
    kubeconfig_is_named_kind || die "apply-kill-takeover private kubeconfig does not select the named kind cluster"
    kubectl --kubeconfig "$KUBECONFIG_FILE" config rename-context "kind-$KIND_CLUSTER" k8 >/dev/null
    KUBECTL_CONTEXT_ARGS=(--context k8)
    HELM_CONTEXT_ARGS=(--kube-context k8)
    local server expected
    server="$(kubectl --kubeconfig "$KUBECONFIG_FILE" --context k8 config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
    expected="$(kind get kubeconfig --name "$KIND_CLUSTER" | awk '/server:/{print $2; exit}')"
    [[ -n "$server" && "$server" == "$expected" ]] || die "private k8 context does not point to the named kind cluster"
    [[ "$(kubectl --kubeconfig "$KUBECONFIG_FILE" config current-context)" == k8 ]] \
        || die "private kubeconfig did not select context k8"
    log "private context k8 selects kind cluster $KIND_CLUSTER"
}

image_for() {
    local name="$1" tag="$2"
    printf 'ghcr.io/curie-eng/%s:%s' "$name" "$tag"
}

IMAGES=(curie-api curie-worker curie-dispatcher curie-ui curie-runner)

retag_candidate_versions() {
    local src_tag="$1" version="$2" required="${3:-optional}" img src dest short
    EXCLUSIVE_KIND_TAG=""
    for img in "${IMAGES[@]}"; do
        src="$(image_for "$img" "$src_tag")"
        if docker image inspect "$src" >/dev/null 2>&1; then
            :
        elif docker image inspect "${img}:${src_tag}" >/dev/null 2>&1; then
            src="${img}:${src_tag}"
        else
            if [[ "$required" == required ]]; then
                die "required source image $img:$src_tag is not local"
            fi
            log "candidate image $img:$src_tag is not local; skipping retag"
            continue
        fi
        dest="$(image_for "$img" "$version")"
        short="${img}:${version}"
        docker tag "$src" "$dest"
        docker tag "$src" "$short"
        log "tagged $src as $dest and $short"
    done
}

load_tag_images() {
    local tag="$1" img ref
    EXCLUSIVE_KIND_TAG=""
    for img in "${IMAGES[@]}"; do
        ref="$(image_for "$img" "$tag")"
        if docker image inspect "$ref" >/dev/null 2>&1; then
            :
        elif docker image inspect "${img}:${tag}" >/dev/null 2>&1; then
            ref="${img}:${tag}"
        else
            log "pulling $ref"
            docker pull "$ref"
        fi
        log "kind load $ref"
        kind load docker-image "$ref" --name "$KIND_CLUSTER"
        if docker image inspect "${img}:${tag}" >/dev/null 2>&1; then
            kind load docker-image "${img}:${tag}" --name "$KIND_CLUSTER" || true
        fi
    done
}

prepare_candidate_images() {
    local src="${CANDIDATE_TAG:-upgrade-candidate}"
    retag_candidate_versions "$src" "0.10.0"
    load_tag_images "0.8.8"
    load_tag_images "0.10.0"
    retag_candidate_versions "0.10.0" "0.10.1" required
    # Do not load 0.10.0 and 0.10.1 together: they are the same digest in CI,
    # and converge refuses a tagged alias with more than one name.
    exclusive_kind_tag "0.10.0"
}

kind_node() {
    kind get nodes --name "$KIND_CLUSTER" 2>/dev/null | head -1
}

untag_kind_siblings() {
    local keep="$1" node img tag ref
    EXCLUSIVE_KIND_TAG=""
    node="$(kind_node)"
    [[ -n "$node" ]] || return 0
    for img in "${IMAGES[@]}"; do
        for tag in 0.10.0 0.10.1 matrix-candidate upgrade-candidate; do
            [[ "$tag" == "$keep" ]] && continue
            for ref in \
                "ghcr.io/curie-eng/${img}:${tag}" \
                "docker.io/library/${img}:${tag}" \
                "docker.io/curie-eng/${img}:${tag}" \
                "${img}:${tag}"; do
                docker exec "$node" crictl rmi "$ref" >/dev/null 2>&1 || true
                docker exec "$node" ctr -n k8s.io images untag "$ref" >/dev/null 2>&1 || true
            done
        done
    done
}

exclusive_kind_tag() {
    local keep="$1" node img ref
    if [[ -n "$keep" && "$EXCLUSIVE_KIND_TAG" == "$keep" ]]; then
        log "kind node already holds exclusive app tag $keep"
        return 0
    fi
    node="$(kind_node)"
    [[ -n "$node" ]] || return 0
    # Untag siblings before load. 0.10.0 and 0.10.1 are the same digest in CI;
    # loading 0.10.1 while 0.10.0 remains makes converge refuse the alias.
    untag_kind_siblings "$keep"
    for img in "${IMAGES[@]}"; do
        ref="$(image_for "$img" "$keep")"
        if docker image inspect "$ref" >/dev/null 2>&1; then
            :
        elif docker image inspect "${img}:${keep}" >/dev/null 2>&1; then
            ref="${img}:${keep}"
        else
            continue
        fi
        log "kind load $ref"
        kind load docker-image "$ref" --name "$KIND_CLUSTER"
    done
    untag_kind_siblings "$keep"
    EXCLUSIVE_KIND_TAG="$keep"
    log "kind node $node holds exclusive app tag $keep"
}

image_sets() {
    local pull_policy="$1"
    # Do not override repository or tag: empty tag follows chart appVersion, so
    # a published 0.8.8 install stays on :0.8.8 and a packaged 0.10.0/0.10.1
    # chart supplies those tags. cluster upgrade has no --set, so the overlay
    # must not pin a stale repository/tag.
    # Record the chart's published development caller pair so subsequent
    # upgrades retain the same caller identity.
    cat <<EOF
security.gvisor.mode=off
security.allowDevDefaults=true
connectorCaller.signingKey=Y3VyaWUtZGV2LWNvbm5lY3Rvci1jYWxsZXItc2VlZCE=
connectorCaller.verifyKey=tkmNbO5SSLE0IM84sH4uJ94DxtriNZ/APXha3FiyP6c=
api.migrate.enabled=true
worker.replicas=1
worker.image.pullPolicy=${pull_policy}
api.image.pullPolicy=${pull_policy}
dispatcher.image.pullPolicy=${pull_policy}
dispatcher.deploy=false
ui.image.pullPolicy=${pull_policy}
ui.deploy=false
agentSandbox.runner.imagePullPolicy=${pull_policy}
agentSandbox.runner.prewarm.enabled=false
agentSandbox.runner.prewarm.imagePullPolicy=${pull_policy}
langfuse.deploy=false
langfuse.host=langfuse.example.com
clickhouse.deploy=false
mailAdapter.deploy=false
otelCollector.deploy=false
otelCollector.telemetryDisabled=true
EOF
}

json_field() {
    local file="$1" field="$2"
    python3 -c '
import json,sys
raw=sys.stdin.read().strip()
if not raw:
    raise SystemExit("empty json")
line=raw.splitlines()[-1]
d=json.loads(line)
val=d
for part in sys.argv[1].split("."):
    if isinstance(val, dict):
        val=val.get(part)
    else:
        val=None
        break
if val is True:
    print("true")
elif val is False:
    print("false")
elif val is None:
    print("")
else:
    print(val)
' "$field" <"$file"
}

helm_version() {
    helm_ns get metadata "$RELEASE" -o json 2>/dev/null | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("version") or "")
except Exception:
    print("")'
}

helm_release_status() {
    helm_ns status "$RELEASE" -o json 2>/dev/null | python3 -c 'import json,sys
try:
    print((json.load(sys.stdin).get("info") or {}).get("status") or "")
except Exception:
    print("")'
}

helm_revision() {
    helm_ns status "$RELEASE" -o json 2>/dev/null | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("version") or "")
except Exception:
    print("")'
}

wait_rollout() {
    local deploy
    for deploy in api worker; do
        kubectl_ns rollout status "deploy/$(fullname)-$deploy" --timeout=300s
    done
}

api_health() {
    kubectl_ns exec "deploy/$(fullname)-api" -c api -- python -c \
        'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8000/health").read().decode())'
}

postgres_exec() {
    local sql="$1"
    kubectl_ns exec "$(fullname)-postgres-0" -- \
        psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "$sql"
}

insert_sentinel() {
    postgres_exec "CREATE TABLE IF NOT EXISTS upgrade_matrix_sentinel (id text primary key, v text); INSERT INTO upgrade_matrix_sentinel (id, v) VALUES ('${SENTINEL_ID}', 'pre-upgrade') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;"
}

read_sentinel() {
    kubectl_ns exec "$(fullname)-postgres-0" -- \
        psql -U postgres -d postgres -tA -c "SELECT v FROM upgrade_matrix_sentinel WHERE id = '${SENTINEL_ID}';"
}

assert_sentinel() {
    local got
    got="$(read_sentinel | tr -d '[:space:]')"
    [[ "$got" == "pre-upgrade" ]] || die "sentinel missing or changed (got '$got')"
    log "sentinel row unchanged"
}

assert_review_feedback_table() {
    local got
    got="$(kubectl_ns exec "$(fullname)-postgres-0" -- \
        psql -U postgres -d postgres -tA -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'curie' AND table_name = 'github_review_feedback';" | tr -d '[:space:]')"
    [[ "$got" == "1" ]] || die "curie.github_review_feedback table missing (count '$got')"
    log "curie.github_review_feedback exists"
}

alembic_current() {
    kubectl_ns exec "deploy/$(fullname)-api" -c api -- alembic -c alembic.ini current
}

assert_alembic() {
    local want="$1" got
    got="$(alembic_current || true)"
    echo "$got" | grep -q "$want" || die "alembic current '$got' does not contain $want"
    log "alembic current contains $want"
}

uninstall_owned() {
    if helm_ns status "$RELEASE" >/dev/null 2>&1; then
        helm_ns uninstall "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
    fi
    kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1 || true
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        if ! kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" get namespace "$NAMESPACE" >/dev/null 2>&1; then
            break
        fi
        sleep 2
    done
    OWNED_HELM=0
}

helm_install_088() {
    refuse_soak "$NAMESPACE" "$RELEASE"
    load_tag_images "0.8.8"
    kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" create namespace "$NAMESPACE" >/dev/null 2>&1 || true
    local sets=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && sets+=(--set "$line")
    done < <(image_sets "IfNotPresent")
    log "helm install published 0.8.8 ns=$NAMESPACE release=$RELEASE"
    helm_ns install "$RELEASE" "$ASSET_DIR/curie-0.8.8.tgz" \
        --create-namespace \
        --wait --timeout 15m \
        "${sets[@]}"
    OWNED_HELM=1
    wait_rollout
    REV_088="$(helm_revision)"
    log "published 0.8.8 helm revision $REV_088"
}

helm_install_089() {
    refuse_soak "$NAMESPACE" "$RELEASE"
    local img ref
    for img in "${IMAGES[@]}"; do
        ref="$(image_for "$img" "0.8.9")"
        log "refreshing published image $ref"
        docker pull "$ref"
    done
    load_tag_images "0.8.9"
    kubectl --kubeconfig "$KUBECONFIG_FILE" "${KUBECTL_CONTEXT_ARGS[@]}" create namespace "$NAMESPACE" >/dev/null 2>&1 || true
    local sets=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && sets+=(--set "$line")
    done < <(image_sets "IfNotPresent")
    log "helm install published 0.8.9 ns=$NAMESPACE release=$RELEASE"
    helm_ns install "$RELEASE" "$ASSET_DIR/curie-0.8.9.tgz" \
        --create-namespace \
        --wait --timeout 15m \
        "${sets[@]}"
    OWNED_HELM=1
    wait_rollout
    REV_089="$(helm_revision)"
    log "published 0.8.9 helm revision $REV_089"
}

cluster_up_n() {
    local chart="$1"
    refuse_soak "$NAMESPACE" "$RELEASE"
    exclusive_kind_tag "0.10.0"
    local sets=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && sets+=(--set "$line")
    done < <(image_sets "Never")
    log "cluster up packaged chart=$chart ns=$NAMESPACE release=$RELEASE"
    "$BIN" cluster up \
        --namespace "$NAMESPACE" \
        --release "$RELEASE" \
        --chart "$chart" \
        --dev \
        --no-expose \
        "${sets[@]}"
    OWNED_HELM=1
    wait_rollout
}

cluster_upgrade() {
    local to="$1" chart="$2"
    shift 2
    local extra=("$@")
    refuse_soak "$NAMESPACE" "$RELEASE"
    settle_helm_operation || return $?
    if [[ "$to" == "0.10.0" || "$to" == "0.10.1" ]]; then
        exclusive_kind_tag "$to"
    fi
    local out status=0
    local saved_fail="${CURIE_UPGRADE_TEST_FAIL_AT-}"
    local saved_interrupt="${CURIE_UPGRADE_TEST_INTERRUPT_AFTER-}"
    if [[ -n "$FAIL_AT_HOOK" ]]; then
        export CURIE_UPGRADE_TEST_FAIL_AT="$FAIL_AT_HOOK"
    else
        unset CURIE_UPGRADE_TEST_FAIL_AT || true
    fi
    if [[ -n "$INTERRUPT_AFTER_HOOK" ]]; then
        export CURIE_UPGRADE_TEST_INTERRUPT_AFTER="$INTERRUPT_AFTER_HOOK"
    else
        unset CURIE_UPGRADE_TEST_INTERRUPT_AFTER || true
    fi
    log "cluster upgrade --to $to chart=$chart fail_at=${CURIE_UPGRADE_TEST_FAIL_AT-} interrupt_after=${CURIE_UPGRADE_TEST_INTERRUPT_AFTER-}"
    local cmd=("$BIN" --json cluster upgrade --yes --to "$to"
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$chart")
    cmd+=("${extra[@]}")
    if [[ -n "$FAIL_AT_HOOK" || -n "$INTERRUPT_AFTER_HOOK" ]]; then
        local envcmd=(/usr/bin/env)
        [[ -n "$FAIL_AT_HOOK" ]] && envcmd+=("CURIE_UPGRADE_TEST_FAIL_AT=$FAIL_AT_HOOK")
        [[ -n "$INTERRUPT_AFTER_HOOK" ]] && envcmd+=("CURIE_UPGRADE_TEST_INTERRUPT_AFTER=$INTERRUPT_AFTER_HOOK")
        envcmd+=("${cmd[@]}")
        cmd=("${envcmd[@]}")
    fi
    log "upgrade argv: ${cmd[*]}"
    if "${cmd[@]}" >"$EVIDENCE_DIR/last-upgrade.json" 2>"$EVIDENCE_DIR/last-upgrade.err"; then
        status=0
    else
        status=$?
    fi
    out="$(cat "$EVIDENCE_DIR/last-upgrade.json" 2>/dev/null || true)"
    if [[ -n "$saved_fail" ]]; then
        export CURIE_UPGRADE_TEST_FAIL_AT="$saved_fail"
    else
        unset CURIE_UPGRADE_TEST_FAIL_AT || true
    fi
    if [[ -n "$saved_interrupt" ]]; then
        export CURIE_UPGRADE_TEST_INTERRUPT_AFTER="$saved_interrupt"
    else
        unset CURIE_UPGRADE_TEST_INTERRUPT_AFTER || true
    fi
    printf '%s\n' "$out" | tee "$EVIDENCE_DIR/last-upgrade.json" >/dev/null
    return "$status"
}

record_upgrade_json() {
    local name="$1"
    cp "$EVIDENCE_DIR/last-upgrade.json" "$EVIDENCE_DIR/${name}.json" 2>/dev/null || true
    cp "$EVIDENCE_DIR/last-upgrade.err" "$EVIDENCE_DIR/${name}.err" 2>/dev/null || true
}

assert_upgrade_status() {
    local want="$1"
    local got
    got="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "status")"
    [[ "$got" == "$want" ]] || die "upgrade status '$got' wanted '$want'"
}

assert_upgrade_phase() {
    local want="$1"
    local got
    got="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "phase")"
    [[ "$got" == "$want" ]] || die "upgrade phase '$got' wanted '$want'"
}

scenario_wanted() {
    local want="$1"
    [[ "$SCENARIO" == "all" || "$SCENARIO" == "$want" ]]
}

run_fresh_n() {
    cluster_up_n "$CHART_0100"
    local ver
    ver="$(helm_version)"
    [[ "$ver" == "0.10.0" ]] || die "fresh-n helm version is '$ver' not 0.10.0"
    wait_rollout
    log "fresh-n helm version 0.10.0"
}

run_n1_to_n() {
    uninstall_owned
    helm_install_088
    insert_sentinel
    assert_sentinel
    assert_alembic "$PUBLISHED_HEAD"
    local status=0
    cluster_upgrade "0.10.0" "$CHART_0100" --forward-only || status=$?
    record_upgrade_json "n1-to-n"
    [[ "$status" -eq 0 ]] || die "n1-to-n cluster upgrade exited $status"
    assert_upgrade_status "succeeded"
    wait_rollout
    ver="$(helm_version)"
    [[ "$ver" == "0.10.0" ]] || die "n1-to-n helm version is '$ver' not 0.10.0"
    assert_sentinel
    assert_review_feedback_table
    assert_alembic "$TARGET_HEAD"
    log "n1-to-n nonempty upgrade kept the sentinel and reached $TARGET_HEAD"
}

run_same_version() {
    local before after status=0
    before="$(helm_revision)"
    cluster_upgrade "0.10.0" "$CHART_0100" || status=$?
    record_upgrade_json "same-version"
    [[ "$status" -eq 0 ]] || die "same-version cluster upgrade exited $status"
    assert_upgrade_status "succeeded"
    after="$(helm_revision)"
    local unchanged
    unchanged="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "unchanged")"
    log "same-version helm revision before=$before after=$after unchanged=$unchanged"
    [[ "$unchanged" == "true" || "$before" == "$after" ]] \
        || log "same-version documented helm revision $before -> $after"
}

run_fail_every_phase() {
    local phase status=0
    local phases
    if [[ -n "${CURIE_E2E_FAIL_PHASES:-}" ]]; then
        read -r -a phases <<< "$CURIE_E2E_FAIL_PHASES"
    else
        phases=("${MATRIX_PHASES[@]}")
    fi
    for phase in "${phases[@]}"; do
        # Later phases (canary/commit) still run converge. Start each row
        # from healthy 0.10.0 so a leftover 0.10.1 apply cannot fail converge
        # before FAIL_AT is reached.
        restore_n
        log "fail-every-phase FAIL_AT=$phase"
        FAIL_AT_HOOK="$phase"
        set +e
        cluster_upgrade "0.10.1" "$CHART_0101"
        status=$?
        set -e
        FAIL_AT_HOOK=""
        record_upgrade_json "fail-$phase"
        local st
        st="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "status" || true)"
        [[ "$st" == "failed" ]] || die "FAIL_AT=$phase status='$st' (need structured status=failed)"
        assert_upgrade_phase "$phase"
        log "fail-every-phase $phase failed as expected"
    done
    if [[ "$(helm_version)" == "0.10.1" ]]; then
        log "fail-every-phase left helm at 0.10.1; retrying to succeed"
        cluster_upgrade "0.10.1" "$CHART_0101" || true
        wait_rollout || true
        cluster_upgrade "0.10.0" "$CHART_0100" || true
        wait_rollout || true
        if [[ "$(helm_version)" == "0.10.1" ]]; then
            "$BIN" --json cluster rollback --yes --namespace "$NAMESPACE" --release "$RELEASE" \
                >"$EVIDENCE_DIR/fail-phase-restore.json" || true
            wait_rollout || true
        fi
    fi
}

clear_upgrade_checkpoint() {
    local cm="${RELEASE}-upgrade-checkpoint"
    if kubectl_ns get configmap "$cm" >/dev/null 2>&1; then
        kubectl_ns delete configmap "$cm" --wait=true >/dev/null
        log "deleted leftover $cm so the next upgrade cannot resume it"
    fi
}

checkpoint_field() {
    local field="$1"
    local cm="${RELEASE}-upgrade-checkpoint"
    kubectl_ns get configmap "$cm" -o jsonpath='{.data.record}' 2>/dev/null | python3 -c '
import json,sys
raw=sys.stdin.read().strip()
if not raw:
    raise SystemExit(0)
try:
    d=json.loads(raw)
except Exception:
    raise SystemExit(0)
val=d.get(sys.argv[1])
if val is None:
    raise SystemExit(0)
print(val)
' "$field" || true
}

recover_helm_lock() {
    local st
    st="$(helm_release_status)"
    case "$st" in
        pending-upgrade|pending-rollback|pending-install)
            log "helm status is $st; rolling back to last deployed revision"
            helm_ns rollback "$RELEASE" --wait --timeout 180s >/dev/null 2>&1 || true
            ;;
    esac
}

helm_revision_for_version() {
    local want="$1"
    helm_ns history "$RELEASE" -o json 2>/dev/null | python3 -c '
import json,sys
want=sys.argv[1]
try:
    hist=json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for row in reversed(list(hist)):
    chart=str(row.get("chart") or "")
    app=str(row.get("app_version") or "")
    if want in chart or app == want:
        print(row.get("revision") or "")
        break
' "$want" || true
}

restore_n() {
    # A leftover in_progress 0.10.1 is a foreign record: resume would keep
    # going to 0.10.1. Delete only that. A leftover in_progress 0.10.0 is
    # resumed below so converge/canary/commit can finish.
    local target status rev
    target="$(checkpoint_field target_version)"
    status="$(checkpoint_field status)"
    recover_helm_lock
    if [[ "$status" == "in_progress" && "$target" == "0.10.1" ]]; then
        clear_upgrade_checkpoint
        target=""
        status=""
    fi
    if [[ "$(helm_version)" != "0.10.0" || ( "$status" == "in_progress" && "$target" == "0.10.0" ) ]]; then
        log "restoring helm 0.10.0 (currently $(helm_version), checkpoint_target=${target:-none} checkpoint_status=${status:-none})"
        if [[ "$status" == "in_progress" && "$target" == "0.10.0" ]]; then
            cluster_upgrade "0.10.0" "$CHART_0100" || true
        else
            # A full cluster upgrade --wait can sit on a hook Job for the
            # whole Helm timeout. Rollback to the last 0.10.0 revision is the
            # harness restore; it is not the product mutator under test.
            rev="$(helm_revision_for_version 0.10.0)"
            if [[ -n "$rev" ]]; then
                # pullPolicy Never: the node may hold exclusive 0.10.1, so load
                # 0.10.0 first or the rollback waits out its whole timeout.
                exclusive_kind_tag "0.10.0"
                log "rolling back to helm revision $rev (0.10.0)"
                helm_ns rollback "$RELEASE" "$rev" --wait --timeout 180s || \
                    cluster_upgrade "0.10.0" "$CHART_0100" || true
            else
                cluster_upgrade "0.10.0" "$CHART_0100" || true
            fi
        fi
        wait_rollout || true
    fi
    exclusive_kind_tag "0.10.0"
    # The restore upgrade itself writes a record. Wipe it so the next
    # FAIL_AT 0.10.1 cannot be refused as "upgrade to 0.10.0 already in progress".
    clear_upgrade_checkpoint
    [[ "$(helm_version)" == "0.10.0" ]] || die "restore_n left helm at $(helm_version)"
}

run_interrupt_resume() {
    local phase status=0
    local phases
    if [[ -n "${CURIE_E2E_INTERRUPT_PHASES:-}" ]]; then
        read -r -a phases <<< "$CURIE_E2E_INTERRUPT_PHASES"
    else
        phases=("${INTERRUPT_PHASES[@]}")
    fi
    for phase in "${phases[@]}"; do
        # A leftover in_progress record for 0.10.1 skips already-completed
        # phases, so INTERRUPT_AFTER=plan can exit 0. Start each row from a
        # clean 0.10.0 with no checkpoint.
        log "interrupt-resume restoring 0.10.0 before $phase"
        restore_n
        log "interrupt-resume INTERRUPT_AFTER=$phase"
        INTERRUPT_AFTER_HOOK="$phase"
        set +e
        cluster_upgrade "0.10.1" "$CHART_0101"
        status=$?
        set -e
        INTERRUPT_AFTER_HOOK=""
        record_upgrade_json "interrupt-$phase"
        (( status != 0 )) || die "INTERRUPT_AFTER=$phase exited 0"
        if [[ "$phase" == "checkpoint" ]]; then
            set +e
            cluster_upgrade "0.10.0" "$CHART_0100"
            local refuse=$?
            set -e
            record_upgrade_json "interrupt-different-to"
            (( refuse != 0 )) || die "different --to while in_progress must be refused"
            log "different --to while in_progress refused"
        fi
        set +e
        cluster_upgrade "0.10.1" "$CHART_0101"
        status=$?
        set -e
        record_upgrade_json "resume-$phase"
        [[ "$status" -eq 0 ]] || die "resume after $phase exited $status"
        assert_upgrade_status "succeeded"
        local resumed
        resumed="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "resumed")"
        [[ "$resumed" == "true" ]] || die "resume after $phase resumed='$resumed'"
        log "interrupt-resume after $phase succeeded resumed=true"
    done
}

run_n_to_n1() {
    # interrupt-resume ends on 0.10.1. A leftover in_progress 0.10.0 from a
    # helm --wait timeout refuses --to 0.10.1. restore_n resumes or clears it.
    restore_n
    local status=0
    cluster_upgrade "0.10.1" "$CHART_0101" || status=$?
    record_upgrade_json "n-to-n1"
    [[ "$status" -eq 0 ]] || die "n-to-n1 exited $status"
    assert_upgrade_status "succeeded"
    wait_rollout
    [[ "$(helm_version)" == "0.10.1" ]] || die "n-to-n1 helm version is $(helm_version)"
    log "n-to-n1 helm version 0.10.1"
}

run_guarded_rollback() {
    local status=0
    # n-to-n1 left exclusive 0.10.1 on the node. Rollback to 0.10.0 cannot
    # pull that tag with pullPolicy Never.
    exclusive_kind_tag "0.10.0"
    set +e
    "$BIN" --json cluster rollback --yes --namespace "$NAMESPACE" --release "$RELEASE" \
        >"$EVIDENCE_DIR/guarded-rollback.json" 2>"$EVIDENCE_DIR/guarded-rollback.err"
    status=$?
    set -e
    (( status != 0 )) || die "rollback to 0.10.0 unexpectedly succeeded at schema $TARGET_HEAD"
    local err
    err="$(cat "$EVIDENCE_DIR/guarded-rollback.json" "$EVIDENCE_DIR/guarded-rollback.err" 2>/dev/null || true)"
    echo "$err" | grep -F "outside its declared schema range" >/dev/null \
        || die "rollback refusal did not identify the schema range: $err"
    echo "$err" | grep -F "$TARGET_HEAD" >/dev/null \
        || die "rollback refusal did not name live schema head $TARGET_HEAD: $err"
    [[ "$(helm_version)" == "0.10.1" ]] || die "refused rollback changed helm version to $(helm_version)"
    kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.readyReplicas}{"\n"}' | grep -vq '^0$' \
        || die "api not Ready after refused rollback"
    api_health >/dev/null || die "api health failed after refused rollback"
    assert_sentinel
    assert_alembic "$TARGET_HEAD"
    log "rollback to 0.10.0 refused at schema $TARGET_HEAD; 0.10.1 serves"
}

helm_history_088() {
    helm_ns history "$RELEASE" -o json 2>/dev/null | python3 -c '
import json,sys
try:
    hist=json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for row in hist:
    chart=str(row.get("chart") or "")
    app=str(row.get("app_version") or "")
    if "0.8.8" in chart or app == "0.8.8":
        print(row.get("revision") or "")
        break
' || true
}

run_rollback_published_088() {
    local found
    found="$(helm_history_088)"
    if [[ -z "$found" ]]; then
        log "0.8.8 helm revision not in history; re-establishing nonempty N-1 then N"
        uninstall_owned
        helm_install_088
        insert_sentinel
        local boot=0
        cluster_upgrade "0.10.0" "$CHART_0100" --forward-only || boot=$?
        record_upgrade_json "rollback-088-bootstrap"
        [[ "$boot" -eq 0 ]] || die "rollback-088 bootstrap 0.8.8->0.10.0 exited $boot"
        wait_rollout
        assert_sentinel
        assert_review_feedback_table
        found="$REV_088"
    fi
    [[ -n "$found" ]] || found="$(helm_history_088)"
    [[ -n "$found" ]] || die "could not find a 0.8.8 helm revision"
    REV_088="$found"
    # exclusive 0.10.x tags never remove 0.8.8, but the node may have dropped
    # the published images after hours of retag. Reload before rollback.
    load_tag_images "0.8.8"
    local status=0
    set +e
    "$BIN" --json cluster rollback --yes --revision "$REV_088" \
        --namespace "$NAMESPACE" --release "$RELEASE" \
        >"$EVIDENCE_DIR/rollback-088.json" 2>"$EVIDENCE_DIR/rollback-088.err"
    status=$?
    set -e
    if (( status == 0 )); then
        wait_rollout || die "rollback-088 rollout timed out (helm $(helm_version))"
        [[ "$(helm_version)" == "0.8.8" ]] || die "rollback-088 helm version is $(helm_version)"
        assert_sentinel
        log "rollback-published-088 succeeded; 0.8.8 serves and sentinel is readable"
    else
        local err
        err="$(cat "$EVIDENCE_DIR/rollback-088.json" "$EVIDENCE_DIR/rollback-088.err" 2>/dev/null || true)"
        echo "$err" | grep -Ei "schema|window|${PUBLISHED_HEAD}|${TARGET_HEAD}" >/dev/null \
            || die "rollback-088 refusal did not name the schema window: $err"
        assert_sentinel
        kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.readyReplicas}{"\n"}' | grep -vq '^0$' \
            || die "0.10.x stopped serving after refused 0.8.8 rollback"
        log "rollback-published-088 refused with schema window; sentinel retained; N still serving"
    fi
}

run_rollback_published_089() {
    uninstall_owned
    helm_install_089
    insert_sentinel
    assert_sentinel
    assert_alembic "$PUBLISHED_HEAD"

    local boot=0
    cluster_upgrade "0.10.0" "$CHART_0100" --forward-only || boot=$?
    record_upgrade_json "rollback-089-bootstrap"
    [[ "$boot" -eq 0 ]] || die "rollback-089 bootstrap 0.8.9 to 0.10.0 exited $boot"
    wait_rollout
    [[ "$(helm_version)" == "0.10.0" ]] || die "rollback-089 bootstrap did not reach 0.10.0"
    assert_sentinel
    assert_alembic "$TARGET_HEAD"

    helm_ns get manifest "$RELEASE" --revision "$REV_089" \
        >"$EVIDENCE_DIR/rollback-089-retained-manifest.yaml"
    if grep -Eq 'app.kubernetes.io/component:[[:space:]]*schema-compat' \
        "$EVIDENCE_DIR/rollback-089-retained-manifest.yaml"; then
        die "published 0.8.9 retained manifest unexpectedly has app.kubernetes.io/component=schema-compat"
    fi
    log "published 0.8.9 retained manifest has no app.kubernetes.io/component=schema-compat object"

    load_tag_images "0.8.9"
    local status=0
    set +e
    "$BIN" --json cluster rollback --yes --revision "$REV_089" \
        --namespace "$NAMESPACE" --release "$RELEASE" \
        >"$EVIDENCE_DIR/rollback-089.json" 2>"$EVIDENCE_DIR/rollback-089.err"
    status=$?
    set -e
    (( status != 0 )) || die "published 0.8.9 rollback unexpectedly succeeded"

    local err
    err="$(cat "$EVIDENCE_DIR/rollback-089.json" "$EVIDENCE_DIR/rollback-089.err" 2>/dev/null || true)"
    echo "$err" | grep -F "0.8.9" >/dev/null \
        || die "rollback-089 refusal did not name 0.8.9: $err"
    echo "$err" | grep -F "$PUBLISHED_HEAD" >/dev/null \
        || die "rollback-089 refusal did not name published head $PUBLISHED_HEAD: $err"
    echo "$err" | grep -F "$TARGET_HEAD" >/dev/null \
        || die "rollback-089 refusal did not name live head $TARGET_HEAD: $err"
    echo "$err" | grep -F "outside its declared schema range" >/dev/null \
        || die "rollback-089 refusal did not name the declared schema range: $err"
    if echo "$err" | grep -F "could not establish" >/dev/null; then
        die "rollback-089 failed identity classification instead of applying the published range: $err"
    fi
    [[ "$(helm_version)" == "0.10.0" ]] \
        || die "refused rollback changed helm from 0.10.0 to $(helm_version)"
    assert_sentinel
    assert_alembic "$TARGET_HEAD"
    kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.readyReplicas}{"\n"}' | grep -vq '^0$' \
        || die "api has no readyReplicas after refused published 0.8.9 rollback"
    api_health >/dev/null || die "api health failed after refused published 0.8.9 rollback"
    log "published 0.8.9 refused at schema head $PUBLISHED_HEAD; 0.10.0 and sentinel remain healthy"

    local advance=0
    cluster_upgrade "0.10.1" "$CHART_0101" || advance=$?
    record_upgrade_json "rollback-089-guarded-setup"
    [[ "$advance" -eq 0 ]] || die "rollback-089 guarded setup exited $advance"
    wait_rollout
    [[ "$(helm_version)" == "0.10.1" ]] || die "rollback-089 guarded setup did not reach 0.10.1"
    run_guarded_rollback
}

schema_migrate_busy() {
    kubectl_ns get jobs,pods -l "app.kubernetes.io/component=schema-migrate" \
        -o name 2>/dev/null | grep -q .
}

interrupt_schema_migrate() {
    kubectl_ns delete jobs,pods -l "app.kubernetes.io/component=schema-migrate" \
        --wait=false --ignore-not-found >/dev/null 2>&1 || true
}

run_migration_crash() {
    uninstall_owned
    helm_install_088
    insert_sentinel
    local pid status=0
    (
        cluster_upgrade "0.10.0" "$CHART_0100" --forward-only
    ) >"$EVIDENCE_DIR/migration-crash-upgrade.log" 2>&1 &
    pid=$!
    local deadline=$((SECONDS + 240)) seen=0
    while (( SECONDS < deadline )); do
        if schema_migrate_busy; then
            seen=1
            kubectl_ns get jobs,pods -l "app.kubernetes.io/component=schema-migrate" \
                -o wide >"$EVIDENCE_DIR/migration-crash-observed.txt" 2>&1 || true
            log "observed schema-migrate Job/pod; deleting it once"
            interrupt_schema_migrate
            break
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 0.2
    done
    if (( seen != 1 )); then
        kubectl_ns get jobs,pods -o wide >"$EVIDENCE_DIR/migration-crash-timeout.txt" 2>&1 || true
        die "schema-migrate Job was not observed running; refusing a no-op retry as crash proof"
    fi
    # helm blocks ~900s on the deleted hook Job. Give the interrupted upgrade
    # 120s to fail on its own, then kill it and recover what the kill leaves.
    local exit_deadline=$((SECONDS + 120))
    while (( SECONDS < exit_deadline )) && kill -0 "$pid" 2>/dev/null; do
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        terminate_tree "$pid"
        local kill_deadline=$((SECONDS + 30))
        while (( SECONDS < kill_deadline )) && kill -0 "$pid" 2>/dev/null; do
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
        wait "$pid" || status=$?
        log "first migration-crash upgrade terminated after 120s (exit $status); recovering ownership and helm lock"
        recover_killed_upgrade_ownership
        recover_helm_lock
    else
        wait "$pid" || status=$?
        log "first migration-crash upgrade exited on its own with $status"
    fi
    log "first migration-crash upgrade exited $status; retrying"
    set +e
    cluster_upgrade "0.10.0" "$CHART_0100" --forward-only
    status=$?
    set -e
    record_upgrade_json "migration-crash-retry"
    [[ "$status" -eq 0 ]] || die "migration-crash retry exited $status"
    assert_upgrade_status "succeeded"
    wait_rollout
    assert_sentinel
    assert_review_feedback_table
    assert_alembic "$TARGET_HEAD"
    local dup
    dup="$(kubectl_ns exec "$(fullname)-postgres-0" -- \
        psql -U postgres -d postgres -tA -c "SELECT count(*) FROM curie.alembic_version;" | tr -d '[:space:]')"
    [[ "$dup" == "1" ]] || die "alembic_version has $dup rows after retry"
    log "migration-crash retry reached $TARGET_HEAD once; sentinel unchanged"
}

run_converge_negative() {
    if [[ "$(helm_version)" != "0.10.0" ]]; then
        cluster_upgrade "0.10.0" "$CHART_0100" || true
        wait_rollout || true
    fi
    kubectl_ns set image "deploy/$(fullname)-api" "api=ghcr.io/curie-eng/curie-api:does-not-exist-2590"
    local status=0
    set +e
    cluster_upgrade "0.10.0" "$CHART_0100"
    status=$?
    set -e
    record_upgrade_json "converge-negative"
    local images manifest
    images="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "convergence.images" || true)"
    manifest="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "convergence.manifest_matches" || true)"
    local st
    st="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "status" || true)"
    if [[ "$st" == "failed" && ( "$images" == "false" || "$manifest" == "false" ) ]]; then
        log "converge-negative failed with images=$images manifest_matches=$manifest"
    else
        die "converge-negative did not fail the observer (status='$st' images=$images manifest=$manifest exit=$status)"
    fi
    kubectl_ns rollout undo "deploy/$(fullname)-api" >/dev/null 2>&1 || true
    wait_rollout || true
    set +e
    cluster_upgrade "0.10.0" "$CHART_0100"
    status=$?
    set -e
    record_upgrade_json "converge-positive"
    [[ "$status" -eq 0 ]] || die "converge-positive exited $status"
    assert_upgrade_status "succeeded"
    images="$(json_field "$EVIDENCE_DIR/last-upgrade.json" "convergence.images")"
    [[ "$images" == "true" ]] || die "converge-positive images=$images"
    local live
    live="$(kubectl_ns get pods -l "app.kubernetes.io/component=api" -o jsonpath='{range .items[*]}{.spec.containers[*].image}{"\n"}{end}')"
    echo "$live" | grep -E '0.10.0|curie-api' >/dev/null \
        || die "live api images do not match target tags: $live"
    log "converge-positive images true; live pods $live"
}

terminate_tree() {
    local pid="$1" child
    [[ -n "$pid" ]] || return 0
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        terminate_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
}

sigkill_tree() {
    local pid="$1" child command args children
    [[ -n "$pid" ]] || return 0
    command="$(ps -p "$pid" -o comm= 2>/dev/null | tr -d '[:space:]' || true)"
    args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$command" == helm && "$args" =~ (^|[[:space:]])upgrade([[:space:]]|$) ]]; then
        APPLY_KILL_HELM_SEEN=1
    fi
    children="$(pgrep -P "$pid" 2>/dev/null || true)"
    APPLY_KILL_PIDS+=("$pid")
    # Stop the parent before its child exits can trigger normal cleanup.
    kill -KILL "$pid" 2>/dev/null || true
    for child in $children; do
        sigkill_tree "$child"
    done
}

running_release_hook_jobs() {
    kubectl_ns get jobs -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/managed-by=Helm" \
        -o json | python3 -c '
import json, sys
document = json.load(sys.stdin)
items = document["items"]
if not isinstance(items, list):
    raise SystemExit("release hook Job list has no items array")
for item in items:
    metadata = item.get("metadata") or {}
    hook = (metadata.get("annotations") or {}).get("helm.sh/hook")
    active = (item.get("status") or {}).get("active", 0)
    if not isinstance(active, int) or isinstance(active, bool) or active < 0:
        raise SystemExit("release hook Job has an invalid active count")
    if hook and active > 0:
        name = metadata.get("name")
        if not isinstance(name, str) or not name:
            raise SystemExit("running release hook Job has no name")
        print(name)
'
}

recover_killed_upgrade_ownership() {
    local cm="${RELEASE}-upgrade-checkpoint"
    # SIGTERM skips finish_owned_upgrade. Strip the holder after the process
    # tree is gone; deleting the checkpoint is not recovery (cli/README.md).
    if kubectl_ns get configmap "$cm" >/dev/null 2>&1; then
        kubectl_ns annotate configmap "$cm" \
            curietech.ai/upgrade-holder- \
            curietech.ai/upgrade-action- \
            --overwrite >/dev/null
        log "cleared upgrade holder annotations on $cm after confirmed process stop"
    fi
}

run_previous_serves() {
    recover_killed_upgrade_ownership
    if [[ "$(helm_version)" != "0.10.0" || "$(helm_release_status)" != "deployed" ]]; then
        cluster_upgrade "0.10.0" "$CHART_0100" || true
        wait_rollout || true
    fi
    local pid status=0 seen=0 previous_image
    previous_image="$(kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.spec.template.spec.containers[0].image}')"
    [[ -n "$previous_image" ]] || die "could not read previous api image"
    (
        cluster_upgrade "0.10.1" "$CHART_0101"
    ) >"$EVIDENCE_DIR/previous-serves-upgrade.log" 2>&1 &
    pid=$!
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        if schema_migrate_busy; then
            seen=1
            break
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 0.2
    done
    (( seen == 1 )) || die "previous-serves did not observe migrate/apply start before kill"
    terminate_tree "$pid"
    local wait_deadline=$((SECONDS + 30))
    while (( SECONDS < wait_deadline )) && kill -0 "$pid" 2>/dev/null; do
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
    fi
    wait "$pid" || status=$?
    log "killed in-flight cluster upgrade (exit ${status:-0})"
    (( status != 0 )) || die "previous-serves kill left a successful upgrade; the fault was not injected"
    local helm_ver helm_st
    helm_ver="$(helm_version)"
    helm_st="$(helm_release_status)"
    # A failed/pending 0.10.1 revision is the apply-side fault. Only a deployed
    # 0.10.1 means the previous version is gone.
    if [[ "$helm_ver" == "0.10.1" && "$helm_st" == "deployed" ]]; then
        die "helm deployed 0.10.1 (status=$helm_st); previous version is not serving"
    fi
    log "helm after kill version=$helm_ver status=$helm_st"
    local live_image
    live_image="$(kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.spec.template.spec.containers[0].image}')"
    [[ "$live_image" == "$previous_image" ]] \
        || die "api image after kill is '$live_image' not previous '$previous_image'"
    kubectl_ns get deploy "$(fullname)-api" -o jsonpath='{.status.readyReplicas}{"\n"}' | grep -vq '^0$' \
        || die "previous version is not Ready after apply-side kill"
    api_health >/dev/null || die "previous version API health failed after apply-side kill"
    log "previous version still serves after apply-side kill image=$live_image"
    recover_killed_upgrade_ownership
    set +e
    cluster_upgrade "0.10.1" "$CHART_0101"
    status=$?
    set -e
    record_upgrade_json "previous-serves-retry"
    [[ "$status" -eq 0 ]] || die "fail-forward retry after previous-serves kill exited $status"
    assert_upgrade_status "succeeded"
    log "fail-forward retry after previous-serves kill succeeded"
}

run_apply_kill_takeover() {
    [[ "$(helm_version)" == "0.10.0" && "$(helm_release_status)" == deployed ]] \
        || die "apply-kill-takeover must start from the shard's deployed 0.10.0 state"
    local img from_image to_image from_id to_id
    EXCLUSIVE_KIND_TAG=""
    for img in "${IMAGES[@]}"; do
        from_image="$(image_for "$img" "0.10.0")"
        to_image="$(image_for "$img" "0.10.1")"
        docker build --tag "$to_image" - <<EOF
FROM $from_image
LABEL curie.matrix/variant=0.10.1
EOF
        docker tag "$to_image" "${img}:0.10.1"
        from_id="$(docker image inspect --format '{{.Id}}' "$from_image")"
        to_id="$(docker image inspect --format '{{.Id}}' "$to_image")"
        [[ "$from_id" != "$to_id" ]] || die "$img 0.10.0 and 0.10.1 image digests must differ"
        kind load docker-image "$from_image" "$to_image" --name "$KIND_CLUSTER"
        log "$img has distinct 0.10.0 and 0.10.1 image digests on kind"
    done

    local history="$EVIDENCE_DIR/apply-kill-takeover-history.json" serving_revision
    helm_ns history "$RELEASE" -o json --max 256 >"$history"
    serving_revision="$(python3 -c '
import json, sys
rows = json.load(open(sys.argv[1]))
deployed = [row for row in rows if row["status"] == "deployed"]
row = max(deployed, key=lambda row: int(row["revision"]))
if row.get("app_version") != "0.10.0":
    raise SystemExit("serving revision is not 0.10.0")
print(row["revision"])
' "$history")"
    local pid status=0 seen=0 killed_revision newest newest_status
    local CURIE_UPGRADE_TEST_FAIL_AT="" CURIE_UPGRADE_TEST_INTERRUPT_AFTER=""
    "$BIN" --json cluster upgrade --yes --to "0.10.1" \
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$CHART_0101" \
        >"$EVIDENCE_DIR/apply-kill-takeover-killed.json" 2>"$EVIDENCE_DIR/apply-kill-takeover-killed.err" &
    pid=$!
    APPLY_KILL_PID="$pid"
    local deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
        helm_ns history "$RELEASE" -o json --max 256 >"$history"
        newest="$(python3 -c '
import json, sys
row = max(json.load(open(sys.argv[1])), key=lambda row: int(row["revision"]))
print(row["revision"], row["status"])
' "$history")"
        read -r killed_revision newest_status <<<"$newest"
        if [[ "$newest_status" == pending-upgrade ]]; then
            seen=1
            break
        fi
        kill -0 "$pid" 2>/dev/null || die "upgrade exited before a pending-upgrade revision was observed"
        sleep 1
    done
    (( seen == 1 )) || die "apply-kill-takeover did not observe pending-upgrade within 300s"
    sleep 20
    kill -0 "$pid" 2>/dev/null || die "upgrade finished before the apply SIGKILL"
    APPLY_KILL_PIDS=()
    APPLY_KILL_HELM_SEEN=0
    sigkill_tree "$pid"
    wait "$pid" || status=$?
    APPLY_KILL_PID=""
    (( status == 137 )) || die "apply-kill-takeover expected the binary's SIGKILL exit 137, got $status"
    (( APPLY_KILL_HELM_SEEN == 1 )) || die "apply SIGKILL did not include a running helm upgrade descendant"
    local killed_pid process_state stop_deadline=$((SECONDS + 30))
    for killed_pid in "${APPLY_KILL_PIDS[@]}"; do
        while true; do
            process_state="$(ps -p "$killed_pid" -o stat= 2>/dev/null | tr -d '[:space:]' || true)"
            [[ -n "$process_state" && "$process_state" != Z* ]] || break
            (( SECONDS < stop_deadline )) || die "SIGKILL descendant $killed_pid is still running"
            sleep 0.2
        done
    done

    local cm="${RELEASE}-upgrade-checkpoint" holder resource_version
    holder="$(kubectl_ns get configmap "$cm" -o jsonpath='{.metadata.annotations.curietech\.ai/upgrade-holder}')"
    [[ -n "$holder" ]] || die "SIGKILL did not leave the upgrade holder annotation"
    resource_version="$(kubectl_ns get configmap "$cm" -o jsonpath='{.metadata.resourceVersion}')"
    helm_ns history "$RELEASE" -o json --max 256 >"$history"
    python3 -c '
import json, sys
rows = json.load(open(sys.argv[1]))
newest = max(rows, key=lambda row: int(row["revision"]))
assert int(newest["revision"]) == int(sys.argv[2]) and newest["status"] == "pending-upgrade", "SIGKILL did not retain the pending-upgrade revision"
assert any(int(row["revision"]) == int(sys.argv[3]) and row["status"] == "deployed" for row in rows), "SIGKILL changed the serving revision"
' "$history" "$killed_revision" "$serving_revision"
    log "SIGKILL stopped the binary and helm child; revision $killed_revision is pending above serving revision $serving_revision"

    status=0
    "$BIN" --json cluster upgrade --yes --to "0.10.1" \
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$CHART_0101" \
        >"$EVIDENCE_DIR/apply-kill-takeover-refused.json" 2>"$EVIDENCE_DIR/apply-kill-takeover-refused.err" || status=$?
    (( status != 0 )) || die "plain rerun accepted the killed holder"
    grep -Fq -- "$holder" "$EVIDENCE_DIR/apply-kill-takeover-refused.json" "$EVIDENCE_DIR/apply-kill-takeover-refused.err" \
        || die "plain rerun refusal did not name the killed holder"
    grep -Fq -- "--take-over $holder" "$EVIDENCE_DIR/apply-kill-takeover-refused.json" "$EVIDENCE_DIR/apply-kill-takeover-refused.err" \
        || die "plain rerun refusal did not name the exact takeover command"
    status=0
    "$BIN" --json cluster upgrade --yes --to "0.10.1" --dry-run \
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$CHART_0101" \
        >"$EVIDENCE_DIR/apply-kill-takeover-dry-refused.json" 2>"$EVIDENCE_DIR/apply-kill-takeover-dry-refused.err" || status=$?
    (( status != 0 )) || die "dry run accepted the killed holder without --take-over"
    grep -Fq -- "$holder" "$EVIDENCE_DIR/apply-kill-takeover-dry-refused.json" "$EVIDENCE_DIR/apply-kill-takeover-dry-refused.err" \
        || die "dry-run refusal did not name the killed holder"
    # Cluster hook Jobs can outlive the killed Helm process. Both takeover
    # paths refuse active hooks, so wait before planning or mutating.
    local running_hooks deadline=$((SECONDS + 600))
    while true; do
        running_hooks="$(running_release_hook_jobs)"
        [[ -n "$running_hooks" ]] || break
        (( SECONDS < deadline )) || die "release hook Jobs still running after 600s: $running_hooks"
        sleep 1
    done
    "$BIN" --json cluster upgrade --yes --to "0.10.1" --dry-run --take-over "$holder" \
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$CHART_0101" \
        >"$EVIDENCE_DIR/apply-kill-takeover-dry-plan.json" 2>"$EVIDENCE_DIR/apply-kill-takeover-dry-plan.err"
    python3 -c '
import json, shlex, sys
document = json.load(open(sys.argv[1]))
assert document["dry_run"] is True, "matching takeover did not emit a dry-run plan"
plan = document["plan"]
assert "phase plan: 0.10.0 -> 0.10.1" in plan, "dry run did not plan from serving version 0.10.0"
assert f"take over upgrade ownership from {sys.argv[2]}" in plan, "dry run did not plan the named takeover"
commands = [shlex.split(line) for line in plan if line.startswith("helm ")]
rollback = next(index for index, argv in enumerate(commands) if argv[:4] == ["helm", "rollback", sys.argv[3], sys.argv[4]])
upgrade = next(index for index, argv in enumerate(commands) if argv[:2] == ["helm", "upgrade"])
assert rollback < upgrade, "dry run did not plan rollback before upgrade"
assert commands[rollback][4:] == ["-n", sys.argv[5], "--wait", "--timeout", "15m"], "dry run rollback did not use the prescribed namespace, wait and timeout"
' "$EVIDENCE_DIR/apply-kill-takeover-dry-plan.json" "$holder" "$RELEASE" "$serving_revision" "$NAMESPACE"
    [[ "$(kubectl_ns get configmap "$cm" -o jsonpath='{.metadata.resourceVersion}')" == "$resource_version" ]] \
        || die "refused runs or dry-run plans mutated upgrade ownership"
    [[ "$(helm_revision)" == "$killed_revision" && "$(helm_release_status)" == pending-upgrade ]] \
        || die "refused runs or dry-run plans mutated the pending Helm revision"

    status=0
    "$BIN" --json cluster upgrade --yes --to "0.10.1" --take-over "$holder" \
        --namespace "$NAMESPACE" --release "$RELEASE" --chart "$CHART_0101" \
        >"$EVIDENCE_DIR/apply-kill-takeover.json" 2>"$EVIDENCE_DIR/apply-kill-takeover.err" || status=$?
    (( status == 0 )) || die "real takeover exited $status"
    [[ "$(json_field "$EVIDENCE_DIR/apply-kill-takeover.json" status)" == succeeded ]] \
        || die "real takeover did not report succeeded"
    helm_ns history "$RELEASE" -o json --max 256 >"$history"
    python3 -c '
import json, sys
rows = json.load(open(sys.argv[1]))
newest = max(rows, key=lambda row: int(row["revision"]))
assert newest["status"] == "deployed" and newest["app_version"] == "0.10.1", "takeover did not deploy 0.10.1"
assert any(int(sys.argv[2]) < int(row["revision"]) < int(newest["revision"]) and row.get("description") == f"Rollback to {sys.argv[3]}" for row in rows), "takeover did not create a rollback revision between the killed and deployed revisions"
' "$history" "$killed_revision" "$serving_revision"
    [[ -z "$(kubectl_ns get configmap "$cm" -o jsonpath='{.metadata.annotations.curietech\.ai/upgrade-holder}')" ]] \
        || die "successful takeover left an upgrade holder annotation"
    [[ "$(helm_version)" == "0.10.1" ]] || die "successful takeover metadata is not 0.10.1"
    wait_rollout
    kubectl_ns get deploy "$(fullname)-api" -o json | python3 -c '
import json, sys
deployment = json.load(sys.stdin)
assert (deployment.get("status") or {}).get("readyReplicas", 0) > 0, "API is not Ready after takeover"
'
    api_health >/dev/null || die "API health failed after takeover"
    log "apply-kill-takeover deployed 0.10.1 through the CLI rollback with a Ready API and no holder"
}

run_rollback_to_serving() {
    local V CHART_V serving pending="" status=0
    V="$(awk '$1 == "version:" { print $2; exit }' "$REPO_ROOT/charts/curie/Chart.yaml")"
    [[ -n "$V" ]] || die "rollback-to-serving could not read the checkout chart version"
    # The checkout version admits the live tree head; 0.10.x does not.
    retag_candidate_versions "0.10.0" "$V" required
    helm package "$REPO_ROOT/charts/curie" --version "$V" --app-version "$V" -d "$WORKDIR/charts" >/dev/null
    CHART_V="$WORKDIR/charts/curie-${V}.tgz"
    [[ -f "$CHART_V" ]] || die "rollback-to-serving chart package is missing"
    exclusive_kind_tag "$V"
    cluster_upgrade "$V" "$CHART_V" || status=$?
    record_upgrade_json "rollback-serving-setup"
    (( status == 0 )) || die "rollback-to-serving setup upgrade exited $status"
    assert_upgrade_status "succeeded"
    wait_rollout
    helm_ns history "$RELEASE" -o json >"$EVIDENCE_DIR/rollback-serving-before.json"
    serving="$(python3 -c '
import json, sys
row = max(json.load(sys.stdin), key=lambda r: int(r["revision"]))
if row["status"] != "deployed" or row["app_version"] != sys.argv[1] or row["chart"] != "curie-" + sys.argv[1]:
    raise SystemExit("newest revision is not deployed at the checkout version")
print(row["revision"])
' "$V" <"$EVIDENCE_DIR/rollback-serving-before.json")" \
        || die "rollback-to-serving setup did not establish a serving revision at $V"

    # Start Helm directly so the recorded PID is the process that holds its lock.
    helm --kubeconfig "$KUBECONFIG_FILE" "${HELM_CONTEXT_ARGS[@]}" upgrade "$RELEASE" "$CHART_V" -n "$NAMESPACE" \
        --reuse-values --wait --timeout 10m \
        >"$EVIDENCE_DIR/rollback-serving-interrupted.log" 2>&1 &
    ROLLBACK_SERVING_HELM_PID=$!
    local deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
        helm_ns history "$RELEASE" -o json >"$EVIDENCE_DIR/rollback-serving-pending.json"
        pending="$(python3 -c '
import json, sys
row = max(json.load(sys.stdin), key=lambda r: int(r["revision"]))
if row["status"] == "pending-upgrade" and int(row["revision"]) > int(sys.argv[1]):
    print(row["revision"])
' "$serving" <"$EVIDENCE_DIR/rollback-serving-pending.json")"
        [[ -z "$pending" ]] || break
        kill -0 "$ROLLBACK_SERVING_HELM_PID" 2>/dev/null || break
        sleep 0.2
    done
    [[ -n "$pending" ]] || die "rollback-to-serving did not observe pending-upgrade within 300s"
    kill -9 "$ROLLBACK_SERVING_HELM_PID" \
        || die "rollback-to-serving Helm exited before SIGKILL"
    status=0
    wait "$ROLLBACK_SERVING_HELM_PID" 2>/dev/null || status=$?
    ROLLBACK_SERVING_HELM_PID=""
    (( status != 0 )) || die "rollback-to-serving interrupted Helm reported success"
    helm_ns history "$RELEASE" -o json >"$EVIDENCE_DIR/rollback-serving-killed.json"
    python3 -c '
import json, sys
rows = json.load(sys.stdin)
newest = max(rows, key=lambda r: int(r["revision"]))
serving, pending, version = sys.argv[1:]
if int(newest["revision"]) != int(pending) or newest["status"] != "pending-upgrade":
    raise SystemExit("killed upgrade did not leave its newest revision pending-upgrade")
if not any(int(r["revision"]) == int(serving) and r["status"] == "deployed" and r["app_version"] == version for r in rows):
    raise SystemExit("killed upgrade did not retain the serving revision")
' "$serving" "$pending" "$V" <"$EVIDENCE_DIR/rollback-serving-killed.json" \
        || die "rollback-to-serving did not preserve the required interrupted history"

    status=0
    "$BIN" --json cluster rollback --dry-run --namespace "$NAMESPACE" --release "$RELEASE" \
        >"$EVIDENCE_DIR/rollback-serving-dry-run.json" 2>"$EVIDENCE_DIR/rollback-serving-dry-run.err" \
        || status=$?
    (( status == 0 )) || die "rollback-to-serving dry run exited $status"
    python3 -c '
import json, shlex, sys
doc = json.load(sys.stdin)
release, serving, pending, version = sys.argv[1:]
plan = doc["plan"]
summary = f"selected revision {serving} (deployed, curie-{version}) from revision {pending}; skipped none"
commands = [shlex.split(line) for line in plan]
if doc.get("dry_run") is not True or summary not in plan:
    raise SystemExit("dry run does not identify the serving and pending revisions")
if not any(args[:4] == ["helm", "rollback", release, serving] for args in commands):
    raise SystemExit("dry run does not name the serving revision in its Helm rollback command")
' "$RELEASE" "$serving" "$pending" "$V" <"$EVIDENCE_DIR/rollback-serving-dry-run.json" \
        || die "rollback-to-serving dry run selected the wrong target"

    status=0
    "$BIN" --json cluster rollback --yes --namespace "$NAMESPACE" --release "$RELEASE" \
        >"$EVIDENCE_DIR/rollback-serving.json" 2>"$EVIDENCE_DIR/rollback-serving.err" \
        || status=$?
    (( status == 0 )) || die "rollback-to-serving rollback exited $status"
    [[ "$(json_field "$EVIDENCE_DIR/rollback-serving.json" "to_revision")" == "$serving" ]] \
        || die "rollback-to-serving rollback did not target serving revision $serving"
    [[ "$(json_field "$EVIDENCE_DIR/rollback-serving.json" "from_revision")" == "$pending" ]] \
        || die "rollback-to-serving rollback did not start from pending revision $pending"
    helm_ns history "$RELEASE" -o json >"$EVIDENCE_DIR/rollback-serving-after.json"
    python3 -c '
import json, sys
row = max(json.load(sys.stdin), key=lambda r: int(r["revision"]))
serving, pending, version = sys.argv[1:]
if int(row["revision"]) <= int(pending) or row["status"] != "deployed" or row["app_version"] != version or row["chart"] != "curie-" + version:
    raise SystemExit("newest rollback revision is not deployed at the serving version")
if row["description"] != "Rollback to " + serving:
    raise SystemExit("newest revision does not record rollback to the serving revision")
' "$serving" "$pending" "$V" <"$EVIDENCE_DIR/rollback-serving-after.json" \
        || die "rollback-to-serving did not restore the serving version in Helm history"
    wait_rollout
    kubectl_ns get deploy "$(fullname)-api" -o json | python3 -c '
import json, sys
if int(json.load(sys.stdin).get("status", {}).get("readyReplicas", 0)) < 1:
    raise SystemExit("API has no ready replicas")
' || die "rollback-to-serving API is not Ready"
    api_health >/dev/null || die "rollback-to-serving API health failed"
    log "rollback-to-serving restored revision $serving at $V over pending revision $pending; API is Ready and healthy"
}

# These observations come from the real claim and its bound Pod. A replacement
# claim, a deleting claim, or a Pod on another image cannot pass the proof.
# The Pod's labels are not checked: a platform-template Pod carries no agent
# label, and the claim is matched to its Pod by status.sandbox.name only.
# The last argument is "ready" (claim and Pod must be Ready) or "bound" (the
# claim binds a live Pod on the image; readiness is not required).
runner_layer_claim_observation() {
    python3 - "$@" <<'PY'
import json, sys
claim_file, pod_file, agent, image, expected_uid, mode = sys.argv[1:]
if mode not in ("ready", "bound"):
    raise SystemExit(f"unknown observation mode {mode!r}")
claim = json.load(open(claim_file))
pod = json.load(open(pod_file))
def require(condition, message):
    if not condition:
        raise SystemExit(message)
def ready(document):
    return any(row.get("type") == "Ready" and row.get("status") == "True"
               for row in document.get("status", {}).get("conditions", []))
metadata = claim["metadata"]
uid = metadata.get("uid")
require(bool(uid) and (not expected_uid or uid == expected_uid), "claim UID changed or is absent")
require(not metadata.get("deletionTimestamp"), "claim is being deleted")
require(metadata.get("labels", {}).get("curietech.ai/agent") == agent, "claim agent label differs")
require("curietech.ai/managed-by" not in metadata.get("labels", {}), "claim is visible to the orphan reaper")
require(claim.get("status", {}).get("sandbox", {}).get("name") == pod["metadata"]["name"],
        "claim does not bind the observed Pod")
require(not pod["metadata"].get("deletionTimestamp"), "Pod is being deleted")
runners = [container for container in pod.get("spec", {}).get("containers", [])
           if container.get("name") == "runner"]
require(len(runners) == 1 and runners[0].get("image") == image, "bound runner image differs")
if mode == "ready":
    require(ready(claim) and ready(pod), "claim or bound Pod is not Ready")
print(uid)
PY
}

assert_runner_layer_claim() {
    local claim="$1" agent="$2" image="$3" expected_uid="${4:-}" pod
    kubectl_ns wait --for=condition=Ready "sandboxclaim/$claim" --timeout=180s >&2 || return $?
    kubectl_ns get sandboxclaim "$claim" -o json >"$WORKDIR/retire-claim.json" || return $?
    pod="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get("status", {}).get("sandbox", {}).get("name", ""))' \
        "$WORKDIR/retire-claim.json")" || return $?
    [[ -n "$pod" ]] || die "runner-layer-retire claim $claim has no bound sandbox"
    kubectl_ns wait --for=condition=Ready "pod/$pod" --timeout=180s >&2 || return $?
    kubectl_ns get pod "$pod" -o json >"$WORKDIR/retire-pod.json" || return $?
    runner_layer_claim_observation "$WORKDIR/retire-claim.json" "$WORKDIR/retire-pod.json" \
        "$agent" "$image" "$expected_uid" ready
}

# The old claim sits on the chart's per-agent pool, whose template has no
# per-claim plugin bundle, so its runner cannot become Ready. The scenario only
# needs it bound to a live Pod on the layer image so the upgrade must delete it.
assert_runner_layer_bound_claim() {
    local claim="$1" agent="$2" image="$3" pod="" deadline
    deadline=$((SECONDS + 180))
    while :; do
        pod="$(kubectl_ns get sandboxclaim "$claim" -o jsonpath='{.status.sandbox.name}' 2>/dev/null || true)"
        [[ -z "$pod" ]] && (( SECONDS < deadline )) || break
        sleep 2
    done
    [[ -n "$pod" ]] || die "runner-layer-retire claim $claim bound no sandbox within 180s"
    deadline=$((SECONDS + 180))
    until kubectl_ns get pod "$pod" -o json >"$WORKDIR/retire-pod.json" 2>/dev/null; do
        (( SECONDS < deadline )) || die "runner-layer-retire claim $claim Pod $pod did not appear within 180s"
        sleep 2
    done
    kubectl_ns get sandboxclaim "$claim" -o json >"$WORKDIR/retire-claim.json" || return $?
    runner_layer_claim_observation "$WORKDIR/retire-claim.json" "$WORKDIR/retire-pod.json" \
        "$agent" "$image" "" bound
}

# Fails unless the Pod labels file (kubectl jsonpath '{.metadata.labels}'
# output, possibly empty) lacks curietech.ai/agent, as a platform-template
# Pod does (#4332).
runner_layer_pod_lacks_agent_label() {
    python3 - "$1" <<'PY'
import json, sys
text = open(sys.argv[1]).read().strip()
labels = json.loads(text) if text else {}
if "curietech.ai/agent" in labels:
    raise SystemExit("target Pod carries a curietech.ai/agent label")
PY
}

kind_runner_digest() {
    local reference="$1" node digest
    node="$(kind_node)"
    [[ -n "$node" ]] || die "runner-layer-retire has no kind node"
    digest="$(docker exec "$node" ctr -n k8s.io images list \
        | awk -v ref="$reference" '$1 == ref {print $3}')"
    if [[ "$digest" != sha256:* ]] || ! is_sha256 "${digest#sha256:}"; then
        die "kind has no manifest digest for runner $reference"
    fi
    printf '%s' "$digest"
}

restore_retire_runner_tags() {
    if [[ -n "$RETIRE_RUNNER_ORIGINAL_ID" ]]; then
        docker tag "$RETIRE_RUNNER_ORIGINAL_ID" "$(image_for curie-runner 0.10.1)" || return $?
        docker tag "$RETIRE_RUNNER_ORIGINAL_ID" curie-runner:0.10.1 || return $?
        RETIRE_RUNNER_ORIGINAL_ID=""
    fi
    if [[ -n "$RETIRE_RUNNER_DERIVED_TAG" ]]; then
        docker image rm "$RETIRE_RUNNER_DERIVED_TAG" >/dev/null || return $?
        RETIRE_RUNNER_DERIVED_TAG=""
    fi
    if [[ -n "$RETIRE_LAYER_TAG" ]]; then
        docker image rm "$RETIRE_LAYER_TAG" >/dev/null || return $?
        RETIRE_LAYER_TAG=""
    fi
}

run_runner_layer_retire() {
    restore_n
    local agent=acme-retire old_claim=acme-retire-old target_claim=acme-retire-target
    local current_runner target_runner current_id target_id target_digest layer_tag layer_digest layer_image
    local node target_image target_pod target_uid target_uid_after old_uid old_remaining status=0 saved_chart="$CHART_0101"
    current_runner="$(image_for curie-runner 0.10.0)"
    target_runner="$(image_for curie-runner 0.10.1)"
    current_id="$(docker image inspect "$current_runner" --format '{{.Id}}')"
    target_id="$(docker image inspect "$target_runner" --format '{{.Id}}')"
    if [[ "$current_id" == "$target_id" ]]; then
        # The candidate's two version tags normally alias one image. A LABEL
        # changes its digest without changing any runner behavior.
        RETIRE_RUNNER_ORIGINAL_ID="$target_id"
        RETIRE_RUNNER_DERIVED_TAG="${target_runner%:*}:matrix-${KIND_CLUSTER}-retire"
        printf 'FROM %s\nLABEL curietech.ai/matrix="runner-layer-retire"\n' "$current_runner" \
            | docker build --pull=false -t "$RETIRE_RUNNER_DERIVED_TAG" - >&2
        docker tag "$RETIRE_RUNNER_DERIVED_TAG" "$target_runner"
        docker tag "$target_runner" curie-runner:0.10.1
    fi
    EXCLUSIVE_KIND_TAG=""
    kind load docker-image "$target_runner" --name "$KIND_CLUSTER" >&2
    target_digest="$(kind_runner_digest "$target_runner")"
    [[ "$target_digest" != "$(kind_runner_digest "$current_runner")" ]] \
        || die "runner-layer-retire needs distinct 0.10.0 and 0.10.1 runner digests"
    node="$(kind_node)"
    # The layer is owner-built by repository identity, retagged from this
    # candidate, and addressable by the digest the chart requires for a bind.
    # It is the 0.10.0 runner image, so the upgrade's exclusive_kind_tag 0.10.1
    # may crictl rmi it from the node. Nothing may schedule on the layer after
    # the upgrade starts; only the pre-upgrade old claim uses it.
    layer_tag="ghcr.io/acme-corp/acme-runner:matrix-${KIND_CLUSTER}"
    docker tag "$current_runner" "$layer_tag"
    RETIRE_LAYER_TAG="$layer_tag"
    kind load docker-image "$layer_tag" --name "$KIND_CLUSTER" >&2
    layer_digest="$(kind_runner_digest "$layer_tag")"
    layer_image="${layer_tag%:*}@${layer_digest}"
    docker exec "$node" ctr -n k8s.io images tag --force "$layer_tag" "$layer_image" >/dev/null
    docker exec "$node" ctr -n k8s.io images tag --force "$target_runner" \
        "${target_runner%:*}@${target_digest}" >/dev/null
    local chart_dir="$WORKDIR/runner-layer-retire/chart"
    mkdir -p "$chart_dir" "$WORKDIR/runner-layer-retire/packaged"
    tar -xzf "$CHART_0101" -C "$chart_dir"
    # Planning pins tagged refs through the registry, not the node's local
    # tags. Pin only this private packaged chart so the planner observes the
    # kind-loaded target digest even when the published version tags alias.
    python3 - "$chart_dir/curie/values.yaml" "$target_digest" <<'PY'
import json, pathlib, sys
path, digest = pathlib.Path(sys.argv[1]), sys.argv[2]
lines = path.read_text().splitlines(keepends=True)
in_sandbox = in_runner = False
changed = 0
for index, line in enumerate(lines):
    if line.strip() and not line.startswith((" ", "#")):
        in_sandbox = line.startswith("agentSandbox:")
        in_runner = False
    if in_sandbox and line.startswith("  ") and not line.startswith("   ") and not line.lstrip().startswith("#"):
        in_runner = line.startswith("  runner:")
    if in_sandbox and in_runner and line.startswith("    digest:"):
        lines[index] = "    digest: " + json.dumps(digest) + "\n"
        changed += 1
if changed != 1:
    raise SystemExit("private target chart must contain exactly one runner digest field")
path.write_text("".join(lines))
PY
    helm package "$chart_dir/curie" -d "$WORKDIR/runner-layer-retire/packaged" >/dev/null
    CHART_0101="$WORKDIR/runner-layer-retire/packaged/curie-0.10.1.tgz"
    helm_ns upgrade "$RELEASE" "$CHART_0100" --reset-then-reuse-values \
        --set-string "agentSandbox.runnerImages.${agent}=${layer_image}" --wait --timeout 15m >&2
    wait_rollout >&2
    helm_ns get values "$RELEASE" -o json >"$WORKDIR/retire-target-values.json"
    python3 - "$WORKDIR/retire-target-values.json" "$agent" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
values = json.loads(path.read_text())
sandbox = values.get("agentSandbox", {})
if "digest" in sandbox.get("runner", {}):
    raise SystemExit("retirement setup must not retain a platform runner digest override")
sandbox.get("runnerImages", {}).pop(sys.argv[2], None)
path.write_text(json.dumps(values))
PY
    helm_ns template "$RELEASE" "$CHART_0101" -f "$WORKDIR/retire-target-values.json" \
        --show-only templates/agent-sandbox.yaml >"$WORKDIR/retire-target-render.yaml"
    python3 - "$WORKDIR/retire-target-render.yaml" "$(fullname)-runner" <<'PY' \
        | kubectl_ns create --dry-run=client -f - -o json >"$WORKDIR/retire-platform-template.json"
import pathlib, re, sys
documents = re.split(r"(?m)^---\s*$", pathlib.Path(sys.argv[1]).read_text())
templates = [document for document in documents
             if re.search(r"(?m)^kind: SandboxTemplate$", document)
             and re.search(r"(?m)^  name: " + re.escape(sys.argv[2]) + r"$", document)]
if len(templates) != 1:
    raise SystemExit("target chart must render exactly one platform SandboxTemplate")
print(templates[0])
PY
    target_image="$(python3 -c 'import json,sys; print(next(c["image"] for c in json.load(open(sys.argv[1]))["spec"]["podTemplate"]["spec"]["containers"] if c["name"] == "runner"))' \
        "$WORKDIR/retire-platform-template.json")"
    [[ "$target_image" == "${target_runner%:*}@${target_digest}" ]] \
        || die "target chart does not render the kind-loaded runner digest"
    docker run --rm --entrypoint test "$target_runner" \
        -f /app/packages/plugin-format/tests/fixtures/valid_bundle/.claude-plugin/plugin.json \
        || die "runner-layer-retire target runner $target_runner lacks the fixture plugin bundle at /app/packages/plugin-format/tests/fixtures/valid_bundle"
    python3 - "$WORKDIR/retire-platform-template.json" "$agent" "$old_claim" "$target_claim" \
        "$(fullname)-agent-${agent}-runner-pool" >"$WORKDIR/retire-resources.json" <<'PY'
import json, sys
template_file, agent, old_claim, target_claim, old_pool = sys.argv[1:]
template = json.load(open(template_file))
api = template["apiVersion"]
template["metadata"] = {"name": "acme-retire-admin-template"}
# The worker injects a real plugin bundle per claim and the template's
# CURIE_PLUGIN_DIR is a placeholder. This admin claim gets no bundle, so point
# the runner at the fixture bundle the runner image ships; its Pod then boots
# Ready on the unchanged target image.
runners = [container for container in template["spec"]["podTemplate"]["spec"]["containers"]
           if container.get("name") == "runner"]
if len(runners) != 1:
    raise SystemExit("admin template must have exactly one runner container")
plugin_env = [row for row in runners[0].get("env", []) if row.get("name") == "CURIE_PLUGIN_DIR"]
if len(plugin_env) != 1:
    raise SystemExit("admin template runner has no CURIE_PLUGIN_DIR env entry")
plugin_env[0].pop("valueFrom", None)
plugin_env[0]["value"] = "/app/packages/plugin-format/tests/fixtures/valid_bundle"
# No curietech.ai/agent label on the pod template: a platform-template Pod
# carries none, so retirement must match this claim's Pod by name alone.
template["spec"]["podTemplate"]["metadata"] = {}
pool = {"apiVersion": api, "kind": "SandboxWarmPool",
        "metadata": {"name": "acme-retire-admin-pool"},
        "spec": {"replicas": 0, "updateStrategy": {"type": "OnReplenish"},
                 "sandboxTemplateRef": {"name": template["metadata"]["name"]}}}
def claim(name, warm_pool):
    # No managed-by label: neither real claim may be deleted by the worker's
    # orphan reaper before the retirement operation being tested.
    return {"apiVersion": api, "kind": "SandboxClaim",
            "metadata": {"name": name, "labels": {"curietech.ai/agent": agent}},
            "spec": {"warmPoolRef": {"name": warm_pool}}}
print(json.dumps({"apiVersion": "v1", "kind": "List", "items": [
    template, pool, claim(old_claim, old_pool), claim(target_claim, pool["metadata"]["name"])]}))
PY
    kubectl_ns create -f "$WORKDIR/retire-resources.json" >&2
    old_uid="$(assert_runner_layer_bound_claim "$old_claim" "$agent" "$layer_image")"
    target_uid="$(assert_runner_layer_claim "$target_claim" "$agent" "$target_image")"
    target_pod="$(kubectl_ns get sandboxclaim "$target_claim" \
        -o jsonpath='{.status.sandbox.name}')"
    [[ -n "$target_pod" ]] || die "runner-layer-retire target claim has no bound sandbox"
    kubectl_ns get pod "$target_pod" -o jsonpath='{.metadata.labels}' \
        >"$EVIDENCE_DIR/runner-layer-retire-target-pod-labels.json"
    runner_layer_pod_lacks_agent_label "$EVIDENCE_DIR/runner-layer-retire-target-pod-labels.json" \
        || die "runner-layer-retire target Pod $target_pod carries a curietech.ai/agent label"
    log "runner-layer-retire has the old claim bound on the layer image, a Ready target claim with the same agent label, and a target Pod with no agent label"
    cluster_upgrade "0.10.1" "$CHART_0101" || status=$?
    record_upgrade_json runner-layer-retire
    [[ "$status" -eq 0 ]] || die "runner-layer-retire cluster upgrade exited $status"
    assert_upgrade_status succeeded
    [[ "$(helm_version)" == 0.10.1 ]] || die "runner-layer-retire did not reach 0.10.1"
    helm_ns get values "$RELEASE" -o json | python3 -c '
import json, sys
images = json.load(sys.stdin).get("agentSandbox", {}).get("runnerImages", {})
if sys.argv[1] in (images or {}):
    raise SystemExit("upgrade did not clear the owner-built layer")
' "$agent"
    old_remaining="$(kubectl_ns get sandboxclaim "$old_claim" --ignore-not-found -o name)"
    [[ -z "$old_remaining" ]] \
        || die "runner-layer-retire left the old-layer claim in place"
    target_uid_after="$(assert_runner_layer_claim "$target_claim" "$agent" "$target_image" "$target_uid")"
    [[ "$target_uid_after" == "$target_uid" ]] \
        || die "runner-layer-retire did not preserve the target-layer claim"
    python3 -c 'import json,sys; print(json.dumps({"old_claim_uid":sys.argv[1], "old_claim_deleted":True, "target_claim_uid":sys.argv[2], "target_claim_uid_unchanged":True, "target_pod_ready":True, "target_image":sys.argv[3]}))' \
        "$old_uid" "$target_uid" "$target_image" >"$EVIDENCE_DIR/runner-layer-retire-claims.json"
    CHART_0101="$saved_chart"
    restore_retire_runner_tags
    log "runner-layer-retire deleted the old claim and preserved the target claim UID and Ready Pod"
}

write_evidence() {
    local elapsed=$((SECONDS - STARTED_AT)) shard_json scenarios_json
    shard_json="$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1] or None))' "$SHARD")"
    scenarios_json="$(python3 -c '
import json, sys
try:
    rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
except FileNotFoundError:
    rows = []
print(json.dumps(rows))
' "$EVIDENCE_DIR/scenarios.jsonl")"
    cat >"$EVIDENCE_DIR/summary.json" <<EOF
{
  "issue": 2590,
  "commit": "$CANDIDATE",
  "published": "0.8.8",
  "strict_published_rollback": "0.8.9",
  "published_head": "$PUBLISHED_HEAD",
  "target_head": "$TARGET_HEAD",
  "n": "0.10.0",
  "n1": "0.10.1",
  "candidate_cli": "$CANDIDATE",
  "candidate_image_tag": "${CANDIDATE_TAG:-}",
  "kind_cluster": "$KIND_CLUSTER",
  "namespace": "$NAMESPACE",
  "release": "$RELEASE",
  "shard": $shard_json,
  "scenarios": $scenarios_json,
  "elapsed_seconds": $elapsed
}
EOF
    if (( JSON )); then
        cat "$EVIDENCE_DIR/summary.json"
    fi
}

run_soak_refusal() {
    log "soak-refusal covered by parse-time refuse_soak"
}

scenario_fn() {
    case "$1" in
        soak-refusal) echo run_soak_refusal ;;
        fresh-n) echo run_fresh_n ;;
        n1-to-n-nonempty) echo run_n1_to_n ;;
        same-version) echo run_same_version ;;
        fail-every-phase) echo run_fail_every_phase ;;
        interrupt-resume) echo run_interrupt_resume ;;
        n-to-n1) echo run_n_to_n1 ;;
        guarded-rollback) echo run_guarded_rollback ;;
        rollback-published-088) echo run_rollback_published_088 ;;
        rollback-published-089) echo run_rollback_published_089 ;;
        migration-crash) echo run_migration_crash ;;
        converge-negative) echo run_converge_negative ;;
        previous-serves) echo run_previous_serves ;;
        apply-kill-takeover) echo run_apply_kill_takeover ;;
        rollback-to-serving) echo run_rollback_to_serving ;;
        runner-layer-retire) echo run_runner_layer_retire ;;
        *) die "no runner for scenario '$1'" ;;
    esac
}

# Harness setup for `setup` shards: the nonempty 0.10.0 state the serial chain
# reaches after n1-to-n-nonempty. Not a scenario; timed as setup=nonempty-n.
setup_nonempty_n() {
    uninstall_owned
    helm_install_088
    insert_sentinel
    assert_sentinel
    assert_alembic "$PUBLISHED_HEAD"
    local status=0 ver
    cluster_upgrade "0.10.0" "$CHART_0100" --forward-only || status=$?
    record_upgrade_json "setup-nonempty-n"
    [[ "$status" -eq 0 ]] || die "setup nonempty-n cluster upgrade exited $status"
    assert_upgrade_status "succeeded"
    wait_rollout
    ver="$(helm_version)"
    [[ "$ver" == "0.10.0" ]] || die "setup nonempty-n helm version is '$ver' not 0.10.0"
    assert_sentinel
    assert_review_feedback_table
    assert_alembic "$TARGET_HEAD"
    log "setup nonempty-n reached 0.10.0 at $TARGET_HEAD with the sentinel"
}

run_shard() {
    local item name phases
    log "shard $SHARD setup=$SHARD_SETUP items=${SHARD_ITEMS[*]}"
    if (( SHARD_SETUP )); then
        run_timed "setup=nonempty-n" "setup=nonempty-n" all setup_nonempty_n
    fi
    for item in "${SHARD_ITEMS[@]}"; do
        name="${item%%:*}"
        phases="all"
        [[ "$item" == *:* ]] && phases="${item#*:}"
        case "$name" in
            fail-every-phase) CURIE_E2E_FAIL_PHASES="${phases//+/ }" ;;
            interrupt-resume) CURIE_E2E_INTERRUPT_PHASES="${phases//+/ }" ;;
        esac
        run_scenario_timed "$name" "$phases" "$(scenario_fn "$name")"
        CURIE_E2E_FAIL_PHASES=""
        CURIE_E2E_INTERRUPT_PHASES=""
    done
}

run_serial() {
    local name phases
    for name in "${SCENARIOS_ALL[@]}"; do
        [[ "$name" == soak-refusal ]] && continue
        scenario_wanted "$name" || continue
        if [[ "$name" == apply-kill-takeover ]]; then
            prepare_takeover_context
            run_timed "setup=nonempty-n" "setup=nonempty-n" all setup_nonempty_n
        fi
        phases="all"
        case "$name" in
            fail-every-phase) [[ -z "${CURIE_E2E_FAIL_PHASES:-}" ]] || phases="${CURIE_E2E_FAIL_PHASES// /+}" ;;
            interrupt-resume) [[ -z "${CURIE_E2E_INTERRUPT_PHASES:-}" ]] || phases="${CURIE_E2E_INTERRUPT_PHASES// /+}" ;;
        esac
        run_scenario_timed "$name" "$phases" "$(scenario_fn "$name")"
    done
}

run_matrix() {
    refuse_soak "$NAMESPACE" "$RELEASE"
    resolve_bin
    candidate_identity
    STARTED_AT=$SECONDS
    WORKDIR="$(mktemp -d /tmp/curie-cluster-upgrade-matrix.XXXXXX)"
    mkdir -p "$EVIDENCE_DIR"
    : >"$EVIDENCE_DIR/scenarios.jsonl"
    trap cleanup EXIT
    if [[ -z "$SHARD" && "$SCENARIO" == "soak-refusal" ]]; then
        run_scenario_timed soak-refusal all run_soak_refusal
        write_evidence
        return 0
    fi
    if [[ -z "$SHARD" && "$SCENARIO" == "all" ]]; then
        run_scenario_timed soak-refusal all run_soak_refusal
    fi
    local prelude_started=$SECONDS
    fetch_published
    package_n_charts
    ensure_kind
    if [[ "$SHARD" == s16 ]]; then
        prepare_takeover_context
    fi
    prepare_candidate_images
    log "prelude elapsed_seconds=$((SECONDS - prelude_started))"
    if [[ -n "$SHARD" ]]; then
        run_shard
    else
        run_serial
    fi
    write_evidence
    log "cluster-upgrade-matrix finished on candidate $CANDIDATE${SHARD:+ shard $SHARD}"
}

parse_args "$@"
if (( LIST_SHARDS )); then
    if (( JSON )); then
        shard_manifest json "$SHARDS"
    else
        shard_manifest check "$SHARDS"
        printf '%s\n' "$SHARDS"
    fi
    exit 0
fi
if (( SELF_TEST )); then
    run_self_test
    exit 0
fi
# Soak identity is a parse-time refuse. Do it before the exclusive lock so a
# unit test (or a second invocation) cannot be blocked by a live matrix run.
refuse_soak "$NAMESPACE" "$RELEASE"
exec 9>"$LOCK_FILE"
if ! "$GNU_PROCESS" flock -n 9; then
    die "another cluster-upgrade-matrix holds $LOCK_FILE"
fi
run_matrix
