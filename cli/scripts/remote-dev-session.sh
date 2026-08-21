#!/usr/bin/env bash
# EXPERIMENTAL developer-facing session loop for remote development, over an
# ALREADY-INSTALLED cluster release. Sibling of cli/scripts/remote-dev-spike.sh:
# the spike falsifies the claim once against the compose Docker substrate, this
# script is the repeatable loop against a real Kubernetes release.
#
# It never runs `cluster up` and never installs, upgrades, or uninstalls
# anything: the release, the namespace, and the sandbox pods are the platform's,
# and this script only deploys a bundle, sends turns, and reads the workspace.
#
#   curie dev remote-dev-session start --namespace <ns> --channel <id> \
#       --listen-host <ip> --repo-url <url> --task "<text>" \
#       [--plugin-dir <dir>] [--timeout-secs <n>]
#   curie dev remote-dev-session turn "<instruction>"
#   curie dev remote-dev-session status
#   curie dev remote-dev-session finish [--branch <name>] [--cluster | --push | --pr]
#   curie dev remote-dev-session down
#
# Cluster access comes from the caller's KUBECONFIG and current context. This
# script never sets, guesses, or switches a context: picking one for the caller
# is how a scratch run lands in somebody's production cluster.
#
# Env knobs:
#   CURIE_BIN   path to a prebuilt curie binary (skip cargo build)
#
# Stdout is ONE JSON object (the machine-readable result). Every human progress
# line goes to stderr, so `curie dev remote-dev-session turn ... | jq` stays
# parseable.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Fixed path, not a mktemp: every verb after `start` is a separate process and
# has to find what `start` left behind.
STATE_DIR="/tmp/curie-remote-dev-session"
STATE_FILE="$STATE_DIR/state.json"
# The workspace path inside the sandbox. /home/runner is one of the two writable
# mounts on the read-only rootfs (the other is /tmp), so it is the only place a
# checkout can live.
SANDBOX_REPO="/home/runner/workspace/repo"
# Sandbox pods are named per thread, so a name filter matches nothing; the
# component label is the only handle that selects them.
SANDBOX_SELECTOR="app.kubernetes.io/component=runner-sandbox"
# The sandbox pod carries init containers, so every exec must name the runner
# container explicitly or kubectl picks the wrong one.
RUNNER_CONTAINER="runner"
# The claim can lag the turn by a couple of seconds, so the after-snapshot is
# retried rather than read once.
POD_WAIT_SECS=60
JOB_WAIT_SECS=300

BIN=""

MODE=""
NAMESPACE=""
CHANNEL=""
LISTEN_HOST=""
REPO_URL=""
TASK=""
PLUGIN_DIR=""
TIMEOUT_SECS="240"
BRANCH=""
CLUSTER=0
PUSH=0
PR=0
FORCE=0
INSTRUCTION=""
THREAD=""
POD=""
TURNS="0"

say() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
# Exit 2 means "you invoked this wrong", exit 1 means "the session ran and
# something failed". A caller that cannot tell those apart retries a typo.
usage_die() { printf 'error: %s\n' "$*" >&2; exit 2; }

usage() {
    cat >&2 <<'USAGE'
usage:
  remote-dev-session start --namespace <ns> --channel <id> --listen-host <ip> \
      --repo-url <url> --task "<text>" [--plugin-dir <dir>] [--timeout-secs <n>]
  remote-dev-session turn "<instruction>"
  remote-dev-session status
  remote-dev-session finish [--branch <name>] [--cluster | --push | --pr]
  remote-dev-session down [--force]
USAGE
}

# A --repo-url may carry userinfo (https://user:token@host/...). The token must
# never reach stderr, state.json, or the emitted JSON; the unredacted URL is
# used only for the clone itself. Unanchored and global, because git's own
# error output embeds the URL mid-line (fatal: unable to access '<url>').
redact_url() {
    printf '%s' "$1" | sed -E "s#([a-z+]+://)[^/@[:space:]']+@#\1***@#g"
}

# ---------------------------------------------------------------- arg parsing

if [[ $# -eq 0 ]]; then
    usage
    usage_die "a verb is required."
fi
MODE="$1"
shift
case "$MODE" in
    start|turn|status|finish|down) ;;
    *)
        usage
        usage_die "unknown verb '$MODE'. Expected one of: start, turn, status, finish, down." ;;
esac

case "$MODE" in
    start)
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --namespace)
                    [[ $# -ge 2 ]] || usage_die "--namespace needs a value"
                    NAMESPACE="$2"; shift 2 ;;
                --channel)
                    [[ $# -ge 2 ]] || usage_die "--channel needs a value"
                    CHANNEL="$2"; shift 2 ;;
                --listen-host)
                    [[ $# -ge 2 ]] || usage_die "--listen-host needs a value"
                    LISTEN_HOST="$2"; shift 2 ;;
                --repo-url)
                    [[ $# -ge 2 ]] || usage_die "--repo-url needs a value"
                    REPO_URL="$2"; shift 2 ;;
                --task)
                    [[ $# -ge 2 ]] || usage_die "--task needs a value"
                    TASK="$2"; shift 2 ;;
                --plugin-dir)
                    [[ $# -ge 2 ]] || usage_die "--plugin-dir needs a value"
                    PLUGIN_DIR="$2"; shift 2 ;;
                --timeout-secs)
                    [[ $# -ge 2 ]] || usage_die "--timeout-secs needs a value"
                    TIMEOUT_SECS="$2"; shift 2 ;;
                *) usage_die "unknown argument '$1' for verb 'start'" ;;
            esac
        done
        # No default namespace, on purpose: `curie cluster deploy` defaults to
        # `curie`, and inheriting that default here would point a session at
        # whatever release happens to own that namespace.
        [[ -n "$NAMESPACE" ]] || usage_die "start requires --namespace"
        # One agent per channel: a second agent on an occupied channel is
        # rejected by the platform, and the usual default (C0LOCALDEV) is
        # already taken by the weather example on most installs.
        [[ -n "$CHANNEL" ]] || usage_die "start requires --channel"
        # Required plumbing, never auto-detected: auto-detection can pick a
        # Tailscale address that the cluster's pods cannot route back to, and
        # the failure is a silent timeout minutes later.
        [[ -n "$LISTEN_HOST" ]] || usage_die "start requires --listen-host"
        [[ -n "$REPO_URL" ]] || usage_die "start requires --repo-url"
        [[ -n "$TASK" ]] || usage_die "start requires --task"
        [[ "$TIMEOUT_SECS" =~ ^[0-9]+$ ]] || usage_die "--timeout-secs must be a whole number of seconds"
        [[ -n "$PLUGIN_DIR" ]] || PLUGIN_DIR="$REPO_ROOT/examples/coder"
        ;;
    turn)
        [[ $# -ge 1 ]] || usage_die "turn requires the instruction text"
        INSTRUCTION="$1"
        shift
        [[ $# -eq 0 ]] || usage_die "turn takes exactly one instruction argument; quote it"
        [[ -n "$INSTRUCTION" ]] || usage_die "the instruction text must not be empty"
        ;;
    status)
        [[ $# -eq 0 ]] || usage_die "$MODE takes no arguments"
        ;;
    down)
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --force) FORCE=1; shift ;;
                *) usage_die "unknown argument '$1' for verb 'down'" ;;
            esac
        done
        ;;
    finish)
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --branch)
                    [[ $# -ge 2 ]] || usage_die "--branch needs a value"
                    BRANCH="$2"; shift 2 ;;
                --cluster) CLUSTER=1; shift ;;
                --push) PUSH=1; shift ;;
                --pr) PUSH=1; PR=1; shift ;;
                *) usage_die "unknown argument '$1' for verb 'finish'" ;;
            esac
        done
        if (( CLUSTER && (PUSH || PR) )); then
            usage_die "--cluster cannot be combined with --push or --pr"
        fi
        ;;
esac

# --------------------------------------------------------------- prerequisites

require_tools() {
    local missing=()
    local tool
    for tool in kubectl jq git "$@"; do
        command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
    done
    if (( ${#missing[@]} )); then
        die "missing required tool(s): ${missing[*]}. Install them and re-run."
    fi
}

resolve_bin() {
    if [[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN:-}" ]]; then
        # Absolutize: this script invokes the binary from other directories, so
        # a relative $CURIE_BIN stops resolving after the first cd.
        BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"
        say "using prebuilt binary: $BIN"
    else
        say "=== cargo build --release (cli) ==="
        (cd "$REPO_ROOT/cli" && cargo build --release --quiet)
        # Ask cargo where the artifact landed rather than assuming cli/target:
        # a build.target-dir override (shared target dirs) moves it.
        local target_dir
        target_dir="$(cd "$REPO_ROOT/cli" && cargo metadata --format-version 1 --no-deps 2>/dev/null | jq -r '.target_directory // ""')"
        [[ -n "$target_dir" ]] || target_dir="$REPO_ROOT/cli/target"
        BIN="$target_dir/release/curie"
    fi
    [[ -x "$BIN" ]] || die "no usable curie binary at $BIN"
}

# ------------------------------------------------------------------- state I/O

load_state() {
    [[ -f "$STATE_FILE" ]] || die "no session state at $STATE_FILE. Run \`curie dev remote-dev-session start ...\` first."
    NAMESPACE="$(jq -r '.namespace // ""' "$STATE_FILE")"
    CHANNEL="$(jq -r '.channel // ""' "$STATE_FILE")"
    LISTEN_HOST="$(jq -r '.listen_host // ""' "$STATE_FILE")"
    THREAD="$(jq -r '.thread // ""' "$STATE_FILE")"
    POD="$(jq -r '.pod // ""' "$STATE_FILE")"
    REPO_URL="$(jq -r '.repo_url // ""' "$STATE_FILE")"
    PLUGIN_DIR="$(jq -r '.plugin_dir // ""' "$STATE_FILE")"
    TIMEOUT_SECS="$(jq -r '.timeout_secs // "240"' "$STATE_FILE")"
    TURNS="$(jq -r '.turns // 0' "$STATE_FILE")"
    [[ -n "$NAMESPACE" && -n "$CHANNEL" && -n "$THREAD" && -n "$POD" ]] \
        || die "$STATE_FILE is incomplete; run \`curie dev remote-dev-session down\` and start again."
}

# Read-modify-write through a temp file so an interrupted update cannot leave a
# truncated state.json behind. Arguments are passed straight to jq (options
# first, filter last), exactly as jq itself expects them.
update_state() {
    local tmp="$STATE_DIR/state.json.tmp"
    jq "$@" "$STATE_FILE" > "$tmp"
    mv "$tmp" "$STATE_FILE"
}

# ---------------------------------------------------------------- cluster glue

kexec() {
    kubectl exec -n "$NAMESPACE" "$POD" -c "$RUNNER_CONTAINER" -- "$@"
}

sandbox_pods() {
    kubectl get pods -n "$NAMESPACE" -l "$SANDBOX_SELECTOR" -o name 2>/dev/null | sort
}

# The single machine-readable turn. `cluster message` exits non-zero on a
# timeout, so the exit code is swallowed here and the terminal shape of the
# payload is judged instead (an unparseable payload is its own failure below).
send_turn() {
    local text="$1" thread="${2:-}"
    local args=(--json cluster message
        --namespace "$NAMESPACE"
        --channel "$CHANNEL"
        --listen-host "$LISTEN_HOST"
        --timeout-secs "$TIMEOUT_SECS")
    if [[ -n "$thread" ]]; then
        args+=(--thread "$thread")
    fi
    "$BIN" "${args[@]}" "$text" || true
}

# Accept the finalized shape and nothing else. An empty reply is NOT rejected:
# a model can finalize with no text and no workspace change, and reporting that
# honestly is the whole point of the before/after diff comparison below.
assert_turn_shape() {
    local label="$1" payload="$2" verdict
    verdict="$(printf '%s' "$payload" | jq -r '
        if type != "object" then "unparseable"
        elif .dry_run then "dry_run"
        elif .awaiting_approval then "awaiting_approval"
        elif .timed_out then "timed_out"
        elif .status == "enqueued" then "enqueued"
        elif .finalized == true then "ok"
        else "not_finalized" end
    ' 2>/dev/null || echo unparseable)"
    case "$verdict" in
        ok) ;;
        awaiting_approval)
            die "$label: the turn parked on an approval gate instead of finalizing; the session bundle must not require approval." ;;
        timed_out)
            die "$label: no reply finalized within ${TIMEOUT_SECS}s. Raise --timeout-secs, or check that --listen-host is an address the cluster's pods can reach." ;;
        dry_run)
            die "$label: got a dry-run descriptor; the session must run for real." ;;
        enqueued)
            die "$label: the turn was enqueued onto the real stream and no reply was waited for, so this session cannot read the result." ;;
        *)
            printf '%s\n' "$payload" >&2
            die "$label: the payload is neither finalized nor a known terminal shape." ;;
    esac
}

# The observable state of the workspace: tracked edits plus the porcelain, since
# an untracked new file is a real change that `git diff` alone never shows.
workspace_signature() {
    printf '%s\n--\n%s' "$(workspace_diff)" "$(workspace_porcelain)"
}

workspace_diff() {
    kexec git -C "$SANDBOX_REPO" diff
}

workspace_porcelain() {
    kexec git -C "$SANDBOX_REPO" status --porcelain
}

# =================================================================== verb: start

verb_start() {
    require_tools
    [[ -d "$PLUGIN_DIR" ]] || die "--plugin-dir '$PLUGIN_DIR' is not a directory."
    if [[ -e "$STATE_FILE" ]]; then
        die "$STATE_FILE already exists, so a previous session was never closed out. Run \`curie dev remote-dev-session down\` first."
    fi
    resolve_bin

    local safe_url
    safe_url="$(redact_url "$REPO_URL")"
    say "=== remote-dev session: start (namespace $NAMESPACE, channel $CHANNEL) ==="
    say "repo url for the sandbox: $safe_url"

    say
    say "=== curie --json cluster deploy --plugin-dir $PLUGIN_DIR ==="
    local deploy_json agent_id deployment_id
    deploy_json="$("$BIN" --json cluster deploy \
        --plugin-dir "$PLUGIN_DIR" \
        --namespace "$NAMESPACE" \
        --slack-channel "$CHANNEL")"
    agent_id="$(printf '%s' "$deploy_json" | jq -r '.agent.id // ""')"
    deployment_id="$(printf '%s' "$deploy_json" | jq -r '.deployment.id // ""')"
    [[ -n "$agent_id" && "$agent_id" != "null" ]] || die "deploy receipt carried no agent.id: $deploy_json"
    [[ -n "$deployment_id" && "$deployment_id" != "null" ]] || die "deploy receipt carried no deployment.id: $deploy_json"
    say "deployed agent $agent_id, deployment $deployment_id"

    # Snapshot BEFORE the turn: the claim is identified by difference, so a
    # sandbox that was already running (another session's) can never be mistaken
    # for this one's.
    local before after
    before="$(sandbox_pods)"

    say
    say "=== turn 1: session start (claims the managed Agent Sandbox) ==="
    # Claim-only on purpose: the checkout does not exist yet, so handing the
    # model its task here would make it hunt an empty workspace. The task goes
    # out as its own turn AFTER the clone lands.
    local out thread
    out="$(send_turn "Remote development session start. Do nothing yet: the working repository checkout will appear at $SANDBOX_REPO before your next instruction, and that instruction will carry your task.")"
    printf '%s\n' "$out" >&2
    assert_turn_shape "turn-1" "$out"
    thread="$(printf '%s' "$out" | jq -r '.thread // ""')"
    [[ -n "$thread" ]] || die "turn-1 finalized but carried no thread identity, so later turns cannot rejoin this session."

    say
    say "=== identify the claimed sandbox pod ==="
    local new_pods count waited=0
    while :; do
        after="$(sandbox_pods)"
        new_pods="$(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") | sed '/^$/d')"
        count="$(printf '%s' "$new_pods" | grep -c . || true)"
        if [[ "$count" == "1" ]]; then
            break
        fi
        if (( waited >= POD_WAIT_SECS )); then
            say "expected exactly one NEW sandbox pod after turn 1, saw $count after ${waited}s:"
            printf '%s\n' "$new_pods" >&2
            die "cannot identify the sandbox this session claimed."
        fi
        sleep 2
        waited=$(( waited + 2 ))
    done
    POD="${new_pods#pod/}"
    say "claimed sandbox pod: $POD"

    say
    say "=== clone the repository into the sandbox ==="
    # The shipped release runner has no git; this command only works against an
    # install whose runner image was derived with git (the Milestone 1 recipe).
    # Fail that loudly here instead of as a cryptic exec error mid-clone.
    if ! kubectl exec -n "$NAMESPACE" "$POD" -c "$RUNNER_CONTAINER" -- git --version >/dev/null 2>&1; then
        die "the runner image in pod $POD has no usable git. This session needs a release whose agentSandbox.runner.image is a git-bearing derivative of the runner (FROM the runner image, apt-get install git); the shipped image does not carry it."
    fi
    # The unredacted URL appears exactly here, as the clone's argv, and nowhere
    # else: every other mention of it goes through redact_url. The timeout is
    # load-bearing: on a sealed install the egress NetworkPolicy blackholes the
    # connection, so an unbounded clone hangs instead of failing.
    local clone_err
    if ! clone_err="$(kubectl exec -n "$NAMESPACE" "$POD" -c "$RUNNER_CONTAINER" -- timeout 120 git clone "$REPO_URL" "$SANDBOX_REPO" 2>&1)"; then
        say "the sandbox could not clone $safe_url. git said:"
        # git's own stderr echoes the remote URL (userinfo included), so it goes
        # through the same redaction as every other mention.
        redact_url "$clone_err" >&2
        printf '\n' >&2
        say "if this timed out or could not reach the host, the release's runner egress probably does not allow it: the repository's HTTPS ranges must be opened at install time (curie cluster up --allow-web-egress <cidr>)."
        die "clone into the sandbox failed, so the session has no checkout to work on."
    fi
    say "cloned $safe_url into $SANDBOX_REPO"

    say
    say "=== turn 2: hand the model its task in the now-present checkout ==="
    local out2 reply
    out2="$(send_turn "The repository checkout is now present at $SANDBOX_REPO. Begin your task: $TASK" "$thread")"
    printf '%s\n' "$out2" >&2
    assert_turn_shape "turn-2" "$out2"
    reply="$(printf '%s' "$out2" | jq -r '.reply // ""')"

    mkdir -p "$STATE_DIR"
    # The REDACTED url is what gets recorded: state.json is a world-readable
    # file under /tmp and must never hold a credential.
    jq -n \
        --arg namespace "$NAMESPACE" \
        --arg channel "$CHANNEL" \
        --arg listen_host "$LISTEN_HOST" \
        --arg thread "$thread" \
        --arg pod "$POD" \
        --arg repo_url "$safe_url" \
        --arg plugin_dir "$PLUGIN_DIR" \
        --arg timeout_secs "$TIMEOUT_SECS" \
        '{namespace: $namespace, channel: $channel, listen_host: $listen_host,
          thread: $thread, pod: $pod, repo_url: $repo_url,
          plugin_dir: $plugin_dir, timeout_secs: $timeout_secs,
          turns: 2, workdirs: [], unpushed: []}' > "$STATE_FILE"

    jq -n \
        --arg namespace "$NAMESPACE" \
        --arg channel "$CHANNEL" \
        --arg listen_host "$LISTEN_HOST" \
        --arg state_file "$STATE_FILE" \
        --arg thread "$thread" \
        --arg pod "$POD" \
        --arg repo_url "$safe_url" \
        --arg reply "$reply" \
        '{session: {namespace: $namespace, channel: $channel,
                    listen_host: $listen_host, state_file: $state_file},
          thread: $thread, pod: $pod, repo_url: $repo_url, reply: $reply}'
}

# ==================================================================== verb: turn

verb_turn() {
    require_tools
    load_state
    resolve_bin
    say "=== remote-dev session: turn against pod $POD (thread $THREAD) ==="

    # Captured BEFORE the turn, because a finalized reply says nothing about
    # whether the workspace moved: a model can finalize with an empty reply and
    # no edit at all, and only this comparison can tell the difference.
    local pre post out reply changed
    pre="$(workspace_signature)"

    say
    say "=== send the instruction ==="
    out="$(send_turn "$INSTRUCTION" "$THREAD")"
    printf '%s\n' "$out" >&2
    assert_turn_shape "turn" "$out"
    reply="$(printf '%s' "$out" | jq -r '.reply // ""')"

    post="$(workspace_signature)"
    if [[ "$pre" == "$post" ]]; then
        changed=false
        say "workspace UNCHANGED by this turn (the reply text is not evidence of an edit)"
    else
        changed=true
        say "workspace changed by this turn"
    fi

    update_state '.turns = (.turns + 1)'
    local turns
    turns="$(jq -r '.turns' "$STATE_FILE")"

    local diff
    diff="$(workspace_diff)"

    jq -n \
        --arg thread "$THREAD" \
        --argjson turn "$turns" \
        --arg reply "$reply" \
        --argjson changed "$changed" \
        --arg diff "$diff" \
        '{thread: $thread, turn: $turn, reply: $reply,
          changed: $changed, diff: $diff}'
}

# ================================================================== verb: status

verb_status() {
    require_tools
    load_state
    say "=== remote-dev session: status ==="

    local phase changed_files diff
    phase="$(kubectl get pod -n "$NAMESPACE" "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ -n "$phase" ]] || phase="Unknown"
    say "pod $POD is $phase"

    changed_files="$(workspace_porcelain)"
    diff="$(workspace_diff)"

    jq -n \
        --arg thread "$THREAD" \
        --arg pod "$POD" \
        --arg pod_phase "$phase" \
        --argjson turns "$TURNS" \
        --arg changed_files "$changed_files" \
        --arg diff "$diff" \
        '{thread: $thread, pod: $pod, pod_phase: $pod_phase, turns: $turns,
          changed_files: $changed_files, diff: $diff}'
}

# ================================================================== verb: finish

verb_finish_cluster() {
    local diff="$1"
    local REPO_PATH ID RESOURCE_NAME RUNNER_IMAGE

    # The existence check has no capture, so the token stays off host storage.
    if ! kubectl get secret curie-secrets -n "$NAMESPACE" \
        -o jsonpath='{.data.githubToken}' 2>/dev/null | grep . >/dev/null; then
        die "the release has no GitHub token; set it with CURIE_GITHUB""_TOKEN=... curie cluster up --github-token or patch curie-secrets."
    fi

    # State's repo_url is the REDACTED form, so a private repo's URL carries a
    # literal ***@ userinfo. It is not part of the repository path; strip it.
    [[ "$REPO_URL" =~ ^https://([^/@]+@)?github\.com/ ]] \
        || die "in-cluster publication only supports github.com https URLs."
    REPO_PATH="$(printf '%s' "$REPO_URL" | sed -E 's#^https://([^/@]+@)?github\.com/##')"
    REPO_PATH="${REPO_PATH%.git}"

    if [[ -z "$BRANCH" ]]; then
        BRANCH="task/remote-dev-session-$(printf '%s' "$THREAD" | tr -c 'A-Za-z0-9._-' '-')"
    fi

    ID="$(printf '%s' "$THREAD" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9-' '-')"
    RESOURCE_NAME="remote-dev-publish-$ID"
    RESOURCE_NAME="${RESOURCE_NAME:0:63}"
    while [[ "$RESOURCE_NAME" == *- ]]; do
        RESOURCE_NAME="${RESOURCE_NAME%-}"
    done

    RUNNER_IMAGE="$(kubectl get pod -n "$NAMESPACE" "$POD" \
        -o jsonpath='{.spec.containers[?(@.name=="runner")].image}' 2>/dev/null || true)"
    [[ -n "$RUNNER_IMAGE" ]] \
        || die "claimed sandbox pod $POD has no runner image to use for in-cluster publication."

    local publish_dir patch_bytes
    local LC_ALL=C
    publish_dir="$(mktemp -d /tmp/curie-remote-dev-session-publish.XXXXXX)"
    printf '%s\n' "$diff" > "$publish_dir/session.patch"
    patch_bytes=$(( ${#diff} + 1 ))
    if (( patch_bytes > 900000 )); then
        rm -rf "$publish_dir"
        die "the session patch exceeds 900000 bytes; ConfigMaps have a 1MiB limit, so in-cluster publication cannot safely carry it."
    fi

    # Clone and push logs are redacted inside the Job before kubectl can return them.
    cat > "$publish_dir/publish.sh" <<'PUBLISH'
#!/usr/bin/env bash
set -euo pipefail

redact() { sed -E 's#([a-z+]+://)[^/@[:space:]]+@#\1***@#g'; }

export HOME=/tmp/publish-home
mkdir -p "$HOME"
git config --global user.name "Brian Conn"
git config --global user.email "bcconn2112@gmail.com"
git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/${REPO_PATH}.git" /tmp/publish-repo 2>&1 | redact >&2
cd /tmp/publish-repo
BASE="$(git symbolic-ref --short HEAD)"
git checkout -b "$BRANCH"
git apply /publish/session.patch
git add -A
git commit \
    -m "Remote dev session changes" \
    -m "Produced over ${TURNS} remote development session turn(s)."
git push -u origin HEAD 2>&1 | redact >&2

python3 - "$BASE" <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

repo_path = os.environ["REPO_PATH"]
title = "Remote dev session changes"
body = f"Produced over {os.environ['TURNS']} remote development session turn(s)."
payload = json.dumps(
    {
        "title": title,
        "head": os.environ["BRANCH"],
        "base": sys.argv[1],
        "body": body,
    }
).encode("utf-8")
request = urllib.request.Request(
    f"https://api.github.com/repos/{repo_path}/pulls",
    data=payload,
    method="POST",
    headers={
        "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "curie-remote-dev-session",
    },
)
try:
    with urllib.request.urlopen(request) as response:
        result = json.load(response)
except urllib.error.HTTPError as error:
    print(error.read().decode("utf-8", errors="replace"), file=sys.stderr)
    raise SystemExit(1)

print(f"CURIE_PR_URL={result['html_url']}")
PY
PUBLISH

    say "=== upload publication inputs to ConfigMap $RESOURCE_NAME ==="
    if ! kubectl create configmap "$RESOURCE_NAME" -n "$NAMESPACE" \
        --from-file="$publish_dir" >/dev/null; then
        rm -rf "$publish_dir"
        die "could not create publication ConfigMap $RESOURCE_NAME."
    fi
    if ! kubectl label configmap "$RESOURCE_NAME" -n "$NAMESPACE" \
        app.kubernetes.io/managed-by=curie-remote-dev-session --overwrite >/dev/null; then
        rm -rf "$publish_dir"
        die "could not label publication ConfigMap $RESOURCE_NAME."
    fi
    rm -rf "$publish_dir"

    local name_json image_json repo_path_json branch_json turns_json
    name_json="$(jq -Rn --arg value "$RESOURCE_NAME" '$value')"
    image_json="$(jq -Rn --arg value "$RUNNER_IMAGE" '$value')"
    repo_path_json="$(jq -Rn --arg value "$REPO_PATH" '$value')"
    branch_json="$(jq -Rn --arg value "$BRANCH" '$value')"
    turns_json="$(jq -Rn --arg value "$TURNS" '$value')"

    say "=== create publication Job $RESOURCE_NAME ==="
    # No retries: push and pull request creation are not safely repeatable.
    kubectl apply -n "$NAMESPACE" -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $name_json
  labels:
    app.kubernetes.io/managed-by: curie-remote-dev-session
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: curie-remote-dev-session
    spec:
      restartPolicy: Never
      containers:
        - name: publish
          image: $image_json
          command: ["bash", "/publish/publish.sh"]
          env:
            - name: GITHUB_TOKEN
              valueFrom:
                secretKeyRef:
                  name: curie-secrets
                  key: githubToken
            - name: REPO_PATH
              value: $repo_path_json
            - name: BRANCH
              value: $branch_json
            - name: TURNS
              value: $turns_json
          volumeMounts:
            - name: publish
              mountPath: /publish
              readOnly: true
      volumes:
        - name: publish
          configMap:
            name: $name_json
EOF

    local waited=0 succeeded failed logs pr_url
    while :; do
        succeeded="$(kubectl get job "$RESOURCE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
        failed="$(kubectl get job "$RESOURCE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.failed}' 2>/dev/null || true)"
        if [[ -n "$succeeded" && "$succeeded" != "0" ]]; then
            break
        fi
        if [[ -n "$failed" && "$failed" != "0" ]]; then
            kubectl logs -n "$NAMESPACE" "job/$RESOURCE_NAME" >&2 || true
            die "in-cluster publication Job $RESOURCE_NAME failed."
        fi
        if (( waited >= JOB_WAIT_SECS )); then
            kubectl logs -n "$NAMESPACE" "job/$RESOURCE_NAME" >&2 || true
            die "in-cluster publication Job $RESOURCE_NAME did not finish within ${JOB_WAIT_SECS}s."
        fi
        sleep 5
        waited=$(( waited + 5 ))
    done

    logs="$(kubectl logs -n "$NAMESPACE" "job/$RESOURCE_NAME" 2>&1 || true)"
    printf '%s\n' "$logs" >&2
    pr_url="$(printf '%s\n' "$logs" | sed -n 's/^CURIE_PR_URL=//p' | tail -n1)"
    [[ -n "$pr_url" ]] \
        || die "in-cluster publication Job $RESOURCE_NAME succeeded without reporting a pull request URL."

    jq -n \
        --arg branch "$BRANCH" \
        --arg job "$RESOURCE_NAME" \
        --arg pr_url "$pr_url" \
        '{branch: $branch, job: $job, pr_url: $pr_url,
          pushed: true, cluster: true}'
}

verb_finish() {
    if (( PR )); then
        require_tools gh
    else
        require_tools
    fi
    load_state
    say "=== remote-dev session: finish ==="

    # Stage everything first so the patch carries new files and staged edits,
    # not just tracked modifications; --binary keeps non-text content intact.
    # The reset afterwards puts the sandbox index back so later turn/status
    # comparisons keep seeing the working-tree state they expect.
    local diff
    kexec git -C "$SANDBOX_REPO" add -A \
        || die "could not stage the sandbox checkout for extraction."
    diff="$(kexec git -C "$SANDBOX_REPO" diff --cached --binary)"
    kexec git -C "$SANDBOX_REPO" reset -q
    [[ -n "$diff" ]] || die "the sandbox checkout has no changes at all (tracked or new), so there is nothing to carry out of this session."

    if (( CLUSTER )); then
        verb_finish_cluster "$diff"
        return
    fi

    local parent workdir patch_file
    parent="$(mktemp -d /tmp/curie-remote-dev-session-finish.XXXXXX)"
    workdir="$parent/repo"
    patch_file="$parent/session.patch"
    printf '%s\n' "$diff" > "$patch_file"
    # Recorded as UNPUSHED until a push succeeds: `down` refuses to delete
    # unpushed workdirs without --force, because after a no-push finish this
    # directory holds the only copy of the branch.
    update_state --arg dir "$parent" '.unpushed += [$dir]'
    say "patch written to $patch_file"

    # The recorded URL is the REDACTED one, so a repo that needed inline
    # credentials cannot be re-cloned here by design; configure a git credential
    # helper for that host instead of putting the token back into state.json.
    say "=== clone $REPO_URL on the host ==="
    local clone_err
    if ! clone_err="$(git clone "$REPO_URL" "$workdir" 2>&1)"; then
        redact_url "$clone_err" >&2
        printf '\n' >&2
        die "cloning $REPO_URL on the host failed, so the session changes cannot be turned into a branch."
    fi

    if [[ -z "$BRANCH" ]]; then
        BRANCH="task/remote-dev-session-$(printf '%s' "$THREAD" | tr -c 'A-Za-z0-9._-' '-')"
    fi
    say "=== branch $BRANCH ==="
    git -C "$workdir" checkout -q -b "$BRANCH" \
        || die "could not create branch '$BRANCH' in $workdir."
    git -C "$workdir" apply "$patch_file" \
        || die "\`git apply\` rejected $patch_file. The host clone's HEAD has moved away from what the sandbox cloned; re-run against the matching base."
    git -C "$workdir" add -A
    git -C "$workdir" commit -q \
        -m "Remote dev session changes" \
        -m "Produced over $TURNS remote development session turn(s)." \
        || die "the commit failed in $workdir. Set a git identity (user.name and user.email) and re-run."
    say "committed on $BRANCH"

    local pushed=false pr_url=""
    if (( PUSH )); then
        say "=== git push -u origin HEAD ==="
        git -C "$workdir" push -u origin HEAD >&2 \
            || die "the push failed from $workdir."
        pushed=true
        # Pushed work is safe to clean; move it out of the unpushed set.
        update_state --arg dir "$parent" '.unpushed -= [$dir] | .workdirs += [$dir]'
        if (( PR )); then
            say "=== gh pr create --fill ==="
            local gh_out
            gh_out="$(cd "$workdir" && gh pr create --fill)" \
                || die "\`gh pr create --fill\` failed from $workdir."
            # gh prints progress lines before the URL, so the URL is the last line.
            pr_url="$(printf '%s\n' "$gh_out" | tail -n1)"
            say "pull request: $pr_url"
        else
            say "pushed; no pull request opened (--push stops at the push). To open one:"
            say "  cd $workdir && gh pr create --fill"
        fi
    else
        say
        say "not pushed. From $workdir, run:"
        say "  git push -u origin HEAD"
        say "  gh pr create --fill"
    fi

    jq -n \
        --arg branch "$BRANCH" \
        --arg workdir "$workdir" \
        --arg patch_file "$patch_file" \
        --argjson pushed "$pushed" \
        --arg pr_url "$pr_url" \
        '{branch: $branch, workdir: $workdir, patch_file: $patch_file,
          pushed: $pushed}
         + (if $pr_url == "" then {} else {pr_url: $pr_url} end)'
}

# ==================================================================== verb: down

verb_down() {
    require_tools
    say "=== remote-dev session: down ==="
    # Deliberately NOT a teardown of the platform: the release, the namespace,
    # and the sandbox pod all belong to the platform (the worker's sandbox
    # substrate claims and reaps a pod per thread), and a session command that
    # uninstalled them would take down work it does not own. `down` removes only
    # what this script itself created for the session.
    local cleanup_namespace=""
    if [[ -f "$STATE_FILE" ]]; then
        cleanup_namespace="$(jq -r '.namespace // ""' "$STATE_FILE")"
        # Unpushed finish workdirs hold the ONLY copy of their branch; without
        # --force, refuse rather than destroy them.
        local unpushed
        unpushed="$(jq -r '.unpushed[]? // empty' "$STATE_FILE")"
        if [[ -n "$unpushed" ]] && (( ! FORCE )); then
            say "these finish workdirs hold branches that were never pushed:"
            printf '  %s\n' "$unpushed" >&2
            die "refusing to delete unpushed work. Push those branches first (finish printed the commands), or run \`down --force\` to discard them."
        fi
        local dirs dir
        dirs="$(jq -r '(.workdirs[]? // empty), (.unpushed[]? // empty)' "$STATE_FILE")"
        while IFS= read -r dir; do
            [[ -n "$dir" ]] || continue
            say "removing host workdir $dir"
            rm -rf "$dir"
        done <<< "$dirs"
    else
        say "no session state at $STATE_FILE; nothing recorded to clean up."
    fi
    if [[ -n "$cleanup_namespace" ]]; then
        say "removing in-cluster publication Jobs and ConfigMaps from namespace $cleanup_namespace"
        kubectl delete jobs,configmaps -n "$cleanup_namespace" \
            -l app.kubernetes.io/managed-by=curie-remote-dev-session \
            --ignore-not-found >/dev/null || true
    fi
    rm -rf "$STATE_DIR"
    say "the sandbox pod and the cluster release are left alone; they are the platform's."

    jq -n '{cleaned: true}'
}

case "$MODE" in
    start) verb_start ;;
    turn) verb_turn ;;
    status) verb_status ;;
    finish) verb_finish ;;
    down) verb_down ;;
esac
