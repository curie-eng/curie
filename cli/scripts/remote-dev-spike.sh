#!/usr/bin/env bash
# THROWAWAY spike for the remote development product thesis. Not a gate, not an
# E2E test, and not a supported surface -- it exists to falsify one claim end to
# end and is expected to be deleted with the spike.
#
# The claim under test: a single command can stand up a managed Agent Sandbox,
# get a real repository checked out INSIDE it, run a coding-harness turn against
# that checkout, and hand back a machine-readable session identity plus a real
# `git diff` of what changed. Every link is observed, never asserted in prose:
# the sandbox is a container that actually exists, the clone is a real clone over
# a real git transport, the sentinel file changes on disk, and the diff comes out
# of `git diff` in the sandbox.
#
# ASSUMPTION, stated because the whole spike rests on it: Docker middle mode
# (apps/worker/src/curie_worker/sandbox/docker.py, DockerSandboxClient) IS the
# managed Agent Sandbox substrate for this spike. The claim, the affinity route,
# the per-thread naming, and the reaping are the same substrate code the
# Kubernetes path uses; only the container runtime differs. A cluster-tier
# version of this spike is a separate exercise.
#
# HONESTY, and the thing to read before trusting any output: under the FAKE
# model (the default) the runner is contractually a no-op -- it returns a canned
# reply and edits nothing. So the sentinel edit is applied by THIS SCRIPT acting
# as the trusted checkout-supervisor / workspace-inspector, exactly as a real
# remote-dev control plane would, and the emitted JSON says so
# (`edit_actor: "supervisor-deterministic"`). What the fake run proves is the
# plumbing: claim, clone, turn round trip, workspace mutation, diff readback.
# It proves NOTHING about a model's ability to make the edit. Under `--live` the
# script does NOT touch the file and instead ASSERTS the model made the edit
# (`edit_actor: "live-model"`); that is the run to point at when the question is
# whether the harness can actually code.
#
# Single instance per box, by design: the state lives at a fixed path
# (/tmp/curie-remote-dev-spike), the compose stack is a singleton, and the
# spike must OWN that stack because the worker's runner image is fixed at
# `local up` time. Two concurrent spikes on one box is not a supported shape.
#
#   curie dev remote-dev-spike [run] [--repo-url <url>] [--live] [--keep]
#   curie dev remote-dev-spike follow-up
#   curie dev remote-dev-spike down
#
# Env knobs:
#   CURIE_BIN                    path to a prebuilt curie binary (skip cargo build)
#   CURIE_RD_SPIKE_BASE_IMAGE    runner image the spike image derives from
#   CURIE_RD_SPIKE_GIT_PORT      host port for the throwaway git daemon (default 9419)
#
# Stdout is ONE JSON object (the machine-readable result). Every human progress
# line goes to stderr, so `curie dev remote-dev-spike | jq` stays parseable.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The derived runner image: the published runner has NO git binary and its
# rootfs is read-only, so `apt-get install git` inside a running sandbox cannot
# work. git must be baked in ahead of the claim.
SPIKE_IMAGE="curie-runner-remote-dev-spike:latest"
BASE_IMAGE="${CURIE_RD_SPIKE_BASE_IMAGE:-ghcr.io/curie-eng/curie-runner:latest}"
# Sandbox containers are named per thread (curie-thread-<hash>-<nonce>), so a
# name filter matches nothing; the substrate label is the only handle that
# actually selects them (apps/worker/src/curie_worker/sandbox/types.py).
SANDBOX_LABEL="curietech.ai/managed-by=curie-sandbox-substrate"
# Fixed path, not a mktemp: `follow-up` and `down` are separate processes and
# have to find what `run --keep` left behind.
STATE_DIR="/tmp/curie-remote-dev-spike"
STATE_FILE="$STATE_DIR/state.json"
WORK="$STATE_DIR/workdir"
GIT_PORT="${CURIE_RD_SPIKE_GIT_PORT:-9419}"
GIT_PID_FILE="$STATE_DIR/git-daemon.pid"
# The workspace path inside the sandbox. /home/runner is one of the two writable
# tmpfs mounts on the read-only rootfs (the other is /tmp), so it is the only
# place a checkout can live.
SANDBOX_REPO="/home/runner/workspace/repo"
# Fixed so the test repo's commit sha is deterministic and the spike can prove
# the clone landed the exact commit it served.
FIXED_DATE="1700000000 +0000"
BUNDLE_SRC="$REPO_ROOT/examples/weather"

MODE="run"
REPO_URL=""
REPO_URL_OVERRIDDEN=0
LIVE=0
KEEP=0

# Set once this run brought the compose stack up, so the teardown trap never
# reaps a stack it did not start.
STACK_OWNED=0
GIT_DAEMON_PID=""
BIN=""

say() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
# Exit 2 means "you invoked this wrong", exit 1 means "the spike ran and
# something failed". A caller that cannot tell those apart retries a typo.
usage_die() { printf 'error: %s\n' "$*" >&2; exit 2; }

# A --repo-url may carry userinfo (https://user:token@host/...). The token must
# never reach stderr, state.json, or the emitted JSON; the unredacted URL is
# used only for the clone itself. Unanchored and global, because git's own
# error output embeds the URL mid-line (fatal: unable to access '<url>').
redact_url() {
    printf '%s' "$1" | sed -E "s#([a-z+]+://)[^/@[:space:]']+@#\1***@#g"
}

# ---------------------------------------------------------------- arg parsing

if [[ $# -gt 0 && "$1" != -* ]]; then
    MODE="$1"
    shift
fi
case "$MODE" in
    run|follow-up|down) ;;
    *) usage_die "unknown mode '$MODE'. Expected one of: run, follow-up, down." ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-url)
            [[ $# -ge 2 ]] || usage_die "--repo-url needs a value"
            REPO_URL="$2"
            REPO_URL_OVERRIDDEN=1
            shift 2 ;;
        --live) LIVE=1; shift ;;
        --keep) KEEP=1; shift ;;
        *) usage_die "unknown argument '$1' for mode '$MODE'" ;;
    esac
done
if [[ "$MODE" != "run" ]] && (( REPO_URL_OVERRIDDEN || LIVE || KEEP )); then
    # A follow-up cannot change the repo or the model mode: both were fixed when
    # `run` brought the stack up, and accepting the flag would imply otherwise.
    usage_die "--repo-url, --live, and --keep apply to \`run\` only; '$MODE' reads them from the recorded state."
fi

# --------------------------------------------------------------- prerequisites

require_tools() {
    local missing=()
    for tool in docker jq git; do
        command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
    done
    if (( ${#missing[@]} )); then
        die "missing required tool(s): ${missing[*]}. Install them and re-run."
    fi
    # Only a default `run` publishes the fixture over a daemon: a --repo-url run
    # starts none (that is the whole point of the escape hatch the
    # missing-daemon message below points at), and follow-up/down reuse or kill
    # what run left, so neither may be blocked by an absent daemon binary.
    if (( REPO_URL_OVERRIDDEN )) || [[ "$MODE" != "run" ]]; then
        return 0
    fi
    # `git daemon` ships as a separate binary in git-core and is absent from
    # some slim installs; discovering that AFTER building an image and booting a
    # stack wastes minutes, so check it up front.
    if [[ ! -x "$(git --exec-path)/git-daemon" ]] && ! git daemon --help >/dev/null 2>&1; then
        die "\`git daemon\` is not available (looked in $(git --exec-path)). Install the git-daemon package, or pass --repo-url pointing at a repository the sandbox can already reach."
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

stack_running() {
    [[ -n "$(docker ps -q --filter 'name=curie-api' 2>/dev/null)" ]]
}

# ------------------------------------------------------------------- teardown

kill_git_daemon() {
    local pid="${1:-}"
    [[ -n "$pid" ]] || return 0
    kill "$pid" >/dev/null 2>&1 || true
    # git daemon forks per connection; the process group dies with the leader in
    # practice, and a stray child holding the port is caught by the port
    # precheck on the next run rather than silently reused.
    wait "$pid" 2>/dev/null || true
}

# Only once the compose stack is gone: while it is up, a sandbox carrying this
# label may belong to a live session that is not this spike's, and force-removing
# it would break work this script has no business touching.
sweep_sandbox_orphans() {
    if stack_running; then
        return 0
    fi
    local orphans
    orphans="$(docker ps -a --filter "label=$SANDBOX_LABEL" --format '{{.Names}}' \
        | grep '^curie-thread-' || true)"
    if [[ -n "$orphans" ]]; then
        say "removing leftover sandbox containers:"
        printf '%s\n' "$orphans" >&2
        # shellcheck disable=SC2086
        docker rm -f $orphans >/dev/null 2>&1 || true
    fi
}

assert_no_survivors() {
    local survivors
    # By LABEL, never by name: `--filter name=curie` is a host-wide substring
    # match that reds on other worktrees' containers, and a check that cries
    # wolf is a check nobody reads.
    survivors="$(docker ps --filter 'label=com.docker.compose.project=curie' --format '{{.Names}}')"
    if [[ -n "$survivors" ]]; then
        say "teardown left compose services running:"
        printf '%s\n' "$survivors" >&2
        return 1
    fi
    survivors="$(docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}')"
    if [[ -n "$survivors" ]]; then
        say "teardown left sandbox containers running:"
        printf '%s\n' "$survivors" >&2
        return 1
    fi
    say "no curie containers running (both label filters clean)"
}

full_teardown() {
    if (( STACK_OWNED )); then
        say "=== curie local down ==="
        "$BIN" local down >&2 || say "warning: \`local down\` failed; sweeping anyway."
        STACK_OWNED=0
        local orphans
        orphans="$(docker ps -aq --filter "label=$SANDBOX_LABEL" 2>/dev/null || true)"
        if [[ -n "$orphans" ]]; then
            say "sweeping orphaned sandbox containers"
            # shellcheck disable=SC2086
            docker rm -f $orphans >/dev/null 2>&1 || true
        fi
    fi
    kill_git_daemon "$GIT_DAEMON_PID"
    GIT_DAEMON_PID=""
    rm -rf "$STATE_DIR"
}

# Armed only by `run`, and only after the point where there is something to tear
# down. Captures the real exit code first so a teardown failure cannot turn a
# red run green.
run_cleanup() {
    local code=$?
    set +e
    if (( code == 0 && KEEP )); then
        say
        say "--keep: leaving the stack, the git daemon, and $STATE_DIR in place."
        say "follow-up turn: curie dev remote-dev-spike follow-up"
        say "tear down:      curie dev remote-dev-spike down"
        exit "$code"
    fi
    say
    say "=== teardown ==="
    full_teardown
    if (( code == 0 )); then
        assert_no_survivors || code=1
    fi
    exit "$code"
}

# ------------------------------------------------------------------ assertions

# Accept ONLY the finalized-with-a-reply shape. Mirrors
# cli/scripts/e2e-ladder.sh's assert_finalized_reply: the other terminal shapes
# in cli/schema/message.schema.json have different causes and a merged message
# wastes debugging time.
assert_finalized_reply() {
    local label="$1" payload="$2" verdict reply
    verdict="$(printf '%s' "$payload" | jq -r '
        if type != "object" then "unparseable"
        elif .dry_run then "dry_run"
        elif .awaiting_approval then "awaiting_approval"
        elif .timed_out then "timed_out"
        elif (.finalized == true and (.reply | type) == "string") then
            (if (.reply | gsub("^\\s+|\\s+$"; "")) == "" then "empty_reply" else "ok" end)
        else "not_finalized" end
    ' 2>/dev/null || echo unparseable)"
    reply="$(printf '%s' "$payload" | jq -r '.reply // ""' 2>/dev/null || echo "")"

    case "$verdict" in
        ok) ;;
        empty_reply)
            die "$label: the turn finalized but the reply was empty, so nothing made it back through the plumbing." ;;
        awaiting_approval)
            die "$label: the turn parked on an approval gate instead of finalizing; the spike's bundle must not require approval." ;;
        timed_out)
            die "$label: no reply finalized before the deadline. The worker never completed the turn." ;;
        dry_run)
            die "$label: got a dry-run descriptor; the spike must run for real." ;;
        *)
            printf '%s\n' "$payload" >&2
            die "$label: the payload is neither finalized nor a known terminal shape." ;;
    esac
    say "$label: turn finalized with a reply (plumbing observed; reply content deliberately not graded)"
    if (( LIVE )) && [[ "$reply" == "all done" ]]; then
        # Live-only negative control, not a grader: "all done" is the fake
        # model's only reply (runner/src/curie_runner/fake.py), so a live run
        # that returns it never reached a real model.
        die "$label: --live returned the fake model's canned reply, so the run was not live."
    fi
}

# ---------------------------------------------------------------- shared steps

# All `curie local message` invocations run with CWD set to the spike workdir so
# `.curie/` state (last-turn.json, which --continue reads) never lands in the
# repo checkout this script is executed from.
spike_message() {
    local out
    out="$(cd "$WORK" && "$BIN" --json local message "$@" || true)"
    printf '%s' "$out"
}

read_thread_field() {
    local field="$1"
    if [[ -f "$WORK/.curie/last-turn.json" ]]; then
        jq -r --arg f "$field" '.[$f] // ""' "$WORK/.curie/last-turn.json" 2>/dev/null || echo ""
    else
        echo ""
    fi
}

sandbox_env() {
    local container="$1" key="$2"
    docker inspect "$container" \
        --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
        | sed -n "s/^${key}=//p" | head -n1
}

emit_result() {
    local mode="$1" container="$2" edit_actor="$3" diff="$4"
    local observed_sha="$5" expected_sha="$6" repo_url="$7"
    jq -n \
        --arg spike "remote-dev-sandbox" \
        --arg mode "$mode" \
        --arg channel "$(read_thread_field channel)" \
        --arg thread_ts "$(read_thread_field thread_ts)" \
        --arg container "$container" \
        --arg sandbox_id "$(sandbox_env "$container" CURIE_SANDBOX_ID)" \
        --arg session_id "$(sandbox_env "$container" CURIE_SESSION_ID)" \
        --arg image "$SPIKE_IMAGE" \
        --arg repo_url "$repo_url" \
        --arg expected_sha "$expected_sha" \
        --arg observed_sha "$observed_sha" \
        --arg edit_actor "$edit_actor" \
        --arg diff "$diff" \
        '{
            spike: $spike,
            mode: $mode,
            thread: { channel: $channel, thread_ts: $thread_ts },
            sandbox: {
                container: $container,
                curie_sandbox_id: $sandbox_id,
                curie_session_id: $session_id,
                image: $image
            },
            repo: {
                url: $repo_url,
                expected_head_sha: (if $expected_sha == "" then null else $expected_sha end),
                observed_head_sha: $observed_sha
            },
            edit_actor: $edit_actor,
            diff: $diff
        }'
}

assert_diff_contains() {
    local diff="$1"
    shift
    local needle
    for needle in "$@"; do
        if [[ "$diff" != *"$needle"$'\n'* && "$diff" != *"$needle" ]]; then
            printf '%s\n' "$diff" >&2
            die "the sandbox's \`git diff\` does not contain the expected line '$needle'. The workspace did not change the way the spike claims."
        fi
    done
}

# ==================================================================== mode: run

mode_run() {
    require_tools
    say "=== remote-dev spike: run (model mode: $( ((LIVE)) && echo LIVE || echo FAKE ) ) ==="

    if stack_running; then
        die "a compose stack is already running. This spike must OWN the stack, because the worker's runner image (CURIE_RUNNER_IMAGE) is fixed at \`local up\` time and the spike needs its derived, git-bearing image. Run \`curie local down\` first."
    fi
    if [[ -f "$STATE_FILE" ]]; then
        die "$STATE_FILE already exists, so a previous spike was never torn down. Run \`curie dev remote-dev-spike down\` first."
    fi
    if (( LIVE )); then
        if [[ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${ANTHROPIC_API_KEY:-}" && -z "${CURIE_CREDENTIALS:-}" ]]; then
            die "--live needs a model credential. Export CLAUDE_CODE_OAUTH_TOKEN, ANTHROPIC_API_KEY, or CURIE_CREDENTIALS (an sk-or- key routes to OpenRouter; set CURIE_MODEL to pick the model), or drop --live to run sealed against the fake model."
        fi
    fi
    # A connect probe rather than `ss`/`lsof`, so the precheck needs no extra
    # binary and reports the condition that actually matters: something is
    # already answering on the port the daemon must bind. Irrelevant under
    # --repo-url, which binds nothing.
    if (( ! REPO_URL_OVERRIDDEN )) && timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$GIT_PORT" 2>/dev/null; then
        die "port $GIT_PORT is already in use; the spike's throwaway git daemon cannot bind it. Set CURIE_RD_SPIKE_GIT_PORT to a free port."
    fi

    resolve_bin

    say
    say "=== build $SPIKE_IMAGE (adds git to $BASE_IMAGE) ==="
    # Context-free build from stdin: the only thing this image adds is a package,
    # so shipping the repo as build context would be pure waste.
    docker build -t "$SPIKE_IMAGE" - <<DOCKERFILE
FROM ${BASE_IMAGE}
USER 0
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER 1000:1000
DOCKERFILE

    mkdir -p "$WORK"
    # Armed here, before anything durable exists: from this point on every exit
    # path -- including a signal -- goes through the teardown, so a half-built
    # spike cannot strand a stack, a daemon, or a state dir.
    trap run_cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    local expected_sha=""
    if (( REPO_URL_OVERRIDDEN )); then
        say
        say "=== --repo-url given: no fixture repository, no mirror to serve ==="
        say "the sandbox clones the URL you passed, so this spike publishes nothing."
    else
        say
        say "=== create the deterministic test repository ==="
        local src="$WORK/repo-src"
        mkdir -p "$src"
        (
            cd "$src"
            git init -q -b main
            # Local, never global: this box's identity must not leak into the
            # fixture, and the sha has to be reproducible run over run.
            git config user.name "Curie Spike"
            git config user.email "spike@example.invalid"
            printf 'remote-dev spike fixture repository\n' > README.md
            printf 'sentinel=unchanged\n' > SENTINEL.txt
            git add -A
            GIT_AUTHOR_DATE="$FIXED_DATE" GIT_COMMITTER_DATE="$FIXED_DATE" \
                git commit -q -m "seed remote-dev spike fixture"
        )
        expected_sha="$(git -C "$src" rev-parse HEAD)"
        say "fixture HEAD: $expected_sha"

        mkdir -p "$WORK/serve"
        git clone -q --bare "$src" "$WORK/serve/repo.git"
    fi

    say
    say "=== curie local up --minimal (CURIE_RUNNER_IMAGE=$SPIKE_IMAGE) ==="
    # Ownership is claimed BEFORE the boot blocks: containers exist for the whole
    # health-wait window, so flipping the flag afterwards would strand a stack
    # this run created if it died mid-boot.
    STACK_OWNED=1
    if (( LIVE )); then
        # Live means live: an inherited CURIE_FAKE_MODEL=1 would silently seal a
        # run the operator asked to be real.
        env -u CURIE_FAKE_MODEL CURIE_RUNNER_IMAGE="$SPIKE_IMAGE" "$BIN" local up --minimal >&2
    else
        # Exported explicitly, not merely defaulted: `local up` auto-flips to live
        # when a credential happens to be in the shell, and a spike run must be
        # sealed unless it was asked to be live.
        CURIE_RUNNER_IMAGE="$SPIKE_IMAGE" CURIE_FAKE_MODEL=1 "$BIN" local up --minimal >&2
    fi

    if (( REPO_URL_OVERRIDDEN )); then
        say
        say "=== --repo-url given: no git daemon, and no gateway to resolve ==="
    else
        say
        say "=== serve the fixture over git:// ==="
        git daemon --export-all --base-path="$WORK/serve" --port="$GIT_PORT" \
            --reuseaddr --pid-file="$GIT_PID_FILE" >/dev/null 2>&1 &
        GIT_DAEMON_PID=$!
        # Sanity-check the daemon actually serves before anything downstream
        # depends on it, so a clone failure inside the sandbox means a SANDBOX
        # networking problem and not a dead daemon.
        local probe="$WORK/daemon-probe"
        local ok=0
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if git clone -q "git://127.0.0.1:$GIT_PORT/repo.git" "$probe" 2>/dev/null; then
                ok=1
                break
            fi
            sleep 0.5
        done
        rm -rf "$probe"
        (( ok )) || die "the throwaway git daemon on port $GIT_PORT never served the fixture from the host, so the sandbox has no chance either."
        say "git daemon serving on port $GIT_PORT (pid $GIT_DAEMON_PID, host clone verified)"

        # Only the fixture path needs the gateway: it is the sandbox's route back
        # to a daemon bound on the host.
        local gateway
        gateway="$(docker network inspect curie_runner -f '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true)"
        [[ -n "$gateway" ]] || die "could not resolve the curie_runner network gateway, so the sandbox has no route back to the host's git daemon. Pass --repo-url with a URL the sandbox can reach."
        REPO_URL="git://$gateway:$GIT_PORT/repo.git"
    fi
    say "repo url for the sandbox: $(redact_url "$REPO_URL")"

    say
    say "=== curie --json local deploy --plugin-dir $BUNDLE_SRC ==="
    local deploy_json agent_id deployment_id
    deploy_json="$("$BIN" --json local deploy --plugin-dir "$BUNDLE_SRC")"
    agent_id="$(printf '%s' "$deploy_json" | jq -r '.agent.id // ""')"
    deployment_id="$(printf '%s' "$deploy_json" | jq -r '.deployment.id // ""')"
    [[ -n "$agent_id" && "$agent_id" != "null" ]] || die "deploy receipt carried no agent.id: $deploy_json"
    [[ -n "$deployment_id" && "$deployment_id" != "null" ]] || die "deploy receipt carried no deployment.id: $deploy_json"
    say "deployed agent $agent_id, deployment $deployment_id"

    # Snapshot BEFORE the turn: the claim is identified by difference, so a
    # sandbox that was already running (another session's) can never be mistaken
    # for this spike's.
    local before after
    before="$(docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}' | sort)"

    say
    say "=== turn 1: session start (claims the managed Agent Sandbox) ==="
    local out
    out="$(spike_message "Remote-dev spike: session start. A repository will be cloned to $SANDBOX_REPO.")"
    printf '%s\n' "$out" >&2
    assert_finalized_reply "turn-1" "$out"

    after="$(docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}' | sort)"
    local new_containers container count
    new_containers="$(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") | sed '/^$/d')"
    count="$(printf '%s' "$new_containers" | grep -c . || true)"
    if [[ "$count" != "1" ]]; then
        say "expected exactly one NEW sandbox container after turn 1, saw $count:"
        printf '%s\n' "$new_containers" >&2
        die "cannot identify the sandbox this spike claimed."
    fi
    container="$new_containers"
    say "claimed sandbox: $container"
    say "  CURIE_SANDBOX_ID=$(sandbox_env "$container" CURIE_SANDBOX_ID)"
    say "  CURIE_SESSION_ID=$(sandbox_env "$container" CURIE_SESSION_ID)"

    say
    say "=== checkout-supervisor: clone into the sandbox ==="
    # The unredacted URL appears exactly here, as the clone's argv, and nowhere
    # else: every other mention of it goes through redact_url.
    local clone_err safe_url
    safe_url="$(redact_url "$REPO_URL")"
    if ! clone_err="$(docker exec "$container" git clone "$REPO_URL" "$SANDBOX_REPO" 2>&1)"; then
        say "the sandbox could not clone $safe_url. git said:"
        # git's own stderr echoes the remote URL (userinfo included), so it goes
        # through the same redaction as every other mention.
        redact_url "$clone_err" >&2
        printf '\n' >&2
        die "clone into the sandbox failed -- this is the falsifiable negative path, and it just fired."
    fi
    say "cloned $safe_url into $SANDBOX_REPO"

    local observed_sha reported_expected=""
    observed_sha="$(docker exec "$container" git -C "$SANDBOX_REPO" rev-parse HEAD | tr -d '\r\n')"
    if (( REPO_URL_OVERRIDDEN )); then
        say "observed HEAD in the sandbox: $observed_sha (--repo-url overrode the fixture, so no sha is asserted)"
    else
        reported_expected="$expected_sha"
        [[ "$observed_sha" == "$expected_sha" ]] || die "the sandbox checked out $observed_sha but the fixture served $expected_sha, so it is not the repository this spike published."
        say "observed HEAD in the sandbox matches the fixture: $observed_sha"
    fi

    say
    say "=== turn 2: coding turn ==="
    out="$(spike_message --continue "In $SANDBOX_REPO, edit SENTINEL.txt: replace the line sentinel=unchanged with sentinel=changed-by-turn-1. Make no other changes.")"
    printf '%s\n' "$out" >&2
    assert_finalized_reply "turn-2" "$out"

    local edit_actor
    if (( LIVE )); then
        edit_actor="live-model"
        local content
        content="$(docker exec "$container" cat "$SANDBOX_REPO/SENTINEL.txt")"
        if [[ "$content" != *"sentinel=changed-by-turn-1"* ]]; then
            printf '%s\n' "$content" >&2
            die "--live: the model did not make the edit. SENTINEL.txt still does not contain sentinel=changed-by-turn-1."
        fi
        say "live model made the sentinel edit"
    else
        edit_actor="supervisor-deterministic"
        say "fake model: THIS SCRIPT is applying the sentinel edit as the trusted checkout-supervisor. The fake runner is contractually a no-op, so nothing here says a model can code."
        docker exec "$container" sed -i 's/^sentinel=.*/sentinel=changed-by-turn-1/' "$SANDBOX_REPO/SENTINEL.txt"
    fi

    say
    say "=== workspace-inspector: git diff inside the sandbox ==="
    local diff
    diff="$(docker exec "$container" git -C "$SANDBOX_REPO" diff)"
    [[ -n "$diff" ]] || die "the sandbox's \`git diff\` is empty, so nothing observable changed in the workspace."
    assert_diff_contains "$diff" "+sentinel=changed-by-turn-1"
    say "diff observed, and it carries the sentinel change"

    mkdir -p "$STATE_DIR"
    # The REDACTED url is what gets recorded: `follow-up` never re-clones, so it
    # has no use for the credential, and state.json is a world-readable file
    # under /tmp.
    jq -n \
        --arg workdir "$WORK" \
        --arg container "$container" \
        --arg repo_url "$safe_url" \
        --arg pid "${GIT_DAEMON_PID:-}" \
        --argjson keep "$KEEP" \
        --argjson live "$LIVE" \
        --arg expected_sha "$reported_expected" \
        '{workdir: $workdir, container: $container, repo_url: $repo_url,
          git_daemon_pid: $pid, keep: ($keep == 1), live: ($live == 1),
          expected_head_sha: $expected_sha}' > "$STATE_FILE"

    emit_result run "$container" "$edit_actor" "$diff" "$observed_sha" "$reported_expected" "$safe_url"
}

# ============================================================== mode: follow-up

mode_follow_up() {
    require_tools
    [[ -f "$STATE_FILE" ]] || die "no spike state at $STATE_FILE. Run \`curie dev remote-dev-spike run --keep\` first; a run without --keep tears everything down on the way out."
    resolve_bin

    local container repo_url expected_sha state_live
    WORK="$(jq -r '.workdir' "$STATE_FILE")"
    container="$(jq -r '.container' "$STATE_FILE")"
    # Already redacted when `run` recorded it, and kept that way: a follow-up
    # turn never re-clones, so the credential is not needed here at all.
    repo_url="$(jq -r '.repo_url' "$STATE_FILE")"
    expected_sha="$(jq -r '.expected_head_sha // ""' "$STATE_FILE")"
    state_live="$(jq -r '.live' "$STATE_FILE")"
    # The recorded run's model mode governs, not this invocation's flags: the
    # stack's mode was fixed at `local up` time and a follow-up cannot change it.
    [[ "$state_live" == "true" ]] && LIVE=1 || LIVE=0

    [[ -n "$(docker ps -q --filter "name=^${container}$" 2>/dev/null)" ]] \
        || die "the recorded sandbox container '$container' is not running, so the follow-up cannot reach the same checkout. Tear down with \`curie dev remote-dev-spike down\` and run the spike again."

    say "=== remote-dev spike: follow-up against $container ==="

    say
    say "=== turn 3: follow-up coding turn ==="
    local out
    out="$(spike_message --continue "In $SANDBOX_REPO, append the line sentinel-2=changed-by-turn-2 to SENTINEL.txt.")"
    printf '%s\n' "$out" >&2
    assert_finalized_reply "turn-3" "$out"

    local edit_actor
    if (( LIVE )); then
        edit_actor="live-model"
        local content
        content="$(docker exec "$container" cat "$SANDBOX_REPO/SENTINEL.txt")"
        if [[ "$content" != *"sentinel-2=changed-by-turn-2"* ]]; then
            printf '%s\n' "$content" >&2
            die "--live: the model did not make the follow-up edit. SENTINEL.txt does not contain sentinel-2=changed-by-turn-2."
        fi
        say "live model made the follow-up edit"
    else
        edit_actor="supervisor-deterministic"
        say "fake model: THIS SCRIPT is appending the follow-up line as the trusted checkout-supervisor."
        docker exec "$container" sh -c "echo 'sentinel-2=changed-by-turn-2' >> $SANDBOX_REPO/SENTINEL.txt"
    fi

    # Same container, same checkout: the claim under test is session CONTINUITY,
    # so the follow-up must land in the workspace turn 1 created rather than a
    # fresh one. The re-read below is what proves it, since turn 1's change is
    # only still in the diff if the working tree survived.
    local newest
    newest="$(docker inspect "$container" --format '{{.Name}}' | sed 's#^/##')"
    [[ "$newest" == "$container" ]] || die "container identity drifted: state records '$container' but docker reports '$newest'."

    say
    say "=== workspace-inspector: git diff inside the sandbox ==="
    local diff observed_sha
    diff="$(docker exec "$container" git -C "$SANDBOX_REPO" diff)"
    [[ -n "$diff" ]] || die "the sandbox's \`git diff\` is empty after the follow-up turn."
    assert_diff_contains "$diff" "+sentinel=changed-by-turn-1" "+sentinel-2=changed-by-turn-2"
    say "diff carries BOTH turns' changes, so the follow-up hit the same checkout"
    observed_sha="$(docker exec "$container" git -C "$SANDBOX_REPO" rev-parse HEAD | tr -d '\r\n')"

    emit_result follow-up "$container" "$edit_actor" "$diff" "$observed_sha" "$expected_sha" "$repo_url"
}

# =================================================================== mode: down

mode_down() {
    require_tools
    resolve_bin
    say "=== remote-dev spike: down ==="

    # Ownership comes from what THIS spike recorded, never from "a stack happens
    # to be running": a running stack with no spike state is somebody else's, and
    # tearing it down is exactly the damage this mode must not do.
    local pid="" owned=0
    if [[ -f "$STATE_FILE" ]]; then
        owned=1
        pid="$(jq -r '.git_daemon_pid // ""' "$STATE_FILE")"
        WORK="$(jq -r '.workdir // ""' "$STATE_FILE")"
    fi
    if [[ -f "$GIT_PID_FILE" ]]; then
        # A weaker signal than state.json -- a `run` that died before writing its
        # state leaves this behind -- but it is still unambiguously this spike's
        # leftover, so it still authorizes the teardown.
        owned=1
        [[ -n "$pid" ]] || pid="$(cat "$GIT_PID_FILE" 2>/dev/null || true)"
    fi

    if (( owned )); then
        # Unconditional, not gated on the stack being up: `local down` is safe on
        # a stopped stack, and gating it would strand the volumes and networks of
        # a spike whose containers already exited.
        say "=== curie local down (spike state found, so this stack is ours) ==="
        "$BIN" local down >&2 || say "warning: \`local down\` failed; sweeping anyway."
        kill_git_daemon "$pid"
        sweep_sandbox_orphans
        rm -rf "$STATE_DIR"
        assert_no_survivors
        say "spike torn down"
        return 0
    fi

    say "no spike state at $STATE_FILE and no $GIT_PID_FILE, so this spike owns no teardown."
    if stack_running; then
        # Reported, never touched, and never a failure: this is the normal
        # outcome of running `down` on a box where someone else has a stack up.
        say "a compose stack IS running; it is not this spike's, so it is left alone:"
        docker ps --filter 'label=com.docker.compose.project=curie' --format '{{.Names}}' >&2
        say "its sandbox containers are left alone too -- a live session may own them."
    else
        sweep_sandbox_orphans
    fi
    rm -rf "$STATE_DIR"
    say "nothing of this spike's was left to tear down."
}

case "$MODE" in
    run) mode_run ;;
    follow-up) mode_follow_up ;;
    down) mode_down ;;
esac
