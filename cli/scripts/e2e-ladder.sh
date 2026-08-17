#!/bin/bash
# Cold-start parity ladder for the curie CLI (issue #690).
#
# This is an E2E test, not a gate: it drives the SAME bundle through each
# deployment tier with the tier's own real verbs and asserts a turn actually
# finalized. Rung 1 (skill) is the existing `cli/scripts/e2e.sh`, invoked as is
# so the skill leg has exactly one implementation. Rung 2 (local) is
# `local up --minimal` -> `local deploy` -> `local message` -> `local down`,
# against `compose.dev.yaml`. The `local-release` mode is the same round trip
# against `compose.release.yaml` instead -- the generated, checkout-free
# artifact `curie local up` runs on a release binary (issue #695), one half
# of the `compose.dev.yaml` / generated-release-compose parity seam named in
# AGENTS.md. CI validates that generated file today only by `docker compose
# config` and service-count assertions (the `compose` job), never by running a
# turn through it -- exactly the gap this mode closes. Rung 3 (cluster) is
# `cluster deploy` -> `cluster message` against a release that is ALREADY
# installed; the ladder never installs or uninstalls one.
#
# What it is NOT: it is not a compose test and not a helm test. Every step goes
# through a `curie` verb, because the point is to catch a tier whose verb
# drifted from its sibling. The one raw-docker use is the post-teardown
# assertion that nothing curie-related survived.
#
# Blast radius of the teardown sweep: sandbox containers are matched by the
# substrate label, which is host-wide and not per-worktree, so the sweep only
# runs when THIS ladder brought the compose stack up. A stack it merely reused
# belongs to another session, and so do that session's sandboxes.
#
# Fake model by default, so a default run is credential-free even on a box that
# HAS credentials. Under CURIE_E2E_LIVE=1 the ladder runs the real model on
# every rung and requires a credential up front. Under fake, the local and
# cluster rungs assert PLUMBING only -- that a turn finalized and a reply came
# back -- never reply CONTENT (ADR-0055, #612): an assertion tuned to the
# fake's canned reply manufactures a green. Rung 1 (skill) runs its own real
# eval graders against whichever model it booted (cli/scripts/e2e.sh), fake or
# live, since that leg is acceptance evidence for #325, not a plumbing probe.
#
# Requirements: docker, a cargo toolchain (or $CURIE_BIN), and an
# curie-runner image (`curie build`). Rung 3 additionally needs a reachable
# cluster with a release installed. Run from anywhere:
#
#   bash cli/scripts/e2e-ladder.sh
#
# Env knobs:
#   CURIE_E2E_TIERS        comma list of rungs (default skill,local; `all` =
#                            skill,local,cluster). A NAMED tier is REQUIRED: if
#                            cluster is named and no release responds, exit 1.
#                            `local-release` is a fourth, separately-named rung
#                            (the same local round trip against the generated
#                            compose.release.yaml instead of compose.dev.yaml);
#                            it is NOT folded into `all` because it needs the
#                            release-pinned images (ghcr.io/curie-eng/curie-api
#                            and -worker-local) built and tagged locally first,
#                            a step `all`'s existing skill/local/cluster rungs
#                            don't require -- name it explicitly, e.g.
#                            CURIE_E2E_TIERS=skill,local,local-release.
#   CURIE_E2E_LIVE         1 = real model on every named rung, including
#                            rung 1 (cli/scripts/e2e.sh reads this same var
#                            itself rather than being told by the ladder).
#   CURIE_BIN              path to a prebuilt curie binary (skip cargo build)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIERS="${CURIE_E2E_TIERS:-skill,local}"
LIVE="${CURIE_E2E_LIVE:-0}"
# Fixed, not an env knob: the ladder asserts PLUMBING, so the bundle it ships is
# a fixed input of the test rather than something a caller varies.
BUNDLE_SRC="$REPO_ROOT/examples/weather"
# Hardcoded, and deliberately NOT an env knob: the stub port is the constant
# DEFAULT_LOCAL_STUB_PORT in cli/src/message.rs, pinned to the compose worker's
# SLACK_API_BASE_URL. An override would only move this script's precheck, so it
# could green-light an occupied 8155 and then hang on the message timeout.
STUB_PORT=8155
PROMPT="What is the weather in Denver right now?"
# The fake model's only reply (runner/src/curie_runner/fake.py). It is used
# ONLY as a live-mode negative control -- "the reply must not be this" -- never
# as a pass condition. Matching it to green is the #612 bypass.
FAKE_SENTINEL="all done"

# Set once the ladder itself brought the compose stack up. The thread that
# brought a stack up owns tearing it down, so a stack that was already running
# when the ladder started is reused and left alone.
LOCAL_STACK_OWNED=0

# The label the sandbox substrate stamps on every runner container it spawns
# (cli/src/docker.rs SANDBOX_LABEL, apps/worker sandbox/types.py). Container
# NAMES are per-thread (curie-thread-<digest>-<nonce>), so a name filter
# matches nothing; the label is the only handle that actually selects them.
SANDBOX_LABEL="curietech.ai/managed-by=curie-sandbox-substrate"

# The leftover-runner case (#747) stands in a container of its own. The name is
# unique to this run and is NEVER curie-runner-local: that default belongs to
# whatever real `skill up` a developer has going on this box, and this case
# removes what it names.
CONFLICT_NAME="curie-ladder-747-leftover-$$"
CONFLICT_CREATED=0
# The image that case creates its stand-in from. Already a requirement of the
# ladder (rung 1 boots a real runner), so this adds no new prerequisite.
RUNNER_IMAGE="curie-runner"

echo "=== Resolve the curie binary ==="
if [[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN:-}" ]]; then
    # Absolutize: the ladder invokes the binary from other directories, so a
    # relative $CURIE_BIN (as CI passes) must be pinned here or it stops
    # resolving later.
    BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"
    echo "using prebuilt binary: $BIN"
else
    (cd "$REPO_ROOT/cli" && cargo build --release --quiet)
    BIN="$REPO_ROOT/cli/target/release/curie"
fi
"$BIN" --version

WORKDIR="$(mktemp -d)"
cleanup() {
    # Capture the real exit code FIRST: a teardown command that fails must not
    # turn a red run green, and a successful teardown must not mask a red rung.
    local code=$?
    set +e
    # The compose worker spawns runner containers as SIBLINGS on the host daemon
    # via the mounted docker socket, so a rung that died before `local down` can
    # strand them. This raw sweep is a BACKSTOP, not duplication: `local down`
    # already reaps this same label itself (docker::reap_labeled in
    # cli/src/local.rs, #613) and bails loudly if the reap is incomplete, so on
    # any normal path there is nothing here to find. It exists only for the case
    # where `local down` never ran or failed, which is exactly the case that
    # strands containers. Sweep ONLY when this ladder owned the stack: the label is
    # host-wide, and force-removing another session's sandboxes would break a
    # run this one has no business touching.
    if (( LOCAL_STACK_OWNED )); then
        echo
        echo "=== teardown: curie local down ==="
        # Tolerated failure, on top of `set +e`: the stack may never have
        # finished coming up, and a failed `local down` must not skip the
        # sandbox sweep below or change the exit code captured above.
        "$BIN" local down || echo "warning: \`local down\` failed during teardown; sweeping anyway." >&2
        local orphans
        orphans="$(docker ps -aq --filter "label=$SANDBOX_LABEL" 2>/dev/null)"
        if [[ -n "$orphans" ]]; then
            echo "sweeping orphaned sandbox containers"
            # shellcheck disable=SC2086
            docker rm -f $orphans >/dev/null 2>&1
        fi
    fi
    # Only the container THIS run created, matched by its exact unique name, so
    # the sweep can never reach a runner belonging to another session. Cleared by
    # the case itself once `skill down` has removed it.
    if (( CONFLICT_CREATED )); then
        docker rm -f "$CONFLICT_NAME" >/dev/null 2>&1
    fi
    rm -rf "$WORKDIR"
    exit "$code"
}
trap cleanup EXIT
# Without these, a Ctrl-C or a kill can end the shell without running the EXIT
# trap, stranding a running stack on a box that cannot afford one.
trap 'exit 130' INT
trap 'exit 143' TERM

# The ONE place the fake/live asymmetry is stated. There is no shared
# fake-model control across tiers: skill reads CURIE_E2E_LIVE itself (see
# cli/scripts/e2e.sh) and derives its own `--fake-model` flag from it, local
# reads CURIE_FAKE_MODEL, and cluster bakes it into the install. Keeping the
# local/cluster translation here means that seam is written down once; skill
# is exempt because CURIE_E2E_LIVE is already in this process's environment
# by the time `bash e2e.sh` is invoked below, so it needs no translation.
apply_model_mode() {
    if [[ "$LIVE" == "1" ]]; then
        if [[ -z "${ANTHROPIC_API_KEY:-}" && -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${CURIE_CREDENTIALS:-}" ]]; then
            echo "error: CURIE_E2E_LIVE=1 needs a model credential in the environment, and none is set." >&2
            echo "fix: export ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN, or CURIE_CREDENTIALS, or drop CURIE_E2E_LIVE to run sealed against the fake model." >&2
            exit 1
        fi
        # Live means live: an inherited CURIE_FAKE_MODEL=1 would silently seal
        # a run the operator asked to be real.
        unset CURIE_FAKE_MODEL
        echo "model mode: LIVE (real model on the skill, local, and cluster rungs)"
    else
        # Exported, not merely defaulted: a developer shell that happens to carry
        # ANTHROPIC_API_KEY must still get the sealed run. That is what
        # credential-free-by-default means.
        export CURIE_FAKE_MODEL=1
        echo "model mode: FAKE (sealed; CURIE_FAKE_MODEL=1 exported for the local rung)"
    fi
    echo "note: the cluster rung's model is a property of the installed release (cluster up --fake-model), not of this run."
}

# Accept ONLY the `reply` shape with finalized == true and a non-empty reply.
# The other three shapes in cli/schema/message.schema.json get distinct
# messages, because awaiting-approval and timed-out have different causes and a
# merged message wastes debugging time.
assert_finalized_reply() {
    local label="$1" payload="$2" verdict reply
    # stdout only: --json puts the payload on stdout and human text on stderr,
    # so a combined-stream parse fails intermittently and reads like a product bug.
    verdict="$(printf '%s' "$payload" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print("unparseable")
    sys.exit(0)
if not isinstance(d, dict):
    print("unparseable")
elif d.get("dry_run"):
    print("dry_run")
elif d.get("awaiting_approval"):
    print("awaiting_approval")
elif d.get("timed_out"):
    print("timed_out")
elif d.get("finalized") is True and isinstance(d.get("reply"), str):
    # An empty reply is a distinct failure, not a parse failure: the turn
    # finalized but nothing came back, which is exactly the plumbing break
    # this assertion exists to catch.
    print("ok" if d["reply"].strip() else "empty_reply")
    print(d["reply"])
else:
    print("not_finalized")
' || echo "unparseable")"
    # Split the two-line protocol on the FIRST newline: line one is the verdict,
    # everything after it is the reply. `reply` first, because the second
    # expansion overwrites the string both read.
    reply="${verdict#*$'\n'}"
    verdict="${verdict%%$'\n'*}"

    case "$verdict" in
        ok) ;;
        empty_reply)
            echo "$label: the turn finalized but the reply was empty, so no output made it back through the plumbing." >&2
            return 1 ;;
        awaiting_approval)
            echo "$label: the turn parked on an approval gate instead of finalizing; the ladder's bundle must not require approval." >&2
            return 1 ;;
        timed_out)
            echo "$label: no reply finalized before the deadline. The worker never completed the turn." >&2
            return 1 ;;
        dry_run)
            echo "$label: got a dry-run descriptor; the ladder must run for real, never with --dry-run." >&2
            return 1 ;;
        not_finalized)
            echo "$label: the payload is neither finalized nor a known terminal shape." >&2
            printf '%s\n' "$payload" >&2
            return 1 ;;
        *)
            echo "$label: could not parse message --json output:" >&2
            printf '%s\n' "$payload" >&2
            return 1 ;;
    esac

    echo "$label: turn finalized with a reply (plumbing asserted, content deliberately not graded)"
    if [[ "$LIVE" == "1" ]]; then
        # Live-only negative control, not a grader: the fake model cannot say
        # anything but the sentinel, so a live run that returns it never reached
        # a real model.
        if [[ "$reply" == "$FAKE_SENTINEL" ]]; then
            echo "$label: live run returned the fake model's canned reply, so the run was not live." >&2
            return 1
        fi
        echo "$label: reply is not the fake sentinel (live negative control)"
    fi
}

# The local reply stub binds a fixed port. A second ladder, or any process
# holding it, would otherwise hang until the 300s message timeout and look like
# a product failure.
assert_stub_port_free() {
    if (exec 3<>"/dev/tcp/127.0.0.1/$STUB_PORT") 2>/dev/null; then
        echo "error: port $STUB_PORT is already in use, and the local reply stub must bind it." >&2
        echo "fix: stop the process holding it (another ladder run, or a stale local message), then re-run." >&2
        return 1
    fi
}

# A leftover runner container of the target name must fail `skill up` with the
# actionable remedies (exit 2), and `skill down --name` must clear it from a
# directory holding no `.curie/runner.json` (#747).
#
# Live-docker, because the reported defect was a WIRING defect: the planners are
# unit-tested, but nothing proved `skill up` reaches the preflight or that
# `skill down` reaches the removal. Nothing is booted -- the stand-in is created,
# never started, and the preflight matches on `docker ps -a`.
case_leftover_runner_container() {
    echo
    echo "=== case: a leftover runner container is recoverable from the CLI (#747) ==="
    if ! docker image inspect "$RUNNER_IMAGE" >/dev/null 2>&1; then
        echo "error: image '$RUNNER_IMAGE' is not present, and the #747 case creates its leftover from it." >&2
        echo "fix: build it with \`curie build\`, then re-run." >&2
        return 1
    fi
    # Claim ownership BEFORE creating, so a signal between the two cannot strand
    # the container: `docker rm -f` on a name that never existed is a no-op.
    CONFLICT_CREATED=1
    docker create --name "$CONFLICT_NAME" "$RUNNER_IMAGE" sleep 60 >/dev/null

    local out code
    out="$("$BIN" skill up --fake-model --plugin-dir "$WORKDIR/bundle" --name "$CONFLICT_NAME" 2>&1)" && code=0 || code=$?
    printf '%s\n' "$out"
    if (( code != 2 )); then
        echo "skill up on a taken container name must exit 2 (usage), got $code." >&2
        return 1
    fi
    # The whole point of #747: the operator's own remedy, not docker's raw
    # exit-125 "name is already in use by container" text.
    if [[ "$out" != *"container name conflict"* || "$out" != *"skill down --name $CONFLICT_NAME"* ]]; then
        echo "skill up did not surface the actionable name-conflict remedies." >&2
        return 1
    fi
    echo "skill up refused the taken name with the actionable remedies"

    # From $WORKDIR, not the bundle: the reported wedge was a directory with no
    # recorded runner state, which is exactly what `--name` exists to clear.
    (cd "$WORKDIR" && "$BIN" skill down --name "$CONFLICT_NAME")
    # Exact-name filter, never a substring: `name=curie` is host-wide and would
    # report another session's runner as this case's failure.
    if [[ -n "$(docker ps -aq --filter "name=^${CONFLICT_NAME}$")" ]]; then
        echo "skill down --name left '$CONFLICT_NAME' behind." >&2
        return 1
    fi
    CONFLICT_CREATED=0
    echo "skill down --name cleared the leftover with no recorded state"
}

# Rung 1: the existing skill-tier round trip, invoked as is. Never copied. The
# #747 recovery case rides here because it drives skill-tier verbs and shares
# rung 1's runner-image requirement.
rung_skill() {
    echo
    echo "########## rung 1/3: skill ##########"
    bash "$REPO_ROOT/cli/scripts/e2e.sh"
    case_leftover_runner_container
}

# Rung 2: the compose tier, cold start to teardown.
rung_local() {
    echo
    echo "########## rung 2/3: local (compose) ##########"

    assert_stub_port_free

    if [[ -n "$(docker ps -q --filter 'name=curie-api' 2>/dev/null)" ]]; then
        # Reuse it and do NOT tear it down: the thread that brought a stack up
        # owns tearing it down, in both directions.
        echo "a compose stack is already running; reusing it and leaving teardown to whoever started it"
        if [[ "$LIVE" == "1" ]]; then
            # Model mode is fixed at `local up` time, so a reused stack may have
            # been started sealed. Warn rather than refuse: the live-only fake
            # sentinel control below catches it for real.
            echo "warning: CURIE_E2E_LIVE=1, but the reused stack's model mode was fixed by whoever ran \`local up\` and is NOT verified here." >&2
            echo "warning: if that stack was started with the fake model, this 'live' rung runs sealed; \`local down\` then re-run to be sure." >&2
        fi
    else
        echo
        echo "=== curie local up --minimal ==="
        # --minimal selects the core profile and blanks the OTel endpoint itself
        # (core has no collector), so the ladder passes no profiles and no OTel env.
        #
        # Claim ownership BEFORE starting, never after: `local up` blocks for
        # seconds while it waits for health, and containers exist for that whole
        # window. Setting the flag afterwards means a signal or a mid-boot
        # failure leaves the trap disowning a stack this run created, stranding
        # it. Claiming a stack that then fails to boot is harmless, because
        # `local down` is safe against a partial or already-stopped stack.
        LOCAL_STACK_OWNED=1
        "$BIN" local up --minimal
    fi

    echo
    echo "=== curie local deploy ==="
    # No --api-url: the default IS the cold-start path a real user hits, and
    # exercising the default is the point. First create binds C0LOCALDEV, so the
    # message below can resolve the sole deployed agent with no --channel.
    "$BIN" local deploy --plugin-dir "$WORKDIR/bundle"

    echo
    echo "=== curie local message --json ==="
    # Re-probe: the precheck above ran before `local up` and `local deploy`,
    # potentially minutes ago, and the stub binds the port only now.
    assert_stub_port_free
    local out
    # `|| true`: the timeout shape exits non-zero, and the assertion helper is
    # what must classify it, not set -e.
    out="$("$BIN" --json local message "$PROMPT" || true)"
    printf '%s\n' "$out"
    assert_finalized_reply "local" "$out"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval ==="
        "$BIN" local eval --cases "$WORKDIR/bundle/evals/cases.json"
    fi

    if (( LOCAL_STACK_OWNED )); then
        echo
        echo "=== curie local down ==="
        "$BIN" local down
        LOCAL_STACK_OWNED=0

        echo
        echo "=== assert nothing curie-related survived ==="
        # Both checks filter by LABEL, never by name. `--filter name=curie` is
        # a host-wide SUBSTRING match, so on a shared box it reds on containers
        # belonging to other worktrees and sessions (an unrelated
        # `curie-runner-local` from a concurrent `skill up` is enough), and a
        # gate that cries wolf is a gate someone disables. A run may only assert
        # on what it owns.
        local survivors
        # The compose project name is pinned to `curie` by the CLI
        # (cli/src/local.rs COMPOSE_PROJECT_NAME), so this selects exactly the
        # services `local up` started and nothing else.
        survivors="$(docker ps --filter 'label=com.docker.compose.project=curie' --format '{{.Names}}')"
        if [[ -n "$survivors" ]]; then
            echo "local down left compose services running:" >&2
            printf '%s\n' "$survivors" >&2
            return 1
        fi
        # Sandbox containers are named per thread, so a `name=curie-runner`
        # filter matches nothing and the assertion would pass no matter what
        # survived.
        survivors="$(docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}')"
        if [[ -n "$survivors" ]]; then
            echo "sibling sandbox containers survived teardown:" >&2
            printf '%s\n' "$survivors" >&2
            return 1
        fi
        echo "no curie containers running"
    fi
}

# local-release mode: the same local round trip as rung_local, but against the
# GENERATED compose.release.yaml (compose/generate_release_compose.py) instead
# of the checked-in compose.dev.yaml -- the artifact a release binary's
# `curie local up` actually runs, per the compose.dev.yaml / generated
# release compose parity seam (issue #695, AGENTS.md). CI's existing `compose`
# job already asserts this generated file parses and renders the right service
# counts; this mode is the missing half, that a real turn survives it.
rung_local_release() {
    echo
    echo "########## rung: local-release (compose, generated release artifact) ##########"

    local release_compose="$WORKDIR/compose.release.yaml"
    echo
    echo "=== generate compose.release.yaml from compose.dev.yaml ==="
    # No --version: same invocation as the `compose` CI job's config-only
    # check, so this rung exercises the SAME generated text that job only
    # parses, not a differently-pinned variant of it. Run with cwd=$REPO_ROOT:
    # the generator reads compose.dev.yaml and otel/collector-config.yaml by
    # relative path.
    (cd "$REPO_ROOT" && python3 compose/generate_release_compose.py) > "$release_compose"

    # The generated file has no build directives (generate_release_compose.py's
    # T1 replaces the curie-worker build overlay with a pinned
    # ghcr.io/curie-eng/curie-worker-local image, and curie-api/-migrate
    # were already a pull, never a build) -- every curie-owned image it needs
    # must already exist locally under the tag the generator pinned, or `local
    # up` will try to pull a private GHCR image with no credentials. Check only
    # the core profile's images (--minimal is what this rung brings up) and
    # only the curie-owned ones: postgres/valkey/rustfs are public and pulled
    # on demand same as rung_local already assumes.
    local missing=0 image
    while IFS= read -r image; do
        [[ "$image" == ghcr.io/curie-eng/curie-* ]] || continue
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            echo "error: image '$image' is required by compose.release.yaml's core profile and is not present locally." >&2
            missing=1
        fi
    done < <(docker compose -f "$release_compose" --profile core config --images)
    if (( missing )); then
        echo "fix: build and tag the missing image(s) locally under the tag compose.release.yaml pins (see .github/workflows/ci.yaml's e2e-ladder job for the exact build+tag steps CI uses), then re-run." >&2
        return 1
    fi

    assert_stub_port_free

    if [[ -n "$(docker ps -q --filter 'name=curie-api' 2>/dev/null)" ]]; then
        # Reuse it and do NOT tear it down, matching rung_local's rule: the
        # thread that brought a stack up owns tearing it down.
        echo "a compose stack is already running; reusing it and leaving teardown to whoever started it"
        if [[ "$LIVE" == "1" ]]; then
            echo "warning: CURIE_E2E_LIVE=1, but the reused stack's model mode was fixed by whoever ran \`local up\` and is NOT verified here." >&2
            echo "warning: if that stack was started with the fake model, this 'live' rung runs sealed; \`local down\` then re-run to be sure." >&2
        fi
    else
        echo
        echo "=== clear any stale volumes from a prior non-wiped teardown ==="
        # compose.dev.yaml and compose.release.yaml pin the SAME compose
        # project name (`curie`), so a prior `local down` (rung_local's, kept
        # deliberately non-destructive) can leave this rung's Postgres/Valkey
        # state non-empty. Nothing is running (checked above), so this can only
        # ever touch a stack this run itself would otherwise create -- never a
        # stack this run is about to reuse. Wiping first makes this rung an
        # actual cold start rather than one that might silently inherit state
        # and mask the exact compose-env-wiring drift (#545) it exists to catch.
        "$BIN" local down --wipe --yes -f "$release_compose" >/dev/null 2>&1 || true

        echo
        echo "=== curie local up --minimal -f compose.release.yaml ==="
        LOCAL_STACK_OWNED=1
        "$BIN" local up --minimal -f "$release_compose"
    fi

    echo
    echo "=== curie local deploy (release-compose stack) ==="
    # A separate bundle copy from rung_local's, never the same directory: deploy
    # records state into the bundle dir, and reusing rung_local's copy here
    # would carry over its recorded agent/version ids instead of a fresh
    # cold-start deploy.
    "$BIN" local deploy --plugin-dir "$WORKDIR/bundle-release"

    echo
    echo "=== curie local message --json (release-compose stack) ==="
    assert_stub_port_free
    local out
    out="$("$BIN" --json local message "$PROMPT" || true)"
    printf '%s\n' "$out"
    assert_finalized_reply "local-release" "$out"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval (release compose stack) ==="
        "$BIN" local eval --cases "$WORKDIR/bundle-release/evals/cases.json"
    fi

    if (( LOCAL_STACK_OWNED )); then
        echo
        echo "=== curie local down -f compose.release.yaml ==="
        "$BIN" local down -f "$release_compose"
        LOCAL_STACK_OWNED=0

        echo
        echo "=== assert nothing curie-related survived ==="
        local survivors
        survivors="$(docker ps --filter 'label=com.docker.compose.project=curie' --format '{{.Names}}')"
        if [[ -n "$survivors" ]]; then
            echo "local down left compose services running:" >&2
            printf '%s\n' "$survivors" >&2
            return 1
        fi
        survivors="$(docker ps --filter "label=$SANDBOX_LABEL" --format '{{.Names}}')"
        if [[ -n "$survivors" ]]; then
            echo "sibling sandbox containers survived teardown:" >&2
            printf '%s\n' "$survivors" >&2
            return 1
        fi
        echo "no curie containers running"
    fi
}

# Rung 3: the deployed release. Requires one to already exist; it is never
# installed or torn down here, because the cluster is shared.
rung_cluster() {
    echo
    echo "########## rung 3/3: cluster ##########"

    echo
    echo "=== curie cluster status (gate) ==="
    # Gate on the PAYLOAD, not the exit code: `cluster status` is a read-only
    # report verb and exits 0 even when the release is absent (it just prints
    # "release curie not found"), so an exit-code gate never fires and the
    # rung falls through into a confusing `cluster deploy` failure instead.
    # --json puts the object on stdout and human text on stderr.
    local status_json found
    status_json="$("$BIN" --json cluster status 2>/dev/null || true)"
    printf '%s\n' "$status_json"
    found="$(printf '%s' "$status_json" | python3 -c '
import json, sys
try:
    d = json.loads(sys.stdin.read())
except Exception:
    print("no")
    sys.exit(0)
print("yes" if isinstance(d, dict) and d.get("release_found") is True else "no")
' || echo "no")"
    if [[ "$found" != "yes" ]]; then
        echo "error: CURIE_E2E_TIERS named the cluster rung, but no installed release was reported by \`curie --json cluster status\`." >&2
        echo "fix: install a release with \`curie cluster up --fake-model\` (or point kubectl at the right context), or drop cluster from CURIE_E2E_TIERS." >&2
        return 1
    fi

    echo
    echo "=== curie cluster deploy ==="
    # No --api-url: deploy auto-discovers the release's UI /api proxy over
    # NodePort. No --secret: it is declined at this tier by design (#440).
    "$BIN" cluster deploy --plugin-dir "$WORKDIR/bundle"

    echo
    echo "=== curie cluster message --json ==="
    # No --thread: an existing thread keeps the sandbox and bundle it first
    # booted with, so reusing one could silently test a stale bundle. cluster
    # message manages its own port-forwards and reply stub; never forward by hand.
    #
    # CURIE_E2E_LISTEN_HOST (optional): the host the in-cluster worker uses to
    # reach this run's reply stub, forwarded verbatim as `cluster message
    # --listen-host`. Leave it UNSET for a cluster whose kubeconfig points at a
    # routable API server (k8scratch, a real cloud cluster): `cluster message`
    # then auto-detects the local IP the kernel would use to reach that API and
    # advertises it, and the worker posts its reply there. SET it only where that
    # auto-detection cannot produce a pod-reachable host -- most importantly a
    # kind/minikube cluster, whose API server is bound on loopback
    # (127.0.0.1:<port>), so the auto-detected host is 127.0.0.1, which an
    # in-cluster pod cannot route to. CI's kind job sets it to the kind Docker
    # network gateway (the host's address on that bridge, which every node
    # container can reach), so the pod->host reply leg -- the one reachability
    # surface this rung exists to gate -- resolves. It is the documented
    # `--listen-host` operator escape hatch, not a test-only shortcut: the exact
    # value any loopback-API-server cluster needs.
    local msg_args=(--json cluster message "$PROMPT")
    if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
        echo "using --listen-host ${CURIE_E2E_LISTEN_HOST} (worker->stub reply host)"
        msg_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
    fi
    local out
    out="$("$BIN" "${msg_args[@]}" || true)"
    printf '%s\n' "$out"
    assert_finalized_reply "cluster" "$out"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie cluster eval ==="
        # --json (unlike the other rungs) so a PASSING case's reply is logged too: #1602
        local eval_args=(--json cluster eval --cases "$WORKDIR/bundle/evals/cases.json")
        if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
            eval_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
        fi
        "$BIN" "${eval_args[@]}"
    fi
}

echo
echo "=== ladder configuration ==="
echo "tiers: $TIERS"
apply_model_mode

if [[ "$TIERS" == "all" ]]; then
    TIERS="skill,local,cluster"
fi
RUN_SKILL=0
RUN_LOCAL=0
RUN_LOCAL_RELEASE=0
RUN_CLUSTER=0
IFS=',' read -r -a SELECTED <<< "$TIERS"
for tier in "${SELECTED[@]}"; do
    case "$tier" in
        skill) RUN_SKILL=1 ;;
        local) RUN_LOCAL=1 ;;
        local-release) RUN_LOCAL_RELEASE=1 ;;
        cluster) RUN_CLUSTER=1 ;;
        "") ;;
        *)
            echo "error: unknown tier '$tier' in CURIE_E2E_TIERS." >&2
            echo "fix: use a comma list of skill, local, local-release, cluster, or the shorthand 'all' (skill, local, cluster)." >&2
            exit 1 ;;
    esac
done
if (( ! RUN_SKILL && ! RUN_LOCAL && ! RUN_LOCAL_RELEASE && ! RUN_CLUSTER )); then
    echo "error: CURIE_E2E_TIERS selected no rungs." >&2
    echo "fix: set it to a comma list of skill, local, local-release, cluster, or 'all'." >&2
    exit 1
fi

# Throwaway COPIES of the bundle: deploy records state into the bundle dir, and
# that must never land in the tree. Separate copies for rung_local and
# rung_local_release so neither carries the other's recorded deploy state.
cp -r "$BUNDLE_SRC" "$WORKDIR/bundle"
cp -r "$BUNDLE_SRC" "$WORKDIR/bundle-release"

# Rungs run strictly in order and never in parallel: they share host ports, and
# rung 1 must release its runner container before rung 2 starts.
if (( RUN_SKILL )); then
    rung_skill
else
    echo
    echo "SKIPPING rung 1 (skill): not named in CURIE_E2E_TIERS."
fi
if (( RUN_LOCAL )); then
    rung_local
else
    echo
    echo "SKIPPING rung 2 (local): not named in CURIE_E2E_TIERS."
fi
if (( RUN_LOCAL_RELEASE )); then
    rung_local_release
else
    echo
    echo "SKIPPING rung (local-release): not named in CURIE_E2E_TIERS. Needs the"
    echo "release-pinned images built and tagged locally first; name it explicitly,"
    echo "e.g. CURIE_E2E_TIERS=skill,local,local-release."
fi
if (( RUN_CLUSTER )); then
    rung_cluster
else
    echo
    echo "SKIPPING rung 3 (cluster): not named in CURIE_E2E_TIERS. It needs a live"
    echo "release and host-reachable pods, so it is opt-in: CURIE_E2E_TIERS=all."
fi

echo
echo "LADDER PASS (tiers: $TIERS)"
