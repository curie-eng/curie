#!/bin/bash
# Cold-start parity ladder for the curie CLI (issue #690).
#
# This is an E2E test, not a gate: it drives the SAME bundle through each
# deployment tier with the tier's own real verbs and asserts a turn actually
# finalized.
#
# "The same bundle" is PROVEN, not asserted in prose. It cannot be proven by
# packing once: no CLI surface accepts a pre-packed archive, so every tier packs
# for itself -- skill packs and hashes client-side (cli/src/bundle.rs), while
# local and cluster upload raw bytes the platform hashes server-side
# (apps/api/src/curie_api/deploy.py). So the ladder packs ONCE PER RUNG from one
# canonical source tree and asserts the independently computed sha256 values are
# equal. That is a superset of pack-once: it additionally proves the skill
# tier's client-side packer and the platform's server-side hasher agree on the
# same source. The same reasoning applies to the case set -- the digest covers
# `evals/cases.json`, so digest equality IS the case-id proof at the deployed
# tiers, while each tier's own suite loader independently reports the suite name
# and case count via `eval --dry-run`.
#
# Rung 1 (skill) is the existing `cli/scripts/e2e.sh`, invoked as is
# so the skill leg has exactly one implementation, and handed THIS run's bundle
# copy through CURIE_E2E_BUNDLE. Rung 2 (local) is
# `local up` -> `local deploy` -> `local message` -> `local down`,
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
# fake's canned reply manufactures a green. Rung 1 (skill) invokes its own
# tier's evaluator on whichever model it booted (cli/scripts/e2e.sh), but a FAKE
# skill run is not a graded run: the skill evaluator refuses to consult a grader
# on a fake turn (cli/src/evals.rs turn_outcome returns PlumbingOk), so every
# case reports plumbing_ok and no grader ever reads a reply. A fake skill rung
# therefore verifies PLUMBING and CASE IDENTITY only -- that the suite loaded,
# that its cases are the ones the ladder packed, and that each turn completed.
# GRADING, at every rung including skill, happens in LIVE mode only.
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
#   CURIE_E2E_CONNECTOR_BUNDLE
#                          opt-in: path to a bundle that declares connectors
#                            (examples/sre-bot) to drive every named rung
#                            through INSTEAD of the fixed weather bundle, so the
#                            rungs assert hosted-connector parity (ADR 0113,
#                            #1690). Unset by default: the default ladder's
#                            bundle stays fixed, so this is an added rung input
#                            rather than a change to the existing gate.
#   CURIE_E2E_CONNECTOR_REGISTRY
#                          registry ref (e.g. ghcr.io/acme-corp) for the
#                            connector build. Unset builds the host platform
#                            into the local Docker daemon, which the skill and
#                            local rungs accept and the CLUSTER rung refuses by
#                            design, so the cluster rung requires this.
#   CURIE_E2E_CONNECTOR_OMIT_SECRET
#                          name ONE of the fixture's three credentials to skip
#                            provisioning. The falsifiable negative: the rung
#                            must then fail closed on the missing credential
#                            rather than starting a connector without it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIERS="${CURIE_E2E_TIERS:-skill,local}"
LIVE="${CURIE_E2E_LIVE:-0}"
# Fixed, not an env knob: the ladder asserts PLUMBING, so the bundle it ships is
# a fixed input of the test rather than something a caller varies.
BUNDLE_SRC="$REPO_ROOT/examples/weather"

# --- the connector rung input (ADR 0113, #1690), opt-in and off by default ---
#
# Set CURIE_E2E_CONNECTOR_BUNDLE and every named rung runs a scratch copy of
# THAT bundle instead, with its connectors turned on by a checked-in fixture and
# its images built from source before the first rung. The default above is
# untouched, so this adds a rung input and changes no existing gate.
CONNECTOR_BUNDLE="${CURIE_E2E_CONNECTOR_BUNDLE:-}"
CONNECTOR_REGISTRY="${CURIE_E2E_CONNECTOR_REGISTRY:-}"
CONNECTOR_OMIT_SECRET="${CURIE_E2E_CONNECTOR_OMIT_SECRET:-}"
# A whole file, never a scripted uncomment: see the fixture's own header for why
# a `sed` against a comment block is the green-but-vacuous failure this rung
# exists to prevent.
CONNECTOR_FIXTURE="$REPO_ROOT/cli/scripts/fixtures/sre-bot-connectors-enabled.yaml"
# The connectors the fixture builds FROM SOURCE, and the tool set each must
# serve. These two are the subject of the assertion; the fixture's third
# connector is an ordinary `image:` one, hosted beside them as the control.
CONNECTOR_BUILT=(k8s-write tempo)
CONNECTOR_TOOLS_K8S_WRITE="restart_deployment"
CONNECTOR_TOOLS_TEMPO="get_trace,list_trace_tag_values,list_trace_tags,search_traces"
# The port every connector in the fixture serves on (`ConnectorSpec.port`'s
# default). Release, agent and namespace are deliberately NOT constants here:
# each rung reads them off the scope its own tier handed the runner, which is
# what makes the assertion a parity check rather than a restatement.
CONNECTOR_PORT=8000
# Pinned by the first rung to report an entry set, matched by every later one --
# the same shape as PARITY_DIGEST, and compared byte for byte.
CONNECTOR_ENTRIES=""
CONNECTOR_ENTRY_RUNGS=""
# `name=image` lines from the one `curie build --plugin-dir` receipt this run
# produced, so every rung is checked against what THIS run resolved.
CONNECTOR_IMAGES=""
# The scope the last rung's tier actually handed its runner, recorded by
# assert_connector_parity so the post-teardown sweep looks for those exact
# names.
CONNECTOR_SCOPE_RELEASE=""
CONNECTOR_SCOPE_AGENT=""
CONNECTOR_SCOPE_NAMESPACE=""
# The runner this script's own skill-tier connector case boots. Never the
# default name: that one belongs to whatever real `skill up` a developer has
# going on this box.
CONNECTOR_RUNNER_NAME="curie-ladder-connectors-$$"
# The runner the changed-source case boots, on its own scratch copy of the
# bundle. A second name for the same reason as the one above, and a second name
# rather than a reuse because both runners may be recorded at once if the first
# case fails before its teardown.
CHANGED_RUNNER_NAME="curie-ladder-changed-$$"
# The runner the hermetic negative boots, in the runs where no connector bundle
# is named at all. Same rule again: never the default name.
HERMETIC_RUNNER_NAME="curie-ladder-hermetic-$$"
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

# The component label every connector container carries, in both the skill start
# path and the local compose overlay (cli/src/docker.rs
# CONNECTOR_COMPONENT_LABEL). Teardown reaps by label, so this is the only
# handle that selects a connector container; its NAME differs between the two
# tiers (`curie-connector-<session>-<name>` vs compose's own).
CONNECTOR_LABEL="curietech.ai/component=connector"

if [[ -n "$CONNECTOR_BUNDLE" ]]; then
    if [[ ! -d "$CONNECTOR_BUNDLE" ]]; then
        echo "error: CURIE_E2E_CONNECTOR_BUNDLE is set to '$CONNECTOR_BUNDLE', which is not a directory." >&2
        exit 1
    fi
    # Absolutized once: every later use is from another directory.
    CONNECTOR_BUNDLE="$(cd "$CONNECTOR_BUNDLE" && pwd)"
    BUNDLE_SRC="$CONNECTOR_BUNDLE"
    # The connector bundle is not a weather bot, and a prompt it cannot answer
    # would make a live rung's grade meaningless. Plumbing is still all a fake
    # rung asserts.
    PROMPT="Is any pod crashlooping right now?"
fi

# Set once the ladder itself brought the compose stack up. The thread that
# brought a stack up owns tearing it down, so a stack that was already running
# when the ladder started is reused and left alone.
LOCAL_STACK_OWNED=0

# The local observability proof owns one uniquely named Collector sink. It is
# deliberately separate from the product Collector: querying the product's own
# exporter would only prove configuration, while this receiver proves bytes
# crossed the API -> worker -> sandbox boundary. The stubbed CLI contract tests
# set STUB_STATE; they exercise ladder decisions without a Docker daemon and do
# not pretend to be this runtime proof.
LOCAL_OTEL_SINK_NAME="curie-ladder-otel-sink-$$"
LOCAL_OTEL_SINK_OWNED=0
LOCAL_OTEL_NETWORK_OWNED=0
LOCAL_OTEL_SINK_ACTIVE=0
LOCAL_OTEL_ENDPOINT=""
LOCAL_OTEL_METRICS_ENDPOINT=""
LOCAL_OTEL_FAILURE_MODE=0
OTEL_E2E_SECRET_SENTINEL="xapp-"
OTEL_E2E_SECRET_SENTINEL+="0-0000000000-0000000000-$$"

# Cross-rung artifact identity, pinned by the first rung to report a digest and
# matched by every later one (see assert_bundle_identity). Empty until then.
PARITY_DIGEST=""
# What the run summary is allowed to claim: the rungs that ran, and the rungs
# whose identity / suite / model mode were each actually asserted. Accumulated
# by the assertions themselves, so the summary can never over-report.
RAN_RUNGS=""
PARITY_RUNGS=""
SUITE_RUNGS=""
MODE_RUNGS=""
# Whether the cluster rung proved its turn could only bind to the deployment it
# just created. Set only by the full active-set assertion, which is conditional
# at that tier (see rung_cluster), so the summary reports upload identity and
# runtime binding as the two separate claims they are.
CLUSTER_BINDING_PROVEN=0

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
    stop_local_otel_sink
    # Only the container THIS run created, matched by its exact unique name, so
    # the sweep can never reach a runner belonging to another session. Cleared by
    # the case itself once `skill down` has removed it.
    if (( CONFLICT_CREATED )); then
        docker rm -f "$CONFLICT_NAME" >/dev/null 2>&1
    fi
    # Connector containers this run's own bundle started, matched by the exact
    # aliases derived from the scope a rung recorded -- never a bare sweep of
    # the connector label, which is host-wide and would reach a concurrent
    # session's connectors. A normal path has already reaped these through the
    # tier's own `down`; this covers the run that died before reaching it.
    if [[ -n "$CONNECTOR_SCOPE_RELEASE" ]]; then
        local connector object survivor
        for connector in kubernetes "${CONNECTOR_BUILT[@]}"; do
            object="$(connector_object_name "$CONNECTOR_SCOPE_RELEASE" "$CONNECTOR_SCOPE_AGENT" "$connector" 2>/dev/null)" || continue
            survivor="$(connector_container_for_alias "$object.$CONNECTOR_SCOPE_NAMESPACE.svc.cluster.local")" || continue
            echo "sweeping stranded connector container $survivor"
            docker rm -f "$survivor" >/dev/null 2>&1
        done
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
    # Was a disclaimer that the cluster rung's mode is unverified. It is now
    # verified: every deployed rung reads CURIE_FAKE_MODEL off the artifact that
    # is actually running and fails on a contradiction (assert_model_mode).
    echo "note: each deployed rung's effective mode is VERIFIED against this choice by reading CURIE_FAKE_MODEL off the running worker; a contradiction fails that rung."
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

# Cross-rung artifact identity. The digest identifies the ARCHIVE a tier
# actually shipped, and the two sides compute it independently: skill hashes the
# bytes it packed client-side (cli/src/bundle.rs), local and cluster get back the
# platform's server-side hash of the bytes they uploaded
# (apps/api/src/curie_api/deploy.py). The first rung to report pins the value;
# every later rung must match it, so equality proves both hashers agree on one
# source tree -- and, because `evals/cases.json` is inside that archive, that
# every rung graded the same case ids.
#
# Deliberately called at the END of a rung rather than at its deploy step: the
# rung's suite and mode evidence must reach the transcript BEFORE a cross-rung
# divergence stops the run, because "the tier's own plan line matched and only
# the digest moved" is exactly what distinguishes a case-ids-only divergence
# from a suite divergence. Aborting at the deploy step would throw away the
# evidence that makes the failure diagnosable.
assert_bundle_identity() {
    local label="$1" digest="$2"
    if [[ -z "$digest" || "$digest" == "null" || "$digest" == "None" ]]; then
        echo "$label: no bundle digest was reported, so this rung's artifact identity is unknown and cannot be compared." >&2
        return 1
    fi
    if [[ -z "$PARITY_DIGEST" ]]; then
        PARITY_DIGEST="$digest"
        PARITY_RUNGS="$PARITY_RUNGS $label"
        echo "$label: bundle identity pinned for this ladder run: $digest"
        return 0
    fi
    if [[ "$digest" != "$PARITY_DIGEST" ]]; then
        echo "$label: bundle identity DIVERGED -- this ladder pinned $PARITY_DIGEST, but the $label rung shipped $digest." >&2
        echo "fix: the rungs are not running the same artifact. Check that nothing mutated the bundle copies between rungs, and that every copy's regular-file mtimes were normalized (pack_tar_gz embeds per-file mtime, so equal content is not enough)." >&2
        return 1
    fi
    PARITY_RUNGS="$PARITY_RUNGS $label"
    echo "$label: bundle identity matches the pinned digest: $digest"
}

# The suite the TIER's own frozen loader resolved, read off `eval --dry-run`.
# Runs in fake mode as well as live: a dry run sends no turn, grades nothing and
# needs no reachable stack (it returns before connecting), yet it still loads and
# validates the cases file. The plan line carries a suite NAME and a case COUNT
# and no ids at all, so this is a cross-check that the ladder handed the tier the
# file it thinks it did -- never a case-id readback. The case-id claim at these
# tiers rests on digest equality (assert_bundle_identity).
assert_suite() {
    local label="$1" payload="$2" line grade_line="" count="" suite=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^grade\ ([0-9]+)\ case\(s\)\ from\ suite\ \"(.*)\"\ against\ the\ .+\ tier$ ]]; then
            grade_line="$line"
            count="${BASH_REMATCH[1]}"
            suite="${BASH_REMATCH[2]}"
        fi
    done < <(printf '%s' "$payload" | python3 -c '
import json, sys
try:
    plan = json.loads(sys.stdin.read()).get("plan") or []
except Exception:
    plan = []
for entry in plan:
    print(entry)
' || true)
    if [[ -z "$grade_line" ]]; then
        # Print the raw payload, not just a verdict: a corrupt .curie in the
        # invoking cwd can red this check for reasons that have nothing to do
        # with parity, and the raw output is what tells the two apart.
        echo "$label: the tier's \`eval --dry-run\` plan carried no \"grade N case(s) from suite ... tier\" line, so the suite it resolved cannot be read." >&2
        printf '%s\n' "$payload" >&2
        return 1
    fi
    # Re-match to repopulate BASH_REMATCH: the loop above may have run further
    # iterations since the match that set grade_line.
    [[ "$grade_line" =~ ^grade\ ([0-9]+)\ case\(s\)\ from\ suite\ \"(.*)\"\ against\ the\ .+\ tier$ ]]
    local count="${BASH_REMATCH[1]}" suite="${BASH_REMATCH[2]}"
    if [[ "$suite" != "$EXPECT_SUITE" || "$count" != "$EXPECT_CASE_COUNT" ]]; then
        echo "$label: the tier resolved a different suite than the ladder packed. Expected suite \"$EXPECT_SUITE\" with $EXPECT_CASE_COUNT case(s); the tier's own loader reported: $grade_line" >&2
        return 1
    fi
    SUITE_RUNGS="$SUITE_RUNGS $label"
    echo "$label: suite parity asserted (name and count only, no ids) -- the tier's own loader reported: $grade_line"
}

# The effective model mode of the DEPLOYED artifact, compared against what this
# run asked for. Truthiness rule mirrored from cli/src/local.rs
# fake_model_is_truthy; an ABSENT value means live, because compose only
# materializes a value through ${CURIE_FAKE_MODEL:-1} and the chart only sets it
# on a sealed install.
#
# Bounded to FRESH claims, deliberately: mode reaches a run through the binding
# (apps/worker/src/curie_worker/binding.py fake_model=...), read per claim, so
# this describes the mode the NEXT turn gets. It says nothing about a sandbox
# that was already running when the ladder started. Each rung deploys and then
# messages, so that is exactly the turn it cares about -- but do not later read
# this assertion as a claim about pre-existing sandboxes.
assert_model_mode() {
    local label="$1" observed="$2" truthy=0
    case "${observed,,}" in
        1|true|yes) truthy=1 ;;
    esac
    if [[ "$LIVE" == "1" ]]; then
        if (( truthy )); then
            echo "$label: this run asked for the LIVE model, but the deployed worker carries CURIE_FAKE_MODEL=$observed, so the rung would run sealed against the fake model and any grade it reported would be manufactured." >&2
            echo "fix: for a compose rung run \`curie local down\` and re-run so the stack boots live; for the cluster rung reinstall the release without \`cluster up --fake-model\`." >&2
            return 1
        fi
        MODE_RUNGS="$MODE_RUNGS $label"
        echo "$label: deployed worker carries no truthy CURIE_FAKE_MODEL (observed: '$observed'), so the rung is live as asked"
        return 0
    fi
    if (( ! truthy )); then
        echo "$label: this run is sealed (fake model), but the deployed worker's CURIE_FAKE_MODEL is '$observed', which is not truthy, so the rung would call a real model." >&2
        echo "fix: bring the deployment up sealed, or set CURIE_E2E_LIVE=1 to run the ladder live deliberately." >&2
        return 1
    fi
    MODE_RUNGS="$MODE_RUNGS $label"
    echo "$label: deployed worker carries CURIE_FAKE_MODEL=$observed (sealed, as asked)"
}

# A deploy receipt proves what was UPLOADED. It does NOT prove the following
# `message` and `eval` turns ran that deployment: `deploy` defaults the
# environment to dev (cli/src/commands.rs) while the worker's runtime binding
# prefers prod over recency (apps/worker/src/curie_worker/binding.py's
# `ORDER BY (d.environment = 'prod') DESC, d.deployed_at DESC`) over the ACTIVE
# set only. So one stale active prod row for this agent serves the turn while the
# ladder reports the digest of the dev bundle it just uploaded: a green ladder
# proving the wrong artifact. Not hypothetical here -- `local down` is
# deliberately non-destructive and the compose project name is pinned to
# `curie`, so Postgres volumes and their deployment rows outlive every run and
# are shared across worktrees.
#
# The invariant asserted, chosen over re-implementing that ORDER BY in shell:
# exactly ONE active deployment exists for this agent, and it is this run's.
# Why that is sufficient, each link checkable:
#  1. A channel maps to exactly one agent, enforced in the database --
#     apps/api/src/curie_api/models.py:44
#     `slack_channel: Mapped[str] = mapped_column(unique=True)`, added by #38
#     because without it the create succeeded and the second agent was silently
#     shadowed by the worker's resolver at runtime.
#  2. That agent has exactly one active deployment, which is what this
#     assertion counts.
#  3. The resolver selects over the ACTIVE set only --
#     apps/worker/src/curie_worker/binding.py:172
#     `JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'`,
#     the same predicate counted here -- so no other status value can serve a
#     row this assertion never saw.
# Therefore the row the worker resolves for this channel can only be this run's,
# whatever ordering it applies.
#
# Residual, named and deliberately not engineered around: the read happens before
# the turn, so a deployment created in that window is not caught. That needs a
# concurrent deployer against the same stack, which is why the ladder owns its
# stack rather than fighting a foreign one.
assert_sole_active_deployment() {
    local label="$1" agent_id="$2" deployment_id="$3" api_base verdict listed
    # The CLI's own public inputs, with the CLI's own defaults (cli/src/main.rs's
    # --api-url/--api-key clap declarations, defaulting to the crate constants
    # DEFAULT_LOCAL_API_URL and DEFAULT_API_KEY in cli/src/message.rs). Reading
    # the same two variables means this queries whatever API the deploy above
    # actually went to; it is not a ladder-local or test-only knob.
    api_base="${CURIE_API_URL:-http://localhost:28000}"
    # python3 + urllib, already a hard dependency of this script, so the read
    # adds no new binary requirement.
    verdict="$(python3 -c '
import json, sys, urllib.request
base, key, agent_id, deployment_id = sys.argv[1:5]
url = base.rstrip("/") + "/deployments?agent_id=" + agent_id
request = urllib.request.Request(url, headers={"X-API-Key": key})
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        rows = json.load(response)
except Exception as exc:
    print("unreachable")
    print("%s: %s" % (type(exc).__name__, exc))
    sys.exit(0)
if not isinstance(rows, list):
    print("unparseable")
    print(repr(rows)[:400])
    sys.exit(0)
# Validate EVERY row before filtering, and fail on any that is not a
# DeploymentOut (apps/api/src/curie_api/schemas.py). Skipping malformed rows
# inside the filter instead would be fail-open on an unexpected shape: a
# response carrying the expected active row PLUS a null or a truncated object
# would be silently reduced to the one good row and pass as proof that exactly
# one active deployment exists. An unexpected shape must surface as itself.
required = ("id", "agent_id", "version_id", "environment", "status")
malformed = [
    r for r in rows
    if not isinstance(r, dict) or any(field not in r for field in required)
]
if malformed:
    print("malformed_row")
    print(repr(malformed[:3])[:400])
    sys.exit(0)
active = [r for r in rows if r.get("status") == "active"]
listed = ", ".join(
    "%s (environment %s)" % (r.get("id"), r.get("environment")) for r in active
) or "<none>"
print("ok" if len(active) == 1 and active[0].get("id") == deployment_id else "shadowed")
print(listed)
' "$api_base" "${CURIE_API_KEY:-curie-dev-key}" "$agent_id" "$deployment_id" || echo "probe_failed")"
    # Two-line protocol, split on the FIRST newline; `listed` first, because the
    # second expansion overwrites the string both read.
    listed="${verdict#*$'\n'}"
    verdict="${verdict%%$'\n'*}"

    case "$verdict" in
        ok)
            echo "$label: exactly one active deployment for this agent, and it is the one this rung just created ($deployment_id), so the turn below can only bind to it"
            ;;
        malformed_row)
            echo "$label: the deployments read from $api_base/deployments?agent_id=$agent_id returned a row that is not a deployment object carrying id, agent_id, version_id, environment and status: $listed" >&2
            echo "why: a malformed row is not filtered away here, deliberately. Discarding it would be fail-open -- the expected active row plus a null would then read as 'exactly one active deployment', certifying a binding this ladder never actually checked." >&2
            echo "fix: this is an API or proxy problem, not a stale-state one; check what is answering GET /deployments at $api_base before trusting any rung's identity claim." >&2
            return 1
            ;;
        shadowed)
            echo "$label: the turn is NOT guaranteed to run the deployment this rung just created ($deployment_id). Active deployments for agent $agent_id: $listed" >&2
            echo "why: the worker's runtime binding prefers an active prod deployment over recency, so a stale row can serve the turn while this rung reports the digest it just uploaded -- a green ladder proving the wrong artifact." >&2
            echo "fix: curie local down --wipe --yes, then re-run the ladder." >&2
            return 1
            ;;
        *)
            echo "$label: could not read the active deployment set from $api_base/deployments?agent_id=$agent_id ($verdict): $listed" >&2
            return 1
            ;;
    esac
}

# The two mode probes below are the SECOND deliberate use of this script's
# documented raw-tool exception (see the header): no `curie` verb reports the
# effective model mode of a deployed tier -- neither local-status.schema.json nor
# cluster-status.schema.json carries a model field, and `local rebuild --json`'s
# model_mode is a re-derivation from the invoking shell, not a read of the
# running stack. So the ladder mirrors the CLI's own internal probe
# (cli/src/message.rs probe_fake_model), which is deliberately a read of the
# OUTPUT rather than a re-derivation of the input. A CLI verb that surfaced this
# value is the named follow-up; adding one here is forbidden by scope.
#
# The project label on the `docker ps` below is deliberate EXTRA scoping, NOT a
# mirror: the CLI selects on the service label alone, which is host-wide and
# would match a concurrent worktree's worker. The compose project name is pinned
# to `curie` by the CLI (cli/src/local.rs COMPOSE_PROJECT_NAME), so adding it
# selects this ladder's stack and nothing else. Do not "fix" it back to an exact
# mirror of the CLI's selector.
probe_local_fake_model() {
    local line value="" workers=()
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            workers+=("$line")
        fi
    done < <(docker ps --filter 'label=com.docker.compose.project=curie' --filter 'label=com.docker.compose.service=curie-worker' --format '{{.Names}}')
    # Exactly one, never "inspect the first": two matches mean the scoping
    # assumption broke and the probe's answer would be meaningless.
    if (( ${#workers[@]} != 1 )); then
        echo "local mode probe: expected exactly one running container matching label=com.docker.compose.project=curie plus label=com.docker.compose.service=curie-worker, found ${#workers[@]} (${workers[*]:-none})." >&2
        return 1
    fi
    # Captured into a variable and status-checked, never read through a process
    # substitution: a process substitution discards `docker inspect`'s exit
    # status, so a container that died since the `docker ps` above, or a daemon
    # blip, would read as zero env lines and return an empty string with exit 0.
    # An absent CURIE_FAKE_MODEL is legitimately live mode, and assert_model_mode
    # accepts an empty value as live under CURIE_E2E_LIVE=1 -- so an unread probe
    # would PASS the live-mode assertion having verified nothing at all.
    local env_dump
    if ! env_dump="$(docker inspect "${workers[0]}" --format '{{range .Config.Env}}{{println .}}{{end}}')"; then
        echo "local mode probe: \`docker inspect ${workers[0]}\` failed, so the deployed worker's effective model mode is unknown. Refusing to treat an unread probe as live." >&2
        return 1
    fi
    while IFS= read -r line; do
        if [[ "$line" == CURIE_FAKE_MODEL=* ]]; then
            value="${line#CURIE_FAKE_MODEL=}"
        fi
    done <<< "$env_dump"
    printf '%s' "$value"
}

probe_cluster_fake_model() {
    # Release and namespace both default to `curie` (cli/src/main.rs), which is
    # what this rung runs against; the deployment name is built the same way the
    # CLI builds it, as `deployment/<release>-worker`.
    kubectl -n curie get deployment/curie-worker \
        -o 'jsonpath={.spec.template.spec.containers[*].env[?(@.name=="CURIE_FAKE_MODEL")].value}'
}

# One field out of a `deploy --json` receipt. deploy.schema.json is a oneOf over
# deploy_result, aggregate_success, deploy_failure and connector_failure; the
# ladder never passes --all-targets, so it gets deploy_result. The extractor
# still fails with a named error and prints the payload rather than emitting an
# empty string, so an unexpected shape surfaces as itself instead of pinning an
# empty digest and making every later comparison vacuous.
deploy_field() {
    local label="$1" payload="$2" path="$3" value
    value="$(printf '%s' "$payload" | python3 -c '
import json, sys
value = json.loads(sys.stdin.read())
for key in sys.argv[1].split("."):
    value = value[key]
print(value)
' "$path")" || {
        echo "$label: the deploy --json receipt carried no $path; deploy.schema.json requires it on the deploy_result branch, so this is an unexpected payload shape." >&2
        printf '%s\n' "$payload" >&2
        return 1
    }
    printf '%s' "$value"
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

# ---------------------------------------------------------------------------
# The connector rung (ADR 0113, #1690). Everything below is inert unless
# CURIE_E2E_CONNECTOR_BUNDLE is set.
# ---------------------------------------------------------------------------

connector_mode() {
    [[ -n "$CONNECTOR_BUNDLE" ]]
}

# Two distinct, well-formed, scratch-scoped kubeconfigs plus the tempo token,
# provisioned into THIS RUN's process environment and the scratch bundle copies
# only. Nothing of the operator's is read, written or restored: the CLI resolves
# a connector credential from the environment first and the host vault second
# (cli/src/commands.rs resolve_connector_secret), so exporting is sufficient and
# `curie secrets set` -- which writes the operator's real store -- is never run.
#
# The kubeconfigs need no cluster. These rungs assert HOSTING, not live
# Kubernetes access: the write connector refuses to start without a kubeconfig
# file and starts with a well-formed one, which is exactly the fail-closed
# behavior under test.
provision_connector_credentials() {
    local creds="$WORKDIR/connector-creds"
    mkdir -p "$creds"
    chmod 700 "$creds"

    local name
    for name in K8S_READONLY_KUBECONFIG K8S_WRITE_KUBECONFIG GRAFANA_SERVICE_ACCOUNT_TOKEN; do
        if [[ "$CONNECTOR_OMIT_SECRET" == "$name" ]]; then
            echo "connector credentials: SKIPPING $name deliberately (CURIE_E2E_CONNECTOR_OMIT_SECRET)."
            echo "connector credentials: the rung below MUST now fail closed on the missing credential. A rung that starts a connector anyway is the failure this run is looking for."
            continue
        fi
        case "$name" in
            K8S_READONLY_KUBECONFIG|K8S_WRITE_KUBECONFIG)
                # A SEPARATE credential per connector, never one reused: that is
                # the example's own rule (examples/sre-bot/connectors.yaml), and
                # reusing one here would quietly assert the opposite shape.
                local user="ladder-reader" file="$creds/readonly.kubeconfig"
                if [[ "$name" == "K8S_WRITE_KUBECONFIG" ]]; then
                    user="ladder-writer"
                    file="$creds/writer.kubeconfig"
                fi
                cat > "$file" <<YAML
apiVersion: v1
kind: Config
clusters: [{name: ladder, cluster: {server: https://kubernetes.default.svc}}]
users: [{name: $user, user: {token: not-a-real-token-$user}}]
contexts: [{name: ladder, context: {cluster: ladder, user: $user}}]
current-context: ladder
YAML
                chmod 600 "$file"
                export "$name"="$(cat "$file")"
                ;;
            GRAFANA_SERVICE_ACCOUNT_TOKEN)
                # A placeholder, and sufficient: the tempo connector refuses to
                # start without one, and the tool call this rung makes is an
                # input-validation path that never reaches Grafana.
                export GRAFANA_SERVICE_ACCOUNT_TOKEN="not-a-real-token-ladder"
                ;;
        esac
        echo "connector credentials: $name provisioned into this run's environment only"
    done
}

# Copy the connector fixture into each scratch bundle. The shipped manifest
# already carries the approval gate for the declared write connector.
prepare_connector_bundle() {
    local dir="$1"
    cp "$CONNECTOR_FIXTURE" "$dir/connectors.yaml"
    echo "connector fixture applied to $dir (connectors.yaml)"
}

# One build before the first rung, so every rung consumes the same lock and the
# bundle bytes each rung packs are identical (the PARITY_DIGEST assertion).
#
# Without --registry this builds the host platform into the local Docker daemon
# and records the local image id, which the skill and local rungs accept and the
# cluster rung refuses by design -- so the cluster rung requires a registry.
build_connector_images() {
    local dir="$1"
    local out
    local build_args=(--json build --plugin-dir "$dir")
    if [[ -n "$CONNECTOR_REGISTRY" ]]; then
        build_args+=(--registry "$CONNECTOR_REGISTRY")
        echo "=== curie build --plugin-dir (registry delivery: $CONNECTOR_REGISTRY) ==="
    else
        echo "=== curie build --plugin-dir (local-daemon delivery) ==="
    fi
    out="$("$BIN" "${build_args[@]}")"
    printf '%s\n' "$out"
    # name=image lines, read back from the receipt rather than by parsing the
    # lock file: the receipt is the CLI's own agent-facing contract, and it says
    # what this run actually resolved.
    CONNECTOR_IMAGES="$(printf '%s' "$out" | python3 -c '
import json, sys
payload = json.loads(sys.stdin.read())
records = payload["connectors"]
if not records:
    sys.exit("the build receipt lists no connectors, so the fixture declared nothing to build")
for record in records:
    print("%s=%s" % (record["name"], record["image"]))
')"
    printf '%s\n' "$CONNECTOR_IMAGES"
}

# The image `curie build` resolved for one connector.
connector_image() {
    local want="$1" line
    while IFS= read -r line; do
        if [[ "$line" == "$want="* ]]; then
            printf '%s' "${line#*=}"
            return 0
        fi
    done <<< "$CONNECTOR_IMAGES"
    echo "no build receipt entry for connector '$want'" >&2
    return 1
}

# One field of one connector's `connectors.lock.yaml` entry.
#
# An awk read of the two-space block serde_norway writes, not a YAML parse: the
# ladder's python3 use is json-only on purpose and a YAML module would be a new
# required input. It FAILS LOUDLY on a miss and dumps the file, rather than
# returning an empty string -- an empty value compared against another empty
# value is the vacuous green this ladder exists to prevent.
lock_field() {
    local dir="$1" connector="$2" key="$3" value
    value="$(awk -v want="  $connector:" -v key="    $key: " '
$0 == want { inside = 1; next }
inside && /^  [^ ]/ { inside = 0 }
inside && index($0, key) == 1 { print substr($0, length(key) + 1); found = 1; exit }
END { exit(found ? 0 : 1) }
' "$dir/connectors.lock.yaml")" || {
        echo "no '$key' recorded for connector '$connector' in $dir/connectors.lock.yaml. That file's shape is what this read depends on; compare it against ConnectorLockEntryDecl in cli/src/connector_build.rs." >&2
        cat "$dir/connectors.lock.yaml" >&2 || true
        return 1
    }
    # Quoted only if the value needed it; the comparisons downstream are on the
    # value, never on its rendering.
    value="${value%\"}"; value="${value#\"}"
    value="${value%\'}"; value="${value#\'}"
    printf '%s' "$value"
}

# A hand mirror of connector_render.object_name / service_dns, which is what
# BOTH sides derive independently: the CLI names the container's network alias
# from it, and the runner derives the URL it dials from it. The ladder recomputes
# it from the scope the RUNNER was actually given, so a tier whose two sides
# disagree fails here instead of surfacing as a connection timeout mid-turn.
connector_object_name() {
    local release="$1" agent="$2" connector="$3"
    local name="$release-$agent-mcp-$connector"
    if (( ${#name} > 63 )); then
        echo "connector object name '$name' exceeds 63 characters, so the CLI truncates it with a digest and this ladder's hand mirror no longer matches. Shorten the fixture's agent or connector name." >&2
        return 1
    fi
    printf '%s' "$name"
}

# The one compose worker this ladder's stack is running, selected exactly the
# way probe_local_fake_model selects it (project label plus service label), so
# the connector scope is read off the same container whose model mode is read.
local_worker_container() {
    local line workers=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && workers+=("$line")
    done < <(docker ps --filter 'label=com.docker.compose.project=curie' --filter 'label=com.docker.compose.service=curie-worker' --format '{{.Names}}')
    if (( ${#workers[@]} != 1 )); then
        echo "connector scope probe: expected exactly one running curie-worker in the compose project 'curie', found ${#workers[@]} (${workers[*]:-none})." >&2
        return 1
    fi
    printf '%s' "${workers[0]}"
}

# One env var read off a running container's inspected environment.
container_env_value() {
    local container="$1" key="$2" dump line
    if ! dump="$(docker inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}')"; then
        echo "could not inspect '$container' to read $key" >&2
        return 1
    fi
    while IFS= read -r line; do
        if [[ "$line" == "$key="* ]]; then
            printf '%s' "${line#*=}"
            return 0
        fi
    done <<< "$dump"
    printf ''
}

# The connector container answering to one alias, by LABEL then by alias --
# never by name, which differs between the skill start path
# (`curie-connector-<session>-<name>`) and the local compose overlay's own.
connector_container_for_alias() {
    local alias="$1" container aliases
    while IFS= read -r container; do
        [[ -n "$container" ]] || continue
        aliases="$(docker inspect "$container" --format '{{range .NetworkSettings.Networks}}{{range .Aliases}}{{println .}}{{end}}{{end}}' 2>/dev/null || true)"
        if grep -qxF "$alias" <<< "$aliases"; then
            printf '%s' "$container"
            return 0
        fi
    done < <(docker ps --filter "label=$CONNECTOR_LABEL" --format '{{.Names}}')
    return 1
}

# The MCP probe itself: an initialize / notifications/initialized / tools/list
# round trip, and optionally one deterministic tool call, against a connector's
# real URL. Written once into $WORKDIR and piped to `python -` inside a
# connector container, so it needs no image the ladder does not already run and
# no host port -- connectors deliberately publish none.
write_connector_probe() {
    cat > "$WORKDIR/mcp_probe.py" <<'PY'
"""Handshake with one connector over streamable HTTP and report its tools.

argv: <url> <expected-tools-csv> [<tool-to-call>]

Runs INSIDE a connector container, dialing another connector by the network
alias Curie assigned it, so a pass covers three things at once: the alias
resolves, the server is serving MCP, and its tool surface is the expected one.
"""

import json
import sys
import urllib.request

url, expected_csv = sys.argv[1], sys.argv[2]
call_tool = sys.argv[3] if len(sys.argv) > 3 else ""
expected = sorted(name for name in expected_csv.split(",") if name)

state = {"session": None, "version": "2024-11-05"}


def post(body, notification=False):
    headers = {
        "Content-Type": "application/json",
        # Both, because a streamable-HTTP server may answer either shape.
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": state["version"],
    }
    if state["session"]:
        headers["mcp-session-id"] = state["session"]
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        session = response.headers.get("mcp-session-id")
        if session:
            state["session"] = session
        raw = response.read().decode("utf-8", "replace")
    if notification:
        return None
    for line in raw.splitlines():
        if line.startswith("data:"):
            return json.loads(line[5:].strip())
    return json.loads(raw)


def fail(message):
    sys.stderr.write("%s: %s\n" % (url, message))
    raise SystemExit(1)


try:
    handshake = post(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": state["version"],
                "capabilities": {},
                "clientInfo": {"name": "curie-e2e-ladder", "version": "0"},
            },
        }
    )
except Exception as exc:
    fail("the MCP handshake did not complete: %s: %s" % (type(exc).__name__, exc))

if not isinstance(handshake, dict) or "result" not in handshake:
    fail("initialize returned no result: %r" % (handshake,))
state["version"] = handshake["result"].get("protocolVersion", state["version"])

post({"jsonrpc": "2.0", "method": "notifications/initialized"}, notification=True)

listed = post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
if not isinstance(listed, dict) or "result" not in listed:
    fail("tools/list returned no result: %r" % (listed,))
tools = sorted(tool["name"] for tool in listed["result"]["tools"])
if expected and tools != expected:
    fail("tools/list returned %s; expected %s" % (",".join(tools), ",".join(expected)))

if call_tool:
    # Deterministic and ungated by construction: an empty argument takes the
    # server's own input-validation path, so it needs no live backend and
    # cannot depend on data that changes between rungs.
    called = post(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": call_tool, "arguments": {"trace_id": ""}},
        }
    )
    body = json.dumps(called)
    if "trace_id is required" not in body:
        fail("%s did not take its input-validation path: %s" % (call_tool, body[:400]))

print("OK %s" % ",".join(tools))
PY
}

# Cross-rung entry parity. The entry the runner mounts is
# `http://<release>-<agent>-mcp-<connector>.<namespace>.svc.cluster.local:<port>/mcp`,
# and the NAMESPACE is the one component that legitimately differs between tiers
# -- it is the namespace the connector is deployed into. So the pinned, byte-
# compared value is everything else: the object name, the port and the path,
# which is the part both sides derive independently and the part a release or
# agent drift would move. Each rung's full URL is printed beside it.
assert_connector_entries() {
    local label="$1" entries="$2"
    if [[ -z "$CONNECTOR_ENTRIES" ]]; then
        CONNECTOR_ENTRIES="$entries"
        CONNECTOR_ENTRY_RUNGS="$CONNECTOR_ENTRY_RUNGS $label"
        echo "$label: connector MCP entries pinned for this ladder run:"
        printf '%s\n' "$entries"
        return 0
    fi
    if [[ "$entries" != "$CONNECTOR_ENTRIES" ]]; then
        echo "$label: connector MCP entries DIVERGED from the pinned set." >&2
        echo "pinned:" >&2
        printf '%s\n' "$CONNECTOR_ENTRIES" >&2
        echo "$label:" >&2
        printf '%s\n' "$entries" >&2
        echo "fix: the tiers do not agree on the address a connector answers to, so a turn on one of them dials a name nothing owns. Compare the connector scope each tier hands the runner." >&2
        return 1
    fi
    CONNECTOR_ENTRY_RUNGS="$CONNECTOR_ENTRY_RUNGS $label"
    echo "$label: connector MCP entries match the pinned set"
}

# The whole per-rung connector assertion.
#
# `kind` is docker (skill, local, local-release) or kubectl (cluster), and the
# release/agent/namespace are read from the scope the TIER handed the runner --
# never hardcoded here. That is what makes this a parity assertion rather than a
# restatement: the ladder recomputes the address from the runner's own inputs
# and then requires a connector to be answering on it.
assert_connector_parity() {
    local label="$1" kind="$2" release="$3" agent="$4" namespace="$5"
    local connector object alias url entries="" host_ref="" image expected probe_pod=""

    if [[ -z "$release" || -z "$agent" || -z "$namespace" ]]; then
        echo "$label: the tier reported an incomplete connector scope (release='$release' agent='$agent' namespace='$namespace'), so no connector address can be derived. A partial scope switches connectors off entirely." >&2
        return 1
    fi
    echo "$label: connector scope as the tier gave it to the runner: release=$release agent=$agent namespace=$namespace"
    # Recorded so the post-teardown sweep looks for the same names this rung
    # just proved were up, rather than re-deriving them once the tier that
    # reported them is gone.
    CONNECTOR_SCOPE_RELEASE="$release"
    CONNECTOR_SCOPE_AGENT="$agent"
    CONNECTOR_SCOPE_NAMESPACE="$namespace"

    for connector in kubernetes "${CONNECTOR_BUILT[@]}"; do
        object="$(connector_object_name "$release" "$agent" "$connector")" || return 1
        alias="$object.$namespace.svc.cluster.local"
        url="http://$alias:$CONNECTOR_PORT/mcp"
        entries="$entries$connector=$object:$CONNECTOR_PORT/mcp"$'\n'

        if [[ "$kind" == "docker" ]]; then
            local container
            if ! container="$(connector_container_for_alias "$alias")"; then
                echo "$label: no running container labeled $CONNECTOR_LABEL answers to the alias '$alias', which is the name the runner derives and dials." >&2
                docker ps --filter "label=$CONNECTOR_LABEL" --format '{{.Names}} {{.Image}}' >&2 || true
                return 1
            fi
            image="$(docker inspect "$container" --format '{{.Config.Image}}')"
            echo "$label: $connector is up as $container on $url"
            [[ "$connector" == "tempo" ]] && host_ref="$container"
        else
            if ! kubectl -n "$namespace" get "deployment/$object" >/dev/null 2>&1; then
                echo "$label: no Deployment $object exists in namespace $namespace, so the connector the runner would dial at $url is not running." >&2
                return 1
            fi
            # Applied does not mean serving: the pod can still be pulling its
            # image or starting up when the Deployment object first appears,
            # which is exactly the race that sent the MCP probe below
            # "Connection refused" against a live cluster. Wait for the
            # rollout to finish before this connector is dialed.
            if ! kubectl -n "$namespace" rollout status "deployment/$object" --timeout=120s; then
                echo "$label: deployment/$object in namespace $namespace did not become available within 120s, so the MCP probe that follows would race pod startup." >&2
                return 1
            fi
            image="$(kubectl -n "$namespace" get "deployment/$object" -o 'jsonpath={.spec.template.spec.containers[*].image}')"
            echo "$label: $connector is up as deployment/$object on $url"
            [[ "$connector" == "tempo" ]] && host_ref="$object"
        fi

        # The built connectors run the exact artifact the build resolved, and
        # nothing else: that is the entire point of the lock (ADR 0113). The
        # third connector is an ordinary pinned `image:`, checked against the
        # fixture rather than the receipt.
        if [[ " ${CONNECTOR_BUILT[*]} " == *" $connector "* ]]; then
            expected="$(connector_image "$connector")" || return 1
            if [[ "$image" != "$expected" ]]; then
                echo "$label: $connector is running image '$image', but the build resolved '$expected'. A tier that starts anything else has lost the pin the lock exists to hold." >&2
                return 1
            fi
            echo "$label: $connector runs the resolved image $image"
        fi
    done

    if [[ -z "$host_ref" ]]; then
        echo "$label: the tempo connector was not located, so there is nothing to run the MCP probe from." >&2
        return 1
    fi

    # On a real cluster the connector NetworkPolicy admits ingress ONLY from
    # pods labeled app.kubernetes.io/component=runner-sandbox (plus the
    # release's own name/instance labels; see
    # charts/curie/templates/security-networkpolicy.yaml) -- verified live: a
    # plain pod dialing a healthy connector gets "Connection refused". Probing
    # from a pod carrying those same three labels is the positive half of ADR
    # 0113's proof ("the runner can use the same hosted MCP server
    # configuration"); a bare pod being refused is the isolation negative, not
    # a bug in this probe. The docker-hosted rungs have no such policy to
    # satisfy, so only the kubectl kind needs this stand-in.
    if [[ "$kind" != "docker" ]]; then
        probe_pod="$release-$agent-mcp-probe"
        kubectl -n "$namespace" delete pod "$probe_pod" --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kubectl -n "$namespace" run "$probe_pod" \
            --image="$(connector_image tempo)" \
            --restart=Never \
            --labels="app.kubernetes.io/name=curie,app.kubernetes.io/instance=$release,app.kubernetes.io/component=runner-sandbox" \
            --command -- sleep 300
        if ! kubectl -n "$namespace" wait --for=condition=Ready "pod/$probe_pod" --timeout=60s; then
            echo "$label: the runner-shaped probe pod $probe_pod never became Ready, so the MCP probe cannot run from it." >&2
            kubectl -n "$namespace" delete pod "$probe_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
            return 1
        fi
    fi

    # The probe runs FROM the tempo container (docker) or the runner-shaped
    # probe pod (kubectl) and dials each built connector by its alias, so a
    # pass covers alias resolution and netpol admission as well as serving.
    local probe_out tools
    for connector in "${CONNECTOR_BUILT[@]}"; do
        object="$(connector_object_name "$release" "$agent" "$connector")" || return 1
        url="http://$object.$namespace.svc.cluster.local:$CONNECTOR_PORT/mcp"
        local probe_args=("$url")
        if [[ "$connector" == "tempo" ]]; then
            probe_args+=("$CONNECTOR_TOOLS_TEMPO" "get_trace")
        else
            probe_args+=("$CONNECTOR_TOOLS_K8S_WRITE")
        fi
        if [[ "$kind" == "docker" ]]; then
            probe_out="$(docker exec -i "$host_ref" python - "${probe_args[@]}" < "$WORKDIR/mcp_probe.py")" || {
                echo "$label: the MCP probe against $connector failed." >&2
                return 1
            }
        else
            probe_out="$(kubectl -n "$namespace" exec -i "$probe_pod" -- python - "${probe_args[@]}" < "$WORKDIR/mcp_probe.py")" || {
                echo "$label: the MCP probe against $connector failed." >&2
                kubectl -n "$namespace" delete pod "$probe_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
                return 1
            }
        fi
        tools="${probe_out#OK }"
        echo "$label: $connector handshook and served tools/list -> $tools"
    done
    echo "$label: the write verb was NOT exercised, deliberately: an approval round trip needs a second human actor, which no automated rung can supply without faking the thing under test. It is proved by the example's hands-on walkthrough instead."

    if [[ -n "$probe_pod" ]]; then
        kubectl -n "$namespace" delete pod "$probe_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi

    assert_connector_entries "$label" "$entries"
}

# After a teardown, nothing labeled as a connector may survive. Scoped to the
# aliases this run's own bundle uses, never a bare host-wide label sweep: the
# label is host-wide and another session's connectors are not this run's to
# report on.
assert_connectors_reaped() {
    local label="$1" connector object alias survivor
    if [[ -z "$CONNECTOR_SCOPE_RELEASE" ]]; then
        echo "$label: no connector scope was recorded by this rung, so there is nothing to sweep for. assert_connector_parity must run before this." >&2
        return 1
    fi
    for connector in kubernetes "${CONNECTOR_BUILT[@]}"; do
        object="$(connector_object_name "$CONNECTOR_SCOPE_RELEASE" "$CONNECTOR_SCOPE_AGENT" "$connector")" || return 1
        alias="$object.$CONNECTOR_SCOPE_NAMESPACE.svc.cluster.local"
        if survivor="$(connector_container_for_alias "$alias")"; then
            echo "$label: teardown left the connector container '$survivor' running (alias $alias)." >&2
            return 1
        fi
    done
    echo "$label: no connector containers survived teardown"
}

# The unchanged path, asserted rather than assumed: a bundle that declares no
# hosted connector must start none.
#
# The PROJECT is a parameter and not a constant, because the tiers do not agree
# on it: `connector_project_label` (cli/src/docker.rs) stamps the COMPOSE
# PROJECT at the local tier and the runner's SESSION ID at the skill tier, so
# the `curietech.ai/project=curie` this used to hardcode selects nothing at all
# on a skill-tier container -- an empty survivor list every time, which is a
# green that proves nothing. Scoped rather than swept host-wide for the same
# reason every other sweep here is scoped: the component label is host-wide and
# another session's connectors are not this run's to report on.
assert_no_connector_containers() {
    local label="$1" project="$2" survivors
    if [[ -z "$project" ]]; then
        echo "$label: no connector project scope was read, so this sweep would match nothing no matter what started. An unscoped negative is a green that proves nothing, which is exactly what this assertion exists to avoid." >&2
        return 1
    fi
    survivors="$(docker ps --filter "label=$CONNECTOR_LABEL" --filter "label=curietech.ai/project=$project" --format '{{.Names}}')"
    if [[ -n "$survivors" ]]; then
        echo "$label: this bundle declares no hosted connector, but connector containers are running for project '$project':" >&2
        printf '%s\n' "$survivors" >&2
        return 1
    fi
    echo "$label: no connector containers for a bundle that declares none (project $project)"
}

# The declared HOSTED connector names: the two-space-indented top-level keys
# under `connectors:`, minus any declaration carrying a four-space `url:` or
# `unhosted_url:` key -- the same predicate the real consumer applies
# (connector_build.rs is_hosted), since a remote declaration deliberately
# starts no container and must not be asserted as one. Nested maps sit at
# four spaces and comment lines fail the name class, so this tolerant read
# matches exactly the declaration keys without needing a YAML parser on the
# runner.
declared_connector_names() {
    awk '
        function flush() { if (name != "") print name }
        /^  [A-Za-z0-9][A-Za-z0-9_-]*:/ {
            flush()
            name = $1
            sub(/:.*/, "", name)
            next
        }
        name != "" && /^    (url|unhosted_url):/ { name = "" }
        END { flush() }
    ' "$1"
}

# Whether the shared bundle copy declares hosted connectors, read off the
# declaration itself. The default rungs used to hardcode "the stock bundle
# declares none", which went stale the day examples/weather grew its
# netpol-probe enforcement fixture: the assertion must track what THIS bundle
# claims, not what the example used to be.
bundle_declares_connectors() {
    [[ -f "$WORKDIR/bundle/connectors.yaml" ]] \
        && [[ -n "$(declared_connector_names "$WORKDIR/bundle/connectors.yaml")" ]]
}

# The dual of assert_no_connector_containers, for a bundle that DOES declare
# hosted connectors: each declared name must be up as a container carrying its
# identity label in this tier's project scope. Scoped by the same three labels
# the reconciler selects on, so this asserts the exact containers the runner
# would dial rather than any lookalike.
assert_declared_connectors_hosted() {
    local label="$1" project="$2" release="$3" agent="$4" connector object found
    local -a declared=()
    while IFS= read -r connector; do
        declared+=("$connector")
    done < <(declared_connector_names "$WORKDIR/bundle/connectors.yaml")
    if [[ ${#declared[@]} -eq 0 ]]; then
        echo "$label: connectors.yaml exists but no declared connector names could be read from it, so this assertion has nothing to check. Refusing the vacuous pass." >&2
        return 1
    fi
    if [[ -z "$release" || -z "$agent" ]]; then
        echo "$label: the tier reported an incomplete connector scope (release='$release' agent='$agent'), so no identity label can be derived." >&2
        return 1
    fi
    for connector in "${declared[@]}"; do
        object="$(connector_object_name "$release" "$agent" "$connector")" || return 1
        found="$(docker ps --filter "label=$CONNECTOR_LABEL" \
            --filter "label=curietech.ai/project=$project" \
            --filter "label=curietech.ai/connector=$object" --format '{{.Names}}')"
        if [[ -z "$found" ]]; then
            echo "$label: the bundle declares connector '$connector' but no running container carries its identity label (curietech.ai/connector=$object) in project '$project'." >&2
            docker ps --filter "label=$CONNECTOR_LABEL" --format '{{.Names}} {{.Label "curietech.ai/connector"}}' >&2 || true
            return 1
        fi
        echo "$label: declared connector '$connector' is hosted as $found"
    done
}

# The skill tier's connector leg, run by the ladder itself rather than inside
# cli/scripts/e2e.sh: that script owns its own up/message/down cycle and has
# torn everything down by the time it returns, so there is no moment in it at
# which a connector can be observed. Same bundle copy, its own runner name.
case_connector_hosting_skill() {
    echo
    echo "=== case: the skill tier hosts the bundle's connectors (ADR 0113) ==="
    # --fake-model unconditionally: this case sends no turn, so a model
    # credential would be a prerequisite it does not need.
    "$BIN" skill up --fake-model --plugin-dir "$WORKDIR/bundle" --name "$CONNECTOR_RUNNER_NAME"

    local release agent namespace code=0
    release="$(container_env_value "$CONNECTOR_RUNNER_NAME" CURIE_CONNECTOR_RELEASE)"
    agent="$(container_env_value "$CONNECTOR_RUNNER_NAME" CURIE_CONNECTOR_AGENT)"
    namespace="$(container_env_value "$CONNECTOR_RUNNER_NAME" CURIE_CONNECTOR_NAMESPACE)"
    assert_connector_parity "skill" docker "$release" "$agent" "$namespace" || code=1

    # Torn down whatever the assertion said, so a failed assertion cannot strand
    # containers; the teardown sweep below only runs when there is a recorded
    # scope to sweep for.
    (cd "$WORKDIR/bundle" && "$BIN" skill down) || code=1
    if (( code == 0 )); then
        assert_connectors_reaped "skill" || code=1
    fi
    return "$code"
}

# The hermetic negative at the skill tier, and the reason it boots a runner of
# its own rather than sweeping after `cli/scripts/e2e.sh`: the project label a
# skill-tier connector container carries is the RUNNER'S SESSION ID
# (cli/src/docker.rs connector_project_label), which exists only while that
# runner does. e2e.sh has torn its runner down by the time it returns, so a
# sweep placed after it has no project to scope to and would assert nothing.
# This boots the same bundle copy rung 1 just ran, reads the session off the
# runner it started, and sweeps while it is up.
case_no_connector_hosting_skill() {
    echo
    echo "=== case: a bundle declaring no hosted connector starts none (ADR 0113) ==="
    # A scratch copy with connectors.yaml removed, because the shared bundle is
    # NOT connector-free: examples/weather deliberately carries the hosted
    # netpol-probe fixture (19f9cd48) for the cluster NetworkPolicy gate, and
    # this case's first run against it proved the point by finding that
    # connector hosted. The claim under test is about a bundle that declares
    # none, so build one.
    rm -rf "$WORKDIR/bundle-hermetic"
    cp -R "$WORKDIR/bundle" "$WORKDIR/bundle-hermetic"
    rm -rf "$WORKDIR/bundle-hermetic/.curie" "$WORKDIR/bundle-hermetic/connectors.yaml" \
        "$WORKDIR/bundle-hermetic/connectors.lock.yaml"
    # --fake-model unconditionally, for the same reason the connector case does
    # it: this case sends no turn, so a model credential is a prerequisite it
    # does not need.
    "$BIN" skill up --fake-model --plugin-dir "$WORKDIR/bundle-hermetic" --name "$HERMETIC_RUNNER_NAME"

    local session code=0
    session="$(container_env_value "$HERMETIC_RUNNER_NAME" CURIE_SESSION_ID)"
    assert_no_connector_containers "skill" "$session" || code=1

    # Torn down whatever the assertion said, so a failed assertion cannot strand
    # the runner.
    (cd "$WORKDIR/bundle-hermetic" && "$BIN" skill down) || code=1
    return "$code"
}

# An edited connector source must move the lock AND the container that runs it.
# That is the entire reason `connectors.lock.yaml` records a `source_digest`
# (ADR 0113): without this, a tier that brought up the previously locked image
# after a source edit would look identical to a correct run, and every other
# connector assertion here would still pass.
#
# It runs on a THIRD scratch copy, never `$WORKDIR/bundle`: the shared copy's
# bytes are what every later rung packs and compares (assert_bundle_identity),
# so mutating it would red the cross-rung digest assertion on a change this case
# made rather than on a real divergence.
case_connector_changed_source_skill() {
    echo
    echo "=== case: an edited connector source moves the lock and the running container (ADR 0113) ==="
    local dir="$WORKDIR/bundle-changed"
    rm -rf "$dir"
    cp -r "$WORKDIR/bundle" "$dir"
    # The copy inherits rung 1's recorded runner state, and `skill up` refuses a
    # directory that already records one.
    rm -rf "$dir/.curie"

    local before_digest before_image
    before_digest="$(lock_field "$dir" tempo source_digest)" || return 1
    before_image="$(lock_field "$dir" tempo image)" || return 1
    echo "tempo before the edit: source_digest=$before_digest image=$before_image"

    # An appended comment: it moves the bytes the source digest covers and the
    # layer the image is built from (server.py is the last COPY in the
    # connector's Dockerfile), and changes nothing the server does.
    echo "# curie parity ladder changed-source probe ($$)" >> "$dir/connectors/tempo/server.py"

    if [[ -n "$CONNECTOR_REGISTRY" ]]; then
        # `skill up`'s auto-rebuild resolves into the LOCAL DAEMON, and
        # `write_lock` refuses to replace a registry-delivered lock with a
        # local-daemon one behind a bring-up (cli/src/connector_build.rs
        # lock_overwrite_refusal) -- by design, because only a pushed image is
        # deployable to a cluster. So when this run built for a registry, the
        # rebuild is the explicit one that refusal's own fix line names. The
        # claim under test is unchanged either way: the edit must move the lock
        # and the container.
        echo "=== curie build --plugin-dir (changed source, registry delivery: $CONNECTOR_REGISTRY) ==="
        "$BIN" build --plugin-dir "$dir" --registry "$CONNECTOR_REGISTRY"
    else
        echo "the rebuild below is skill up's OWN (cli/src/commands.rs, ADR 0113 Decision 3), not a hand-run build: the production consumer of a stale lock is the tier's bring-up, so that is what this case exercises."
    fi

    "$BIN" skill up --fake-model --plugin-dir "$dir" --name "$CHANGED_RUNNER_NAME"

    local code=0
    assert_connector_source_change "$dir" "$before_digest" "$before_image" || code=1

    (cd "$dir" && "$BIN" skill down) || code=1
    return "$code"
}

# The assertion half of the case above, split out so the teardown runs whatever
# it says.
assert_connector_source_change() {
    local dir="$1" before_digest="$2" before_image="$3"
    local after_digest after_image release agent namespace object alias container running

    after_digest="$(lock_field "$dir" tempo source_digest)" || return 1
    after_image="$(lock_field "$dir" tempo image)" || return 1
    if [[ "$after_digest" == "$before_digest" ]]; then
        echo "skill: the tempo connector's source was edited, but connectors.lock.yaml still records source_digest $after_digest. The lock did not notice the edit, so every later tier would bring up the pre-edit image believing it current." >&2
        return 1
    fi
    if [[ "$after_image" == "$before_image" ]]; then
        echo "skill: the tempo connector's source_digest moved to $after_digest, but the lock still names image '$after_image'. A moved digest that resolves the same artifact is the stale-image bug wearing a fresh label." >&2
        return 1
    fi
    echo "skill: the edit moved the lock: source_digest $before_digest -> $after_digest, image $before_image -> $after_image"

    release="$(container_env_value "$CHANGED_RUNNER_NAME" CURIE_CONNECTOR_RELEASE)"
    agent="$(container_env_value "$CHANGED_RUNNER_NAME" CURIE_CONNECTOR_AGENT)"
    namespace="$(container_env_value "$CHANGED_RUNNER_NAME" CURIE_CONNECTOR_NAMESPACE)"
    if [[ -z "$release" || -z "$agent" || -z "$namespace" ]]; then
        echo "skill: the runner started from the edited copy reported an incomplete connector scope (release='$release' agent='$agent' namespace='$namespace'), so no connector address can be derived." >&2
        return 1
    fi
    object="$(connector_object_name "$release" "$agent" tempo)" || return 1
    alias="$object.$namespace.svc.cluster.local"
    if ! container="$(connector_container_for_alias "$alias")"; then
        echo "skill: no running container labeled $CONNECTOR_LABEL answers to '$alias' after the rebuild, so the rebuilt connector is not up at all." >&2
        docker ps --filter "label=$CONNECTOR_LABEL" --format '{{.Names}} {{.Image}}' >&2 || true
        return 1
    fi
    running="$(docker inspect "$container" --format '{{.Config.Image}}')"
    if [[ "$running" != "$after_image" ]]; then
        echo "skill: the lock now resolves tempo to '$after_image', but container '$container' is running '$running'. The bring-up started the pre-edit artifact, which is the exact failure the lock's source_digest exists to catch." >&2
        return 1
    fi
    echo "skill: the restarted tempo connector runs the rebuilt image $running"
}

# A cluster deploy whose locked image the REGISTRY cannot resolve must refuse,
# and must refuse before it has touched the cluster (ADR 0113, cli/src/commands.rs
# registry_preflight). The failure this guards against is not theoretical: the
# lock is what a node pulls from, so an image that is gone from the registry
# surfaces after apply as a pod stuck on ImagePullBackOff, with a healthy
# connector Deployment already replaced. Refusing up front is what keeps the
# running release intact, so both halves are asserted -- the refusal AND the
# untouched Deployment.
case_connector_registry_missing_cluster() {
    local release="$1" agent="$2" namespace="$3"
    echo
    echo "=== case: cluster deploy refuses a lock the registry cannot resolve (ADR 0113) ==="
    local lock="$WORKDIR/bundle/connectors.lock.yaml"
    local backup="$WORKDIR/connectors.lock.yaml.good"
    local object before after good bad out code=0

    object="$(connector_object_name "$release" "$agent" tempo)" || return 1
    before="$(kubectl -n "$namespace" get "deployment/$object" -o 'jsonpath={.spec.template.spec.containers[*].image}')"
    if [[ -z "$before" ]]; then
        echo "cluster: deployment/$object reports no container image, so there is no before-and-after to compare the refused deploy against." >&2
        return 1
    fi

    good="$(connector_image tempo)" || return 1
    # The digest's 64 hex characters replaced, and NOTHING else: the reference
    # stays `<repo>@sha256:<64 lowercase hex>`, the one shape `parse_lock`
    # accepts for registry delivery (cli/src/connector_build.rs). A tag here
    # would be refused at the read instead, and this case would then assert a
    # green against the lock reader rather than against the registry preflight
    # it is aimed at. All-f is absent from the registry by construction: it is
    # the digest of no content anyone has ever pushed, and a sha256 collision
    # with it is not a thing that happens.
    bad="${good%@*}@sha256:$(printf 'f%.0s' {1..64})"

    cp "$lock" "$backup"
    # The IMAGE only, never the source_digest: moving the digest trips
    # `lock_preflight`'s staleness refusal first, and this case would then
    # assert a green against the wrong refusal entirely.
    sed -i "s|$good|$bad|" "$lock"
    if ! grep -qF "$bad" "$lock"; then
        echo "cluster: connectors.lock.yaml still does not name '$bad' after the edit, so the deploy below would run against a perfectly good lock and prove nothing." >&2
        cp "$backup" "$lock"
        touch -t 200001010000 "$lock"
        return 1
    fi
    echo "cluster: tempo's locked image corrupted to '$bad' for this one deploy"

    out="$("$BIN" --json cluster deploy --plugin-dir "$WORKDIR/bundle" 2>&1)" && code=0 || code=$?
    printf '%s\n' "$out"

    # Restored BEFORE the assertions, so a red one cannot carry a corrupt lock
    # into the rest of this rung. Restored from the byte-for-byte backup rather
    # than rebuilt: a rebuild costs minutes and, more to the point, re-stamps the
    # file's mtime, which pack_tar_gz embeds -- and this copy's packed bytes are
    # what assert_bundle_identity compares. The fixed epoch is the one the ladder
    # normalizes every bundle file to.
    cp "$backup" "$lock"
    touch -t 200001010000 "$lock"

    if (( code != 2 )); then
        echo "cluster: a deploy whose locked image the registry cannot resolve must exit 2 (usage), got $code." >&2
        return 1
    fi
    if [[ "$out" != *"registry could not resolve"* ]]; then
        echo "cluster: the deploy failed, but not with the registry-resolution refusal, so this case never reached the preflight it is aimed at. A deploy that fails for some other reason is not evidence the lock is checked." >&2
        return 1
    fi
    echo "cluster: cluster deploy refused the unresolvable image with exit 2"

    after="$(kubectl -n "$namespace" get "deployment/$object" -o 'jsonpath={.spec.template.spec.containers[*].image}')"
    if [[ "$after" != "$before" ]]; then
        echo "cluster: the refused deploy still moved deployment/$object from '$before' to '$after'. A preflight that refuses after touching the cluster has already broken the release it was meant to protect." >&2
        return 1
    fi
    echo "cluster: deployment/$object still runs $before -- the refusal changed nothing on the cluster"
}

start_local_otel_sink() {
    # The Rust executing-contract tests intentionally replace docker/curie with
    # stubs. Keep those tests about ladder branching; only the real local rung
    # claims runtime OTel evidence.
    if [[ -n "${STUB_STATE:-}" ]]; then
        echo "local: runtime OTel sink skipped under the command-stub harness"
        return 0
    fi
    if [[ -n "$(docker ps -q --filter 'name=^/curie-api$' 2>/dev/null)" ]]; then
        echo "local: a compose stack is already running, so this rung cannot replace its immutable OTel endpoint with the task-owned sink." >&2
        echo "fix: let the owning session run \`curie local down\`, then re-run CURIE_E2E_TIERS=local curie dev e2e-ladder." >&2
        return 1
    fi

    local network=curie_runner
    if ! docker network inspect "$network" >/dev/null 2>&1; then
        docker network create \
            --label com.docker.compose.project=curie \
            --label com.docker.compose.network=curie_runner \
            "$network" >/dev/null
        LOCAL_OTEL_NETWORK_OWNED=1
    fi

    local output_dir="$WORKDIR/otel-sink"
    mkdir -p "$output_dir"
    chmod 0777 "$output_dir"
    local sink_config="$WORKDIR/otel-e2e-sink-config.yaml"
    awk '
        { print }
        $0 == "service:" {
            print "  telemetry:"
            print "    metrics:"
            print "      level: detailed"
            print "      address: 0.0.0.0:8888"
        }
    ' "$REPO_ROOT/cli/scripts/fixtures/otel-e2e-sink-config.yaml" > "$sink_config"
    LOCAL_OTEL_SINK_OWNED=1
    local start_log="$WORKDIR/otel-sink-start.log" attempt started=0
    # Docker's ephemeral host-port allocator can race the kernel's current
    # listeners even on a fresh CI runner. Retry only that explicit bind race;
    # configuration/image failures remain immediately loud.
    for attempt in $(seq 1 5); do
        if docker run -d \
            --name "$LOCAL_OTEL_SINK_NAME" \
            --label "curietech.ai/e2e-owner=$LOCAL_OTEL_SINK_NAME" \
            --network "$network" \
            --user 0 \
            -p 0.0.0.0::4318 \
            -p 127.0.0.1::13133 \
            -p 127.0.0.1::8888 \
            -v "$sink_config:/etc/otelcol-contrib/config.yaml:ro" \
            -v "$output_dir:/var/lib/otel-e2e" \
            otel/opentelemetry-collector-contrib:0.119.0 \
            --config=/etc/otelcol-contrib/config.yaml >"$start_log" 2>&1; then
            started=1
            break
        fi
        if ! grep -Fq 'address already in use' "$start_log"; then
            cat "$start_log" >&2
            return 1
        fi
        docker rm -f "$LOCAL_OTEL_SINK_NAME" >/dev/null 2>&1 || true
        echo "local: Docker host-port allocation raced on attempt $attempt; retrying task-owned sink" >&2
        sleep 1
    done
    if (( ! started )); then
        cat "$start_log" >&2
        echo "local: Docker could not allocate private sink ports after 5 attempts" >&2
        return 1
    fi

    local gateway otlp_port health_port metrics_port
    gateway="$(docker network inspect "$network" --format '{{(index .IPAM.Config 0).Gateway}}')"
    otlp_port="$(docker port "$LOCAL_OTEL_SINK_NAME" 4318/tcp | awk -F: 'END {print $NF}')"
    health_port="$(docker port "$LOCAL_OTEL_SINK_NAME" 13133/tcp | awk -F: 'END {print $NF}')"
    metrics_port="$(docker port "$LOCAL_OTEL_SINK_NAME" 8888/tcp | awk -F: 'END {print $NF}')"
    if [[ -z "$gateway" || -z "$otlp_port" || -z "$health_port" || -z "$metrics_port" ]]; then
        echo "local: task-owned OTel sink did not publish its gateway and ports" >&2
        return 1
    fi
    LOCAL_OTEL_ENDPOINT="http://$gateway:$otlp_port"
    LOCAL_OTEL_METRICS_ENDPOINT="http://127.0.0.1:$metrics_port/metrics"
    export OTEL_EXPORTER_OTLP_ENDPOINT="$LOCAL_OTEL_ENDPOINT"
    export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

    local attempt
    for attempt in $(seq 1 30); do
        if curl -fsS "http://127.0.0.1:$health_port/" >/dev/null 2>&1; then
            LOCAL_OTEL_SINK_ACTIVE=1
            echo "local: task-owned OTel sink is healthy at $LOCAL_OTEL_ENDPOINT"
            assert_local_otel_zero_export_control
            return 0
        fi
        sleep 1
    done
    docker logs "$LOCAL_OTEL_SINK_NAME" >&2 || true
    echo "local: task-owned OTel sink did not become healthy" >&2
    return 1
}

stop_local_otel_sink() {
    if (( LOCAL_OTEL_SINK_OWNED )); then
        docker rm -f "$LOCAL_OTEL_SINK_NAME" >/dev/null 2>&1 || true
        LOCAL_OTEL_SINK_OWNED=0
    fi
    if (( LOCAL_OTEL_NETWORK_OWNED )); then
        docker network rm curie_runner >/dev/null 2>&1 || true
        LOCAL_OTEL_NETWORK_OWNED=0
    fi
    LOCAL_OTEL_SINK_ACTIVE=0
    LOCAL_OTEL_METRICS_ENDPOINT=""
}

local_otel_query() {
    local mode="$1" baseline="${2:-}"
    python3 - "$mode" "$WORKDIR/otel-sink" "$baseline" "$PROMPT" "$OTEL_E2E_SECRET_SENTINEL" <<'PY'
import json
import pathlib
import sys

mode, root_raw, baseline_raw, prompt, sentinel = sys.argv[1:]
root = pathlib.Path(root_raw)

def documents(name):
    path = root / f"{name}.json"
    if not path.exists():
        return []
    result = []
    for line in path.read_text(errors="replace").splitlines():
        if line.strip():
            result.append(json.loads(line))
    return result

def attr_map(items):
    values = {}
    for item in items or []:
        value = item.get("value", {})
        scalar = next((value[k] for k in (
            "stringValue", "intValue", "doubleValue", "boolValue"
        ) if k in value), None)
        values[item.get("key")] = scalar
    return values

spans = []
for doc in documents("traces"):
    for resource_spans in doc.get("resourceSpans", []):
        resource = attr_map(resource_spans.get("resource", {}).get("attributes"))
        for scope_spans in resource_spans.get("scopeSpans", []):
            for span in scope_spans.get("spans", []):
                spans.append((span, resource))

logs = []
for doc in documents("logs"):
    for resource_logs in doc.get("resourceLogs", []):
        resource = attr_map(resource_logs.get("resource", {}).get("attributes"))
        for scope_logs in resource_logs.get("scopeLogs", []):
            for record in scope_logs.get("logRecords", []):
                logs.append((record, resource))

metrics = []
for doc in documents("metrics"):
    for resource_metrics in doc.get("resourceMetrics", []):
        resource = attr_map(resource_metrics.get("resource", {}).get("attributes"))
        for scope_metrics in resource_metrics.get("scopeMetrics", []):
            for metric in scope_metrics.get("metrics", []):
                points = []
                kind = next(
                    (kind for kind in ("gauge", "sum", "histogram", "exponentialHistogram")
                     if kind in metric),
                    None,
                )
                if kind is not None:
                    points.extend(metric[kind].get("dataPoints", []))
                metrics.append((metric.get("name"), kind, points, resource))

def point_value(kind, point):
    if kind in ("histogram", "exponentialHistogram"):
        return float(point.get("count", 0))
    for key in ("asInt", "asDouble"):
        if key in point:
            return float(point[key])
    return 0.0

def metric_series():
    # OTLP cumulative exports repeat the same time series. Retain only its
    # latest point, keyed by stable resource identity plus bounded attributes,
    # so a before/after delta is a real instrument change rather than a count of
    # export cycles. A recreated service has a new instance id and therefore a
    # separate monotonically increasing series instead of looking like a reset.
    latest = {}
    sequence = 0
    for name, kind, points, resource in metrics:
        for point in points:
            sequence += 1
            attrs = attr_map(point.get("attributes"))
            key = (
                name,
                resource.get("service.name"),
                resource.get("service.instance.id"),
                json.dumps(attrs, sort_keys=True, separators=(",", ":")),
            )
            stamp = int(point.get("timeUnixNano") or sequence)
            candidate = {
                "name": name,
                "service": resource.get("service.name"),
                "instance": resource.get("service.instance.id"),
                "attributes": attrs,
                "value": point_value(kind, point),
            }
            if key not in latest or stamp >= latest[key][0]:
                latest[key] = (stamp, candidate)
    return [candidate for _, candidate in latest.values()]

def snapshot():
    return {
        "trace_ids": sorted({span.get("traceId") for span, _ in spans if span.get("traceId")}),
        "metrics": metric_series(),
    }

def load_baseline():
    assert baseline_raw, f"{mode} requires a before snapshot"
    return json.loads(pathlib.Path(baseline_raw).read_text())

def metric_total(series, name, **wanted):
    return sum(
        item["value"] for item in series
        if item["name"] == name
        and all(item["attributes"].get(key) == value for key, value in wanted.items())
    )

def delta(before, current, name, **wanted):
    return metric_total(current, name, **wanted) - metric_total(before, name, **wanted)

if mode == "snapshot":
    print(json.dumps(snapshot(), sort_keys=True))
    raise SystemExit(0)

if mode == "no-turn":
    series = metric_series()
    assert metric_total(series, "curie.turn.accepted") == 0, (
        "turn.accepted changed before the ladder sent a turn"
    )
    assert metric_total(series, "curie.turn.completed") == 0, (
        "turn.completed changed before the ladder sent a turn"
    )
    raise SystemExit(0)

# `curie local message` delegates its producer write to a bounded, Slack-free
# process in the real dispatcher image. Its producer span injects the carrier
# beside the frozen payload, so this exact runtime proof must start at dispatcher
# enqueue and cross Valkey into worker -> sandbox -> runner -> reply.
required_spans = {
    "curie.queue.enqueue", "curie.queue.process", "curie.turn.process",
    "curie.sandbox.claim", "curie.runner.rpc", "agent.run",
}
by_trace = {}
for span, resource in spans:
    by_trace.setdefault(span.get("traceId"), []).append((span, resource))
healthy = []
for trace_id, trace_spans in by_trace.items():
    names = {span.get("name") for span, _ in trace_spans}
    services = {resource.get("service.name") for _, resource in trace_spans}
    has_reply = bool({"curie.reply.post", "curie.reply.update"} & names)
    if required_spans <= names and has_reply and {
        "curie-dispatcher", "curie-worker", "curie-runner"
    } <= services:
        healthy.append(trace_id)

if mode == "healthy":
    before = load_baseline()
    new_trace_ids = set(by_trace) - set(before["trace_ids"])
    new_healthy = [trace_id for trace_id in healthy if trace_id in new_trace_ids]
    assert new_healthy, (
        "expected a new end-to-end trace after the before snapshot; "
        f"span names={sorted({span.get('name') for span, _ in spans})}; "
        f"services={sorted({resource.get('service.name') for _, resource in spans})}"
    )
    for trace_id in new_healthy:
        trace_spans = by_trace[trace_id]
        assert all(
            span.get("status", {}).get("code") not in (2, "STATUS_CODE_ERROR")
            for span, _ in trace_spans
        ), f"healthy trace {trace_id} contains an ERROR span"
        agent_runs = [span for span, _ in trace_spans if span.get("name") == "agent.run"]
        assert agent_runs and all(
            span.get("status", {}).get("code") in (1, "STATUS_CODE_OK")
            for span in agent_runs
        ), f"healthy trace {trace_id} did not finish agent.run with explicit OK"
        failure_events = [
            attrs.get("curie.outcome")
            for span, _ in trace_spans
            for event in span.get("events", [])
            for attrs in [attr_map(event.get("attributes"))]
            if attrs.get("curie.outcome") in ("failure", "classified_failure")
        ]
        assert not failure_events, (
            f"healthy trace {trace_id} carried failure outcomes {failure_events}"
        )

    # The health probe immediately before the turn exercises API telemetry;
    # the one-shot producer and turn exercise dispatcher, worker, and runner.
    # Every platform service must therefore emit both a new span and a log with
    # the same trace/span context after the baseline snapshot.
    platform_services = {"curie-api", "curie-dispatcher", "curie-worker", "curie-runner"}
    exercised = {
        resource.get("service.name")
        for trace_id in new_trace_ids
        for _, resource in by_trace[trace_id]
        if resource.get("service.name") in platform_services
    }
    correlated_services = {
        resource.get("service.name")
        for record, resource in logs
        if record.get("traceId") in new_trace_ids
        and record.get("spanId")
        and resource.get("service.name") in exercised
    }
    assert exercised == platform_services, (
        "expected every platform service to emit a new span: "
        f"{sorted(platform_services - exercised)}"
    )
    assert platform_services <= correlated_services, (
        "services emitted spans without correlated OTLP logs: "
        f"{sorted(platform_services - correlated_services)}"
    )

    required_metrics = {
        "curie.turn.accepted", "curie.turn.completed", "curie.turn.duration",
        "curie.queue.message.age", "curie.sandbox.lifecycle", "curie.runner.rpc.result",
    }
    names = {name for name, _, _, _ in metrics}
    assert required_metrics <= names, f"missing operational metrics: {sorted(required_metrics - names)}"
    forbidden = {"event.id", "session.id", "sandbox.id", "user.id", "trace_id", "span_id"}
    for name, _, points, _ in metrics:
        for point in points:
            keys = set(attr_map(point.get("attributes")))
            assert not keys & forbidden, f"metric {name} has high-cardinality attributes {sorted(keys & forbidden)}"

    current = metric_series()
    previous = before["metrics"]
    positive_deltas = {
        "curie.turn.accepted": delta(previous, current, "curie.turn.accepted"),
        "curie.turn.completed[done]": delta(
            previous, current, "curie.turn.completed", outcome="done"
        ),
        "curie.turn.duration[done]": delta(
            previous, current, "curie.turn.duration", outcome="done"
        ),
        "curie.queue.message.age": delta(previous, current, "curie.queue.message.age"),
        "curie.sandbox.lifecycle": delta(previous, current, "curie.sandbox.lifecycle"),
        "curie.runner.rpc.result[success]": delta(
            previous, current, "curie.runner.rpc.result", outcome="success"
        ),
    }
    assert all(value > 0 for value in positive_deltas.values()), (
        f"successful turn did not move every required counter/measurement: {positive_deltas}"
    )
    classified_delta = delta(
        previous, current, "curie.turn.completed", outcome="classified_failure"
    )
    assert classified_delta == 0, (
        "healthy control changed the classified_failure counter by "
        f"{classified_delta}"
    )
    print(json.dumps({
        "healthy_new_traces": len(new_healthy),
        "correlated_services": sorted(correlated_services),
        "metric_deltas": positive_deltas,
        "classified_failure_delta": classified_delta,
    }, sort_keys=True))
    raise SystemExit(0)

if mode == "redacted":
    payload = "\n".join(
        (root / name).read_text(errors="replace")
        for name in ("traces.json", "logs.json", "metrics.json")
        if (root / name).exists()
    )
    assert prompt not in payload, "the user prompt was exported verbatim"
    assert sentinel not in payload, "the credential-shaped sentinel was exported"
    raise SystemExit(0)

if mode == "failed":
    before = load_baseline()
    new_trace_ids = set(by_trace) - set(before["trace_ids"])
    failed_trace_ids = []
    for trace_id in new_trace_ids:
        trace_spans = by_trace[trace_id]
        names = {span.get("name") for span, _ in trace_spans}
        if not required_spans <= names:
            continue
        error_span_names = {
            span.get("name")
            for span, _ in trace_spans
            if span.get("status", {}).get("code") in (2, "STATUS_CODE_ERROR")
        }
        has_error = {"curie.turn.process", "agent.run"} <= error_span_names
        has_classified = any(
            attr_map(event.get("attributes")).get("curie.outcome") == "classified_failure"
            for span, _ in trace_spans
            for event in span.get("events", [])
        )
        if has_error and has_classified:
            failed_trace_ids.append(trace_id)
    assert failed_trace_ids, (
        "no new worker-to-runner trace carried ERROR on turn.process + agent.run and classified_failure"
    )
    error_log_traces = {
        record.get("traceId")
        for record, _ in logs
        if record.get("traceId") in failed_trace_ids
        and record.get("spanId")
        and (
            int(record.get("severityNumber") or 0) >= 17
            or str(record.get("severityText") or "").upper() in ("ERROR", "FATAL")
        )
    }
    assert error_log_traces, (
        "the new failed trace had no ERROR LogRecord with the same traceId"
    )
    current = metric_series()
    previous = before["metrics"]
    classified_delta = delta(
        previous, current, "curie.turn.completed", outcome="classified_failure"
    )
    assert classified_delta > 0, (
        "the injected failure did not increase curie.turn.completed{outcome=classified_failure}"
    )
    done_delta = delta(previous, current, "curie.turn.completed", outcome="done")
    assert done_delta == 0, (
        f"the injected failure incorrectly increased successful completions by {done_delta}"
    )
    print(json.dumps({
        "failed_new_traces": len(failed_trace_ids),
        "error_log_trace_matches": len(error_log_traces),
        "classified_failure_delta": classified_delta,
        "done_delta": done_delta,
    }, sort_keys=True))
    raise SystemExit(0)

raise AssertionError(f"unknown local OTel query mode: {mode}")
PY
}

assert_bounded_metric_attributes() {
    # The query's forbidden set is intentionally repeated at the call site so a
    # source review cannot mistake this for an unexercised helper: event.id,
    # session.id, sandbox.id, user.id, trace_id, and span_id are rejected.
    local_otel_query healthy "$1"
}

local_otel_write_snapshot() {
    local destination="$1"
    local_otel_query snapshot > "$destination"
}

wait_for_local_otel_metric_settle() {
    # App readers export every 10s; cross one full interval plus margin so a
    # prior turn cannot arrive after the failure baseline is captured.
    sleep 12
}

local_otel_self_metric_value() {
    local metric="$1"
    curl -fsS "$LOCAL_OTEL_METRICS_ENDPOINT" | python3 -c '
import re, sys
name = sys.argv[1]
total = 0.0
for line in sys.stdin:
    if re.match(r"^" + re.escape(name) + r"(?:\{|\s)", line):
        total += float(line.rsplit(None, 1)[1])
print(total)
' "$metric"
}

assert_local_otel_zero_export_control() {
    local accepted sent
    local_otel_query no-turn
    accepted="$(local_otel_self_metric_value otelcol_receiver_accepted_metric_points)"
    sent="$(local_otel_self_metric_value otelcol_exporter_sent_metric_points)"
    python3 -c '
import sys
raise SystemExit(0 if all(float(value) == 0 for value in sys.argv[1:]) else 1)
' "$accepted" "$sent" || {
        echo "local: fresh sink received/exported metric points before any platform service started (accepted=$accepted sent=$sent)" >&2
        return 1
    }
    echo "local: zero-export negative proved a fresh sink has no turn counters and accepted=0 sent=0 metric points"
}

assert_local_otel_no_turn_pipeline_live() {
    local attempt accepted=0 sent=0
    for attempt in $(seq 1 45); do
        if local_otel_query no-turn >/dev/null 2>&1; then
            accepted="$(local_otel_self_metric_value otelcol_receiver_accepted_metric_points)"
            sent="$(local_otel_self_metric_value otelcol_exporter_sent_metric_points)"
            if python3 -c '
import sys
raise SystemExit(0 if all(float(value) > 0 for value in sys.argv[1:]) else 1)
' "$accepted" "$sent"; then
                local_otel_query no-turn
                echo "local: no-turn control kept app turn counters at zero while Collector accepted=$accepted and exported=$sent metric points"
                return 0
            fi
        fi
        sleep 2
    done
    echo "local: no-turn control could not distinguish a live metric pipeline from zero export (accepted=$accepted sent=$sent)" >&2
    return 1
}

assert_local_otel_redacted() {
    local_otel_query redacted
}

assert_local_otel_healthy_turn() {
    local baseline="$1" attempt
    # Local causal trace: curie.queue.enqueue -> curie.queue.process ->
    # curie.turn.process -> curie.sandbox.claim -> curie.runner.rpc ->
    # agent.run -> curie.reply.update or curie.reply.post. Correlated logs must carry traceId,
    # spanId, and resource service.name for dispatcher, worker, and runner.
    # Operational metrics pinned here are
    # curie.turn.accepted, curie.turn.completed, curie.turn.duration,
    # curie.queue.message.age, curie.sandbox.lifecycle, and
    # curie.runner.rpc.result. Metric points must reject event.id, session.id,
    # sandbox.id, user.id, trace_id, and span_id.
    for attempt in $(seq 1 45); do
        if assert_bounded_metric_attributes "$baseline" >/dev/null 2>&1; then
            assert_bounded_metric_attributes "$baseline"
            assert_local_otel_redacted
            echo "local: OTel healthy control proved causality, log correlation, bounded metric attributes, and redaction"
            return 0
        fi
        sleep 2
    done
    assert_bounded_metric_attributes "$baseline"
}

inject_local_runner_failure() {
    LOCAL_OTEL_FAILURE_MODE=1
    export CURIE_FAKE_MODEL=0
    export CURIE_MODEL_BASE_URL="$LOCAL_OTEL_ENDPOINT"
    export CURIE_MODEL_API_BACKEND=messages
    docker compose --profile core --profile full -f "$REPO_ROOT/compose.dev.yaml" \
        up -d --force-recreate --no-deps curie-worker >/dev/null
    sleep 3
}

restore_local_runner_health() {
    if [[ "$LIVE" == "1" ]]; then
        export CURIE_FAKE_MODEL=0
    else
        export CURIE_FAKE_MODEL=1
    fi
    unset CURIE_MODEL_BASE_URL CURIE_MODEL_API_BACKEND
    docker compose --profile core --profile full -f "$REPO_ROOT/compose.dev.yaml" \
        up -d --force-recreate --no-deps curie-worker >/dev/null
    LOCAL_OTEL_FAILURE_MODE=0
    sleep 3
}

assert_local_otel_failed_turn() {
    local baseline="$1" attempt
    for attempt in $(seq 1 45); do
        if local_otel_query failed "$baseline" >/dev/null 2>&1; then
            local_otel_query failed "$baseline"
            echo "local: injected non-retryable runner failure exported a new causally matched ERROR trace/log and classified_failure metric delta"
            return 0
        fi
        sleep 2
    done
    local_otel_query failed "$baseline"
}

case_local_otel_runner_failure() {
    (( LOCAL_OTEL_SINK_ACTIVE )) || return 0
    echo
    echo "=== case: local runner failure is observable and recovers ==="
    # Negative evidence requires explicit ERROR status and classified_failure
    # outcome before the restored healthy trace. This injected model_not_found
    # is deliberately non-retryable; manufacturing a retry would weaken the
    # kernel's bounded failure classification rather than strengthen the proof.
    local failure_before="$WORKDIR/otel-before-failure.json"
    local restored_before="$WORKDIR/otel-before-restored.json"
    local failure_out failure_code=0 restored_out
    wait_for_local_otel_metric_settle
    local_otel_write_snapshot "$failure_before"
    inject_local_runner_failure
    failure_out="$("$BIN" --json local message --channel C0LOCALDEV "runner failure control" 2>&1)" || failure_code=$?
    printf '%s\n' "$failure_out"
    if (( failure_code != 0 )) && [[ "$failure_out" != *'"finalized":true'* ]]; then
        # An unexpectedly shaped failure cannot be allowed to strand the
        # injected worker configuration.
        restore_local_runner_health
        echo "local: injected runner failure produced neither a finalized escalation nor a queryable reply" >&2
        return 1
    fi
    # Assert before recreating the worker: the live BatchSpanProcessor owns
    # the failed turn's pending export, so restoring first can erase the very
    # trace/log/metric evidence this control is meant to observe.
    if ! assert_local_otel_failed_turn "$failure_before"; then
        # The evidence assertion itself may fail, but failure must still leave
        # the ordinary local runner configuration restored for later cleanup.
        restore_local_runner_health
        return 1
    fi
    restore_local_runner_health

    local_otel_write_snapshot "$restored_before"
    restored_out="$("$BIN" --json local message --channel C0LOCALDEV "restored healthy control" || true)"
    printf '%s\n' "$restored_out"
    assert_finalized_reply "local" "$restored_out"
    assert_local_otel_healthy_turn "$restored_before"
}

# Rung 1: the existing skill-tier round trip. Still exactly one implementation
# and never copied -- but no longer unparameterized: it is handed THIS run's
# bundle copy through CURIE_E2E_BUNDLE, so rung 1 boots and grades the same
# artifact and the same case set as every later rung. That deliberately
# supersedes #690's "keep the current skill-tier leg as is"; the `--from-spec`
# deal-desk leg it refers to (#325's acceptance evidence) is preserved as the
# behavior of a bare `bash cli/scripts/e2e.sh`, which is where that evidence now
# lives.
#
# The #747 recovery case rides here because it drives skill-tier verbs and shares
# rung 1's runner-image requirement. It is unaffected by the shared bundle: it
# expects exit 2 from `skill up`'s name-conflict preflight and never reaches a
# pack, so it neither reads nor perturbs artifact identity.
rung_skill() {
    echo
    echo "########## rung 1/3: skill ##########"
    RAN_RUNGS="$RAN_RUNGS skill"

    local log="$WORKDIR/rung-skill.log"
    # `tee`, not a command substitution: rung 1 runs for minutes and its progress
    # must keep streaming. `set -o pipefail` is already on (set -euo pipefail
    # above), so a failing e2e.sh still fails this rung through the pipe rather
    # than being masked by tee's exit status -- never add `|| true` here. The log
    # lives inside $WORKDIR, so the existing `rm -rf` in the trap covers it and
    # this adds no new cleanup path.
    CURIE_E2E_BUNDLE="$WORKDIR/bundle" bash "$REPO_ROOT/cli/scripts/e2e.sh" | tee "$log"

    # e2e.sh already prints its client-side digest on a stable line, and exits 1
    # on a null or empty one BEFORE reaching that line, so a present line always
    # carries a real value. Parsing it keeps the env contract at one variable:
    # e2e.sh gains no output contract of its own.
    local line digest=""
    while IFS= read -r line; do
        if [[ "$line" == "initial bundle digest: "* ]]; then
            digest="${line#initial bundle digest: }"
        fi
    done < "$log"
    if [[ -z "$digest" ]]; then
        echo "skill: cli/scripts/e2e.sh printed no \"initial bundle digest:\" line, so rung 1's artifact identity cannot be read." >&2
        return 1
    fi
    # Rung 1 is the one rung that reads its case ids back DIRECTLY, inside
    # e2e.sh's --json skill eval assertion, in fake mode as well as live.
    SUITE_RUNGS="$SUITE_RUNGS skill"
    assert_bundle_identity "skill" "$digest"

    case_leftover_runner_container

    if connector_mode; then
        case_connector_hosting_skill
        case_connector_changed_source_skill
    else
        # The hermetic claim, exercised on every ordinary run rather than only
        # in connector mode: the default bundle declares no hosted connector, so
        # this rung is where "declares none, starts none" is actually testable.
        case_no_connector_hosting_skill
    fi
}

# Rung 2: the compose tier, cold start to teardown.
rung_local() {
    echo
    echo "########## rung 2/3: local (compose) ##########"
    RAN_RUNGS="$RAN_RUNGS local"

    start_local_otel_sink

    assert_stub_port_free

    if [[ -n "$(docker ps -q --filter 'name=curie-api' 2>/dev/null)" ]]; then
        # Reuse it and do NOT tear it down: the thread that brought a stack up
        # owns tearing it down, in both directions.
        echo "a compose stack is already running; reusing it and leaving teardown to whoever started it"
        # Model mode is fixed at `local up` time, so a reused stack may have been
        # started sealed. That used to be a warning; it is now VERIFIED by
        # assert_model_mode below, off the reused stack's own running worker, and
        # a contradiction is a hard failure with a fix line.
        echo "note: the reused stack's model mode was fixed by whoever ran \`local up\`; it is verified below against this run's mode, and a mismatch fails this rung."
    else
        echo
        local up_args=(local up)
        if (( ! LOCAL_OTEL_SINK_ACTIVE )) && [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
            up_args+=(--minimal)
        fi
        echo "=== curie ${up_args[*]} ==="
        # Ordinary bundles use the core profile. A trajectory sidecar selects the
        # full profile because its matrix read requires Langfuse.
        #
        # Claim ownership BEFORE starting, never after: `local up` blocks for
        # seconds while it waits for health, and containers exist for that whole
        # window. Setting the flag afterwards means a signal or a mid-boot
        # failure leaves the trap disowning a stack this run created, stranding
        # it. Claiming a stack that then fails to boot is harmless, because
        # `local down` is safe against a partial or already-stopped stack.
        LOCAL_STACK_OWNED=1
        "$BIN" "${up_args[@]}"
    fi

    echo
    echo "=== curie --json local deploy ==="
    # No --api-url: the default IS the cold-start path a real user hits, and
    # exercising the default is the point. First create binds C0LOCALDEV, so the
    # message below can resolve the sole deployed agent with no --channel.
    #
    # --json for the receipt: `local status --json` carries no digest
    # (cli/schema/local-status.schema.json is only `services`), so the deploy
    # receipt's bundle.sha256 -- the platform's server-side hash of the bytes this
    # rung uploaded -- is the ONLY surface that reports this tier's artifact
    # identity. Read from stdout only; the human text is on stderr.
    local deploy_json digest agent_id agent_name deployment_id
    deploy_json="$("$BIN" --json local deploy --plugin-dir "$WORKDIR/bundle")"
    printf '%s\n' "$deploy_json"
    digest="$(deploy_field "local" "$deploy_json" bundle.sha256)"
    agent_id="$(deploy_field "local" "$deploy_json" agent.id)"
    agent_name="$(deploy_field "local" "$deploy_json" agent.name)"
    deployment_id="$(deploy_field "local" "$deploy_json" deployment.id)"

    echo
    echo "=== assert the turn will bind to the deployment this rung created ==="
    # BEFORE the turn, deliberately: a shadowed binding must stop the rung rather
    # than produce a green turn against the wrong artifact.
    assert_sole_active_deployment "local" "$agent_id" "$deployment_id"

    echo
    echo "=== assert the deployed worker's effective model mode ==="
    local observed_mode
    observed_mode="$(probe_local_fake_model)"
    assert_model_mode "local" "$observed_mode"

    echo
    echo "=== assert the bundle's connectors (ADR 0113) ==="
    if connector_mode; then
        local worker
        worker="$(local_worker_container)"
        assert_connector_parity "local" docker \
            "$(container_env_value "$worker" CURIE_RELEASE)" \
            "$agent_name" \
            "$(container_env_value "$worker" CURIE_NAMESPACE)"
    elif bundle_declares_connectors; then
        # The stock bundle declares hosted connectors (the weather bundle's
        # netpol-probe enforcement fixture), so "declares none, starts none"
        # is not this bundle's claim. Assert the dual instead: every declared
        # connector is up under its identity label. `curie` is the compose
        # project the CLI pins (cli/src/local.rs COMPOSE_PROJECT).
        local worker
        worker="$(local_worker_container)"
        assert_declared_connectors_hosted "local" curie \
            "$(container_env_value "$worker" CURIE_RELEASE)" "$agent_name"
    else
        # `curie` is the compose project the CLI pins (cli/src/local.rs
        # COMPOSE_PROJECT), and the project this tier stamps on a connector
        # container is that same name.
        assert_no_connector_containers "local" curie
    fi

    echo
    echo "=== curie local message --json ==="
    # Re-probe: the precheck above ran before `local up` and `local deploy`,
    # potentially minutes ago, and the stub binds the port only now.
    assert_stub_port_free
    local out healthy_before="$WORKDIR/otel-before-healthy.json"
    if (( LOCAL_OTEL_SINK_ACTIVE )); then
        # This is the falsifiable half of the "no turns" distinction: the
        # platform has already exported ordinary metric points, yet no turn
        # counter has moved. A dead/zero-export pipeline cannot pass it.
        assert_local_otel_no_turn_pipeline_live
        local_otel_write_snapshot "$healthy_before"
    fi
    # `|| true`: the timeout shape exits non-zero, and the assertion helper is
    # what must classify it, not set -e.
    # Exercise one API span/log after the baseline. Pinning the channel on the
    # message still keeps the turn's causal trace independent of API lookup.
    curl -fsS "${CURIE_API_URL:-http://localhost:28000}/health" >/dev/null
    # The credential-shaped fake user reaches an args-style runner log and the
    # closed user correlation span attribute. The sink assertion below proves
    # neither dangerous path exports it; the prompt also proves content stays
    # out of telemetry.
    out="$("$BIN" --json local message --channel C0LOCALDEV --user "$OTEL_E2E_SECRET_SENTINEL" "$PROMPT $OTEL_E2E_SECRET_SENTINEL" || true)"
    printf '%s\n' "$out"
    assert_finalized_reply "local" "$out"

    if (( LOCAL_OTEL_SINK_ACTIVE )); then
        assert_local_otel_healthy_turn "$healthy_before"
        case_local_otel_runner_failure
    fi

    echo
    echo "=== curie local eval --dry-run (suite parity) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    assert_suite "local" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval ==="
        (cd "$WORKDIR/bundle" && "$BIN" "${eval_args[@]}")
    fi

    if (( LOCAL_STACK_OWNED )); then
        echo
        echo "=== curie local down ==="
        "$BIN" local down
        LOCAL_STACK_OWNED=0
        stop_local_otel_sink

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
        if connector_mode; then
            assert_connectors_reaped "local"
        fi
    fi

    # Last, not at the deploy step: see assert_bundle_identity's comment. The
    # rung's suite and mode evidence is on the transcript by now, so a divergence
    # here is diagnosable as an identity divergence specifically.
    assert_bundle_identity "local" "$digest"
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
    RAN_RUNGS="$RAN_RUNGS local-release"

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
    # the selected profile's images and
    # only the curie-owned ones: postgres/valkey/rustfs are public and pulled
    # on demand same as rung_local already assumes.
    local compose_profile="core"
    if [[ -f "$WORKDIR/bundle-release/evals/trajectory.json" ]]; then
        compose_profile="full"
    fi
    local missing=0 image
    while IFS= read -r image; do
        [[ "$image" == ghcr.io/curie-eng/curie-* ]] || continue
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            echo "error: image '$image' is required by compose.release.yaml's $compose_profile profile and is not present locally." >&2
            missing=1
        fi
    done < <(docker compose -f "$release_compose" \
        --profile "$compose_profile" --profile slack config --images)
    if (( missing )); then
        echo "fix: build and tag the missing image(s) locally under the tag compose.release.yaml pins (see .github/workflows/ci.yaml's e2e-ladder job for the exact build+tag steps CI uses), then re-run." >&2
        return 1
    fi

    assert_stub_port_free

    if [[ -n "$(docker ps -q --filter 'name=curie-api' 2>/dev/null)" ]]; then
        # Reuse it and do NOT tear it down, matching rung_local's rule: the
        # thread that brought a stack up owns tearing it down.
        echo "a compose stack is already running; reusing it and leaving teardown to whoever started it"
        # Same rule as rung_local, and changed together with it: the reused
        # stack's mode is verified by assert_model_mode below rather than
        # disclaimed in a warning.
        echo "note: the reused stack's model mode was fixed by whoever ran \`local up\`; it is verified below against this run's mode, and a mismatch fails this rung."
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
        local up_args=(local up -f "$release_compose")
        if [[ "$compose_profile" == "core" ]]; then
            up_args+=(--minimal)
        fi
        echo "=== curie ${up_args[*]} ==="
        LOCAL_STACK_OWNED=1
        "$BIN" "${up_args[@]}"
    fi

    echo
    echo "=== curie --json local deploy (release-compose stack) ==="
    # A separate bundle copy from rung_local's, never the same directory: deploy
    # records state into the bundle dir, and reusing rung_local's copy here
    # would carry over its recorded agent/version ids instead of a fresh
    # cold-start deploy. The copies now pack to the same digest by construction
    # (their regular-file mtimes are normalized where they are created), so a
    # separate copy no longer means a separate identity.
    local deploy_json digest agent_id agent_name deployment_id
    deploy_json="$("$BIN" --json local deploy --plugin-dir "$WORKDIR/bundle-release")"
    printf '%s\n' "$deploy_json"
    digest="$(deploy_field "local-release" "$deploy_json" bundle.sha256)"
    agent_id="$(deploy_field "local-release" "$deploy_json" agent.id)"
    agent_name="$(deploy_field "local-release" "$deploy_json" agent.name)"
    deployment_id="$(deploy_field "local-release" "$deploy_json" deployment.id)"

    echo
    echo "=== assert the turn will bind to the deployment this rung created ==="
    # Normally trivially satisfied here, because this rung's pre-`up`
    # `local down --wipe` above clears the deployment rows before it deploys. It
    # is still asserted, because that wipe is skipped entirely when the rung
    # reuses a running stack -- which is exactly the case where a shadow exists.
    assert_sole_active_deployment "local-release" "$agent_id" "$deployment_id"

    echo
    echo "=== assert the deployed worker's effective model mode ==="
    local observed_mode
    observed_mode="$(probe_local_fake_model)"
    assert_model_mode "local-release" "$observed_mode"

    echo
    echo "=== assert the bundle's connectors (ADR 0113) ==="
    if connector_mode; then
        local worker
        worker="$(local_worker_container)"
        assert_connector_parity "local-release" docker \
            "$(container_env_value "$worker" CURIE_RELEASE)" \
            "$agent_name" \
            "$(container_env_value "$worker" CURIE_NAMESPACE)"
    elif bundle_declares_connectors; then
        # Same dual as rung 2, against the release compose file: a declaring
        # bundle must host its connectors here too, since the generated file
        # shares the pinned project name and the same delivery overlay.
        local worker
        worker="$(local_worker_container)"
        assert_declared_connectors_hosted "local-release" curie \
            "$(container_env_value "$worker" CURIE_RELEASE)" "$agent_name"
    else
        # Same compose project as rung 2: the release compose file the CLI
        # generates carries the same pinned project name.
        assert_no_connector_containers "local-release" curie
    fi

    echo
    echo "=== curie local message --json (release-compose stack) ==="
    assert_stub_port_free
    local out
    out="$("$BIN" --json local message "$PROMPT" || true)"
    printf '%s\n' "$out"
    assert_finalized_reply "local-release" "$out"

    echo
    echo "=== curie local eval --dry-run (suite parity, release compose stack) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle-release/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle-release/evals/cases.json")
    fi
    assert_suite "local-release" "$(cd "$WORKDIR/bundle-release" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval (release compose stack) ==="
        (cd "$WORKDIR/bundle-release" && "$BIN" "${eval_args[@]}")
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
        if connector_mode; then
            assert_connectors_reaped "local-release"
        fi
    fi

    assert_bundle_identity "local-release" "$digest"
}

# Rung 3: the deployed release. Requires one to already exist; it is never
# installed or torn down here, because the cluster is shared.
rung_cluster() {
    echo
    echo "########## rung 3/3: cluster ##########"
    RAN_RUNGS="$RAN_RUNGS cluster"

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
    echo "=== curie --json cluster deploy ==="
    # No --api-url: deploy auto-discovers the release's UI /api proxy over
    # NodePort. No --secret: it is declined at this tier by design (#440).
    # --json for the receipt: `cluster status --json` carries no digest, so the
    # deploy receipt's bundle.sha256 is the only artifact-identity surface here
    # too.
    local deploy_json digest agent_id agent_name deployment_id deployment_status deployment_environment
    deploy_json="$("$BIN" --json cluster deploy --plugin-dir "$WORKDIR/bundle")"
    printf '%s\n' "$deploy_json"
    digest="$(deploy_field "cluster" "$deploy_json" bundle.sha256)"
    agent_id="$(deploy_field "cluster" "$deploy_json" agent.id)"
    agent_name="$(deploy_field "cluster" "$deploy_json" agent.name)"
    deployment_id="$(deploy_field "cluster" "$deploy_json" deployment.id)"
    deployment_status="$(deploy_field "cluster" "$deploy_json" deployment.status)"
    deployment_environment="$(deploy_field "cluster" "$deploy_json" deployment.environment)"

    echo
    echo "=== assert this rung's deployment is the terminal state a turn can bind to ==="
    # The receipt's own terminal state first: it is readable at every tier, with
    # no API credential at all.
    if [[ "$deployment_status" != "active" ]]; then
        echo "cluster: the deployment this rung created reports status '$deployment_status', not 'active', so no turn can bind to it." >&2
        return 1
    fi
    if [[ "$deployment_environment" != "dev" ]]; then
        echo "cluster: the deployment this rung created landed in environment '$deployment_environment'; this run asked for 'dev' (deploy's own default, and the ladder passes no --environment)." >&2
        return 1
    fi
    echo "cluster: deploy receipt reports an active deployment in environment $deployment_environment"

    # The full runtime-binding assertion is CONDITIONAL here, and only here,
    # because the cluster API key has no default -- it is resolved from the
    # installed release (cli/src/main.rs's --api-key value_parser) -- and CI's
    # cluster ladder job supplies none, so requiring it would red a job this
    # change cannot touch.
    #
    # So: when the operator's environment already carries CURIE_API_KEY, this
    # rung performs exactly the same one-active-deployment assertion the local
    # rungs perform, through the same helper, as a hard failure. When it does
    # not, the rung does not fail and does not claim parity: the receipt above
    # proves which artifact was UPLOADED, nothing proves the turn RAN it, and the
    # summary reports the two as separate claims.
    if [[ -n "${CURIE_API_KEY:-}" ]]; then
        assert_sole_active_deployment "cluster" "$agent_id" "$deployment_id"
        CLUSTER_BINDING_PROVEN=1
    else
        echo "cluster: runtime binding is NOT proven at this tier for this run. The deploy receipt proves which artifact was UPLOADED to the cluster; it does not prove the turn below ran that artifact, so a stale ACTIVE prod deployment shadowing the turn would go undetected here."
        echo "cluster: to prove it, export CURIE_API_KEY (and CURIE_API_URL if the release's API is not on the default) and re-run; this rung then reads the active deployment set exactly as the local rungs do."
    fi

    echo
    echo "=== assert the installed release's effective model mode ==="
    # kubectl is only reached inside this rung, which already gated on a
    # reachable release above, so a skill/local-only invocation never needs it.
    local observed_mode
    observed_mode="$(probe_cluster_fake_model)"
    assert_model_mode "cluster" "$observed_mode"

    if connector_mode; then
        echo
        echo "=== assert the bundle's connectors (ADR 0113) ==="
        # Read off the installed release's own worker, the same deployment
        # probe_cluster_fake_model reads, so the scope is the one the cluster
        # actually hands the runner rather than a ladder assumption. The
        # namespace is genuinely different here from the skill and local rungs
        # -- it is the namespace the release is installed into -- which is why
        # the pinned entry set excludes it.
        local cluster_release cluster_namespace
        cluster_release="$(kubectl -n curie get deployment/curie-worker \
            -o 'jsonpath={.spec.template.spec.containers[*].env[?(@.name=="CURIE_RELEASE")].value}')"
        cluster_namespace="$(kubectl -n curie get deployment/curie-worker \
            -o 'jsonpath={.spec.template.spec.containers[*].env[?(@.name=="CURIE_NAMESPACE")].value}')"
        assert_connector_parity "cluster" kubectl "$cluster_release" "$agent_name" "$cluster_namespace"
        case_connector_registry_missing_cluster "$cluster_release" "$agent_name" "$cluster_namespace"
    fi

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

    echo
    echo "=== curie cluster eval --dry-run (suite parity) ==="
    # ONE array feeding BOTH cluster eval calls (this dry-run plan and the live
    # grade below), so `--listen-host` cannot reach one and be forgotten on the
    # other. `--json` is deliberately NOT in the array: both call sites need it
    # for DIFFERENT reasons -- a machine-readable `--dry-run` plan here, an
    # auditable green on the live grade below -- and passing it once per call site
    # is what keeps it from being passed twice at either.
    local eval_args=(cluster eval)
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
        eval_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
    fi
    assert_suite "cluster" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie cluster eval (REPORT ONLY, does not fail this rung: #1603) ==="
        # --json on this rung's live eval and no other rung's: when a case
        # verdict exists, the payload carries its output whether the verdict is
        # green or red. That made the old regex greens auditable from the job
        # log (#1602).
        #
        # Report only on THIS rung alone (#1603). Trajectory records a tool
        # request before execution, so a denied or failed WebFetch can still
        # satisfy identity and order. Fetch success remains unproved, and this
        # oracle cannot distinguish an attempt from successful execution.
        # Fatal grading requires proof that fetch succeeds, not merely that it
        # was requested. Post trajectory run 32347598515 stopped before a case
        # verdict because its result had no resolved model. That was #1709,
        # fixed by PR #1715 on main and pending the normal forward merge to
        # next, so the run is historical evidence rather than the remaining
        # reason for report only. The skill and local rungs passed their
        # trajectory grades in that run, which proves request sequence rather
        # than fetch success. The local release rung did not reach eval because
        # its UI image was missing, so it supplied no grade evidence.
        # The rung is NOT blind either way: the plumbing assertions above (the
        # turn finalizes with a reply, and under live mode that reply is not the
        # fake sentinel) still fail it, and they are what caught the sandbox
        # reaper race in #1601.
        if ! (cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}"); then
            echo "cluster: eval did not produce a passing grade. Not failing the rung: this eval is report only (#1603)." >&2
        fi
    fi

    assert_bundle_identity "cluster" "$digest"
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

# The connector rung's inputs, and the ORDER is load-bearing. The fixture and
# the lock are packed like any other bundle file, so both must land before the
# mtime normalization below and before any rung packs -- otherwise the copies
# pack to different bytes and every multi-rung run is red by construction.
if connector_mode; then
    echo
    echo "=== connector bundle: fixture, credentials, build ==="
    if (( RUN_CLUSTER )) && [[ -z "$CONNECTOR_REGISTRY" ]]; then
        echo "error: the cluster rung is named and CURIE_E2E_CONNECTOR_REGISTRY is unset." >&2
        echo "why: without a registry the build delivers into the local Docker daemon, and a cluster deploy refuses a local-daemon lock by design -- the nodes cannot pull it." >&2
        echo "fix: export CURIE_E2E_CONNECTOR_REGISTRY=<ref you can push to>, or drop cluster from CURIE_E2E_TIERS." >&2
        exit 1
    fi
    prepare_connector_bundle "$WORKDIR/bundle"
    prepare_connector_bundle "$WORKDIR/bundle-release"
    provision_connector_credentials
    write_connector_probe
    # ONE build, and its lock is COPIED to the second copy rather than built
    # again: a second build would resolve its own image reference, and the two
    # copies would then pack to different bytes. Every rung consumes this one
    # lock unchanged, which is what makes the digest assertion meaningful.
    build_connector_images "$WORKDIR/bundle"
    cp "$WORKDIR/bundle/connectors.lock.yaml" "$WORKDIR/bundle-release/connectors.lock.yaml"
fi

# Normalize every regular file's mtime across both copies. This is load-bearing,
# not hygiene: the digest every rung asserts on identifies an ARCHIVE, not a
# source tree, because pack_tar_gz embeds per-file mtime (cli/src/bundle.rs), and
# `cp -r` does NOT preserve mtimes. Two copies of identical content would
# therefore pack to different bytes and different digests, and every multi-rung
# run would be red by construction. Only regular files become tar entries (the
# packer recurses directories and appends only files), and uid/gid are constant
# inside one ladder process, so pinning file mtimes to a fixed epoch is exactly
# sufficient to make both copies pack byte-identically.
find "$WORKDIR/bundle" "$WORKDIR/bundle-release" -type f -exec touch -t 200001010000 {} +

# The one place the expected suite is derived, from the file the ladder itself
# packed, so no rung can be compared against an externally pinned value. The
# case ids are recorded for the summary; they are PROVEN at the deployed tiers by
# digest equality (evals/cases.json is inside the packed archive), and read back
# directly only at the skill rung and, under CURIE_E2E_LIVE=1, at every rung.
CASES_FILE="$WORKDIR/bundle/evals/cases.json"
if [[ ! -f "$CASES_FILE" ]]; then
    echo "error: the ladder's bundle carries no evals/cases.json at $CASES_FILE, so there is no common suite to assert." >&2
    echo "fix: point BUNDLE_SRC at a bundle that ships an eval suite." >&2
    exit 1
fi
{
    read -r EXPECT_SUITE
    read -r EXPECT_CASE_COUNT
    read -r EXPECT_CASE_IDS
} < <(python3 -c '
import json, sys
suite = json.load(open(sys.argv[1]))
ids = sorted(case["id"] for case in suite["cases"])
print(suite["name"])
print(len(ids))
print(",".join(ids))
' "$CASES_FILE")
echo
echo "=== common bundle and suite ==="
echo "bundle source: $BUNDLE_SRC"
echo "suite: \"$EXPECT_SUITE\" with $EXPECT_CASE_COUNT case(s)"
echo "case ids: $EXPECT_CASE_IDS"

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
echo "=== parity summary (what this run actually proved) ==="
echo "rungs run:$RAN_RUNGS"
if [[ -z "$PARITY_DIGEST" ]]; then
    echo "bundle identity: NOT reported by any rung that ran, so nothing about artifact identity was proven."
else
    echo "bundle identity: $PARITY_DIGEST, reported by rung(s):$PARITY_RUNGS"
    if (( $(wc -w <<< "$PARITY_RUNGS") < 2 )); then
        echo "note: only one rung reported a digest, so the CROSS-RUNG digest comparison was vacuous for this tier set -- only that rung's own identity was recorded. This is NOT a cross-rung parity claim."
    fi
fi
if connector_mode; then
    if [[ -z "$CONNECTOR_ENTRIES" ]]; then
        echo "connectors: NOT asserted by any rung that ran, so nothing about connector hosting was proven."
    else
        echo "connectors: hosted and serving MCP at rung(s):$CONNECTOR_ENTRY_RUNGS, on the entries"
        while IFS= read -r entry; do
            [[ -n "$entry" ]] && echo "  $entry"
        done <<< "$CONNECTOR_ENTRIES"
        echo "connectors: the entry set above is the object name, port and path -- the part both sides derive independently. The namespace differs by tier by design and is printed per rung above."
        if (( $(wc -w <<< "$CONNECTOR_ENTRY_RUNGS") < 2 )); then
            echo "note: only one rung asserted connectors, so the CROSS-RUNG comparison was vacuous for this tier set. This is NOT a cross-tier connector parity claim."
        fi
        echo "connectors: the gated write verb was NOT exercised at any rung -- an approval round trip needs a second human actor. Read this as hosting parity only."
    fi
fi
echo "suite: \"$EXPECT_SUITE\" with $EXPECT_CASE_COUNT case(s), resolved by the tier's own loader at rung(s):$SUITE_RUNGS"
echo "case ids: $EXPECT_CASE_IDS (proven at local/cluster by digest equality; read back directly at skill, and at every rung under CURIE_E2E_LIVE=1)"
echo "model mode read off the deployed artifact at rung(s):${MODE_RUNGS:- none}"
if [[ "$LIVE" == "1" ]]; then
    echo "grading: GRADED rungs:$RAN_RUNGS -- each ran its tier's own evaluator against a real model."
else
    echo "grading: PLUMBING-ONLY rungs:$RAN_RUNGS -- sealed against the fake model, so no rung graded reply content and a green here must never be read as a graded pass (ADR-0055, #612)."
fi
if (( RUN_CLUSTER )); then
    if (( CLUSTER_BINDING_PROVEN )); then
        echo "cluster rung: upload identity AND runtime binding both proven -- exactly one active deployment existed for this agent before the turn, and it was the one this rung created, so the turn could only bind to the artifact whose digest is reported above."
    else
        echo "cluster rung: upload-identity-proven / runtime-binding-UNPROVEN. The digest above is the artifact this rung UPLOADED to the cluster; no read was performed to show the turn ran it, so a stale ACTIVE prod deployment shadowing the turn would have gone undetected. Read that digest as an upload record, never as a cluster parity claim, and see the cluster rung's own note above for how to prove it."
    fi
fi

echo
echo "LADDER PASS (tiers: $TIERS)"
