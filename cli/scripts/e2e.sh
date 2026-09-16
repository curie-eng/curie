#!/bin/bash
# Scripted E2E for the curie CLI (task I1 done-when).
#
# Round-trips a synthetic event through a real local runner container with zero
# Slack involved: init a bundle, start the runner (fake model, offline by
# default; live model under CURIE_E2E_LIVE=1), send a message and stream the
# NDJSON reply, run the eval cases, stop. This is rung 1 (skill) of the
# cold-start parity ladder (issue #690, cli/scripts/e2e-ladder.sh); the
# ladder's local rung (`local deploy` -> `local message` with a real reply
# assertion) covers deploying a bundle against a running platform API, so this
# script no longer does so itself (issue #694).
#
# Requirements: docker, a curie-runner image (build per runner/README.md),
# and a cargo toolchain (or $CURIE_BIN). Run from anywhere:
#
#   bash cli/scripts/e2e.sh
#
# Env knobs:
#   CURIE_E2E_IMAGE     runner image (default curie-runner)
#   CURIE_E2E_PORT      host port (default 7245)
#   CURIE_E2E_NETWORK   docker network to join (e.g. curie_default)
#   CURIE_E2E_OTEL      OTLP endpoint (e.g. http://otel-collector:4318)
#   CURIE_E2E_LIVE      1 = real model, requiring a credential in the
#                         environment (ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN,
#                         or CURIE_CREDENTIALS); default 0 runs the runner's
#                         scripted fake model, offline and credential-free. This
#                         is the SAME env var cli/scripts/e2e-ladder.sh sets for
#                         its own local and cluster rungs, so a single
#                         CURIE_E2E_LIVE=1 now runs every rung live.
#   CURIE_BIN           path to a prebuilt curie binary (skip cargo build)
#   CURIE_E2E_BUNDLE    absolute path to an EXISTING bundle directory to drive
#                         instead of scaffolding one. Unset by default, and
#                         unset this script behaves exactly as it always has.
#                         cli/scripts/e2e-ladder.sh sets it so rung 1 drives the
#                         same artifact and the same case set as every later
#                         rung, which is what makes the ladder's cross-rung
#                         identity comparison mean anything. A value that is not
#                         a directory is a hard error, never a silent fallback to
#                         the scaffold.
set -euo pipefail

CLI_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${CURIE_E2E_IMAGE:-curie-runner}"
PORT="${CURIE_E2E_PORT:-7245}"
CONTAINER="curie-e2e-runner"
LIVE="${CURIE_E2E_LIVE:-0}"

echo "=== Resolve the curie binary ==="
if [[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN:-}" ]]; then
    # Absolutize: this script cd's into a scaffolded bundle directory before
    # invoking the binary, so a relative $CURIE_BIN (as the ladder and CI
    # pass) must be pinned to an absolute path here or it stops resolving
    # after the cd.
    BIN="$(cd "$(dirname "$CURIE_BIN")" && pwd)/$(basename "$CURIE_BIN")"
    echo "using prebuilt binary: $BIN"
else
    (cd "$CLI_DIR" && cargo build --release --quiet)
    BIN="$CLI_DIR/target/release/curie"
fi
"$BIN" --version

echo
echo "=== Resolve model mode ==="
if [[ "$LIVE" == "1" ]]; then
    if [[ -z "${ANTHROPIC_API_KEY:-}" && -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" && -z "${CURIE_CREDENTIALS:-}" ]]; then
        echo "error: CURIE_E2E_LIVE=1 needs a model credential in the environment, and none is set." >&2
        echo "fix: export ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN, or CURIE_CREDENTIALS, or drop CURIE_E2E_LIVE to run sealed against the fake model." >&2
        exit 1
    fi
    echo "model mode: LIVE (real model; \`skill up\` forwards the ambient credential)"
else
    echo "model mode: FAKE (sealed; --fake-model, offline, no credential)"
fi

WORKDIR="$(mktemp -d)"

# Crash-safety state for the #1087 immutability proof further down, which
# deliberately MUTATES a SKILL.md inside the bundle under test. Declared here,
# BEFORE the trap is installed, because the trap reads them: under `set -u` a
# trap that fires before these are assigned would itself die on an unbound
# variable and skip the whole teardown it exists to perform.
SKILL_MUTATION_PENDING=0
SKILL_FILE_PATH=""
SKILL_FILE_BACKUP=""

cleanup() {
    # Capture the real status first: everything below is best-effort, and a
    # teardown command's own status must not become this script's exit code.
    local rc=$?
    # Restore BEFORE the `rm -rf` below, because the `cp -p` backup lives under
    # $WORKDIR: deleting the workdir first would destroy the only copy that can
    # put the caller's file back. The window this covers is every failure path
    # between the append and the successful restore -- and with
    # CURIE_E2E_BUNDLE set that file belongs to a directory this script does NOT
    # own, so leaving the marker (and, worse, a shifted mtime, which
    # `pack_tar_gz` folds into the bundle digest) would corrupt the caller's
    # bundle and make a LATER ladder run fail as if the product were broken.
    if [[ "$SKILL_MUTATION_PENDING" == "1" ]]; then
        if [[ -f "$SKILL_FILE_BACKUP" ]]; then
            cp -p "$SKILL_FILE_BACKUP" "$SKILL_FILE_PATH" || true
            echo "cleanup: restored $SKILL_FILE_PATH from the pre-mutation backup (content and mtime)" >&2
        else
            echo "cleanup: WARNING -- $SKILL_FILE_PATH was mutated but its backup is gone; the file still carries the E2E marker." >&2
        fi
        SKILL_MUTATION_PENDING=0
    fi
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [[ "$rc" -ne 0 && -n "${CURIE_E2E_BUNDLE:-}" ]]; then
        # Failure path against a caller-supplied bundle only. `skill up` writes
        # `.curie/runner.json` plus a materialized `.curie/snapshots/<digest>/`
        # inside the bundle directory, and the success path's final `skill down`
        # releases them (and asserts the snapshot root is empty). On a failure
        # that never reaches it, this removes them directly rather than calling
        # `skill down`: the container is already force-removed above, so the
        # only thing left to release is these files, and the failure may well BE
        # a broken `skill down`. Scoped to the caller-supplied case because the
        # scaffolded bundle lives inside $WORKDIR, which the `rm -rf` below
        # already takes, and to a non-zero exit so the success path is untouched.
        rm -rf "${CURIE_E2E_BUNDLE}/.curie"
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# These are every model credential and model routing input recognized by the
# current CLI, plus the deprecated cluster credential alias. The empty private
# store below is the other half of the negative control: making credential
# discovery mandatory must make this first observation fail rather than quietly
# borrowing a credential from the host running the E2E.
TRY_ENV_UNSET=(
    -u CURIE_CREDENTIALS
    -u CURIE_MODEL_CREDENTIALS
    -u CLAUDE_CODE_OAUTH_TOKEN
    -u ANTHROPIC_API_KEY
    -u CURIE_MODEL_BASE_URL
    -u ANTHROPIC_BASE_URL
    -u CURIE_MODEL
    -u CURIE_FAKE_MODEL
)
TRY_RUNNER_CONTAINER="curie-runner-local"

# `try` uses the product's standard local runner name. Refuse to exercise any
# try path when it already belongs to somebody else; the EXIT trap only
# removes the E2E-specific runner, never this shared name.
if docker inspect "$TRY_RUNNER_CONTAINER" >/dev/null 2>&1; then
    echo "error: try E2E needs $TRY_RUNNER_CONTAINER to be absent before it starts." >&2
    echo "fix: stop the existing local runner, then rerun this E2E." >&2
    exit 1
fi

echo
echo "=== curie try (clean keyless first run, closed stdin) ==="
TRY_ROOT="$WORKDIR/try-clean"
TRY_CONFIG_DIR="$TRY_ROOT/config"
TRY_TMPDIR="$TRY_ROOT/tmp"
mkdir -p "$TRY_CONFIG_DIR" "$TRY_TMPDIR"
TRY_JSON="$(
    cd "$TRY_ROOT"
    env "${TRY_ENV_UNSET[@]}" \
        TMPDIR="$TRY_TMPDIR" \
        CURIE_CONFIG_DIR="$TRY_CONFIG_DIR" \
        "$BIN" --json try </dev/null
)"
printf '%s\n' "$TRY_JSON"
printf '%s' "$TRY_JSON" | python3 -c '
import json, sys

raw = sys.stdin.read()
try:
    payload = json.loads(raw)
except json.JSONDecodeError as error:
    sys.exit(f"error: curie --json try must emit exactly one JSON value: {error}")
if not isinstance(payload, dict):
    sys.exit("error: curie --json try must emit one JSON object")
if "all done" not in payload.get("reply", ""):
    sys.exit(f"error: keyless curie try did not return the fake reply: {payload!r}")
if payload.get("status") != "done":
    sys.exit(f"error: keyless curie try did not finish done: {payload!r}")
if payload.get("finalized") is not True:
    sys.exit(f"error: keyless curie try did not emit a finalized reply: {payload!r}")
print("confirmed: keyless curie try emitted exactly one finalized reply object")
'
if [[ -e "$TRY_ROOT/curie-demo" ]]; then
    echo "error: plain \`curie try\` retained $TRY_ROOT/curie-demo; only --keep may create it." >&2
    exit 1
fi
if [[ -n "$(find "$TRY_TMPDIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "error: plain \`curie try\` left its disposable scaffold under $TRY_TMPDIR." >&2
    exit 1
fi
echo "confirmed: plain curie try left no ./curie-demo or temporary scaffold"

echo
echo "=== curie try (discovered credential, safely rejected) ==="
TRY_CREDENTIAL_ROOT="$WORKDIR/try-credential"
TRY_CREDENTIAL_CONFIG_DIR="$TRY_CREDENTIAL_ROOT/config"
TRY_CREDENTIAL_TMPDIR="$TRY_CREDENTIAL_ROOT/tmp"
TRY_CREDENTIAL_STDOUT="$TRY_CREDENTIAL_ROOT/stdout"
TRY_CREDENTIAL_STDERR="$TRY_CREDENTIAL_ROOT/stderr"
TRY_CREDENTIAL_NAME="ANTHROPIC_API_KEY"
TRY_CREDENTIAL_VALUE="curie-e2e-fake-credential-value"
mkdir -p "$TRY_CREDENTIAL_CONFIG_DIR" "$TRY_CREDENTIAL_TMPDIR"

if (
    cd "$TRY_CREDENTIAL_ROOT"
    env "${TRY_ENV_UNSET[@]}" \
        TMPDIR="$TRY_CREDENTIAL_TMPDIR" \
        CURIE_CONFIG_DIR="$TRY_CREDENTIAL_CONFIG_DIR" \
        "$TRY_CREDENTIAL_NAME=$TRY_CREDENTIAL_VALUE" \
        "$BIN" try </dev/null >"$TRY_CREDENTIAL_STDOUT" 2>"$TRY_CREDENTIAL_STDERR"
); then
    echo "error: curie try accepted the E2E's deliberately fake discovered credential." >&2
    exit 1
fi
if ! grep -qF "$TRY_CREDENTIAL_NAME" "$TRY_CREDENTIAL_STDOUT" && \
   ! grep -qF "$TRY_CREDENTIAL_NAME" "$TRY_CREDENTIAL_STDERR"; then
    echo "error: curie try rejected the credential without naming its discovered source." >&2
    exit 1
fi
if grep -qF "$TRY_CREDENTIAL_VALUE" "$TRY_CREDENTIAL_STDOUT" "$TRY_CREDENTIAL_STDERR"; then
    echo "error: curie try exposed a discovered credential value in its output." >&2
    exit 1
fi
if [[ -n "$(find "$TRY_CREDENTIAL_TMPDIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "error: rejected credential run left its disposable scaffold under $TRY_CREDENTIAL_TMPDIR." >&2
    exit 1
fi
if docker inspect "$TRY_RUNNER_CONTAINER" >/dev/null 2>&1; then
    echo "error: rejected credential run left its local runner behind." >&2
    exit 1
fi
echo "confirmed: discovered credential is named but never exposed, then its scaffold and runner are removed"

echo
echo "=== curie try --keep (graduate into normal skill commands) ==="
TRY_KEEP_ROOT="$WORKDIR/try-keep"
TRY_KEEP_CONFIG_DIR="$TRY_KEEP_ROOT/config"
TRY_KEEP_PROJECT="$TRY_KEEP_ROOT/curie-demo"
mkdir -p "$TRY_KEEP_CONFIG_DIR"
TRY_KEEP_OUTPUT="$(
    cd "$TRY_KEEP_ROOT"
    env "${TRY_ENV_UNSET[@]}" \
        CURIE_CONFIG_DIR="$TRY_KEEP_CONFIG_DIR" \
        "$BIN" try --keep </dev/null 2>&1
)"
printf '%s\n' "$TRY_KEEP_OUTPUT"
if ! printf '%s' "$TRY_KEEP_OUTPUT" | grep -qF "all done"; then
    echo "error: keyless \`curie try --keep\` did not return the fake runner reply." >&2
    exit 1
fi
if [[ ! -f "$TRY_KEEP_PROJECT/.claude-plugin/plugin.json" ]]; then
    echo "error: \`curie try --keep\` did not retain the standard plugin manifest." >&2
    exit 1
fi
python3 - "$TRY_KEEP_PROJECT/.claude-plugin/plugin.json" <<'PY'
import json, sys

with open(sys.argv[1], encoding="utf-8") as handle:
    manifest = json.load(handle)
if manifest.get("name") != "curie-demo":
    raise SystemExit(f"error: retained scaffold has the wrong plugin name: {manifest!r}")
print("confirmed: --keep retained the standard curie-demo scaffold")
PY

(
    cd "$TRY_KEEP_PROJECT"

    echo
    echo "=== graduated project: curie skill up --fake-model ==="
    "$BIN" skill up --fake-model

    echo
    echo "=== graduated project: curie skill status --json ==="
    GRADUATED_STATUS_JSON="$("$BIN" skill status --json)"
    printf '%s\n' "$GRADUATED_STATUS_JSON"
    printf '%s' "$GRADUATED_STATUS_JSON" | python3 -c '
import json, sys

payload = json.load(sys.stdin)
digest = payload.get("bundle_digest")
if not isinstance(digest, str) or not digest:
    sys.exit(f"error: graduated project status has no bundle digest: {payload!r}")
print(f"confirmed: graduated project bundle digest is {digest}")
'

    echo
    echo "=== graduated project: curie skill message ==="
    GRADUATED_REPLY="$("$BIN" skill message "hello again")"
    printf '%s\n' "$GRADUATED_REPLY"
    if ! printf '%s' "$GRADUATED_REPLY" | grep -qF "all done"; then
        echo "error: the graduated project did not return the fake runner reply." >&2
        exit 1
    fi

    echo
    echo "=== graduated project: curie skill down ==="
    "$BIN" skill down
    if [[ -e .curie/runner.json ]]; then
        echo "error: graduated project teardown left .curie/runner.json behind." >&2
        exit 1
    fi
)
if [[ ! -d "$TRY_KEEP_PROJECT" ]]; then
    echo "error: normal skill commands removed the graduated curie-demo project." >&2
    exit 1
fi
echo "confirmed: normal skill up, status, message, and down operate on the retained project"

echo
echo "=== curie try --keep (refuse an existing graduated project) ==="
TRY_KEEP_MANIFEST="$TRY_KEEP_PROJECT/.claude-plugin/plugin.json"
TRY_KEEP_MANIFEST_SHA256="$(sha256sum "$TRY_KEEP_MANIFEST" | awk '{print $1}')"
if TRY_KEEP_COLLISION_OUTPUT="$(
    cd "$TRY_KEEP_ROOT"
    env "${TRY_ENV_UNSET[@]}" \
        CURIE_CONFIG_DIR="$TRY_KEEP_CONFIG_DIR" \
        "$BIN" try --keep </dev/null 2>&1
)"; then
    printf '%s\n' "$TRY_KEEP_COLLISION_OUTPUT" | sed 's/^/  /'
    echo "error: curie try --keep overwrote an existing curie-demo project." >&2
    exit 1
fi
printf '%s\n' "$TRY_KEEP_COLLISION_OUTPUT" | sed 's/^/  /'
if [[ "$(sha256sum "$TRY_KEEP_MANIFEST" | awk '{print $1}')" != "$TRY_KEEP_MANIFEST_SHA256" ]]; then
    echo "error: refused curie try --keep altered the existing plugin manifest." >&2
    exit 1
fi
echo "confirmed: a second --keep refuses and preserves the graduated plugin manifest"

echo
echo "=== curie try --keep (refuse an unowned nonempty directory) ==="
TRY_UNOWNED_ROOT="$WORKDIR/try-unowned"
TRY_UNOWNED_CONFIG_DIR="$TRY_UNOWNED_ROOT/config"
TRY_UNOWNED_PROJECT="$TRY_UNOWNED_ROOT/curie-demo"
mkdir -p "$TRY_UNOWNED_CONFIG_DIR" "$TRY_UNOWNED_PROJECT"
printf '%s\n' "user notes" > "$TRY_UNOWNED_PROJECT/notes.txt"
TRY_UNOWNED_SHA256="$(sha256sum "$TRY_UNOWNED_PROJECT/notes.txt" | awk '{print $1}')"
if TRY_UNOWNED_OUTPUT="$(
    cd "$TRY_UNOWNED_ROOT"
    env "${TRY_ENV_UNSET[@]}" \
        CURIE_CONFIG_DIR="$TRY_UNOWNED_CONFIG_DIR" \
        "$BIN" try --keep </dev/null 2>&1
)"; then
    printf '%s\n' "$TRY_UNOWNED_OUTPUT" | sed 's/^/  /'
    echo "error: curie try --keep wrote into an unowned nonempty directory." >&2
    exit 1
fi
printf '%s\n' "$TRY_UNOWNED_OUTPUT" | sed 's/^/  /'
if [[ "$(sha256sum "$TRY_UNOWNED_PROJECT/notes.txt" | awk '{print $1}')" != "$TRY_UNOWNED_SHA256" ]]; then
    echo "error: refused curie try --keep altered the user's file." >&2
    exit 1
fi
if [[ -e "$TRY_UNOWNED_PROJECT/.claude-plugin/plugin.json" ]]; then
    echo "error: refused curie try --keep created a plugin manifest in the unowned directory." >&2
    exit 1
fi
echo "confirmed: try --keep refuses an unowned nonempty directory and leaves user files untouched"

echo
echo "=== Resolve the bundle under test ==="
if [[ -n "${CURIE_E2E_BUNDLE:-}" ]]; then
    # Fail loudly rather than fall back: a typo silently re-scaffolding
    # deal-desk would re-open the very skill-vs-local bundle divergence the
    # ladder's identity comparison exists to close, and it would do so as a
    # green run.
    if [[ ! -d "$CURIE_E2E_BUNDLE" ]]; then
        echo "error: CURIE_E2E_BUNDLE is set to '$CURIE_E2E_BUNDLE', which is not a directory." >&2
        echo "fix: point it at an existing bundle directory, or unset it to scaffold deal-desk from the inline spec." >&2
        exit 1
    fi
    BUNDLE_DIR="$CURIE_E2E_BUNDLE"
    echo "bundle: supplied by the caller (CURIE_E2E_BUNDLE), scaffold skipped: $BUNDLE_DIR"
else
    BUNDLE_DIR="$WORKDIR/deal-desk"
    echo "bundle: scaffolded here from the inline agent spec: $BUNDLE_DIR"

    echo
    echo "=== curie init --from-spec (non-interactive, agent-authored spec) ==="
    # AC #2: a coding agent writes a spec, the CLI scaffolds a runnable bundle
    # from it with zero prompts, and the spec-scaffolded evals/cases.json runs on
    # the eval path. This leg is #325's acceptance evidence and is deliberately
    # kept as the STANDALONE behavior of this script -- the ladder supplies its
    # own common bundle instead, so the ladder run no longer carries #325's
    # scaffold evidence and a bare `bash cli/scripts/e2e.sh` (plus CI's own
    # --from-spec unit coverage) does.
    # The grader is falsifiable (it requires the agent to name itself), NOT
    # tuned to the fake model's canned "all done" reply: a grader written to match
    # the fake manufactures a green, and CI carrying one is exactly why #612 went
    # unnoticed. Under --fake-model the grader is never consulted at all -- the run
    # reports the non-graded plumbing_ok (ADR-0055).
    cat > "$WORKDIR/agent-spec.json" <<'EOF'
{
  "name": "deal-desk",
  "description": "Prices and reviews deal desk requests.",
  "skills": [
    {
      "name": "deal-desk",
      "description": "Invoke when a rep submits a pricing exception request.",
      "allowed_tools": ["WebSearch", "WebFetch"],
      "instructions": "Price the exception against the guardrails, then summarize the decision.\n"
    }
  ],
  "evals": [
    {
      "id": "introduces-itself",
      "input": "In one short sentence, introduce yourself as the deal-desk agent.",
      "grader": { "kind": "contains", "expected": "deal-desk", "case_sensitive": false }
    }
  ]
}
EOF
    "$BIN" init --from-spec "$WORKDIR/agent-spec.json" --dir "$BUNDLE_DIR"
fi

cd "$BUNDLE_DIR"

if [[ -z "${CURIE_E2E_BUNDLE:-}" ]]; then
    # Standalone only. Writing a file under evals/ into a CALLER-SUPPLIED bundle
    # would change the packed archive and therefore the bundle digest every
    # ladder rung asserts on (`.curie` is excluded from the pack,
    # cli/src/bundle.rs; `evals/` is not).
    #
    # A second suite for the explicit `--cases` leg. Its graders are real domain
    # graders, deliberately NOT matched to the fake-model script's canned "all done"
    # reply: this run is offline under --fake-model, so the graders are never
    # consulted and the suite reports plumbing_ok. Writing them to match the fake
    # would only re-create the bypass that let #612 ship green.
    cat > evals/e2e-cases.json <<'EOF'
{
  "name": "e2e",
  "cases": [
    {
      "id": "introduces-itself",
      "input": "In one short sentence, introduce yourself as the deal-desk agent.",
      "grader": { "kind": "contains", "expected": "deal-desk", "case_sensitive": false }
    },
    {
      "id": "names-its-domain",
      "input": "What kind of requests do you handle?",
      "grader": { "kind": "contains", "expected": "pricing", "case_sensitive": false }
    }
  ]
}
EOF
fi

echo
if [[ "$LIVE" == "1" ]]; then
    echo "=== curie skill up (live model) ==="
else
    echo "=== curie skill up (fake model, offline) ==="
fi
START_ARGS=(--plugin-dir . --image "$IMAGE" --port "$PORT" --name "$CONTAINER")
if [[ "$LIVE" == "1" ]]; then
    : # Real credential resolution (CURIE_CREDENTIALS, else the ambient SDK
      # creds) happens inside `skill up` itself once --fake-model is omitted;
      # see commands::select_passthrough_env.
else
    START_ARGS+=(--fake-model)
fi
if [[ -n "${CURIE_E2E_NETWORK:-}" ]]; then
    START_ARGS+=(--network "$CURIE_E2E_NETWORK")
fi
if [[ -n "${CURIE_E2E_OTEL:-}" ]]; then
    START_ARGS+=(--otel-endpoint "$CURIE_E2E_OTEL")
fi
UP_RC=0
UP_OUTPUT="$("$BIN" skill up "${START_ARGS[@]}" 2>&1)" || UP_RC=$?
printf '%s\n' "$UP_OUTPUT"
if (( UP_RC != 0 )); then
    echo "error: \`skill up\` exited $UP_RC." >&2
    if [[ -z "$UP_OUTPUT" ]]; then
        echo "error: \`skill up\` produced no diagnostic output." >&2
    fi
    exit "$UP_RC"
fi

# The boot panel must NAME the model path it resolved. `skill up` picks the
# credential at boot (commands::select_passthrough_env) but used to say nothing
# about it, so a first-run user with nothing exported got a clean panel and then
# `model-credential-rejected` from the very command that panel recommends -- one
# command after the CLI already knew. Asserted on the real run, not just in a
# unit test, because the failure mode being guarded is the row going missing
# from the panel rather than the summary function returning the wrong string.
if [[ "$LIVE" == "1" ]]; then
    EXPECT_MODEL='Model.*(CURIE_CREDENTIALS|ANTHROPIC_API_KEY|CLAUDE_CODE_OAUTH_TOKEN)'
else
    EXPECT_MODEL='Model.*fake \(offline'
fi
if ! printf '%s' "$UP_OUTPUT" | grep -qE "$EXPECT_MODEL"; then
    echo "error: \`skill up\` did not report the resolved model path in its boot panel." >&2
    echo "expected a 'Model' row matching /$EXPECT_MODEL/; see commands::model_credential_summary." >&2
    exit 1
fi
# ...and must not cry wolf: a credential IS resolved on both paths here, so the
# missing-credential warning appearing would make the real warning worthless.
if printf '%s' "$UP_OUTPUT" | grep -q "no model credential resolved"; then
    echo "error: \`skill up\` warned about a missing model credential on a run that has one." >&2
    exit 1
fi

echo
echo "=== curie skill status ==="
"$BIN" skill status

echo
echo "=== curie skill status --json (#1087: capture the initial bundle digest) ==="
STATUS_JSON_1="$("$BIN" skill status --json)"
printf '%s\n' "$STATUS_JSON_1"
DIGEST_1="$(printf '%s' "$STATUS_JSON_1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["bundle_digest"])')"
if [[ -z "$DIGEST_1" || "$DIGEST_1" == "None" ]]; then
    echo "error: expected \`skill status --json\` to report a non-null bundle_digest for a just-booted runner; got '$DIGEST_1'." >&2
    echo "payload: $STATUS_JSON_1" >&2
    exit 1
fi
echo "initial bundle digest: $DIGEST_1"

echo
echo "=== #1087 AC1: edit the source after boot, confirm the running container keeps the ORIGINAL snapshot ==="
# Derived from the bundle, not hardcoded to `deal-desk`: a caller-supplied
# bundle names its skill directory whatever it likes. First match in sorted
# order, so the choice is deterministic.
SKILL_FILE_PATH=""
while IFS= read -r CANDIDATE; do
    if [[ -z "$SKILL_FILE_PATH" ]]; then
        SKILL_FILE_PATH="$CANDIDATE"
    fi
done < <(find "$BUNDLE_DIR/skills" -mindepth 2 -maxdepth 2 -name SKILL.md | sort)
if [[ -z "$SKILL_FILE_PATH" ]]; then
    echo "error: no skills/*/SKILL.md exists under $BUNDLE_DIR, so the #1087 immutability proof has nothing to mutate." >&2
    exit 1
fi
# The same file inside the runner's read-only /plugin mount, so the mount check
# below follows the file this proof actually edited.
SKILL_FILE_REL="${SKILL_FILE_PATH#"$BUNDLE_DIR"/}"
# `cp -p` into $WORKDIR (outside the bundle, so it never enters the pack), never
# `$(cat ...)` plus `printf '%s\n'`: that round trip strips trailing newlines and
# adds exactly one back, which is byte-lossy for any file not ending in exactly
# one newline, and it gives the restored file a NEW mtime. Both matter now --
# `pack_tar_gz` embeds per-file mtime (cli/src/bundle.rs:181-184), so a lossy or
# mtime-shifting restore silently changes the bundle digest the ladder's later
# rungs assert against, and the failure would read as a product bug.
SKILL_FILE_BACKUP="$WORKDIR/skill-md-original"
cp -p "$SKILL_FILE_PATH" "$SKILL_FILE_BACKUP"
MARKER="e2e-post-boot-source-edit-$$"
# Claim the mutation BEFORE making it, never after: a failure (or a signal)
# inside the append itself is exactly the case that has to be recoverable, and a
# flag set afterwards would leave that window uncovered. The EXIT trap restores
# from $SKILL_FILE_BACKUP whenever this is still 1.
SKILL_MUTATION_PENDING=1
echo "$MARKER" >> "$SKILL_FILE_PATH"

MOUNTED_SKILL_MD="$(docker exec "$CONTAINER" cat "/plugin/$SKILL_FILE_REL")"
if grep -qF "$MARKER" <<<"$MOUNTED_SKILL_MD"; then
    echo "error: the running container's /plugin/$SKILL_FILE_REL contains the marker '$MARKER', added to the HOST source after boot." >&2
    echo "expected: the runner keeps executing the immutable snapshot it packed at \`skill up\` time, unaffected by a later host edit." >&2
    exit 1
fi
echo "confirmed: the running container's /plugin mount does not see the post-boot host edit ($SKILL_FILE_REL)"

STATUS_JSON_1B="$("$BIN" skill status --json)"
DIGEST_1B="$(printf '%s' "$STATUS_JSON_1B" | python3 -c 'import json,sys; print(json.load(sys.stdin)["bundle_digest"])')"
if [[ "$DIGEST_1B" != "$DIGEST_1" ]]; then
    echo "error: \`skill status --json\` reported bundle_digest '$DIGEST_1B' after a host-only source edit; expected it to stay '$DIGEST_1'." >&2
    echo "expected: the digest identifies the snapshot mounted at boot, not the current (mutated) source." >&2
    exit 1
fi
echo "confirmed: bundle_digest is unchanged by the post-boot host edit ($DIGEST_1)"

echo
echo "=== curie skill message (synthetic event, streamed NDJSON reply) ==="
"$BIN" skill message "@curie can we approve the Meridian deal at 18% discount?"

echo
echo "=== curie skill eval --json (the bundle's own evals/cases.json) ==="
# No --cases: exercise the bundle's own evals/cases.json. Standalone that is the
# suite the --from-spec scaffold wrote, proving spec -> bundle -> skill eval
# passes end to end offline (AC #2); under a caller-supplied bundle it is the
# ladder's common suite, which is what makes the suite common across rungs with
# no second env var. --json puts the payload on stdout and the human report on
# stderr, so ONE run gives both the readable output and the machine-readable
# rows the assertions below read.
EVAL_JSON="$("$BIN" --json skill eval)"
printf '%s\n' "$EVAL_JSON"
# The skill tier is the one rung that reads its case ids back directly: its rows
# carry `id` even on the fake model, because a fake turn is reported as the
# non-graded third state `plumbing_ok` (ADR-0055) rather than being skipped. So
# assert the ids ARE the bundle's ids, and assert the fake/live grading posture
# positively: sealed means nothing was graded, live means everything was.
if [[ "$LIVE" == "1" ]]; then
    EXPECT_PLUMBING=0
else
    EXPECT_PLUMBING=all
fi
printf '%s' "$EVAL_JSON" | python3 -c '
import json, sys
payload = json.loads(sys.stdin.read())
expect_plumbing = sys.argv[1]
suite = json.load(open(sys.argv[2]))
expected_ids = sorted(c["id"] for c in suite["cases"])
reported_ids = sorted(c["id"] for c in payload["cases"])
if reported_ids != expected_ids:
    sys.exit(
        "error: skill eval graded case ids %s; the bundle under test declares %s.\n"
        "expected: the eval path runs the very suite that ships inside the bundle."
        % (",".join(reported_ids), ",".join(expected_ids))
    )
total, passed, failed, plumbing = (
    payload["total"], payload["passed"], payload["failed"], payload["plumbing_ok"]
)
if expect_plumbing == "all":
    if not (plumbing == total and passed == 0 and failed == 0):
        sys.exit(
            "error: a sealed (fake-model) run must report every case as the non-graded "
            "plumbing_ok and grade nothing; got total=%d passed=%d failed=%d plumbing_ok=%d.\n"
            "expected: ADR-0055/#612 -- a fake turn returns one canned reply whatever the "
            "input, so a graded verdict on it is manufactured."
            % (total, passed, failed, plumbing)
        )
elif plumbing != 0:
    sys.exit(
        "error: a live run must grade every case, but %d of %d were reported as the "
        "non-graded plumbing_ok, so the run was sealed against the fake model."
        % (plumbing, total)
    )
print(
    "confirmed: skill eval ran the case ids the bundle declares (%s) with total=%d passed=%d "
    "failed=%d plumbing_ok=%d" % (",".join(reported_ids), total, passed, failed, plumbing)
)
' "$EXPECT_PLUMBING" "$BUNDLE_DIR/evals/cases.json"

if [[ -z "${CURIE_E2E_BUNDLE:-}" ]]; then
    echo
    echo "=== curie skill eval (explicit cases file) ==="
    # Standalone only, paired with the guard on the heredoc that writes this
    # file: it does not exist in a caller-supplied bundle, and creating it there
    # would move that bundle's packed digest.
    "$BIN" skill eval --cases evals/e2e-cases.json
fi

echo
echo "=== #1905 / #1087 AC3: plain re-up on the mutated source, confirm a NEW bundle digest is packed ==="
# Leave the recorded runner up. A second `skill up` without `--replace` must
# recognize this directory's runner, replace it, and boot the edited snapshot.
UP_OUTPUT_2="$("$BIN" skill up "${START_ARGS[@]}" 2>&1)"
printf '%s\n' "$UP_OUTPUT_2"
if printf '%s' "$UP_OUTPUT_2" | grep -q "a local runner is already recorded"; then
    echo "error: plain \`skill up\` after an edit refused and asked for --replace." >&2
    echo "expected: a verified same-directory runner is replaced automatically (#1905)." >&2
    exit 1
fi
if ! printf '%s' "$UP_OUTPUT_2" | grep -q "bundle changed: replacing the recorded runner"; then
    echo "error: plain \`skill up\` after an edit did not report that it replaced the recorded runner." >&2
    echo "output: $UP_OUTPUT_2" >&2
    exit 1
fi
STATUS_JSON_2="$("$BIN" skill status --json)"
DIGEST_2="$(printf '%s' "$STATUS_JSON_2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["bundle_digest"])')"
if [[ -z "$DIGEST_2" || "$DIGEST_2" == "None" ]]; then
    echo "error: expected \`skill status --json\` to report a non-null bundle_digest after re-\`skill up\`; got '$DIGEST_2'." >&2
    echo "payload: $STATUS_JSON_2" >&2
    exit 1
fi
if [[ "$DIGEST_2" == "$DIGEST_1" ]]; then
    echo "error: bundle_digest is still '$DIGEST_1' after a fresh \`skill up\` on a source that changed since the first boot." >&2
    echo "expected: a restart packages the changed source as a NEW digest." >&2
    exit 1
fi
echo "confirmed: re-up on changed source produced a new bundle digest ($DIGEST_1 -> $DIGEST_2)"

MOUNTED_SKILL_MD_2="$(docker exec "$CONTAINER" cat "/plugin/$SKILL_FILE_REL")"
if ! grep -qF "$MARKER" <<<"$MOUNTED_SKILL_MD_2"; then
    echo "error: after plain \`skill up\` the running container's /plugin/$SKILL_FILE_REL does not contain the host edit marker '$MARKER'." >&2
    echo "expected: the replacement runner serves the edited snapshot (#1905)." >&2
    exit 1
fi
echo "confirmed: the replacement runner's /plugin mount serves the edited snapshot ($SKILL_FILE_REL)"

echo
echo "=== restore the source and tear down the second runner ==="
cp -p "$SKILL_FILE_BACKUP" "$SKILL_FILE_PATH"
# Released only once the restore actually succeeded, so the trap does not skip a
# restore that never happened (and, on the success path, does not repeat one).
SKILL_MUTATION_PENDING=0
"$BIN" skill down

echo
echo "=== #1087: confirm teardown released the bundle snapshot ==="
SNAPSHOT_ROOT="$BUNDLE_DIR/.curie/snapshots"
if [[ -d "$SNAPSHOT_ROOT" ]]; then
    REMAINING="$(find "$SNAPSHOT_ROOT" -mindepth 1 -maxdepth 1)"
    if [[ -n "$REMAINING" ]]; then
        echo "error: expected $SNAPSHOT_ROOT to be empty (or absent) after the final \`skill down\`, but it still contains:" >&2
        echo "$REMAINING" >&2
        exit 1
    fi
fi
echo "confirmed: no bundle snapshot left under $SNAPSHOT_ROOT"

echo
echo "E2E PASS"
