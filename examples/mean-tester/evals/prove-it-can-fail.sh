#!/usr/bin/env bash
# Prove the mean tester can fail (ADR 0169 d8, #3043). Needs a model credential.
#
#   examples/mean-tester/evals/prove-it-can-fail.sh
#
# Three runs of the same suite against the replay connector:
#   1. the real skill        -> every case green
#   2. a skill that always   -> every FAIL-expecting case red
#      says PASS
#   3. a skill that always   -> every PASS-expecting case red
#      says FAIL
set -euo pipefail
cd "$(dirname "$0")/.."
BUNDLE="$PWD"
WORK="$(mktemp -d)"
trap 'kill "${REPLAY_PID:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

# The connector reads its two tokens from ONE variable, MEAN_TESTER_CREDENTIALS
# (a JSON object with slack_bot_token and github_token), because a hosted
# connector with two SecretRefs cannot be deployed (plugin_format/connectors.py
# refuses it without a bearer_secret, and runner/src/curie_runner/connectors.py
# drops the derived Authorization header only for a lone SecretRef). See
# connectors/probes/mean_tester_probes/config.py:_credentials and
# connectors.yaml's `secrets:` entry.
export MEAN_TESTER_CREDENTIALS='{"slack_bot_token":"replay","github_token":"replay"}' \
       MEAN_TESTER_CHANNELS=C0EXAMPLE4 MEAN_TESTER_REPOS=replay/replay@main MEAN_TESTER_SETTLE_S=0 \
       MEAN_TESTER_REPLY_TIMEOUT_S=5 \
       MEAN_TESTER_REPLAY_DIR="$BUNDLE/evals/fixtures" PORT=18731
# A replayed reply is final at once (SETTLE_S=0), so the reply timeout only
# ever runs out for `never-answers`, which has no reply to wait for. Five
# seconds keeps that case inside a turn; it is also the window the probe caps
# count over, which one probe per case stays well under.
( cd connectors/probes && python -m mean_tester_probes.server ) & REPLAY_PID=$!
sleep 2
# The tier's documented laptop path for an authed MCP server the runner
# reaches over the network: connectors.yaml's `unhosted_url:
# ${MEAN_TESTER_PROBES_MCP_URL}` plus `skill up --secret
# MEAN_TESTER_PROBES_MCP_URL` below, forwarding this value into the runner
# container by name. `host.docker.internal` is how that container reaches a
# port bound on this host (cli/src/message.rs:1157, DOCKER_INTERNAL_HOST; the
# same address the cluster-message local stub advertises for the identical
# reason, cli/src/message.rs:6631-6658).
export MEAN_TESTER_PROBES_MCP_URL=http://host.docker.internal:18731/mcp

run() {  # $1 = bundle dir, $2 = label; prints the pass count line
  # `skill eval` and `skill down` take no --plugin-dir: they read the running
  # runner's recorded state from `.curie/runner.json` in the CURRENT
  # DIRECTORY (cli/src/commands.rs:3525, `state::load(Path::new("."))` inside
  # `eval`; cli/src/main.rs:4085, `commands::stop(name,
  # std::path::Path::new("."))` for `down` -- `SkillAction::Down` carries only
  # a `name`, cli/src/main.rs:1511-1517). `skill up` is the odd one out with
  # an explicit `--plugin-dir` (cli/src/main.rs:1349-1352), but it writes that
  # same state file INTO the plugin dir it was given
  # (cli/src/commands.rs:2335-2340: "the follow-up commands are documented to
  # run from the bundle directory"). So the whole sequence runs with cwd set
  # to the bundle directory throughout, rather than passing it as a flag to
  # eval/down, which clap would refuse as an unknown argument.
  (
    cd "$1"
    curie skill up --replace --secret MEAN_TESTER_PROBES_MCP_URL >/dev/null
    curie skill eval --json | tee "$WORK/$2.json" | python3 -c \
      'import json,sys; r=json.load(sys.stdin); print(sys.argv[1], r["passed"], "/", r["total"])' "$2"
    curie skill down >/dev/null
  )
}

stub() {  # $1 = verdict every probe gets
  cp -R "$BUNDLE" "$WORK/$1"
  cat > "$WORK/$1/skills/mean-tester/SKILL.md" <<EOF
---
name: mean-tester
description: Mean test another agent when someone asks you to test it.
---
Call mcp__probes__read_target, mcp__probes__send_probes with one probe, and
mcp__probes__collect_replies. Then reply exactly: "round 1/1: $( [ "$1" = PASS ] && echo "1 PASS · 0 FAIL" || echo "0 PASS · 1 FAIL" ) · 0 UNCLEAR".
EOF
  echo "$WORK/$1"
}

run "$BUNDLE" real
run "$(stub PASS)" always-pass
run "$(stub FAIL)" always-fail
python3 - "$WORK" <<'PY'
import json, sys, pathlib
w = pathlib.Path(sys.argv[1])
real, allpass, allfail = (json.loads((w / f"{n}.json").read_text()) for n in ("real", "always-pass", "always-fail"))
assert real["passed"] == real["total"], "the real skill must be green"
assert allpass["passed"] < allpass["total"], "an always-PASS tester must be red"
assert allfail["passed"] < allfail["total"], "an always-FAIL tester must be red"
print("proved: the suite catches a tester that always passes and one that always fails")
PY
