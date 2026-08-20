#!/usr/bin/env bash
# Behavioral diff of a candidate bundle against the deployed one, across the
# consistency ladder (`curie dev tier-diff`).
#
# The interface is the `curie` subcommand; this script is only the implementation
# seam that reaches the Python diff engine, per the one-entry-point rule in
# CLAUDE.md. It lives under `dev` because it needs a source checkout and the
# worker's dev toolchain, and because the report's JSON shape has no committed
# versioned schema yet -- promoting it to a top-level verb would publish an
# agent-facing contract this spike has not earned.
#
# Arguments are passed straight through to the engine. A non-zero exit from
# `--fail-on-change` surfaces as a script failure, which is what a CI gate wants.
set -euo pipefail

# An inherited VIRTUAL_ENV pointing at a different checkout (a sibling worktree,
# say) makes uv print a warning and then ignore it anyway. Clearing it here keeps
# the output to the report itself.
unset VIRTUAL_ENV
exec uv run --directory apps/worker python -m curie_worker.eval.tierdiff_cli "$@"
