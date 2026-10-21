#!/usr/bin/env bash
# Thin source-checkout entry for `curie dev learning-eval` and
# `curie dev learning-eval-verify` (#4780). The Rust dispatch arm passes the
# pinned candidate executable as --curie-bin; the Python module owns the whole
# lifecycle and prints at most one JSON object on stdout.
set -euo pipefail
exec uv run --locked --package curie-worker python3 -m curie_worker.eval.learning "$@"
