#!/usr/bin/env bash
# Verb parity gate: sibling message verbs must expose the same conversation
# control and fresh by default semantics across skill, local, and cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cargo test --manifest-path "$REPO_ROOT/cli/Cargo.toml" \
    --test integration command_surface::message_tiers_share_the_conversation_flag_and_default \
    -- --exact --nocapture
