#!/usr/bin/env bash
# @spec docs/superpowers/specs/2026-10-08-mcp-connector-valkey-ingress-optout.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/test_data_tier_mcp_valkey_peer.py"
