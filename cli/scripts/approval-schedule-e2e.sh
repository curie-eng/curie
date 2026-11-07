#!/usr/bin/env bash
set -euo pipefail
exec uv run --locked python3 tools/approval-schedule-e2e/verify.py "$@"
