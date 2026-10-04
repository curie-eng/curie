#!/usr/bin/env bash
# Source-checkout entry for the recorded GitHub factory fixture.
set -euo pipefail
exec python3 "$(dirname "$0")/../../tools/github-stub/github_stub.py" "$@"
