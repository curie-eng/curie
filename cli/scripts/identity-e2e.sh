#!/usr/bin/env bash
# Entry for `curie dev identity-e2e`. The driver is standard-library Python so
# it runs from a bare source checkout; see docs/testing/identity-e2e.md.
set -euo pipefail
exec python3 "$(dirname "$0")/../../tools/identity-e2e/identity_e2e.py" "$@"
