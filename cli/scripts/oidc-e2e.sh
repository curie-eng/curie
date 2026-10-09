#!/usr/bin/env bash
# Entry for `curie dev oidc-e2e` (#2908, #3800). The driver is standard-library
# Python so it runs from a bare source checkout; see its module docstring for
# the CURIE_OIDC_E2E_* inputs.
set -euo pipefail
exec python3 "$(dirname "$0")/../../tools/oidc-e2e/oidc_e2e.py" "$@"
