#!/usr/bin/env bash
# Entry for `curie dev factory-e2e` (#2966). The driver is standard-library
# Python so it runs from a bare source checkout; see its module docstring for
# the CURIE_FACTORY_* inputs.
set -euo pipefail
exec python3 "$(dirname "$0")/../../tools/factory-e2e/factory_e2e.py" "$@"
