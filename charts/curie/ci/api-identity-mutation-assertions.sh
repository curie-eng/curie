#!/usr/bin/env bash
# Keep the real Helm mutation suite in curie dev chart-check's shell inventory.
set -euo pipefail
python3 "$(dirname "${BASH_SOURCE[0]}")/test_api_identity_assertions.py"
