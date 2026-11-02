#!/usr/bin/env bash
# Keep the real Helm mutation suite in curie dev chart-check's shell inventory.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
uv run --project "$ROOT" pytest -q "$ROOT/charts/curie/ci/test_api_identity_assertions.py"
