#!/usr/bin/env bash
#
# Consumer-path self-test for the released-upgrade live-manifest verifier
# (#2097). helm-ci and `curie dev chart-check` only discover executable *.sh
# files in this directory, so the Python verifier is invoked from here.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/live_manifest_parity.py" --self-test
