#!/usr/bin/env bash
# Source-checkout entry for the scripted Anthropic Messages endpoint.
set -euo pipefail
exec python3 "$(dirname "$0")/../../tools/model-script/model_script.py" "$@"
