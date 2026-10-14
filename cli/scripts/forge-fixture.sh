#!/usr/bin/env bash
# Entry for `curie dev forge-fixture` (#4398).
#
# Usage: forge-fixture.sh <kind> <up|self-test|down> --state-dir <dir>
#                         [--advertise-host <host>] [--port <port>]
#
# Each forge fixture lives in tools/forge-fixtures/<kind>/fixture.py, so each
# forge's pull request adds only its own fixture directory. This script is the
# fixed entry: it refuses a kind whose fixture is not installed (exit 3,
# without creating the state dir), otherwise it hands the action and flags to
# the fixture.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: forge-fixture.sh <kind> <up|self-test|down> --state-dir <dir> [--advertise-host <host>] [--port <port>]" >&2
  exit 2
fi

kind="$1"
action="$2"
root="$(cd "$(dirname "$0")/../.." && pwd)"
fixture="$root/tools/forge-fixtures/$kind/fixture.py"

if [ ! -f "$fixture" ]; then
  echo "forge-fixture: $kind is not installed: tools/forge-fixtures/$kind/fixture.py is missing" >&2
  exit 3
fi

exec python3 "$fixture" "$action" "${@:3}"
