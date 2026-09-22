#!/usr/bin/env bash

set -uo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HARNESS="$ROOT/cli/scripts/provider_harness.py"

if [[ ! -f "$HARNESS" ]]; then
    echo "provider harness is missing" >&2
    exit 1
fi

cd "$ROOT"
python3 "$HARNESS" "$@" &
child=$!

forward_signal() {
    local name="$1"
    kill -s "$name" "$child" 2>/dev/null || true
}

trap 'forward_signal TERM' TERM
trap 'forward_signal HUP' HUP
trap 'forward_signal INT' INT

status=0
while true; do
    wait "$child"
    status=$?
    if kill -0 "$child" 2>/dev/null; then
        continue
    fi
    break
done

trap - TERM HUP INT
exit "$status"
