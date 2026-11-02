#!/usr/bin/env bash
# Run one cluster runtime assertion and record an execution receipt (#3391).
#
# Usage: bash tools/runtime-assertion-gate/run.sh <runtime-script> [args...]
# The shared workload installation fixture is accepted only with phase up.
#
# The receipt is written only after the script exits 0, and it records the git
# blob id of the exact script that ran. `E2E required` refuses a changed
# runtime assertion that has no matching receipt, so a step that is skipped,
# cannot start, or fails can never turn the required verdict green.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: run.sh <runtime-script> [args...]" >&2
  exit 2
fi

script="$1"
shift
case "$script" in
  charts/curie/ci/scenarios/workload-install.sh)
    if [[ $# != 1 || "$1" != up ]]; then
      echo "run.sh: workload-install.sh requires exactly the up phase" >&2
      exit 2
    fi
    ;;
  charts/curie/ci/runtime/*.sh) ;;
  *)
    echo "run.sh: $script is not a charts/curie/ci/runtime assertion" >&2
    exit 2
    ;;
esac
if [[ ! -f "$script" ]]; then
  echo "run.sh: $script does not exist" >&2
  exit 2
fi

receipts="${RUNTIME_ASSERTION_RECEIPTS:-${RUNNER_TEMP:?RUNNER_TEMP or RUNTIME_ASSERTION_RECEIPTS is required}/runtime-assertion-receipts}"
blob="$(git hash-object -- "$script")"

bash "$script" "$@"

mkdir -p "$receipts"
name="$(basename "$script" .sh)"
if [[ "$script" == charts/curie/ci/scenarios/workload-install.sh ]]; then
  printf '{"script": "%s", "blob": "%s", "status": "pass", "phase": "up"}\n' "$script" "$blob" \
    > "$receipts/$name.json"
else
  printf '{"script": "%s", "blob": "%s", "status": "pass"}\n' "$script" "$blob" \
    > "$receipts/$name.json"
fi
echo "runtime assertion passed: $script ($blob)"
