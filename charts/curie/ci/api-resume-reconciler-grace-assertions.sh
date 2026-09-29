#!/usr/bin/env bash
#
# Render assertions for the API resume reconciler grace relationship:
#
#   (a) the default grace derives from the default delivery budget plus reserve;
#   (b) an explicit 1800 second delivery budget derives grace from that budget
#       plus reserve, without an API override;
#   (c) an explicit grace at the raised floor renders and reaches the API; and
#   (d) an explicit YAML null selects the derived default; and
#   (e) an explicit grace below that floor is refused during rendering.
#
# These assertions read the API Deployment structurally, so a similarly named
# environment variable on another workload cannot produce a false pass.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL [$1] $2" >&2; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

assert_api_grace() {
  local case_id="$1" expected="$2" output="$TMP/$1.yaml"
  shift 2

  if ! helm template curie "$CHART" "$@" >"$output" 2>&1; then
    fail "$case_id" "helm template failed; it must render successfully
$(head -5 "$output")"
  fi

  if ! CASE_ID="$case_id" python3 - "$output" "$expected" <<'PY'
import os
import sys

import yaml

documents = [document for document in yaml.safe_load_all(open(sys.argv[1])) if document]
deployments = [
    document
    for document in documents
    if document.get("kind") == "Deployment"
    and document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "api"
]
if len(deployments) != 1:
    raise SystemExit(
        f"FAIL [{os.environ['CASE_ID']}] expected exactly one API Deployment, "
        f"found {len(deployments)}"
    )

environment = {
    entry["name"]: entry.get("value")
    for container in deployments[0]["spec"]["template"]["spec"]["containers"]
    for entry in container.get("env", [])
}
actual = environment.get("RESUME_RECONCILER_GRACE_SECONDS")
expected = sys.argv[2]
if actual != expected:
    raise SystemExit(
        f"FAIL [{os.environ['CASE_ID']}] RESUME_RECONCILER_GRACE_SECONDS "
        f"rendered {actual!r}, expected {expected!r}"
    )
PY
  then
    fail "$case_id" "the API grace assertion failed"
  fi
}

# (a) Default delivery budget 10800 plus reserve 60 yields the derived grace 10860.
assert_api_grace a 10860

# (b) With no API override, grace follows this delivery budget plus reserve.
assert_api_grace b 1860 \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860

# (c) An explicit value exactly at the raised floor is accepted and reaches the API.
assert_api_grace c 1860 \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.deliveryShutdownReserveSeconds=60 \
  --set worker.terminationGracePeriodSeconds=1860 \
  --set api.resumeReconciler.graceSeconds=1860

# (d) An explicit YAML null selects the derived default.
assert_api_grace d 1860 \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 \
  --set-json api.resumeReconciler.graceSeconds=null

# (e) A grace below delivery budget plus reserve must fail at render time.
invalid_output=""
if invalid_output="$(helm template curie "$CHART" \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800 \
  --set worker.terminationGracePeriodSeconds=1860 \
  --set api.resumeReconciler.graceSeconds=1800 2>&1)"; then
  fail e "helm accepted api.resumeReconciler.graceSeconds=1800 below the required delivery floor of 1860"
fi
for token in \
  "api.resumeReconciler.graceSeconds" \
  "worker.deliveryBudgetSeconds" \
  "worker.deliveryShutdownReserveSeconds" \
  "(1800) + " \
  "(60) = 1860"; do
  grep -qF "$token" <<<"$invalid_output" \
    || fail e "the refusal does not identify $token
$(head -3 <<<"$invalid_output")"
done

echo "api-resume-reconciler-grace-assertions: all five assertions passed"
