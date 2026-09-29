#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART="$ROOT/charts/curie"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

NOTES_CHART="$TMP/chart"
cp -a "$CHART" "$NOTES_CHART"
cp "$CHART/templates/NOTES.txt" "$NOTES_CHART/NOTES.txt"
cat >"$NOTES_CHART/templates/notes-check.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF

render_notes() {
  helm template curie "$NOTES_CHART" \
    --show-only templates/notes-check.yaml "$@"
}

default_notes="$(render_notes)"
grep -Fq 'WARNING: a successful install does not prove NetworkPolicy enforcement.' \
  <<<"$default_notes" \
  || { echo 'default NOTES do not warn that install success leaves NetworkPolicy unverified' >&2; exit 1; }
grep -Fq 'helm test curie -n default' <<<"$default_notes" \
  || { echo 'default NOTES do not name the command that verifies NetworkPolicy enforcement' >&2; exit 1; }

disabled_notes="$(render_notes --set preflights.networkPolicyProbe.enabled=false)"
if grep -Fq 'NetworkPolicy enforcement' <<<"$disabled_notes"; then
  echo 'NOTES warn about a disabled NetworkPolicy probe as though it could be run' >&2
  exit 1
fi

echo 'PASS: enabled NOTES fail loud about unverified NetworkPolicy enforcement and name the probe command'
