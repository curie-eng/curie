#!/usr/bin/env bash
#
# Every Secret reference the chart renders must be listed by the credential
# inventory (ADR 0163 decision 4), and an inventory that hands a rotation-owned
# key to ESO must be refused.
#
# An AWS install syncs the inventory's store: sm rows from Secrets Manager and
# points the chart's existingSecret knobs at them. A chart change that adds a
# credential the inventory does not list would install fine and then have no
# provider-held value on a rebuild; a minted or workload-rotated key marked
# ESO-managed would be reverted by ESO within seconds of every rotation. Both
# are silent at install time, so this gate fails them at render time.
#
# `curie dev secrets-inventory` does the work; this script runs it and two
# planted negative controls, so a check that quietly stopped looking (a walker
# that skips env refs, a validator that skips the rotation rule) fails here
# instead of passing everything. Only `helm template` is spawned: no cluster,
# no aws, no kubectl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO="$(cd "$CHART/../.." && pwd)"
INVENTORY="$REPO/cli/src/provider/platform-inventory.yaml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- the binary under test ---------------------------------------------------
if [[ -n "${CURIE_BIN:-}" && -x "${CURIE_BIN}" ]]; then
  BIN="$CURIE_BIN"
else
  cargo build --release --locked --manifest-path "$REPO/cli/Cargo.toml" >&2
  # The target directory is configurable (CARGO_TARGET_DIR, a shared-target
  # config entry); ask cargo rather than assuming ./target.
  TARGET_DIR="$(cargo metadata --no-deps --format-version 1 \
    --manifest-path "$REPO/cli/Cargo.toml" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
  BIN="$TARGET_DIR/release/curie"
fi
if [[ ! -x "$BIN" ]]; then
  echo "FAIL: no executable curie binary at $BIN" >&2
  exit 1
fi

cd "$REPO"

# --- positive: the real chart and inventory ----------------------------------
if ! "$BIN" dev secrets-inventory --chart "$CHART"; then
  echo "FAIL: the chart renders a Secret reference the inventory does not list" >&2
  exit 1
fi
echo "PASS: every rendered Secret reference is listed"

# --- control a: a planted secretKeyRef no entry lists ------------------------
cp -r "$CHART" "$TMP/chart"
cat > "$TMP/chart/templates/zz-planted-unlisted.yaml" <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: planted
spec:
  containers:
    - name: planted
      image: busybox
      env:
        - name: PLANTED
          valueFrom:
            secretKeyRef:
              name: planted-unlisted
              key: plantedKey
YAML
if "$BIN" dev secrets-inventory --chart "$TMP/chart" >"$TMP/a.out" 2>&1; then
  cat "$TMP/a.out" >&2
  echo "FAIL: control a exited 0 with an unlisted secretKeyRef planted" >&2
  exit 1
fi
if ! grep -q "references Secret planted-unlisted key plantedKey" "$TMP/a.out"; then
  cat "$TMP/a.out" >&2
  echo "FAIL: control a failed without naming planted-unlisted/plantedKey" >&2
  exit 1
fi
echo "PASS: control a (unlisted secretKeyRef) fails and names it"

# --- control b: the minted Grafana token handed to ESO -----------------------
# Flip only the grafana-connector-token entry's store line, then prove the flip
# happened, so a reformatted inventory cannot turn this control into a no-op.
awk '
  /^  - logical_name:/ { in_entry = ($3 == "grafana-connector-token") }
  in_entry && /^    store: cluster$/ { print "    store: sm"; flipped = 1; next }
  { print }
  END { if (!flipped) exit 3 }
' "$INVENTORY" > "$TMP/inventory.yaml" || {
  echo "FAIL: control b found no grafana-connector-token store: cluster line to flip" >&2
  exit 1
}
if "$BIN" dev secrets-inventory --chart "$CHART" --inventory "$TMP/inventory.yaml" \
  >"$TMP/b.out" 2>&1; then
  cat "$TMP/b.out" >&2
  echo "FAIL: control b exited 0 with a rotation-owned key marked ESO-managed" >&2
  exit 1
fi
if ! grep -q "GRAFANA_SERVICE_ACCOUNT_TOKEN is rotation-owned" "$TMP/b.out"; then
  cat "$TMP/b.out" >&2
  echo "FAIL: control b failed without naming the rotation-owned key" >&2
  exit 1
fi
echo "PASS: control b (rotation-owned key marked store: sm) is refused"
