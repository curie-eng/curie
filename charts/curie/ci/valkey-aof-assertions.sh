#!/usr/bin/env bash
# The in-chart Valkey command must enable AOF (#3349).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

RENDER_DIR="$TMP/render"
helm template rel "$CHART" --output-dir "$RENDER_DIR" >/dev/null \
  || fail "helm template failed"
MANIFEST="$RENDER_DIR/curie/templates/valkey.yaml"
[[ -f "$MANIFEST" ]] || fail "valkey manifest was not rendered"

valkey_command_has_aof() {
  python3 - "$1" <<'PY'
import sys
from pathlib import Path

import yaml

wanted = ("appendonly yes", "appendfsync everysec", "--dir /data")
found = False
for doc in yaml.safe_load_all(Path(sys.argv[1]).read_text()):
    if not isinstance(doc, dict):
        continue
    template = doc.get("spec", {}).get("template", doc.get("spec", {}))
    containers = template.get("spec", {}).get("containers", [])
    for container in containers:
        if container.get("name") != "valkey":
            continue
        found = True
        joined = " ".join(str(part) for part in (container.get("command") or []))
        missing = [phrase for phrase in wanted if phrase not in joined]
        if missing:
            print(f"valkey command missing {missing}: {joined!r}", file=sys.stderr)
            sys.exit(1)
        mounts = container.get("volumeMounts") or []
        if not any(mount.get("name") == "data" and mount.get("mountPath") == "/data" for mount in mounts):
            print("valkey data volume is not mounted at /data", file=sys.stderr)
            sys.exit(1)
        claims = doc.get("spec", {}).get("volumeClaimTemplates") or []
        if not any((claim.get("metadata") or {}).get("name") == "data" for claim in claims):
            print("valkey data volume has no PVC template", file=sys.stderr)
            sys.exit(1)
if not found:
    print("no valkey container command in manifest", file=sys.stderr)
    sys.exit(1)
PY
}

valkey_command_has_aof "$MANIFEST" \
  || fail "rendered valkey command is missing appendonly yes or appendfsync everysec"

python3 - "$MANIFEST" "$TMP/stripped.yaml" <<'PY'
import sys
from pathlib import Path

import yaml

source, dest = sys.argv[1:]
docs = list(yaml.safe_load_all(Path(source).read_text()))

def strip_command(command: list[object]) -> list[str]:
    parts = [str(part) for part in command]
    kept: list[str] = []
    index = 0
    while index < len(parts):
        nxt = parts[index + 1] if index + 1 < len(parts) else ""
        if parts[index] in {"--appendonly", "appendonly"} and nxt == "yes":
            index += 2
            continue
        if parts[index] in {"--appendfsync", "appendfsync"} and nxt == "everysec":
            index += 2
            continue
        text = parts[index]
        for phrase in (
            "--appendonly yes",
            "--appendfsync everysec",
            "appendonly yes",
            "appendfsync everysec",
        ):
            text = text.replace(phrase, " ")
        kept.append(" ".join(text.split()))
        index += 1
    return kept

for doc in docs:
    if not isinstance(doc, dict):
        continue
    template = doc.get("spec", {}).get("template", doc.get("spec", {}))
    for container in template.get("spec", {}).get("containers", []):
        if container.get("name") == "valkey" and container.get("command"):
            container["command"] = strip_command(container["command"])

Path(dest).write_text(yaml.safe_dump_all(docs))
PY

set +e
valkey_command_has_aof "$TMP/stripped.yaml"
status=$?
set -e
if [[ "$status" -eq 0 ]]; then
  fail "AOF checker accepted a valkey command with appendonly and appendfsync removed"
fi

echo "valkey AOF appendonly yes and appendfsync everysec: OK"

helm template rel "$CHART" --output-dir "$TMP/byo" \
  --set valkey.deploy=false --set-string valkey.host=redis.acme.internal >/dev/null \
  || fail "BYO valkey render failed"
[[ ! -e "$TMP/byo/curie/templates/valkey.yaml" ]] \
  || fail "BYO valkey render includes a chart owned valkey"
