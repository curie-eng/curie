#!/usr/bin/env bash
#
# Live-object half of the factory sandbox resource profile upgrade proof
# (#4889). Run by the released-upgrade CI job after it upgrades an installed
# release to the candidate chart, and again after a second plain
# retained-values upgrade. It reads the SandboxTemplate objects the release
# actually holds in the cluster and asserts:
#
#   * the factory agent's template carries the full factory envelope on the
#     runner container and every init container (requests cpu 50m, memory 8Gi,
#     ephemeral-storage 10Gi; limits cpu 2, memory 8Gi, ephemeral-storage
#     20Gi), /home/runner 15Gi and /tmp 4Gi, and the workspace the operator set
#     explicitly (agentSandbox.workspaceSizeLimits.<agent>), not the profile's;
#   * the generic template keeps the standard envelope (requests 50m, 192Mi,
#     512Mi; limits 1, 768Mi, 4Gi; workspace 1Gi; /home/runner and /tmp 512Mi),
#     the non-factory control.
#
# Usage:
#   released-upgrade-resource-profile-assertions.sh --context <kube-context> \
#     --namespace <ns> --release <release> --agent <factory-agent> \
#     --workspace <size>
#
# Every input comes from the caller; nothing about the target cluster is
# assumed. Needs kubectl and python3. Read only.
set -euo pipefail

fail() { echo "FAIL: $*" >&2; exit 1; }

CONTEXT="" NAMESPACE="" RELEASE="" AGENT="" WORKSPACE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --context) CONTEXT="${2:-}"; shift 2 ;;
    --namespace) NAMESPACE="${2:-}"; shift 2 ;;
    --release) RELEASE="${2:-}"; shift 2 ;;
    --agent) AGENT="${2:-}"; shift 2 ;;
    --workspace) WORKSPACE="${2:-}"; shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done
for pair in "context:$CONTEXT" "namespace:$NAMESPACE" "release:$RELEASE" "agent:$AGENT" "workspace:$WORKSPACE"; do
  [[ -n "${pair#*:}" ]] || fail "--${pair%%:*} is required"
done
command -v kubectl >/dev/null || fail "kubectl is required"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

kubectl --context "$CONTEXT" -n "$NAMESPACE" get sandboxtemplates.extensions.agents.x-k8s.io \
  -l "app.kubernetes.io/instance=$RELEASE" -o json > "$TMP/templates.json" \
  || fail "could not read SandboxTemplates of release $RELEASE in namespace $NAMESPACE"

python3 - "$TMP/templates.json" "$AGENT" "$WORKSPACE" <<'PY'
import json
import sys

path, agent, workspace = sys.argv[1:4]
items = json.load(open(path)).get("items", [])
templates = {t["metadata"]["name"]: t for t in items}

STANDARD = {
    "requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
    "limits": {"cpu": "1", "memory": "768Mi", "ephemeral-storage": "4Gi"},
    "workspace": "1Gi", "/home/runner": "512Mi", "/tmp": "512Mi",
}
FACTORY = {
    "requests": {"cpu": "50m", "memory": "8Gi", "ephemeral-storage": "10Gi"},
    "limits": {"cpu": "2", "memory": "8Gi", "ephemeral-storage": "20Gi"},
    "workspace": workspace, "/home/runner": "15Gi", "/tmp": "4Gi",
}

generic = [n for n in templates if "-agent-" not in n]
factory = [n for n in templates if n.endswith(f"-agent-{agent}-runner")]
errors = []
if len(generic) != 1:
    errors.append(f"want one generic SandboxTemplate, got {sorted(templates)}")
if len(factory) != 1:
    errors.append(f"want one {agent} SandboxTemplate, got {sorted(templates)}")


def check(label, name, want):
    spec = templates[name]["spec"]["podTemplate"]["spec"]
    runner = next((c for c in spec.get("containers", []) if c["name"] == "runner"), None)
    if runner is None:
        errors.append(f"{label} {name}: no runner container")
        return
    for c in [runner] + list(spec.get("initContainers") or []):
        res = c.get("resources") or {}
        for side in ("requests", "limits"):
            got = {k: str(v) for k, v in (res.get(side) or {}).items()}
            if got != want[side]:
                errors.append(f"{label} {name}: container {c['name']} {side} {got}, want {want[side]}")
    mounts = {m["name"]: m["mountPath"] for m in runner.get("volumeMounts", [])}
    sizes = {}
    for v in spec.get("volumes", []):
        if "emptyDir" not in v:
            continue
        if v["name"] == "workspace":
            sizes["workspace"] = str(v["emptyDir"].get("sizeLimit"))
        elif v["name"].startswith("writable-"):
            sizes[mounts.get(v["name"], v["name"])] = str(v["emptyDir"].get("sizeLimit"))
    for key in ("workspace", "/home/runner", "/tmp"):
        if sizes.get(key) != want[key]:
            errors.append(f"{label} {name}: volume {key} sizeLimit {sizes.get(key)!r}, want {want[key]!r}")


if not errors:
    check("factory agent", factory[0], FACTORY)
    check("generic", generic[0], STANDARD)
if errors:
    print("FAIL: live SandboxTemplates do not carry the expected sandbox envelopes:", file=sys.stderr)
    for error in errors:
        print("  " + error, file=sys.stderr)
    raise SystemExit(1)
print(f"OK: live {factory[0]} carries the factory envelope with workspace {workspace}; "
      f"{generic[0]} keeps the standard envelope")
PY
