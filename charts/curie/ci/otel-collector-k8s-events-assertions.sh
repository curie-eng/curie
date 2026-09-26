#!/usr/bin/env bash
# Opt-in Kubernetes Events source for the chart's collector (#2954).
# Off by default; one value turns on a k8sobjects receiver scoped to the
# release namespace, wired into the logs pipeline, with read-only events RBAC.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

cat > "$TMP/sink.yaml" <<'YAML'
otelCollector:
  extraExporters:
    otlphttp/logs-sink:
      endpoint: http://logs-sink.example.com:4318
      retry_on_failure: { enabled: true, max_interval: 5s, max_elapsed_time: 30s }
      sending_queue: { enabled: true, storage: file_storage, queue_size: 100 }
  extraLogPipelineExporters: [otlphttp/logs-sink]
YAML

helm template curie "$CHART" --namespace tenant-a > "$TMP/default.yaml"
helm template curie "$CHART" --namespace tenant-a -f "$TMP/sink.yaml" \
  --set otelCollector.kubernetesEvents.enabled=true > "$TMP/on.yaml"

python3 - "$TMP/default.yaml" "$TMP/on.yaml" <<'PY'
import sys, yaml

def docs(path):
    return [d for d in yaml.safe_load_all(open(path)) if d]

def collector(ds):
    cm = next(d for d in ds if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "curie-otel-collector")
    dep = next(d for d in ds if d["kind"] == "Deployment" and d["metadata"]["name"] == "curie-otel-collector")
    return yaml.safe_load(cm["data"]["collector-config.yaml"]), dep["spec"]["template"]["spec"]

def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr); sys.exit(1)

default, on = docs(sys.argv[1]), docs(sys.argv[2])

# Default: no receiver, no identity, no RBAC.
cfg, pod = collector(default)
if any(r.startswith("k8sobjects") for r in cfg["receivers"]):
    fail("default render declares a k8sobjects receiver")
if cfg["service"]["pipelines"]["logs"]["receivers"] != ["otlp"]:
    fail("default logs pipeline gained a receiver")
if "serviceAccountName" in pod:
    fail("default collector pod was given a ServiceAccount")
for d in default:
    if d["kind"] in ("Role", "RoleBinding", "ServiceAccount") and "otel-collector" in d["metadata"]["name"]:
        fail(f"default render emits {d['kind']} {d['metadata']['name']}")

# Enabled: receiver scoped to the release namespace, feeding the logs pipeline.
cfg, pod = collector(on)
rec = cfg["receivers"].get("k8sobjects/events") or fail("k8sobjects/events receiver missing")
if rec.get("auth_type") != "serviceAccount":
    fail("receiver must authenticate with the pod ServiceAccount")
objs = rec["objects"]
if [(o["name"], o.get("group"), o["mode"], o["namespaces"]) for o in objs] != [("events", "events.k8s.io", "watch", ["tenant-a"])]:
    fail(f"receiver objects not scoped to events in the release namespace: {objs}")
logs = cfg["service"]["pipelines"]["logs"]
if logs["receivers"] != ["otlp", "k8sobjects/events"]:
    fail(f"logs pipeline receivers: {logs['receivers']}")
if "otlphttp/logs-sink" not in logs["exporters"]:
    fail("events do not reach the operator's log exporter")
for sig in ("traces", "metrics"):
    if cfg["service"]["pipelines"][sig]["receivers"] != ["otlp"]:
        fail(f"{sig} pipeline gained the events receiver")

# RBAC: dedicated SA bound to a namespaced read-only events Role.
if pod.get("serviceAccountName") != "curie-otel-collector":
    fail("collector pod does not run as curie-otel-collector")
by = {(d["kind"], d["metadata"]["name"]): d for d in on}
("ServiceAccount", "curie-otel-collector") in by or fail("ServiceAccount missing")
role = by.get(("Role", "curie-otel-collector-events")) or fail("Role missing")
if any(d["kind"] == "ClusterRole" and "otel-collector" in d["metadata"]["name"] for d in on):
    fail("events access must not be cluster-scoped")
if role["rules"] != [{"apiGroups": ["", "events.k8s.io"], "resources": ["events"], "verbs": ["get", "list", "watch"]}]:
    fail(f"Role is not read-only events: {role['rules']}")
rb = by.get(("RoleBinding", "curie-otel-collector-events")) or fail("RoleBinding missing")
if rb["roleRef"]["name"] != "curie-otel-collector-events" or rb["subjects"] != [
    {"kind": "ServiceAccount", "name": "curie-otel-collector", "namespace": "tenant-a"}]:
    fail(f"RoleBinding does not bind the collector SA: {rb}")
print("PASS: kubernetesEvents off by default; opt-in receiver, pipeline and RBAC render as specified")
PY

# Negative control: enabling events with only the nop log exporter is refused.
if helm template curie "$CHART" --set otelCollector.kubernetesEvents.enabled=true > "$TMP/nop.out" 2>&1; then
  fail "events enabled with only nop log exporters rendered; it would record nothing"
fi
grep -q "extraLogPipelineExporters" "$TMP/nop.out" || fail "refusal does not name extraLogPipelineExporters: $(cat "$TMP/nop.out")"

# The debug exporter counts as a sink for development installs.
helm template curie "$CHART" --set otelCollector.kubernetesEvents.enabled=true \
  --set otelCollector.debugExporter.enabled=true > /dev/null \
  || fail "events with debugExporter enabled should render"

echo "PASS: nop-only refusal and debug allowance"
