#!/usr/bin/env bash
#
# Render-assertion test for issue #4408. The in-chart RustFS crash-looped on a
# volume whose inode table was full: the 3Gi default formats with 196,608
# inodes, every Langfuse event upload costs about three, and RustFS exits
# fatally on a full disk before its own lifecycle expiry can run. The chart
# raises the default claim to 10Gi, keeps an existing StatefulSet's claim size
# (volumeClaimTemplates are immutable, so a plain default bump fails every
# upgrade), and adds a `storage-guard` init container that clears expired
# event objects before RustFS starts. rustfs-storage-guard-behavior.sh runs the
# guard script itself against the real image; this script pins the render.
#
#   1. DEFAULT render: StatefulSet rel-curie-rustfs has a `data` claim
#      template requesting 10Gi (Decision 1).
#   2. HELPER: curie.rustfs.claimSize, driven with fixture StatefulSets the way
#      `lookup` would return them, returns the existing `data` claim size (3Gi),
#      also when `data` is not the first claim template, and returns the
#      default when given an empty dict (what `lookup` yields under
#      `helm template`, Argo CD, or a fresh install) (Decision 2).
#   3. DEFAULT render: exactly one init container `storage-guard` with the
#      RustFS image, rustfs.containerSecurityContext, rustfs.resources, the
#      `data` volume at /data, and the guard script mounted from a chart
#      ConfigMap whose `rustfs-storage-guard.sh` key equals
#      files/rustfs-storage-guard.sh byte for byte; command runs that script
#      with /bin/sh; EVENTS_DIR=/data/<rustfs.bucket>/events and
#      RETENTION_MINUTES=<retentionDays * 1440>. The main `rustfs` container
#      keeps its image and /health probes.
#   4. retentionDays=0 renders RETENTION_MINUTES "0"; rustfs.bucket=other
#      renders EVENTS_DIR /data/other/events (both are values, not literals).
#   5. rustfs.persistence.enabled=false: no storage-guard init container, no
#      ConfigMap carrying the guard script anywhere in the render, and the
#      `data` volume is an emptyDir (Decision 6).
#   6. rustfs.deploy=false: no rustfs StatefulSet and no guard ConfigMap
#      anywhere in the render (Decision 6).
#   7. values-dev.yaml overlay still requests 3Gi (Decision 1).
#
# The helper fixture follows sandbox-capacity-fit-assertions.sh: copy the
# chart to a temp dir, add fixture YAML plus a zz-*.yaml template that renders
# the helper's outputs into a ConfigMap, render, assert.
#
# NOTE ON `--output-dir`: do not use `--show-only` (it failed in this
# environment even when the template exists). Render to a directory and read
# the written files; never a stdout pipe (a piped `helm template` has been
# observed to truncate silently while still exiting 0).
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
GUARD_FILE="$CHART/files/rustfs-storage-guard.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local name="$1"
  shift
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  mkdir -p "$RENDER_DIR"
  helm template rel "$CHART" --output-dir "$RENDER_DIR" "$@" >"$TMP/$name.log" 2>&1 \
    || fail "helm template failed for render '$name': $(cat "$TMP/$name.log")"
}

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
"""Checks for the rustfs StatefulSet claim size and storage-guard init container.

argv[1] = mode, argv[2] = helm --output-dir tree, then mode arguments:
  claim <size>                         data claim template requests <size>
  guard <values.yaml> <guard file> <events dir> <retention minutes>
  no-guard                             persistence disabled: no guard, emptyDir data
  no-sts                               deploy=false: no rustfs StatefulSet, no guard
"""
import pathlib
import posixpath
import sys

import yaml

MODE = sys.argv[1]
ROOT = pathlib.Path(sys.argv[2])
ARGS = sys.argv[3:]
STS_NAME = "rel-curie-rustfs"
GUARD_KEY = "rustfs-storage-guard.sh"
INIT_NAME = "storage-guard"


def die(message):
    raise SystemExit(f"FAIL: {message}")


def load_docs(root):
    docs = []
    for file_path in sorted(root.rglob("*.yaml")):
        for doc in yaml.safe_load_all(file_path.read_text()):
            if isinstance(doc, dict):
                docs.append((file_path, doc))
    return docs


DOCS = load_docs(ROOT)
if not DOCS:
    die(f"{ROOT}: render produced no YAML documents")


def rustfs_statefulsets():
    return [
        doc
        for _, doc in DOCS
        if doc.get("kind") == "StatefulSet"
        and (doc.get("metadata") or {}).get("name") == STS_NAME
    ]


def the_statefulset():
    found = rustfs_statefulsets()
    if len(found) != 1:
        die(f"expected exactly one StatefulSet {STS_NAME}, got {len(found)}")
    return found[0]


def pod_spec(sts):
    return ((sts.get("spec") or {}).get("template") or {}).get("spec") or {}


def guard_configmaps():
    """Every rendered ConfigMap, in any file, that carries the guard script key."""
    return [
        (path, doc)
        for path, doc in DOCS
        if doc.get("kind") == "ConfigMap" and GUARD_KEY in (doc.get("data") or {})
    ]


def guard_inits(spec):
    return [c for c in spec.get("initContainers") or [] if c.get("name") == INIT_NAME]


def env_map(container):
    out = {}
    for item in container.get("env") or []:
        if "value" not in item:
            die(f"{INIT_NAME} env {item.get('name')!r} has no literal value: {item!r}")
        out[item["name"]] = item["value"]
    return out


if MODE == "claim":
    (want,) = ARGS
    sts = the_statefulset()
    templates = (sts.get("spec") or {}).get("volumeClaimTemplates") or []
    data = [t for t in templates if (t.get("metadata") or {}).get("name") == "data"]
    if len(data) != 1:
        die(f"{STS_NAME}: expected one volumeClaimTemplate named data, got {len(data)}")
    got = (((data[0].get("spec") or {}).get("resources") or {}).get("requests") or {}).get("storage")
    if str(got) != want:
        die(f"{STS_NAME}: data claim template requests {got!r}, want {want!r}")
    print(f"  ok: {STS_NAME} data claim template requests {got}")

elif MODE == "guard":
    values_path, guard_path, want_events, want_minutes = ARGS
    rustfs_values = yaml.safe_load(pathlib.Path(values_path).read_text())["rustfs"]
    guard_text = pathlib.Path(guard_path).read_text()
    sts = the_statefulset()
    spec = pod_spec(sts)

    inits = guard_inits(spec)
    if len(inits) != 1:
        names = [c.get("name") for c in spec.get("initContainers") or []]
        die(f"{STS_NAME}: expected exactly one init container {INIT_NAME!r}, got init containers {names}")
    init = inits[0]

    if init.get("image") != rustfs_values["image"]:
        die(f"{INIT_NAME} image {init.get('image')!r} != rustfs.image {rustfs_values['image']!r}")
    if init.get("securityContext") != rustfs_values["containerSecurityContext"]:
        die(
            f"{INIT_NAME} securityContext {init.get('securityContext')!r} != "
            f"rustfs.containerSecurityContext {rustfs_values['containerSecurityContext']!r}"
        )
    if init.get("resources") != rustfs_values["resources"]:
        die(f"{INIT_NAME} resources {init.get('resources')!r} != rustfs.resources {rustfs_values['resources']!r}")

    mounts = init.get("volumeMounts") or []
    if not any(m.get("name") == "data" and m.get("mountPath") == "/data" for m in mounts):
        die(f"{INIT_NAME} does not mount volume 'data' at /data: {mounts!r}")

    command = init.get("command")
    if not isinstance(command, list) or len(command) != 2 or command[0] != "/bin/sh":
        die(f"{INIT_NAME} command must be ['/bin/sh', '<dir>/{GUARD_KEY}'], got {command!r}")
    script_path = command[1]
    if posixpath.basename(script_path) != GUARD_KEY:
        die(f"{INIT_NAME} command runs {script_path!r}, not a file named {GUARD_KEY}")
    script_dir = posixpath.dirname(script_path)

    dir_mounts = [m for m in mounts if m.get("mountPath") == script_dir]
    if len(dir_mounts) != 1:
        die(f"{INIT_NAME}: expected one volumeMount at {script_dir!r} (the script's directory), got {dir_mounts!r}")
    if dir_mounts[0].get("subPath"):
        die(f"{INIT_NAME}: script mount at {script_dir!r} uses subPath; mount the ConfigMap directory")
    volume_name = dir_mounts[0].get("name")
    volumes = [v for v in spec.get("volumes") or [] if v.get("name") == volume_name]
    if len(volumes) != 1:
        die(f"pod spec has {len(volumes)} volumes named {volume_name!r}, want 1")
    cm_ref = volumes[0].get("configMap") or {}
    cm_name = cm_ref.get("name")
    if not cm_name:
        die(f"volume {volume_name!r} is not a configMap volume: {volumes[0]!r}")
    key = GUARD_KEY
    if cm_ref.get("items"):
        mapped = [i for i in cm_ref["items"] if i.get("path") == GUARD_KEY]
        if len(mapped) != 1:
            die(f"configMap volume {volume_name!r} items do not project {GUARD_KEY}: {cm_ref['items']!r}")
        key = mapped[0].get("key")
        if key != GUARD_KEY:
            die(f"configMap volume projects key {key!r} as {GUARD_KEY}; the ConfigMap key must be {GUARD_KEY}")

    cms = [
        doc
        for _, doc in DOCS
        if doc.get("kind") == "ConfigMap" and (doc.get("metadata") or {}).get("name") == cm_name
    ]
    if len(cms) != 1:
        die(f"volume {volume_name!r} names ConfigMap {cm_name!r}, which the chart renders {len(cms)} times")
    data = cms[0].get("data") or {}
    if key not in data:
        die(f"ConfigMap {cm_name!r} has no key {key!r}; keys: {sorted(data)}")
    if data[key] != guard_text:
        die(
            f"ConfigMap {cm_name!r} key {key!r} differs from {guard_path} "
            f"({len(data[key])} vs {len(guard_text)} chars); it must be the file byte for byte"
        )

    env = env_map(init)
    if env.get("EVENTS_DIR") != want_events:
        die(f"{INIT_NAME} EVENTS_DIR={env.get('EVENTS_DIR')!r}, want {want_events!r}")
    if env.get("RETENTION_MINUTES") != want_minutes:
        die(f"{INIT_NAME} RETENTION_MINUTES={env.get('RETENTION_MINUTES')!r}, want {want_minutes!r} (a string)")

    mains = [c for c in spec.get("containers") or [] if c.get("name") == "rustfs"]
    if len(mains) != 1:
        die(f"{STS_NAME}: expected one container named rustfs, got {len(mains)}")
    main = mains[0]
    if main.get("image") != rustfs_values["image"]:
        die(f"rustfs container image {main.get('image')!r} != rustfs.image")
    for probe in ("readinessProbe", "livenessProbe"):
        path = ((main.get(probe) or {}).get("httpGet") or {}).get("path")
        if path != "/health":
            die(f"rustfs container {probe} httpGet.path is {path!r}, want /health")
    if not any(m.get("name") == "data" and m.get("mountPath") == "/data" for m in main.get("volumeMounts") or []):
        die("rustfs container no longer mounts data at /data")

    print(
        f"  ok: {INIT_NAME} image={init['image']} command={command} "
        f"EVENTS_DIR={want_events} RETENTION_MINUTES={want_minutes} ConfigMap={cm_name}"
    )

elif MODE == "no-guard":
    spec = pod_spec(the_statefulset())
    if guard_inits(spec):
        die(f"persistence disabled but {STS_NAME} still has an init container {INIT_NAME!r}")
    cms = guard_configmaps()
    if cms:
        die(f"persistence disabled but the render still carries {GUARD_KEY} in {[str(p) for p, _ in cms]}")
    data = [v for v in spec.get("volumes") or [] if v.get("name") == "data"]
    if len(data) != 1 or "emptyDir" not in data[0]:
        die(f"persistence disabled: expected one emptyDir volume named data, got {data!r}")
    print(f"  ok: persistence disabled renders no {INIT_NAME}, no guard ConfigMap, emptyDir data")

elif MODE == "no-sts":
    if rustfs_statefulsets():
        die(f"rustfs.deploy=false still rendered StatefulSet {STS_NAME}")
    cms = guard_configmaps()
    if cms:
        die(f"rustfs.deploy=false still renders {GUARD_KEY} in {[str(p) for p, _ in cms]}")
    print(f"  ok: rustfs.deploy=false renders no rustfs StatefulSet and no guard ConfigMap")

else:
    die(f"unknown mode {MODE!r}")
PY

echo "=== Assertion 1: default render requests 10Gi for the RustFS data claim ==="
render default
python3 "$CHECKER" claim "$RENDER_DIR" 10Gi

echo "=== Assertion 2: curie.rustfs.claimSize keeps an existing StatefulSet's claim size ==="
TEST_CHART="$TMP/chart"
cp -a "$CHART" "$TEST_CHART"
mkdir -p "$TEST_CHART/storage-guard-fixtures"
# Shaped like `kubectl get statefulset -o yaml`, which is what `lookup` returns.
cat > "$TEST_CHART/storage-guard-fixtures/sts-data-first.yaml" <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rel-curie-rustfs
  namespace: curie
  generation: 3
  resourceVersion: "184467"
  uid: 5b0c6a52-8a3e-4b8e-9d8e-3f4c1e2a7b10
  labels:
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 1
  serviceName: rel-curie-rustfs
  podManagementPolicy: OrderedReady
  persistentVolumeClaimRetentionPolicy:
    whenDeleted: Retain
    whenScaled: Retain
  selector:
    matchLabels:
      app.kubernetes.io/component: rustfs
  template:
    metadata:
      labels:
        app.kubernetes.io/component: rustfs
    spec:
      containers:
        - name: rustfs
          image: rustfs/rustfs:1.0.0-beta.12
  volumeClaimTemplates:
    - apiVersion: v1
      kind: PersistentVolumeClaim
      metadata:
        creationTimestamp: null
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 3Gi
        volumeMode: Filesystem
      status:
        phase: Pending
status:
  availableReplicas: 1
  readyReplicas: 1
  replicas: 1
YAML
cat > "$TEST_CHART/storage-guard-fixtures/sts-data-second.yaml" <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rel-curie-rustfs
  namespace: curie
spec:
  replicas: 1
  serviceName: rel-curie-rustfs
  selector:
    matchLabels:
      app.kubernetes.io/component: rustfs
  template:
    metadata:
      labels:
        app.kubernetes.io/component: rustfs
    spec:
      containers:
        - name: rustfs
          image: rustfs/rustfs:1.0.0-beta.12
  volumeClaimTemplates:
    - apiVersion: v1
      kind: PersistentVolumeClaim
      metadata:
        name: cache
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 1Gi
        volumeMode: Filesystem
    - apiVersion: v1
      kind: PersistentVolumeClaim
      metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 3Gi
        volumeMode: Filesystem
YAML
# The default argument is deliberately not 10Gi, so a helper that ignores it
# and hardcodes the shipped default cannot pass.
cat > "$TEST_CHART/templates/zz-storage-guard-claim-check.yaml" <<'EOF2'
{{- $first := .Files.Get "storage-guard-fixtures/sts-data-first.yaml" | fromYaml }}
{{- $second := .Files.Get "storage-guard-fixtures/sts-data-second.yaml" | fromYaml }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: storage-guard-claim-check
data:
  dataFirst: {{ include "curie.rustfs.claimSize" (dict "statefulSet" $first "default" "7Gi") | quote }}
  dataSecond: {{ include "curie.rustfs.claimSize" (dict "statefulSet" $second "default" "7Gi") | quote }}
  empty: {{ include "curie.rustfs.claimSize" (dict "statefulSet" (dict) "default" "7Gi") | quote }}
  emptyShipped: {{ include "curie.rustfs.claimSize" (dict "statefulSet" (dict) "default" .Values.rustfs.persistence.size) | quote }}
EOF2
HELPER_OUT_DIR="$TMP/helper-out"
helm template rel "$TEST_CHART" --output-dir "$HELPER_OUT_DIR" >"$TMP/helper.log" 2>&1 \
  || fail "2 helper fixture render failed (is curie.rustfs.claimSize defined?): $(cat "$TMP/helper.log")"
HELPER_OUT="$HELPER_OUT_DIR/curie/templates/zz-storage-guard-claim-check.yaml"
[[ -s "$HELPER_OUT" ]] || fail "2 helper fixture template did not render"
python3 - "$HELPER_OUT" <<'PY'
import sys
import yaml

data = yaml.safe_load(open(sys.argv[1]))["data"]
want = {"dataFirst": "3Gi", "dataSecond": "3Gi", "empty": "7Gi", "emptyShipped": "10Gi"}
fails = [f"{k}: got {data.get(k)!r}, want {v!r}" for k, v in want.items() if str(data.get(k, "")).strip() != v]
if fails:
    raise SystemExit("FAIL: 2 curie.rustfs.claimSize:\n  " + "\n  ".join(fails))
print("  ok: existing data claim 3Gi kept (first and non-first template); empty lookup yields the default")
PY

echo "=== Assertion 3: default render carries the storage-guard init container ==="
[[ -f "$GUARD_FILE" ]] || fail "3 guard script $GUARD_FILE does not exist"
python3 "$CHECKER" guard "$RENDER_DIR" "$CHART/values.yaml" "$GUARD_FILE" /data/langfuse/events 2880

echo "=== Assertion 4: retention and bucket flow from values ==="
render retention0 --set langfuse.eventUpload.retentionDays=0
python3 "$CHECKER" guard "$RENDER_DIR" "$CHART/values.yaml" "$GUARD_FILE" /data/langfuse/events 0
render bucket --set rustfs.bucket=other
python3 "$CHECKER" guard "$RENDER_DIR" "$CHART/values.yaml" "$GUARD_FILE" /data/other/events 2880

echo "=== Assertion 5: rustfs.persistence.enabled=false renders no guard ==="
render ephemeral --set rustfs.persistence.enabled=false
python3 "$CHECKER" no-guard "$RENDER_DIR"

echo "=== Assertion 6: rustfs.deploy=false renders no RustFS and no guard ==="
cat > "$TMP/byo.yaml" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: 192.0.2.10/32
      ports: [{ protocol: TCP, port: 443 }]
EOF
render off -f "$TMP/byo.yaml"
python3 "$CHECKER" no-sts "$RENDER_DIR"

echo "=== Assertion 7: values-dev overlay keeps its 3Gi claim ==="
render dev -f "$CHART/values-dev.yaml"
python3 "$CHECKER" claim "$RENDER_DIR" 3Gi

echo
echo "PASS: rustfs storage guard render assertions (#4408): 10Gi default, claim-size helper, storage-guard init container, opt-outs, values-dev."
