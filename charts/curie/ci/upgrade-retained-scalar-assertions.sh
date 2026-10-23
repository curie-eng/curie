#!/usr/bin/env bash
#
# `curie cluster upgrade` must hand Helm back the retained values it read, with
# every scalar still meaning what it meant (#2741).
#
# The upgrade driver reads the installed overlay with `helm get values -o yaml`,
# parses it into a serde_json::Value, and re-serializes it to a temp file that
# is handed straight back to Helm via `-f` -- once for the pre-mutation schema
# render, once for the Apply itself. The re-serializer emits YAML 1.2, where
# `off` is an ordinary string and needs no quotes. Helm's Go parser is YAML
# 1.1, where a bare `off` is the BOOLEAN false. So an install carrying
# `security.gvisor.mode: "off"` (exactly what `values-e2e-nogvisor.yaml`
# selects) comes back as `mode: false` on upgrade, stops matching the `off`
# branch in `curie.gvisor.className`, and the upgrade starts rendering the
# blocking gVisor enforcement preflight plus `runtimeClassName: gvisor` on
# runner pods -- which a cluster with no runsc RuntimeClass refuses at
# admission. The operator changed nothing; the round trip changed it for them.
#
# The same class covers every YAML 1.1 boolean word: yes, no, on, off, y, n.
# The fixture therefore carries three of them in two unrelated places, so a fix
# that special-cases gVisor is not enough to pass.
#
# Two things make these assertions mean something:
#
#   * the overlay is captured from the REAL CLI, at the real `-f` boundary, on
#     BOTH paths that build it (`retained_overlay` and the `--forward-only`
#     `merge_forward_only`). A unit round trip through the Rust types cannot
#     see this bug, because the bug is in what the bytes mean to the NEXT
#     reader.
#   * the captured bytes are read back by real `helm` -- the YAML 1.1 parser
#     whose opinion actually ships -- asked both whether gVisor is still off
#     and what the two fixture env scalars rendered as. Reading it back with a
#     YAML 1.2 parser would agree with the writer and prove nothing, and
#     PyYAML is only a SECONDARY reader here: its implicit resolver does not
#     treat a bare `y`/`n` as a boolean, so it cannot see the whole class.
#
# The same lane also carries a factory release (#4889) through the real CLI:
# the captured overlay must keep factory.sandboxEgress, the explicit
# agentResourceProfiles opt-out and every explicit envelope override, so real
# `helm template` of it renders the full factory envelope for the factory agent
# and the standard one elsewhere; a release without the factory stays standard.
#
# `helm`/`kubectl` are the recording stubs `cli/tests/data/upgrade-driver.py`
# already provides for `cluster_upgrade_live.rs`, so the run is offline and
# touches no cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO="$(cd "$CHART/../.." && pwd)"
DRIVER="$REPO/cli/tests/data/upgrade-driver.py"

if [[ ! -f "$DRIVER" ]]; then
  echo "FAIL: the recording helm/kubectl driver is missing: $DRIVER" >&2
  exit 1
fi

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

cargo test --locked --manifest-path "$REPO/cli/Cargo.toml" --test integration cluster_up_inference::retained_ >&2

# --- the retained install ----------------------------------------------------
# Three YAML 1.1 boolean words, in three unrelated places, all of them strings
# in the installed release. JSON is YAML, and JSON has no bare scalars, so the
# fixture itself cannot be the thing that loses the quoting.
RETAINED='{
  "connectorCaller": {"existingSecret": "acme-caller-pair"},
  "security": {"gvisor": {"mode": "off"}},
  "api": {"extraEnv": [
    {"name": "CURIE_FIXTURE_FLAG", "value": "yes"},
    {"name": "CURIE_FIXTURE_SHORT", "value": "n"}
  ]}
}'

run_capture() {
  # run_capture <label> <root> [extra curie flags...]
  # The installed release's values are $RETAINED_DOC when set, else $RETAINED.
  local label="$1" root="$2"
  shift 2
  mkdir -p "$root"
  local name
  for name in helm kubectl; do
    cp "$DRIVER" "$root/$name"
    chmod +x "$root/$name"
  done
  printf '%s\n' "${RETAINED_DOC:-$RETAINED}" > "$root/retained.json"
  # A non-zero exit is tolerated: the capture happens during Validate, before
  # any mutation, and the assertions below refuse to pass without one.
  PATH="$root:$PATH" \
  UPGRADE_DRIVER_ROOT="$root" \
  UPGRADE_DRIVER_SCENARIO=healthy \
    "$BIN" --color=never --json cluster upgrade \
      --to 0.9.0 --namespace ns --release rel --chart "$CHART" --yes "$@" \
      > "$root/stdout.json" 2> "$root/stderr.txt" </dev/null || true

  shopt -s nullglob
  local captures=("$root"/values-*.yaml)
  shopt -u nullglob
  if [[ ${#captures[@]} -eq 0 ]]; then
    echo "FAIL: the $label run handed helm no -f values document, so this test" >&2
    echo "      asserted nothing. The run's stderr was:" >&2
    sed 's/^/    /' "$root/stderr.txt" >&2
    exit 1
  fi
  echo "OK: the $label run captured ${#captures[@]} values document(s)"
}

assert_captures() {
  # assert_captures <label> <root> [expect-forward-only]
  local label="$1" root="$2"
  shift 2
  shopt -s nullglob
  local captures=("$root"/values-*.yaml)
  shopt -u nullglob

  local capture
  for capture in "${captures[@]}"; do
    # --- real Helm, the parser whose opinion ships ---------------------------
    # A real (non-fake) model is what makes the enforcement preflight render at
    # all under `auto`, so it is the render that can tell `off` from `false`.
    local rendered="$capture.rendered"
    if ! helm template t "$CHART" \
        --set agentSandbox.runner.fakeModel=false \
        -f "$capture" > "$rendered" 2> "$capture.err"; then
      echo "FAIL: $label: helm refused the overlay curie handed it ($capture)" >&2
      sed 's/^/    /' "$capture.err" >&2
      exit 1
    fi
    local signal
    for signal in 'preflight-gvisor' 'runtimeClassName: gvisor'; do
      if grep -qF "$signal" "$rendered"; then
        echo "FAIL: $label: the retained overlay no longer reads as gVisor OFF." >&2
        echo "      Rendering $capture produced '$signal', so Helm read" >&2
        echo "      security.gvisor.mode as something other than the string \"off\"." >&2
        echo "      The captured document was:" >&2
        sed 's/^/    /' "$capture" >&2
        exit 1
      fi
    done

    # --- real Helm again, this time on the two fixture env scalars -----------
    # The gVisor signals above only cover security.gvisor.mode. The other two
    # boolean words have to be read back through Helm as well, because PyYAML
    # is NOT a faithful stand-in for Helm's YAML 1.1: PyYAML's implicit
    # resolver accepts yes/no/on/off/true/false but NOT a bare `y` or `n`,
    # while Helm's Go parser resolves both. A capture carrying `value: n`
    # therefore passes every PyYAML check below while Helm renders
    # CURIE_FIXTURE_SHORT as the boolean false. Do not simplify this away.
    #
    # The observable is the rendered manifest: `curie.extraEnv` toYaml's the
    # list, so a value Helm read as a boolean comes out as an unquoted
    # `value: false` while a string comes out quoted. Helm quotes every
    # boolean-shaped string it emits, so reading its OWN output back with
    # PyYAML is unambiguous -- the ambiguity only exists on the input side.
    python3 - "$label" "$capture" "$rendered" <<'PY'
import sys

import yaml

label, capture, rendered = sys.argv[1], sys.argv[2], sys.argv[3]
expected = {"CURIE_FIXTURE_FLAG": "yes", "CURIE_FIXTURE_SHORT": "n"}

seen = {}
for doc in yaml.safe_load_all(open(rendered)):
    if not isinstance(doc, dict):
        continue
    stack = [doc]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            name = node.get("name")
            if name in expected and "value" in node:
                seen[name] = node["value"]
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)

failures = []
for name, want in expected.items():
    if name not in seen:
        failures.append(
            f"{name} never reached the rendered manifest, so Helm was never "
            f"asked what its value means"
        )
        continue
    value = seen[name]
    if not isinstance(value, str) or value != want:
        failures.append(
            f"{name} rendered as {value!r} ({type(value).__name__}), expected "
            f"the string {want!r}: Helm's YAML 1.1 parser resolved the "
            f"unquoted scalar in the retained overlay to a boolean"
        )

if failures:
    print(f"FAIL: {label}: helm rendered {capture} with corrupted env scalars", file=sys.stderr)
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    print("  the captured document was:", file=sys.stderr)
    for line in open(capture).read().splitlines():
        print("    " + line, file=sys.stderr)
    raise SystemExit(1)
PY

    # --- PyYAML, a secondary reading of the captured bytes -------------------
    # Kept for the structural checks (key presence, the --forward-only marker)
    # and as a second opinion on the scalars it CAN resolve. It is not the
    # authority: see the `y`/`n` divergence above.
    python3 - "$label" "$capture" "$@" <<'PY'
import sys

import yaml

label, path = sys.argv[1], sys.argv[2]
expect_forward_only = "expect-forward-only" in sys.argv[3:]
text = open(path).read()
doc = yaml.safe_load(text) or {}


def at(*keys):
    """Resolve a path of mapping keys and list indices."""
    node = doc
    for key in keys:
        if isinstance(key, int):
            if not isinstance(node, list) or len(node) <= key:
                return None, False
        elif not isinstance(node, dict) or key not in node:
            return None, False
        node = node[key]
    return node, True


failures = []
expected = [
    (("security", "gvisor", "mode"), "off"),
    (("api", "extraEnv", 0, "value"), "yes"),
    (("api", "extraEnv", 1, "value"), "n"),
]
for keys, want in expected:
    value, present = at(*keys)
    dotted = ".".join(str(key) for key in keys)
    if not present:
        failures.append(f"{dotted} was dropped from the retained overlay entirely")
    elif not isinstance(value, str) or value != want:
        failures.append(
            f"{dotted} came back as {value!r} ({type(value).__name__}), "
            f"expected the string {want!r}: the round trip emitted it unquoted "
            f"and a YAML 1.1 reader resolved it to a boolean"
        )

# Non-vacuity for the second path: the flag must actually have reached the
# overlay builder, or this run is just a rerun of the first one. Asserted
# structurally, so it holds whatever serialization the fix chooses.
if expect_forward_only:
    value, present = at("api", "migrate", "forwardOnly")
    if not present or value is not True:
        failures.append(
            "--forward-only did not put api.migrate.forwardOnly: true in the "
            f"overlay (got {value!r}), so the second path was never exercised"
        )

if failures:
    print(f"FAIL: {label}: {path}", file=sys.stderr)
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    print("  the captured document was:", file=sys.stderr)
    for line in text.splitlines():
        print("    " + line, file=sys.stderr)
    raise SystemExit(1)
PY
  done
  echo "OK: $label preserved every boolean-shaped string across the round trip"
}

# --- path 1: retained_overlay ------------------------------------------------
run_capture "plain upgrade" "$TMP/plain"
assert_captures "plain upgrade" "$TMP/plain"

# --- path 2: merge_forward_only ----------------------------------------------
# `--forward-only` rebuilds the overlay through a second serializer. Asserting
# the marker it injects keeps this from silently degrading into a rerun of
# path 1 if the flag ever stops reaching the overlay builder.
run_capture "--forward-only upgrade" "$TMP/forward" --forward-only
assert_captures "--forward-only upgrade" "$TMP/forward" expect-forward-only

# --- path 3: the factory sandbox resource profile (#4889) ---------------------
# A release whose operator selected the factory and moved single fields must
# get back, through the real `curie cluster upgrade` retained overlay, the full
# factory envelope with every explicit override still winning: the profile is
# chart-derived from factory.sandboxEgress plus factory.enabled, so dropping or
# rewriting any of these keys on the round trip changes what the sandbox gets.
PROFILE_FIXTURE="$SCRIPT_DIR/fixtures/factory-resource-profile.yaml"
[[ -f "$PROFILE_FIXTURE" ]] || { echo "FAIL: missing fixture $PROFILE_FIXTURE" >&2; exit 1; }
FACTORY_RETAINED="$(python3 - "$PROFILE_FIXTURE" <<'PY'
import json, sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
doc["factory"]["sandboxEgress"]["acme-optout"] = True
caller_pair = "acme-caller-pair"  # a Secret name, as in the other paths
doc["connectorCaller"] = {"existingSecret": caller_pair}
doc["security"] = {"gvisor": {"mode": "off"}}
doc["agentSandbox"] = {
    "agentResourceProfiles": {"acme-optout": "standard"},
    "workspaceSizeLimits": {"acme-factory": "18Gi"},
    "runner": {"resources": {"limits": {"cpu": "3"}}},
    "resourceProfiles": {"factory": {"tmpSizeLimit": "6Gi"}},
}
print(json.dumps(doc, indent=2))
PY
)"
CONTROL_RETAINED="$(python3 -c 'import json; print(json.dumps({"connectorCaller": {"existingSecret": "acme-" + "caller-pair"}, "security": {"gvisor": {"mode": "off"}}}, indent=2))')"

PROFILE_CHECK="$TMP/profile-check.py"
cat > "$PROFILE_CHECK" <<'PY'
"""profile-check.py <label> <capture> <rendered> <factory|control>"""
import sys

import yaml

label, capture, rendered, mode = sys.argv[1:5]
failures = []

STD = {"cpu": ["50m", "3"], "memory": ["192Mi", "768Mi"], "ephemeral-storage": ["512Mi", "4Gi"],
       "workspace": "1Gi", "/tmp": "512Mi", "/home/runner": "512Mi"}
FACTORY = {"cpu": ["50m", "3"], "memory": ["8Gi", "8Gi"], "ephemeral-storage": ["10Gi", "20Gi"],
           "workspace": "18Gi", "/tmp": "6Gi", "/home/runner": "15Gi"}
if mode == "factory":
    want = {"generic": STD, "acme-optout": STD, "acme-factory": FACTORY}
else:
    want = {"generic": dict(STD, cpu=["50m", "1"])}

templates = {d["metadata"]["name"]: d for d in yaml.safe_load_all(open(rendered))
             if d and d.get("kind") == "SandboxTemplate"}
agents = sorted(n.split("-agent-", 1)[1][: -len("-runner")] for n in templates if "-agent-" in n)
want_agents = sorted(k for k in want if k != "generic")
if agents != want_agents:
    failures.append(f"per-agent SandboxTemplates {agents}, want {want_agents}")
for key, envelope in want.items():
    hits = [t for n, t in templates.items()
            if ("-agent-" not in n if key == "generic" else n.endswith(f"-agent-{key}-runner"))]
    if len(hits) != 1:
        failures.append(f"want one {key} SandboxTemplate, got {sorted(templates)}")
        continue
    spec = hits[0]["spec"]["podTemplate"]["spec"]
    runner = next(c for c in spec["containers"] if c["name"] == "runner")
    mounts = {m["name"]: m["mountPath"] for m in runner.get("volumeMounts", [])}
    for c in [runner] + list(spec.get("initContainers") or []):
        res = c.get("resources") or {}
        for r in ("cpu", "memory", "ephemeral-storage"):
            pair = [str((res.get(side) or {}).get(r)) for side in ("requests", "limits")]
            if pair != envelope[r]:
                failures.append(f"{key}: container {c['name']} {r} {pair}, want {envelope[r]}")
    sizes = {}
    for v in spec.get("volumes", []):
        if v["name"] == "workspace":
            sizes["workspace"] = str(v["emptyDir"].get("sizeLimit"))
        elif v["name"].startswith("writable-"):
            sizes[mounts.get(v["name"], v["name"])] = str(v["emptyDir"].get("sizeLimit"))
    for label_ in ("workspace", "/tmp", "/home/runner"):
        if sizes.get(label_) != envelope[label_]:
            failures.append(f"{key}: volume {label_} sizeLimit {sizes.get(label_)!r}, want {envelope[label_]!r}")

if mode == "factory":
    doc = yaml.safe_load(open(capture)) or {}

    def at(*keys):
        node = doc
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                return None
            node = node[key]
        return node

    for keys, value in [
        (("factory", "enabled"), True),
        (("factory", "sandboxEgress", "acme-factory"), True),
        (("factory", "sandboxEgress", "acme-optout"), True),
        (("agentSandbox", "agentResourceProfiles", "acme-optout"), "standard"),
        (("agentSandbox", "workspaceSizeLimits", "acme-factory"), "18Gi"),
        (("agentSandbox", "runner", "resources", "limits", "cpu"), "3"),
        (("agentSandbox", "resourceProfiles", "factory", "tmpSizeLimit"), "6Gi"),
    ]:
        if at(*keys) != value:
            failures.append(f"{'.'.join(keys)} came back as {at(*keys)!r}, want {value!r}")

if failures:
    print(f"FAIL: {label}: {capture}", file=sys.stderr)
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    raise SystemExit(1)
PY

assert_profile_captures() {
  # assert_profile_captures <label> <root> <factory|control>
  local label="$1" root="$2" mode="$3"
  shopt -s nullglob
  local captures=("$root"/values-*.yaml)
  shopt -u nullglob
  local capture
  for capture in "${captures[@]}"; do
    if ! helm template t "$CHART" -f "$capture" > "$capture.profile" 2> "$capture.profile.err"; then
      echo "FAIL: $label: helm refused the overlay curie handed it ($capture)" >&2
      sed 's/^/    /' "$capture.profile.err" >&2
      exit 1
    fi
    python3 "$PROFILE_CHECK" "$label" "$capture" "$capture.profile" "$mode"
  done
  echo "OK: $label kept the sandbox resource envelope across the retained-values round trip"
}

RETAINED_DOC="$FACTORY_RETAINED" run_capture "factory profile upgrade" "$TMP/factory"
assert_profile_captures "factory profile upgrade" "$TMP/factory" factory

RETAINED_DOC="$CONTROL_RETAINED" run_capture "non-factory control upgrade" "$TMP/control"
assert_profile_captures "non-factory control upgrade" "$TMP/control" control

printf '%s\n' 'Retained-values scalar preservation assertions passed for both overlay paths.'
printf '%s\n' 'Retained-values factory resource profile assertions passed (factory envelope, overrides, non-factory control).'
