#!/usr/bin/env bash
#
# Render-assertion: the sandbox staging init containers say WHY they staged
# nothing (issue #2612).
#
# A SandboxClaim `spec.env` entry with no `containerName` is injected into the
# runner container only -- `envVarsInjectionPolicy: Overrides` never reaches an
# init container without one. So a hand-written claim carrying
# CURIE_BUNDLE_REF / CURIE_WORKSPACE_REF / CURIE_WORKSPACE_SHA256 leaves
# bundle-fetch, bundle-extract and workspace-init on the template's own empty
# defaults. All three take their no-op path, exit 0, and the runner boots over
# an empty plugin dir and crash-loops on [manifest.missing] -- observed live on
# chart 0.8.7 while closing #2571 AC 5, with every container reporting success.
#
# The no-op itself is correct and must stay: a warm or unbound pod has no ref
# and must not fail. What changes is that the no-op now NAMES the one mistake
# that produces it, in the log of the container that took it, so the operator
# reading `kubectl logs -c bundle-fetch` is told about `containerName` instead
# of being told "skipping".
#
# Proves:
#   (a) Every staging init container the SandboxTemplate renders emits the
#       shared notice on its no-op path, naming its OWN container name and the
#       env key it consumes.
#   (b) The notice names `containerName` -- the actual fix -- not just the
#       absent variable.
#   (c) The old bare "skipping" wording, which said nothing about the cause, is
#       gone from every staging init container.
#   (d) BEHAVIORAL: each staging init container's rendered command is EXECUTED
#       with an empty env, and its actual stdout must carry the notice. Static
#       text matching alone is not enough -- commenting the logging statements
#       out leaves the wording in the command string, so a text-only checker
#       accepts the exact regression this script exists to prevent. Running the
#       branch is the only assertion that cannot be satisfied by a comment.
#   (e) NEGATIVE: an init container whose no-op branch drops the notice fails
#       the same checker the default render uses.
#
# The chart/worker half of this contract -- that the worker copies each staging
# key per init container with an explicit containerName, and that the container
# names have not drifted -- is pinned in
# apps/worker/tests/sandbox/test_claim_env_init_container_parity.py.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

SANDBOX_TPL=templates/agent-sandbox.yaml

fail() { echo "FAIL: $*" >&2; exit 1; }

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import sys
import yaml

# container name -> the env key whose absence takes the no-op path.
STAGING_INIT = {
    "bundle-fetch": "CURIE_BUNDLE_REF",
    "bundle-extract": "CURIE_BUNDLE_REF",
    "workspace-init": "CURIE_WORKSPACE_REF",
}


def sandbox_init_containers(path):
    with open(path) as fh:
        for doc in yaml.safe_load_all(fh):
            if doc and doc.get("kind") == "SandboxTemplate":
                spec = doc["spec"]["podTemplate"]["spec"]
                return {c["name"]: c for c in (spec.get("initContainers") or [])}
    raise SystemExit(f"{path}: no SandboxTemplate rendered")


def main(argv):
    label, path = argv[1], argv[2]
    expected = set(argv[3:])
    rendered = sandbox_init_containers(path)
    present = expected & set(rendered)
    if present != expected:
        raise SystemExit(
            f"{label}: expected staging init containers {sorted(expected)}, "
            f"rendered {sorted(rendered)}"
        )
    for name in sorted(present):
        command = "\n".join(str(part) for part in rendered[name].get("command") or [])
        key = STAGING_INIT[name]
        if "containerName" not in command:
            raise SystemExit(
                f"{label}: init container {name!r} does not mention "
                "'containerName' on its no-op path. A claim that set "
                f"{key} would be told nothing about why staging was skipped "
                "(#2612)."
            )
        if f"containerName: {name}" not in command:
            raise SystemExit(
                f"{label}: init container {name!r} does not name ITSELF as the "
                "containerName to target; an operator copying the hint would "
                "target the wrong container (#2612)."
            )
        if key not in command:
            raise SystemExit(
                f"{label}: init container {name!r} no-op notice does not name "
                f"{key} (#2612)."
            )
        if "skipping bundle fetch" in command or "starting with an empty plugin dir" in command:
            raise SystemExit(
                f"{label}: init container {name!r} still carries the pre-#2612 "
                "bare wording, which reports the no-op without its cause."
            )
    print(f"{label}: OK ({', '.join(sorted(present))})")


if __name__ == "__main__":
    main(sys.argv)
PY

echo "=== (a)(b)(c) default render: bundle staging init containers ==="
helm template rel "$CHART" --set agentSandbox.deploy=true \
  --show-only "$SANDBOX_TPL" > "$TMP/default.yaml"
python3 "$CHECKER" "default values" "$TMP/default.yaml" bundle-fetch bundle-extract \
  || fail "default render: staging init containers do not explain their no-op"

echo "=== (a)(b)(c) workspace staging init container ==="
helm template rel "$CHART" --set agentSandbox.deploy=true \
  --set agentSandbox.runner.workspace.enabled=true \
  --show-only "$SANDBOX_TPL" > "$TMP/workspace.yaml"
python3 "$CHECKER" "workspace enabled" "$TMP/workspace.yaml" \
  bundle-fetch bundle-extract workspace-init \
  || fail "workspace render: staging init containers do not explain their no-op"

echo "=== (d) BEHAVIORAL: run each no-op branch and read its real output ==="
# Render against a temp bundle mount path so the containers' own pre-guard setup
# (bundle-extract's `mkdir -p <mountPath>/current`) works outside a pod, then
# execute each rendered command verbatim with an EMPTY env -- which is exactly
# what an init container sees when the claim's staging entry never reached it.
# workspace.mountPath is pinned by values.schema.json and needs no override:
# workspace-init only constructs pathlib.Path objects before its guard, so it
# never touches /workspace on the no-op path.
RUNROOT="$TMP/run"
mkdir -p "$RUNROOT/bundles"
helm template rel "$CHART" --set agentSandbox.deploy=true \
  --set agentSandbox.runner.workspace.enabled=true \
  --set "agentSandbox.runner.bundleFetch.mountPath=$RUNROOT/bundles" \
  --show-only "$SANDBOX_TPL" > "$TMP/runnable.yaml"

EXTRACT="$TMP/extract.py"
cat >"$EXTRACT" <<'PY'
import sys
import yaml

path, name = sys.argv[1], sys.argv[2]
for doc in yaml.safe_load_all(open(path)):
    if doc and doc.get("kind") == "SandboxTemplate":
        for c in doc["spec"]["podTemplate"]["spec"].get("initContainers") or []:
            if c["name"] == name:
                # argv after the interpreter: `/bin/sh -c <script>` or
                # `python -c <script>`. The script is the last element.
                sys.stdout.write(str(c["command"][-1]))
                raise SystemExit(0)
raise SystemExit(f"{path}: no init container named {name}")
PY

run_no_op() {
  # $1 = container name, $2 = interpreter argv0, $3 = the env key it consumes
  local name="$1" interpreter="$2" key="$3" script out
  script="$TMP/$name.script"
  python3 "$EXTRACT" "$TMP/runnable.yaml" "$name" > "$script" \
    || fail "could not extract the rendered command for $name"
  # env -i: no CURIE_* at all, the mis-shaped-claim condition exactly.
  out="$(env -i PATH="$PATH" "$interpreter" "$script" 2>&1)" \
    || fail "$name: its no-op path exited non-zero with an empty env: $out"
  case "$out" in
    *"containerName: $name"*) ;;
    *) fail "$name: ran its no-op path and printed $(printf '%q' "$out") -- no 'containerName: $name' guidance in the REAL output. A notice that exists only in the command text (commented out, or after an early exit) does not help anyone reading kubectl logs." ;;
  esac
  case "$out" in
    *"$key"*) ;;
    *) fail "$name: its no-op output does not name $key" ;;
  esac
  echo "$name: executed its no-op path, stdout names $key and containerName: $name"
}

run_no_op bundle-fetch /bin/sh CURIE_BUNDLE_REF
run_no_op bundle-extract /bin/sh CURIE_BUNDLE_REF
run_no_op workspace-init python3 CURIE_WORKSPACE_REF

echo "=== (e) NEGATIVE: a no-op branch that drops the notice must fail ==="
python3 - "$TMP/workspace.yaml" "$TMP/mutant.yaml" <<'PY'
import re
import sys

src, dest = sys.argv[1], sys.argv[2]
text = open(src).read()
# Strip the containerName guidance out of the rendered notice, leaving the
# pre-#2612 shape: the no-op is reported, its cause is not.
mutated = re.sub(
    r"If you set CURIE_[A-Z_]+ on a SandboxClaim[^\"\n]*?containerName: [a-z-]+\.",
    "skipping.",
    text,
)
if mutated == text:
    raise SystemExit("mutation did not apply: the notice wording moved")
open(dest, "w").write(mutated)
PY
if python3 "$CHECKER" "mutant" "$TMP/mutant.yaml" bundle-fetch bundle-extract workspace-init \
  > "$TMP/mutant.out" 2>&1; then
  cat "$TMP/mutant.out" >&2
  fail "NEGATIVE case passed: a no-op path with no containerName guidance was accepted"
fi
echo "mutant correctly rejected: $(head -1 "$TMP/mutant.out")"

echo "ALL CLAIM-ENV INIT CONTAINER ASSERTIONS PASSED"
