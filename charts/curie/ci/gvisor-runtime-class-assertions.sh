#!/usr/bin/env bash
#
# Render-assertion test for issue #3558.
#
# security.gvisor.mode=auto currently stamps runtimeClassName only when Helm
# lookup finds the RuntimeClass. helm template lookup is empty, so a real
# model (agentSandbox.runner.fakeModel=false, or inference.deploy) renders
# every runner SandboxTemplate with no runtimeClassName. The admission
# expression and the security probe check are omitted with it.
#
# Asserts:
#
#   1. Real model, mode auto (the default): every SandboxTemplate
#      spec.podTemplate.spec.runtimeClassName is gvisor, including curie-runner.
#   2. Secondary real-model path: inference.deploy with persistence enabled,
#      fakeModel left unset. Same runtimeClassName assertion.
#   3. NEGATIVE: default helm template (fake model). Every SandboxTemplate
#      omits runtimeClassName.
#   4. NEGATIVE: security.gvisor.mode=off with a real model. Every
#      SandboxTemplate omits runtimeClassName.
#   5. The case 1 render's ValidatingAdmissionPolicy contains a validation
#      expression requiring runtimeClassName == 'gvisor'. The case 3 render
#      does not contain that expression.
#   6. Security probe, fakeModel=false: the Role grants get on
#      sandboxtemplates in extensions.agents.x-k8s.io, REQUIRED_RUNTIME_CLASS
#      is gvisor, RUNNER_TEMPLATES includes curie-runner, and with an acme
#      runner image digest it also includes curie-agent-acme-runner. The
#      probe command defines check_runner_sandbox_template_class.
#   7. That function, extracted from the rendered command (not copied here),
#      exits non-zero and reports FAIL plus the template name when the
#      SandboxTemplate class is empty, and exits 0 when the class is gvisor.
#
# Runnable locally and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"

cleanup() {
  [[ -n "${TMP:-}" && -d "$TMP" ]] && rm -rf -- "$TMP"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render_chart() {
  local name="$1"
  shift
  local out="$TMP/${name}.yaml"
  helm template curie "$CHART" "$@" >"$out"
  printf '%s\n' "$out"
}

ACME_DIGEST="ghcr.io/example.com/curie-runner@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
import pathlib
import re
import sys

import yaml


EXPR = "runtimeClassName == 'gvisor'"
FUNCTION_NAME = "check_runner_sandbox_template_class"


def die(message):
    raise SystemExit(message)


def load_docs(path):
    return [doc for doc in yaml.safe_load_all(pathlib.Path(path).read_text()) if doc]


def sandbox_templates(docs, path):
    templates = [doc for doc in docs if doc.get("kind") == "SandboxTemplate"]
    if not templates:
        die(f"{path}: expected at least one SandboxTemplate")
    names = [((doc.get("metadata") or {}).get("name")) for doc in templates]
    if "curie-runner" not in names:
        die(f"{path}: SandboxTemplate curie-runner is missing; found {names!r}")
    return templates


def runtime_class(template):
    pod_spec = (
        (template.get("spec") or {}).get("podTemplate") or {}
    ).get("spec") or {}
    if "runtimeClassName" not in pod_spec:
        return None
    value = pod_spec.get("runtimeClassName")
    if value is None:
        return None
    if isinstance(value, str) and value == "":
        return ""
    return value


def template_name(template, index):
    return (template.get("metadata") or {}).get("name") or f"#{index}"


def assert_class_present(path):
    docs = load_docs(path)
    for index, template in enumerate(sandbox_templates(docs, path)):
        got = runtime_class(template)
        if got != "gvisor":
            die(
                f"{path}: SandboxTemplate {template_name(template, index)} "
                f"runtimeClassName is {got!r}, want 'gvisor'"
            )
    print("  ok: every SandboxTemplate runtimeClassName is gvisor")


def assert_class_absent(path):
    docs = load_docs(path)
    for index, template in enumerate(sandbox_templates(docs, path)):
        got = runtime_class(template)
        if got not in (None, ""):
            die(
                f"{path}: SandboxTemplate {template_name(template, index)} "
                f"must omit runtimeClassName, got {got!r}"
            )
    print("  ok: every SandboxTemplate omits runtimeClassName")


def validation_expressions(docs):
    expressions = []
    for doc in docs:
        if doc.get("kind") != "ValidatingAdmissionPolicy":
            continue
        validations = (doc.get("spec") or {}).get("validations") or []
        for item in validations:
            if isinstance(item, dict):
                expressions.append(item.get("expression"))
    return expressions


def assert_admission_present(path):
    expressions = validation_expressions(load_docs(path))
    if not any(isinstance(item, str) and EXPR in item for item in expressions):
        die(
            f"{path}: ValidatingAdmissionPolicy has no validation expression "
            f"requiring {EXPR}"
        )
    print(f"  ok: admission policy requires {EXPR}")


def assert_admission_absent(path):
    for item in validation_expressions(load_docs(path)):
        if isinstance(item, str) and "runtimeClassName" in item:
            die(
                f"{path}: fake-model render still has a runtimeClassName "
                f"admission expression: {item!r}"
            )
    print("  ok: fake-model render omits the runtimeClassName admission expression")


def probe_container(docs, path):
    jobs = [doc for doc in docs if doc.get("kind") == "Job"]
    if len(jobs) != 1:
        die(f"{path}: expected exactly one Job, found {len(jobs)}")
    containers = (
        jobs[0].get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    )
    probes = [container for container in containers if container.get("name") == "probe"]
    if len(probes) != 1:
        die(f"{path}: expected exactly one probe container, found {len(probes)}")
    return probes[0]


def env_value(container, name, path):
    entries = [entry for entry in container.get("env") or [] if entry.get("name") == name]
    if len(entries) != 1:
        die(f"{path}: probe env {name!r} appears {len(entries)} times")
    if set(entries[0]) != {"name", "value"}:
        die(f"{path}: probe env {name!r} must be one literal value, got {entries[0]!r}")
    value = entries[0]["value"]
    if not isinstance(value, str):
        die(f"{path}: probe env {name!r} must be a string, got {value!r}")
    return value


def command_script(container, path):
    command = container.get("command") or []
    if not command:
        die(f"{path}: probe container has no command")
    return "\n".join(str(part) for part in command)


def extract_function(script):
    match = re.search(
        r"(?m)^[ \t]*(?:function[ \t]+)?"
        + FUNCTION_NAME
        + r"(?:[ \t]*\(\))?[ \t]*\{",
        script,
    )
    if match is None:
        die(
            f"probe command does not define shell function {FUNCTION_NAME} "
            "(case 7 cannot execute it)"
        )
    start = match.start()
    index = match.end() - 1
    depth = 0
    state = "code"
    while index < len(script):
        char = script[index]
        if state == "code":
            if char == "\\":
                index += 2
                continue
            if char == "'":
                state = "single"
            elif char == '"':
                state = "double"
            elif char == "#" and (index == 0 or script[index - 1].isspace()):
                state = "comment"
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return script[start : index + 1]
        elif state == "single":
            if char == "'":
                state = "code"
        elif state == "double":
            if char == "\\":
                index += 2
                continue
            if char == '"':
                state = "code"
        elif state == "comment":
            if char == "\n":
                state = "code"
        index += 1
    die(f"probe command function {FUNCTION_NAME} has no closing brace")


def role_grants_template_get(docs):
    for doc in docs:
        if doc.get("kind") != "Role":
            continue
        for rule in doc.get("rules") or []:
            groups = rule.get("apiGroups") or []
            resources = rule.get("resources") or []
            verbs = rule.get("verbs") or []
            if (
                "extensions.agents.x-k8s.io" in groups
                and "sandboxtemplates" in resources
                and "get" in verbs
            ):
                return True
    return False


def assert_probe(path, required_tokens):
    docs = load_docs(path)
    if not role_grants_template_get(docs):
        die(
            f"{path}: no Role grants get on sandboxtemplates in "
            "apiGroup extensions.agents.x-k8s.io"
        )
    container = probe_container(docs, path)
    required = env_value(container, "REQUIRED_RUNTIME_CLASS", path)
    if required != "gvisor":
        die(f"{path}: REQUIRED_RUNTIME_CLASS is {required!r}, want 'gvisor'")
    tokens = env_value(container, "RUNNER_TEMPLATES", path).split()
    missing = [token for token in required_tokens if token not in tokens]
    if missing:
        die(
            f"{path}: RUNNER_TEMPLATES missing {missing!r}; "
            f"whitespace tokens are {tokens!r}"
        )
    extract_function(command_script(container, path))
    print(
        "  ok: probe Role, REQUIRED_RUNTIME_CLASS, RUNNER_TEMPLATES "
        f"{required_tokens!r}, and {FUNCTION_NAME}"
    )


def write_function(path, dest):
    docs = load_docs(path)
    container = probe_container(docs, path)
    body = extract_function(command_script(container, path))
    pathlib.Path(dest).write_text(body + "\n")
    print(f"  ok: extracted {FUNCTION_NAME} from the rendered probe command")


def main():
    path = sys.argv[1]
    mode = sys.argv[2]
    if mode == "class-present":
        assert_class_present(path)
        return
    if mode == "class-absent":
        assert_class_absent(path)
        return
    if mode == "admission-present":
        assert_admission_present(path)
        return
    if mode == "admission-absent":
        assert_admission_absent(path)
        return
    if mode == "probe":
        assert_probe(path, sys.argv[3:])
        return
    if mode == "extract":
        write_function(path, sys.argv[3])
        return
    die(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
PY

echo "=== Assertion 1: real model, mode auto, empty lookup stamps gvisor ==="
REAL_RENDER="$(render_chart real-model --set agentSandbox.runner.fakeModel=false)"
python3 "$CHECKER" "$REAL_RENDER" class-present

echo "=== Assertion 2: inference.deploy is a real model even with fakeModel unset ==="
INFERENCE_RENDER="$(render_chart inference-deploy \
  --set inference.deploy=true \
  --set inference.persistence.enabled=true)"
python3 "$CHECKER" "$INFERENCE_RENDER" class-present

echo "=== Assertion 3: default fake-model render omits runtimeClassName ==="
DEFAULT_RENDER="$(render_chart default)"
python3 "$CHECKER" "$DEFAULT_RENDER" class-absent

echo "=== Assertion 4: mode off omits runtimeClassName on a real model ==="
OFF_RENDER="$(render_chart mode-off \
  --set security.gvisor.mode=off \
  --set agentSandbox.runner.fakeModel=false)"
python3 "$CHECKER" "$OFF_RENDER" class-absent

echo "=== Assertion 5: admission expression follows the effective runtime class ==="
python3 "$CHECKER" "$REAL_RENDER" admission-present
python3 "$CHECKER" "$DEFAULT_RENDER" admission-absent

echo "=== Assertion 6: security probe contract for the runner template class ==="
PROBE_RENDER="$(render_chart probe \
  --show-only templates/security-probe.yaml \
  --set agentSandbox.runner.fakeModel=false)"
python3 "$CHECKER" "$PROBE_RENDER" probe curie-runner

PROBE_ACME_RENDER="$(render_chart probe-acme \
  --show-only templates/security-probe.yaml \
  --set agentSandbox.runner.fakeModel=false \
  --set "agentSandbox.runnerImages.acme=${ACME_DIGEST}")"
python3 "$CHECKER" "$PROBE_ACME_RENDER" probe curie-runner curie-agent-acme-runner

echo "=== Assertion 7: extracted probe function fails closed, then passes on gvisor ==="
FN_FILE="$TMP/check_runner_sandbox_template_class.sh"
python3 "$CHECKER" "$PROBE_RENDER" extract "$FN_FILE"

STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"
cat > "$STUB_BIN/kubectl" <<'EOF'
#!/usr/bin/env bash
# Stub for: kubectl get sandboxtemplate <name> -n <ns> -o jsonpath={.spec.podTemplate.spec.runtimeClassName}
set -euo pipefail
want='jsonpath={.spec.podTemplate.spec.runtimeClassName}'
if [[ $# -eq 7 && "$1" == "get" && "$2" == "sandboxtemplate" && "$4" == "-n" && "$6" == "-o" && "$7" == "$want" ]]; then
  if [[ -n "${FIXTURE_RUNTIME_CLASS:-}" ]]; then
    printf '%s\n' "$FIXTURE_RUNTIME_CLASS"
  fi
  exit 0
fi
echo "unexpected kubectl invocation: $*" >&2
exit 1
EOF
chmod 0755 "$STUB_BIN/kubectl"

run_extracted() {
  local fixture_class="$1"
  local log="$2"
  set +e
  PATH="$STUB_BIN:$PATH" \
    FIXTURE_RUNTIME_CLASS="$fixture_class" \
    REQUIRED_RUNTIME_CLASS=gvisor \
    NS=curie \
    RUNNER_TEMPLATES=curie-runner \
    bash --noprofile --norc -c '
fail=0
source "$1"
check_runner_sandbox_template_class
exit "$fail"
' bash "$FN_FILE" >"$log" 2>&1
  local rc=$?
  set -e
  printf '%s\n' "$rc"
}

EMPTY_LOG="$TMP/probe-empty.log"
EMPTY_RC="$(run_extracted "" "$EMPTY_LOG")"
if [[ "$EMPTY_RC" -eq 0 ]]; then
  fail "empty SandboxTemplate class: check_runner_sandbox_template_class exited 0; output: $(cat "$EMPTY_LOG")"
fi
if ! grep -Eq '(^|[^[:alnum:]_])FAIL([^[:alnum:]_]|$)' "$EMPTY_LOG"; then
  fail "empty SandboxTemplate class: output missing the word FAIL: $(cat "$EMPTY_LOG")"
fi
if ! grep -Eq '(^|[^[:alnum:]_-])curie-runner([^[:alnum:]_-]|$)' "$EMPTY_LOG"; then
  fail "empty SandboxTemplate class: output missing template name curie-runner: $(cat "$EMPTY_LOG")"
fi
echo "  ok: empty class exits non-zero and reports FAIL for curie-runner"

GVISOR_LOG="$TMP/probe-gvisor.log"
GVISOR_RC="$(run_extracted gvisor "$GVISOR_LOG")"
if [[ "$GVISOR_RC" -ne 0 ]]; then
  fail "gvisor SandboxTemplate class: check_runner_sandbox_template_class exited ${GVISOR_RC}; output: $(cat "$GVISOR_LOG")"
fi
echo "  ok: gvisor class exits 0"

echo "PASS: real-model auto with empty lookup stamps gvisor on every runner SandboxTemplate, fake-model and mode off omit it, the admission expression follows that class, and the security probe fails when curie-runner lacks gvisor"
