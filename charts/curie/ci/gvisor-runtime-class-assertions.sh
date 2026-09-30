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
#   7. The rendered probe command (the Job container /bin/bash -c script, not
#      an extracted function) exits non-zero when the SandboxTemplate class is
#      empty. Its output contains the bad-template line (FAIL and curie-runner)
#      and SECURITY PROBE: FAIL. With the class set to gvisor it exits 0,
#      prints SECURITY PROBE: PASS, and does not print FAIL: SandboxTemplate.
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

echo "=== Assertion 7: rendered probe fails closed, then passes on gvisor ==="
PROBE_ENV="$TMP/probe-env.sh"
PROBE_SCRIPT="$TMP/probe-script.sh"
python3 - "$PROBE_RENDER" "$PROBE_ENV" "$PROBE_SCRIPT" <<'PY'
import pathlib
import re
import shlex
import sys

import yaml

path, env_out, script_out = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(pathlib.Path(path).read_text()) if doc]
jobs = [doc for doc in docs if doc.get("kind") == "Job"]
if len(jobs) != 1:
    raise SystemExit(f"{path}: expected exactly one Job, found {len(jobs)}")
containers = (
    jobs[0].get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
)
probes = [container for container in containers if container.get("name") == "probe"]
if len(probes) != 1:
    raise SystemExit(f"{path}: expected exactly one probe container, found {len(probes)}")
container = probes[0]
lines = []
for entry in container.get("env") or []:
    if not isinstance(entry, dict) or set(entry) != {"name", "value"}:
        raise SystemExit(f"{path}: probe env entry must be one literal name/value, got {entry!r}")
    name = entry["name"]
    value = entry["value"]
    if not isinstance(name, str) or not isinstance(value, str):
        raise SystemExit(f"{path}: probe env name/value must be strings, got {entry!r}")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None:
        raise SystemExit(f"{path}: probe env name is not a shell identifier: {name!r}")
    lines.append(f"export {name}={shlex.quote(value)}")
if not lines:
    raise SystemExit(f"{path}: probe container has no env")
pathlib.Path(env_out).write_text("\n".join(lines) + "\n")
command = container.get("command") or []
if (
    len(command) != 3
    or command[0] != "/bin/bash"
    or command[1] != "-c"
    or not isinstance(command[2], str)
    or command[2] == ""
):
    raise SystemExit(f"{path}: probe command is not [/bin/bash, -c, script]")
pathlib.Path(script_out).write_text(command[2])
PY

STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"
CURL_COUNT="$TMP/curl-count"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' 'set -euo pipefail'
  printf 'COUNT_FILE=%q\n' "$CURL_COUNT"
  cat <<'EOF'
# One process per kubectl call. The curl reply sequence lives in COUNT_FILE.
args=("$@")
len=${#args[@]}

has_exact() {
  local want="$1"
  local arg
  for arg in "${args[@]}"; do
    [[ "$arg" == "$want" ]] && return 0
  done
  return 1
}

contains_substr() {
  local needle="$1"
  local arg
  for arg in "${args[@]}"; do
    [[ "$arg" == *"$needle"* ]] && return 0
  done
  return 1
}

has_seq() {
  local -a seq=("$@")
  local slen=${#seq[@]}
  local i j
  local -i limit
  (( slen > 0 && len >= slen )) || return 1
  limit=$((len - slen))
  i=0
  while (( i <= limit )); do
    j=0
    while (( j < slen )); do
      [[ "${args[$((i + j))]}" == "${seq[$j]}" ]] || break
      j=$((j + 1))
    done
    (( j == slen )) && return 0
    i=$((i + 1))
  done
  return 1
}

match_runtime_class_get() {
  local i
  local -i limit
  (( len >= 7 )) || return 1
  limit=$((len - 7))
  i=0
  while (( i <= limit )); do
    if [[ "${args[$i]}" == get \
      && "${args[$((i + 1))]}" == sandboxtemplate \
      && "${args[$((i + 3))]}" == -n \
      && "${args[$((i + 5))]}" == -o \
      && "${args[$((i + 6))]}" == 'jsonpath={.spec.podTemplate.spec.runtimeClassName}' ]]; then
      return 0
    fi
    i=$((i + 1))
  done
  return 1
}

if match_runtime_class_get; then
  if [[ -n "${FIXTURE_RUNTIME_CLASS:-}" ]]; then
    printf '%s\n' "$FIXTURE_RUNTIME_CLASS"
  fi
  exit 0
fi
if has_seq get networkpolicy; then
  exit 1
fi
if has_seq create token; then
  printf '%s\n' probe-token
  exit 0
fi
if has_seq auth can-i get secret/sp-agent-a-creds; then
  printf '%s\n' yes
  exit 0
fi
if has_seq auth can-i get secret/sp-agent-b-creds; then
  printf '%s\n' no
  exit 0
fi
if has_seq auth can-i list secrets; then
  printf '%s\n' no
  exit 0
fi
if has_exact exec && contains_substr curl; then
  n=0
  if [[ -f "$COUNT_FILE" ]]; then
    n=$(<"$COUNT_FILE")
  fi
  case "$n" in
    ''|*[!0-9]*) n=0 ;;
  esac
  n=$((n + 1))
  printf '%s\n' "$n" >"$COUNT_FILE"
  case "$n" in
    1) printf 'rc=0\n' ;;
    2) printf 'rc=28\n' ;;
    3) printf 'rc=28\n' ;;
    4) printf 'rc=0\n' ;;
    5) printf 'rc=28\n' ;;
  esac
  exit 0
fi
if has_exact exec && contains_substr nslookup; then
  exit 0
fi
if has_exact exec && contains_substr 'nc '; then
  if [[ -n "${DT_ALLOWED_POD:-}" ]] && contains_substr "$DT_ALLOWED_POD"; then
    printf 'rc=0\n'
  else
    printf 'rc=1\n'
  fi
  exit 0
fi
if has_exact run && { contains_substr sp-gvisor-check || contains_substr runtimeClassName; }; then
  printf '%s\n' 'Error from server (NotFound): runtimeclasses.node.k8s.io "gvisor" not found'
  exit 0
fi
exit 0
EOF
} >"$STUB_BIN/kubectl"
chmod 0755 "$STUB_BIN/kubectl"

run_rendered_probe() {
  local fixture_class="$1"
  local log="$2"
  printf '0\n' >"$CURL_COUNT"
  set +e
  PATH="${STUB_BIN}:${PATH}" \
    FIXTURE_RUNTIME_CLASS="$fixture_class" \
    bash --noprofile --norc -c '
set -euo pipefail
set -a
source "$1"
set +a
script=""
IFS= read -r -d "" script < "$2" || true
bash -c "$script"
' bash "$PROBE_ENV" "$PROBE_SCRIPT" >"$log" 2>&1
  local rc=$?
  set -e
  printf '%s\n' "$rc"
}

EMPTY_LOG="$TMP/probe-empty.log"
EMPTY_RC="$(run_rendered_probe "" "$EMPTY_LOG")"
if [[ "$EMPTY_RC" -eq 0 ]]; then
  fail "empty SandboxTemplate class: rendered probe exited 0; log: $(cat "$EMPTY_LOG")"
fi
if ! grep -F 'FAIL: SandboxTemplate' "$EMPTY_LOG" | grep -F 'curie-runner' >/dev/null; then
  fail "empty SandboxTemplate class: log missing the bad-template line (FAIL and curie-runner): $(cat "$EMPTY_LOG")"
fi
if ! grep -F 'SECURITY PROBE: FAIL' "$EMPTY_LOG" >/dev/null; then
  fail "empty SandboxTemplate class: log missing SECURITY PROBE: FAIL: $(cat "$EMPTY_LOG")"
fi
echo "  ok: empty class: rendered probe exits non-zero and reports FAIL for curie-runner"

GVISOR_LOG="$TMP/probe-gvisor.log"
GVISOR_RC="$(run_rendered_probe gvisor "$GVISOR_LOG")"
if [[ "$GVISOR_RC" -ne 0 ]]; then
  fail "gvisor SandboxTemplate class: rendered probe exited ${GVISOR_RC}; log: $(cat "$GVISOR_LOG")"
fi
if ! grep -F 'SECURITY PROBE: PASS' "$GVISOR_LOG" >/dev/null; then
  fail "gvisor SandboxTemplate class: log missing SECURITY PROBE: PASS: $(cat "$GVISOR_LOG")"
fi
if grep -F 'FAIL: SandboxTemplate' "$GVISOR_LOG" >/dev/null; then
  fail "gvisor SandboxTemplate class: log contains FAIL: SandboxTemplate: $(cat "$GVISOR_LOG")"
fi
echo "  ok: gvisor class: rendered probe exits 0 with SECURITY PROBE: PASS"

echo "PASS: real-model auto with empty lookup stamps gvisor on every runner SandboxTemplate, fake-model and mode off omit it, the admission expression follows that class, and the rendered security probe fails when curie-runner lacks gvisor and passes when the class is gvisor"
