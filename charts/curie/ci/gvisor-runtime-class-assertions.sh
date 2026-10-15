#!/usr/bin/env bash
#
# Render and execution assertions for issues #3558 and #4420.
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
#   7. The rendered Claim 4 block and final probe summary exit with a failure
#      when the SandboxTemplate class is empty. Its output contains the
#      bad-template line (FAIL and curie-runner)
#      and SECURITY PROBE: FAIL. With the class set to gvisor it exits 0,
#      prints SECURITY PROBE: PASS, and does not print FAIL: SandboxTemplate.
#   8. Both rendered kernel checks reject runc, including a gvisor-looking
#      release and pod hostname, and accept current and older gVisor kernels.
#      The probe executes its actual kubectl run command before reading logs.
#   9. Neither rendered script uses uname -a, and the sentry version constant
#      appears only in the shared chart helper.
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
script = command[2]
claim4 = re.search(r"(?m)^[ \t]*# =+ Claim 4:.*$", script)
claim5 = re.search(r"(?m)^[ \t]*# =+ Claim 5:.*$", script)
summary = re.search(
    r'(?m)^[ \t]*\[ "\$fail" = 0 \].*\n[ \t]*exit \$fail[ \t]*(?:\n|$)',
    script,
)
if claim4 is None or claim5 is None or claim4.start() >= claim5.start():
    raise SystemExit(f"{path}: probe command has no bounded Claim 4 block")
if summary is None:
    raise SystemExit(f"{path}: probe command has no final fail summary and exit")
pathlib.Path(script_out).write_text(
    "fail=0\n" + script[claim4.start() : claim5.start()] + summary.group(0)
)
PY

STUB_BIN="$TMP/bin"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/uname" <<'EOF'
#!/usr/bin/env sh
set -eu
case "${1:-}" in
  -a) printf 'Linux %s %s %s x86_64 GNU/Linux\n' "$FIXTURE_UNAME_NODENAME" "$FIXTURE_UNAME_RELEASE" "$FIXTURE_UNAME_VERSION" ;;
  -n) printf '%s\n' "$FIXTURE_UNAME_NODENAME" ;;
  -r) printf '%s\n' "$FIXTURE_UNAME_RELEASE" ;;
  -v) printf '%s\n' "$FIXTURE_UNAME_VERSION" ;;
  -s) printf '%s\n' Linux ;;
  *) printf 'unexpected uname argument: %s\n' "$*" >&2; exit 1 ;;
esac
EOF
cat >"$STUB_BIN/sleep" <<'EOF'
#!/usr/bin/env sh
exit 0
EOF
cat >"$STUB_BIN/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args=("$@")
len=${#args[@]}

contains_substr() {
  local needle="$1"
  local arg
  for arg in "${args[@]}"; do
    [[ "$arg" == *"$needle"* ]] && return 0
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
if [[ "${args[0]:-}" == run ]] && { contains_substr sp-gvisor-check || contains_substr runtimeClassName; }; then
  case "$FIXTURE_POD_MODE" in
    not-found)
      printf '%s\n' 'Error from server (NotFound): runtimeclasses.node.k8s.io "gvisor" not found'
      exit 0
      ;;
    scheduled)
      for (( i = 0; i < len; i++ )); do
        if [[ "${args[$i]}" == -- && $((i + 1)) -lt len ]]; then
          "${args[@]:$((i + 1))}" >"$FIXTURE_POD_LOG"
          printf '%s\n' 'pod/sp-gvisor-check created'
          exit 0
        fi
      done
      ;;
  esac
fi
if [[ "${args[0]:-}" == wait ]] && contains_substr sp-gvisor-check; then
  exit 0
fi
if [[ "${args[0]:-}" == logs ]] && contains_substr sp-gvisor-check; then
  cat "$FIXTURE_POD_LOG"
  exit 0
fi
if [[ "${args[0]:-}" == delete ]] && contains_substr sp-gvisor-check; then
  exit 0
fi
printf 'unexpected Claim 4 kubectl invocation: %s\n' "$*" >&2
exit 1
EOF
chmod 0755 "$STUB_BIN/kubectl" "$STUB_BIN/uname" "$STUB_BIN/sleep"

run_rendered_probe() {
  local fixture_class="$1"
  local log="$2"
  local pod_mode="$3"
  set +e
  PATH="${STUB_BIN}:${PATH}" \
    FIXTURE_RUNTIME_CLASS="$fixture_class" \
    FIXTURE_POD_MODE="$pod_mode" \
    FIXTURE_UNAME_NODENAME=sp-gvisor-check \
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
EMPTY_RC="$(run_rendered_probe "" "$EMPTY_LOG" not-found)"
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
GVISOR_RC="$(run_rendered_probe gvisor "$GVISOR_LOG" not-found)"
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

for log in "$EMPTY_LOG" "$GVISOR_LOG"; do
  grep -F "NOT-TESTABLE: RuntimeClass 'gvisor' absent" "$log" >/dev/null || \
    fail "NotFound fixture lost its NOT-TESTABLE report: $(cat "$log")"
done

echo "=== Assertion 8: both rendered scripts identify the sentry independently of names ==="
PREFLIGHT_RENDER="$(render_chart preflight \
  --show-only templates/preflight-gvisor.yaml \
  --set agentSandbox.runner.fakeModel=false)"
PREFLIGHT_SCRIPT="$TMP/preflight-script.sh"
python3 - "$PREFLIGHT_RENDER" "$PREFLIGHT_SCRIPT" <<'PY'
import pathlib
import sys

import yaml

path, script_out = sys.argv[1:]
docs = [doc for doc in yaml.safe_load_all(pathlib.Path(path).read_text()) if doc]
jobs = [doc for doc in docs if doc.get("kind") == "Job"]
if len(jobs) != 1:
    raise SystemExit(f"{path}: expected exactly one preflight Job, found {len(jobs)}")
command = jobs[0]["spec"]["template"]["spec"]["containers"][0]["command"]
if len(command) != 3 or command[:2] != ["/bin/sh", "-c"] or not isinstance(command[2], str):
    raise SystemExit(f"{path}: preflight command is not [/bin/sh, -c, script]")
pathlib.Path(script_out).write_text(command[2])
PY

# gVisor LinuxVersion: pkg/sentry/kernel/version/version.go, also present in
# https://github.com/google/gvisor/blob/release-20230102.0/pkg/sentry/syscalls/linux/linux64.go
# https://github.com/google/gvisor/blob/master/pkg/sentry/kernel/version/version.go
SENTRY_KERNEL_VERSION='#1 SMP Sun Jan 10 15:06:54 PST 2016'
RUNC_KERNEL_VERSION='#146-Ubuntu SMP PREEMPT_DYNAMIC Thu Sep  3 16:12:30 UTC 2026'
fixture_failures=0
for fixture in runc runc-lookalike current-gvisor older-gvisor; do
  case "$fixture" in
    runc) release=6.8.0-146-generic; version="$RUNC_KERNEL_VERSION"; expected=FAIL ;;
    runc-lookalike) release=6.8.0-gvisor-lookalike; version="$RUNC_KERNEL_VERSION"; expected=FAIL ;;
    current-gvisor) release=4.19.0-gvisor; version="$SENTRY_KERNEL_VERSION"; expected=PASS ;;
    older-gvisor) release=4.4.0; version="$SENTRY_KERNEL_VERSION"; expected=PASS ;;
  esac
  export FIXTURE_UNAME_RELEASE="$release" FIXTURE_UNAME_VERSION="$version"
  preflight_log="$TMP/preflight-${fixture}.log"
  if PATH="${STUB_BIN}:${PATH}" FIXTURE_UNAME_NODENAME=curie-preflight-gvisor-x7k2p \
    sh "$PREFLIGHT_SCRIPT" >"$preflight_log" 2>&1; then
    preflight_rc=0
  else
    preflight_rc=$?
  fi
  export FIXTURE_POD_LOG="$TMP/pod-${fixture}.log"
  probe_log="$TMP/probe-${fixture}.log"
  probe_rc="$(run_rendered_probe gvisor "$probe_log" scheduled)"
  [[ -s "$FIXTURE_POD_LOG" ]] || fail "$fixture: kubectl run did not save pod command stdout"
  for check in preflight probe; do
    if [[ "$check" == preflight ]]; then
      rc="$preflight_rc"; log="$preflight_log"; summary="RESULT: ${expected}"
      detail="$summary"
    else
      rc="$probe_rc"; log="$probe_log"; summary="SECURITY PROBE: ${expected}"
      if [[ "$expected" == PASS ]]; then
        detail='PASS: pod under RuntimeClass'
      else
        detail='NOT gVisor'
      fi
    fi
    if { [[ "$expected" == PASS && "$rc" -eq 0 ]] || [[ "$expected" == FAIL && "$rc" -ne 0 ]]; } \
      && grep -F "$summary" "$log" >/dev/null \
      && grep -F "$detail" "$log" >/dev/null; then
      echo "  ok: $check $fixture: $expected"
    else
      echo "FAIL: $check $fixture: wanted $expected, exit=$rc; log: $(cat "$log")" >&2
      fixture_failures=$((fixture_failures + 1))
    fi
  done
done
[[ "$fixture_failures" -eq 0 ]] || fail "$fixture_failures kernel fixture checks failed"

echo "=== Assertion 9: rendered scripts exclude nodenames and share the source constant ==="
if grep -F 'uname -a' "$PREFLIGHT_SCRIPT" "$PROBE_SCRIPT"; then
  fail "rendered kernel check still uses uname -a"
fi
python3 - "$CHART/templates" "$SENTRY_KERNEL_VERSION" <<'PY'
import pathlib
import re
import sys

templates = pathlib.Path(sys.argv[1])
constant = sys.argv[2]
matches = [path for path in templates.rglob("*") if path.is_file() and constant in path.read_text()]
helper = templates / "_helpers.tpl"
if matches != [helper] or helper.read_text().count(constant) != 1:
    raise SystemExit("sentry version must appear exactly once in templates/_helpers.tpl")
definition = re.search(
    r'{{-?\s*define "curie\.gvisor\.sentryKernelVersion".*?{{-?\s*end\s*-?}}',
    helper.read_text(),
    re.DOTALL,
)
if definition is None or constant not in definition.group(0):
    raise SystemExit("sentry version must belong to curie.gvisor.sentryKernelVersion")
PY

echo "PASS: runtime classes and admission match the model mode, the probe reports absent classes, and both kernel checks reject runc and accept current and older gVisor independently of pod names"
