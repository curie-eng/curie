#!/usr/bin/env bash
#
# Render-assertion test for runner template admission (#3842, AC2).
#
# The worker is the only runtime writer of SandboxTemplates, and a compromised
# worker that can write a template with a hostPath, a host namespace, a mounted
# ServiceAccount token, another ServiceAccount, or an arbitrary image has a
# path to the node. templates/runner-resources-admission.yaml therefore admits
# the worker's template writes only in the shape the chart itself renders.
#
# Asserts:
#
#   1. The runner-resources ValidatingAdmissionPolicy carries exactly one
#      validation per rule (hostPath, host namespaces, ServiceAccount token,
#      ServiceAccount pin, images, host fields), each identified by its own
#      message and guarded to sandboxtemplates.
#   2. The image allowlist in that policy equals the set of images in every
#      rendered runner SandboxTemplate plus agentSandbox.runner.admission
#      .extraImages, on the default render, on a render with a per-agent
#      runnerImages digest plus an extraImages entry, and on a render with
#      bundleFetch disabled. Equality, not containment: the chart's own
#      templates are inside the allowlist (liveness), and nothing else is.
#   3. An extraImages entry with a quote or a space fails render naming
#      agentSandbox.runner.admission.extraImages; a valid one renders (case 2).
#   4. The ServiceAccount pin names <fullname>-runner; with
#      agentSandbox.runner.serviceAccount.create=false the expression admits an
#      absent or default serviceAccountName and names no chart SA.
#   5. Every runner SandboxTemplate renders automountServiceAccountToken: false,
#      with serviceAccount.create=false too, and so does the runner
#      ServiceAccount; automountToken=true fails render naming the key.
#   6. The claim-cleanup (DELETE of sandboxtemplates) and worker-secrets
#      (CREATE of secrets) policies render with the worker ServiceAccount
#      matchCondition, failurePolicy Fail, a Deny binding, and validations on
#      the curietech.ai/sandbox-claim label; the runner-resources policy keeps
#      CREATE/UPDATE only.
#   7. IRSA: a runner role ARN equal to the api, worker, or langfuse role ARN
#      fails render naming agentSandbox.runner.serviceAccount.annotations;
#      distinct ARNs render; no annotation renders.
#
# Render to a directory and read the written files. A piped `helm template`
# has been observed to truncate silently while still exiting 0.
#
# Runnable locally and from CI. Fails loudly, naming the assertion.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT

RELEASE=curie
NAMESPACE=curie-rta
FULLNAME=curie
ACME_DIGEST="registry.example.com/acme/curie-runner@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
EXTRA_IMAGE="registry.example.com/tools/helper:1.2.3"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render_dir() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --output-dir "$out" "$@" >/dev/null
  printf '%s\n' "$out"
}

# must_fail_naming LABEL NEEDLE HELM_ARGS...: the render must fail, and its
# error must name NEEDLE, so a render that fails for an unrelated reason
# cannot pass as the refusal under test.
must_fail_naming() {
  local label="$1"
  local needle="$2"
  shift 2
  local slug="${label//[^A-Za-z0-9]/_}"
  local out="$TMP/fail-$slug"
  mkdir -p "$out"
  if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --output-dir "$out" "$@" >/dev/null 2>"$out.err"; then
    fail "${label}: render must fail"
  fi
  if ! grep -qF -- "$needle" "$out.err"; then
    fail "${label}: render failed without naming ${needle}; output was: $(cat "$out.err")"
  fi
  echo "  ok: ${label} is refused at render, naming ${needle}"
}

values_file() {
  local name="$1"
  local file="$TMP/values-${name}.yaml"
  cat >"$file"
  printf '%s\n' "$file"
}

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import pathlib
import re
import sys

import yaml

GROUP = "extensions.agents.x-k8s.io"
GUARD = "request.resource.resource != 'sandboxtemplates'"
CLAIM_LABEL = "curietech.ai/sandbox-claim"

HOSTPATH_MSG = "runner templates must not mount a hostPath volume"
HOSTNS_MSG = "runner templates must not share the host network, PID, or IPC namespace"
TOKEN_MSG = "runner templates must not mount a ServiceAccount token"
SA_MSG = "runner templates must run as the chart runner ServiceAccount"
IMAGES_MSG = (
    "runner templates may run only the images the chart renders or "
    "agentSandbox.runner.admission.extraImages lists"
)
HOSTFIELDS_MSG = "runner templates must not run privileged, add capabilities, or bind a host port"

# message -> substrings its expression must contain
RULES = {
    HOSTPATH_MSG: ("volumes", "hostPath"),
    HOSTNS_MSG: ("hostNetwork", "hostPID", "hostIPC"),
    TOKEN_MSG: ("automountServiceAccountToken", "projected", "serviceAccountToken"),
    SA_MSG: ("serviceAccountName",),
    IMAGES_MSG: ("containers", "initContainers", ".image"),
    HOSTFIELDS_MSG: ("privileged", "capabilities", "add", "hostPort", "initContainers"),
}


def die(message):
    raise SystemExit(message)


def load(root):
    docs = []
    for path in sorted(pathlib.Path(root).rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if isinstance(doc, dict):
                docs.append(doc)
    if not docs:
        die(f"{root}: Helm wrote no YAML documents")
    return docs


def named(docs, kind, name):
    found = [
        d for d in docs
        if d.get("kind") == kind and (d.get("metadata") or {}).get("name") == name
    ]
    if len(found) != 1:
        have = sorted(
            (d.get("metadata") or {}).get("name") for d in docs if d.get("kind") == kind
        )
        die(f"expected exactly one {kind} {name!r}, found {len(found)}; rendered {kind}s: {have}")
    return found[0]


def validations(policy):
    return (policy.get("spec") or {}).get("validations") or []


def rule(policy, message):
    hits = [v for v in validations(policy) if v.get("message") == message]
    if len(hits) != 1:
        have = [v.get("message") for v in validations(policy)]
        die(
            f"{policy['metadata']['name']}: expected exactly one validation with message "
            f"{message!r}, found {len(hits)}; messages are {have}"
        )
    expr = hits[0].get("expression")
    if not isinstance(expr, str) or not expr.strip():
        die(f"{policy['metadata']['name']}: validation {message!r} has no expression")
    return expr


def runner_templates(docs):
    templates = [d for d in docs if d.get("kind") == "SandboxTemplate"]
    if not templates:
        die("no SandboxTemplate rendered")
    return templates


def pod_spec(template):
    return (((template.get("spec") or {}).get("podTemplate") or {}).get("spec")) or {}


def template_images(docs):
    images = set()
    for template in runner_templates(docs):
        spec = pod_spec(template)
        for container in (spec.get("initContainers") or []) + (spec.get("containers") or []):
            image = container.get("image")
            if not image:
                die(f"SandboxTemplate {template['metadata']['name']}: container {container.get('name')!r} has no image")
            images.add(image)
    return images


def allowlist(policy):
    expr = rule(policy, IMAGES_MSG)
    lists = re.findall(r"\bin\s*\[([^\]]*)\]", expr)
    if len(lists) < 2:
        die(
            f"image validation must test containers and initContainers against a literal "
            f"list; found {len(lists)} list(s) in {expr!r}"
        )
    parsed = []
    for body in lists:
        items = re.findall(r"'([^']*)'", body)
        rest = re.sub(r"'[^']*'", "", body).replace(",", "").strip()
        if rest:
            die(f"image list has non-literal content {rest!r}: {body!r}")
        parsed.append(items)
    first = parsed[0]
    for other in parsed[1:]:
        if sorted(other) != sorted(first):
            die(f"container and initContainer image lists differ: {first} vs {other}")
    if len(set(first)) != len(first):
        die(f"image allowlist has duplicates: {first}")
    return set(first)


def assert_rules(root, runner_sa):
    docs = load(root)
    policy = named(docs, "ValidatingAdmissionPolicy", f"{FULLNAME}-runner-resources")
    for message, needles in RULES.items():
        expr = rule(policy, message)
        if GUARD not in expr:
            die(f"validation {message!r} is not guarded by {GUARD!r}: {expr!r}")
        missing = [n for n in needles if n not in expr]
        if missing:
            die(f"validation {message!r} expression lacks {missing}: {expr!r}")
    sa_expr = rule(policy, SA_MSG)
    if runner_sa:
        if f"'{runner_sa}'" not in sa_expr:
            die(f"ServiceAccount pin does not name '{runner_sa}': {sa_expr!r}")
        if "'default'" in sa_expr:
            die(f"ServiceAccount pin admits default while the chart creates {runner_sa}: {sa_expr!r}")
    else:
        if "'default'" not in sa_expr or "!has(" not in sa_expr:
            die(f"with serviceAccount.create=false the pin must admit absent or default: {sa_expr!r}")
        if f"'{FULLNAME}-runner'" in sa_expr:
            die(f"with serviceAccount.create=false the pin still names the chart runner SA: {sa_expr!r}")
    ops = sorted(
        op
        for r in ((policy.get("spec") or {}).get("matchConstraints") or {}).get("resourceRules") or []
        for op in r.get("operations") or []
    )
    if ops != ["CREATE", "UPDATE"]:
        die(f"{FULLNAME}-runner-resources operations are {ops}, want CREATE and UPDATE only")
    print(f"  ok: one validation per rule, guarded to sandboxtemplates; SA pin {runner_sa or 'absent/default'}")


def assert_images(root, extras):
    docs = load(root)
    policy = named(docs, "ValidatingAdmissionPolicy", f"{FULLNAME}-runner-resources")
    allowed = allowlist(policy)
    rendered = template_images(docs)
    want = rendered | set(extras)
    if allowed != want:
        die(
            "image allowlist drifted from the rendered runner templates: "
            f"missing {sorted(want - allowed)}, extra {sorted(allowed - want)}"
        )
    print(f"  ok: allowlist equals the {len(rendered)} rendered template image(s) plus {len(extras)} extra(s)")


def assert_no_automount(root, expect_sa):
    docs = load(root)
    for template in runner_templates(docs):
        spec = pod_spec(template)
        name = template["metadata"]["name"]
        if spec.get("automountServiceAccountToken") is not False:
            die(
                f"SandboxTemplate {name}: automountServiceAccountToken is "
                f"{spec.get('automountServiceAccountToken')!r}, want false"
            )
    accounts = [
        d for d in docs
        if d.get("kind") == "ServiceAccount" and d["metadata"]["name"] == f"{FULLNAME}-runner"
    ]
    if expect_sa:
        if len(accounts) != 1:
            die(f"expected the {FULLNAME}-runner ServiceAccount, found {len(accounts)}")
        if accounts[0].get("automountServiceAccountToken") is not False:
            die(f"{FULLNAME}-runner ServiceAccount automountServiceAccountToken is {accounts[0].get('automountServiceAccountToken')!r}")
    elif accounts:
        die(f"serviceAccount.create=false still renders {FULLNAME}-runner")
    print(f"  ok: every runner SandboxTemplate renders automountServiceAccountToken: false (runner SA rendered: {expect_sa})")


def match_condition(policy):
    conditions = (policy.get("spec") or {}).get("matchConditions") or []
    if len(conditions) != 1:
        die(f"{policy['metadata']['name']}: expected one matchCondition, found {conditions}")
    return conditions[0].get("expression")


def assert_binding(docs, policy_name):
    bindings = [
        d for d in docs
        if d.get("kind") == "ValidatingAdmissionPolicyBinding"
        and (d.get("spec") or {}).get("policyName") == policy_name
    ]
    if len(bindings) != 1:
        die(f"expected one binding for {policy_name}, found {len(bindings)}")
    actions = (bindings[0].get("spec") or {}).get("validationActions")
    if actions != ["Deny"]:
        die(f"binding for {policy_name} validationActions is {actions!r}, want ['Deny']")


def assert_side_policies(root):
    docs = load(root)
    base = named(docs, "ValidatingAdmissionPolicy", f"{FULLNAME}-runner-resources")
    worker_cond = match_condition(base)
    want_user = f"system:serviceaccount:{NAMESPACE}:{FULLNAME}-worker"
    if want_user not in (worker_cond or ""):
        die(f"runner-resources matchCondition does not name {want_user}: {worker_cond!r}")

    specs = {
        f"{FULLNAME}-runner-claim-cleanup": {
            "rules": [([GROUP], ["sandboxtemplates"], ["DELETE"])],
            "needles": ("oldObject", CLAIM_LABEL),
        },
        f"{FULLNAME}-worker-secrets": {
            "rules": [([""], ["secrets"], ["CREATE"])],
            "needles": ("object.metadata", CLAIM_LABEL, "-tokens", "Opaque"),
        },
    }
    for name, want in specs.items():
        policy = named(docs, "ValidatingAdmissionPolicy", name)
        spec = policy.get("spec") or {}
        if spec.get("failurePolicy") != "Fail":
            die(f"{name}: failurePolicy is {spec.get('failurePolicy')!r}, want Fail")
        if match_condition(policy) != worker_cond:
            die(f"{name}: matchCondition {match_condition(policy)!r} is not the worker SA condition {worker_cond!r}")
        rules = (spec.get("matchConstraints") or {}).get("resourceRules") or []
        got = [
            (r.get("apiGroups"), r.get("resources"), sorted(r.get("operations") or []))
            for r in rules
        ]
        if got != [(g, r, sorted(o)) for g, r, o in want["rules"]]:
            die(f"{name}: resourceRules are {got}, want {want['rules']}")
        if name.endswith("-worker-secrets") and rules[0].get("apiVersions") != ["v1"]:
            die(f"{name}: apiVersions is {rules[0].get('apiVersions')!r}, want ['v1']")
        exprs = " ".join(v.get("expression") or "" for v in validations(policy))
        if not validations(policy):
            die(f"{name}: no validations")
        missing = [n for n in want["needles"] if n not in exprs]
        if missing:
            die(f"{name}: validations lack {missing}: {exprs!r}")
        assert_binding(docs, name)
    assert_binding(docs, f"{FULLNAME}-runner-resources")
    print("  ok: claim-cleanup (DELETE) and worker-secrets (CREATE) policies bind only the worker SA, fail closed, and deny")


def main():
    global FULLNAME, NAMESPACE
    root, mode, FULLNAME, NAMESPACE, *args = sys.argv[1:]
    if mode == "rules":
        assert_rules(root, args[0] if args else "")
    elif mode == "images":
        assert_images(root, args)
    elif mode == "automount":
        assert_no_automount(root, args[0] == "sa")
    elif mode == "side-policies":
        assert_side_policies(root)
    else:
        die(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
PY

check() {
  local root="$1"
  local mode="$2"
  shift 2
  python3 "$CHECKER" "$root" "$mode" "$FULLNAME" "$NAMESPACE" "$@"
}

ACME_VALUES="$(values_file acme <<EOF
agentSandbox:
  runnerImages:
    acme: "${ACME_DIGEST}"
  runner:
    admission:
      extraImages:
        - "${EXTRA_IMAGE}"
EOF
)"

DEFAULT_OUT="$(render_dir default)" || fail "default render must succeed"
ACME_OUT="$(render_dir acme --values "$ACME_VALUES")" \
  || fail "a per-agent runnerImages digest plus a valid extraImages entry must render"
NOSA_OUT="$(render_dir nosa --set agentSandbox.runner.serviceAccount.create=false)" \
  || fail "agentSandbox.runner.serviceAccount.create=false must render"
NOBF_OUT="$(render_dir nobf --set agentSandbox.runner.bundleFetch.enabled=false)" \
  || fail "agentSandbox.runner.bundleFetch.enabled=false must render"

echo "=== Assertion 1: one validation per rule on the runner-resources policy ==="
check "$DEFAULT_OUT" rules "${FULLNAME}-runner" || fail "assertion 1 (default render)"

echo "=== Assertion 2: the image allowlist equals the rendered runner template images ==="
check "$DEFAULT_OUT" images || fail "assertion 2 (default render)"
check "$ACME_OUT" images "$EXTRA_IMAGE" || fail "assertion 2 (runnerImages.acme plus extraImages)"
if ! grep -rqF -- "$ACME_DIGEST" "$ACME_OUT"; then
  fail "assertion 2: the acme runnerImages digest is absent from the render, so the per-agent path was not exercised"
fi
check "$NOBF_OUT" images || fail "assertion 2 (bundleFetch disabled)"

echo "=== Assertion 3: extraImages entries are CEL-safe or the render fails naming the key ==="
must_fail_naming "extraImages with a quote" "agentSandbox.runner.admission.extraImages" \
  --values "$(values_file quote <<'EOF'
agentSandbox:
  runner:
    admission:
      extraImages:
        - "registry.example.com/x:1' || true || '"
EOF
)"
must_fail_naming "extraImages with a space" "agentSandbox.runner.admission.extraImages" \
  --values "$(values_file space <<'EOF'
agentSandbox:
  runner:
    admission:
      extraImages:
        - "registry.example.com/x:1 extra"
EOF
)"
echo "  ok: a valid extraImages entry renders (assertion 2, acme render)"

echo "=== Assertion 4: the ServiceAccount pin follows serviceAccount.create ==="
check "$NOSA_OUT" rules "" || fail "assertion 4 (serviceAccount.create=false)"

echo "=== Assertion 5: no runner template mounts a ServiceAccount token, and the knob cannot turn it on ==="
check "$DEFAULT_OUT" automount sa || fail "assertion 5 (default render)"
check "$NOSA_OUT" automount nosa || fail "assertion 5 (serviceAccount.create=false)"
check "$ACME_OUT" automount sa || fail "assertion 5 (per-agent template)"
must_fail_naming "automountToken=true" "agentSandbox.runner.serviceAccount.automountToken" \
  --set agentSandbox.runner.serviceAccount.automountToken=true

echo "=== Assertion 6: claim-cleanup and worker-secrets policies ==="
check "$DEFAULT_OUT" side-policies || fail "assertion 6"

echo "=== Assertion 7: the runner role ARN may not reuse a read/write chart identity ==="
SHARED_ARN="arn:aws:iam::000000000000:role/curie-shared"
for owner in api worker langfuse; do
  must_fail_naming "runner role ARN equal to ${owner}" "agentSandbox.runner.serviceAccount.annotations" \
    --set-string "agentSandbox.runner.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${SHARED_ARN}" \
    --set-string "${owner}.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${SHARED_ARN}"
done
DISTINCT_OUT="$(render_dir distinct \
  --set-string "agentSandbox.runner.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::000000000000:role/curie-runner" \
  --set-string "api.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::000000000000:role/curie-api" \
  --set-string "worker.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::000000000000:role/curie-worker" \
  --set-string "langfuse.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::000000000000:role/curie-langfuse")" \
  || fail "assertion 7: distinct role ARNs must render"
grep -rqF -- "role/curie-runner" "$DISTINCT_OUT" \
  || fail "assertion 7: the distinct-ARN render does not carry the runner role annotation"
echo "  ok: distinct role ARNs render"
echo "  ok: no annotation renders (default render above)"

echo
echo "PASS: runner template admission renders one rule per host escape, token mount, SA swap and image swap; the allowlist is exactly the chart's own images plus extraImages; automount stays off; cleanup and Secret writes are label-scoped; the runner role cannot reuse a read/write identity"
