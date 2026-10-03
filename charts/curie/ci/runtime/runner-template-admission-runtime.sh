#!/usr/bin/env bash
#
# Live proof for runner template admission (#3842, AC2) against a real
# Kubernetes API server (1.30 or newer).
#
#   CURIE_ADMISSION_CONTEXT=<kube context> bash \
#     charts/curie/ci/runtime/runner-template-admission-runtime.sh
#
# CI runs it through tools/runtime-assertion-gate/run.sh in the kind cluster
# job e2e-cluster-chart-regressions.
#
# It renders the chart under a unique release name and applies ONLY the
# admission policies and bindings from templates/runner-resources-admission.yaml
# and the worker ServiceAccount, Role and RoleBinding from templates/worker.yaml.
# The rendered runner SandboxTemplate from templates/agent-sandbox.yaml is the
# fixture source; it is never applied as the chart would apply it. Requests are
# made as the worker ServiceAccount through impersonation with
# --dry-run=server, so each one is authorized by the rendered Role and admitted
# or refused by the rendered policies exactly as the worker's own token would be.
#
# Must admit, as the worker:
#   - a copy of the chart's runner template named *-resources (liveness: the
#     chart's own images, ServiceAccount and token setting pass every rule);
#   - a per-claim-shaped copy carrying the claim label and a token secretKeyRef;
#   - an Opaque *-tokens Secret carrying the claim label;
#   - DELETE of a per-claim template (created by the admin first).
# Must deny, as the worker, each with the policy's own message (so a schema
# error or an RBAC Forbidden cannot pass as the refusal):
#   hostPath; hostNetwork; hostPID; hostIPC; automount true; automount absent;
#   projected ServiceAccount token; serviceAccountName default; runner image
#   unlisted; init container image unlisted; privileged; capabilities.add;
#   hostPort; an unlabelled Secret; a kubernetes.io/service-account-token
#   Secret; DELETE of the chart's own template.
# Negative control: the same hostPath template created by the cluster admin is
# admitted and keeps its hostPath, so the denial is the worker-scoped policy,
# not the CRD schema or the API server.
#
# Everything is named from RUN_ID. Cleanup deletes by exact name and then
# verifies each object is gone; a leak, or a lookup that fails for any reason
# other than NotFound, fails the script.
set -euo pipefail

CTX="${CURIE_ADMISSION_CONTEXT:?set CURIE_ADMISSION_CONTEXT to the test cluster context}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
RUN_ID="${RUN_ID:-$(date +%s)}"
REL="rta${RUN_ID}"
NS="test-3842-rta-${RUN_ID}"
FULL="${REL}-curie"
WORKER_SA="${FULL}-worker"
CHART_TEMPLATE="${FULL}-runner"
TEMPLATE_CRD="sandboxtemplates.extensions.agents.x-k8s.io"
RESOURCES_POLICY="${FULL}-runner-resources"
CLEANUP_POLICY="${FULL}-runner-claim-cleanup"
SECRETS_POLICY="${FULL}-worker-secrets"
OWNED_TEMPLATE="rta-owned-resources"

HOSTPATH_MSG="runner templates must not mount a hostPath volume"
HOSTNS_MSG="runner templates must not share the host network, PID, or IPC namespace"
TOKEN_MSG="runner templates must not mount a ServiceAccount token"
SA_MSG="runner templates must run as the chart runner ServiceAccount"
IMAGES_MSG="runner templates may run only the images the chart renders or agentSandbox.runner.admission.extraImages lists"
HOSTFIELDS_MSG="runner templates must not run privileged, add capabilities, or bind a host port"

k() { kubectl --context "$CTX" "$@"; }
as_worker() {
  kubectl --context "$CTX" --as "system:serviceaccount:${NS}:${WORKER_SA}" \
    --as-group system:serviceaccounts --as-group "system:serviceaccounts:${NS}" "$@"
}

OUT="$(mktemp -d)"
OWNED_VAP=()
OWNED_VAPB=()
CREATED_NS=0
CREATED_CRD=0

# absent KIND NAME [NAMESPACE]: 0 only on a real NotFound. Any other lookup
# error is not proof the object is gone.
absent() {
  local kind="$1" name="$2" ns="${3:-}"
  local out
  local args=(get "$kind" "$name" -o name)
  [[ -n "$ns" ]] && args+=(-n "$ns")
  if out="$(k "${args[@]}" 2>&1)"; then
    return 1
  fi
  grep -q "NotFound" <<<"$out"
}

cleanup() {
  set +e
  local leaked=()
  if (( CREATED_NS )); then
    k delete sandboxtemplate -n "$NS" "$OWNED_TEMPLATE" "$CHART_TEMPLATE" \
      --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1
  fi
  local name
  for name in "${OWNED_VAPB[@]}"; do
    k delete validatingadmissionpolicybinding "$name" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1
  done
  for name in "${OWNED_VAP[@]}"; do
    k delete validatingadmissionpolicy "$name" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1
  done
  if (( CREATED_NS )); then
    k delete namespace "$NS" --ignore-not-found --wait=true --timeout=180s >/dev/null 2>&1
  fi
  if (( CREATED_CRD )); then
    k delete crd "$TEMPLATE_CRD" --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1
  fi
  for name in "${OWNED_VAPB[@]}"; do
    absent validatingadmissionpolicybinding "$name" || leaked+=("validatingadmissionpolicybinding/$name")
  done
  for name in "${OWNED_VAP[@]}"; do
    absent validatingadmissionpolicy "$name" || leaked+=("validatingadmissionpolicy/$name")
  done
  if (( CREATED_NS )); then
    absent namespace "$NS" || leaked+=("namespace/$NS")
  fi
  if (( CREATED_CRD )); then
    absent crd "$TEMPLATE_CRD" || leaked+=("crd/$TEMPLATE_CRD")
  fi
  rm -rf -- "$OUT"
  if (( ${#leaked[@]} )); then
    echo "FAIL cleanup left objects behind (or could not prove them gone): ${leaked[*]}" >&2
    return 1
  fi
  return 0
}
on_exit() {
  local rc=$?
  trap - EXIT
  if ! cleanup; then
    rc=1
  fi
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

pass=0
ok() { echo "PASS $1"; pass=$((pass + 1)); }
fail() { echo "FAIL $1" >&2; exit 1; }

# expect_admitted NAME CMD...: the request must succeed.
expect_admitted() {
  local name="$1"; shift
  local out
  if ! out="$("$@" 2>&1)"; then
    fail "$name: was refused: $out"
  fi
  ok "$name admitted"
}

# expect_denied NAME POLICY MESSAGES CMD...: the request must fail, refused by
# POLICY with one of its own MESSAGES (newline separated). A schema error, an
# RBAC Forbidden, or a different policy's refusal is a failure.
expect_denied() {
  local name="$1" policy="$2" messages="$3"; shift 3
  local out
  if out="$("$@" 2>&1)"; then
    fail "$name: was admitted: $out"
  fi
  if ! grep -qF "ValidatingAdmissionPolicy '${policy}'" <<<"$out" || ! grep -qF "denied request" <<<"$out"; then
    fail "$name: refused, but not by ValidatingAdmissionPolicy '${policy}': $out"
  fi
  local message matched=0
  while IFS= read -r message; do
    [[ -z "$message" ]] && continue
    if grep -qF -- "$message" <<<"$out"; then
      matched=1
      break
    fi
  done <<<"$messages"
  (( matched )) || fail "$name: refused by ${policy} without its message (${messages//$'\n'/ | }): $out"
  echo "PASS $name refused: $(head -c 300 <<<"$out" | tr '\n' ' ')"
  pass=$((pass + 1))
}

worker_create() { as_worker create --dry-run=server -n "$NS" -f "$1"; }
admin_create() { k create -n "$NS" -f "$1"; }
fixture() { printf '%s/fixtures/%s.json\n' "$OUT" "$1"; }

# --- preflight ------------------------------------------------------------
command -v helm >/dev/null || fail "helm is required"
python3 -c 'import yaml' 2>/dev/null || fail "python3 with PyYAML is required"
k api-resources --api-group=admissionregistration.k8s.io -o name 2>/dev/null \
  | grep -qx 'validatingadmissionpolicies.admissionregistration.k8s.io' \
  || fail "context $CTX serves no admissionregistration.k8s.io/v1 ValidatingAdmissionPolicy (Kubernetes 1.30 or newer is required)"
KUBE_VERSION="$(k version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"].lstrip("v").split("+")[0])')"
[[ -n "$KUBE_VERSION" ]] || fail "could not read the server version of $CTX"
absent namespace "$NS" || fail "namespace $NS already exists or cannot be looked up; this script never adopts a namespace"

# --- CRD: only the template CRD is needed; install it only when absent ------
if absent crd "$TEMPLATE_CRD"; then
  CREATED_CRD=1
  k apply -f "$CHART/crds/crd-sandboxtemplates.yaml" >/dev/null || fail "apply the SandboxTemplate CRD"
  k wait --for=condition=Established "crd/$TEMPLATE_CRD" --timeout=60s >/dev/null \
    || fail "the SandboxTemplate CRD did not become Established"
  echo "installed $TEMPLATE_CRD for this run (deleted on exit)"
else
  k get crd "$TEMPLATE_CRD" -o name >/dev/null || fail "could not read $TEMPLATE_CRD"
fi

# --- render, never piped ----------------------------------------------------
mkdir -p "$OUT/render" "$OUT/fixtures" "$OUT/messages"
helm template "$REL" "$CHART" -n "$NS" \
  --kube-version "$KUBE_VERSION" \
  --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy \
  --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicyBinding \
  --output-dir "$OUT/render" >/dev/null \
  || fail "helm template of $CHART failed"

python3 - "$OUT" "$NS" "$FULL" "$CHART_TEMPLATE" "$OWNED_TEMPLATE" <<'PY' || fail "could not select the rendered objects and build the fixtures"
import copy, json, pathlib, sys

import yaml

out, ns, full, chart_template, owned_template = sys.argv[1:]
out = pathlib.Path(out)
render = out / "render"


def docs(basename):
    paths = list(render.rglob(basename))
    if len(paths) != 1:
        sys.exit(f"expected one rendered {basename}, found {paths}")
    return [d for d in yaml.safe_load_all(paths[0].read_text()) if isinstance(d, dict)]


apply = []
vap, vapb = [], []
for doc in docs("runner-resources-admission.yaml"):
    kind = doc.get("kind")
    if kind == "ValidatingAdmissionPolicy":
        vap.append(doc["metadata"]["name"])
        messages = []
        for v in (doc.get("spec") or {}).get("validations") or []:
            messages.append(v.get("message") or f"failed expression: {v.get('expression', '').strip()}")
        (out / "messages" / f"{doc['metadata']['name']}.txt").write_text("\n".join(messages) + "\n")
        apply.append(doc)
    elif kind == "ValidatingAdmissionPolicyBinding":
        vapb.append(doc["metadata"]["name"])
        apply.append(doc)
if not vap:
    sys.exit("templates/runner-resources-admission.yaml rendered no ValidatingAdmissionPolicy")

worker = {
    (d.get("kind"), d["metadata"]["name"]): d
    for d in docs("worker.yaml")
    if d.get("kind") in ("ServiceAccount", "Role", "RoleBinding")
}
for kind in ("ServiceAccount", "Role", "RoleBinding"):
    doc = worker.get((kind, f"{full}-worker"))
    if doc is None:
        sys.exit(f"templates/worker.yaml rendered no {kind} {full}-worker")
    doc["metadata"]["namespace"] = ns
    apply.append(doc)

(out / "apply.yaml").write_text("---\n".join(yaml.safe_dump(d) for d in apply))
(out / "vap.txt").write_text("\n".join(vap) + "\n")
(out / "vapb.txt").write_text("\n".join(vapb) + "\n")

templates = [
    d for d in docs("agent-sandbox.yaml")
    if d.get("kind") == "SandboxTemplate" and d["metadata"]["name"] == chart_template
]
if len(templates) != 1:
    sys.exit(f"templates/agent-sandbox.yaml rendered no SandboxTemplate {chart_template}")
source = templates[0]


def base(name, labels=None):
    doc = copy.deepcopy(source)
    meta = {"name": name, "namespace": ns, "labels": dict(source["metadata"].get("labels") or {})}
    meta["labels"].update(labels or {})
    doc["metadata"] = meta
    doc.pop("status", None)
    return doc


def pod(doc):
    return doc["spec"]["podTemplate"]["spec"]


def runner(doc):
    found = [c for c in pod(doc)["containers"] if c.get("name") == "runner"]
    if len(found) != 1:
        sys.exit(f"{chart_template} has no single container named runner")
    return found[0]


def write(case, doc):
    (out / "fixtures" / f"{case}.json").write_text(json.dumps(doc))


claim_label = {"curietech.ai/sandbox-claim": "rta-claim"}
write("chart-copy", base("rta-copy-resources"))

doc = base("rta-claim-resources", claim_label)
runner(doc).setdefault("env", []).append({
    "name": "CURIE_HISTORY_TOKEN",
    "valueFrom": {"secretKeyRef": {"name": "rta-claim-tokens", "key": "CURIE_HISTORY_TOKEN"}},
})
write("claim-copy", doc)
write("owned-template", base(owned_template, {"curietech.ai/sandbox-claim": "rta-owned"}))
write("chart-template", base(chart_template))

if not pod(source).get("initContainers"):
    sys.exit(f"{chart_template} renders no init container; the init image case cannot run")
if pod(source).get("automountServiceAccountToken") is not False:
    sys.exit(f"{chart_template} does not render automountServiceAccountToken: false")


def mutate(case, fn):
    doc = base(f"rta-deny-{case}-resources")
    fn(doc)
    write(f"deny-{case}", doc)


def add_volume(volume):
    return lambda d: pod(d).setdefault("volumes", []).append(volume)


mutate("hostpath", add_volume({"name": "rta-host", "hostPath": {"path": "/", "type": "Directory"}}))
mutate("hostnetwork", lambda d: pod(d).update(hostNetwork=True))
mutate("hostpid", lambda d: pod(d).update(hostPID=True))
mutate("hostipc", lambda d: pod(d).update(hostIPC=True))
mutate("automount-true", lambda d: pod(d).update(automountServiceAccountToken=True))
mutate("automount-absent", lambda d: pod(d).pop("automountServiceAccountToken"))
mutate("projected-token", add_volume({
    "name": "rta-token",
    "projected": {"sources": [{"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}]},
}))
mutate("sa-default", lambda d: pod(d).update(serviceAccountName="default"))
mutate("image", lambda d: runner(d).update(image="registry.example.com/unlisted/runner:1"))
mutate("init-image", lambda d: pod(d)["initContainers"][0].update(image="registry.example.com/unlisted/init:1"))
mutate("privileged", lambda d: runner(d).setdefault("securityContext", {}).update(privileged=True))
mutate("cap-add", lambda d: runner(d).setdefault("securityContext", {}).setdefault("capabilities", {}).update(add=["NET_ADMIN"]))
mutate("hostport", lambda d: runner(d).setdefault("ports", []).append(
    {"name": "rta-host", "containerPort": 9999, "hostPort": 9999, "protocol": "TCP"}))


def secret(name, labels, kind=None, annotations=None):
    doc = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": ns, "labels": labels},
        "stringData": {"CURIE_HISTORY_TOKEN": "placeholder"},
    }
    if kind:
        doc["type"] = kind
    if annotations:
        doc["metadata"]["annotations"] = annotations
    return doc


write("secret-claim", secret("rta-claim-tokens", claim_label, "Opaque"))
write("secret-unlabelled", secret("rta-unlabelled-tokens", {}, "Opaque"))
write("secret-satoken", secret(
    "rta-satoken-tokens", claim_label, "kubernetes.io/service-account-token",
    {"kubernetes.io/service-account.name": "default"},
))
PY

mapfile -t OWNED_VAP <"$OUT/vap.txt"
mapfile -t OWNED_VAPB <"$OUT/vapb.txt"
policy_messages() {
  local file="$OUT/messages/$1.txt"
  [[ -s "$file" ]] || fail "the chart rendered no ValidatingAdmissionPolicy $1 (or it has no validations)"
  cat "$file"
}

k create namespace "$NS" >/dev/null || fail "create namespace $NS"
CREATED_NS=1
k apply -f "$OUT/apply.yaml" >/dev/null || fail "apply the rendered policies, bindings, and worker RBAC"
echo "applied ${OWNED_VAP[*]} and the worker RBAC into $NS"

# A new policy takes a moment to reach the admission plugin. Wait until the
# worker's hostPath write is refused by the policy itself, never a stale admit.
activated=0
last=""
for _ in $(seq 1 15); do
  last="$(worker_create "$(fixture deny-hostpath)" 2>&1 || true)"
  if grep -qF "ValidatingAdmissionPolicy '${RESOURCES_POLICY}'" <<<"$last" && grep -qF "$HOSTPATH_MSG" <<<"$last"; then
    activated=1
    break
  fi
  sleep 2
done
(( activated )) || fail "activation: within 30s ${RESOURCES_POLICY} never refused a hostPath template from the worker with \"${HOSTPATH_MSG}\"; last response: $last"

# --- must admit -----------------------------------------------------------
expect_admitted "chart runner template copy (chart images, SA, automount off)" worker_create "$(fixture chart-copy)"
expect_admitted "per-claim template copy with a token secretKeyRef" worker_create "$(fixture claim-copy)"
expect_admitted "labelled Opaque -tokens Secret" worker_create "$(fixture secret-claim)"
admin_create "$(fixture owned-template)" >/dev/null || fail "admin create of $OWNED_TEMPLATE"
expect_admitted "delete of a per-claim template" \
  as_worker delete sandboxtemplate "$OWNED_TEMPLATE" -n "$NS" --dry-run=server

# --- must deny: template shape ---------------------------------------------
deny_template() { expect_denied "$1" "$RESOURCES_POLICY" "$2" worker_create "$(fixture "deny-$3")"; }
deny_template "hostPath volume" "$HOSTPATH_MSG" hostpath
deny_template "hostNetwork" "$HOSTNS_MSG" hostnetwork
deny_template "hostPID" "$HOSTNS_MSG" hostpid
deny_template "hostIPC" "$HOSTNS_MSG" hostipc
deny_template "automountServiceAccountToken true" "$TOKEN_MSG" automount-true
deny_template "automountServiceAccountToken absent" "$TOKEN_MSG" automount-absent
deny_template "projected ServiceAccount token volume" "$TOKEN_MSG" projected-token
deny_template "serviceAccountName default" "$SA_MSG" sa-default
deny_template "runner image unlisted" "$IMAGES_MSG" image
deny_template "init container image unlisted" "$IMAGES_MSG" init-image
deny_template "privileged container" "$HOSTFIELDS_MSG" privileged
deny_template "capabilities.add" "$HOSTFIELDS_MSG" cap-add
deny_template "hostPort" "$HOSTFIELDS_MSG" hostport

# --- must deny: Secrets and deletes ----------------------------------------
SECRET_MESSAGES="$(policy_messages "$SECRETS_POLICY")"
expect_denied "unlabelled Secret" "$SECRETS_POLICY" "$SECRET_MESSAGES" \
  worker_create "$(fixture secret-unlabelled)"
expect_denied "service-account-token Secret" "$SECRETS_POLICY" "$SECRET_MESSAGES" \
  worker_create "$(fixture secret-satoken)"
CLEANUP_MESSAGES="$(policy_messages "$CLEANUP_POLICY")"
admin_create "$(fixture chart-template)" >/dev/null || fail "admin create of $CHART_TEMPLATE"
expect_denied "delete of the chart's own template" "$CLEANUP_POLICY" "$CLEANUP_MESSAGES" \
  as_worker delete sandboxtemplate "$CHART_TEMPLATE" -n "$NS" --dry-run=server

# --- negative control: the denial is the worker-scoped policy ---------------
control="$(k create --dry-run=server -n "$NS" -o json -f "$(fixture deny-hostpath)" 2>&1)" \
  || fail "negative control: the cluster admin's hostPath template was refused, so the worker denial may not be the worker-scoped policy: $control"
python3 -c '
import json, sys
doc = json.loads(sys.stdin.read())
vols = doc["spec"]["podTemplate"]["spec"].get("volumes") or []
sys.exit(0 if any("hostPath" in v for v in vols) else 1)
' <<<"$control" || fail "negative control: the admitted admin template lost its hostPath (schema pruning), so the worker denial proves nothing about the policy"
ok "negative control: the same hostPath template from the cluster admin is admitted with its hostPath intact"

echo "ALL PASS ($pass checks) context=$CTX run=$RUN_ID"
