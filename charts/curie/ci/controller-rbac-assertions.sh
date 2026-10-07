#!/usr/bin/env bash
#
# Render-assertion test for issue #350 (controller NetworkPolicy RBAC:
# cluster-read + namespaced-mutate split, plus the install-time controller-ready
# gate). Pins the verb-split shape so re-vendoring the upstream agent-sandbox
# controller -- or any RBAC edit -- cannot silently regress in EITHER direction:
# re-widening mutate to cluster scope (regresses #66) or re-breaking the
# informer by confining cluster LIST/WATCH to a namespaced Role (the #350
# crash-loop). See docs/adr/0023-controller-networkpolicy-rbac-cluster-read-namespace-mutate.md.
#
# Seven assertions. (a)-(e) scan the FULL multi-doc render (ClusterRoles come from
# BOTH templates/agent-sandbox.yaml and the vendored
# files/agent-sandbox/controller.yaml, so no --show-only); (f) EXECUTES the
# rendered preflight script against a stub kubectl:
#
#   (a) Exactly ONE cluster-scope ClusterRole grants networkpolicies, its verb
#       set is exactly {get,list,watch}, bound to SA agent-sandbox-controller in
#       agent-sandbox-system. (informer can sync; read-only)
#   (b) NO ClusterRole grants any mutate verb on networkpolicies anywhere
#       (the #66 regression tripwire; passes today, guards future re-vendoring).
#   (c) A namespaced Role agent-sandbox-controller-networkpolicies in the release
#       namespace keeps create/delete/patch/update/get and drops list/watch,
#       bound to the same SA.
#   (d) The controller-ready preflight gate renders with defaults and suppresses
#       correctly under agentSandbox.controller.deploy=false and
#       preflights.controllerReady.enabled=false. Its Role grants only the
#       deployment/pod/log reads plus get on pods/proxy needed for metrics.
#   (e) The gate's FAIL diagnostic has a lease-specific branch (issue #507).
#   (f) The gate's classifier BEHAVES: run the rendered script under sh with a
#       stub kubectl serving crafted logs. It must not fabricate an RBAC match
#       from two concatenated logs, and cause-specific remediation must print
#       only under its own branch (issue #611). (e) is presence-only and cannot
#       see either bug.
#   (g) Startup logs and positive successful-reconcile metrics both pass, zero
#       success counters and failed metrics requests refuse, and every failure
#       takes precedence over either success signal (issue #4005).
#
# Runnable locally (from anywhere) and from CI. Fails loudly, naming the
# violated assertion.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

# Deterministic release name + namespace so the release-namespace assertion (c)
# is unambiguous. curie.fullname collapses to the release name here (the chart
# name "curie" is a substring of "curie-assert").
RELEASE="curie-assert"
NS="curie-assert"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

DEFAULT="$TMP/default.yaml"
NOCTRL="$TMP/noctrl.yaml"
NOGATE="$TMP/nogate.yaml"

echo "=== Rendering chart (defaults) ==="
helm template "$RELEASE" "$CHART" --namespace "$NS" > "$DEFAULT"

echo "=== Rendering chart (agentSandbox.controller.deploy=false) ==="
helm template "$RELEASE" "$CHART" --namespace "$NS" \
  --set agentSandbox.controller.deploy=false > "$NOCTRL"

echo "=== Rendering chart (preflights.controllerReady.enabled=false) ==="
helm template "$RELEASE" "$CHART" --namespace "$NS" \
  --set preflights.controllerReady.enabled=false > "$NOGATE"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# All four assertions run in one PyYAML pass over the three renders. The Python
# emits an "ok:" line per assertion and exits nonzero (printing the reason) on
# the first violation, which the bash fail() then surfaces by assertion label.
ASSERT_PY="$TMP/assert.py"
cat > "$ASSERT_PY" <<'PY'
import sys, yaml

default_path, noctrl_path, nogate_path, release_ns = sys.argv[1:5]

CONTROLLER_SA = ("agent-sandbox-controller", "agent-sandbox-system")
NP_ROLE = "agent-sandbox-controller-networkpolicies"
READ_SUFFIX = "networkpolicies-read"
PREFLIGHT_SUFFIX = "-preflight-controller"
MUTATE_VERBS = {"create", "delete", "patch", "update", "deletecollection", "*"}


def load(path):
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if d]


def docs_of_kind(docs, kind):
    return [d for d in docs if d.get("kind") == kind]


def rule_is_about_networkpolicies(rule):
    # A rule may list several resources/apiGroups in one entry; treat it as
    # touching networkpolicies if "networkpolicies" (or the "*" wildcard) is in
    # its resources.
    resources = rule.get("resources") or []
    return "networkpolicies" in resources or "*" in resources


def networkpolicy_verbs(role):
    verbs = set()
    for rule in role.get("rules") or []:
        if rule_is_about_networkpolicies(rule):
            verbs.update(rule.get("verbs") or [])
    return verbs


def role_mentions_networkpolicies(role):
    return any(rule_is_about_networkpolicies(r) for r in (role.get("rules") or []))


def binding_targets(binding, role_kind, role_name):
    ref = binding.get("roleRef") or {}
    if ref.get("kind") != role_kind or ref.get("name") != role_name:
        return False
    for subj in binding.get("subjects") or []:
        if (
            subj.get("kind") == "ServiceAccount"
            and (subj.get("name"), subj.get("namespace")) == CONTROLLER_SA
        ):
            return True
    return False


def die(msg):
    sys.stdout.write(msg + "\n")
    sys.exit(1)


default_docs = load(default_path)
noctrl_docs = load(noctrl_path)
nogate_docs = load(nogate_path)

cluster_roles = docs_of_kind(default_docs, "ClusterRole")
cluster_role_bindings = docs_of_kind(default_docs, "ClusterRoleBinding")

# --- (a) Exactly one cluster-scope read grant, nothing more ---
np_cluster_roles = [cr for cr in cluster_roles if role_mentions_networkpolicies(cr)]
if len(np_cluster_roles) != 1:
    names = sorted((cr.get("metadata") or {}).get("name") for cr in np_cluster_roles)
    die(
        "(a) exactly one cluster read grant — expected exactly 1 ClusterRole "
        "with a networkpolicies rule, found %d: %s" % (len(np_cluster_roles), names)
    )
read_cr = np_cluster_roles[0]
read_cr_name = (read_cr.get("metadata") or {}).get("name")
verbs = networkpolicy_verbs(read_cr)
if verbs != {"get", "list", "watch"}:
    die(
        "(a) exactly one cluster read grant — ClusterRole %r networkpolicies "
        "verbs must be exactly {get, list, watch}, got %s"
        % (read_cr_name, sorted(verbs))
    )
if not any(binding_targets(b, "ClusterRole", read_cr_name) for b in cluster_role_bindings):
    die(
        "(a) exactly one cluster read grant — no ClusterRoleBinding binds "
        "ClusterRole %r to ServiceAccount agent-sandbox-controller in "
        "agent-sandbox-system" % read_cr_name
    )
print("  ok: (a) exactly one networkpolicies ClusterRole %r, verbs {get,list,watch}, bound to the controller SA" % read_cr_name)

# --- (b) No cluster-wide mutate, anywhere ---
for cr in cluster_roles:
    name = (cr.get("metadata") or {}).get("name")
    bad = networkpolicy_verbs(cr) & MUTATE_VERBS
    if bad:
        die(
            "(b) no cluster-wide mutate — ClusterRole %r grants mutate verb(s) "
            "%s on networkpolicies (regresses #66)" % (name, sorted(bad))
        )
print("  ok: (b) no ClusterRole grants create/delete/patch/update/* on networkpolicies")

# --- (c) Namespaced mutate intact ---
np_roles = [
    r
    for r in docs_of_kind(default_docs, "Role")
    if (r.get("metadata") or {}).get("name") == NP_ROLE
]
if len(np_roles) != 1:
    die("(c) namespaced mutate intact — expected exactly one Role %r, found %d" % (NP_ROLE, len(np_roles)))
np_role = np_roles[0]
role_ns = (np_role.get("metadata") or {}).get("namespace")
if role_ns != release_ns:
    die("(c) namespaced mutate intact — Role %r must be in the release namespace %r, got %r" % (NP_ROLE, release_ns, role_ns))
rverbs = networkpolicy_verbs(np_role)
required = {"create", "delete", "patch", "update", "get"}
missing = required - rverbs
if missing:
    die("(c) namespaced mutate intact — Role %r networkpolicies verbs must be a superset of %s, missing %s" % (NP_ROLE, sorted(required), sorted(missing)))
forbidden = {"list", "watch"} & rverbs
if forbidden:
    die("(c) namespaced mutate intact — Role %r must NOT grant %s on networkpolicies (now served cluster-wide, #350)" % (NP_ROLE, sorted(forbidden)))
role_bindings = [
    rb
    for rb in docs_of_kind(default_docs, "RoleBinding")
    if (rb.get("metadata") or {}).get("namespace") == release_ns
]
if not any(binding_targets(rb, "Role", NP_ROLE) for rb in role_bindings):
    die("(c) namespaced mutate intact — no RoleBinding in %r binds Role %r to the controller SA" % (release_ns, NP_ROLE))
print("  ok: (c) namespaced Role %r keeps create/delete/patch/update/get, drops list/watch, bound to the controller SA" % NP_ROLE)

# --- (d) Gate renders/suppresses with its flags ---
def names_by_kind(docs, kind):
    return [(d.get("metadata") or {}).get("name") for d in docs_of_kind(docs, kind)]

def has_suffix(names, suffix):
    return [n for n in names if n and n.endswith(suffix)]

# (d.1) defaults: preflight Job + its ServiceAccount render.
default_jobs = has_suffix(names_by_kind(default_docs, "Job"), PREFLIGHT_SUFFIX)
if not default_jobs:
    die("(d) gate renders — defaults must render a Job whose name ends with %r; none found" % PREFLIGHT_SUFFIX)
default_sas = has_suffix(names_by_kind(default_docs, "ServiceAccount"), PREFLIGHT_SUFFIX)
if not default_sas:
    die("(d) gate renders — defaults must render a ServiceAccount whose name ends with %r; none found" % PREFLIGHT_SUFFIX)
print("  ok: (d.1) defaults render preflight Job %s and its ServiceAccount" % default_jobs)

# (d.2) controller.deploy=false: NONE of the gate Job, the read ClusterRole/CRB,
# the namespaced Role/RoleBinding render.
noctrl_offenders = []
noctrl_offenders += ["Job " + n for n in has_suffix(names_by_kind(noctrl_docs, "Job"), PREFLIGHT_SUFFIX)]
noctrl_offenders += ["ClusterRole " + n for n in has_suffix(names_by_kind(noctrl_docs, "ClusterRole"), READ_SUFFIX)]
noctrl_offenders += [
    "ClusterRoleBinding " + n
    for n in has_suffix(names_by_kind(noctrl_docs, "ClusterRoleBinding"), READ_SUFFIX)
]
noctrl_offenders += ["Role " + n for n in names_by_kind(noctrl_docs, "Role") if n == NP_ROLE]
noctrl_offenders += ["RoleBinding " + n for n in names_by_kind(noctrl_docs, "RoleBinding") if n == NP_ROLE]
if noctrl_offenders:
    die("(d) gate suppresses — with controller.deploy=false these must NOT render: %s" % noctrl_offenders)
print("  ok: (d.2) controller.deploy=false suppresses the gate Job, the read ClusterRole/CRB, and the namespaced Role/RoleBinding")

# (d.3) controllerReady.enabled=false (deploy still true): Job absent, but the
# read ClusterRole and namespaced Role STILL render (RBAC split is independent).
nogate_jobs = has_suffix(names_by_kind(nogate_docs, "Job"), PREFLIGHT_SUFFIX)
if nogate_jobs:
    die("(d) gate suppresses — with controllerReady.enabled=false the preflight Job must be absent, found %s" % nogate_jobs)
nogate_read = has_suffix(names_by_kind(nogate_docs, "ClusterRole"), READ_SUFFIX)
if not nogate_read:
    die("(d) RBAC split independent of gate — with controllerReady.enabled=false the read ClusterRole (*%s) must still render" % READ_SUFFIX)
nogate_role = [n for n in names_by_kind(nogate_docs, "Role") if n == NP_ROLE]
if not nogate_role:
    die("(d) RBAC split independent of gate — with controllerReady.enabled=false Role %r must still render" % NP_ROLE)
print("  ok: (d.3) controllerReady.enabled=false suppresses only the Job; the RBAC split still renders")

# (d.4) Metrics use the API-server pod proxy, with no additional write or
# cluster-scoped grant. Compare complete rules so a wildcard cannot hide here.
preflight_roles = [
    role for role in docs_of_kind(default_docs, "Role")
    if ((role.get("metadata") or {}).get("name") or "").endswith(PREFLIGHT_SUFFIX)
]
if len(preflight_roles) != 1:
    die("(d.4) preflight RBAC: expected exactly one controller-ready Role, got %d" % len(preflight_roles))
preflight_role = preflight_roles[0]
if (preflight_role.get("metadata") or {}).get("namespace") != CONTROLLER_SA[1]:
    die("(d.4) preflight RBAC: controller-ready Role must stay in agent-sandbox-system")
expected_rules = [
    {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list", "watch"]},
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]},
    {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
    {"apiGroups": [""], "resources": ["pods/proxy"], "verbs": ["get"]},
]
actual_rules = preflight_role.get("rules") or []
if len(actual_rules) != len(expected_rules) or any(rule not in actual_rules for rule in expected_rules):
    die("(d.4) preflight RBAC: expected only deployment/pod/log reads and pods/proxy get, got %r" % actual_rules)
print("  ok: (d.4) preflight Role adds only pods/proxy get to its existing read grants")
PY

if ! out="$(python3 "$ASSERT_PY" "$DEFAULT" "$NOCTRL" "$NOGATE" "$NS" 2>&1)"; then
  fail "$out"
fi
echo "$out"

# --- (e) The preflight FAIL diagnostic classifies the CAUSE (issue #507) ---
# A leader-election lease timeout and the #350 RBAC crash-loop both restart the
# controller pod, so the gate must not blame NetworkPolicy RBAC unconditionally.
# Assert the rendered Job script both DETECTS the lease signal and emits a
# lease-specific (non-RBAC) diagnostic, so a regression back to the hardcoded
# RBAC blame fails here.
grep -q "leader election lost\|failed to renew lease" "$DEFAULT" \
  || fail "(e) cause classification — preflight script must grep for a leader-election lease signal (issue #507)"
grep -q "lost its leader-election lease" "$DEFAULT" \
  || fail "(e) cause classification — preflight FAIL diagnostic must have a lease-specific branch (issue #507)"
grep -q "NOT an RBAC/NetworkPolicy problem" "$DEFAULT" \
  || fail "(e) cause classification — the lease diagnostic must explicitly disclaim RBAC as the cause (issue #507)"
echo "  ok: (e) the controller-ready gate distinguishes a lease timeout from an RBAC failure"

# --- (f) The classifier BEHAVES correctly, executed against a stub kubectl ---
# (e) above is presence-only: it greps the render for strings and so cannot see
# either a log-concatenation false match or a diagnostic leaking across case
# branches. So actually RUN the rendered script under `sh` with a stub kubectl
# that serves crafted logs per scenario, and assert on its stdout (issue #611).
FDIR="$TMP/f"
export FDIR
mkdir -p "$FDIR/bin"

# Pull the inline /bin/sh -c script body out of the preflight Job's container.
python3 - "$DEFAULT" "$FDIR/preflight.sh" <<'PY' || fail "(f) behavioral classifier -- could not extract the preflight script from the render"
import sys, yaml

render_path, out_path = sys.argv[1:3]

with open(render_path) as f:
    docs = [d for d in yaml.safe_load_all(f) if d]

jobs = [
    d
    for d in docs
    if d.get("kind") == "Job"
    and ((d.get("metadata") or {}).get("name") or "").endswith("-preflight-controller")
]
if len(jobs) != 1:
    sys.stdout.write("expected exactly one *-preflight-controller Job, found %d\n" % len(jobs))
    sys.exit(1)

containers = jobs[0]["spec"]["template"]["spec"]["containers"]
command = containers[0].get("command") or []
if len(command) < 3 or command[0] != "/bin/sh" or command[1] != "-c":
    sys.stdout.write("preflight container command is not the expected /bin/sh -c form: %r\n" % (command,))
    sys.exit(1)

with open(out_path, "w") as f:
    f.write(command[2])
PY

# Stub kubectl. Dispatches on its args and serves the logs for ${SCENARIO}; the
# --previous case must be matched before the plain logs case (the real call adds
# --previous to an otherwise identical argv).
cat > "$FDIR/bin/kubectl" <<'STUB'
#!/bin/sh
case "$*" in
  *get*--raw*)
    case "$*" in
      */api/v1/namespaces/agent-sandbox-system/pods/agent-sandbox-controller-0:8080/proxy/metrics*) ;;
      *) echo "unexpected controller metrics proxy path: $*" >&2; exit 2 ;;
    esac
    printf '%s\n' "$*" >> "$FDIR/metrics-calls"
    # agent-sandbox v0.5.0 pins controller-runtime v0.23.3:
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/go.mod
    # Its CounterVec has controller/result labels, and successful reconciles
    # increment result="success"; error/requeue outcomes must not count:
    # https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.3/pkg/internal/controller/metrics/metrics.go
    # https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.3/pkg/internal/controller/controller.go
    if [ "$SCENARIO" = metrics_zero ]; then
      printf '# controller_runtime_reconcile_total{controller="comment",result="success"} 99\n'
      printf 'controller_runtime_reconcile_total{controller="sandbox",result="success"} 0\n'
      printf 'controller_runtime_reconcile_total{controller="sandboxwarmpool",result="success"} 0\n'
      printf 'controller_runtime_reconcile_total{controller="sandbox",result="error"} 12\n'
      printf 'controller_runtime_reconcile_total{controller="sandbox",result="requeue"} 8\n'
      printf 'controller_runtime_reconcile_errors_total{controller="sandbox",result="success"} 4\n'
    else
      printf '# HELP controller_runtime_reconcile_total Total number of reconciliations per controller\n'
      printf '# TYPE controller_runtime_reconcile_total counter\n'
      printf 'controller_runtime_reconcile_total{controller="sandbox",result="success"} 0\n'
      printf 'controller_runtime_reconcile_total{result="success",controller="sandboxwarmpool"} 2e+00\n'
      printf 'controller_runtime_reconcile_total{controller="sandbox",result="error"} 7\n'
    fi
    # Even a failed proxy command whose partial stdout contains a positive
    # success sample cannot establish controller health.
    [ "$SCENARIO" != metrics_error ] || exit 1
    ;;
  *rollout*status*)
    exit 0
    ;;
  *get*pods*jsonpath*restartCount*)
    case "$SCENARIO" in
      lease_glue|rbac|rbac_*|lease_*|restart_*) printf '1' ;;
      *) printf '0' ;;
    esac
    ;;
  *get*pods*jsonpath*)
    printf 'agent-sandbox-controller-0'
    ;;
  *get*pods*)
    printf 'agent-sandbox-controller-0   0/1   Error   1   30s\n'
    ;;
  *logs*--previous*)
    if [ "${SCENARIO}" = "lease_glue" ]; then
      # FIRST line is a leases-forbidden line: glued onto the current log's
      # networkpolicies-mentioning last line it fabricates an RBAC signature.
      printf 'Error: leases.coordination.k8s.io "agent-sandbox-controller" is forbidden: User cannot get resource\n'
      printf 'E0717 12:00:01 failed to renew lease agent-sandbox-system/agent-sandbox-controller: context deadline exceeded\n'
    fi
    ;;
  *logs*)
    printf 'I0717 12:00:00 starting manager\n'
    case "$SCENARIO" in
      startup|*_startup) printf 'I0717 12:00:00 Starting workers\n' ;;
    esac
    case "$SCENARIO" in
      lease_glue)
        # LAST line mentions networkpolicies but carries no "forbidden".
        printf 'I0717 12:00:00 reflector starting for networkpolicies\n'
        ;;
      rbac|rbac_*)
        printf 'E0717 12:00:00 reflector: failed to list *v1.NetworkPolicy: networkpolicies.networking.k8s.io is forbidden: User "system:serviceaccount:agent-sandbox-system:agent-sandbox-controller" cannot list resource "networkpolicies" at the cluster scope\n'
        ;;
      lease_*)
        printf 'E0717 12:00:00 failed to renew lease agent-sandbox-system/agent-sandbox-controller: context deadline exceeded\n'
        ;;
    esac
    ;;
  *) echo "unexpected kubectl request: $*" >&2; exit 2 ;;
esac
exit 0
STUB
chmod +x "$FDIR/bin/kubectl"

# The rendered script owns its logical TIMEOUT; avoid wall-clock delays in the
# zero/error metrics cases while still executing every iteration and refusal.
printf '#!/bin/sh\nexit 0\n' > "$FDIR/bin/sleep"
chmod +x "$FDIR/bin/sleep"

# TIMEOUT is small so the poll loop cannot linger; every scenario breaks out on
# the first iteration anyway. The FAIL path is expected to exit 1, so the
# function itself must not propagate that under set -e (it would abort this
# script) -- it stashes the exit code in $FDIR/rc for the caller to assert on
# instead.
run_preflight() {
  rm -f "$FDIR/metrics-calls"
  local rc=0
  if PATH="$FDIR/bin:$PATH" SCENARIO="$1" \
      CONTROLLER_NS=agent-sandbox-system DEPLOY=agent-sandbox-controller TIMEOUT=5 \
      sh "$FDIR/preflight.sh" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  echo "$rc" > "$FDIR/rc"
}

# (f.1) The glue regression (AC1): a lease failure whose current log also
# mentions networkpolicies must classify as lease, not rbac.
lease_out="$(run_preflight lease_glue)"
lease_rc="$(cat "$FDIR/rc")"
[ "$lease_rc" -eq 1 ] \
  || fail "(f.1) exit code -- the lease FAIL path must exit 1 (ADR-0023's gate must not pass a broken controller), got $lease_rc:
$lease_out"
echo "$lease_out" | grep -q "lost its leader-election lease" \
  || fail "(f.1) classifier glue -- a lease failure whose logs also mention networkpolicies must classify as lease, got:
$lease_out"
echo "$lease_out" | grep -q "forbidden networkpolicies log" \
  && fail "(f.1) classifier glue -- concatenating the current and previous logs fabricated an RBAC match (issue #611), got:
$lease_out"
echo "  ok: (f.1) a lease failure whose logs mention networkpolicies classifies as lease, not rbac, and exits 1"

# (f.2) Branch leak (AC2/AC3): the crash-loop hint is written for the #350 RBAC
# signature and must not trail a lease diagnostic. Presence-only greps cannot
# catch this; only running the lease branch can. Anchored on the hint's
# distinctive prose ("delete the controller") rather than "CrashLoopBackOff",
# since that status also appears in the FAIL diagnostic's own `kubectl get
# pods` dump and would false-FAIL the moment the stub's stubbed pod status
# stops being "Error".
echo "$lease_out" | grep -q "NOT an RBAC/NetworkPolicy problem" \
  || fail "(f.2) branch leak -- the lease diagnostic must disclaim RBAC, got:
$lease_out"
echo "$lease_out" | grep -q "delete the controller" \
  && fail "(f.2) branch leak -- the RBAC crash-loop hint must NOT print under the lease branch (issue #611), got:
$lease_out"
echo "  ok: (f.2) the lease branch emits no RBAC crash-loop hint"

# (f.3) The RBAC path still works: a genuine forbidden-networkpolicies line
# classifies as rbac AND keeps its crash-loop remediation.
rbac_out="$(run_preflight rbac)"
rbac_rc="$(cat "$FDIR/rc")"
[ "$rbac_rc" -eq 1 ] \
  || fail "(f.3) exit code -- the rbac FAIL path must exit 1 (ADR-0023's gate must not pass a broken controller), got $rbac_rc:
$rbac_out"
echo "$rbac_out" | grep -q "forbidden-networkpolicies logged" \
  || fail "(f.3) rbac path -- a genuine forbidden-networkpolicies log must classify as rbac, got:
$rbac_out"
echo "$rbac_out" | grep -q "delete the controller" \
  || fail "(f.3) rbac path -- the rbac branch must keep its crash-loop remediation, got:
$rbac_out"
echo "  ok: (f.3) a genuine RBAC failure classifies as rbac, keeps its crash-loop hint, and exits 1"

# --- (g) Durable health and failure precedence (issue #4005) ---
startup_out="$(run_preflight startup)"
startup_rc="$(cat "$FDIR/rc")"
[ "$startup_rc" -eq 0 ] && echo "$startup_out" | grep -q "RESULT: PASS.*Starting workers" \
  || fail "(g.1) healthy startup logs must pass, got rc=$startup_rc:
$startup_out"
[ ! -s "$FDIR/metrics-calls" ] \
  || fail "(g.1) Starting workers must retain its fast path without fetching metrics"
echo "  ok: (g.1) healthy startup logs pass without requesting metrics"

metrics_out="$(run_preflight metrics_positive)"
metrics_rc="$(cat "$FDIR/rc")"
[ "$metrics_rc" -eq 0 ] && echo "$metrics_out" | grep -qi "RESULT: PASS.*reconcile" \
  || fail "(g.2) successful reconcile metrics without startup logs must pass, got rc=$metrics_rc:
$metrics_out"
[ -s "$FDIR/metrics-calls" ] \
  || fail "(g.2) the metrics success path must fetch the selected controller pod through :8080/proxy/metrics"
echo "  ok: (g.2) positive successful-reconcile metrics pass with no startup log"

for scenario in metrics_zero metrics_error; do
  scenario_out="$(run_preflight "$scenario")"
  scenario_rc="$(cat "$FDIR/rc")"
  [ "$scenario_rc" -eq 1 ] && echo "$scenario_out" | grep -q "RESULT: FAIL" \
    || fail "(g.3) $scenario must refuse, got rc=$scenario_rc:
$scenario_out"
  [ -s "$FDIR/metrics-calls" ] \
    || fail "(g.3) $scenario must exercise the metrics request"
  echo "$scenario_out" | grep -q "RESULT: PASS" \
    && fail "(g.3) $scenario emitted a false PASS:
$scenario_out"
done
echo "  ok: (g.3) zero successful reconciles and failed metrics requests refuse"

for cause in rbac lease restart; do
  for signal in startup metrics; do
    scenario_out="$(run_preflight "${cause}_${signal}")"
    scenario_rc="$(cat "$FDIR/rc")"
    [ "$scenario_rc" -eq 1 ] && echo "$scenario_out" | grep -q "RESULT: FAIL" \
      || fail "(g.4) $cause must take precedence over $signal, got rc=$scenario_rc:
$scenario_out"
    echo "$scenario_out" | grep -q "RESULT: PASS" \
      && fail "(g.4) $cause emitted a false PASS with misleading $signal:
$scenario_out"
    [ ! -s "$FDIR/metrics-calls" ] \
      || fail "(g.4) $cause must be classified before requesting positive metrics"
    case "$cause" in
      rbac) expected="forbidden-networkpolicies logged" ;;
      lease) expected="lost its leader-election lease" ;;
      restart) expected="restartCount>0" ;;
    esac
    echo "$scenario_out" | grep -q "$expected" \
      || fail "(g.4) $cause classification was lost with misleading $signal:
$scenario_out"
  done
done
echo "  ok: (g.4) RBAC, lease, and restart failures take precedence over startup logs and positive metrics"

echo
echo "PASS: controller RBAC stays read-only at cluster scope and mutate stays namespaced; preflight RBAC adds only pods/proxy get; the gate renders and suppresses correctly, preserves cause-specific diagnostics, passes healthy startup logs or positive successful-reconcile metrics, and refuses every failure before either success signal."
