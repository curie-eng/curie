#!/usr/bin/env bash
# Assert Rail 1 (ADR-0067) actually ENFORCES, not merely that it is applied.
#
# The distinction is the whole point. Every NetworkPolicy the chart ships is
# applied on any cluster; whether it is EVALUATED depends on the CNI. kindnet
# (kind's default) and minikube's default bridge implement no NetworkPolicy
# controller, so a totally broken Rail 1 is indistinguishable from a correct one
# -- both let everything through, and every "can the sandbox reach X?" assertion
# passes. That is how an egress rule naming a Service ClusterIP, which can never
# match because kube-proxy DNATs to a pod IP before policy is evaluated, reached
# a real cluster (#1153).
#
# So this is structured as a NON-VACUITY check. It asserts a denied direction is
# genuinely blocked BEFORE trusting any allowed direction. If the deny does not
# hold, the CNI is not enforcing and the script FAILS -- it does not skip and it
# does not pass. A green run on a non-enforcing cluster would be worse than no
# check at all, because it reads as proof.
set -euo pipefail

NS="${1:-${CURIE_NETPOL_NS:-curie}}"
RELEASE="${2:-${CURIE_NETPOL_RELEASE:-curie}}"
APP="${3:-${CURIE_NETPOL_APP:-curie}}"
# The namespace-axis probe needs a namespace that is NOT the connectors'. A
# namespace this script creates carries no NetworkPolicy of its own, so a denial
# observed from it is attributable to the connector's ingress policy and not to a
# neighbour's egress default-deny. `default` was rejected for exactly that
# reason: it always exists, but on a real operator cluster it may already carry
# an egress default-deny, which would make the foreign denial pass vacuously.
#
# "Carries no policy" is an assumption, not a fact -- the namespace is reused
# across runs, an admission webhook can inject a policy into a namespace it did
# not create, and the override hands the choice to an operator -- so it is
# VERIFIED below rather than trusted. The two sources also get different
# treatment: only the derived default is ours to create, so an explicitly
# supplied namespace is never created here.
FOREIGN_NS="${CURIE_NETPOL_FOREIGN_NS:-${NS}-netpol-foreign}"
PROBE_IMAGE="${CURIE_NETPOL_PROBE_IMAGE:-curlimages/curl:8.10.1}"
# The deny target must actually LISTEN, so a blocked attempt is a timeout rather
# than an ambiguous connection-refused that a missing listener would also give.
TARGET_IMAGE="${CURIE_NETPOL_TARGET_IMAGE:-hashicorp/http-echo:1.0}"

SANDBOX_POD="netpol-probe-sandbox"
OUTSIDE_POD="netpol-probe-outside"
DENY_TARGET_POD="netpol-probe-deny-target"
FOREIGN_POD="netpol-probe-foreign"

# Non-blocking on the way out: the run is over, nothing waits on the pods.
# The pod is deleted; $FOREIGN_NS itself deliberately is NOT. Deleting a
# namespace from a non-blocking trap leaves it Terminating for tens of seconds,
# and a Terminating namespace REJECTS creates -- so the next run would fail on
# namespace creation for reasons that have nothing to do with policy. That is
# the same race cleanup_and_settle exists to avoid, one level up. An empty
# namespace is inert; the pod is the thing that must not leak.
cleanup() {
  kubectl -n "$NS" delete pod "$SANDBOX_POD" "$OUTSIDE_POD" "$DENY_TARGET_POD" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$FOREIGN_NS" delete pod "$FOREIGN_POD" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
# BLOCKING before the apply. A previous run's pod may still be terminating, and
# re-applying the same name against a deleting pod silently binds the OLD one --
# `wait --for=Ready` then passes on a pod that is on its way out and every exec
# after it fails for reasons that have nothing to do with policy.
cleanup_and_settle() {
  kubectl -n "$NS" delete pod "$SANDBOX_POD" "$OUTSIDE_POD" "$DENY_TARGET_POD" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "$FOREIGN_NS" delete pod "$FOREIGN_POD" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== Rail 1 enforcement check (namespace=$NS release=$RELEASE app=$APP) =="

kubectl -n "$NS" get networkpolicy "${RELEASE}-runner-default-deny-egress" >/dev/null 2>&1 \
  || fail "no ${RELEASE}-runner-default-deny-egress in $NS; is the chart installed?"

# The sandbox probe wears exactly the labels Rail 1 selects, so the policies
# under test apply to it. The outside probe and deny target wear none of the
# runner sandbox labels.
#
# The foreign probe is the third shape, and the one that covers the second axis.
# The connector ingress `from` block is a single bare podSelector peer, which is
# implicitly scoped to the policy's own namespace, so it can be widened two
# independent ways: on the POD axis (a second peer, an empty podSelector, an
# ipBlock), which the unlabelled same-namespace outside probe catches; and on the
# NAMESPACE axis (a namespaceSelector: {} merged INTO that peer), which admits
# sandbox-labelled pods in every namespace while leaving the unlabelled probe
# denied. The foreign probe wears the SAME labels as the sandbox probe and sits
# in a DIFFERENT namespace -- it differs from the sandbox on exactly the axis the
# other two probes hold constant, which is the only reason it can see that
# widening (#1502).
if [ -n "${CURIE_NETPOL_FOREIGN_NS:-}" ]; then
  # Operator-supplied: neither created nor even read. `get namespace` and
  # `create namespace` are BOTH cluster-scoped, and this override exists
  # precisely for the operator who lacks that RBAC -- an escape hatch that trips
  # on the same permission it is escaping is a guard in name only. If the
  # namespace is actually missing, the policy listing, the pod apply and the
  # Ready wait below all fail loudly, so nothing passes vacuously by skipping it.
  echo "  ok  foreign namespace $FOREIGN_NS supplied by CURIE_NETPOL_FOREIGN_NS (not created here)"
else
  # Re-check existence after a failed create rather than trusting the create's
  # exit status: two first runs against one cluster both miss on `get`, one
  # create wins, and the loser hard-fails on AlreadyExists for a namespace that
  # by then exists and is perfectly usable.
  kubectl get namespace "$FOREIGN_NS" >/dev/null 2>&1 \
    || kubectl create namespace "$FOREIGN_NS" >/dev/null 2>&1 \
    || kubectl get namespace "$FOREIGN_NS" >/dev/null 2>&1 \
    || fail "could not create namespace $FOREIGN_NS for the namespace-axis ingress probe.

Creating it needs cluster-scoped RBAC to get and create namespaces, because the
foreign probe must run somewhere other than $NS. Grant that, or set
CURIE_NETPOL_FOREIGN_NS to an existing namespace that carries no NetworkPolicy
of its own -- when it is set this script neither gets nor creates the namespace,
so no cluster-scoped permission is required at all.

Skipping the leg is not an option here. A green run missing a deny leg reads as
proof that connector ingress is narrow when in fact nothing checked it, which is
worse than no check at all (#1502)."
fi

# $FOREIGN_NS has to be a policy VACUUM or a denial observed from it is
# unattributable, and nothing so far establishes that. Concretely: an admission
# webhook (Kyverno, Gatekeeper, Rancher) injects an egress policy into the
# namespace that allows the pod CIDR and DNS but not the Service CIDR. Both
# foreign positive controls below still pass -- they use a pod IP and DNS -- the
# connector curl then fails on the PROBE'S OWN EGRESS, and the leg prints ok
# while connector ingress is wide open. Listing policies is namespaced, so it
# still works for an operator who supplied $FOREIGN_NS without cluster-scoped
# rights.
if ! FOREIGN_POLICIES="$(kubectl -n "$FOREIGN_NS" get networkpolicy \
     -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null)"; then
  fail "could not list NetworkPolicies in $FOREIGN_NS.

Either that namespace does not exist, or this context cannot read
NetworkPolicies in it. The listing cannot be skipped: the foreign probe's
denial is only attributable to connector ingress if its own namespace carries no
policy of its own. Reading NetworkPolicies is a namespaced permission, not a
cluster-scoped one -- grant it in $FOREIGN_NS, or point CURIE_NETPOL_FOREIGN_NS
at an existing policy-free namespace you can read (#1502)."
fi
if [ -n "$FOREIGN_POLICIES" ]; then
  fail "namespace $FOREIGN_NS carries NetworkPolicies of its own: $FOREIGN_POLICIES

A denial observed from that namespace is then unattributable -- it could be the
connector's ingress policy, or it could be an egress rule applying to the probe
itself. An injected egress policy that allows the pod CIDR and DNS but not the
Service CIDR leaves both foreign positive controls passing, so this leg would
print ok while connector ingress is in fact wide open.

Point CURIE_NETPOL_FOREIGN_NS at a namespace that carries no NetworkPolicy, or
remove the policies from $FOREIGN_NS (#1502)."
fi
echo "  ok  foreign namespace $FOREIGN_NS carries no NetworkPolicy of its own"

cleanup_and_settle
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $SANDBOX_POD
  labels:
    app.kubernetes.io/name: $APP
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: $OUTSIDE_POD
  labels:
    app.kubernetes.io/name: netpol-probe-outside
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: $DENY_TARGET_POD
  labels:
    app.kubernetes.io/name: netpol-probe-deny-target
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $TARGET_IMAGE
      args: ["-listen=:8000", "-text=reachable"]
YAML

# Label-for-label identical to $SANDBOX_POD. Any difference here would make a
# foreign denial ambiguous -- it could be the namespace axis or it could be the
# labels -- and the whole point is that the namespace is the only variable.
kubectl -n "$FOREIGN_NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $FOREIGN_POD
  labels:
    app.kubernetes.io/name: $APP
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: runner-sandbox
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $PROBE_IMAGE
      command: ["sleep", "600"]
YAML

kubectl -n "$NS" wait --for=condition=Ready \
  "pod/$SANDBOX_POD" "pod/$OUTSIDE_POD" "pod/$DENY_TARGET_POD" --timeout=180s >/dev/null \
  || fail "probe pods did not become ready"

# Separate call: `wait` is namespace-scoped, so the foreign pod cannot join the
# list above. A pod that never came up would silently curl nothing, and this
# fail() is what stops that from reading as a denial.
kubectl -n "$FOREIGN_NS" wait --for=condition=Ready \
  "pod/$FOREIGN_POD" --timeout=180s >/dev/null \
  || fail "the foreign-namespace probe pod did not become ready in $FOREIGN_NS"

# The labels are read back off the LIVE object, never assumed from the apply. A
# mutating webhook can strip or rewrite app.kubernetes.io/component on a pod it
# did not create, and a future edit to the YAML above has the identical effect.
# Neither shows up anywhere else: the pod still goes Ready and both foreign
# positive controls still pass, because neither depends on labels.
FOREIGN_LABELS="$(kubectl -n "$FOREIGN_NS" get pod "$FOREIGN_POD" \
  -o jsonpath='{.metadata.labels.app\.kubernetes\.io/name} {.metadata.labels.app\.kubernetes\.io/instance} {.metadata.labels.app\.kubernetes\.io/component}' 2>/dev/null || true)"
if [ "$FOREIGN_LABELS" != "$APP $RELEASE runner-sandbox" ]; then
  fail "the foreign probe in $FOREIGN_NS is not wearing the runner-sandbox labels.

  expected  $APP $RELEASE runner-sandbox
  read back $FOREIGN_LABELS

Wearing them is the entire reason this probe can see the namespace axis. An
unlabelled pod is denied by a correct connector policy and by a
namespace-widened one alike, so this leg would degrade into a second copy of the
same-namespace outside probe -- green, and blind to the widening it exists to
catch (#1502)."
fi
echo "  ok  foreign probe wears the runner-sandbox labels"

DENY_TARGET_IP="$(kubectl -n "$NS" get pod "$DENY_TARGET_POD" -o jsonpath='{.status.podIP}')"
[ -n "$DENY_TARGET_IP" ] || fail "could not read the deny target's pod IP"

# ---------------------------------------------------------------------------
# GATE: the deny must hold. Everything below is meaningless without this.
# ---------------------------------------------------------------------------
# Rail 1 denies all sandbox egress except what an allow policy adds, and nothing
# allows the deny target. Reaching it means policy is not being evaluated.
if kubectl -n "$NS" exec "$SANDBOX_POD" -- \
     curl -s -m 8 -o /dev/null "http://${DENY_TARGET_IP}:8000/" 2>/dev/null; then
  fail "the sandbox reached a pod Rail 1 denies.

This is NOT a policy bug -- it means the CNI is not enforcing NetworkPolicy at
all, so every other assertion here would pass vacuously and this suite would
certify a broken Rail 1 as working.

  kind      needs 'disableDefaultCNI: true' plus a policy-enforcing CNI;
            the default kindnet implements no NetworkPolicy controller.
  minikube  needs 'minikube start --cni=calico'.
  k3s/k3d   enforce by default via kube-router.

Re-run against a cluster whose CNI enforces."
fi
echo "  ok  deny holds -- the CNI enforces NetworkPolicy (non-vacuity gate)"

# The outside probe must reach the same known listener before its inability to
# reach a connector can prove ingress enforcement. Otherwise a broken outside
# network would make every connector ingress denial pass vacuously.
if ! kubectl -n "$NS" exec "$OUTSIDE_POD" -- \
      curl -s -m 8 -o /dev/null "http://${DENY_TARGET_IP}:8000/" 2>/dev/null; then
  fail "the outside probe cannot reach the deny target; a broken outside network cannot certify connector ingress"
fi
echo "  ok  outside positive control reaches the deny target"

# ---------------------------------------------------------------------------
# Now the allow direction means something.
# ---------------------------------------------------------------------------
# The outside probe must resolve Service DNS before its inability to reach a
# connector can prove ingress enforcement. Otherwise a broken outside DNS
# path would make every connector ingress denial pass vacuously.
if ! kubectl -n "$NS" exec "$OUTSIDE_POD" -- \
      getent hosts kubernetes.default.svc.cluster.local >/dev/null 2>&1; then
  fail "the outside probe cannot resolve DNS; connector ingress denial would be vacuous"
fi
echo "  ok  outside positive control resolves DNS"

# The foreign probe carries its own pair of positive controls, for the same
# reason the outside probe does: without them its denial could be caused by the
# prober rather than by the policy, and a vacuous deny leg is the defect this
# whole script exists to prevent.
#
# Both controls MUST pass on a correct cluster, and that is provable rather than
# hopeful. NetworkPolicy is namespace-scoped, so nothing in $NS restricts egress
# from a pod in $FOREIGN_NS; and the deny target is selected by no ingress
# policy at all, so its ingress is unrestricted. A failure here therefore means
# the prober is broken, NOT that policy is working.
if ! kubectl -n "$FOREIGN_NS" exec "$FOREIGN_POD" -- \
      curl -s -m 8 -o /dev/null "http://${DENY_TARGET_IP}:8000/" 2>/dev/null; then
  fail "the foreign probe in $FOREIGN_NS cannot reach the deny target cross-namespace; a broken cross-namespace network cannot certify connector ingress"
fi
echo "  ok  foreign positive control reaches the deny target cross-namespace"

# Sharper here than for the outside probe: the foreign probe reaches connectors
# by NAME, so broken DNS would make every foreign denial vacuous while looking
# exactly like enforcement.
if ! kubectl -n "$FOREIGN_NS" exec "$FOREIGN_POD" -- \
      getent hosts kubernetes.default.svc.cluster.local >/dev/null 2>&1; then
  fail "the foreign probe cannot resolve DNS; connector ingress denial from $FOREIGN_NS would be vacuous"
fi
echo "  ok  foreign positive control resolves DNS"

# DNS is the allow every sandbox depends on (curie-runner-allow-dns); without it
# a sandbox cannot resolve any Service name, which surfaces as a confusing
# connection failure rather than a policy error.
if ! kubectl -n "$NS" exec "$SANDBOX_POD" -- \
      getent hosts kubernetes.default.svc.cluster.local >/dev/null 2>&1; then
  fail "the sandbox cannot resolve DNS; ${RELEASE}-runner-allow-dns is not effective"
fi
echo "  ok  allow holds -- the sandbox can resolve DNS"

# Every connector Service is exercised in both directions. A missing Service
# makes the connector policy check vacuous and is therefore fatal. Connectors
# are listed by the name label their Services share: behind a caller proxy
# (ADR-0168 decision 7) a connector also has a direct Service on the server's
# port, which only an operator policy opens, and it is not a second connector.
CONNECTORS=()
while IFS= read -r svc; do
  [ -n "$svc" ] && CONNECTORS+=("$svc")
done < <(
  kubectl -n "$NS" get svc -l app.kubernetes.io/part-of="$RELEASE" \
    -o jsonpath='{range .items[*]}{.metadata.labels.app\.kubernetes\.io/name}{"\n"}{end}' \
    2>/dev/null | grep -- "-mcp-" | sort -u || true
)
CONNECTOR_COUNT="${#CONNECTORS[@]}"
(( CONNECTOR_COUNT > 0 )) \
  || fail "found 0 connector Services in $NS; connector NetworkPolicy enforcement would be vacuous"
echo "  ok  connector Service count: $CONNECTOR_COUNT"

for svc in "${CONNECTORS[@]}"; do
  kubectl -n "$NS" rollout status "deployment/$svc" --timeout=180s >/dev/null 2>&1 \
    || fail "connector Deployment $svc did not become ready"

  PORT="$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.ports[0].port}')"
  kubectl -n "$NS" exec "$SANDBOX_POD" -- \
    curl -s -m 10 -o /dev/null "http://${svc}:${PORT}/" 2>/dev/null \
    || fail "the sandbox cannot reach connector Service $svc:$PORT.

Its egress rule is applied but not matching. The usual cause is an ipBlock of
the Service ClusterIP: kube-proxy DNATs the destination to a pod IP before
NetworkPolicy is evaluated, so such a rule can never match (ADR-0086)."
  echo "  ok  sandbox reaches connector $svc:$PORT"

  if kubectl -n "$NS" exec "$OUTSIDE_POD" -- \
       curl -s -m 10 -o /dev/null "http://${svc}:${PORT}/" 2>/dev/null; then
    fail "the outside probe reached connector Service $svc:$PORT; connector ingress is not restricted to runner sandboxes"
  fi
  echo "  ok  outside probe cannot reach connector $svc:$PORT"

  # This connector's exact FQDN must resolve from $FOREIGN_NS before its
  # unreachability can mean anything. The generic control above proves only that
  # kubernetes.default resolves, and a Cilium FQDN policy or a CoreDNS view can
  # permit that name while refusing this one -- curl would exit non-zero without
  # ever attempting the connection and the denial below would read as
  # enforcement.
  if ! kubectl -n "$FOREIGN_NS" exec "$FOREIGN_POD" -- \
        getent hosts "${svc}.${NS}.svc.cluster.local" >/dev/null 2>&1; then
    fail "the foreign probe cannot resolve ${svc}.${NS}.svc.cluster.local from $FOREIGN_NS.

Its failure to reach $svc would then be a DNS failure, not a policy denial, and
this leg would certify connector ingress as narrow without a single packet ever
having been sent to it (#1502)."
  fi
  echo "  ok  foreign probe resolves ${svc}.${NS}.svc.cluster.local"

  # By FQDN, never the short name. $svc resolves only inside $NS; from
  # $FOREIGN_NS it would fail DNS, curl would exit non-zero, and this leg would
  # read that as "policy denied me" without ever attempting the connection --
  # a false green of the worst kind. The FQDN forces the connection to be made
  # so that its failure is attributable to the ingress policy.
  if kubectl -n "$FOREIGN_NS" exec "$FOREIGN_POD" -- \
       curl -s -m 10 -o /dev/null "http://${svc}.${NS}.svc.cluster.local:${PORT}/" 2>/dev/null; then
    fail "a sandbox-labelled pod in $FOREIGN_NS reached connector Service $svc:$PORT.

The ingress 'from' peer has been widened on the NAMESPACE axis: a
namespaceSelector: {} merged into the existing peer makes it admit
sandbox-labelled pods in every namespace instead of only $NS. Any pod anyone can
schedule anywhere in this cluster can now wear those three labels and talk to a
connector holding a production credential.

The unlabelled same-namespace outside probe stays DENIED under this widening,
which is why it reports green and why this probe exists at all (#1502,
ADR-0086)."
  fi
  echo "  ok  foreign sandbox-labelled probe cannot reach connector $svc:$PORT"
done

echo "== Rail 1, connector egress, and connector ingress enforce; connector ingress proven narrow on BOTH axes (pod labels and namespace) =="
