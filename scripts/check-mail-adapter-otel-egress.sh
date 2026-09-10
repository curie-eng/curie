#!/usr/bin/env bash
# Assert the mail adapter's EXTERNAL OTel collector egress rule (#2361) actually
# ENFORCES, not merely that it renders.
#
# The mail adapter is the only first-party workload the chart gives an egress
# NetworkPolicy at all, so it is the only one whose own rail can silently drop
# its OTLP export. With `otelCollector.deploy` false and an external
# `otelCollector.endpoint`, that export leaves the cluster to an address the
# chart cannot know, so the operator declares it as
# `mailAdapter.otelEgress.httpsCidrs` and the template renders one ipBlock peer.
#
# A render assertion cannot prove that rule. Every NetworkPolicy this chart ships
# is applied on any cluster; whether it is EVALUATED depends on the CNI. kindnet
# (kind's default) and minikube's default bridge implement no NetworkPolicy
# controller, so an ipBlock on the wrong port -- or one deleted outright -- is
# indistinguishable from a correct one: everything gets through and every
# "can the adapter reach the collector?" assertion passes.
#
# So, like scripts/check-netpol-enforcement.sh, this is a NON-VACUITY check. It
# asserts an UNDECLARED peer is genuinely blocked BEFORE trusting the declared
# one. If the deny does not hold, the CNI is not enforcing (or the policy is
# broader than declared) and the script FAILS -- it does not skip and it does not
# pass. A green run on a non-enforcing cluster would be worse than no check at
# all, because it reads as proof that AC5 holds.
set -euo pipefail

CHART="${1:-${CURIE_MAIL_OTEL_CHART:-charts/curie}}"
# Its OWN namespace, never the release's. The probe pods below wear whatever
# labels the rendered policy selects, which are the real mail-adapter labels --
# dropped into an installed release's namespace they would collide with the
# actual adapter pod and the policy under test would bind two workloads at once.
NS="${2:-${CURIE_MAIL_OTEL_NS:-curie-mail-otel-egress}}"
PROBE_IMAGE="${CURIE_MAIL_OTEL_PROBE_IMAGE:-curlimages/curl:8.10.1}"
# The peers must actually LISTEN, so a blocked attempt is a timeout rather than
# an ambiguous connection-refused that a missing listener would also give.
TARGET_IMAGE="${CURIE_MAIL_OTEL_TARGET_IMAGE:-hashicorp/http-echo:1.0}"
# One port for both the listeners and the rendered `mailAdapter.otelEgress.port`,
# so a mismatch between the declared port and the reachable one is a FAILURE of
# this check rather than something it papers over.
PORT="${CURIE_MAIL_OTEL_PORT:-5678}"

ALLOWED_POD="mail-otel-target-allowed"
UNDECLARED_POD="mail-otel-target-undeclared"
PROBE_POD="mail-otel-probe"

fail() { echo "FAIL: $*" >&2; exit 1; }

# Non-blocking on the way out: the run is over, nothing waits on the pods.
# The pods are deleted; $NS itself deliberately is NOT. Deleting a namespace
# from a non-blocking trap leaves it Terminating for tens of seconds, and a
# Terminating namespace REJECTS creates -- so the next run would fail on
# namespace creation for reasons that have nothing to do with policy. That is
# the same race cleanup_and_settle exists to avoid, one level up. An empty
# namespace is inert; the pods and the policy are what must not leak.
cleanup() {
  kubectl -n "$NS" delete pod "$ALLOWED_POD" "$UNDECLARED_POD" "$PROBE_POD" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete networkpolicy -l "curie.dev/check=mail-adapter-otel-egress" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
# BLOCKING before the apply. A previous run's pod may still be terminating, and
# re-applying the same name against a deleting pod silently binds the OLD one --
# `wait --for=Ready` then passes on a pod that is on its way out and every exec
# after it fails for reasons that have nothing to do with policy.
cleanup_and_settle() {
  kubectl -n "$NS" delete pod "$ALLOWED_POD" "$UNDECLARED_POD" "$PROBE_POD" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== mail-adapter external-collector egress enforcement check (namespace=$NS chart=$CHART port=$PORT) =="

command -v helm >/dev/null 2>&1 || fail "helm is required to render the policy under test"

# Re-check existence after a failed create rather than trusting the create's exit
# status: two runs against one cluster both miss on `get`, one create wins, and
# the loser hard-fails on AlreadyExists for a namespace that by then exists and
# is perfectly usable.
kubectl get namespace "$NS" >/dev/null 2>&1 \
  || kubectl create namespace "$NS" >/dev/null 2>&1 \
  || kubectl get namespace "$NS" >/dev/null 2>&1 \
  || fail "could not create namespace $NS for the mail-adapter egress probe.

Creating it needs cluster-scoped RBAC to get and create namespaces. Grant that,
or set CURIE_MAIL_OTEL_NS to an existing namespace you may write pods and
NetworkPolicies into -- but NOT to a namespace holding a real curie release, or
the probe pod's labels would collide with the live mail adapter."

cleanup_and_settle

# Two listeners, identical in image, port, namespace and labels. The ONLY
# difference between them is whether their /32 is named in the declared list, so
# a difference in reachability is attributable to the ipBlock and to nothing
# else. Anything that made them differ otherwise -- a distinct port, a label the
# policy happens to select -- would let a broken rule still produce the expected
# blocked/reachable pair.
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $ALLOWED_POD
  labels:
    app.kubernetes.io/name: mail-otel-target
spec:
  restartPolicy: Never
  containers:
    - name: target
      image: $TARGET_IMAGE
      args: ["-listen=:$PORT", "-text=allowed"]
---
apiVersion: v1
kind: Pod
metadata:
  name: $UNDECLARED_POD
  labels:
    app.kubernetes.io/name: mail-otel-target
spec:
  restartPolicy: Never
  containers:
    - name: target
      image: $TARGET_IMAGE
      args: ["-listen=:$PORT", "-text=undeclared"]
YAML

kubectl -n "$NS" wait --for=condition=Ready \
  "pod/$ALLOWED_POD" "pod/$UNDECLARED_POD" --timeout=180s >/dev/null \
  || fail "the collector-stand-in pods did not become ready in $NS"

ALLOWED_IP="$(kubectl -n "$NS" get pod "$ALLOWED_POD" -o jsonpath='{.status.podIP}')"
UNDECLARED_IP="$(kubectl -n "$NS" get pod "$UNDECLARED_POD" -o jsonpath='{.status.podIP}')"
[ -n "$ALLOWED_IP" ] || fail "could not read the declared peer's pod IP"
[ -n "$UNDECLARED_IP" ] || fail "could not read the undeclared peer's pod IP"
# Equal IPs would make the two probes the same probe: the negative leg would be
# testing the very address the positive leg declares, so one of the two must be
# wrong and yet the pair could still read as green.
[ "$ALLOWED_IP" != "$UNDECLARED_IP" ] \
  || fail "both collector stand-ins report the same pod IP ($ALLOWED_IP); this check cannot distinguish declared from undeclared"
echo "  ok  declared peer $ALLOWED_IP, undeclared peer $UNDECLARED_IP"

# Render ONLY the mail adapter's own template from the REAL chart. Rendering the
# shipped template rather than hand-writing a policy is the point: this proves
# what the chart produces, so deleting the ipBlock rule or moving it to the wrong
# port makes this check go red.
#
# `otelCollector.deploy=false` plus an external `otelCollector.endpoint` is the
# exact configuration #2361 exists for -- the in-chart collector peer is gone and
# the ipBlock is the only thing that can carry the export.
#
# The `otelCollector.egress[0]` triple is UNRELATED to the policy under test. It
# is the RUNNER's BYO-collector peer, demanded by the pre-existing #2317 gate in
# templates/security-networkpolicy.yaml, which refuses to render at all when an
# external endpoint is configured with no runner-side peer. It is set here only
# so the render succeeds; only `mailAdapter.otelEgress.*` governs the mail
# adapter's rule.
POLICY="$(helm template curie "$CHART" -n "$NS" \
  --set mailAdapter.deploy=true \
  --set mailAdapter.agentmail.apiKey=check-mail-otel-egress-key \
  --set mailAdapter.agentmail.inboxId=probe@example.test \
  --set mailAdapter.egressSecret=check-mail-otel-egress-secret \
  --set mailAdapter.channelToken=check-mail-otel-egress-token \
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24' \
  --set otelCollector.deploy=false \
  --set "otelCollector.endpoint=https://otel.example.test:$PORT" \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set "otelCollector.egress[0].ports[0].port=$PORT" \
  --set "mailAdapter.otelEgress.httpsCidrs[0]=$ALLOWED_IP/32" \
  --set-string "mailAdapter.otelEgress.port=$PORT" \
  --show-only templates/mail-adapter.yaml \
  | python3 -c '
import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.stdin) if isinstance(d, dict)]
pols = [d for d in docs if d.get("kind") == "NetworkPolicy"]
if len(pols) != 1:
    sys.exit(f"expected exactly 1 NetworkPolicy from templates/mail-adapter.yaml, got {len(pols)}")
p = pols[0]
p.setdefault("metadata", {}).setdefault("labels", {})["curie.dev/check"] = "mail-adapter-otel-egress"
print(yaml.safe_dump(p))')" \
  || fail "rendering templates/mail-adapter.yaml failed; the check cannot proceed without the policy under test"
[ -n "$POLICY" ] || fail "templates/mail-adapter.yaml rendered no NetworkPolicy"

# ---------------------------------------------------------------------------
# STRUCTURAL VACUITY GUARD, before a single packet is sent.
# ---------------------------------------------------------------------------
# A policy that allowed everything would sail through the positive probe and
# would only be caught by the negative one -- and the negative one is also what
# catches a non-enforcing CNI, so the two failure modes would be
# indistinguishable. Asserting the shape first separates them: past this point a
# reachable undeclared peer means the CNI, not the rule.
echo "$POLICY" | grep -q -- "$ALLOWED_IP/32" \
  || fail "the rendered policy does not carry the declared peer $ALLOWED_IP/32.

mailAdapter.otelEgress.httpsCidrs was set and the template did not turn it into
an ipBlock. The probes below would then be testing a rule that does not exist."
if echo "$POLICY" | grep -q -- "$UNDECLARED_IP"; then
  fail "the rendered policy names the UNDECLARED peer $UNDECLARED_IP.

Whatever produced that, the negative leg is no longer a negative leg, so a
blocked/reachable pair below would prove nothing about the declared list."
fi
echo "  ok  rendered policy carries the declared peer and not the undeclared one"

kubectl -n "$NS" apply -f - >/dev/null <<<"$POLICY" \
  || fail "could not apply the rendered mail-adapter policy into $NS"

# The probe wears exactly the labels the rendered policy selects, DERIVED from
# the policy rather than hardcoded. Hardcoding them would silently decouple this
# check from the chart: a rename in the template's podSelector would leave the
# probe unselected, the policy would bind nothing, both peers would answer, and
# the negative leg would report a non-enforcing CNI on a perfectly good cluster.
SELECTOR="$(echo "$POLICY" | python3 -c '
import sys, yaml
p = yaml.safe_load(sys.stdin)
ml = (p.get("spec", {}).get("podSelector", {}) or {}).get("matchLabels") or {}
if not ml:
    sys.exit("the rendered policy has no podSelector.matchLabels; it would select every pod in the namespace")
print(",".join(f"{k}={v}" for k, v in ml.items()))')" \
  || fail "could not derive the probe labels from the rendered policy's podSelector"
echo "  ok  probe labels derived from the policy podSelector: $SELECTOR"

kubectl -n "$NS" run "$PROBE_POD" --image="$PROBE_IMAGE" --restart=Never \
  --labels="$SELECTOR" --command -- sleep 600 >/dev/null \
  || fail "could not create the probe pod $PROBE_POD in $NS"
kubectl -n "$NS" wait --for=condition=Ready "pod/$PROBE_POD" --timeout=180s >/dev/null \
  || fail "the probe pod did not become ready in $NS"

# `|| true` so a curl exit code never aborts the script under `set -e`: the exit
# code is the measurement here, not an error.
probe() {
  kubectl -n "$NS" exec "$PROBE_POD" -- \
    curl -sS -m 8 -o /dev/null -w '%{http_code}' "http://${1}:${PORT}/" 2>&1 || true
}

# ---------------------------------------------------------------------------
# GATE: the deny must hold. The positive below is meaningless without this.
# ---------------------------------------------------------------------------
NEG="$(probe "$UNDECLARED_IP")"
case "$NEG" in
  *"timed out"*|*"Failed to connect"*|*"Connection timed out"*|000) ;;
  *)
    fail "the UNDECLARED peer $UNDECLARED_IP:$PORT answered ($NEG).

It is byte-for-byte the same workload as the declared peer except that its /32
is not in mailAdapter.otelEgress.httpsCidrs, so reaching it means one of:

  - the CNI is not enforcing NetworkPolicy at all. kind needs
    'disableDefaultCNI: true' plus a policy-enforcing CNI (the default kindnet
    implements no NetworkPolicy controller); minikube needs '--cni=calico';
    k3s/k3d enforce by default via kube-router.
  - the rendered policy is BROADER than the declared list -- a wider ipBlock, a
    second peer, or an empty 'to' -- so the mail adapter can export to addresses
    the operator never declared.

Either way a green positive after this would prove nothing, which is the exact
false proof this check exists to prevent (#2361)."
    ;;
esac
echo "  ok  undeclared peer is blocked -- the CNI enforces this policy (non-vacuity gate)"

# ---------------------------------------------------------------------------
# Now the allow direction means something.
# ---------------------------------------------------------------------------
POS="$(probe "$ALLOWED_IP")"
[ "$POS" = "200" ] \
  || fail "the DECLARED peer $ALLOWED_IP/32 on TCP $PORT was NOT reachable (got '$POS').

The undeclared peer above was correctly blocked, so the CNI is enforcing and
this is a POLICY defect: the rendered ipBlock does not actually permit the
export it claims to. The usual cause is the port -- the rule renders on
mailAdapter.otelEgress.port, and a rule on the wrong port looks perfectly
correct in a render assertion while dropping every OTLP span the adapter emits
to an external collector (#2361)."
echo "  ok  declared peer answers HTTP 200 on TCP $PORT"

echo "== the mail adapter's external-collector egress rule enforces: undeclared peer blocked, declared peer reachable =="
