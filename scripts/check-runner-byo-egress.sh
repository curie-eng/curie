#!/usr/bin/env bash
# Assert each BYO runner-egress allow (object-store, collector-endpoint,
# api-endpoint; #2369) actually ENFORCES, not merely that it renders.
#
# #2213 and #2317 pinned those CIDRs at helm-template tier. A render
# assertion cannot prove a runner-labelled pod can reach the declared peer,
# or that an undeclared control is blocked. Every NetworkPolicy this chart
# ships is applied on any cluster; whether it is EVALUATED depends on the
# CNI. kindnet (kind's default) and minikube's default bridge implement no
# NetworkPolicy controller, so an ipBlock on the wrong port -- or one
# deleted outright -- is indistinguishable from a correct one: everything
# gets through and every "can the runner reach the BYO peer?" assertion
# passes.
#
# So, like scripts/check-mail-adapter-otel-egress.sh, this is a NON-VACUITY
# check. It asserts an UNDECLARED peer is genuinely blocked BEFORE trusting
# the declared one. If the deny does not hold, the CNI is not enforcing (or
# the policy is broader than declared) and the script FAILS -- it does not
# skip and it does not pass. A green run on a non-enforcing cluster would
# be worse than no check at all.
set -euo pipefail

CHART="${1:-${CURIE_RUNNER_BYO_CHART:-charts/curie}}"
# Its OWN namespace, never the release's. The probe pods below wear whatever
# labels the rendered BYO policy selects, which are the real runner-sandbox
# labels -- dropped into an installed release's namespace they would collide
# with live sandboxes and the policy under test would bind two workloads at
# once.
NS="${2:-${CURIE_RUNNER_BYO_NS:-curie-runner-byo-egress}}"
# netshoot carries curl, getent, and nslookup. The helm-test Claim 1d
# pod uses netshoot too; curlimages/curl has no resolver tools, so the
# mismatch hostname leg would FAIL as "DNS did not resolve" even on a
# correct policy. Tag-only (no digest): kind load of a digest-pinned
# ref stores an import name containerd cannot start.
PROBE_IMAGE="${CURIE_RUNNER_BYO_PROBE_IMAGE:-nicolaka/netshoot:v0.16}"
TARGET_IMAGE="${CURIE_RUNNER_BYO_TARGET_IMAGE:-hashicorp/http-echo:1.0}"

# Names are suffixed per key inside check_key. Re-applying the same pod
# name against a still-Terminating predecessor binds the old object
# (check-netpol-enforcement.sh cleanup_and_settle comment).
ALLOWED_POD=""
UNDECLARED_POD=""
PROBE_POD=""
MISMATCH_SVC=""
CHECK_LABEL="curie.dev/check=runner-byo-egress"
DUMMY_CIDR="192.0.2.40/32"

fail() { echo "FAIL: $*" >&2; exit 1; }

# Non-blocking on the way out: the run is over, nothing waits on the pods.
# The pods/policies are deleted; $NS itself deliberately is NOT. Deleting a
# namespace from a non-blocking trap leaves it Terminating for tens of
# seconds, and a Terminating namespace REJECTS creates.
cleanup() {
  kubectl -n "$NS" delete pod "$ALLOWED_POD" "$UNDECLARED_POD" "$PROBE_POD" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete svc "$MISMATCH_SVC" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete endpoints "$MISMATCH_SVC" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete networkpolicy -l "$CHECK_LABEL" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
# BLOCKING before the apply. A previous run's pod may still be terminating,
# and re-applying the same name against a deleting pod silently binds the
# OLD one.
cleanup_and_settle() {
  kubectl -n "$NS" delete pod "$ALLOWED_POD" "$UNDECLARED_POD" "$PROBE_POD" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "$NS" delete svc "$MISMATCH_SVC" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "$NS" delete endpoints "$MISMATCH_SVC" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "$NS" delete networkpolicy -l "$CHECK_LABEL" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== runner BYO egress enforcement check (namespace=$NS chart=$CHART) =="

command -v helm >/dev/null 2>&1 || fail "helm is required to render the policy under test"
command -v python3 >/dev/null 2>&1 || fail "python3 is required to extract the rendered NetworkPolicies"

case "$NS" in
  curie|default|kube-system|kube-public|kube-node-lease)
    fail "namespace $NS is a platform or cluster namespace; this check needs its own empty namespace (default curie-runner-byo-egress)."
    ;;
esac

kubectl get namespace "$NS" >/dev/null 2>&1 \
  || kubectl create namespace "$NS" >/dev/null 2>&1 \
  || kubectl get namespace "$NS" >/dev/null 2>&1 \
  || fail "could not create namespace $NS for the runner BYO egress probe.

Creating it needs cluster-scoped RBAC to get and create namespaces. Grant that,
or set CURIE_RUNNER_BYO_NS to an existing namespace you may write pods and
NetworkPolicies into -- but NOT to a namespace holding a real curie release, or
the probe pod's labels would collide with live runner sandboxes."

if kubectl -n "$NS" get networkpolicy -o name 2>/dev/null | grep -q 'runner-default-deny-egress'; then
  fail "namespace $NS already has a runner-default-deny-egress NetworkPolicy.

This check must not run in a live Curie release namespace: applying and then
deleting labelled policies would disturb Rail 1. Use the default
curie-runner-byo-egress namespace, or another empty namespace."
fi

TMP="$(mktemp -d)"
trap 'cleanup; rm -rf -- "$TMP"' EXIT

extract_policies() {
  local byo_suffix="$1"
  python3 -c '
import sys, yaml
ns, suffix, label = sys.argv[1], sys.argv[2], sys.argv[3]
docs = [d for d in yaml.safe_load_all(sys.stdin) if isinstance(d, dict)]
pols = [d for d in docs if d.get("kind") == "NetworkPolicy"]
keep_suffixes = (
    "-runner-default-deny-egress",
    "-runner-allow-dns",
    suffix,
)
kept = []
for p in pols:
    name = (p.get("metadata") or {}).get("name") or ""
    if any(name.endswith(s) for s in keep_suffixes):
        md = p.setdefault("metadata", {})
        md["namespace"] = ns
        md.setdefault("labels", {})[label.split("=", 1)[0]] = label.split("=", 1)[1]
        kept.append(p)
names = [(p.get("metadata") or {}).get("name") for p in kept]
want = {s: False for s in keep_suffixes}
for name in names:
    for s in keep_suffixes:
        if name and name.endswith(s):
            want[s] = True
missing = [s for s, ok in want.items() if not ok]
if missing:
    sys.exit(
        "expected default-deny, allow-dns, and "
        + suffix
        + " from templates/security-networkpolicy.yaml; missing "
        + ", ".join(missing)
        + "; kept "
        + ", ".join(n or "?" for n in names)
    )
if len(kept) != 3:
    sys.exit(
        "expected exactly 3 extracted NetworkPolicies, got "
        + str(len(kept))
        + ": "
        + ", ".join(n or "?" for n in names)
    )
print("---\n".join(yaml.safe_dump(p) for p in kept))
' "$NS" "$byo_suffix" "$CHECK_LABEL"
}

# Probe labels come from the BYO policy, never hardcoded. A rename in the
# template's podSelector would otherwise leave the probe unselected.
selector_from_byo() {
  local byo_suffix="$1"
  python3 -c '
import sys, yaml
suffix = sys.argv[1]
docs = [d for d in yaml.safe_load_all(sys.stdin) if isinstance(d, dict)]
byo = None
for d in docs:
    if d.get("kind") != "NetworkPolicy":
        continue
    name = (d.get("metadata") or {}).get("name") or ""
    if name.endswith(suffix):
        byo = d
        break
if byo is None:
    sys.exit("extracted YAML has no NetworkPolicy ending " + suffix)
ml = ((byo.get("spec") or {}).get("podSelector") or {}).get("matchLabels") or {}
if not ml:
    sys.exit("the BYO policy has no podSelector.matchLabels; it would select every pod in the namespace")
print(",".join(f"{k}={v}" for k, v in ml.items()))
' "$byo_suffix"
}

resolve_from_probe() {
  local host="$1"
  local out=""
  out="$(kubectl -n "$NS" exec "$PROBE_POD" -- getent hosts "$host" 2>/dev/null | awk '{print $1; exit}' || true)"
  if [ -n "$out" ]; then
    printf '%s\n' "$out"
    return 0
  fi
  # Busybox nslookup prints "Address 1:" / "Address:" lines; skip the DNS
  # server by taking the last IPv4 on a Name/Address pair.
  out="$(kubectl -n "$NS" exec "$PROBE_POD" -- nslookup "$host" 2>/dev/null | awk '
    $1 ~ /^[Nn]ame:?$/ { seen_name=1; next }
    seen_name && /[Aa]ddress/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/) { print $i; exit }
      }
    }
  ' || true)"
  if [ -n "$out" ]; then
    printf '%s\n' "$out"
    return 0
  fi
  return 1
}

check_key() {
  local key="$1"
  local port="$2"
  local byo_suffix="$3"
  local values="$4"

  ALLOWED_POD="runner-byo-${key}-allowed"
  UNDECLARED_POD="runner-byo-${key}-undeclared"
  PROBE_POD="runner-byo-${key}-probe"
  MISMATCH_SVC="runner-byo-${key}-mismatch"

  echo ""
  echo "== key ${key} (TCP ${port}, policy ${byo_suffix}) =="

  cleanup_and_settle

  # Two listeners, identical in image, port, namespace and labels. The ONLY
  # difference between them is whether their /32 is named in the declared
  # list. Extra labels would let a broken rule still produce the expected
  # blocked/reachable pair.
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $ALLOWED_POD
  labels:
    app.kubernetes.io/name: runner-byo-target
spec:
  restartPolicy: Never
  containers:
    - name: target
      image: $TARGET_IMAGE
      args: ["-listen=:$port", "-text=allowed"]
---
apiVersion: v1
kind: Pod
metadata:
  name: $UNDECLARED_POD
  labels:
    app.kubernetes.io/name: runner-byo-target
spec:
  restartPolicy: Never
  containers:
    - name: target
      image: $TARGET_IMAGE
      args: ["-listen=:$port", "-text=undeclared"]
YAML

  kubectl -n "$NS" wait --for=condition=Ready \
    "pod/$ALLOWED_POD" "pod/$UNDECLARED_POD" --timeout=180s >/dev/null \
    || fail "the ${key} stand-in pods did not become ready in $NS"

  local allowed_ip undeclared_ip
  allowed_ip="$(kubectl -n "$NS" get pod "$ALLOWED_POD" -o jsonpath='{.status.podIP}')"
  undeclared_ip="$(kubectl -n "$NS" get pod "$UNDECLARED_POD" -o jsonpath='{.status.podIP}')"
  [ -n "$allowed_ip" ] || fail "could not read the declared ${key} peer's pod IP"
  [ -n "$undeclared_ip" ] || fail "could not read the undeclared ${key} peer's pod IP"
  [ "$allowed_ip" != "$undeclared_ip" ] \
    || fail "both ${key} stand-ins report the same pod IP ($allowed_ip); this check cannot distinguish declared from undeclared"
  case "$allowed_ip$undeclared_ip" in
    *:*) fail "pod IPs must be IPv4 for this check (got declared=$allowed_ip undeclared=$undeclared_ip); a ClusterIP or IPv6 address in the CIDR is the #1153 false-pass" ;;
  esac
  echo "  ok  declared peer $allowed_ip, undeclared peer $undeclared_ip"

  local values_file="$TMP/${key}.yaml"
  VALUES_TEMPLATE="$values" ALLOWED_IP="$allowed_ip" python3 -c '
import os, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(os.environ["VALUES_TEMPLATE"].replace("ALLOWED_IP", os.environ["ALLOWED_IP"]))
' "$values_file"

  local policy
  policy="$(helm template curie "$CHART" -n "$NS" \
    --values "$values_file" \
    --show-only templates/security-networkpolicy.yaml \
    | extract_policies "$byo_suffix")" \
    || fail "rendering templates/security-networkpolicy.yaml for ${key} failed; the check cannot proceed without the policy under test"
  [ -n "$policy" ] || fail "templates/security-networkpolicy.yaml rendered no extractable NetworkPolicies for ${key}"

  # ---------------------------------------------------------------------------
  # STRUCTURAL VACUITY GUARD, before a single packet is sent.
  # ---------------------------------------------------------------------------
  echo "$policy" | grep -q -- "$allowed_ip/32" \
    || fail "the rendered ${key} policy does not carry the declared peer $allowed_ip/32.

The BYO egress list was set and the template did not turn it into an ipBlock.
The probes below would then be testing a rule that does not exist."
  if echo "$policy" | grep -q -- "$undeclared_ip"; then
    fail "the rendered ${key} policy names the UNDECLARED peer $undeclared_ip.

Whatever produced that, the negative leg is no longer a negative leg, so a
blocked/reachable pair below would prove nothing about the declared list."
  fi
  echo "  ok  rendered policy carries the declared peer and not the undeclared one"

  kubectl -n "$NS" apply -f - >/dev/null <<<"$policy" \
    || fail "could not apply the rendered ${key} policies into $NS"

  local selector
  selector="$(echo "$policy" | selector_from_byo "$byo_suffix")" \
    || fail "could not derive the probe labels from the rendered ${key} policy's podSelector"
  echo "  ok  probe labels derived from the policy podSelector: $selector"

  kubectl -n "$NS" run "$PROBE_POD" --image="$PROBE_IMAGE" --restart=Never \
    --image-pull-policy=IfNotPresent \
    --labels="$selector" --command -- sleep 600 >/dev/null \
    || fail "could not create the probe pod $PROBE_POD in $NS"
  kubectl -n "$NS" wait --for=condition=Ready "pod/$PROBE_POD" --timeout=180s >/dev/null \
    || fail "the probe pod did not become ready in $NS"

  # `|| true` so a curl exit code never aborts the script under `set -e`: the
  # exit code is the measurement here, not an error.
  probe() {
    kubectl -n "$NS" exec "$PROBE_POD" -- \
      curl -sS -m 8 -o /dev/null -w '%{http_code}' "http://${1}:${port}/" 2>&1 || true
  }

  # ---------------------------------------------------------------------------
  # GATE: the deny must hold. The positive below is meaningless without this.
  # ---------------------------------------------------------------------------
  local neg
  neg="$(probe "$undeclared_ip")"
  case "$neg" in
    *"timed out"*|*"Failed to connect"*|*"Connection timed out"*|000) ;;
    *)
      fail "the UNDECLARED peer $undeclared_ip:$port answered ($neg).

It is byte-for-byte the same workload as the declared peer except that its /32
is not in the rendered runner BYO allow, so reaching it means one of:

  - the CNI is not enforcing NetworkPolicy at all. kind needs
    'disableDefaultCNI: true' plus a policy-enforcing CNI (the default kindnet
    implements no NetworkPolicy controller); minikube needs '--cni=calico';
    k3s/k3d enforce by default via kube-router.
  - the rendered policy is BROADER than the declared list -- a wider ipBlock, a
    second peer, or an empty 'to' -- so the runner can export to addresses
    the operator never declared.

Either way a green positive after this would prove nothing, which is the exact
false proof this check exists to prevent (runner BYO / #2369)."
      ;;
  esac
  echo "  ok  undeclared peer is blocked -- the CNI enforces this policy (non-vacuity gate)"

  local pos
  pos="$(probe "$allowed_ip")"
  [ "$pos" = "200" ] \
    || fail "the DECLARED peer $allowed_ip/32 on TCP $port was NOT reachable (got '$pos').

The undeclared peer above was correctly blocked, so the CNI is enforcing and
this is a POLICY defect: the rendered ipBlock does not actually permit the
export it claims to. The usual cause is the port -- a rule on the wrong port
looks perfectly correct in a render assertion while dropping every dial the
runner makes to this BYO peer (#2369)."
  echo "  ok  declared peer answers HTTP 200 on TCP $port"

  # DNS/peer mismatch: a headless Service whose name resolves to the
  # UNDECLARED pod IP while the policy CIDR names the ALLOWED pod. Probing
  # the hostname must be blocked. Never put a Service ClusterIP in the CIDR
  # (#1153); this Service is headless and the CIDR is a pod IP.
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Service
metadata:
  name: $MISMATCH_SVC
  labels:
    curie.dev/check: runner-byo-egress
spec:
  clusterIP: None
  ports:
    - port: $port
      targetPort: $port
---
apiVersion: v1
kind: Endpoints
metadata:
  name: $MISMATCH_SVC
  labels:
    curie.dev/check: runner-byo-egress
subsets:
  - addresses:
      - ip: $undeclared_ip
    ports:
      - port: $port
YAML

  local mismatch_host="${MISMATCH_SVC}.${NS}.svc.cluster.local"
  local resolved=""
  resolved="$(resolve_from_probe "$mismatch_host" || true)"
  if [ -z "$resolved" ]; then
    fail "DNS did not resolve ${mismatch_host} from the probe pod.

Cannot prove a DNS/CIDR mismatch if the name does not resolve. Check that
runner-allow-dns was extracted and applied (Rail 1 allow-dns is required so
a resolve failure is not mistaken for a mismatch pass)."
  fi
  if [ "$resolved" != "$undeclared_ip" ]; then
    fail "DNS for ${mismatch_host} resolved to ${resolved}, not the undeclared pod IP ${undeclared_ip}.

The mismatch leg requires the hostname to name the UNDECLARED peer while the
policy CIDR names the declared one."
  fi
  echo "  ok  mismatch hostname resolves to undeclared peer $undeclared_ip"

  local mismatch
  mismatch="$(probe "$mismatch_host")"
  case "$mismatch" in
    *"timed out"*|*"Failed to connect"*|*"Connection timed out"*|000) ;;
    *)
      fail "the mismatch hostname ${mismatch_host}:${port} answered ($mismatch).

DNS resolved it to the UNDECLARED pod ${undeclared_ip} while the policy CIDR
names ${allowed_ip}/32, so a reachable result is a DNS/CIDR mismatch (rotating
LB addresses, or a hostname the ipBlock does not cover). That is FAIL, never
PASS -- the same silent drop a SaaS collector behind rotating IPs would see
(#2369)."
      ;;
  esac
  echo "  ok  mismatch hostname is blocked (DNS/CIDR split does not pass as success)"

  cleanup_and_settle
}

RUSTFS_VALUES="$(cat <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 9000
  egress:
    - cidr: ALLOWED_IP/32
      ports: [{ protocol: TCP, port: 9000 }]
EOF
)"

STS_VALUES="$(cat <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  auth:
    accessKey: ""
  egress:
    - cidr: ${DUMMY_CIDR}
      ports: [{ protocol: TCP, port: 8443 }]
  stsEgress:
    - cidr: ALLOWED_IP/32
      ports: [{ protocol: TCP, port: 8443 }]
api:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-api
worker:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-worker
agentSandbox:
  runner:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-runner
EOF
)"

OTEL_VALUES="$(cat <<EOF
otelCollector:
  deploy: false
  endpoint: https://otlp.example.net:4318
  egress:
    - cidr: ALLOWED_IP/32
      ports: [{ protocol: TCP, port: 4318 }]
EOF
)"

API_VALUES="$(cat <<EOF
api:
  deploy: false
  egress:
    - cidr: ALLOWED_IP/32
      ports: [{ protocol: TCP, port: 8000 }]
dispatcher:
  apiBaseUrl: https://api.example.net
ui:
  apiBaseUrl: https://api.example.net
EOF
)"

check_key rustfs 9000 "-runner-allow-object-store" "$RUSTFS_VALUES"
# STS is 443 in production; http-echo cannot bind that privileged port as
# the image's non-root user, so the live listener and ipBlock use 8443.
# A rule on the wrong port still fails the positive HTTP 200 check.
check_key sts 8443 "-runner-allow-object-store" "$STS_VALUES"
check_key otel 4318 "-runner-allow-collector-endpoint" "$OTEL_VALUES"
check_key api 8000 "-runner-allow-api-endpoint" "$API_VALUES"

echo "== runner BYO egress enforces: undeclared peer blocked, declared peer reachable, mismatch hostname blocked (rustfs/sts/otel/api) =="
