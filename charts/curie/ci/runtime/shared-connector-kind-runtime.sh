#!/usr/bin/env bash
#
# Live proof for shared connector applications on a real, policy-enforcing
# Kubernetes cluster (ADR 0239, cp5: AC3, AC4, AC5 and AC6 on a real API
# server).
#
#   RUN_ID=<short id> bash charts/curie/ci/runtime/shared-connector-kind-runtime.sh
#
# CI runs it through tools/runtime-assertion-gate/run.sh in the chart leg of
# e2e-cluster-chart-regressions. It never uses that job's shared cluster: it
# creates, and deletes, a kind cluster of its own.
#
# Requirements: kind v0.24.0, kubectl, helm, docker, curl, sha256sum, uv and
# python3 with PyYAML. Docker is whatever endpoint the caller's environment
# already selects; this script never sets DOCKER_HOST or DOCKER_CONTEXT and
# never bind-mounts a host path (files reach the cluster through kubectl, the
# backing stores use container-local tmpfs and anonymous volumes).
#
# What it owns, all named from RUN_ID:
#   kind cluster curie-shared-<RUN_ID> (kindest/node:v1.31.0, disableDefaultCNI,
#     podSubnet 192.168.0.0/16) with Calico v3.28.2 from the same URL and
#     sha256 as .github/workflows/ci.yaml;
#   Docker containers sck-<RUN_ID>-{postgres,valkey,rustfs} (published on
#     127.0.0.1 only) for the host-side API the driver starts;
#   a private kubeconfig and work directory.
# The EXIT trap is registered before anything is created. It deletes the
# cluster and the containers (with their anonymous volumes), then verifies
# each is gone; a leak, or a lookup that cannot prove absence, fails the run.
#
# Steps, in order:
#   1. Create the cluster, install Calico, wait for every node.
#   2. CNI enforcement control: a namespace peer reaches a listener; a
#      deny-all ingress policy on that listener must then refuse it (a
#      non-enforcing CNI fails the run here, before any reach is trusted);
#      deleting the policy restores reach, so the refusal was the policy.
#   3. Listener pods in the release namespace: an in-cluster Postgres carrying
#      the chart's postgres selector labels plus the database target label;
#      db-target-alt-port (target label only, listening on 5432 and 15432);
#      db-unconfigured (other labels, 5432); a Valkey stand-in carrying the
#      chart's valkey selector labels (6379); a provider stand-in (8443, 9443);
#      a namespace peer; a generic connector pod (part-of the release,
#      component mcp-connector).
#   4. helm template with sharedConnectors.enabled, agentSandbox.deploy,
#      worker.connectorReconciler.enabled, security.dataTierNetworkPolicy
#      .enabled and the nogvisor overlay; applicationEgress for application 1
#      names the provider pod on 8443 only. Applied: the worker
#      ServiceAccount, Role and RoleBinding; the worker-secrets
#      ValidatingAdmissionPolicy and its binding; the runner rail default-deny
#      egress; every data-tier policy; every object of
#      shared-connector-networkpolicy.yaml.
#   5. Admission as the worker (impersonation, --dry-run=server). Admits a
#      key Secret, a DSN Secret and a labelled Opaque -tokens Secret. Denies,
#      each with the policy's own message: wrong name (other UUID, suffix,
#      DSN prefix on a key), wrong or missing role label, missing application
#      label, wrong or missing component, uppercase UUID, extra data key,
#      wrong data key (both shapes), empty data, type
#      kubernetes.io/service-account-token, type "opaque", a key Secret
#      carrying sandbox-claim, a -tokens Secret carrying either shared label,
#      and the pre-existing unlabelled and service-account-token -tokens
#      cases. Negative control: the cluster admin creating two denied shapes
#      is admitted, so each denial is the worker-scoped policy.
#      Not denial cases: a Secret sent without type is defaulted to Opaque by
#      the API server before admission (checked: admitted as Opaque), and
#      stringData is folded into data before admission, so neither is
#      observable at this boundary; both are pinned at render instead.
#   6. The driver (shared-connector-kind-driver.py) runs the production API
#      (uvicorn, migrated with alembic, on the Postgres container) and the
#      production connector reconcile loop built by run._build_connector_loop
#      as the worker ServiceAccount through a kubeconfig holding that
#      account's token, so RBAC and the admission policy are the real ones. It
#      proves Secret creation and adoption, byte-identical frozen output,
#      member reach apply and prune, survivor preservation, and every network
#      probe listed in its docstring.
#
# Stated plainly: the shared application Deployment runs pinned busybox stand
# in images, and the probe Pods carry the exact labels the frozen render puts
# on the application Pods and member runners. This is a network-reach and
# ownership proof, not a proxy proof. The real proxy and resolver are proven
# by AC2's suite, readiness reporting by AC3's suite against recorded status
# shapes, and real-proxy readiness in-cluster is checkpoint cp8.
#
# Optional: SHARED_KIND_RECORD_STATUS_FIXTURES=<dir> makes the driver record
# the three Deployment status shapes (available, progressing, deadline
# exceeded) from this cluster into <dir> for AC3's suite.
#
# Output is one line per assertion; any failure exits nonzero.
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="$REPO_ROOT/charts/curie"
DRIVER="$REPO_ROOT/charts/curie/ci/runtime/shared-connector-kind-driver.py"

RAW_RUN_ID="${RUN_ID:-$(date +%s)}"
RUN_ID="$(tr -cd 'a-z0-9' <<<"${RAW_RUN_ID,,}")"
# Keep at most the last 16 characters. ${RUN_ID: -16} alone empties any
# shorter id, so trim only when it is longer.
if (( ${#RUN_ID} > 16 )); then
  RUN_ID="${RUN_ID: -16}"
fi
[[ -n "$RUN_ID" ]] || { echo "FAIL RUN_ID has no usable characters" >&2; exit 1; }

KIND_VERSION="v0.24.0"
NODE_IMAGE="kindest/node:v1.31.0"
CALICO_URL="https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml"
CALICO_SHA256="be59408bf990e96276f631d2f9285c2a0f9802194c0ad1cecdb6d9c52623a1c8"
BUSYBOX="docker.io/library/busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
POSTGRES_IMAGE="postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
VALKEY_IMAGE="valkey/valkey:8.1.10-alpine"
RUSTFS_IMAGE="rustfs/rustfs:1.0.0-beta.12"

CLUSTER="curie-shared-${RUN_ID}"
CTX="kind-${CLUSTER}"
NS="sck-${RUN_ID}"
REL="sck${RUN_ID}"
FULL="${REL}-curie"
APP_NAME="curie"
WORKER_SA="${FULL}-worker"
SECRETS_POLICY="${FULL}-worker-secrets"
DB_TARGET_LABEL="curietech.ai/kind-db-target"
GRANT_SECRET="${FULL}-shared-grants"
CONTAINER_PREFIX="sck-${RUN_ID}"

WORK="$(mktemp -d)"
KCFG="$WORK/kubeconfig"
CREATED_CLUSTER=0
OWNED_CONTAINERS=()
OWNED_VOLUMES=()

# --- teardown: registered before anything is created --------------------------
container_absent() {
  local out
  out="$(docker ps -a -q --filter "name=^/${1}\$" 2>&1)" || return 1
  [[ -z "$out" ]]
}

cleanup() {
  set +e
  local leaked=()
  if [[ -s "$WORK/api.pid" ]]; then
    kill "$(cat "$WORK/api.pid")" >/dev/null 2>&1
  fi
  local name volume
  for name in "${OWNED_CONTAINERS[@]}"; do
    while read -r volume; do
      [[ -n "$volume" ]] && OWNED_VOLUMES+=("$volume")
    done < <(docker inspect -f '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{"\n"}}{{end}}{{end}}' "$name" 2>/dev/null)
    docker rm -f -v "$name" >/dev/null 2>&1
  done
  if (( CREATED_CLUSTER )); then
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KCFG" >/dev/null 2>&1
    local clusters
    if clusters="$(kind get clusters 2>/dev/null)"; then
      grep -qx "$CLUSTER" <<<"$clusters" && leaked+=("kind cluster $CLUSTER")
    else
      leaked+=("kind cluster $CLUSTER (could not list clusters)")
    fi
    local nodes
    if nodes="$(docker ps -a -q --filter "label=io.x-k8s.kind.cluster=${CLUSTER}" 2>&1)"; then
      [[ -n "$nodes" ]] && leaked+=("kind node containers of $CLUSTER")
    else
      leaked+=("kind node containers of $CLUSTER (could not list)")
    fi
  fi
  for name in "${OWNED_CONTAINERS[@]}"; do
    container_absent "$name" || leaked+=("container $name")
  done
  for volume in "${OWNED_VOLUMES[@]}"; do
    docker volume inspect "$volume" >/dev/null 2>&1 && leaked+=("volume $volume")
  done
  rm -rf -- "$WORK"
  if (( ${#leaked[@]} )); then
    echo "FAIL cleanup left resources behind (or could not prove them gone): ${leaked[*]}" >&2
    return 1
  fi
  echo "cleanup: cluster, containers and volumes of run $RUN_ID verified gone"
  return 0
}
on_exit() {
  local rc=$?
  trap - EXIT
  cleanup || rc=1
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

pass=0
ok() { echo "PASS $1"; pass=$((pass + 1)); }
fail() { echo "FAIL $1" >&2; exit 1; }

k() { kubectl --kubeconfig "$KCFG" --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
as_worker() {
  kubectl --kubeconfig "$KCFG" --context "$CTX" \
    --as "system:serviceaccount:${NS}:${WORKER_SA}" \
    --as-group system:serviceaccounts --as-group "system:serviceaccounts:${NS}" "$@"
}

# --- preflight ---------------------------------------------------------------
for tool in kind kubectl helm docker curl sha256sum uv python3; do
  command -v "$tool" >/dev/null || fail "required executable is unavailable: $tool"
done
python3 -c 'import yaml' 2>/dev/null || fail "python3 with PyYAML is required"
kind version | grep -qF "kind ${KIND_VERSION} " \
  || fail "kind ${KIND_VERSION} is required (ci.yaml pins it); found: $(kind version)"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" \
  && fail "kind cluster $CLUSTER already exists; this script never adopts a cluster"
for name in postgres valkey rustfs; do
  container_absent "${CONTAINER_PREFIX}-${name}" \
    || fail "container ${CONTAINER_PREFIX}-${name} already exists (or docker is unreachable)"
done

# --- 1. cluster and Calico ----------------------------------------------------
cat >"$WORK/kind-config.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  disableDefaultCNI: true
  podSubnet: "192.168.0.0/16"
nodes:
  - role: control-plane
EOF
CREATED_CLUSTER=1
kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind-config.yaml" \
  --kubeconfig "$KCFG" >"$WORK/kind-create.log" 2>&1 \
  || { tail -20 "$WORK/kind-create.log" >&2; fail "kind create cluster $CLUSTER"; }
ok "created kind cluster $CLUSTER ($NODE_IMAGE, no default CNI)"

curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 -o "$WORK/calico.yaml" "$CALICO_URL" \
  || fail "download the Calico manifest"
echo "${CALICO_SHA256}  $WORK/calico.yaml" | sha256sum --check --strict >/dev/null \
  || fail "the Calico manifest does not match the sha256 pinned in ci.yaml"
k apply -f "$WORK/calico.yaml" >/dev/null || fail "apply Calico"
k -n kube-system rollout status daemonset/calico-node --timeout=300s >/dev/null \
  || fail "calico-node did not roll out"
k wait --for=condition=Ready node --all --timeout=300s >/dev/null || fail "nodes did not become Ready"
ok "Calico v3.28.2 installed (sha256 verified) and every node Ready"

KUBE_VERSION="$(k version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"].lstrip("v").split("+")[0])')"
[[ -n "$KUBE_VERSION" ]] || fail "could not read the server version"

# --- backing stores for the host-side API ---------------------------------------
gen() { python3 -c 'import secrets; print(secrets.token_hex(24))'; }
PG_PASSWORD="$(gen)"
VALKEY_PASSWORD="$(gen)"
S3_ACCESS="sck$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
S3_SECRET="$(gen)"

start_container() {
  local name="$1" envfile="$2"
  shift 2
  OWNED_CONTAINERS+=("$name")
  docker run -d --name "$name" --env-file "$envfile" "$@" >"$WORK/$name.id" 2>"$WORK/$name.err" \
    || { tail -5 "$WORK/$name.err" >&2; fail "start container $name"; }
}
host_port() {
  local mapping
  mapping="$(docker port "$1" "$2/tcp" | grep -E '^127\.0\.0\.1:' | head -1)"
  [[ -n "$mapping" ]] || fail "container $1 publishes no 127.0.0.1 port for $2"
  printf '%s\n' "${mapping##*:}"
}
wait_tcp() {
  python3 - "$1" "$2" <<'PY' || fail "nothing listens on 127.0.0.1:$1 for $2 within 120s"
import socket, sys, time
port, what = int(sys.argv[1]), sys.argv[2]
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            sys.exit(0)
    except OSError:
        time.sleep(1)
sys.exit(1)
PY
}

printf 'POSTGRES_PASSWORD=%s\n' "$PG_PASSWORD" >"$WORK/pg.env"
start_container "${CONTAINER_PREFIX}-postgres" "$WORK/pg.env" -p 127.0.0.1::5432 "$POSTGRES_IMAGE"
printf 'VALKEY_PASSWORD=%s\n' "$VALKEY_PASSWORD" >"$WORK/valkey.env"
start_container "${CONTAINER_PREFIX}-valkey" "$WORK/valkey.env" -p 127.0.0.1::6379 "$VALKEY_IMAGE" \
  sh -c 'exec valkey-server --requirepass "$VALKEY_PASSWORD"'
printf 'RUSTFS_VOLUMES=/data\nRUSTFS_ADDRESS=0.0.0.0:9000\nRUSTFS_ACCESS_KEY=%s\nRUSTFS_SECRET_KEY=%s\nRUSTFS_OBS_LOG_DIRECTORY=/logs\n' \
  "$S3_ACCESS" "$S3_SECRET" >"$WORK/rustfs.env"
start_container "${CONTAINER_PREFIX}-rustfs" "$WORK/rustfs.env" -p 127.0.0.1::9000 \
  --tmpfs /data:rw,uid=10001,gid=10001 --tmpfs /logs:rw,uid=10001,gid=10001 "$RUSTFS_IMAGE"

PG_PORT="$(host_port "${CONTAINER_PREFIX}-postgres" 5432)"
VALKEY_PORT="$(host_port "${CONTAINER_PREFIX}-valkey" 6379)"
S3_PORT="$(host_port "${CONTAINER_PREFIX}-rustfs" 9000)"
wait_tcp "$PG_PORT" postgres
wait_tcp "$VALKEY_PORT" valkey
wait_tcp "$S3_PORT" rustfs
for _ in $(seq 1 60); do
  docker exec "${CONTAINER_PREFIX}-postgres" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "${CONTAINER_PREFIX}-postgres" pg_isready -U postgres >/dev/null 2>&1 \
  || fail "the Postgres container never became ready"
ok "Postgres 16, Valkey and RustFS containers listen on 127.0.0.1 (no bind mounts)"

# --- pods ----------------------------------------------------------------------
# pod_json NAME LABELS_JSON PORTS(space separated, first named NAMED) [IMAGE]
# A busybox pod that runs one httpd per port and then sleeps; ready once the
# first port accepts. With no ports it only sleeps (a client).
pod_json() {
  python3 - "$NS" "$1" "$2" "$3" "${4:-$BUSYBOX}" <<'PY'
import json, sys
ns, name, labels, ports, image = sys.argv[1:]
ports = [int(p) for p in ports.split()]
script = "".join(f"httpd -p {p} -h /tmp; " for p in ports) + "exec sleep 36000"
container = {
    "name": "probe",
    "image": image,
    "imagePullPolicy": "IfNotPresent",
    "command": ["sh", "-c", script],
    "ports": [{"containerPort": p, "protocol": "TCP"} for p in ports],
}
if ports:
    container["readinessProbe"] = {"tcpSocket": {"port": ports[0]}, "periodSeconds": 2}
print(json.dumps({
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {"name": name, "namespace": ns, "labels": json.loads(labels)},
    "spec": {"terminationGracePeriodSeconds": 0, "containers": [container]},
}))
PY
}
make_pod() { pod_json "$@" | kn apply -f - >/dev/null || fail "create pod $1"; }
pod_ip() {
  local ip
  ip="$(kn get pod "$1" -o jsonpath='{.status.podIP}')"
  [[ -n "$ip" ]] || fail "pod $1 has no IP"
  printf '%s\n' "$ip"
}
wait_ready() {
  kn wait --for=condition=Ready "${@/#/pod/}" --timeout=300s >/dev/null \
    || fail "pods did not become Ready: $*"
}
# Connects from POD to IP:PORT once; 0 only when the TCP handshake completes.
probe() { kn exec "$1" -- nc -z -w 3 "$2" "$3" >/dev/null 2>&1; }
expect_reach() {
  local label="$1" pod="$2" ip="$3" port="$4"
  for _ in $(seq 1 20); do
    probe "$pod" "$ip" "$port" && { ok "$label"; return 0; }
    sleep 2
  done
  fail "$label: $pod never reached $ip:$port"
}
# Three attempts, all refused: one timeout is not proof, a pattern is.
expect_refused() {
  local label="$1" pod="$2" ip="$3" port="$4"
  for _ in 1 2 3; do
    probe "$pod" "$ip" "$port" && fail "$label: $pod reached $ip:$port"
  done
  ok "$label"
}

k create namespace "$NS" >/dev/null || fail "create namespace $NS"

# --- 2. CNI enforcement control ---------------------------------------------------
make_pod cni-target '{"app":"sck-cni-target"}' "8080"
make_pod cni-client '{"app":"sck-cni-client"}' ""
wait_ready cni-target cni-client
CNI_IP="$(pod_ip cni-target)"
expect_reach "CNI control: a peer reaches the listener before any policy" cni-client "$CNI_IP" 8080
kn apply -f - >/dev/null <<EOF || fail "apply the CNI control deny policy"
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sck-cni-deny
spec:
  podSelector:
    matchLabels:
      app: sck-cni-target
  policyTypes: [Ingress]
EOF
enforced=0
for _ in $(seq 1 30); do
  if ! probe cni-client "$CNI_IP" 8080 && ! probe cni-client "$CNI_IP" 8080; then
    enforced=1
    break
  fi
  sleep 2
done
(( enforced )) || fail "CNI control: a deny-all ingress policy did not block the peer within 60s; the CNI does not enforce NetworkPolicy, so no reach result below could be trusted"
ok "CNI control: a deny-all ingress policy blocks the peer (the CNI enforces)"
kn delete networkpolicy sck-cni-deny >/dev/null || fail "delete the CNI control policy"
expect_reach "CNI control: deleting the policy restores reach, so the block was the policy" \
  cni-client "$CNI_IP" 8080
kn delete pod cni-target cni-client --wait=false >/dev/null || fail "delete the CNI control pods"

# --- 3. listeners ---------------------------------------------------------------------
# The real in-cluster Postgres: the chart's postgres selector labels plus the
# database target label the application is registered with.
POSTGRES_LABELS="{\"app.kubernetes.io/name\":\"${APP_NAME}\",\"app.kubernetes.io/instance\":\"${REL}\",\"app.kubernetes.io/component\":\"postgres\",\"${DB_TARGET_LABEL}\":\"${RUN_ID}\"}"
pod_json sck-postgres "$POSTGRES_LABELS" "" "$POSTGRES_IMAGE" | python3 -c '
import json, sys
pod = json.load(sys.stdin)
c = pod["spec"]["containers"][0]
c.pop("command")
c["env"] = [{"name": "POSTGRES_HOST_AUTH_METHOD", "value": "trust"}]
c["ports"] = [{"containerPort": 5432, "protocol": "TCP"}]
c["readinessProbe"] = {"tcpSocket": {"port": 5432}, "periodSeconds": 2}
print(json.dumps(pod))
' | kn apply -f - >/dev/null || fail "create the in-cluster Postgres"
make_pod sck-db-target-alt "{\"${DB_TARGET_LABEL}\":\"${RUN_ID}\",\"app\":\"sck-db-target-alt\"}" "5432 15432"
make_pod sck-db-unconfigured '{"app":"sck-db-unconfigured"}' "5432"
make_pod sck-valkey "{\"app.kubernetes.io/name\":\"${APP_NAME}\",\"app.kubernetes.io/instance\":\"${REL}\",\"app.kubernetes.io/component\":\"valkey\"}" "6379"
make_pod sck-provider '{"app":"sck-provider"}' "8443 9443"
make_pod sck-peer '{"app":"sck-peer"}' ""
make_pod sck-generic-connector "{\"app.kubernetes.io/part-of\":\"${REL}\",\"app.kubernetes.io/component\":\"mcp-connector\",\"app.kubernetes.io/name\":\"${REL}-acme-mcp-generic\"}" ""
wait_ready sck-postgres sck-db-target-alt sck-db-unconfigured sck-valkey sck-provider sck-peer sck-generic-connector
PROVIDER_IP="$(pod_ip sck-provider)"
ok "listener, peer and generic connector pods Ready in $NS"

# --- 4. render and apply the objects under test ------------------------------------
read -r APP1 APP2 APP3 < <(python3 -c 'import uuid; print(*(uuid.uuid4() for _ in range(3)))')
cat >"$WORK/values.yaml" <<EOF
agentSandbox:
  deploy: true
worker:
  connectorReconciler:
    enabled: true
security:
  dataTierNetworkPolicy:
    enabled: true
sharedConnectors:
  enabled: true
  applicationEgress:
    ${APP1}:
      - cidr: ${PROVIDER_IP}/32
        ports:
          - { protocol: TCP, port: 8443 }
EOF
mkdir -p "$WORK/render" "$WORK/fixtures"
helm template "$REL" "$CHART" -n "$NS" \
  -f "$CHART/values-e2e-nogvisor.yaml" -f "$WORK/values.yaml" \
  --kube-version "$KUBE_VERSION" \
  --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy \
  --api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicyBinding \
  --output-dir "$WORK/render" >/dev/null 2>"$WORK/render.err" \
  || { cat "$WORK/render.err" >&2; fail "helm template with sharedConnectors.enabled"; }

python3 - "$WORK" "$NS" "$FULL" "$SECRETS_POLICY" <<'PY' || fail "could not select the rendered objects under test"
import pathlib, sys
import yaml

work, ns, full, secrets_policy = sys.argv[1:]
render = pathlib.Path(work) / "render"


def docs(basename):
    paths = list(render.rglob(basename))
    if len(paths) != 1:
        sys.exit(f"expected one rendered {basename}, found {paths}")
    return [d for d in yaml.safe_load_all(paths[0].read_text()) if isinstance(d, dict)]


apply = []
for doc in docs("worker.yaml"):
    if doc.get("kind") in ("ServiceAccount", "Role", "RoleBinding") and doc["metadata"]["name"] == f"{full}-worker":
        apply.append(doc)
if len(apply) != 3:
    sys.exit("templates/worker.yaml did not render the worker ServiceAccount, Role and RoleBinding")
admission = [
    d for d in docs("runner-resources-admission.yaml")
    if d["metadata"]["name"] == secrets_policy
    and d.get("kind") in ("ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding")
]
if len(admission) != 2:
    sys.exit(f"expected the {secrets_policy} policy and binding")
apply += admission
policy = admission[0] if admission[0]["kind"] == "ValidatingAdmissionPolicy" else admission[1]
messages = [v.get("message") or "" for v in policy["spec"].get("validations") or []]
(pathlib.Path(work) / "secrets-messages.txt").write_text("\n".join(messages) + "\n")
deny = [d for d in docs("security-networkpolicy.yaml") if d["metadata"]["name"] == f"{full}-runner-default-deny-egress"]
if len(deny) != 1:
    sys.exit("security-networkpolicy.yaml rendered no runner default-deny egress")
apply += deny
tier = [d for d in docs("security-datatier-networkpolicy.yaml") if d.get("kind") == "NetworkPolicy"]
if not tier:
    sys.exit("security-datatier-networkpolicy.yaml rendered no policy")
apply += tier
shared = docs("shared-connector-networkpolicy.yaml")
if not shared:
    sys.exit("shared-connector-networkpolicy.yaml rendered nothing with sharedConnectors.enabled")
apply += shared
for doc in apply:
    if doc["kind"] not in ("ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"):
        doc["metadata"]["namespace"] = ns
(pathlib.Path(work) / "apply.yaml").write_text("---\n".join(yaml.safe_dump(d) for d in apply))
print(len(apply))
PY
k apply -f "$WORK/apply.yaml" >/dev/null || fail "apply the rendered objects under test"
ok "applied the worker RBAC, ${SECRETS_POLICY}, the runner default-deny, the data tier and the shared root policies"

# --- 5. admission as the worker ----------------------------------------------------
python3 - "$WORK/fixtures" "$NS" "$REL" <<'PY' || fail "could not build the admission fixtures"
import base64, json, pathlib, secrets, sys, uuid

out, ns, rel = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
app, other = str(uuid.uuid4()), str(uuid.uuid4())


def b64(raw):
    return base64.b64encode(raw).decode("ascii")


# Key material is generated per run, never a literal.
KEY = {"private-keys": b64(secrets.token_bytes(32)) + "\n"}
DSN = {"sealed-dsn": b64(secrets.token_bytes(96))}
CLAIM = {"curietech.ai/sandbox-claim": "sck-claim"}


def labels(role, application=app, drop=(), **extra):
    base = {
        "curietech.ai/shared-application": application,
        "curietech.ai/shared-secret": role,
        "app.kubernetes.io/component": "shared-connector-application",
        "app.kubernetes.io/part-of": rel,
    }
    base.update(extra)
    return {k: v for k, v in base.items() if k not in drop}


def secret(name, lab, data, kind="Opaque", annotations=None):
    doc = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": ns, "labels": lab},
        "data": {k: b64(v.encode()) for k, v in data.items()},
    }
    if kind is not None:
        doc["type"] = kind
    if annotations:
        doc["metadata"]["annotations"] = annotations
    return doc


SA = {"kubernetes.io/service-account.name": "default"}
key_name, dsn_name = f"shared-key-{app}", f"shared-dsn-{app}"
cases = {
    "allow-key": secret(key_name, labels("encryption-key"), KEY),
    "allow-dsn": secret(dsn_name, labels("database-dsn"), DSN),
    "allow-tokens": secret("sck-claim-tokens", CLAIM, {"CURIE_HISTORY_TOKEN": "placeholder"}),
    "typeless-key": secret(key_name, labels("encryption-key"), KEY, kind=None),
    "deny-key-other-uuid": secret(f"shared-key-{other}", labels("encryption-key"), KEY),
    "deny-key-name-suffix": secret(f"{key_name}-x", labels("encryption-key"), KEY),
    "deny-key-dsn-prefix": secret(dsn_name, labels("encryption-key"), KEY),
    "deny-key-wrong-role": secret(key_name, labels("encryption-keys"), KEY),
    "deny-key-no-role": secret(key_name, labels("encryption-key", drop=("curietech.ai/shared-secret",)), KEY),
    "deny-key-no-application": secret(
        key_name, labels("encryption-key", drop=("curietech.ai/shared-application",)), KEY
    ),
    "deny-key-wrong-component": secret(
        key_name, labels("encryption-key", **{"app.kubernetes.io/component": "mcp-connector"}), KEY
    ),
    "deny-key-no-component": secret(
        key_name, labels("encryption-key", drop=("app.kubernetes.io/component",)), KEY
    ),
    "deny-key-uppercase-uuid": secret(
        key_name, labels("encryption-key", application=app.upper()), KEY
    ),
    "deny-key-extra-data": secret(key_name, labels("encryption-key"), {**KEY, "extra": "x"}),
    "deny-key-wrong-data-key": secret(key_name, labels("encryption-key"), DSN),
    "deny-key-empty-data": secret(key_name, labels("encryption-key"), {}),
    "deny-dsn-key-data": secret(dsn_name, labels("database-dsn"), KEY),
    "deny-key-sa-token": secret(
        key_name, labels("encryption-key"), KEY, kind="kubernetes.io/service-account-token", annotations=SA
    ),
    "deny-key-lowercase-opaque": secret(key_name, labels("encryption-key"), KEY, kind="opaque"),
    "deny-key-sandbox-claim": secret(key_name, labels("encryption-key", **CLAIM), KEY),
    "deny-tokens-shared-application": secret(
        "sck-mixed-tokens", {**CLAIM, "curietech.ai/shared-application": app}, {"CURIE_HISTORY_TOKEN": "x"}
    ),
    "deny-tokens-shared-secret": secret(
        "sck-role-tokens", {**CLAIM, "curietech.ai/shared-secret": "encryption-key"}, {"CURIE_HISTORY_TOKEN": "x"}
    ),
    "deny-tokens-unlabelled": secret("sck-unlabelled-tokens", {}, {"CURIE_HISTORY_TOKEN": "x"}),
    "deny-tokens-sa-token": secret(
        "sck-satoken-tokens", CLAIM, {"CURIE_HISTORY_TOKEN": "x"},
        kind="kubernetes.io/service-account-token", annotations=SA,
    ),
}
# The uppercase case names the Secret after the uppercase label, which a
# Secret name cannot hold, so it keeps the lowercase name: the label alone is
# off-pattern and the name no longer equals prefix + label.
for case, doc in cases.items():
    (out / f"{case}.json").write_text(json.dumps(doc))
PY

SECRET_MESSAGES="$(cat "$WORK/secrets-messages.txt")"
[[ -n "${SECRET_MESSAGES//[$'\n']/}" ]] || fail "${SECRETS_POLICY} rendered no validation message"
fixture() { printf '%s/fixtures/%s.json\n' "$WORK" "$1"; }
worker_create() { as_worker create --dry-run=server -n "$NS" -o json -f "$(fixture "$1")"; }

expect_admitted() {
  local label="$1" case="$2" out
  out="$(worker_create "$case" 2>&1)" || fail "$label: was refused: $(head -c 400 <<<"$out")"
  ok "$label admitted as the worker"
}
expect_denied() {
  local label="$1" case="$2" out message matched=0
  if out="$(worker_create "$case" 2>&1)"; then
    fail "$label: was admitted as the worker"
  fi
  grep -qF "ValidatingAdmissionPolicy '${SECRETS_POLICY}'" <<<"$out" && grep -qF "denied request" <<<"$out" \
    || fail "$label: refused, but not by ValidatingAdmissionPolicy '${SECRETS_POLICY}': $(head -c 400 <<<"$out")"
  while IFS= read -r message; do
    [[ -n "$message" ]] && grep -qF -- "$message" <<<"$out" && { matched=1; break; }
  done <<<"$SECRET_MESSAGES"
  (( matched )) || fail "$label: refused without the policy's message: $(head -c 400 <<<"$out")"
  ok "$label refused by ${SECRETS_POLICY} with its message"
}

activated=0
for _ in $(seq 1 30); do
  last="$(worker_create deny-key-extra-data 2>&1 || true)"
  grep -qF "ValidatingAdmissionPolicy '${SECRETS_POLICY}'" <<<"$last" && { activated=1; break; }
  sleep 2
done
(( activated )) || fail "activation: ${SECRETS_POLICY} never refused the worker within 60s: $(head -c 400 <<<"$last")"

expect_admitted "key Secret shared-key-<uuid> (private-keys, Opaque, labelled)" allow-key
expect_admitted "DSN Secret shared-dsn-<uuid> (sealed-dsn, Opaque, labelled)" allow-dsn
expect_admitted "labelled Opaque -tokens Secret" allow-tokens
typeless="$(worker_create typeless-key 2>&1)" || fail "a typeless key Secret was refused: $(head -c 400 <<<"$typeless")"
[[ "$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("type"))' <<<"$typeless")" == "Opaque" ]] \
  || fail "a typeless key Secret was admitted without being defaulted to Opaque"
ok "a key Secret sent without type is defaulted to Opaque before admission (not a denial case)"

expect_denied "key Secret named for another application" deny-key-other-uuid
expect_denied "key Secret with a name suffix" deny-key-name-suffix
expect_denied "key Secret under the DSN prefix" deny-key-dsn-prefix
expect_denied "key Secret with a wrong role label value" deny-key-wrong-role
expect_denied "key Secret without the role label" deny-key-no-role
expect_denied "key Secret without the application label" deny-key-no-application
expect_denied "key Secret with component mcp-connector" deny-key-wrong-component
expect_denied "key Secret without the component label" deny-key-no-component
expect_denied "key Secret with an uppercase application UUID" deny-key-uppercase-uuid
expect_denied "key Secret with an extra data key" deny-key-extra-data
expect_denied "key Secret with the DSN data key" deny-key-wrong-data-key
expect_denied "key Secret with no data" deny-key-empty-data
expect_denied "DSN Secret with the key data key" deny-dsn-key-data
expect_denied "key Secret of type kubernetes.io/service-account-token" deny-key-sa-token
expect_denied "key Secret of type opaque (not Opaque)" deny-key-lowercase-opaque
expect_denied "key Secret carrying curietech.ai/sandbox-claim" deny-key-sandbox-claim
expect_denied "-tokens Secret carrying curietech.ai/shared-application" deny-tokens-shared-application
expect_denied "-tokens Secret carrying curietech.ai/shared-secret" deny-tokens-shared-secret
expect_denied "unlabelled -tokens Secret (pre-existing case)" deny-tokens-unlabelled
expect_denied "service-account-token -tokens Secret (pre-existing case)" deny-tokens-sa-token

for case in deny-key-extra-data deny-key-sa-token; do
  control="$(k create --dry-run=server -n "$NS" -o name -f "$(fixture "$case")" 2>&1)" \
    || fail "negative control: the cluster admin's $case was refused, so the worker denial may not be the policy: $control"
done
ok "negative control: the cluster admin creating the same denied shapes is admitted (the denial is the worker-scoped policy)"

# --- 6. the production reconcile loop and the network probes -------------------------
k -n "$NS" create secret generic "$GRANT_SECRET" \
  --from-literal=url="redis://grant-store.invalid:6379/0" >/dev/null \
  || fail "create the operator grant-store Secret"

export SCK_RUN_ID="$RUN_ID" SCK_WORKDIR="$WORK" SCK_KUBECONFIG="$KCFG" SCK_CONTEXT="$CTX" \
  SCK_NAMESPACE="$NS" SCK_RELEASE="$REL" SCK_FULLNAME="$FULL" SCK_APP_NAME="$APP_NAME" \
  SCK_APPS="$APP1 $APP2 $APP3" SCK_IMAGE="$BUSYBOX" SCK_GRANT_SECRET="$GRANT_SECRET" \
  SCK_DB_TARGET_LABEL="$DB_TARGET_LABEL" SCK_SECRETS_POLICY="$SECRETS_POLICY" \
  SCK_PG_PORT="$PG_PORT" SCK_PG_PASSWORD="$PG_PASSWORD" \
  SCK_VALKEY_PORT="$VALKEY_PORT" SCK_VALKEY_PASSWORD="$VALKEY_PASSWORD" \
  SCK_S3_PORT="$S3_PORT" SCK_S3_ACCESS="$S3_ACCESS" SCK_S3_SECRET="$S3_SECRET" \
  SCK_RECORD_STATUS_FIXTURES="${SHARED_KIND_RECORD_STATUS_FIXTURES:-}"
(cd "$REPO_ROOT" && uv run python "$DRIVER") || fail "the shared connector driver reported a failure (see above)"

echo "ALL PASS ($pass script checks plus the driver's) cluster=$CLUSTER run=$RUN_ID"
