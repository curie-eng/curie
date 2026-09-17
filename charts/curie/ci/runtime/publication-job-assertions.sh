#!/usr/bin/env bash
# Run the publication Job proof against one explicitly owned kind cluster.
set -euo pipefail

: "${CURIE_PUBLICATION_KUBE_CONTEXT:?set the explicit owned kind context}"
: "${CURIE_PUBLICATION_KIND_CLUSTER:?set the explicit owned kind cluster name}"
: "${CURIE_PUBLICATION_RUNNER_IMAGE:?set the locally built runner image}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
REQUESTED_CONTEXT="$CURIE_PUBLICATION_KUBE_CONTEXT"
KIND_CLUSTER="$CURIE_PUBLICATION_KIND_CLUSTER"
RUNNER_IMAGE="$CURIE_PUBLICATION_RUNNER_IMAGE"
NAMESPACE="test-2673-publication"
OWNER_NAME="publication-owner"
SERVICE_ACCOUNT="publication-runner"
FIXTURE_SERVICE="publication-fixture"
POSTGRES_SERVICE="publication-postgres"
POSTGRES_IMAGE="postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
TMP_DIR="$(mktemp -d)"
PRIVATE_KUBECONFIG="$TMP_DIR/kubeconfig"
FIXTURE_IMAGE="curie-2673-publication-fixture:$$"
NAMESPACE_CREATED=0
CLEANUP_STARTED=0
POSTGRES_FORWARD_PID=""
FIXTURE_FORWARD_PID=""
KIND_NODES=()

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

required_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is missing: $1"
}

kc() {
  kubectl --context "$REQUESTED_CONTEXT" "$@"
}

sanitize() {
  sed -E \
    -e 's#(Authorization: )[[:alnum:]_-]+ [^[:space:]]+#\1[REDACTED]#gI' \
    -e 's#(postgres(ql)?://[^:/@[:space:]]+:)[^@[:space:]]+@#\1[REDACTED]@#g'
}

diagnostics() {
  echo "publication cluster diagnostics" >&2
  kc get pods,jobs,configmaps -n "$NAMESPACE" -o wide 2>&1 | sanitize >&2 || true
  kc get events -n "$NAMESPACE" --sort-by=.lastTimestamp 2>&1 \
    | tail -80 | sanitize >&2 || true
  kc logs -n "$NAMESPACE" "pod/$FIXTURE_SERVICE" --tail=80 2>&1 \
    | sanitize >&2 || true
  while IFS= read -r job; do
    [[ -n "$job" ]] || continue
    kc logs -n "$NAMESPACE" "$job" --tail=80 2>&1 | sanitize >&2 || true
  done < <(
    kc get jobs -n "$NAMESPACE" \
      -l curietech.ai/component=publication \
      -o name 2>/dev/null || true
  )
}

stop_process() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    return 1
  fi
}

cleanup() {
  local rc="${1:-$?}" cleanup_failed=0 node image_ref node_images
  (( CLEANUP_STARTED == 0 )) || return
  CLEANUP_STARTED=1
  trap - EXIT INT TERM
  set +e

  if (( rc != 0 && NAMESPACE_CREATED == 1 )); then
    diagnostics
  fi
  stop_process "$FIXTURE_FORWARD_PID" || cleanup_failed=1
  stop_process "$POSTGRES_FORWARD_PID" || cleanup_failed=1

  if (( NAMESPACE_CREATED == 1 )); then
    kc delete namespace "$NAMESPACE" --wait=true --timeout=180s >/dev/null 2>&1
    local namespace_after_cleanup
    if ! namespace_after_cleanup="$(
      kc get namespace "$NAMESPACE" --ignore-not-found -o name 2>/dev/null
    )"; then
      echo "FAIL: could not verify namespace $NAMESPACE cleanup" >&2
      cleanup_failed=1
    elif [[ -n "$namespace_after_cleanup" ]]; then
      echo "FAIL: owned namespace $NAMESPACE still exists after teardown" >&2
      cleanup_failed=1
    else
      echo "teardown confirmed: namespace $NAMESPACE is absent"
    fi
  fi

  image_ref="$FIXTURE_IMAGE"
  if [[ "$image_ref" != */* ]]; then
    image_ref="docker.io/library/$image_ref"
  fi
  for node in "${KIND_NODES[@]}"; do
    docker exec "$node" ctr --namespace k8s.io images rm "$image_ref" \
      >/dev/null 2>&1 || true
    if ! node_images="$(
      docker exec "$node" ctr --namespace k8s.io images list -q 2>/dev/null
    )"; then
      echo "FAIL: could not verify image cleanup on kind node $node" >&2
      cleanup_failed=1
    elif grep -Fx "$image_ref" <<<"$node_images" >/dev/null; then
      echo "FAIL: owned image $image_ref remains on kind node $node" >&2
      cleanup_failed=1
    fi
  done
  docker image rm -f "$FIXTURE_IMAGE" >/dev/null 2>&1 || true
  if docker image inspect "$FIXTURE_IMAGE" >/dev/null 2>&1; then
    echo "FAIL: owned local image $FIXTURE_IMAGE remains after teardown" >&2
    cleanup_failed=1
  fi

  rm -rf -- "$TMP_DIR"
  if [[ -e "$TMP_DIR" ]]; then
    echo "FAIL: owned temporary directory $TMP_DIR remains after teardown" >&2
    cleanup_failed=1
  fi
  if (( cleanup_failed == 1 && rc == 0 )); then
    rc=1
  fi
  exit "$rc"
}
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM

for command_name in docker git kind kubectl openssl uv; do
  required_command "$command_name"
done

case "$REQUESTED_CONTEXT" in
  *ProdCurietechAi*|*StagingCurietechAi*)
    fail "refusing production or staging context $REQUESTED_CONTEXT"
    ;;
  kind-*) ;;
  *) fail "publication proof requires an explicit owned kind context" ;;
esac
[[ "$REQUESTED_CONTEXT" == "kind-$KIND_CLUSTER" ]] || {
  fail "context $REQUESTED_CONTEXT does not match kind cluster $KIND_CLUSTER"
}
kind get clusters | grep -Fx "$KIND_CLUSTER" >/dev/null \
  || fail "owned kind cluster $KIND_CLUSTER does not exist"
docker image inspect "$RUNNER_IMAGE" >/dev/null 2>&1 \
  || fail "runner image $RUNNER_IMAGE does not exist in the local daemon"
CANDIDATE_HEAD="$(git -C "$ROOT" rev-parse HEAD)"
[[ "$CANDIDATE_HEAD" =~ ^[0-9a-f]{40}$ ]] \
  || fail "candidate git HEAD is not an immutable commit"
RUNNER_IMAGE_ID="$(docker image inspect "$RUNNER_IMAGE" --format '{{.Id}}')"
[[ "$RUNNER_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || fail "runner image did not resolve to an immutable image ID"
echo "publication proof candidate git HEAD: $CANDIDATE_HEAD"
echo "publication proof source runner image ID: $RUNNER_IMAGE_ID"

kubectl --context "$REQUESTED_CONTEXT" config view --raw --minify \
  > "$PRIVATE_KUBECONFIG"
chmod 0600 "$PRIVATE_KUBECONFIG"
export KUBECONFIG="$PRIVATE_KUBECONFIG"
[[ "$(kubectl --context "$REQUESTED_CONTEXT" config current-context)" == "$REQUESTED_CONTEXT" ]] \
  || fail "private kubeconfig did not preserve the requested context"
mapfile -t KIND_NODES < <(kind get nodes --name "$KIND_CLUSTER")
(( ${#KIND_NODES[@]} > 0 )) || fail "owned kind cluster has no nodes"

if kc get namespace "$NAMESPACE" >/dev/null 2>&1; then
  fail "namespace $NAMESPACE already exists; refusing to adopt it"
fi
kc create namespace "$NAMESPACE" >/dev/null
NAMESPACE_CREATED=1
kc label namespace "$NAMESPACE" curie-publication-proof=owned >/dev/null

FIXTURE_DNS="$FIXTURE_SERVICE.$NAMESPACE.svc.cluster.local"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj "/CN=Curie publication fixture CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "$TMP_DIR/ca.key" -out "$TMP_DIR/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 \
  -subj "/CN=$FIXTURE_DNS" \
  -keyout "$TMP_DIR/server.key" -out "$TMP_DIR/server.csr" >/dev/null 2>&1
cat > "$TMP_DIR/server.ext" <<EOF
subjectAltName=DNS:$FIXTURE_DNS,IP:127.0.0.1
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF
openssl x509 -req -sha256 -days 1 \
  -in "$TMP_DIR/server.csr" \
  -CA "$TMP_DIR/ca.crt" -CAkey "$TMP_DIR/ca.key" -CAcreateserial \
  -extfile "$TMP_DIR/server.ext" -out "$TMP_DIR/server.crt" >/dev/null 2>&1

cp "$ROOT/apps/worker/tests/fixtures/publication_service.py" \
  "$TMP_DIR/publication_service.py"
cp "$TMP_DIR/ca.crt" "$TMP_DIR/fixture-ca.crt"
cat > "$TMP_DIR/Dockerfile" <<EOF
ARG RUNNER_IMAGE
FROM \${RUNNER_IMAGE}
USER root
COPY fixture-ca.crt /usr/local/share/ca-certificates/curie-publication-fixture.crt
RUN update-ca-certificates \
    && git config --system \
      'url.git://$FIXTURE_DNS:9418/acme-bot.git.insteadOf' \
      'https://github.com/acme-corp/acme-bot.git'
COPY publication_service.py /opt/curie-publication-fixture/server.py
USER 1000:1000
EOF
docker build \
  --build-arg "RUNNER_IMAGE=$RUNNER_IMAGE" \
  -f "$TMP_DIR/Dockerfile" \
  -t "$FIXTURE_IMAGE" \
  "$TMP_DIR" >/dev/null
FIXTURE_IMAGE_ID="$(docker image inspect "$FIXTURE_IMAGE" --format '{{.Id}}')"
[[ "$FIXTURE_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || fail "fixture image did not resolve to an immutable image ID"
echo "publication proof fixture image ID: $FIXTURE_IMAGE_ID"
kind load docker-image "$FIXTURE_IMAGE" --name "$KIND_CLUSTER" >/dev/null

kc create serviceaccount "$SERVICE_ACCOUNT" -n "$NAMESPACE" >/dev/null
kc create configmap "$OWNER_NAME" -n "$NAMESPACE" \
  --from-literal=owner=publication-cluster-proof >/dev/null
kc create secret tls publication-fixture-tls -n "$NAMESPACE" \
  --cert="$TMP_DIR/server.crt" --key="$TMP_DIR/server.key" >/dev/null

kc apply -n "$NAMESPACE" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $FIXTURE_SERVICE
  labels:
    app.kubernetes.io/name: $FIXTURE_SERVICE
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: fixture
      image: $FIXTURE_IMAGE
      imagePullPolicy: Never
      command:
        - python
        - /opt/curie-publication-fixture/server.py
        - --root
        - /srv/git
        - --cert
        - /tls/tls.crt
        - --key
        - /tls/tls.key
      ports:
        - name: https
          containerPort: 8443
        - name: git
          containerPort: 9418
      readinessProbe:
        tcpSocket:
          port: https
        periodSeconds: 1
        timeoutSeconds: 1
        failureThreshold: 60
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: git
          mountPath: /srv/git
        - name: tls
          mountPath: /tls
          readOnly: true
  volumes:
    - name: git
      emptyDir: {}
    - name: tls
      secret:
        secretName: publication-fixture-tls
---
apiVersion: v1
kind: Service
metadata:
  name: $FIXTURE_SERVICE
spec:
  selector:
    app.kubernetes.io/name: $FIXTURE_SERVICE
  ports:
    - name: https
      port: 8443
      targetPort: https
    - name: git
      port: 9418
      targetPort: git
---
apiVersion: v1
kind: Pod
metadata:
  name: $POSTGRES_SERVICE
  labels:
    app.kubernetes.io/name: $POSTGRES_SERVICE
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
    - name: postgres
      image: $POSTGRES_IMAGE
      env:
        - name: POSTGRES_USER
          value: postgres
        - name: POSTGRES_PASSWORD
          value: postgres
        - name: POSTGRES_DB
          value: postgres
      ports:
        - name: postgres
          containerPort: 5432
      readinessProbe:
        exec:
          command: ["pg_isready", "-U", "postgres", "-d", "postgres"]
        periodSeconds: 1
        timeoutSeconds: 1
        failureThreshold: 90
---
apiVersion: v1
kind: Service
metadata:
  name: $POSTGRES_SERVICE
spec:
  selector:
    app.kubernetes.io/name: $POSTGRES_SERVICE
  ports:
    - name: postgres
      port: 5432
      targetPort: postgres
EOF

kc wait -n "$NAMESPACE" \
  --for=condition=Ready \
  "pod/$FIXTURE_SERVICE" "pod/$POSTGRES_SERVICE" \
  --timeout=180s >/dev/null

start_port_forward() {
  local service="$1" remote_port="$2" log_file="$3"
  local pid_name="$4" port_name="$5" pid port="" waited=0
  kc port-forward -n "$NAMESPACE" "service/$service" ":$remote_port" \
    > "$log_file" 2>&1 &
  pid=$!
  printf -v "$pid_name" '%s' "$pid"
  while (( waited < 30 )); do
    port="$(
      sed -nE "s/^Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> $remote_port$/\\1/p" \
        "$log_file" | head -1
    )"
    [[ -n "$port" ]] && break
    kill -0 "$pid" >/dev/null 2>&1 \
      || fail "port forward for service $service exited early"
    sleep 1
    waited=$((waited + 1))
  done
  [[ -n "$port" ]] || fail "port forward for service $service did not become ready"
  printf -v "$port_name" '%s' "$port"
}

POSTGRES_PORT=""
FIXTURE_PORT=""
start_port_forward "$POSTGRES_SERVICE" 5432 \
  "$TMP_DIR/postgres-forward.log" POSTGRES_FORWARD_PID POSTGRES_PORT
start_port_forward "$FIXTURE_SERVICE" 8443 \
  "$TMP_DIR/fixture-forward.log" FIXTURE_FORWARD_PID FIXTURE_PORT

DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:$POSTGRES_PORT/postgres"
(
  cd "$ROOT/apps/api"
  DATABASE_URL="$DATABASE_URL" uv run alembic upgrade head
)

CURIE_PUBLICATION_CLUSTER_PROOF=1 \
CURIE_PUBLICATION_KUBECONFIG="$PRIVATE_KUBECONFIG" \
CURIE_PUBLICATION_NAMESPACE="$NAMESPACE" \
CURIE_PUBLICATION_RUNNER_IMAGE="$FIXTURE_IMAGE" \
CURIE_PUBLICATION_FIXTURE_API="https://127.0.0.1:$FIXTURE_PORT" \
CURIE_PUBLICATION_FIXTURE_CLUSTER_API="https://$FIXTURE_DNS:8443" \
CURIE_PUBLICATION_FIXTURE_CA="$TMP_DIR/ca.crt" \
TEST_DATABASE_URL="$DATABASE_URL" \
  env -u KUBERNETES_SERVICE_HOST -u KUBERNETES_SERVICE_PORT \
    uv run pytest -q -rA apps/worker/tests/test_publication_cluster.py
