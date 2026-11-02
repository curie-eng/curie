#!/usr/bin/env bash
set -euo pipefail
umask 077

invalid() { printf 'FAIL: %s\n' "$*" >&2; exit 2; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

raw_iterations="${CURIE_REPRO_ITERATIONS:-}"
[[ "$raw_iterations" =~ ^[0-9]+$ ]] \
    || invalid "CURIE_REPRO_ITERATIONS must be an integer from 1 to 200"
iterations="${raw_iterations#"${raw_iterations%%[!0]*}"}"
[[ -n "$iterations" && ${#iterations} -le 3 ]] \
    || invalid "CURIE_REPRO_ITERATIONS must be an integer from 1 to 200"
(( iterations >= 1 && iterations <= 200 )) \
    || invalid "CURIE_REPRO_ITERATIONS must be an integer from 1 to 200"
cluster="${CURIE_REPRO_CLUSTER:-}"
# The node's '-control-plane' suffix must also fit a 63-character DNS label.
[[ ${#cluster} -le 49 && "$cluster" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]] \
    || invalid "CURIE_REPRO_CLUSTER must be 1 to 49 lowercase letters, digits or interior hyphens"

for tool in kind docker cargo kubectl helm setsid curl sha256sum python3 mktemp cat tee rm bash dirname; do
    command -v "$tool" >/dev/null || fail "required executable is unavailable: $tool"
done
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd -- "$repo_root"
images=(
    "curie-api:wsdiag-repro-$cluster"
    "curie-worker:wsdiag-repro-$cluster"
    "curie-runner:wsdiag-repro-$cluster"
)
require_cluster_absent() {
    local clusters existing
    clusters="$(kind get clusters)" || fail "could not inventory kind clusters"
    while IFS= read -r existing; do
        [[ "$existing" != "$cluster" ]] || fail "refusing to use a preexisting kind cluster"
    done <<< "$clusters"
}
require_image_absent() {
    local image="$1" existing
    existing="$(docker image ls --filter "reference=$image" --format '{{.Repository}}:{{.Tag}}')" \
        || fail "could not inventory the candidate image tag"
    [[ -z "$existing" ]] || fail "refusing to overwrite a preexisting candidate image tag"
}
require_cluster_absent
for image in "${images[@]}"; do
    require_image_absent "$image"
done

workdir=""
cluster_owned=0
owned_images=()
cleanup() {
    local status=$? cleanup_status=0 remaining image existing
    trap - EXIT
    trap '' HUP INT TERM
    set +e
    if (( cluster_owned )); then
        if ! kind delete cluster --name "$cluster" --kubeconfig "$KUBECONFIG"; then
            printf 'FAIL: could not delete the owned kind cluster\n' >&2
            cleanup_status=1
        fi
        if remaining="$(kind get clusters)"; then
            while IFS= read -r existing; do
                if [[ "$existing" == "$cluster" ]]; then
                    printf 'FAIL: the owned kind cluster remains\n' >&2
                    cleanup_status=1
                fi
            done <<< "$remaining"
        else
            printf 'FAIL: could not verify kind cluster cleanup\n' >&2
            cleanup_status=1
        fi
    fi
    for image in "${owned_images[@]}"; do
        if ! existing="$(docker image ls --filter "reference=$image" \
            --format '{{.Repository}}:{{.Tag}}')"; then
            printf 'FAIL: could not inventory an owned image tag during cleanup\n' >&2
            cleanup_status=1
            continue
        fi
        [[ -n "$existing" ]] || continue
        if ! docker image rm "$image"; then
            printf 'FAIL: could not remove an owned image tag\n' >&2
            cleanup_status=1
        fi
        if ! remaining="$(docker image ls --filter "reference=$image" \
            --format '{{.Repository}}:{{.Tag}}')"; then
            printf 'FAIL: could not verify owned image tag cleanup\n' >&2
            cleanup_status=1
        elif [[ -n "$remaining" ]]; then
            printf 'FAIL: an owned image tag remains\n' >&2
            cleanup_status=1
        fi
    done
    if [[ -n "$workdir" ]] && ! rm -rf -- "$workdir"; then
        printf 'FAIL: could not remove the private working directory\n' >&2
        cleanup_status=1
    fi
    (( status == 0 && cleanup_status == 0 )) && exit 0
    exit 1
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
workdir="$(mktemp -d "${TMPDIR:-/tmp}/curie-wsdiag-repro.XXXXXXXX")"
export KUBECONFIG="$workdir/kubeconfig"
python3 -c 'import yaml' || fail "Python 3 with PyYAML is required by the scenario"

export CARGO_TARGET_DIR="$repo_root/cli/target"
unset CARGO_BUILD_TARGET
cargo build --release --manifest-path cli/Cargo.toml
export CURIE_BIN="$repo_root/cli/target/release/curie"
[[ -f "$CURIE_BIN" && -x "$CURIE_BIN" ]] || fail "the candidate CLI was not built"
dockerfiles=(apps/api/Dockerfile apps/worker/Dockerfile runner/Dockerfile)
for index in "${!images[@]}"; do
    require_image_absent "${images[$index]}"
    owned_images+=("${images[$index]}")
    docker build -f "${dockerfiles[$index]}" -t "${images[$index]}" .
done

cat > "$workdir/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  disableDefaultCNI: true
  podSubnet: "192.168.0.0/16"
nodes:
  - role: control-plane
YAML
require_cluster_absent
cluster_owned=1
kind create cluster --name "$cluster" --image kindest/node:v1.31.0 \
    --config "$workdir/kind.yaml" --kubeconfig "$KUBECONFIG"
export CURIE_SCENARIO_CONTEXT="kind-$cluster"
calico_manifest="$workdir/calico-v3.28.2.yaml"
curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 -o "$calico_manifest" \
    https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml
printf 'be59408bf990e96276f631d2f9285c2a0f9802194c0ad1cecdb6d9c52623a1c8  %s\n' "$calico_manifest" \
    | sha256sum --check --strict
kubectl --context "$CURIE_SCENARIO_CONTEXT" apply -f "$calico_manifest"
kubectl --context "$CURIE_SCENARIO_CONTEXT" -n kube-system \
    rollout status daemonset/calico-node --timeout=300s
kubectl --context "$CURIE_SCENARIO_CONTEXT" wait --for=condition=Ready node --all --timeout=300s
kind load docker-image "${images[@]}" --name "$cluster"
export CURIE_SCENARIO_API_IMAGE="${images[0]}"
export CURIE_SCENARIO_WORKER_IMAGE="${images[1]}"
export CURIE_SCENARIO_RUNNER_IMAGE="${images[2]}"
run_prefix="$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
passed=0
recovery_500=0
other_fail=0
export TMPDIR="$workdir"
run_iteration() (
    fixture_pending=0
    finish_iteration() {
        local status=$?
        trap - EXIT INT TERM
        set +e
        if (( fixture_pending )); then
            bash "$repo_root/charts/curie/ci/scenarios/workload-install.sh" down || status=1
        fi
        exit "$status"
    }
    trap finish_iteration EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    fixture_pending=1
    bash "$repo_root/charts/curie/ci/scenarios/workload-install.sh" up || exit 1
    bash "$repo_root/charts/curie/ci/runtime/workspace-dependency-diagnostic-runtime.sh"
)
for (( iteration=1; iteration<=iterations; iteration++ )); do
    export CURIE_SCENARIO_RUN_ID="wsd-$run_prefix-$iteration"
    export CURIE_SCENARIO_NAMESPACE="test-workload-$CURIE_SCENARIO_RUN_ID"
    export CURIE_SCENARIO_RELEASE="workload-$CURIE_SCENARIO_RUN_ID"
    logfile="$workdir/iteration-$iteration.log"
    set +e
    run_iteration 2>&1 | tee "$logfile"
    statuses=("${PIPESTATUS[@]}")
    set -e
    (( statuses[1] == 0 )) || fail "could not preserve scenario output"
    result="$(python3 "$repo_root/charts/curie/ci/repro/classify_recovery.py" \
        --exit-code "${statuses[0]}" "$logfile")" || fail "could not classify scenario output"
    case "$result" in
        pass) passed=$((passed + 1)) ;;
        recovery-500) recovery_500=$((recovery_500 + 1)) ;;
        other-fail) other_fail=$((other_fail + 1)) ;;
        *) fail "the classifier returned an unknown result" ;;
    esac
    printf 'iteration=%d result=%s\n' "$iteration" "$result"
done
printf 'summary iterations=%d pass=%d recovery_500=%d other_fail=%d\n' \
    "$iterations" "$passed" "$recovery_500" "$other_fail"
exit 0
