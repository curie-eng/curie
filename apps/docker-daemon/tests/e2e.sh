#!/usr/bin/env bash
# Black-box escape test for the per-run Docker daemon image (ADR 0217 decision 4).
#
# Usage: e2e.sh IMAGE [extra dockerd flags...]
#
# Starts IMAGE as a privileged daemon with the ADR's flags on a private network,
# then drives it from a client container (same IMAGE, its docker CLI) over
# tcp://<daemon>:2375. Ordinary work must succeed; every escape must fail with
# the plugin's `refused by rule <rule>` message. Exits nonzero on the first
# mismatch. A second daemon started with --cgroupns=host checks that dockerd
# still runs in a private cgroup namespace. Removes every container, network,
# and volume it created.
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 IMAGE [extra dockerd flags...]" >&2
  exit 2
fi
image=$1
shift

suffix=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
net=curie-dd-e2e-$suffix
daemon=curie-dd-e2e-daemon-$suffix
client=curie-dd-e2e-client-$suffix
iso=curie-dd-e2e-iso-$suffix
passes=0

# Volumes the daemon container mounts (the dind image's anonymous
# /var/lib/docker volume), recorded at start so cleanup can prove them gone.
daemon_volumes=
client_volumes=
iso_volumes=

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ] && docker container inspect "$daemon" >/dev/null 2>&1; then
    echo "--- last daemon log lines ---" >&2
    docker logs --tail 40 "$daemon" >&2 2>&1 || true
  fi
  if [ "$status" -ne 0 ] && docker container inspect "$iso" >/dev/null 2>&1; then
    echo "--- last cgroup isolation daemon log lines ---" >&2
    docker logs --tail 40 "$iso" >&2 2>&1 || true
  fi
  # Tolerant of objects that were never created or are already gone; the
  # absence check below is what decides whether cleanup worked.
  # -v removes the anonymous /var/lib/docker volume the dind image declares.
  docker rm -f -v "$client" "$daemon" "$iso" >/dev/null 2>&1 || true
  # Recorded anonymous volumes are removed explicitly in case -v missed one.
  # shellcheck disable=SC2086
  [ -z "$daemon_volumes$client_volumes$iso_volumes" ] ||
    docker volume rm -f $daemon_volumes $client_volumes $iso_volumes >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  # Each inventory query must succeed: a failed query is not proof of absence.
  local left='' v q_out inventory_failed=''
  if ! q_out=$(docker ps -a --filter "name=$suffix" --format 'container {{.Names}}' 2>&1); then
    inventory_failed="${inventory_failed}docker ps -a failed: $q_out"$'\n'
  else
    [ -z "$q_out" ] || left="${left}${q_out}"$'\n'
  fi
  if ! q_out=$(docker network ls --filter "name=$suffix" --format 'network {{.Name}}' 2>&1); then
    inventory_failed="${inventory_failed}docker network ls failed: $q_out"$'\n'
  else
    [ -z "$q_out" ] || left="${left}${q_out}"$'\n'
  fi
  if ! q_out=$(docker volume ls -q 2>&1); then
    inventory_failed="${inventory_failed}docker volume ls failed: $q_out"$'\n'
  else
    # Recorded anonymous volumes have random names, so match by membership.
    # A here-string, not a pipe: with pipefail, grep -q exiting early would
    # SIGPIPE the writer and turn a match into a miss.
    for v in $daemon_volumes $client_volumes $iso_volumes; do
      if grep -qxF -- "$v" <<<"$q_out"; then
        left="${left}volume $v"$'\n'
      fi
    done
    if printf '%s\n' "$q_out" | grep -F -- "$suffix" >/dev/null; then
      left="${left}$(printf '%s\n' "$q_out" | grep -F -- "$suffix" | sed 's/^/volume /')"$'\n'
    fi
  fi
  if [ -n "$inventory_failed" ]; then
    echo "FAIL cleanup could not enumerate Docker objects:" >&2
    printf '%s' "$inventory_failed" >&2
    [ "$status" -ne 0 ] || status=1
  fi
  if [ -n "$left" ]; then
    echo "FAIL cleanup left objects behind:" >&2
    printf '%s\n' "$left" >&2
    [ "$status" -ne 0 ] || status=1
  fi
  exit "$status"
}
trap cleanup EXIT

fail() {
  echo "FAIL $1" >&2
  echo "--- output ---" >&2
  printf '%s\n' "$2" >&2
  exit 1
}

# Run a shell command line inside the client container.
in_client() {
  docker exec -i "$client" sh -c "$1" 2>&1
}

# allowed <label> <cmd...>: the command must succeed.
allowed() {
  local label=$1 out
  shift
  if out=$(in_client "$*"); then
    echo "PASS allowed: $label"
    passes=$((passes + 1))
  else
    fail "allowed: $label: $*" "$out"
  fi
}

# refused <rule> <cmd...>: the command must fail and name the rule.
refused() {
  local rule=$1 out
  shift
  if out=$(in_client "$*"); then
    fail "refused $rule: command succeeded: $*" "$out"
  fi
  case "$out" in
    *"refused by rule $rule"*) ;;
    *) fail "refused $rule: wrong or missing rule: $*" "$out" ;;
  esac
  echo "PASS refused $rule: $*"
  passes=$((passes + 1))
}

docker network create "$net" >/dev/null
docker run -d --privileged --name "$daemon" --network "$net" "$image" \
  dockerd --userns-remap=default --authorization-plugin=curie-authz \
  -H tcp://0.0.0.0:2375 --tls=false "$@" >/dev/null
daemon_volumes=$(docker container inspect "$daemon" \
  --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}')
docker run -d --name "$client" --network "$net" \
  -e DOCKER_HOST="tcp://$daemon:2375" --entrypoint sh "$image" \
  -c 'sleep 86400' >/dev/null
# The client uses the dind image too, so it also gets an anonymous
# /var/lib/docker volume.
client_volumes=$(docker container inspect "$client" \
  --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}')

ready=no
for _ in $(seq 1 60); do
  if in_client 'docker info' >/dev/null; then
    ready=yes
    break
  fi
  sleep 1
done
[ "$ready" = yes ] || fail "daemon did not answer docker info within 60 s" "$(in_client 'docker info' || true)"

info=$(in_client "docker info --format '{{json .Plugins.Authorization}} {{json .SecurityOptions}}'")
case "$info" in
  *curie-authz*userns*) echo "daemon up: $info" ;;
  *) fail "daemon is missing the plugin or userns remapping" "$info" ;;
esac

# Build contexts, written inside the client (no bind mounts from the host).
in_client 'mkdir -p /w/b /w/c
cat > /w/b/Dockerfile <<"EOF"
FROM alpine:3.20
RUN echo built > /built
EOF
mkdir -p /w/i
cat > /w/i/Dockerfile <<"EOF"
FROM alpine:3.20
RUN --security=insecure true
EOF
cat > /w/c/Dockerfile <<"EOF"
FROM alpine:3.20
RUN printf "#!/bin/sh\nprintf \"HTTP/1.0 200 OK\\r\\n\\r\\ncompose-ok\\n\"\n" > /serve && chmod +x /serve
CMD ["nc", "-lk", "-p", "8080", "-e", "/serve"]
EOF
cat > /w/c/compose.yaml <<"EOF"
services:
  web:
    build: .
    ports: ["8080:8080"]
    volumes: ["data:/data"]
    healthcheck:
      test: ["CMD", "sh", "-c", "wget -q -O - http://127.0.0.1:8080/ | grep -q compose-ok"]
      interval: 1s
      retries: 30
volumes:
  data: {}
EOF'

# Ordinary work.
allowed "info" 'docker info'
allowed "version" 'docker version'
allowed "events with --since/--until" 'docker events --since 2m --until "$(date +%s)"'
allowed "pull alpine:3.20" 'docker pull -q alpine:3.20'
allowed "run" 'docker run --rm alpine:3.20 echo hello'
allowed "run detached" 'docker run -d --name e2e-box alpine:3.20 sleep 600'
allowed "exec" 'docker exec e2e-box true'
allowed "cp in and out" 'echo payload > /tmp/in && docker cp /tmp/in e2e-box:/tmp/in && docker cp e2e-box:/tmp/in /tmp/out && cmp /tmp/in /tmp/out'
allowed "logs" 'docker logs e2e-box'
allowed "top" 'docker top e2e-box'
allowed "stats" 'docker stats --no-stream e2e-box'
allowed "inspect" 'docker inspect e2e-box'
allowed "ps" 'docker ps -a'
allowed "hardened run" 'docker run --rm --cap-drop ALL --cap-add NET_BIND_SERVICE --read-only --security-opt no-new-privileges --tmpfs /tmp --pids-limit 64 --memory 64m --init alpine:3.20 sh -c "echo ok > /tmp/x"'
allowed "volume create" 'docker volume create e2e-vol'
allowed "volume use" 'docker run --rm -v e2e-vol:/data alpine:3.20 sh -c "echo v > /data/v"'
allowed "volume inspect" 'docker volume inspect e2e-vol'
allowed "volume rm" 'docker volume rm e2e-vol'
allowed "network create" 'docker network create e2e-net'
allowed "network connect" 'docker network connect e2e-net e2e-box'
allowed "network disconnect" 'docker network disconnect e2e-net e2e-box'
allowed "network rm" 'docker network rm e2e-net'
allowed "build (BuildKit)" 'docker build -q -t e2e-img:1 /w/b'
allowed "tag" 'docker tag e2e-img:1 e2e-img:2'
allowed "image inspect" 'docker image inspect e2e-img:2'
allowed "rmi" 'docker rmi e2e-img:1 e2e-img:2'
allowed "compose up --build --wait" 'cd /w/c && docker compose -p e2e up --build --wait'
allowed "fetch the published port" "wget -q -O - http://$daemon:8080/ | grep -q compose-ok"
allowed "compose exec" 'cd /w/c && docker compose -p e2e exec -T web ls /data'
allowed "compose down -v" 'cd /w/c && docker compose -p e2e down -v'

# Escapes, each named by the rule that refuses it.
refused privileged 'docker run --rm --privileged alpine:3.20 true'
refused cap-add 'docker run --rm --cap-add SYS_ADMIN alpine:3.20 true'
refused host-namespace 'docker run --rm --userns=host alpine:3.20 true'
refused host-namespace 'docker run --rm --pid=host alpine:3.20 true'
refused host-namespace 'docker run --rm --ipc=host alpine:3.20 true'
refused host-namespace 'docker run --rm --uts=host alpine:3.20 true'
refused host-namespace 'docker run --rm --network=host alpine:3.20 true'
refused host-namespace 'docker run --rm --cgroupns=host alpine:3.20 true'
refused host-bind 'docker run --rm -v /:/host alpine:3.20 true'
refused host-bind 'docker run --rm --mount type=bind,source=/,target=/host alpine:3.20 true'
refused security-opt 'docker run --rm --security-opt seccomp=unconfined alpine:3.20 true'
refused security-opt 'docker run --rm --security-opt apparmor=unconfined alpine:3.20 true'
refused security-opt 'docker run --rm --security-opt systempaths=unconfined alpine:3.20 true'
refused devices 'docker run --rm --device /dev/null:/dev/e2e alpine:3.20 true'
refused devices 'docker run --rm --gpus all alpine:3.20 true'
refused devices 'docker run --rm --device-cgroup-rule "c 1:3 rwm" alpine:3.20 true'
refused sysctls 'docker run --rm --sysctl net.ipv4.ip_forward=1 alpine:3.20 true'
refused cgroup-parent 'docker run --rm --cgroup-parent /e2e alpine:3.20 true'
refused log-driver 'docker run --rm --log-driver syslog alpine:3.20 true'
refused volume-driver-options 'docker run --rm --mount type=volume,source=e2e-evil,target=/x,volume-opt=type=none,volume-opt=o=bind,volume-opt=device=/ alpine:3.20 true'
refused volume-driver-options 'docker volume create --opt type=none --opt o=bind --opt device=/ e2e-evil'
refused network-driver-options 'docker network create -o com.docker.network.bridge.name=e2e0 e2e-net-opts'
refused network-driver 'docker network create -d macvlan e2e-net-macvlan'
refused exec-privileged 'docker exec --privileged e2e-box true'
refused endpoint-not-allowed 'docker plugin ls'
refused endpoint-not-allowed 'docker system df'
refused endpoint-not-allowed 'docker container prune -f'
refused endpoint-not-allowed 'docker commit e2e-box e2e-commit'
refused endpoint-not-allowed 'docker update --memory 128m e2e-box'
# The classic builder's POST /build reads its parameters from a form body the
# plugin cannot see (dockerd forwards the last Content-Type, Go parses the
# first), so the endpoint itself is refused. BuildKit builds use /grpc above.
refused endpoint-not-allowed 'DOCKER_BUILDKIT=0 docker build --network=host /w/b'
refused endpoint-not-allowed 'DOCKER_BUILDKIT=0 docker build /w/b'

# Raw API requests the CLI never sends. busybox wget cannot show a 403 body,
# so /w/raw.sh speaks HTTP over busybox nc, prints the response, and fails
# unless the status is 2xx. Usage: raw.sh CONTENT_TYPE BODY ("" for none).
in_client "cat > /w/raw.sh <<\"EOF\"
ct=\$1 body=\$2
{
  printf 'POST /v1.56/containers/create HTTP/1.1\r\nHost: daemon\r\nConnection: close\r\n'
  [ -z \"\$ct\" ] || printf 'Content-Type: %s\r\n' \"\$ct\"
  printf 'Content-Length: %s\r\n\r\n%s' \"\${#body}\" \"\$body\"
  # Hold stdin open: nc closing its write side reads to dockerd as a client
  # cancel (499) before the response is written. The daemon closes first.
  sleep 5
} | nc -w 10 $daemon 2375 > /tmp/resp
cat /tmp/resp
head -n 1 /tmp/resp | grep -q '^HTTP/1.1 2'
EOF"
refused privileged "sh /w/raw.sh application/json '{\"Image\":\"alpine:3.20\",\"hostconfig\":{\"privileged\":true}}'"
refused body-required "sh /w/raw.sh '' '{\"Image\":\"alpine:3.20\",\"HostConfig\":{\"Privileged\":true}}'"

# A BuildKit build travels over POST /grpc, which the plugin cannot read; the
# daemon's own entitlement setting (daemon.json) refuses network=host there.
out=$(in_client 'docker build --network=host /w/b') && fail "BuildKit --network=host build succeeded" "$out"
case "$out" in
  *network.host*) echo "PASS refused daemon entitlement network.host: docker build --network=host"
    passes=$((passes + 1)) ;;
  *) fail "BuildKit --network=host failed without naming network.host" "$out" ;;
esac

# The same daemon setting refuses the security.insecure entitlement, even when
# the client asks for it explicitly.
out=$(in_client 'docker build --allow security.insecure /w/i') && fail "BuildKit security.insecure build succeeded" "$out"
case "$out" in
  *"security.insecure is not allowed"*) echo "PASS refused daemon entitlement security.insecure: docker build --allow security.insecure"
    passes=$((passes + 1)) ;;
  *) fail "BuildKit security.insecure build failed without naming security.insecure" "$out" ;;
esac

# Fail closed: with the plugin gone, the daemon must refuse everything.
docker exec "$daemon" pkill -x curie-docker-authz
out=$(in_client 'timeout 60 docker info') && fail "docker info succeeded with the plugin killed" "$out"
case "$out" in
  *"plugin curie-authz failed with error"*) ;;
  *) fail "docker info failed with the plugin killed, but not because of the plugin" "$out" ;;
esac
# The refusal must come from a live daemon, not from losing the daemon.
running=$(docker container inspect --format '{{.State.Running}}' "$daemon")
[ "$running" = true ] || fail "daemon container stopped after the plugin was killed" "$running"
docker exec "$daemon" pgrep -x dockerd >/dev/null || fail "dockerd exited after the plugin was killed" "$(docker exec "$daemon" ps)"
echo "PASS fail closed: docker info refused by the dead plugin on a live daemon"
passes=$((passes + 1))

# Cgroup isolation. containerd gives a privileged container no private cgroup
# namespace (Kubernetes privileged pods), and --cgroupns=host reproduces that
# here. Without isolation dockerd sees the host cgroup tree, so a cgroup parent
# a BuildKit build picks can land outside the pod's limits. The entrypoint
# starts dockerd through `curie-docker-authz cgroup-isolate`, which unshares
# the cgroup namespace and remounts cgroup2, so dockerd's own cgroup path is
# namespace relative and never names this container.
#
# Vantage point matters. /proc/<pid>/cgroup is rendered relative to the READER's
# cgroup namespace, and `docker exec` under --cgroupns=host places the reader in
# the host namespace, so reading dockerd's /proc/<pid>/cgroup from the exec shell
# always shows the absolute path even when dockerd is isolated. The checks
# therefore use what is independent of the reader: (1) dockerd's cgroup
# namespace inode differs from the exec shell's, and (2) in dockerd's own
# mountinfo, cgroup2 at /sys/fs/cgroup has a root naming the container's own
# scope rather than /, i.e. it was remounted at the namespace root, so a cgroup
# parent a client picks resolves inside the container's scope. (3) A container
# started in the daemon sees 0::/ as its cgroup.
#
# Observed on the image before cgroup-isolate existed (2026-10-12, k8 engine,
# cgroup v2), the failing state, read from the host namespace:
#   0::/dockerlimit.slice/docker-<full container id>.scope
docker run -d --privileged --cgroupns=host --name "$iso" "$image" dockerd "$@" >/dev/null
iso_volumes=$(docker container inspect "$iso" \
  --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}')
iso_id=$(docker container inspect --format '{{.Id}}' "$iso")
ready=no
for _ in $(seq 1 60); do
  if docker exec "$iso" docker info >/dev/null 2>&1; then
    ready=yes
    break
  fi
  sleep 1
done
[ "$ready" = yes ] || fail "cgroup isolation daemon did not answer docker info within 60 s" \
  "$(docker exec "$iso" docker info 2>&1 || true; docker logs --tail 40 "$iso" 2>&1 || true)"
dockerd_ns=$(docker exec "$iso" sh -c 'readlink /proc/$(pidof dockerd)/ns/cgroup' 2>&1) ||
  fail "could not read dockerd's cgroup namespace in the isolation daemon" "$dockerd_ns"
shell_ns=$(docker exec "$iso" sh -c 'readlink /proc/self/ns/cgroup' 2>&1) ||
  fail "could not read the exec shell's cgroup namespace in the isolation daemon" "$shell_ns"
if [ -z "$dockerd_ns" ] || [ "$dockerd_ns" = "$shell_ns" ]; then
  fail "dockerd shares the exec shell's cgroup namespace (no private cgroup namespace)" "dockerd=$dockerd_ns shell=$shell_ns"
fi
mi=$(docker exec "$iso" sh -c 'cat /proc/$(pidof dockerd)/mountinfo' 2>&1) ||
  fail "could not read dockerd's mountinfo in the isolation daemon" "$mi"
cg_mount=$(printf '%s\n' "$mi" | awk '$5 == "/sys/fs/cgroup"')
if [ -z "$cg_mount" ] || [ "$(printf '%s\n' "$cg_mount" | wc -l)" -ne 1 ]; then
  fail "dockerd does not have exactly one /sys/fs/cgroup mount" "$mi"
fi
cg_fstype=$(printf '%s\n' "$cg_mount" | awk '{for (i = 1; i <= NF; i++) if ($i == "-") { print $(i + 1); exit }}')
cg_root=$(printf '%s\n' "$cg_mount" | awk '{print $4}')
[ "$cg_fstype" = cgroup2 ] ||
  fail "dockerd's /sys/fs/cgroup is not cgroup2" "$cg_mount"
case "$cg_root" in
  /) fail "dockerd's cgroup2 mount is rooted at / (not remounted at the namespace root)" "$cg_mount" ;;
  *"$iso_id"*) ;;
  *) fail "dockerd's cgroup2 mount root does not name the container's own scope" "$cg_mount" ;;
esac
echo "PASS cgroup isolation: dockerd cgroup ns $dockerd_ns differs from exec shell ns $shell_ns; cgroup2 mount root $cg_root"
passes=$((passes + 1))
out=$(docker exec "$iso" docker run --rm alpine:3.20 cat /proc/self/cgroup 2>&1) ||
  fail "docker run failed in the cgroup isolation daemon" "$out"
inner=$(printf '%s\n' "$out" | tail -n 1)
[ "$inner" = "0::/" ] ||
  fail "a container in the isolation daemon does not see 0::/ as its cgroup" "$out"
echo "PASS allowed: run in the cgroup isolation daemon sees $inner"
passes=$((passes + 1))

echo "$passes checks passed"
echo "E2E PASS"
