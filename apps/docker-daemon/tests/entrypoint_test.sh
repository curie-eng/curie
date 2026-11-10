#!/usr/bin/env bash
# Exercises the real apps/docker-daemon/entrypoint.sh without Docker.
#
# The entrypoint must inject `--authorization-plugin=curie-authz
# --userns-remap=default` right after `dockerd`, before any caller argument, so
# no value-consuming caller flag (`--label`, `-H`, ...) can swallow them, and
# must refuse any caller argument mentioning userns-remap other than the exact
# duplicate `--userns-remap=default`, and any caller argument mentioning
# config-file (a caller --config-file would replace daemon.json and with it the
# BuildKit entitlements). The daemon must start through
# `curie-docker-authz cgroup-isolate -- dockerd-entrypoint.sh dockerd ...`, so
# dockerd runs in a private cgroup namespace even when the container runtime
# gives the privileged container none.
#
# Fakes, first on PATH:
#   dockerd-entrypoint.sh  prints its argv one per line and exits 0.
#   curie-docker-authz     plugin mode: records that it ran, binds a real unix
#                          socket at the path given with --socket (the
#                          entrypoint waits on `[ -S ]`), and holds it briefly.
#                          `cgroup-isolate -- CMD...` mode: prints a
#                          `<cgroup-isolate>` line and its argv up to `--`,
#                          then execs CMD. Anything else exits 2.
#
# Env override relied on: CURIE_AUTHZ_SOCKET, the socket path the entrypoint
# passes to the plugin and waits for (default
# /run/docker/plugins/curie-authz.sock). The entrypoint must create its parent
# directory, not a hardcoded /run/docker/plugins.
#
# Usage: bash apps/docker-daemon/tests/entrypoint_test.sh (from any directory).
set -u

here=$(cd "$(dirname "$0")" && pwd)
entrypoint="$here/../entrypoint.sh"
[ -f "$entrypoint" ] || { echo "FAIL: $entrypoint not found" >&2; exit 1; }

root=$(mktemp -d /tmp/curie-ep.XXXXXX)
trap 'rm -rf "$root"' EXIT

bin="$root/bin"
mkdir -p "$bin"

cat >"$bin/dockerd-entrypoint.sh" <<'EOF'
#!/bin/sh
for a in "$@"; do printf '%s\n' "$a"; done
exit 0
EOF

cat >"$bin/curie-docker-authz" <<'EOF'
#!/bin/sh
if [ "${1:-}" = cgroup-isolate ]; then
  printf '%s\n' '<cgroup-isolate>'
  shift
  [ "${1:-}" = -- ] || { echo "fake cgroup-isolate: no --" >&2; exit 2; }
  printf '%s\n' --
  shift
  [ $# -gt 0 ] || { echo "fake cgroup-isolate: no command" >&2; exit 2; }
  exec "$@"
fi
sock=
while [ $# -gt 0 ]; do
  case "$1" in
    --socket) sock=$2; shift 2 ;;
    --socket=*) sock=${1#--socket=}; shift ;;
    *) echo "fake plugin: unexpected argument $1" >&2; exit 2 ;;
  esac
done
[ -n "$sock" ] || { echo "fake plugin: no --socket" >&2; exit 2; }
: >"$CURIE_TEST_PLUGIN_MARKER"
exec python3 -c '
import os, socket, sys, time
p = sys.argv[1]
os.makedirs(os.path.dirname(p), exist_ok=True)
s = socket.socket(socket.AF_UNIX)
s.bind(p)
s.listen(1)
time.sleep(3)
' "$sock"
EOF
chmod +x "$bin/dockerd-entrypoint.sh" "$bin/curie-docker-authz"

INJ1=--authorization-plugin=curie-authz
INJ2=--userns-remap=default
# Printed by the fake when the entrypoint execs through
# `curie-docker-authz cgroup-isolate -- dockerd-entrypoint.sh`. The fake
# dockerd-entrypoint.sh then prints its own argv, so a hardened start's output
# is exactly ISO, `--`, then the dockerd argv.
ISO='<cgroup-isolate>'

n=0
out='' err='' code='' marker=''
# run ARGS...: runs the entrypoint with a fresh socket dir; sets out, err,
# code, and marker (path that exists if the plugin was started).
run() {
  n=$((n + 1))
  local case_dir="$root/c$n"
  mkdir -p "$case_dir"
  marker="$case_dir/plugin-started"
  set +e
  out=$(
    cd "$case_dir" &&
      PATH="$bin:$PATH" \
        CURIE_AUTHZ_SOCKET="$case_dir/plugins/curie-authz.sock" \
        CURIE_TEST_PLUGIN_MARKER="$marker" \
        timeout 20 sh "$entrypoint" "$@" 2>"$case_dir/stderr"
  )
  code=$?
  set -e
  err=$(cat "$case_dir/stderr")
}

fail() {
  echo "FAIL $1: $2" >&2
  echo "  exit code: $code" >&2
  echo "  stdout:" >&2
  printf '%s\n' "$out" | sed 's/^/    /' >&2
  echo "  stderr:" >&2
  printf '%s\n' "$err" | sed 's/^/    /' >&2
  exit 1
}

# expect_exec CASE EXPECTED_DOCKERD_ARGV... : exit 0, plugin started, the
# daemon went through cgroup-isolate exactly once with `--`, then the exact
# dockerd argv.
expect_exec() {
  local name=$1
  shift
  [ "$code" -eq 0 ] || fail "$name" "expected exit 0"
  [ -e "$marker" ] || fail "$name" "the authz plugin was not started"
  local isolations
  isolations=$(printf '%s\n' "$out" | grep -cxF -- "$ISO" || true)
  [ "$isolations" -eq 1 ] ||
    fail "$name" "expected exactly one cgroup-isolate in the exec chain, saw $isolations"
  local want
  want=$(printf '%s\n' "$ISO" -- "$@")
  [ "$out" = "$want" ] || fail "$name" "exec'd argv differs; expected:
$want"
  echo "ok $name"
}

# expect_refused CASE NEEDLE : exit 1, message names NEEDLE, dockerd not run.
expect_refused() {
  local name=$1 needle=$2
  [ "$code" -eq 1 ] || fail "$name" "expected exit 1"
  [ -z "$out" ] || fail "$name" "dockerd-entrypoint.sh ran despite the refusal"
  case "$err" in
    *"$needle"*) ;;
    *) fail "$name" "refusal message does not name the argument $needle" ;;
  esac
  echo "ok $name"
}

set -e

# (a) No hardening flags from the caller: the entrypoint supplies both, first.
run dockerd -H tcp://0.0.0.0:2375
expect_exec a-injects-hardening-flags \
  dockerd "$INJ1" "$INJ2" -H tcp://0.0.0.0:2375

# (b) The ADR 0217 command: caller duplicates are harmless; injected flags lead.
run dockerd --userns-remap=default --authorization-plugin=curie-authz \
  -H tcp://0.0.0.0:2375 --tls=false
expect_exec b-adr-command \
  dockerd "$INJ1" "$INJ2" --userns-remap=default \
  --authorization-plugin=curie-authz -H tcp://0.0.0.0:2375 --tls=false

# (c) A value-consuming flag may only swallow the caller's own duplicate, never
# the injected flags, because those come before every caller argument.
run dockerd --label --authorization-plugin=curie-authz
expect_exec c-label-cannot-consume-injected \
  dockerd "$INJ1" "$INJ2" --label --authorization-plugin=curie-authz

# (d) A later empty --userns-remap= would disable remapping after approval.
run dockerd --userns-remap=default --userns-remap=
expect_refused d-empty-userns-remap-refused "--userns-remap="

# (e) Any other remap target, in the two-argument form, is refused.
run dockerd --userns-remap other
expect_refused e-other-userns-remap-refused "--userns-remap"

# (f) First argument a flag: the daemon is implied and still hardened.
run --debug
expect_exec f-leading-flag-gets-dockerd \
  dockerd "$INJ1" "$INJ2" --debug

# (h) A caller --config-file would replace daemon.json (and its BuildKit
# entitlements); both spellings are refused before anything starts.
run dockerd --config-file=/dev/null
expect_refused h-config-file-equals-refused "--config-file=/dev/null"
[ ! -e "$marker" ] || fail h-config-file-equals-refused "the authz plugin was started"
run dockerd --config-file /dev/null
expect_refused h-config-file-two-arg-refused "--config-file"
run dockerd -H tcp://0.0.0.0:2375 --config-file=/tmp/x.json --tls=false
expect_refused h-config-file-later-refused "--config-file=/tmp/x.json"
run --config-file=/dev/null
expect_refused h-config-file-leading-flag-refused "--config-file=/dev/null"
run dockerd --label=x --config-file /dev/null
expect_refused h-config-file-after-label-refused "--config-file"

# (i) Only option spellings are refused: a harmless value that merely contains
# config-file or userns-remap (a label) must still start.
run dockerd --label=purpose=config-file-check
expect_exec i-label-mentioning-config-file-starts \
  dockerd "$INJ1" "$INJ2" --label=purpose=config-file-check
run dockerd --label=note=userns-remap-x
expect_exec i-label-mentioning-userns-remap-starts \
  dockerd "$INJ1" "$INJ2" --label=note=userns-remap-x

# (g) Not a daemon start: run the command as is, no plugin.
run sh -c 'echo ran-g'
[ "$code" -eq 0 ] || fail g-non-daemon-command "expected exit 0"
[ "$out" = ran-g ] || fail g-non-daemon-command "the command did not run (or went through cgroup-isolate)"
[ ! -e "$marker" ] || fail g-non-daemon-command "the authz plugin was started"
run sh -c true
[ "$code" -eq 0 ] || fail g-non-daemon-command "expected exit 0 for sh -c true"
[ ! -e "$marker" ] || fail g-non-daemon-command "the authz plugin was started"
echo "ok g-non-daemon-command"

echo "all entrypoint cases passed"
