#!/usr/bin/env bash
#
# Behavioural test for issue #4408: the RustFS storage guard, run against the
# chart's real RustFS image (rustfs.image in values.yaml).
#
# RustFS exits fatally when its data dir has no free inodes, before its own
# lifecycle expiry can delete the Langfuse event uploads that filled it, so a
# full volume crash-loops forever. The chart's `storage-guard` init container
# runs files/rustfs-storage-guard.sh first. rustfs-storage-guard-assertions.sh
# proves the chart wires it; this script proves it WORKS, on an inode-limited
# tmpfs (`--tmpfs /data:size=64m,nr_inodes=4000,uid=10001,gid=10001`).
#
# The fixture is built inside the container and is never formatted by RustFS
# first: an unformatted full dir is what makes RustFS exit 1, while a dir it
# already formatted keeps serving /health on tmpfs, which would let the
# recovery check pass without the guard. Object dirs mimic RustFS's layout,
# `<bucket>/<key...>/xl.meta`, and some old objects also carry
# `<uuid>/part.1` (multi-file objects). "Old" means touched 3 days back.
#
#   A. Control (AC 3): inodes filled with old event objects (df -i IFree 0),
#      RustFS without the guard exits non-zero, and not by timeout.
#   B. Recovery (AC 4, 5): protected old files, 50 fresh event objects, the
#      edge-case objects below, then old event objects until full (< 10% free
#      inodes). The guard with EVENTS_DIR=/data/langfuse/events
#      RETENTION_MINUTES=2880 exits 0, the files left under events/ are
#      exactly the expected set (so no other old file remains and all 50
#      fresh objects remain), every old file under langfuse/media,
#      curie-workspaces, curie-bundles and .rustfs.sys survives, and RustFS
#      then starts on the same dir and answers GET /health within 60 s.
#      Edge cases, each asserted by exact path:
#        a. mixed-obj: old xl.meta, fresh <uuid>/part.1. Survives intact,
#           because one fresh file means the object changed.
#        b. nest: old nest/xl.meta with fresh nest/child/xl.meta. The child
#           and every ancestor dir survive; nest/xl.meta is kept too, since
#           the guard keeps every directory above a fresh file. And
#           fresh-parent/xl.meta fresh with fresh-parent/old-child/xl.meta
#           old: old-child is deleted, fresh-parent/xl.meta survives.
#        c. paths with spaces: "proj space/old obj 1" is deleted,
#           "proj space/fresh obj 1" is kept.
#   C. Retention 0 (AC 6): the same full fixture, RETENTION_MINUTES=0: exit 0,
#      nothing deleted.
#   D. Plenty free (AC 6): 200 old event objects on a mostly empty dir
#      (> 10% free inodes and bytes), RETENTION_MINUTES=2880: exit 0, nothing
#      deleted.
#   E. Byte pressure (AC 4): its own tmpfs with plenty of inodes
#      (size=16m,nr_inodes=20000). 20 fresh small event objects, small old
#      protected files under langfuse/media and curie-workspaces, then old
#      event objects each carrying a 1 MiB old part.1 until free bytes are
#      under 10% while free inodes stay well over 10%. The guard with
#      RETENTION_MINUTES=2880 exits 0, logs a non-zero freed count equal to
#      the old objects made, removes every old event object, keeps every
#      fresh object and protected file, and free bytes end over 10%. No
#      RustFS start: this scenario tests the guard's byte trigger only.
#
# DOCKER_HOST may be a remote engine, so nothing is bind-mounted: the guard
# and the fixture builder are fed through `docker exec -i ... sh -s` stdin.
# Every container is named with a per-run prefix and removed on exit.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
GUARD="$CHART/files/rustfs-storage-guard.sh"

IMAGE="$(python3 -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))["rustfs"]["image"])' "$CHART/values.yaml")"
[[ -n "$IMAGE" ]] || { echo "FAIL: could not read rustfs.image from values.yaml" >&2; exit 1; }

PREFIX="rustfs-storage-guard-ci-$$-"
EVENTS=/data/langfuse/events
RETENTION=2880
FRESH_COUNT=50
PLENTY_COUNT=200

TMP="$(mktemp -d)"
cleanup() {
  local ids
  ids="$(docker ps -aq --filter "name=^${PREFIX}" 2>/dev/null || true)"
  if [[ -n "$ids" ]]; then
    # shellcheck disable=SC2086
    docker rm -f $ids >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

start_container() {
  local name="${PREFIX}$1" tmpfs_opts="${2:-size=64m,nr_inodes=4000}"
  # Throwaway test-only keys; this RustFS never leaves the container.
  docker run -d --name "$name" \
    --tmpfs "/data:${tmpfs_opts},uid=10001,gid=10001" \
    -e RUSTFS_ACCESS_KEY=guardtest \
    -e RUSTFS_SECRET_KEY=guardtest-not-a-secret \
    --entrypoint sleep "$IMAGE" 900 >/dev/null \
    || fail "could not start container $name from $IMAGE"
  printf '%s\n' "$name"
}

# Fixture builder, run inside the container as the image's uid 10001.
FIXTURE="$TMP/fixture.sh"
cat > "$FIXTURE" <<'SH'
set -u
EV=/data/langfuse/events
OLD="$(date -d @$(( $(date +%s) - 259200 )) "+%Y-%m-%d %H:%M:%S")"

# One old event object. Returns non-zero when the inode table is exhausted.
mk_old() {
  n="$1"
  d="$EV/otel/proj-$(( n % 7 ))/obj-$n"
  mkdir -p "$d" 2>/dev/null || return 1
  if ! touch "$d/xl.meta" 2>/dev/null; then
    touch -d "$OLD" "$d" 2>/dev/null
    return 1
  fi
  touch -d "$OLD" "$d/xl.meta"
  if [ $(( n % 97 )) -eq 0 ]; then
    u="$d/4f1c2a9e-7b3d-4c11-9e2a-$(printf '%012d' "$n")"
    if ! mkdir "$u" 2>/dev/null || ! touch "$u/part.1" 2>/dev/null; then
      touch -d "$OLD" "$u" "$d" 2>/dev/null
      return 1
    fi
    touch -d "$OLD" "$u/part.1" "$u"
  fi
  touch -d "$OLD" "$d"
}

case "$1" in
  protected)
    for p in langfuse/media curie-workspaces curie-bundles; do
      for n in 1 2 3; do
        mkdir -p "/data/$p/$n" && touch "/data/$p/$n/xl.meta" && touch -d "$OLD" "/data/$p/$n/xl.meta" "/data/$p/$n" || exit 1
      done
    done
    mkdir -p /data/.rustfs.sys && touch /data/.rustfs.sys/old-marker && touch -d "$OLD" /data/.rustfs.sys/old-marker || exit 1
    ;;
  fresh)
    n=0
    while [ "$n" -lt "$2" ]; do
      mkdir -p "$EV/otel/proj-fresh/fresh-$n" && touch "$EV/otel/proj-fresh/fresh-$n/xl.meta" || exit 1
      n=$(( n + 1 ))
    done
    ;;
  old)
    n=0
    while [ "$n" -lt "$2" ]; do
      mk_old "$n" || exit 1
      n=$(( n + 1 ))
    done
    ;;
  old-fill)
    n=0
    while mk_old "$n"; do n=$(( n + 1 )); done
    echo "$n"
    ;;
  special)
    # Edge-case objects for scenario B; paths mirror SPECIAL_* in the runner.
    S="$EV/otel/proj-special"
    U=4f1c2a9e-7b3d-4c11-9e2a-aaaaaaaaaaaa
    mkdir -p "$S/mixed-obj/$U" "$S/nest/child" "$S/fresh-parent/old-child" \
      "$EV/otel/proj space/old obj 1" "$EV/otel/proj space/fresh obj 1" || exit 1
    touch "$S/mixed-obj/xl.meta" "$S/mixed-obj/$U/part.1" \
      "$S/nest/xl.meta" "$S/nest/child/xl.meta" \
      "$S/fresh-parent/xl.meta" "$S/fresh-parent/old-child/xl.meta" \
      "$EV/otel/proj space/old obj 1/xl.meta" "$EV/otel/proj space/fresh obj 1/xl.meta" || exit 1
    touch -d "$OLD" "$S/mixed-obj/xl.meta" "$S/nest/xl.meta" \
      "$S/fresh-parent/old-child/xl.meta" "$S/fresh-parent/old-child" \
      "$EV/otel/proj space/old obj 1/xl.meta" "$EV/otel/proj space/old obj 1" || exit 1
    ;;
  protected-small)
    for p in langfuse/media curie-workspaces; do
      for n in 1 2 3; do
        mkdir -p "/data/$p/$n" && touch "/data/$p/$n/xl.meta" && touch -d "$OLD" "/data/$p/$n/xl.meta" "/data/$p/$n" || exit 1
      done
    done
    ;;
  byte-fill)
    # Old event objects with a 1 MiB part.1 until free bytes are under 10%.
    # Each one is about 6% of a 16 MiB tmpfs, so 10% free always fits one.
    n=0
    while [ "$(df -P /data | awk 'NR==2 {print int($4 * 100 / $2)}')" -ge 10 ]; do
      d="$EV/otel/proj-big/big-$n"
      u="$d/4f1c2a9e-7b3d-4c11-9e2a-$(printf '%012d' "$n")"
      mkdir -p "$u" && touch "$d/xl.meta" \
        && dd if=/dev/zero of="$u/part.1" bs=1048576 count=1 2>/dev/null \
        && touch -d "$OLD" "$u/part.1" "$u" "$d/xl.meta" "$d" || exit 1
      n=$(( n + 1 ))
      [ "$n" -lt 100 ] || exit 1
    done
    echo "$n"
    ;;
  *)
    echo "unknown fixture mode $1" >&2
    exit 2
    ;;
esac
SH

fixture() { local c="$1"; shift; docker exec -i "$c" sh -s -- "$@" < "$FIXTURE"; }
in_c() { docker exec "$1" sh -c "$2"; }

count_old() { in_c "$1" "find $EVENTS -type f -mmin +$RETENTION | wc -l" | tr -d ' '; }
count_old_objects() { in_c "$1" "find $EVENTS -type f -name xl.meta -mmin +$RETENTION | wc -l" | tr -d ' '; }
count_fresh_objects() { in_c "$1" "find $EVENTS -type f -name xl.meta -mmin -$RETENTION | wc -l" | tr -d ' '; }
inode_free() { in_c "$1" "df -iP /data | awk 'NR==2 {print \$4}'"; }
inode_free_pct() { in_c "$1" "df -iP /data | awk 'NR==2 {print int(\$4 * 100 / \$2)}'"; }
byte_free_pct() { in_c "$1" "df -P /data | awk 'NR==2 {print int(\$4 * 100 / \$2)}'"; }

PROTECTED_LIST="/data/langfuse/media/1/xl.meta /data/langfuse/media/2/xl.meta /data/langfuse/media/3/xl.meta \
/data/curie-workspaces/1/xl.meta /data/curie-workspaces/2/xl.meta /data/curie-workspaces/3/xl.meta \
/data/curie-bundles/1/xl.meta /data/curie-bundles/2/xl.meta /data/curie-bundles/3/xl.meta \
/data/.rustfs.sys/old-marker"
missing_protected() { in_c "$1" "for f in $PROTECTED_LIST; do [ -f \"\$f\" ] || echo \"\$f\"; done"; }

# Scenario B edge cases (fixture mode "special"). Old xl.meta files: mixed-obj,
# nest, old-child, "old obj 1". Fresh xl.meta files: nest/child, fresh-parent,
# "fresh obj 1".
SPECIAL_OLD_OBJECTS=4
SPECIAL_FRESH_OBJECTS=3
SP="$EVENTS/otel/proj-special"
SPACE="$EVENTS/otel/proj space"
# Every file that must be under events/ after the guard in scenario B, and
# nothing else: the 50 fresh objects plus the edge-case survivors.
expected_b_files() {
  local n=0
  while [[ "$n" -lt "$FRESH_COUNT" ]]; do
    printf '%s\n' "$EVENTS/otel/proj-fresh/fresh-$n/xl.meta"
    n=$(( n + 1 ))
  done
  printf '%s\n' \
    "$SP/mixed-obj/xl.meta" \
    "$SP/mixed-obj/4f1c2a9e-7b3d-4c11-9e2a-aaaaaaaaaaaa/part.1" \
    "$SP/nest/xl.meta" \
    "$SP/nest/child/xl.meta" \
    "$SP/fresh-parent/xl.meta" \
    "$SPACE/fresh obj 1/xl.meta"
}
# Paths that must be gone after the guard in scenario B.
B_GONE=("$SP/fresh-parent/old-child" "$SPACE/old obj 1")
# Directories that must still exist: every ancestor of a fresh nested file.
B_DIRS=("$EVENTS/otel" "$SP" "$SP/nest" "$SP/nest/child" "$SP/mixed-obj" \
  "$SP/mixed-obj/4f1c2a9e-7b3d-4c11-9e2a-aaaaaaaaaaaa" "$SP/fresh-parent" \
  "$SPACE" "$SPACE/fresh obj 1")
c_exists() { docker exec "$1" test "$2" "$3"; }

# Protected files, 50 fresh event objects, then old event objects until the
# inode table is full. Prints the old object count.
full_fixture() {
  local c="$1" with_special="${2:-}" made
  fixture "$c" protected || fail "$c: could not create protected fixture files"
  fixture "$c" fresh "$FRESH_COUNT" || fail "$c: could not create $FRESH_COUNT fresh event objects"
  if [[ -n "$with_special" ]]; then
    fixture "$c" special || fail "$c: could not create the edge-case event objects"
  fi
  made="$(fixture "$c" old-fill)" || fail "$c: old-fill fixture failed"
  [[ "$made" -gt 100 ]] || fail "$c: only $made old event objects fit; fixture is not representative"
  printf '%s\n' "$made"
}

# Runs the guard exactly as the init container would, configured only by env.
run_guard() {
  local c="$1" minutes="$2"
  docker exec -i -e EVENTS_DIR="$EVENTS" -e RETENTION_MINUTES="$minutes" "$c" sh -s < "$GUARD"
}

require_guard() {
  [[ -f "$GUARD" ]] || fail "guard script $GUARD does not exist (scenarios B to D need it)"
}

echo "=== Image: $IMAGE ==="
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null || fail "could not pull $IMAGE"

echo "=== Scenario A (AC 3): RustFS without the guard on a full inode table ==="
A="$(start_container control)"
fixture "$A" old-fill >/dev/null || fail "A: old-fill fixture failed"
free="$(inode_free "$A")"
[[ "$free" == "0" ]] || fail "A: fixture left $free free inodes; the control needs a full inode table"
pass "A: inode table full (IFree 0) with $(count_old_objects "$A") old event objects"
set +e
A_OUT="$(docker exec "$A" timeout 45 /usr/bin/rustfs /data 2>&1)"
A_RC=$?
set -e
echo "  rustfs output tail:"
printf '%s\n' "$A_OUT" | tail -n 5 | sed 's/^/    /'
[[ "$A_RC" -ne 0 ]] || fail "A: RustFS exited 0 on a full inode table; the control no longer reproduces #4408"
[[ "$A_RC" -ne 124 && "$A_RC" -ne 143 ]] || fail "A: RustFS was still running at the 45 s timeout (rc $A_RC); it did not crash on the full table"
[[ "$A_RC" -lt 125 || "$A_RC" -gt 127 ]] || fail "A: docker exec could not run RustFS (rc $A_RC)"
pass "A: RustFS without the guard exits $A_RC on a full inode table"

echo "=== Scenario B (AC 4, 5): guard recovers a full volume, then RustFS serves ==="
require_guard
B="$(start_container recovery)"
B_MADE="$(full_fixture "$B" special)"
pct="$(inode_free_pct "$B")"
[[ "$pct" -lt 10 ]] || fail "B: fixture has $pct% free inodes; it must be under 10%"
b_old="$(count_old_objects "$B")"
b_fresh="$(count_fresh_objects "$B")"
[[ "$b_old" -eq $(( B_MADE + SPECIAL_OLD_OBJECTS )) ]] || fail "B: counted $b_old old event objects, fixture made $B_MADE plus $SPECIAL_OLD_OBJECTS edge cases"
[[ "$b_fresh" -eq $(( FRESH_COUNT + SPECIAL_FRESH_OBJECTS )) ]] || fail "B: counted $b_fresh fresh event objects before the guard, want $FRESH_COUNT plus $SPECIAL_FRESH_OBJECTS edge cases"
in_c "$B" "[ -f '$SP/fresh-parent/old-child/xl.meta' ] && [ -f '$SPACE/old obj 1/xl.meta' ]" \
  || fail "B: edge-case old objects missing before the guard"
pass "B: fixture has $b_old old and $b_fresh fresh event objects (edge cases included), $pct% inodes free"
set +e
B_OUT="$(run_guard "$B" "$RETENTION" 2>&1)"
B_RC=$?
set -e
echo "  guard output:"
printf '%s\n' "$B_OUT" | sed 's/^/    /'
[[ "$B_RC" -eq 0 ]] || fail "B: guard exited $B_RC, want 0"
pass "B: guard exited 0"
expected="$(expected_b_files | LC_ALL=C sort)"
actual="$(in_c "$B" "find $EVENTS -type f" | LC_ALL=C sort)"
if [[ "$actual" != "$expected" ]]; then
  echo "  diff (- expected, + actual):" >&2
  diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") | sed 's/^/    /' >&2 || true
  fail "B: files under $EVENTS after the guard are not exactly the expected set"
fi
left="$(count_old "$B")"
[[ "$left" -eq 2 ]] || fail "B: $left files older than $RETENTION minutes remain under $EVENTS, want only mixed-obj/xl.meta and nest/xl.meta"
pass "B: files under events/ are exactly the $FRESH_COUNT fresh objects plus the edge-case survivors; every other old object is gone"
for d in "${B_DIRS[@]}"; do
  c_exists "$B" -d "$d" || fail "B: directory '$d' was removed; it holds or is above a fresh file"
done
for g in "${B_GONE[@]}"; do
  ! c_exists "$B" -e "$g" || fail "B: expired object '$g' still exists"
done
pass "B: mixed-obj (old xl.meta, fresh part.1) intact; nest/child and its ancestors kept; fresh-parent/old-child deleted; spaced paths handled"
missing="$(missing_protected "$B")"
[[ -z "$missing" ]] || fail "B: guard deleted protected files outside events/: $missing"
pass "B: old files under langfuse/media, curie-workspaces, curie-bundles, .rustfs.sys survive"
docker exec -d "$B" sh -c '/usr/bin/rustfs /data >/tmp/rustfs.log 2>&1' || fail "B: could not start RustFS"
# shellcheck disable=SC2016 # expands inside the container, not here
if ! in_c "$B" 'deadline=$(( $(date +%s) + 60 )); while [ "$(date +%s)" -lt "$deadline" ]; do wget -q -T 2 -O /dev/null http://127.0.0.1:9000/health && exit 0; sleep 1; done; exit 1'; then
  echo "  rustfs log tail:" >&2
  in_c "$B" 'tail -n 20 /tmp/rustfs.log' 2>&1 | sed 's/^/    /' >&2 || true
  fail "B: RustFS did not answer GET /health within 60 s after the guard"
fi
pass "B: RustFS started on the recovered dir and answered GET /health"

echo "=== Scenario C (AC 6): retention 0 deletes nothing ==="
C="$(start_container retention0)"
C_MADE="$(full_fixture "$C")"
set +e
C_OUT="$(run_guard "$C" 0 2>&1)"
C_RC=$?
set -e
printf '%s\n' "$C_OUT" | sed 's/^/    /'
[[ "$C_RC" -eq 0 ]] || fail "C: guard exited $C_RC with RETENTION_MINUTES=0, want 0"
c_old="$(count_old_objects "$C")"
c_fresh="$(count_fresh_objects "$C")"
[[ "$c_old" -eq "$C_MADE" ]] || fail "C: RETENTION_MINUTES=0 left $c_old of $C_MADE old event objects; it must delete nothing"
[[ "$c_fresh" -eq "$FRESH_COUNT" ]] || fail "C: RETENTION_MINUTES=0 left $c_fresh of $FRESH_COUNT fresh event objects"
missing="$(missing_protected "$C")"
[[ -z "$missing" ]] || fail "C: guard deleted protected files: $missing"
pass "C: RETENTION_MINUTES=0 exits 0 and keeps all $C_MADE old and $FRESH_COUNT fresh objects"

echo "=== Scenario D (AC 6): plenty of free space deletes nothing ==="
D="$(start_container plenty)"
fixture "$D" old "$PLENTY_COUNT" || fail "D: could not create $PLENTY_COUNT old event objects"
ipct="$(inode_free_pct "$D")"
bpct="$(byte_free_pct "$D")"
[[ "$ipct" -gt 10 && "$bpct" -gt 10 ]] || fail "D: fixture has $ipct% free inodes and $bpct% free bytes; both must exceed 10%"
d_before="$(count_old_objects "$D")"
[[ "$d_before" -eq "$PLENTY_COUNT" ]] || fail "D: counted $d_before old event objects, want $PLENTY_COUNT"
set +e
D_OUT="$(run_guard "$D" "$RETENTION" 2>&1)"
D_RC=$?
set -e
printf '%s\n' "$D_OUT" | sed 's/^/    /'
[[ "$D_RC" -eq 0 ]] || fail "D: guard exited $D_RC, want 0"
d_after="$(count_old_objects "$D")"
[[ "$d_after" -eq "$PLENTY_COUNT" ]] || fail "D: guard deleted objects with $ipct% inodes and $bpct% bytes free ($d_after of $PLENTY_COUNT remain)"
pass "D: with $ipct% inodes and $bpct% bytes free the guard exits 0 and keeps all $PLENTY_COUNT objects"

echo "=== Scenario E (AC 4): byte pressure with plenty of inodes triggers deletion ==="
E_FRESH=20
E="$(start_container bytes size=16m,nr_inodes=20000)"
fixture "$E" fresh "$E_FRESH" || fail "E: could not create $E_FRESH fresh event objects"
fixture "$E" protected-small || fail "E: could not create protected fixture files"
E_MADE="$(fixture "$E" byte-fill)" || fail "E: byte-fill fixture failed"
[[ "$E_MADE" -ge 5 ]] || fail "E: only $E_MADE old 1 MiB objects fit; fixture is not representative"
ebpct="$(byte_free_pct "$E")"
eipct="$(inode_free_pct "$E")"
[[ "$ebpct" -lt 10 ]] || fail "E: fixture has $ebpct% free bytes; it must be under 10%"
[[ "$eipct" -gt 50 ]] || fail "E: fixture has $eipct% free inodes; it must be well over 10% so only bytes trigger"
e_old="$(count_old_objects "$E")"
[[ "$e_old" -eq "$E_MADE" ]] || fail "E: counted $e_old old event objects, fixture made $E_MADE"
pass "E: fixture has $E_MADE old 1 MiB event objects, $ebpct% bytes and $eipct% inodes free"
set +e
E_OUT="$(run_guard "$E" "$RETENTION" 2>&1)"
E_RC=$?
set -e
printf '%s\n' "$E_OUT" | sed 's/^/    /'
[[ "$E_RC" -eq 0 ]] || fail "E: guard exited $E_RC, want 0"
grep -Eq "freed $E_MADE expired event objects" <<<"$E_OUT" \
  || fail "E: guard log does not report freeing the $E_MADE expired objects under byte pressure"
e_left="$(count_old "$E")"
[[ "$e_left" -eq 0 ]] || fail "E: $e_left files older than $RETENTION minutes remain under $EVENTS"
! c_exists "$E" -e "$EVENTS/otel/proj-big" || fail "E: $EVENTS/otel/proj-big still exists after its objects expired"
e_fresh="$(count_fresh_objects "$E")"
[[ "$e_fresh" -eq "$E_FRESH" ]] || fail "E: $e_fresh fresh event objects remain, want $E_FRESH"
# shellcheck disable=SC2016 # expands inside the container, not here
e_missing="$(in_c "$E" 'for p in langfuse/media curie-workspaces; do for n in 1 2 3; do [ -f "/data/$p/$n/xl.meta" ] || echo "/data/$p/$n/xl.meta"; done; done')"
[[ -z "$e_missing" ]] || fail "E: guard deleted protected files: $e_missing"
ebpct_after="$(byte_free_pct "$E")"
[[ "$ebpct_after" -gt 10 ]] || fail "E: only $ebpct_after% bytes free after the guard, want over 10%"
pass "E: under byte pressure the guard freed all $E_MADE old objects, kept $E_FRESH fresh objects and protected files, bytes free now $ebpct_after%"

echo
echo "PASS: rustfs storage guard behavior (#4408): control crash, recovery to /health, edge-case objects, protected paths, retention 0, plenty free, byte pressure."
