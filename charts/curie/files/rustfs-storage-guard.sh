#!/bin/sh
# RustFS storage guard (#4408), run by the `storage-guard` init container
# before RustFS starts. RustFS exits fatally when its volume has no free inodes
# or bytes, before its own lifecycle expiry can delete the Langfuse event
# uploads that filled it, so a full volume crash-loops forever. When free
# inodes or bytes are under 10%, this deletes every event object under
# EVENTS_DIR none of whose files changed within RETENTION_MINUTES: the same set
# the bucket's lifecycle rule expires (#2870). It never touches anything
# outside EVENTS_DIR. The chart sets both variables; RETENTION_MINUTES 0 means
# retention is disabled and nothing is deleted.
set -eu

EVENTS_DIR="${EVENTS_DIR:?EVENTS_DIR is required}"
RETENTION_MINUTES="${RETENTION_MINUTES:?RETENTION_MINUTES is required}"
MIN_FREE_PERCENT=10

log() { echo "storage-guard: $*"; }

if [ "$RETENTION_MINUTES" -le 0 ]; then
  log "retention disabled, nothing to free"
  exit 0
fi
if [ ! -d "$EVENTS_DIR" ]; then
  log "$EVENTS_DIR does not exist, nothing to free"
  exit 0
fi

# Prints "<free> <free %>" for one df mode. A total of 0 means the filesystem
# does not report that figure (inodes on some filesystems): report 100%.
df_free() {
  df "$1" "$EVENTS_DIR" | awk 'NR == 2 { t = $(NF-4); a = $(NF-2); printf "%d %d", a, (t > 0 ? int(a * 100 / t) : 100) }'
}
# Prints "<free inodes> <free inode %> <free KiB> <free byte %>".
usage() {
  echo "$(df_free -iP) $(df_free -P)"
}

# shellcheck disable=SC2046 # word splitting of the four numbers is intended
set -- $(usage)
before="free inodes $1 ($2%), free KiB $3 ($4%)"
if [ "$2" -ge "$MIN_FREE_PERCENT" ] && [ "$4" -ge "$MIN_FREE_PERCENT" ]; then
  log "$before, nothing to free"
  exit 0
fi
log "$before, under $MIN_FREE_PERCENT%: deleting event objects unchanged for $RETENTION_MINUTES minutes under $EVENTS_DIR"

# An object directory directly holds xl.meta (large objects add <uuid>/part.N).
# Fresh files are listed first, and every directory above one is kept; then
# each object directory not kept is printed. Nothing is written to disk, so
# this runs on a volume with no free inodes.
expired="$(
  {
    find "$EVENTS_DIR" -type f -mmin -"$RETENTION_MINUTES" | sed 's/^/F /'
    find "$EVENTS_DIR" -type f -name xl.meta | sed 's/^/O /'
  } | awk '
    /^F / { p = substr($0, 3); while (sub(/\/[^\/]*$/, "", p) && p != "" && !(p in keep)) keep[p] = 1; next }
    /^O / { d = substr($0, 3); sub(/\/xl\.meta$/, "", d); if (!(d in keep)) print d }
  '
)"

freed=0
if [ -n "$expired" ]; then
  freed="$(printf '%s\n' "$expired" | wc -l | tr -d ' ')"
  printf '%s\n' "$expired" | tr '\n' '\0' | xargs -0 -r rm -rf -- \
    || log "some expired event objects could not be removed"
  # Parent directories the deletes left empty. Every ancestor of a deleted
  # object strictly below EVENTS_DIR is listed once, deepest first, and handed
  # to rmdir, which removes only the empty ones. Other empty directories under
  # EVENTS_DIR, and EVENTS_DIR itself, are never listed.
  printf '%s\n' "$expired" | awk '
    BEGIN { base = ENVIRON["EVENTS_DIR"]; sub(/\/$/, "", base); base = base "/" }
    { p = $0; while (sub(/\/[^\/]*$/, "", p) && index(p "/", base) == 1 && p "/" != base && !(p in seen)) { seen[p] = 1; print p } }
  ' | sort -r | tr '\n' '\0' | xargs -0 -r rmdir 2>/dev/null || true
fi

# shellcheck disable=SC2046
set -- $(usage)
log "freed $freed expired event objects; before: $before; after: free inodes $1 ($2%), free KiB $3 ($4%)"
exit 0
