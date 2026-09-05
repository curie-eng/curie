#!/usr/bin/env bash
#
# Issue #2319: `postgres.podSecurityContext.fsGroup` must be the gid of the
# `postgres` user INSIDE the pinned Postgres image. The chart shipped
# `fsGroup: 999` -- the Debian-variant uid/gid -- while running the Alpine
# build, where `postgres` is 70. That was harmless in practice (the entrypoint
# runs as root, chowns to `postgres`, chmods PGDATA 0700 and gosu-drops, so a
# stale supplemental group is silently retargeted); the reason to pin it anyway
# is truthfulness. The chart documented 999 as "read from the image" when it
# never was, and a value derived from an image becomes fiction the moment the
# image changes -- drift no render-only check can see.
#
# So this one RUNS the image: `id -g postgres` (and `id -u`) out of the exact
# bytes the chart pins, compared to the rendered fsGroup. That is only
# meaningful against an immutable reference, so the image must be digest-pinned:
# a moving tag would make yesterday's measured gid a claim about bytes nobody
# pulls today.
#
# Proves:
#   1. The default render's postgres container image is digest-pinned.
#   2. The rendered pod `securityContext.fsGroup` equals `id -g postgres` in
#      that image.
#   3. NEGATIVE: the same checker rejects a render whose fsGroup is mutated
#      back to the Debian 999.
#
# Needs docker. Runnable locally (from anywhere) and from CI. Fails loudly --
# it never skips.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail \
  "docker is required: this gate reads 'id -g postgres' out of the pinned image and cannot be satisfied by rendering alone"

RENDER="$TMP/postgres.yaml"
echo "=== Rendering chart (default values) ==="
helm template curie "$CHART" --show-only templates/postgres.yaml >"$RENDER" \
  || fail "postgres template render failed"

FSGROUP="$TMP/fsgroup.py"
cat >"$FSGROUP" <<'PY'
"""Postgres fsGroup contract, in two modes over one StatefulSet walker.

    <render>                          -> prints `<image>\t<fsGroup>`
    <render> <observed_gid> <uid>     -> asserts fsGroup == observed gid

Both modes go through load_postgres(), so the digest-pin guard is enforced on
the assert path (and therefore on the negative path) too.
"""

import pathlib
import sys

import yaml


def load_postgres(path):
    """Return (image, fsGroup) for the postgres StatefulSet in a render."""
    image = None
    fsgroup = None
    for document in yaml.safe_load_all(pathlib.Path(path).read_text()):
        if not document or document.get("kind") != "StatefulSet":
            continue
        pod = (document.get("spec") or {}).get("template", {}).get("spec") or {}
        container = next(
            (c for c in pod.get("containers") or [] if c.get("name") == "postgres"),
            None,
        )
        if container is None:
            continue
        image = (container.get("image") or "").strip()
        fsgroup = (pod.get("securityContext") or {}).get("fsGroup")

    if not image:
        raise SystemExit("no StatefulSet with a `postgres` container in the render")
    if fsgroup is None:
        raise SystemExit("postgres StatefulSet renders no pod securityContext.fsGroup")
    if "@sha256:" not in image:
        raise SystemExit(
            f"postgres image {image!r} must be digest-pinned: the fsGroup is "
            "measured from these exact bytes, and a moving reference makes that "
            "measurement a claim about an image nobody pulls (#2319)"
        )
    return image, fsgroup


args = sys.argv[1:]
if len(args) == 1:
    image, fsgroup = load_postgres(args[0])
    print(f"{image}\t{fsgroup}")
elif len(args) == 3:
    render_path, observed_gid, observed_uid = args
    image, fsgroup = load_postgres(render_path)
    if str(fsgroup) != str(observed_gid):
        raise SystemExit(
            f"postgres.podSecurityContext.fsGroup renders {fsgroup}, but "
            f"`id -g postgres` inside {image} is {observed_gid} (uid "
            f"{observed_uid}). That is undocumented drift, not necessarily a "
            "broken mount -- fsGroup is supposed to be measured from this "
            "image's own gid, not copied from somewhere else (#2319)."
        )
    print(
        f"  ok: fsGroup {fsgroup} == `id -g postgres` {observed_gid} "
        f"(uid {observed_uid}) in {image}"
    )
else:
    raise SystemExit("usage: fsgroup.py <render> [<observed_gid> <observed_uid>]")
PY

echo "=== Pinned postgres image and rendered fsGroup ==="
if ! extracted="$(python3 "$FSGROUP" "$RENDER" 2>&1)"; then
  fail "$extracted"
fi
POSTGRES_IMAGE="${extracted%%$'\t'*}"
RENDERED_FSGROUP="${extracted##*$'\t'}"
echo "  image:   $POSTGRES_IMAGE"
echo "  fsGroup: $RENDERED_FSGROUP"

echo "=== Reading the postgres user out of the pinned image ==="
DOCKER_STDERR="$TMP/docker-id.stderr"
if ! ids="$(docker run --rm --entrypoint sh "$POSTGRES_IMAGE" -c 'id -u postgres; id -g postgres' 2>"$DOCKER_STDERR")"; then
  fail "could not read the postgres user from $POSTGRES_IMAGE: $(cat "$DOCKER_STDERR")"
fi
OBSERVED_UID="$(printf '%s\n' "$ids" | sed -n 1p)"
OBSERVED_GID="$(printf '%s\n' "$ids" | sed -n 2p)"
[[ "$OBSERVED_UID" =~ ^[0-9]+$ ]] \
  || fail "unexpected 'id -u postgres' output from $POSTGRES_IMAGE: ${OBSERVED_UID:-<empty>} (stderr: $(cat "$DOCKER_STDERR"))"
[[ "$OBSERVED_GID" =~ ^[0-9]+$ ]] \
  || fail "unexpected 'id -g postgres' output from $POSTGRES_IMAGE: ${OBSERVED_GID:-<empty>} (stderr: $(cat "$DOCKER_STDERR"))"
echo "  id -u postgres: $OBSERVED_UID"
echo "  id -g postgres: $OBSERVED_GID"

if ! checked="$(python3 "$FSGROUP" "$RENDER" "$OBSERVED_GID" "$OBSERVED_UID" 2>&1)"; then
  fail "$checked"
fi
echo "$checked"

echo "=== Negative: the Debian 999 fsGroup is rejected ==="
MUTANT="$TMP/postgres-999.yaml"
python3 - "$RENDER" "$MUTANT" <<'PY'
import pathlib
import sys

import yaml

src, dest = sys.argv[1:]
documents = list(yaml.safe_load_all(pathlib.Path(src).read_text()))
mutated = False
for document in documents:
    if not document or document.get("kind") != "StatefulSet":
        continue
    pod = (document.get("spec") or {}).get("template", {}).get("spec") or {}
    if not any(c.get("name") == "postgres" for c in pod.get("containers") or []):
        continue
    pod.setdefault("securityContext", {})["fsGroup"] = 999
    mutated = True
if not mutated:
    raise SystemExit("could not find the postgres StatefulSet to mutate")
with pathlib.Path(dest).open("w") as handle:
    yaml.safe_dump_all(documents, handle)
PY

negative_out=""
if negative_out="$(python3 "$FSGROUP" "$MUTANT" "$OBSERVED_GID" "$OBSERVED_UID" 2>&1)"; then
  fail "fsGroup 999 mutation passed the postgres fsGroup contract"
fi
if [[ "$negative_out" != *"fsGroup renders 999"* ]]; then
  fail "fsGroup 999 mutation failed unexpectedly: $negative_out"
fi
echo "  ok: fsGroup 999 against a gid of $OBSERVED_GID is rejected"

echo
echo "PASS: postgres.podSecurityContext.fsGroup ($RENDERED_FSGROUP) is the gid of the postgres user inside the digest-pinned image, and a 999 mutation is rejected"
