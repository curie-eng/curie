#!/usr/bin/env bash
#
# Issue #2318: every image the chart names must resolve to the same bytes for
# the life of a chart version. A values *Image key or a rendered container
# image with no tag, or with tag `latest`, pulls whatever that repository's
# :latest is that day and (for :latest / untagged) defaults imagePullPolicy
# to Always. The security probe's netshoot image was the first miss: a bare
# `nicolaka/netshoot` became the body of four helm-test pods.
#
# Issue #2319 widens that from "has a tag" to "cannot move". A major/minor
# alias like `postgres:16-alpine`, `valkey/valkey:8-alpine`, `busybox:1.36` or
# `clickhouse/clickhouse-server:25.12` is republished upstream: it is a tag, so
# #2318 passed it, but the bytes behind it change without any chart edit. The
# data tier is where that bites hardest -- a moved Postgres build changes the
# uid/gid that owns the mounted volume.
#
# The pin rule enforced here. An image reference is pinned when
#   * it carries `@sha256:<64 hex>`, OR
#   * its tag, after an optional leading `v`, starts with at least three
#     dot-separated numeric components.
# So `1.36.1`, `8.1.10-alpine`, `25.12.11.4`, `1.0.0-beta.12`, `v0.5.0` and
# `0.8.5` pass; `1.36`, `16-alpine`, `8-alpine`, `25.12`, `v0.16`, `latest` and
# a bare repository fail. An upstream whose own version has fewer than three
# components (Postgres 16.15, netshoot v0.16) therefore gets a digest.
#
# Scope: the default chart render, every values *Image key, and every
# third-party `services.*.image` in compose.dev.yaml. First-party
# `ghcr.io/curie-eng/curie-*` images in compose.dev.yaml are exempt --
# compose/generate_release_compose.py pins them to the release version.
# Langfuse images are covered by langfuse-image-pin-assertions.sh; they are not
# special-cased here.
#
# Proves:
#   1. DEFAULT values, the default helm template and compose.dev.yaml have no
#      untagged, :latest, or major/minor-alias image references.
#   2. NEGATIVE: an untagged *Image value is refused.
#   3. NEGATIVE: a :latest *Image value is refused.
#   4. NEGATIVE: a rendered container image mutated to drop its tag, or to
#      :latest, is refused.
#   5. NEGATIVE: a rendered container image mutated back to a major/minor
#      alias is refused, and so is an alias *Image value.
#   6. NEGATIVE: a compose service mutated back to a major/minor alias is
#      refused.
#   7. NEGATIVE: stripping the digest from values postgres.image is refused,
#      in every values file (default plus every values-*.yaml overlay), because
#      postgres.podSecurityContext.fsGroup is the gid of the `postgres` user
#      read out of those exact bytes -- charts/curie/ci/
#      postgres-fsgroup-assertions.sh asserts that against the running image,
#      but only for the default render, so this script is the only one that
#      would catch an overlay's postgres.image losing its digest.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
COMPOSE="$REPO_ROOT/compose.dev.yaml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

[[ -f "$COMPOSE" ]] || fail "compose.dev.yaml not found at $COMPOSE (run this from a repo checkout)"

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import pathlib
import re
import sys

import yaml

UNPINNED_PREFIX = "unpinned image:"

# A tag pins only when it starts with three dot-separated numeric components
# (an optional leading `v` allowed): `1.36.1`, `v0.5.0`, `1.0.0-beta.12`. A
# major/minor alias such as `16-alpine`, `25.12` or `v0.16` is republished
# upstream and needs a digest instead (#2319).
FULL_VERSION_RE = re.compile(r"^v?[0-9]+\.[0-9]+\.[0-9]+")

# Images the release generator pins to the release version, so compose.dev.yaml
# deliberately carries a floating tag for them (compose/generate_release_compose.py).
FIRST_PARTY_PREFIX = "ghcr.io/curie-eng/curie-"


def last_name_component(image):
    # Registry hosts may carry a port (`localhost:5000/foo`), so the tag lives
    # on the last path component, not the first colon in the string.
    return image.split("/")[-1]


def unpinned_reason(image):
    image = (image or "").strip()
    if not image:
        return None
    if "@" in image:
        name, digest = image.rsplit("@", 1)
        if digest.startswith("sha256:") and digest[len("sha256:") :]:
            return None
        return "malformed digest"
    name = last_name_component(image)
    if ":" not in name:
        return "no tag"
    tag = name.rsplit(":", 1)[1]
    if tag == "":
        return "empty tag"
    if tag == "latest":
        return "tag is latest"
    if not FULL_VERSION_RE.match(tag):
        return "tag is a major/minor alias (need a digest or a full version)"
    return None


def walk_image_keys(obj, path, found):
    if isinstance(obj, dict):
        for key, value in obj.items():
            child = f"{path}.{key}" if path else str(key)
            if key.endswith("Image") and isinstance(value, str) and value.strip():
                found.append((child, value.strip()))
            walk_image_keys(value, child, found)
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            walk_image_keys(value, f"{path}[{index}]", found)


def is_container_like(mapping):
    if not isinstance(mapping, dict):
        return False
    if not isinstance(mapping.get("image"), str):
        return False
    return any(
        field in mapping
        for field in (
            "name",
            "command",
            "args",
            "env",
            "ports",
            "resources",
            "securityContext",
            "workingDir",
            "volumeMounts",
        )
    )


def walk_container_images(obj, path, found):
    if is_container_like(obj):
        image = obj["image"].strip()
        if image:
            name = obj.get("name") or path
            found.append((f"{path} ({name})", image))
    if isinstance(obj, dict):
        for key, value in obj.items():
            child = f"{path}.{key}" if path else str(key)
            walk_container_images(value, child, found)
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            walk_container_images(value, f"{path}[{index}]", found)


def collect_values_images(values_paths):
    found = []
    for values_path in values_paths:
        data = yaml.safe_load(pathlib.Path(values_path).read_text()) or {}
        walk_image_keys(data, pathlib.Path(values_path).name, found)
    return found


def collect_render_images(render_path):
    found = []
    documents = yaml.safe_load_all(pathlib.Path(render_path).read_text())
    for document in documents:
        if not document:
            continue
        kind = document.get("kind") or "unknown"
        name = (document.get("metadata") or {}).get("name") or "unnamed"
        walk_container_images(document, f"{kind}/{name}", found)
    return found


def collect_compose_images(compose_path):
    found = []
    data = yaml.safe_load(pathlib.Path(compose_path).read_text()) or {}
    label = pathlib.Path(compose_path).name
    for name, service in (data.get("services") or {}).items():
        if not isinstance(service, dict):
            continue
        image = (service.get("image") or "").strip()
        if not image or FIRST_PARTY_PREFIX in image:
            continue
        found.append((f"{label} services.{name}", image))
    return found


def problems_for(refs):
    problems = []
    for where, image in refs:
        reason = unpinned_reason(image)
        if reason:
            problems.append(f"{UNPINNED_PREFIX} {image} ({where}: {reason})")
    return problems


def postgres_digest_problems(values_paths):
    # Coupling, not style: postgres.podSecurityContext.fsGroup is the gid of the
    # `postgres` user inside these exact bytes (charts/curie/ci/
    # postgres-fsgroup-assertions.sh reads it out of the running image), so a
    # reference that can move silently changes who owns the data volume. This
    # checks every values file directly -- an overlay's postgres.image never
    # reaches collect_render_images, since the default render never applies it.
    problems = []
    for values_path in values_paths:
        data = yaml.safe_load(pathlib.Path(values_path).read_text()) or {}
        image = ((data.get("postgres") or {}).get("image") or "").strip()
        if not image:
            continue
        if "@sha256:" not in image:
            problems.append(
                f"{pathlib.Path(values_path).name} postgres.image must be "
                f"digest-pinned; found {image!r}. postgres.podSecurityContext."
                "fsGroup is the gid read out of these exact bytes, so a "
                "reference that can move upstream silently changes the owner "
                "of the mounted data volume."
            )
    return problems


def main(argv):
    if len(argv) < 4:
        raise SystemExit(
            "usage: check.py <render.yaml> <compose.yaml> <values.yaml> "
            "[more values files...]"
        )
    render_path = argv[1]
    compose_path = argv[2]
    values_paths = argv[3:]
    problems = problems_for(collect_values_images(values_paths))
    problems.extend(problems_for(collect_render_images(render_path)))
    problems.extend(problems_for(collect_compose_images(compose_path)))
    problems.extend(postgres_digest_problems(values_paths))
    if problems:
        raise SystemExit("\n".join(problems))
    print(
        "ok: no untagged, :latest or major/minor-alias *Image values, rendered "
        "container images, or compose service images"
    )


if __name__ == "__main__":
    main(sys.argv)
PY

# Mutator 1: rewrite the image of one rendered container, found by its exact
# current reference. Used for the no-tag / :latest / alias render negatives.
MUTATE_RENDER="$TMP/mutate-render.py"
cat >"$MUTATE_RENDER" <<'PY'
import pathlib
import sys

import yaml

src, dest, needle, replacement = sys.argv[1:]
documents = list(yaml.safe_load_all(pathlib.Path(src).read_text()))
mutated = False
for document in documents:
    if not document:
        continue
    spec = (document.get("spec") or {}).get("template", {}).get("spec") or {}
    for container in spec.get("containers") or []:
        if (container.get("image") or "") == needle:
            container["image"] = replacement
            mutated = True
            break
    if mutated:
        break
if not mutated:
    raise SystemExit(f"could not find a rendered {needle} container to mutate")
with pathlib.Path(dest).open("w") as handle:
    yaml.safe_dump_all(documents, handle)
PY

# Mutator 2: one line-anchored regex substitution against a source YAML file
# (values.yaml or compose.dev.yaml). Exactly one match is required -- a regex
# that stops matching would otherwise turn a negative into a silent no-op that
# still "fails" the checker for the wrong file.
MUTATE_TEXT="$TMP/mutate-text.py"
cat >"$MUTATE_TEXT" <<'PY'
import pathlib
import re
import sys

src, dest, pattern, replacement = sys.argv[1:]
text = pathlib.Path(src).read_text()
updated, count = re.subn(pattern, replacement, text, count=1, flags=re.M)
if count != 1:
    raise SystemExit(
        f"expected exactly one match of {pattern!r} in {src}, found {count}"
    )
pathlib.Path(dest).write_text(updated)
PY

# assert_rejected <ok-message> <expected-substring>... -- <checker args...>
#
# Runs the checker over a mutated tree, fails loudly if it PASSES, fails if the
# rejection does not mention every expected substring (so a negative cannot be
# satisfied by an unrelated problem), and otherwise prints the ok: line.
assert_rejected() {
  local label="$1"
  shift
  local -a expects=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    expects+=("$1")
    shift
  done
  [[ "${1:-}" == "--" ]] || fail "assert_rejected($label): missing -- separator before the checker args"
  shift
  [[ $# -gt 0 ]] || fail "assert_rejected($label): no checker args after --"

  local out=""
  if out="$(python3 "$CHECKER" "$@" 2>&1)"; then
    fail "expected a rejection but the contract passed -- $label"
  fi
  local expect
  for expect in "${expects[@]}"; do
    if [[ "$out" != *"$expect"* ]]; then
      fail "rejected for the wrong reason ($label): expected $expect in: $out"
    fi
  done
  echo "  ok: $label"
}

VALUES_FILES=("$CHART/values.yaml")
for overlay in "$CHART"/values-*.yaml; do
  if [[ -f "$overlay" ]]; then
    VALUES_FILES+=("$overlay")
  fi
done

RENDER="$TMP/chart.yaml"
echo "=== Rendering chart (default values) ==="
helm template curie "$CHART" >"$RENDER"

echo "=== Default values *Image keys, rendered container images, compose images ==="
if ! default_out="$(python3 "$CHECKER" "$RENDER" "$COMPOSE" "${VALUES_FILES[@]}" 2>&1)"; then
  fail "default chart or compose carries an unpinned image: $default_out"
fi
echo "  $default_out"

echo "=== Negative: untagged *Image value is refused ==="
UNTAGGED_VALUES="$TMP/values-untagged.yaml"
python3 "$MUTATE_TEXT" "$CHART/values.yaml" "$UNTAGGED_VALUES" \
  '^(\s*netshootImage:\s*)\S+\s*$' '\1nicolaka/netshoot'
assert_rejected "untagged netshootImage is rejected" \
  "unpinned image: nicolaka/netshoot (" ": no tag)" \
  -- "$RENDER" "$COMPOSE" "$UNTAGGED_VALUES"

echo "=== Negative: :latest *Image value is refused ==="
LATEST_VALUES="$TMP/values-latest.yaml"
python3 "$MUTATE_TEXT" "$CHART/values.yaml" "$LATEST_VALUES" \
  '^(\s*netshootImage:\s*)\S+\s*$' '\1nicolaka/netshoot:latest'
assert_rejected "netshootImage:latest is rejected" \
  "unpinned image: nicolaka/netshoot:latest" \
  -- "$RENDER" "$COMPOSE" "$LATEST_VALUES"

echo "=== Negative: major/minor-alias *Image value is refused ==="
ALIAS_VALUES="$TMP/values-alias.yaml"
python3 "$MUTATE_TEXT" "$CHART/values.yaml" "$ALIAS_VALUES" \
  '^(\s*extractImage:\s*)\S+\s*$' '\1busybox:1.36'
assert_rejected "extractImage on a busybox:1.36 alias is rejected" \
  "unpinned image: busybox:1.36 (" "major/minor alias" \
  -- "$RENDER" "$COMPOSE" "$ALIAS_VALUES"

echo "=== Negative: rendered container image with no tag is refused ==="
NOTAG_RENDER="$TMP/chart-notag.yaml"
python3 "$MUTATE_RENDER" "$RENDER" "$NOTAG_RENDER" "busybox:1.36.1" "busybox"
assert_rejected "rendered busybox with no tag is rejected" \
  "unpinned image: busybox (" ": no tag)" \
  -- "$NOTAG_RENDER" "$COMPOSE" "$CHART/values.yaml"

echo "=== Negative: rendered :latest container image is refused ==="
LATEST_RENDER="$TMP/chart-latest.yaml"
python3 "$MUTATE_RENDER" "$RENDER" "$LATEST_RENDER" "busybox:1.36.1" "busybox:latest"
assert_rejected "rendered busybox:latest is rejected" \
  "unpinned image: busybox:latest" \
  -- "$LATEST_RENDER" "$COMPOSE" "$CHART/values.yaml"

echo "=== Negative: rendered major/minor-alias container image is refused ==="
ALIAS_RENDER="$TMP/chart-alias.yaml"
python3 "$MUTATE_RENDER" "$RENDER" "$ALIAS_RENDER" "busybox:1.36.1" "busybox:1.36"
assert_rejected "rendered busybox:1.36 alias is rejected" \
  "unpinned image: busybox:1.36 (" "major/minor alias" \
  -- "$ALIAS_RENDER" "$COMPOSE" "$CHART/values.yaml"

echo "=== Negative: compose service on a major/minor alias is refused ==="
ALIAS_COMPOSE="$TMP/compose-alias.yaml"
python3 "$MUTATE_TEXT" "$COMPOSE" "$ALIAS_COMPOSE" \
  '^(\s*image:\s*)postgres:\S+\s*$' '\1postgres:16-alpine'
assert_rejected "compose.dev.yaml postgres:16-alpine is rejected" \
  "unpinned image: postgres:16-alpine (compose" "services.postgres" \
  -- "$RENDER" "$ALIAS_COMPOSE" "$CHART/values.yaml"

echo "=== Negative: values postgres.image without a digest is refused ==="
# postgres.image is a plain `image` key, so the values walk (which selects
# `*Image` keys) never sees it. postgres_digest_problems checks it directly
# against every values file instead, on the coupling grounds
# (postgres.podSecurityContext.fsGroup is read out of these exact bytes) --
# so this asserts on that digest-coupling message, not the render.
NODIGEST_VALUES="$TMP/values-postgres-nodigest.yaml"
python3 "$MUTATE_TEXT" "$CHART/values.yaml" "$NODIGEST_VALUES" \
  '^(\s*image:\s*postgres:[^@\s]+)@sha256:[0-9a-f]+\s*$' '\1'
assert_rejected "values postgres.image without a digest is rejected" \
  "postgres.image must be digest-pinned" "postgres.podSecurityContext.fsGroup" \
  -- "$RENDER" "$COMPOSE" "$NODIGEST_VALUES"

echo
echo "PASS: values *Image keys, rendered container images and compose.dev.yaml service images are pinned to a digest or a full version; untagged, :latest, major/minor-alias and digest-stripped postgres.image mutations are rejected"
