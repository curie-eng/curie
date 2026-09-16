#!/bin/sh
# Move the Curie release to the newest published chart, and verify it landed.
#
# Runs as a Job. The bot can start this Job and nothing else -- it supplies no
# argument, so every choice below is the operator's, made when this file and its
# CronJob were installed.
#
# WHY A SCRIPT AND NOT A CONNECTOR TOOL. `helm upgrade` rewrites essentially
# every namespaced object the release owns, which is namespace-admin in every
# honest formulation. A connector holding that credential would hold it for the
# life of the pod; a Job holds it for the ninety seconds it runs. The bot never
# holds it at all: it creates a Job from a template it cannot edit.
#
# WHAT IT DELIBERATELY WILL NOT DO:
#   - take a target version from anywhere but the repository's own releases
#   - run when the newest release is what is already installed
#   - roll back on its own (see RECOVERY below)
set -eu

REPO="${PLATFORM_UPGRADE_REPO:?PLATFORM_UPGRADE_REPO is required}"
RELEASE="${PLATFORM_UPGRADE_RELEASE:?PLATFORM_UPGRADE_RELEASE is required}"
NAMESPACE="${PLATFORM_UPGRADE_NAMESPACE:?PLATFORM_UPGRADE_NAMESPACE is required}"
CHART_PATH="${PLATFORM_UPGRADE_CHART_PATH:-charts/curie}"
TIMEOUT="${PLATFORM_UPGRADE_TIMEOUT:-12m}"

echo "release=${RELEASE} namespace=${NAMESPACE} repo=${REPO}"

installed="$(helm list -n "${NAMESPACE}" -f "^${RELEASE}\$" -o json \
  | sed -n 's/.*"app_version":"\([^"]*\)".*/\1/p')"
if [ -z "${installed}" ]; then
  echo "no release ${RELEASE} in ${NAMESPACE}; refusing" >&2
  exit 1
fi

# The target is whatever the project published last. Read here rather than
# passed in: a version this Job accepted from outside would be a version the
# caller chose, and the caller is a language model.
tag="$(wget -qO- --header='Accept: application/vnd.github+json' \
  "https://api.github.com/repos/${REPO}/releases/latest" \
  | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
if [ -z "${tag}" ]; then
  echo "could not read the newest release of ${REPO}" >&2
  exit 1
fi
target="${tag#v}"

echo "installed=${installed} newest=${target}"
if [ "${installed}" = "${target}" ]; then
  echo "already on the newest release; nothing to do"
  exit 0
fi

# The chart ships in the repository rather than a chart registry, so the release
# tarball IS the distribution. Extracted under /tmp, which is the only writable
# path this Job has.
workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT
echo "fetching ${tag}"
wget -qO "${workdir}/src.tgz" \
  "https://github.com/${REPO}/archive/refs/tags/${tag}.tar.gz"
mkdir -p "${workdir}/src"
tar -xzf "${workdir}/src.tgz" -C "${workdir}/src" --strip-components=1
chart="${workdir}/src/${CHART_PATH}"
[ -f "${chart}/Chart.yaml" ] || { echo "no chart at ${CHART_PATH}" >&2; exit 1; }

# --reset-then-reuse-values, NOT --reuse-values. The plain form carries forward
# only what was previously set and silently drops values the new chart adds,
# which lands as a nil-pointer template error mid-upgrade or, worse, a rendered
# object missing a field nothing checks.
#
# Clearing the image tags is the load-bearing part: the chart renders its own
# appVersion when a tag is empty, so an install pinned to a git-sha tag would
# otherwise upgrade the CHART to the new version while still running the OLD
# images -- a state that reports as upgraded and is not.
echo "upgrading ${installed} -> ${target}"
helm upgrade "${RELEASE}" "${chart}" \
  -n "${NAMESPACE}" \
  --reset-then-reuse-values \
  --set api.image.tag= \
  --set worker.image.tag= \
  --set dispatcher.image.tag= \
  --set ui.image.tag= \
  --set agentSandbox.runner.tag= \
  --wait --timeout "${TIMEOUT}"

# Helm's own --wait covers rollout readiness. This re-reads the release because
# "the command exited 0" and "the release records the new version" are not the
# same statement, and the second is the one an operator will be told.
landed="$(helm list -n "${NAMESPACE}" -f "^${RELEASE}\$" -o json \
  | sed -n 's/.*"app_version":"\([^"]*\)".*/\1/p')"
if [ "${landed}" != "${target}" ]; then
  echo "upgrade reported success but the release records ${landed}" >&2
  exit 1
fi
echo "upgraded ${RELEASE} from ${installed} to ${landed}"

# RECOVERY IS NOT AUTOMATIC, AND SAYING SO IS THE POINT.
#
# `helm rollback` restores objects; it does not restore the database.
# Migrations run in the schema-migrate Job, not in every API pod (#2300).
# Expand-only patches keep application N-1 serving against the new schema.
# Contract/irreversible migrations are forward-only; recovery there is
# restore-from-backup. This Job does not roll back.
echo "note: rollback is an operator action; see docs/PERMISSION-MAP.md entry 4"
