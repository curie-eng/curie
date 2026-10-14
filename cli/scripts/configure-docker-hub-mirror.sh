#!/usr/bin/env bash
# Point the Docker daemon at Google's public Docker Hub pull-through cache.
# https://cloud.google.com/artifact-registry/docs/pull-cached-dockerhub-images
# The daemon falls back to Docker Hub when the cache misses. BuildKit does not
# read this file; workflow setup-buildx steps carry the same host inline.
set -euo pipefail

render=0
daemon="/etc/docker/daemon.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --render)
      render=1
      shift
      ;;
    --daemon-json)
      daemon="${2:?--daemon-json needs a path}"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "$render" -eq 0 && "${CI:-}" != "true" ]]; then
  echo "refusing to restart Docker outside CI" >&2
  exit 2
fi

merged="$(
  python3 - "$daemon" <<'PY'
import json
import sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
except FileNotFoundError:
    data = {}
if not isinstance(data, dict):
    raise SystemExit("daemon.json must be a JSON object")
mirrors = data.get("registry-mirrors")
if mirrors is None:
    mirrors = []
if not isinstance(mirrors, list):
    raise SystemExit("registry-mirrors must be a list")
mirror = "https://mirror.gcr.io"
if mirror not in mirrors:
    mirrors.append(mirror)
data["registry-mirrors"] = mirrors
json.dump(data, sys.stdout)
sys.stdout.write("\n")
PY
)"

if [[ "$render" -eq 1 ]]; then
  printf '%s' "$merged"
  exit 0
fi

if [[ -w "$(dirname "$daemon")" ]]; then
  printf '%s' "$merged" >"$daemon"
else
  printf '%s' "$merged" | sudo tee "$daemon" >/dev/null
fi

if command -v systemctl >/dev/null 2>&1; then
  sudo systemctl restart docker
else
  sudo service docker restart
fi
