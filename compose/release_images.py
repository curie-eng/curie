#!/usr/bin/env python3
"""Derive the curie-owned image set a generated compose.release.yaml requires.

The local-release ladder must provision every ghcr.io/curie-eng/curie-* image
the generated artifact names for the profiles it starts, not a hand-maintained
subset. #2005 closed by adding curie-ui; the next night failed on dispatcher.
This module is the shared consumer: parse the artifact, map each ref to a
local Dockerfile, and --check presence with `docker image inspect` only.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

CURIE_IMAGE_RE = re.compile(
    r"ghcr\.io/curie-eng/curie-[a-z-]+(?::[\w.-]+|@sha256:[0-9a-f]+)?"
)


class Recipe(NamedTuple):
    dockerfile: str
    context: str


# Local build recipes for curie-owned compose images. An image the generated
# artifact requires that is not in this map is a derivation miss: fail it, do
# not pull a mutable tag, and do not skip the service.
RECIPES = {
    "curie-api": Recipe("apps/api/Dockerfile", "."),
    "curie-worker": Recipe("apps/worker/Dockerfile", "."),
    "curie-worker-local": Recipe("compose/worker-local.Dockerfile", "compose"),
    "curie-ui": Recipe("apps/ui/Dockerfile", "."),
    "curie-dispatcher": Recipe("apps/dispatcher/Dockerfile", "."),
    "curie-runner": Recipe("runner/Dockerfile", "."),
}


def short_name(ref: str) -> str:
    name = ref.rsplit("/", 1)[-1]
    name = name.split("@", 1)[0]
    return name.split(":", 1)[0]


def recipe_for(ref: str) -> Recipe | None:
    return RECIPES.get(short_name(ref))


def _yaml():
    try:
        import yaml
    except ImportError:
        return None
    return yaml


def required_curie_images(compose_text: str, profiles: set[str]) -> set[str]:
    """Unique curie-owned image refs for services in any of `profiles`.

    A service with no `profiles` key is always started and is included. This
    matches `docker compose --profile ... config --images` for the generated
    release artifact, whose image refs are already literal (T3).
    """
    yaml = _yaml()
    if yaml is None:
        raise RuntimeError("PyYAML is required to parse compose text in-process")
    doc = yaml.safe_load(compose_text)
    services = doc.get("services") or {}
    images: set[str] = set()
    for svc in services.values():
        if not isinstance(svc, dict):
            continue
        svc_profiles = svc.get("profiles")
        if svc_profiles and not set(svc_profiles) & profiles:
            continue
        image = svc.get("image")
        if isinstance(image, str) and image.startswith("ghcr.io/curie-eng/curie-"):
            images.add(image)
    return images


_DOCKER_TAG_DEST_RE = re.compile(r"docker\s+tag\s+\S+\s+(\S+)")
_DOCKER_BUILD_TAG_RE = re.compile(r"docker\s+build\b[^\n]*?-t\s+(\S+)")


def job_curie_image_tags(job: dict) -> set[str]:
    """Image refs a GitHub Actions job builds or retags locally.

    Counts build-push-action `tags:` and `docker tag`/`docker build -t`
    destinations only. A `docker pull` or `docker image inspect` of the same
    ref is not provisioning: that is how a mutable-tag pull would still pass
    a substring scan (#2424).
    """
    tags: set[str] = set()
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        uses = str(step.get("uses") or "")
        with_ = step.get("with") or {}
        raw = with_.get("tags")
        if isinstance(raw, str) and "build-push-action" in uses:
            tags.update(CURIE_IMAGE_RE.findall(raw))
        run = step.get("run")
        if not isinstance(run, str):
            continue
        for dest in _DOCKER_TAG_DEST_RE.findall(run):
            if CURIE_IMAGE_RE.fullmatch(dest):
                tags.add(dest)
        for dest in _DOCKER_BUILD_TAG_RE.findall(run):
            if CURIE_IMAGE_RE.fullmatch(dest):
                tags.add(dest)
    return tags


def _docker_bin() -> str:
    return os.environ.get("CURIE_RELEASE_IMAGES_DOCKER") or "docker"


def compose_cli_images(compose_path: Path, profiles: set[str]) -> set[str]:
    """curie-owned refs `docker compose config --images` reports for `profiles`."""
    cmd = [_docker_bin(), "compose", "-f", str(compose_path)]
    for profile in sorted(profiles):
        cmd.extend(["--profile", profile])
    cmd.extend(["config", "--images"])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode or 1)
    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip().startswith("ghcr.io/curie-eng/curie-")
    }


def required_from_artifact(compose_path: Path, profiles: set[str]) -> set[str]:
    """Prefer parsing the artifact text; fall back to compose config --images.

    The ladder jobs do not install PyYAML. docker compose is already the
    local-release preflight's docker dependency, so it is the runtime source
    of truth when yaml is absent.
    """
    if _yaml() is not None:
        return required_curie_images(compose_path.read_text(), profiles)
    return compose_cli_images(compose_path, profiles)


def image_present(ref: str) -> bool:
    result = subprocess.run(
        [_docker_bin(), "image", "inspect", ref],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def check(compose_path: Path, profiles: set[str]) -> int:
    required = required_from_artifact(compose_path, profiles)
    profile_label = ",".join(sorted(profiles))
    errors: list[str] = []
    for ref in sorted(required):
        if recipe_for(ref) is None:
            errors.append(
                f"error: image '{ref}' is required by compose.release.yaml's "
                f"{profile_label} profiles and has no local build recipe.\n"
                "fix: add a Dockerfile mapping for this image in "
                "compose/release_images.py and a matching build+tag step in "
                "the local-release ladder jobs, then re-run."
            )
            continue
        if not image_present(ref):
            errors.append(
                f"error: image '{ref}' is required by compose.release.yaml's "
                f"{profile_label} profiles and is not present locally.\n"
                "fix: build and tag the missing image(s) locally under the tag "
                "compose.release.yaml pins (see .github/workflows/ci.yaml's "
                "e2e-ladder-release job for the exact build+tag steps CI uses), "
                "then re-run."
            )
    for err in errors:
        print(err, file=sys.stderr)
    return 1 if errors else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive and check curie-owned images from compose.release.yaml."
    )
    parser.add_argument("--compose", required=True, help="path to generated compose.release.yaml")
    parser.add_argument(
        "--profiles",
        required=True,
        help="comma-separated compose profiles (e.g. full,slack)",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--check",
        action="store_true",
        help="fail if a required image is missing locally or has no recipe",
    )
    mode.add_argument("--list", action="store_true", help="print required image refs")
    args = parser.parse_args()
    profiles = {part.strip() for part in args.profiles.split(",") if part.strip()}
    compose_path = Path(args.compose)
    if args.list:
        for ref in sorted(required_from_artifact(compose_path, profiles)):
            print(ref)
        return
    raise SystemExit(check(compose_path, profiles))


if __name__ == "__main__":
    main()
