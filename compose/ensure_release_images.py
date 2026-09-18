#!/usr/bin/env python3
"""Derive and optionally build the curie images a release-compose profile needs.

The local-release ladder used to grow a hardcoded image list in CI and the
nightly workflow, one missing GHCR ref at a time (#2005, #2245). This helper
is the single derivation: parse the generated compose.release.yaml, take the
profiles the rung will start, then add the two images the rung still needs
even when they are not compose services (the runner the worker spawns, and
the dispatcher `local message` runs as a one-shot).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATE = Path(__file__).resolve().parent / "generate_release_compose.py"

CURIE_IMAGE_PREFIX = "ghcr.io/curie-eng/"


@dataclass(frozen=True)
class ImageBuild:
    dockerfile: str
    context: str
    tags: tuple[str, ...]
    build_args: tuple[tuple[str, str], ...] = ()
    extra_prereq_tags: tuple[str, ...] = ()


IMAGE_BUILDS: dict[str, ImageBuild] = {
    "curie-api": ImageBuild(
        dockerfile="apps/api/Dockerfile",
        context=".",
        tags=(
            "ghcr.io/curie-eng/curie-api:ci-local",
            "ghcr.io/curie-eng/curie-api:latest",
        ),
    ),
    "curie-worker": ImageBuild(
        dockerfile="apps/worker/Dockerfile",
        context=".",
        tags=("ghcr.io/curie-eng/curie-worker:ci-local",),
    ),
    "curie-worker-local": ImageBuild(
        dockerfile="compose/worker-local.Dockerfile",
        context="compose",
        tags=("ghcr.io/curie-eng/curie-worker-local:latest",),
        build_args=(("BASE_TAG", "ci-local"),),
        extra_prereq_tags=("ghcr.io/curie-eng/curie-worker:ci-local",),
    ),
    "curie-ui": ImageBuild(
        dockerfile="apps/ui/Dockerfile",
        context=".",
        tags=("ghcr.io/curie-eng/curie-ui:latest",),
    ),
    "curie-dispatcher": ImageBuild(
        dockerfile="apps/dispatcher/Dockerfile",
        context=".",
        tags=("ghcr.io/curie-eng/curie-dispatcher:latest",),
    ),
    "curie-runner": ImageBuild(
        dockerfile="runner/Dockerfile",
        context=".",
        tags=("curie-runner", "ghcr.io/curie-eng/curie-runner:latest"),
    ),
    "curie-mail-adapter": ImageBuild(
        dockerfile="apps/mail-adapter/Dockerfile",
        context=".",
        tags=("ghcr.io/curie-eng/curie-mail-adapter:latest",),
    ),
}


def image_short_name(image: str) -> str:
    ref = image.split("/")[-1]
    return ref.split(":")[0]


def _service_profiles(service: dict[str, object]) -> set[str]:
    raw = service.get("profiles")
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {str(item) for item in raw}
    return set()


def _compose_services(doc: object) -> dict[str, object]:
    if not isinstance(doc, dict):
        raise SystemExit("error: resolved compose config is not an object")
    services = doc.get("services") or {}
    if not isinstance(services, dict):
        raise SystemExit("error: resolved compose config services is not an object")
    return services


def _curie_images_for_profiles(
    services: dict[str, object], profiles: Sequence[str]
) -> list[str]:
    wanted = set(profiles)
    images: list[str] = []
    seen: set[str] = set()
    for service in services.values():
        if not isinstance(service, dict):
            continue
        service_profiles = _service_profiles(service)
        if service_profiles and service_profiles.isdisjoint(wanted):
            continue
        image = service.get("image")
        if not isinstance(image, str) or not image.startswith(CURIE_IMAGE_PREFIX):
            continue
        if image not in seen:
            seen.add(image)
            images.append(image)
    return images


def _required_service_image(
    services: dict[str, object], service_name: str
) -> str:
    path = f"services.{service_name}.image"
    service = services.get(service_name)
    if not isinstance(service, dict):
        raise SystemExit(f"error: resolved compose config is missing {path}")
    image = service.get("image")
    if not isinstance(image, str) or not image.strip():
        raise SystemExit(f"error: resolved compose config is missing {path}")
    return image


def _required_worker_runner_image(services: dict[str, object]) -> str:
    path = "services.curie-worker.environment.CURIE_RUNNER_IMAGE"
    worker = services.get("curie-worker")
    if not isinstance(worker, dict):
        raise SystemExit(f"error: resolved compose config is missing {path}")
    environment = worker.get("environment")
    image: object | None = None
    if isinstance(environment, dict):
        image = environment.get("CURIE_RUNNER_IMAGE")
    elif isinstance(environment, list):
        prefix = "CURIE_RUNNER_IMAGE="
        for entry in environment:
            if isinstance(entry, str) and entry.startswith(prefix):
                image = entry.removeprefix(prefix)
                break
    if not isinstance(image, str) or not image.strip():
        raise SystemExit(f"error: resolved compose config is missing {path}")
    return image


def _required_release_images(
    services: dict[str, object], profiles: Sequence[str]
) -> list[str]:
    images = _curie_images_for_profiles(services, profiles)
    seen = set(images)
    extras = (
        _required_service_image(services, "curie-dispatcher"),
        _required_worker_runner_image(services),
    )
    for extra in extras:
        if extra not in seen:
            images.append(extra)
            seen.add(extra)
    return images


def _compose_services_from_text(compose_text: str) -> dict[str, object]:
    """Parse resolved compose text; PyYAML is only needed on this path."""
    import yaml

    return _compose_services(yaml.safe_load(compose_text))


def curie_images_for_profiles(compose_text: str, profiles: Sequence[str]) -> list[str]:
    """ghcr curie image refs from services selected by `profiles`."""
    return _curie_images_for_profiles(_compose_services_from_text(compose_text), profiles)


def required_release_images(compose_text: str, profiles: Sequence[str]) -> list[str]:
    """Required refs from resolved compose text, including one shot images."""
    return _required_release_images(_compose_services_from_text(compose_text), profiles)


def _image_present(image: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _build(spec: ImageBuild, *, cwd: Path) -> None:
    for prereq in spec.extra_prereq_tags:
        if not _image_present(prereq):
            prereq_name = image_short_name(prereq)
            prereq_spec = IMAGE_BUILDS[prereq_name]
            _build(prereq_spec, cwd=cwd)
    cmd = ["docker", "build", "-f", spec.dockerfile]
    for key, value in spec.build_args:
        cmd.extend(["--build-arg", f"{key}={value}"])
    for tag in spec.tags:
        cmd.extend(["-t", tag])
    cmd.append(spec.context)
    subprocess.run(cmd, cwd=cwd, check=True)


def build_missing(images: Iterable[str], *, cwd: Path) -> list[str]:
    built: list[str] = []
    for image in images:
        if _image_present(image):
            continue
        name = image_short_name(image)
        spec = IMAGE_BUILDS.get(name)
        if spec is None:
            raise SystemExit(
                f"error: no build mapping for required image {image!r}; "
                "add it to compose/ensure_release_images.py IMAGE_BUILDS"
            )
        if image not in spec.tags:
            available = ", ".join(spec.tags)
            raise SystemExit(
                f"error: build mapping for required image {image!r} creates "
                f"different tags: {available}"
            )
        _build(spec, cwd=cwd)
        built.append(image)
    return built


def generate_compose(cwd: Path) -> str:
    result = subprocess.run(
        [sys.executable, str(GENERATE)],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _compose_services_from_file(path: Path) -> dict[str, object]:
    """Read normalized compose config without requiring PyYAML at runtime."""
    cmd = [
        "docker",
        "compose",
        "-f",
        str(path),
        "--profile",
        "*",
        "config",
        "--format",
        "json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    try:
        doc = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"error: docker compose returned invalid config JSON for {path}: {exc}"
        ) from exc
    return _compose_services(doc)


def curie_images_from_compose_file(path: Path, profiles: Sequence[str]) -> list[str]:
    """ghcr curie image refs from normalized compose config."""
    return _curie_images_for_profiles(_compose_services_from_file(path), profiles)


def required_release_images_from_file(path: Path, profiles: Sequence[str]) -> list[str]:
    """Required refs from normalized compose config, including one shot images."""
    return _required_release_images(_compose_services_from_file(path), profiles)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--profiles",
        default="full",
        help="comma-separated compose profiles the rung will start",
    )
    parser.add_argument(
        "--compose-file",
        default="",
        help="existing generated compose.release.yaml; generate when omitted",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the required image refs, one per line, and exit",
    )
    parser.add_argument(
        "--build-missing",
        action="store_true",
        help="docker-build any required image that is not present locally",
    )
    args = parser.parse_args(argv)
    cwd = REPO_ROOT
    profiles = tuple(item.strip() for item in args.profiles.split(",") if item.strip())
    if args.compose_file:
        images = required_release_images_from_file(Path(args.compose_file), profiles)
    else:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", delete=False
        ) as handle:
            handle.write(generate_compose(cwd))
            generated = Path(handle.name)
        try:
            images = required_release_images_from_file(generated, profiles)
        finally:
            generated.unlink(missing_ok=True)
    if args.list or not args.build_missing:
        for image in images:
            print(image)
        if not args.build_missing:
            return 0
    built = build_missing(images, cwd=cwd)
    for image in built:
        print(f"built {image}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
