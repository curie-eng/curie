"""First-party image bases must be a full version plus digest (#2320).

A floating tag (`python:3.13-slim`, `node:22-slim`) moves under a rebuild
with no commit, so a nightly ladder grade change or a contributor
`curie build` can disagree with CI at the same sha. The pin is the FROM
line itself; Dependabot's docker ecosystem is how a base move becomes a
reviewable PR instead of a silent rebuild.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Shipped first-party images plus the other first-party product Dockerfiles
# that share the same floating-tag shape. Examples, prototypes, and test
# fixtures are out of scope: they are not in the release matrix.
FIRST_PARTY_DOCKERFILES = (
    "apps/api/Dockerfile",
    "apps/dispatcher/Dockerfile",
    "apps/mail-adapter/Dockerfile",
    "apps/ui/Dockerfile",
    "apps/worker/Dockerfile",
    "adapters/discord/Dockerfile",
    "runner/Dockerfile",
)

_FROM = re.compile(
    r"^FROM\s+(?:--platform=\S+\s+)?(?P<image>\S+)(?:\s+AS\s+\S+)?\s*$",
    re.IGNORECASE,
)
_PINNED = re.compile(
    r"^[^:@\s]+(?::[^@\s]*\d+\.\d+\.\d+[^@\s]*)?@sha256:[0-9a-f]{64}$",
    re.IGNORECASE,
)
_FULL_VERSION = re.compile(r"\d+\.\d+\.\d+")
_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$", re.IGNORECASE)


def _logical_instructions(dockerfile_text: str) -> list[str]:
    instructions: list[str] = []
    pending: list[str] = []
    for raw_line in dockerfile_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        continues = line.endswith("\\")
        pending.append(line[:-1].rstrip() if continues else line)
        if not continues:
            instructions.append(" ".join(pending))
            pending = []
    assert not pending, "Dockerfile ends with an unfinished continuation"
    return instructions


def _from_images(dockerfile_text: str) -> list[str]:
    images: list[str] = []
    for instruction in _logical_instructions(dockerfile_text):
        match = _FROM.fullmatch(instruction)
        if match is None:
            continue
        image = match.group("image")
        if image == "scratch" or image.startswith("$") or "${" in image:
            continue
        images.append(image)
    return images


def _pin_error(image: str) -> str | None:
    """Return a reason the FROM ref is not a full version plus digest."""
    if _PINNED.fullmatch(image) and _FULL_VERSION.search(image.split("@", 1)[0]):
        return None
    if _DIGEST.search(image) is None:
        return "missing sha256 digest"
    if _FULL_VERSION.search(image.split("@", 1)[0]) is None:
        return "tag is not a full x.y.z version"
    return "FROM is not pinned to a version plus digest"


def test_pin_parser_accepts_version_plus_digest() -> None:
    assert (
        _pin_error(
            "python:3.13.15-slim@sha256:"
            "9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285"
        )
        is None
    )


def test_pin_parser_rejects_floating_tag() -> None:
    assert _pin_error("python:3.13-slim") == "missing sha256 digest"


def test_pin_parser_rejects_version_without_digest() -> None:
    assert _pin_error("python:3.13.15-slim") == "missing sha256 digest"


def test_pin_parser_rejects_digest_on_floating_minor() -> None:
    digest = "sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285"
    assert _pin_error(f"python:3.13-slim@{digest}") == "tag is not a full x.y.z version"


def test_first_party_dockerfiles_pin_from_to_version_and_digest() -> None:
    failures: list[str] = []
    for relative in FIRST_PARTY_DOCKERFILES:
        path = _REPO_ROOT / relative
        images = _from_images(path.read_text(encoding="utf-8"))
        assert images, f"{relative} has no FROM images to pin"
        for image in images:
            error = _pin_error(image)
            if error is not None:
                failures.append(f"{relative}: FROM {image}: {error}")
    assert failures == []


def test_floating_from_in_a_first_party_dockerfile_is_rejected() -> None:
    dockerfile = (_REPO_ROOT / "runner" / "Dockerfile").read_text(encoding="utf-8")
    mutated = "FROM python:3.13-slim AS node\n" + dockerfile
    errors = [_pin_error(image) for image in _from_images(mutated)]
    assert "missing sha256 digest" in errors


def _dependabot_ecosystem_blocks(text: str) -> dict[str, str]:
    parts = re.split(r"(?m)^  - package-ecosystem:\s*", text)
    blocks: dict[str, str] = {}
    for part in parts[1:]:
        first, _, rest = part.partition("\n")
        name = first.strip()
        assert name not in blocks, f"duplicate dependabot ecosystem {name}"
        blocks[name] = rest
    return blocks


def test_dependabot_watches_first_party_docker_directories() -> None:
    text = (_REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    blocks = _dependabot_ecosystem_blocks(text)
    assert "docker" in blocks, (
        "dependabot.yml must configure the docker ecosystem so base bumps arrive as reviewable PRs"
    )
    docker_block = blocks["docker"]
    missing = [
        directory
        for directory in sorted(
            {f"/{Path(relative).parent.as_posix()}" for relative in FIRST_PARTY_DOCKERFILES}
        )
        if directory not in docker_block
    ]
    assert missing == []
