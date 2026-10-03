#!/usr/bin/env python3
"""Pin the release chart's first-party images to their signed digests (#3843).

The source chart leaves every first-party image `digest: ""`, so a render falls
back to the chart appVersion tag. A tag is mutable: whoever can push to the
registry can move it, and the packaged chart would then deploy bytes this
release never built. The release `chart` job verifies each image's cosign
signature against this workflow's identity, then calls this script to write the
verified index digest into the packaged copy of charts/curie/values.yaml before
`helm package`. The digest wins over the tag in every template, so the released
chart deploys exactly what the release signed.

The edit is a line edit, not a YAML round trip: only the six `digest: ""` lines
change, so every comment and formatting choice in the values file survives. The
script is stdlib only so it runs on the runner's system python3 with nothing
installed.

It fails closed, leaving the file untouched, when a chart image has no supplied
digest, a supplied image is not one the chart references, a repository does not
match the chart's, a digest is not `sha256:` plus 64 lowercase hex, an image is
supplied twice, or a target digest is already set. A partial or wrong pin is
worse than none: it looks verified.

Usage:
  python3 release/pin_chart_digests.py --values charts/curie/values.yaml \\
      --image api=ghcr.io/curie-eng/curie-api@sha256:<64 hex> ...
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_PREFIX = "ghcr.io/curie-eng/curie-"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
KEY_LINE = re.compile(r"^(?P<indent> *)(?P<key>[A-Za-z0-9_-]+):(?P<rest>.*?)(?P<eol>\r?\n)?$")
DIGEST_LINE = re.compile(
    r"^(?P<indent> *)digest:(?P<space> *)(?P<value>[^#\r\n]*?)"
    r"(?P<comment> +#[^\r\n]*)?(?P<eol>\r?\n)?$"
)


class PinError(Exception):
    """A refusal; the message names the offending image."""


@dataclass(frozen=True)
class ChartImage:
    """Where one published image lives in values.yaml."""

    name: str
    path: tuple[str, str]
    repository_key: str

    @property
    def repository(self) -> str:
        return REPOSITORY_PREFIX + self.name


# Every first-party image the chart references, keyed by its published name.
CHART_IMAGES = {
    image.name: image
    for image in (
        ChartImage("api", ("api", "image"), "repository"),
        ChartImage("dispatcher", ("dispatcher", "image"), "repository"),
        ChartImage("mail-adapter", ("mailAdapter", "image"), "repository"),
        ChartImage("worker", ("worker", "image"), "repository"),
        ChartImage("ui", ("ui", "image"), "repository"),
        ChartImage("runner", ("agentSandbox", "runner"), "image"),
    )
}


def _is_content(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _scalar(rest: str) -> str:
    value = re.sub(r"\s+#.*$", "", rest).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value


def _child_keys(lines: list[str], start: int, end: int, label: str) -> dict[str, int]:
    """Map each direct child key of the block lines[start:end] to its line index."""
    indent = None
    children: dict[str, int] = {}
    for i in range(start, end):
        line = lines[i]
        if not _is_content(line):
            continue
        if indent is None:
            indent = _indent_of(line)
        if _indent_of(line) != indent:
            continue
        match = KEY_LINE.match(line)
        if not match:
            continue
        key = match.group("key")
        if key in children:
            raise PinError(f"{label}: key {key!r} appears twice in values")
        children[key] = i
    return children


def _block_end(lines: list[str], header: int) -> int:
    indent = _indent_of(lines[header])
    for i in range(header + 1, len(lines)):
        if _is_content(lines[i]) and _indent_of(lines[i]) <= indent:
            return i
    return len(lines)


def locate(lines: list[str], image: ChartImage) -> tuple[int, str]:
    """Return (digest line index, repository value) for one chart image."""
    start, end = 0, len(lines)
    for key in image.path:
        children = _child_keys(lines, start, end, image.name)
        if key not in children:
            raise PinError(f"{image.name}: values has no {'.'.join(image.path)} block")
        header = children[key]
        start, end = header + 1, _block_end(lines, header)
    children = _child_keys(lines, start, end, image.name)
    block = ".".join(image.path)
    if image.repository_key not in children:
        raise PinError(f"{image.name}: {block} has no {image.repository_key} field")
    if "digest" not in children:
        raise PinError(f"{image.name}: {block} has no digest field")
    repo_line = KEY_LINE.match(lines[children[image.repository_key]])
    assert repo_line is not None
    return children["digest"], _scalar(repo_line.group("rest"))


def parse_image_args(raw: list[str]) -> dict[str, str]:
    """Parse `name=repository@sha256:<hex>` arguments into name -> digest."""
    errors: list[str] = []
    seen: dict[str, int] = {}
    digests: dict[str, str] = {}
    for arg in raw:
        name, sep, reference = arg.partition("=")
        if not sep or not name:
            errors.append(f"--image {arg!r} has no `name=` prefix")
            continue
        seen[name] = seen.get(name, 0) + 1
        if name not in CHART_IMAGES:
            errors.append(f"{name}: not an image the chart references ({', '.join(CHART_IMAGES)})")
            continue
        repository, at, digest = reference.partition("@")
        if not at:
            errors.append(
                f"{name}: {reference!r} is not pinned by digest (`repository@sha256:<hex>`)"
            )
            continue
        if repository != CHART_IMAGES[name].repository:
            errors.append(
                f"{name}: repository {repository!r} is not the chart's "
                f"{CHART_IMAGES[name].repository!r}"
            )
            continue
        if not DIGEST.fullmatch(digest):
            errors.append(f"{name}: digest {digest!r} is not sha256: plus 64 lowercase hex")
            continue
        digests[name] = digest
    for name, count in seen.items():
        if count > 1:
            errors.append(f"{name}: supplied {count} times; pass each image once")
    missing = [name for name in CHART_IMAGES if name not in seen]
    for name in missing:
        errors.append(f"{name}: the chart references this image but no --image was supplied")
    if errors:
        raise PinError("\n".join(errors))
    return digests


def pin(text: str, digests: dict[str, str]) -> str:
    lines = text.splitlines(keepends=True)
    errors: list[str] = []
    edits: dict[int, str] = {}
    for name, image in CHART_IMAGES.items():
        try:
            index, repository = locate(lines, image)
        except PinError as exc:
            errors.append(str(exc))
            continue
        if repository != image.repository:
            errors.append(f"{name}: values repository {repository!r} is not {image.repository!r}")
            continue
        match = DIGEST_LINE.match(lines[index])
        if not match:
            errors.append(f"{name}: cannot parse the digest line {lines[index]!r}")
            continue
        if _scalar(match.group("value")):
            errors.append(
                f"{name}: digest is already set ({lines[index].strip()}); refusing to overwrite"
            )
            continue
        edits[index] = (
            f'{match.group("indent")}digest:{match.group("space") or " "}"{digests[name]}"'
            f'{match.group("comment") or ""}{match.group("eol") or ""}'
        )
    if errors:
        raise PinError("\n".join(errors))

    pinned = [edits.get(i, line) for i, line in enumerate(lines)]
    # Re-locate on the result: every image must now read back its own digest.
    for name, image in CHART_IMAGES.items():
        index, _ = locate(pinned, image)
        match = DIGEST_LINE.match(pinned[index])
        if index not in edits or match is None or _scalar(match.group("value")) != digests[name]:
            raise PinError(f"{name}: digest did not read back after pinning")
    return "".join(pinned)


def write_atomically(path: Path, text: str) -> None:
    mode = path.stat().st_mode & 0o777
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--values", type=Path, required=True, help="the chart values.yaml to pin in place"
    )
    parser.add_argument(
        "--image",
        action="append",
        default=[],
        metavar="NAME=REPOSITORY@sha256:HEX",
        help="one per first-party chart image",
    )
    args = parser.parse_args(argv)
    try:
        digests = parse_image_args(args.image)
        with args.values.open(encoding="utf-8", newline="") as handle:
            text = handle.read()
        write_atomically(args.values, pin(text, digests))
    except PinError as exc:
        print(f"pin_chart_digests: refusing, {args.values} left unchanged:\n{exc}", file=sys.stderr)
        return 1
    for name, digest in digests.items():
        print(f"{name}: pinned {CHART_IMAGES[name].repository}@{digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
