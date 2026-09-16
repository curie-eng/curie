#!/usr/bin/env python3
"""Build ratekit's pure-Python wheel with nothing but the standard library.

Ratekit ships as source and is installed into the sandbox virtualenv from a
local wheelhouse, so the check run needs no package-registry egress at all.
The build is deliberately stdlib-only (``zipfile`` + ``hashlib`` + ``base64``):
the environments this repository is verified in carry no build backend, so an
offline ``pip install <source tree>`` would die with
``BackendUnavailable: Cannot import 'setuptools.build_meta'``.

Usage:

    python tools/build_wheel.py <source-root> <output-dir>

``<source-root>`` is the directory holding the importable package (``src``),
``<output-dir>`` is the wheelhouse to write into. The built file path is
printed on stdout.
"""

from __future__ import annotations

import base64
import hashlib
import pathlib
import sys
import zipfile

NAME = "ratekit"
VERSION = "1.2.0"


def _digest(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def build(source_root: pathlib.Path, output_dir: pathlib.Path) -> pathlib.Path:
    if not source_root.is_dir():
        raise SystemExit(f"source root does not exist: {source_root}")
    output_dir.mkdir(parents=True, exist_ok=True)
    wheel_path = output_dir / f"{NAME}-{VERSION}-py3-none-any.whl"
    dist_info = f"{NAME}-{VERSION}.dist-info"
    records: list[str] = []

    def add(archive: zipfile.ZipFile, arcname: str, data: bytes) -> None:
        archive.writestr(arcname, data)
        records.append(f"{arcname},sha256={_digest(data)},{len(data)}")

    modules = sorted(source_root.rglob("*.py"))
    if not modules:
        raise SystemExit(f"no Python modules found under {source_root}")

    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for module in modules:
            add(archive, module.relative_to(source_root).as_posix(), module.read_bytes())
        add(
            archive,
            f"{dist_info}/METADATA",
            (
                "Metadata-Version: 2.1\n"
                f"Name: {NAME}\n"
                f"Version: {VERSION}\n"
                "Summary: Rate-limit spec parsing for the billing throttle.\n"
            ).encode(),
        )
        add(
            archive,
            f"{dist_info}/WHEEL",
            b"Wheel-Version: 1.0\nGenerator: ratekit-fixture-builder\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n",
        )
        records.append(f"{dist_info}/RECORD,,")
        archive.writestr(f"{dist_info}/RECORD", "\n".join(records) + "\n")

    return wheel_path


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        raise SystemExit(f"usage: {argv[0]} <source-root> <output-dir>")
    print(build(pathlib.Path(argv[1]), pathlib.Path(argv[2])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
