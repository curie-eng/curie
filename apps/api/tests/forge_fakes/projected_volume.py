"""A Secret key mounted as the kubelet mounts it, for credential rotation tests (#4630).

The kubelet's atomic writer keeps each payload in a timestamped directory,
points ``..data`` at the current one, and exposes every key as a symlink
through ``..data``. A Secret update writes a new timestamped directory, renames
a fresh ``..data`` symlink over the old one in a single ``rename``, then removes
the previous directory
(https://kubernetes.io/docs/concepts/configuration/secret/#mounted-secrets-are-updated-automatically).
`ProjectedVolume` lays a key out and replaces it the same way.
"""

from __future__ import annotations

import itertools
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

KEY = "token"

# Broken payloads a reader must refuse: (publish keyword arguments, needs a
# non-root user). ``{token}`` in a content is replaced by the caller's value.
BROKEN: dict[str, tuple[dict[str, Any], bool]] = {
    "missing": ({"content": None}, False),
    "unreadable": ({"content": "{token}", "mode": 0o000}, True),
    "directory": ({"content": None, "as_directory": True}, False),
    "empty": ({"content": ""}, False),
    "whitespace": ({"content": " \t\r\n \x0b\x0c"}, False),
    "line-break": ({"content": "{token}\ntail"}, False),
    "nul": ({"content": "{token}\x00tail"}, False),
    "escape": ({"content": "tok\x1b{token}"}, False),
}


class ProjectedVolume:
    """``<root>/token`` -> ``..data/token``; ``..data`` -> ``..<timestamp>``."""

    def __init__(self, root: Path, content: str) -> None:
        root.mkdir(parents=True)
        self.root = root
        self._serial = itertools.count(1)
        self.publish(content)
        (root / KEY).symlink_to(f"..data/{KEY}")

    @property
    def path(self) -> Path:
        """The key path the pod reads, a symlink through ``..data``."""

        return self.root / KEY

    def publish(
        self, content: str | None, *, mode: int = 0o644, as_directory: bool = False
    ) -> None:
        """Replace the payload. ``content`` None publishes a payload without the
        key, so the key path dangles; ``as_directory`` makes the key a directory."""

        serial = next(self._serial)
        stamp = f"..2026_10_15_12_00_{serial:02d}.{serial:09d}"
        payload = self.root / stamp
        payload.mkdir()
        target = payload / KEY
        if as_directory:
            target.mkdir()
        elif content is not None:
            target.write_bytes(content.encode())
            target.chmod(mode)
        data = self.root / "..data"
        previous = os.readlink(data) if data.is_symlink() else None
        staged = self.root / "..data_tmp"
        staged.symlink_to(stamp)
        os.replace(staged, data)
        if previous is not None:
            shutil.rmtree(self.root / previous)

    def break_with(self, how: str, token: str = "") -> None:
        """Publish the broken payload ``how`` (a `BROKEN` key)."""

        arguments, needs_non_root = BROKEN[how]
        if needs_non_root and os.geteuid() == 0:
            pytest.skip("root reads a mode 000 file")
        arguments = dict(arguments)
        if isinstance(arguments.get("content"), str):
            arguments["content"] = arguments["content"].format(token=token)
        self.publish(**arguments)
