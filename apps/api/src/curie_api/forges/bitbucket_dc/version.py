"""The Bitbucket Data Center version floor (#4401 decision 7).

The code host loop was proven on 9.4 and the fixture pins 9.4.24, so an older
server answers every port call with `ServerTooOld`, naming its version.
"""

from __future__ import annotations

import re

from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import ForgeError, Unsupported

MINIMUM = (9, 4, 0)
_NUMBER = re.compile(r"\d+")


def parse_version(text: str) -> tuple[int, int, int] | None:
    """``9.4.24`` as ``(9, 4, 24)``; a suffix such as ``-rc1`` is ignored and a
    missing part reads as 0. None when ``text`` starts with no number."""

    parts: list[int] = []
    for part in text.strip().split(".")[:3]:
        found = _NUMBER.match(part)
        if found is None:
            break
        parts.append(int(found.group()))
        if found.end() != len(part):
            break
    if not parts:
        return None
    major, minor, patch = (*parts, 0, 0)[:3]
    return major, minor, patch


class ServerTooOld(Unsupported):
    """A port call on a Bitbucket Data Center older than `MINIMUM`."""

    def __init__(self, operation: Operation, version: str) -> None:
        self.operation = operation
        minimum = ".".join(map(str, MINIMUM))
        ForgeError.__init__(
            self, f"Bitbucket Data Center {version} is older than the minimum {minimum}"
        )
