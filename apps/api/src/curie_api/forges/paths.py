"""Repository path and branch name rules shared by forge config and adapters.

ADR 0197 identity: a repository is bound by its immutable project id, and its
path is for display and for the adapter's resolve-and-compare before cloning.
A GitHub path is exactly ``owner/name``. GitLab nests groups, so on every other
code host a path is two or more segments.

This module is the one home of the repository path rule. The workspace policy,
the API schemas and the forge config all validate through
``valid_repository_path``, keyed on the repository's kind.
"""

from __future__ import annotations

import re

from curie_api.forges import types

# A GitHub ``owner/name``: an owner of at most 39 alphanumerics and hyphens, and
# a name of at most 100 characters that does not end in a dot.
REPOSITORY_FULL_NAME_PATTERN = (
    r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9_-])?$"
)
_GITHUB_PATH = re.compile(REPOSITORY_FULL_NAME_PATTERN)

# One segment of a non-GitHub path: no whitespace, no slash, and not a dot
# segment, which git and every forge URL would resolve away.
_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]*")


def valid_repository_path(kind: str, path: str) -> bool:
    """Whether ``path`` is a repository path a ``kind`` code host can have."""

    if not isinstance(path, str):
        return False
    if kind == types.GITHUB:
        return bool(_GITHUB_PATH.fullmatch(path))
    segments = path.split("/")
    return len(segments) >= 2 and all(
        _SEGMENT.fullmatch(segment) and segment not in {".", ".."} for segment in segments
    )
