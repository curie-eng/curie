"""Repository path and branch name rules shared by forge config and adapters.

ADR 0197 identity: a repository is bound by its immutable project id, and its
path is for display and for the adapter's resolve-and-compare before cloning.
A GitHub path is exactly ``owner/name``. GitLab nests groups, so on every other
code host a path is two or more segments.
"""

from __future__ import annotations

import re

from curie_api.forges import types
from curie_api.workspace_policy import valid_repository_name

# One segment of a non-GitHub path: no whitespace, no slash, and not a dot
# segment, which git and every forge URL would resolve away.
_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]*")


def valid_repository_path(kind: str, path: str) -> bool:
    """Whether ``path`` is a repository path a ``kind`` code host can have."""

    if not isinstance(path, str):
        return False
    if kind == types.GITHUB:
        return valid_repository_name(path)
    segments = path.split("/")
    return len(segments) >= 2 and all(
        _SEGMENT.fullmatch(segment) and segment not in {".", ".."} for segment in segments
    )
