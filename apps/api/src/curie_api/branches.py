"""The branch names a factory base may use.

A leaf module: it imports nothing from ``curie_api``, so both ``curie_api.config``
and ``curie_api.forges.config`` can use it without importing each other.
"""

from __future__ import annotations

from collections.abc import Sequence
from fnmatch import fnmatchcase


def valid_base_branch(name: object) -> bool:
    """A literal Git short branch name, always addressed under ``refs/heads/``."""

    return (
        isinstance(name, str)
        and bool(name)
        and name not in {"@", "HEAD"}
        and not name.startswith(("-", "/"))
        and not name.endswith(("/", "."))
        and ".." not in name
        and "//" not in name
        and "@{" not in name
        and not any(
            char.isspace() or ord(char) < 32 or ord(char) == 127 or char in "~^:?*[\\"
            for char in name
        )
        and all(not part.startswith(".") and not part.endswith(".lock") for part in name.split("/"))
    )


def valid_base_pattern(value: object) -> bool:
    """A branch glob with well-formed ``*``, ``?`` and bracket expressions."""

    if not isinstance(value, str) or not value:
        return False
    literal: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char in "*?":
            literal.append("x")
        elif char == "[":
            end = index + 1
            if end < len(value) and value[end] == "!":
                end += 1
            start = end
            if end < len(value) and value[end] == "]":
                end += 1
            end = value.find("]", end)
            if end == -1 or end == start:
                return False
            members = value[start:end]
            if any(c.isspace() or ord(c) < 32 or ord(c) == 127 or c == "\\" for c in members):
                return False
            literal.append("x")
            index = end
        else:
            literal.append(char)
        index += 1
    return valid_base_branch("".join(literal))


def base_allowed(branch: str, patterns: Sequence[str], *, default_branch: str | None) -> bool:
    """Authorize a literal branch against case-sensitive globs or the default."""

    if not valid_base_branch(branch):
        return False
    if not patterns:
        return branch == default_branch
    return all(valid_base_pattern(pattern) for pattern in patterns) and any(
        fnmatchcase(branch, pattern) for pattern in patterns
    )
