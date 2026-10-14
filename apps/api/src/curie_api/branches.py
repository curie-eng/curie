"""The branch names a factory base may use.

A leaf module: it imports nothing from ``curie_api``, so both ``curie_api.config``
and ``curie_api.forges.config`` can use it without importing each other.
"""

from __future__ import annotations

from typing import Any


def valid_base_branch(name: Any) -> bool:
    """A branch name a factory base may use: no whitespace, leading ``-`` or ``..``."""

    return (
        isinstance(name, str)
        and bool(name)
        and not any(char.isspace() for char in name)
        and not name.startswith("-")
        and ".." not in name
    )
