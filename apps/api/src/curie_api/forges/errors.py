"""Typed failures every Tracker and CodeHost adapter raises (ADR 0197).

Callers decide on the class alone. No error carries response text, headers or
tokens from the forge.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from curie_api.forges.capabilities import Operation


class ForgeError(Exception):
    """Base of every port failure."""


class Unavailable(ForgeError):
    """The forge could not answer now: transport, rate limit, 5xx, or a listing
    whose pages could not all be read. Retry later and advance no cursor."""


class Unauthorized(ForgeError):
    """The configured credential was refused."""


class NotFound(ForgeError):
    """The issue, repository, pull request or commit does not exist."""


class Ambiguous(ForgeError):
    """More than one item matched where the port answers at most one, so the
    caller must not pick one."""


class Unsupported(ForgeError):
    """The adapter declared ``operation`` unsupported; the caller must fall back."""

    def __init__(self, operation: Operation) -> None:
        self.operation = operation
        super().__init__(str(operation))


class InvalidPairing(ForgeError):
    """A tracker and code host that may not be bound together, or an adapter
    whose capability declaration does not meet the pairing's mandatory set."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
