"""How a hook delivery names the thing it is about (ADR-0134, amending ADR-0079).

A hook's firings share ONE thread by default, and that is still the posture an
agent gets without configuration. What this module adds is the operator's option
to say that a particular hook's deliveries are about INDEPENDENT things -- one
pull request each, one ticket each -- and should therefore run as independent
threads.

**The partition comes from the agent row, not from the request.** A header naming
the partition would sit outside the bytes the HMAC covers and would hand whoever
holds the hook secret direct control of this agent's sandbox cardinality. The
operator instead configures a JSON Pointer per hook (``agents.hook_partitions``);
the upstream still supplies the VALUE, but only through a field the operator
named.

**A misconfigured delivery refuses; it never falls back to the unpartitioned
id.** Falling back would collapse N intended threads into one and would do it
invisibly -- a working hook that had quietly stopped fanning out. Everything here
that cannot resolve raises rather than returning a default, which is why there is
no ``.get(..., fallback)`` anywhere below.

This module is imported by ``schemas.py`` (the write surface) and by
``routers/hooks.py`` (the ingress). It must not import either of them back.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

# A hook name is an operator-chosen label that ends up inside Valkey key names
# and inside the conversation id, so it is constrained rather than trusted: no
# separators, no unbounded length, nothing that could make two distinct hooks
# build one key. The same shape now also governs the KEYS of
# ``agents.hook_partitions``, so a configured hook name and a fired hook name
# cannot disagree -- a key the ingress would refuse at step 1 could never match a
# firing, and would configure nothing while looking configured.
HOOK_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")

# What a partition VALUE may be. Narrower than the hook name in one direction
# (uppercase is allowed, because the identities this reads are mixed case --
# Slack ids, ticket keys) and identical in the ones that matter: no `:`, so a
# value cannot forge a segment of the conversation id; no `/`, so it cannot
# escape a URL path segment in the transcript ref; no whitespace, control
# characters or `=`, so it cannot forge a line in the ingress log or a second
# entry in the sandbox boot env; and a first character that is alphanumeric, so
# a bare `..` is unrepresentable. Non-ASCII refuses rather than transliterating.
#
# `\A`/`\Z` rather than `^`/`$`: `$` also matches immediately BEFORE a trailing
# newline, which is exactly the log-forging value this bound exists to exclude.
PARTITION_VALUE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,62}\Z")

# An RFC 6901 array index: digits, no leading zero except "0" itself. Without the
# leading-zero rule one array member is addressable two ways, and two pointers
# that read one field are two configurations an operator cannot tell apart.
_ARRAY_INDEX = re.compile(r"\A(0|[1-9][0-9]*)\Z")


class PartitionError(ValueError):
    """A partitioned hook whose delivery cannot name its partition.

    Carries the hook and the pointer in its message because the operator's only
    other signal is a 422 on a route whose caller they do not control: the hook
    says which configuration entry is wrong, and the pointer says which field of
    the document that entry expected to find.
    """

    def __init__(self, hook: str, pointer: str, reason: str) -> None:
        self.hook = hook
        self.pointer = pointer
        self.detail = (
            f"hook {hook!r} is partitioned by pointer {pointer!r}, but this "
            f"delivery {reason}"
        )
        super().__init__(self.detail)


def validate_pointer_syntax(pointer: str) -> str:
    """Refuse a pointer that is not a JSON Pointer at all.

    Applied on the WRITE path, so an operator learns about a malformed pointer
    while they are at the keyboard rather than through a 422 handed to a
    third-party upstream nobody at this end is watching.

    Args:
        pointer: The configured pointer.

    Returns:
        The pointer, unchanged.

    Raises:
        ValueError: If the pointer is neither empty nor ``/``-prefixed.
    """

    if pointer != "" and not pointer.startswith("/"):
        raise ValueError(
            f"pointer {pointer!r} is not a JSON Pointer: it must be either the "
            "empty string (the whole document) or start with '/' (e.g. "
            "'/pull_request/number')"
        )
    # RFC 6901 section 3 permits `~` only as the two escapes `~0` and `~1`; any
    # other `~` -- unfollowed, or followed by anything else -- is not a pointer
    # at all. Left unchecked, `resolve_pointer` would read a bare `~2` or a
    # trailing `~` as two literal characters of a key nobody meant, and a
    # pointer configured before this rule existed must be refused here (and
    # thus turned into a `PartitionError` by `derive_partition`, which calls
    # this through `resolve_pointer`) rather than silently misresolving at
    # delivery time.
    index = pointer.find("~")
    while index != -1:
        if pointer[index : index + 2] not in ("~0", "~1"):
            raise ValueError(
                f"pointer {pointer!r} is not a JSON Pointer: ~ must be followed "
                "by 0 or 1 (RFC 6901 section 3 permits only the escapes ~0 and "
                "~1)"
            )
        index = pointer.find("~", index + 2)
    return pointer


def resolve_pointer(document: Any, pointer: str) -> Any:
    """Read one value out of a decoded document by RFC 6901 pointer.

    Implemented here rather than taken as a dependency: the rule is a dozen
    lines, and the one part of it that is easy to get wrong is the unescaping
    ORDER, which a test pins directly.

    Args:
        document: The decoded delivery body, or any subtree of it.
        pointer: An RFC 6901 pointer.

    Returns:
        The addressed value.

    Raises:
        ValueError: If the pointer is malformed, or a list segment is not a
            well-formed index.
        KeyError: If an object has no such key.
        IndexError: If a list index is past the end.
        TypeError: If the pointer descends into a scalar.
    """

    validate_pointer_syntax(pointer)
    value = document
    for token in pointer.split("/")[1:] if pointer else []:
        # `~1` BEFORE `~0`, and the order is the whole bug: applied the other way
        # round, `~01` becomes `~1` becomes `/`, and the pointer silently reads a
        # different key than the one the operator wrote.
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, Mapping):
            value = value[key]
        elif isinstance(value, list):
            if not _ARRAY_INDEX.fullmatch(key):
                raise ValueError(f"pointer segment {key!r} is not an array index")
            value = value[int(key)]
        else:
            raise TypeError(f"pointer segment {key!r} descends into a non-container")
    return value


def derive_partition(
    config: Mapping[str, Any] | None, hook: str, body: bytes
) -> str | None:
    """The partition this delivery belongs to, or None when the hook has none.

    The None path is the byte-identity path, and it does no work on the payload
    at all: an agent with no configuration, or with configuration for some OTHER
    hook, must keep accepting every body it accepts today -- including bodies
    that are not JSON.

    Args:
        config: The agent's ``hook_partitions`` map, or None.
        hook: The validated hook name that fired.
        body: The raw request body.

    Returns:
        The partition value, or None when this hook is unpartitioned.

    Raises:
        PartitionError: When the hook IS partitioned and this delivery cannot
            name its partition.
    """

    entry = config.get(hook) if config else None
    if entry is None:
        return None

    # `schemas.HookPartitionConfig` is the only writer of this column and makes
    # `pointer` required, so its absence is a corrupted row rather than a
    # configuration an operator can reach.
    pointer: str = entry["pointer"]

    try:
        document = json.loads(body)
    except (ValueError, RecursionError) as exc:
        # The byte bound elsewhere in the ingress path does not bound NESTING
        # depth: a small, well-under-the-limit body of deeply nested arrays
        # blows the decoder's recursion limit instead of failing to parse. That
        # must still land as this same pre-claim 422, never an unhandled 500,
        # so a pathological body is refused before it can consume a delivery
        # claim.
        raise PartitionError(hook, pointer, "carries a body that is not JSON") from exc

    try:
        value = resolve_pointer(document, pointer)
    except (ValueError, LookupError, TypeError) as exc:
        raise PartitionError(
            hook, pointer, "carries a body the pointer does not resolve against"
        ) from exc

    # `bool` first: Python's True IS an int, so an int-shaped coercion below
    # would turn `true` into the partition "True" -- a plausible-looking thread
    # nobody configured.
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise PartitionError(
            hook,
            pointer,
            "resolves to a value that is not a string or an integer, so it "
            "names no stable identity",
        )

    # A JSON number is the commonest stable identity there is (a PR number, an
    # issue number), so it is accepted as its string form: `42` and `"42"` must
    # not become two threads for one pull request.
    partition = str(value)
    if not PARTITION_VALUE.fullmatch(partition):
        raise PartitionError(
            hook,
            pointer,
            "resolves to a value outside the partition bound of 1-63 characters "
            "of letters, digits, dot, dash or underscore, beginning with a "
            "letter or a digit",
        )
    return partition
