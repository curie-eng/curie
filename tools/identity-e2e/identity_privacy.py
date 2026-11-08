"""The evidence privacy rules of `curie dev identity-e2e -- prove`, and the one walker under them.

A scenario's evidence must carry no principal, token, grant, ticket or other
private value of the run. :func:`walk` finds each node that breaks that rule;
``identity_scenarios_attestation.require_private_evidence`` (the guard) fails on
the first, and :func:`scrub` (behind ``Evidence.finish`` and
``identity_scenarios_attestation.scrub_private``) replaces every one in place,
so a failed run writes and prints no private value either.

Standard library only, and it imports no scenario module, so ``identity_scenarios``
and ``identity_scenarios_attestation`` both import it without a cycle.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from typing import Any

# A caller, grant or replay token envelope (curie_connector_proxy.caller) or an
# approver ticket (curie_internal.approver_ticket), and the shortest secret worth
# searching for.
ENVELOPE = re.compile(r"(?P<prefix>cct|ccg|ccr|cat1)\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
MIN_SECRET = 8
# What :func:`scrub` leaves in place of a principal-shaped object and of a string.
PRINCIPAL_REDACTED = "[redacted: principal]"
REDACTED = "[redacted]"

Found = Callable[[str], None]


def searchable(values: Iterable[Any]) -> tuple[str, ...]:
    """The strings among ``values`` worth searching for, in order and once each.

    Non-strings and strings shorter than :data:`MIN_SECRET` characters are ignored.
    """

    return tuple(dict.fromkeys(v for v in values if isinstance(v, str) and len(v) >= MIN_SECRET))


def _carried(text: str, secrets: Sequence[str]) -> str | None:
    """What ``text`` carries that the evidence must not, without echoing it, or None."""

    found = ENVELOPE.search(text)
    if found is not None:
        return f"a {found['prefix']}. envelope"
    if any(secret in text for secret in secrets):
        return "a private value of the run"
    return None


def scrub_text(text: str, secrets: Sequence[str]) -> str:
    """``text`` with every envelope and every one of ``secrets`` replaced by :data:`REDACTED`."""

    text = ENVELOPE.sub(REDACTED, text)
    for secret in secrets:
        text = text.replace(secret, REDACTED)
    return text


def walk(node: Any, path: str, secrets: Sequence[str], found: Found) -> tuple[Any, int]:
    """Visit a JSON-like tree under the privacy rules: ``(replacement, string leaves checked)``.

    Calls ``found`` with a message naming the JSON path of each offending node,
    never its value: a mapping with both an ``issuer`` and a ``subject`` key
    (not descended), a string or other leaf holding a ``cct.``, ``ccg.``,
    ``ccr.`` or ``cat1.`` envelope of two base64url segments of at least 16
    characters or any of ``secrets``, and a mapping key that does. Mapping
    keys are checked before the values.

    Each node is replaced only after ``found`` returns for it, so a ``found``
    that raises leaves the tree untouched: a principal-shaped mapping by
    :data:`PRINCIPAL_REDACTED`, a leaf by :data:`REDACTED`, and a key by
    ``[redacted key <n>]`` (``n`` counts the replaced keys of that mapping) with
    its value kept and walked. Mappings and lists change in place; a tuple or a
    set is rebuilt. A node with nothing to find is returned as it is.
    """

    if node is None or isinstance(node, (bool, int, float)):
        return node, 0
    if isinstance(node, Mapping):
        return _walk_mapping(node, path, secrets, found)
    if isinstance(node, (list, tuple, set, frozenset)):
        return _walk_items(node, path, secrets, found)
    what = _carried(node if isinstance(node, str) else str(node), secrets)
    if what is None:
        return node, 1
    found(f"the evidence carries {what} at {path or '<root>'}")
    return REDACTED, 1


def _walk_mapping(
    node: Mapping[Any, Any], path: str, secrets: Sequence[str], found: Found
) -> tuple[Any, int]:
    where = path or "<root>"
    if "issuer" in node and "subject" in node:
        found(f"the evidence carries a principal-shaped object (issuer and subject) at {where}")
        return PRINCIPAL_REDACTED, 0
    keys: list[Any] = []
    hidden = 0
    for key in node:
        if _carried(str(key), secrets) is None:
            keys.append(key)
            continue
        found(f"a key of the mapping at {where} carries an envelope or a private value")
        hidden += 1
        keys.append(f"[redacted key {hidden}]")
    values = list(node.values())
    items: list[tuple[Any, Any]] = []
    leaves = 0
    for key, value in zip(keys, values, strict=True):
        new, counted = walk(value, f"{path}.{key}" if path else str(key), secrets, found)
        items.append((key, new))
        leaves += counted
    if not hidden and all(new is old for (_, new), old in zip(items, values, strict=True)):
        return node, leaves
    if isinstance(node, MutableMapping):
        node.clear()
        node.update(items)
        return node, leaves
    return dict(items), leaves


def _walk_items(node: Any, path: str, secrets: Sequence[str], found: Found) -> tuple[Any, int]:
    values = list(node)
    walked = [walk(value, f"{path}[{index}]", secrets, found) for index, value in enumerate(values)]
    leaves = sum(counted for _, counted in walked)
    replaced = [new for new, _ in walked]
    if all(new is old for new, old in zip(replaced, values, strict=True)):
        return node, leaves
    if isinstance(node, list):
        node[:] = replaced
        return node, leaves
    if isinstance(node, tuple):
        return tuple(replaced), leaves
    return type(node)(replaced), leaves


def scrub(data: Any, secrets: Iterable[Any] = ()) -> int:
    """Replace in place everything :func:`walk` finds in ``data``; returns how many it replaced.

    ``data`` is a mapping or a list. A principal-shaped mapping at the root,
    which no string can replace in place, is emptied. Zero leaves ``data``
    exactly as it was.
    """

    count = 0

    def counted(_: str) -> None:
        nonlocal count
        count += 1

    replaced, _ = walk(data, "", searchable(secrets), counted)
    if replaced is not data and isinstance(data, MutableMapping):
        data.clear()
    return count
