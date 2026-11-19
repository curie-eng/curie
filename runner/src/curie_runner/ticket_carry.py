"""Carry a connector-issued approval ticket to its gated call (Draft ADR 0288).

ADR 0271 has a hosted connector's preparation tool return a signed
``cat1.<payload>.<signature>`` ticket, and the model must copy it unchanged
into the top-level ``approval_ticket`` argument of the gated tool. A ticket is
1,000 or more characters of base64url, and a model sometimes drops or changes
part of it (long repeated runs are the usual casualty). The API then refuses the
approval and nothing tells the requester why.

This module removes the copy from the model's hands without giving the runner
any authority. It remembers the well-formed tickets a hosted connector returned
in this conversation, reading their claims WITHOUT trusting them. When the
model calls that connector's gated tool with a damaged copy, it puts back the
ticket the copy was taken from, identified by evidence in the copy itself: a
damaged copy keeps either its source's exact ``cat1.<payload>`` part (the
signature was damaged) or its exact signature segment (the payload was
damaged). The source must be the one remembered ticket sharing such a part with
the copy, must name this connector and tool, and must have been issued for
exactly the other arguments of this call.

Because the source is identified by the copy and checked against the call, a
ticket the runner never saw, forgot, or saw twice cannot be confused with
another one: there is simply no unique source, and the call is left alone. A
copy that still parses as a complete ticket is never replaced, since nothing
distinguishes a damaged signature of the same length from a different ticket.

The API still verifies the signature, scope, requester, freshness, digest,
ceiling and nonce of whatever ticket it receives. Fail closed means "leave the
call alone": in every case the rule does not cover, ``restore`` returns None and
the caller records the model's own arguments exactly as before.

Memory is per process, which is per conversation: a suspended sandbox resumes
in a new process with nothing remembered (ADR-0003). The approved call does not
need this memory; it takes the ticket from the grant's stored arguments.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from typing import Any

from curie_internal.approver_ticket import (
    MAX_TICKET_CHARS,
    TicketError,
    execution_arguments_sha256,
    unverified_claims,
)

logger = logging.getLogger(__name__)

TICKET_ARGUMENT = "approval_ticket"
"""The one top-level argument ADR 0271 decision 3 excludes from the digest."""

# Where a candidate may start, and the framing it must have from there. Every
# start is tried on its own, so a malformed prefix cannot swallow the next
# ticket. Only the framing is matched here; ``unverified_claims`` applies the
# real bounded, closed-shape parser.
_START = re.compile(r"cat1\.")
_TOKEN = re.compile(r"cat1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")

# Bounds on what one reply may cost: characters scanned, JSON nodes visited
# (keys and values, each pulled lazily), nesting depth and candidate starts.
# A reply beyond a bound is read only in part, which can only lose a ticket;
# a lost ticket has no source to match, so the call is left alone.
MAX_SCAN_CHARS = 262_144
MAX_SCAN_NODES = 10_000
MAX_SCAN_DEPTH = 32
MAX_CANDIDATES = 64
MAX_REMEMBERED = 32


def live_pair(tool_name: object) -> tuple[str, str] | None:
    """``mcp__<connector>__<tool>`` split once, or None for anything else.

    The same rule as the worker's ``kernel/claim.py::_connector_tool_grant`` and
    the API's ``approver_tickets.live_pair``: a plugin server (``mcp__plugin_``)
    is never a connector, both halves must be nonempty, and the upstream tool
    keeps any later ``__``. The runner cannot import either sibling.
    """

    if not isinstance(tool_name, str):
        return None
    if not tool_name.startswith("mcp__") or tool_name.startswith("mcp__plugin_"):
        return None
    connector, separator, upstream = tool_name.removeprefix("mcp__").partition("__")
    if not separator or not connector or not upstream:
        return None
    return connector, upstream


class _Scan:
    """Whether a bounded scan read the whole reply."""

    complete = True


def _strings(response: Any, scan: _Scan) -> Iterator[str]:
    """Every string in a JSON-like reply, keys included, within the node and depth bounds.

    Children are pulled one at a time from iterators, so a wide container costs
    only the nodes actually visited, never a copy of its children. Reaching a
    bound clears ``scan.complete``.
    """

    nodes = MAX_SCAN_NODES
    stack: list[Iterator[Any]] = [iter((response,))]
    while stack:
        value = next(stack[-1], _DONE)
        if value is _DONE:
            stack.pop()
            continue
        nodes -= 1
        if nodes < 0:
            scan.complete = False
            return
        if isinstance(value, str):
            yield value
        elif isinstance(value, (Mapping, list, tuple)) and len(stack) > MAX_SCAN_DEPTH:
            scan.complete = False
        elif isinstance(value, Mapping):
            stack.append(_keys_and_values(value))
        elif isinstance(value, (list, tuple)):
            stack.append(iter(value))
        elif value is not None and not isinstance(value, (bool, int, float)):
            # Not a JSON value, so its contents were not read. A string hidden
            # inside it could be a competing source.
            scan.complete = False


_DONE = object()


def _keys_and_values(mapping: Mapping[Any, Any]) -> Iterator[Any]:
    for key, value in mapping.items():
        yield key
        yield value


def _candidate_tokens(response: Any) -> tuple[list[str], bool]:
    """The distinct ``cat1`` framings in a reply, in order, and whether it was read whole."""

    found: list[str] = []
    seen: set[str] = set()
    scan = _Scan()
    chars, starts = MAX_SCAN_CHARS, MAX_CANDIDATES
    for text in _strings(response, scan):
        if chars <= 0 or starts <= 0:
            scan.complete = False
            break
        window = text[:chars]
        chars -= len(window)
        if len(window) < len(text):
            scan.complete = False
        for start in _START.finditer(window):
            starts -= 1
            if starts < 0:
                scan.complete = False
                break
            match = _TOKEN.match(window, start.start())
            if match is None:
                continue
            if match.end() == len(window) and len(window) < len(text):
                # The window cut this framing short, so the reply's real token
                # is longer than the match. Never remember a clipped token.
                continue
            token = match.group(0)
            if len(token) <= MAX_TICKET_CHARS and token not in seen:
                seen.add(token)
                found.append(token)
    return found, scan.complete


@dataclass(frozen=True)
class _Remembered:
    """One ticket a hosted connector returned, with its UNTRUSTED claims.

    ``selectable`` is True only when the connector the ticket names returned
    it. A ticket echoed by another connector is never chosen, but it still
    counts as a possible source of a copy, so it can make a match ambiguous.
    """

    token: str
    connector: str
    tool: str
    arguments_sha256: str
    selectable: bool

    @property
    def payload(self) -> str:
        """The ``cat1.<payload>`` part."""

        return self.token.rpartition(".")[0]

    @property
    def signature(self) -> str:
        """The signature segment."""

        return self.token.rpartition(".")[2]


class TicketCarry:
    """The tickets this conversation's hosted connectors returned, and the restore rule."""

    def __init__(self) -> None:
        self._entries: list[_Remembered] = []
        # False once a reply was read only in part or a ticket was forgotten.
        # A hidden string could then be the second possible source of a copy,
        # so the carry stops restoring for the rest of the conversation. An
        # ordinary conversation never reaches either bound.
        self._whole = True

    def observe(self, tool_name: str, response: Any, *, servers: frozenset[str]) -> int:
        """Remember the well-formed tickets one hosted connector tool returned.

        Args:
            tool_name: The live name of the tool that produced ``response``.
            response: The tool's reply as the SDK hands it to a PostToolUse hook.
            servers: The hosted connector servers reached through the caller
                proxy (``ApprovalGate.proxied_connector_servers``). A reply
                from any other tool is ignored.

        Returns:
            How many tickets became selectable sources (tickets naming another
            connector are kept only as evidence and are not counted).
        """

        pair = live_pair(tool_name)
        if pair is None or pair[0] not in servers:
            return 0
        server = pair[0]
        added = 0
        tokens, complete = _candidate_tokens(response)
        if not complete:
            self.stop("a reply was read only in part")
        for token in tokens:
            try:
                claims = unverified_claims(token)
            except TicketError:
                continue
            # A connector may only supply tickets that name itself. A ticket
            # echoed by another connector is kept as evidence only: it is never
            # chosen, but a copy that matches it and another ticket is ambiguous.
            own = claims.connector == server
            index = next(
                (i for i, entry in enumerate(self._entries) if entry.token == token), None
            )
            if index is not None:
                if own and not self._entries[index].selectable:
                    self._entries[index] = replace(self._entries[index], selectable=True)
                    added += 1
                continue
            self._entries.append(
                _Remembered(
                    token=token,
                    connector=claims.connector,
                    tool=claims.tool,
                    arguments_sha256=claims.arguments_sha256,
                    selectable=own,
                )
            )
            if own:
                added += 1
        if len(self._entries) > MAX_REMEMBERED:
            # A forgotten ticket could be the competing source of a later copy.
            del self._entries[: len(self._entries) - MAX_REMEMBERED]
            self.stop("a remembered ticket was forgotten")
        return added

    def stop(self, reason: str) -> None:
        """Stop restoring for the rest of the conversation.

        Called when a string the model may have seen can no longer be counted:
        a reply read only in part, a forgotten ticket, or an observation that
        failed. Every later ``restore`` returns None and ``other_sources``
        returns True.
        """

        if self._whole:
            logger.info("approval ticket carry stops for this conversation: %s", reason)
        self._whole = False

    def other_sources(self, copy: object, token: str) -> bool:
        """Whether ``copy`` could also be a damaged copy of something other than ``token``.

        True when any remembered ticket other than ``token`` matches the copy's
        evidence, or when the carry can no longer tell (``_whole`` is False).
        The approved call's repair uses this so it is never less careful than
        the creation path.
        """

        if not self._whole:
            return True
        return any(
            entry.token != token and damaged_copy_of(copy, entry.token) for entry in self._entries
        )

    def restore(self, tool_name: str, tool_input: Mapping[str, Any]) -> dict[str, Any] | None:
        """The call's arguments with the ticket its copy came from, or None to leave them.

        None in every case the rule does not cover: the call carries no
        top-level ``approval_ticket`` that is a damaged copy (see
        ``damaged_copy_of``), it is not a connector tool, the copy's evidence
        names no remembered ticket or several (across every connector and
        tool, echoes included), the one it names was echoed by another
        connector or belongs to another connector or tool, or that ticket was
        issued for other arguments.
        """

        copy = tool_input.get(TICKET_ARGUMENT)
        pair = live_pair(tool_name)
        if pair is None or not isinstance(copy, str) or not self._whole:
            return None
        sources = [entry for entry in self._entries if damaged_copy_of(copy, entry.token)]
        if len(sources) != 1:
            if sources:
                logger.info(
                    "approval ticket carry left %s unchanged: %d possible sources",
                    tool_name,
                    len(sources),
                )
            return None
        (source,) = sources
        if not source.selectable or (source.connector, source.tool) != pair:
            return None
        try:
            digest = execution_arguments_sha256(dict(tool_input))
        except TicketError:
            return None
        if source.arguments_sha256 != digest:
            # The copy came from a ticket for other details. Repairing it would
            # not make this call match; leave the API's refusal to say so.
            return None
        logger.info("approval ticket carry restored the connector's ticket for %s", tool_name)
        restored = dict(tool_input)
        restored[TICKET_ARGUMENT] = source.token
        return restored


def damaged_copy_of(copy: object, token: str) -> bool:
    """Whether ``copy`` is a damaged copy of ``token``, by evidence in the copy itself.

    A damaged copy is a string that starts like a ticket, is no longer than a
    ticket may be, no longer parses as a complete ticket, and still carries
    either ``token``'s exact ``cat1.<payload>`` part or its exact signature
    segment. A copy that still parses is never a damaged copy: a same-length
    change inside the signature cannot be told apart from a different ticket.
    """

    if not isinstance(copy, str) or not copy.startswith("cat1.") or copy == token:
        return False
    if len(copy) > MAX_TICKET_CHARS:
        return False
    try:
        unverified_claims(copy)
    except TicketError:
        pass
    else:
        return False
    payload, _, signature = copy.rpartition(".")
    source_payload, _, source_signature = token.rpartition(".")
    return payload == source_payload or signature == source_signature
