"""Derive a turn's attachment REFERENCES from a Slack event's ``files`` (#2567).

ADR-0020's required core for a channel port names attachments alongside text,
author and correlation key. Before this module the dispatcher named only the
text: a message carrying both a comment and an upload reached the worker with
the comment intact and every file gone, which is the same silent-loss class
``inbound_text`` closes, one field over.

**This runs BESIDE ``derive_text``, never inside it, and that is the whole
design.** ``derive_text``'s docstring makes the non-empty top-level ``text``
passthrough a *deliberate* byte-identical exception -- a decision from #2006 --
so the fix for a dropped file cannot be "also fold the filenames into the
prompt": that would reverse a decision this ticket has no authority over, and it
would change the prompt of every enqueue that works today. Refs therefore ride
their own ``QueuedTurn.attachments`` field, populated here, from the same event
and independently of whatever the text derivation made of it. The one overlap is
deliberate: a *bare* file share still derives its filename as the prompt through
``inbound_text._emit_file``, because a file dropped with no comment must produce
some prompt; that path is untouched and this module adds to it rather than
replacing it.

**Naming, because Slack overloads the word.** Slack's ``attachments`` key is the
*legacy message attachment* (a pretext/title/fields card, handled by
``inbound_text``); Slack's uploaded files live under ``files``. This module reads
``files`` only, and produces the ACI's ``Attachment`` -- a file reference. The
two senses never meet.

**References, not bytes, and not a URL.** ``Attachment`` carries no url by
design (see its docstring). The reason is the *sandbox*, not the token: this
change grants the bot ``files:read``, so the platform side can fetch, but a
sandbox still cannot -- ADR-0075's Agent Proxy, the host-bound credential
injection such a fetch would need, is Accepted but UNBUILT, and what is live is
ADR-0032's release-wide CIDR egress, which is default-deny. A url on the wire
would therefore be a promise the sandbox cannot keep, and it would invite exactly
the fetch that egress policy exists to refuse. What is carried is the Slack file
id -- adapter-scoped and opaque -- so the platform side resolves it and hands the
bytes down by the route the sandbox *can* use.

**Never raises, on any payload.** The same contract ``derive_text`` promises, for
a sharper reason: this runs *after* the idempotency claim and *after* the
placeholder is already visible in the thread, so a raise here is strictly worse
than a dropped ref. Bolt has acked, the claim survives its TTL, the placeholder
sits in the channel forever, and Slack's redelivery is refused as an already-seen
delivery (``queue.release_event``'s asymmetry, #2006). A ref that cannot be built
is therefore skipped and the turn is minted anyway, and the loss is bounded to
that one entry rather than to the whole array -- Slack ships new file fields on
its own schedule, and one unreadable entry must not cost a multi-file upload
every ref it had.
"""

from typing import Any

from aci_protocol import Attachment

#: Maximum refs carried from one delivery. Slack's own client caps a message at
#: ten files, so this is headroom rather than a policy; it exists because the
#: array arrives from outside and an unbounded loop over it is an unbounded cost
#: on the ingest path, exactly as ``inbound_text``'s budgets are.
_MAX_ATTACHMENTS = 50


def _non_blank_str(value: object) -> str | None:
    """Return ``value`` when it is a usable identifying string, else ``None``.

    Blank is refused alongside absent and wrongly-typed: an ``id`` of ``""``
    resolves to nothing just as surely as a missing one, and a ref that reads as
    resolvable and is not is the shape this whole seam avoids. ``bool`` is not a
    concern here because a bool is not a ``str``.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _to_attachment(entry: object) -> Attachment | None:
    """Build one ref from one Slack file object, or ``None`` if it cannot be.

    Slack's file object (https://docs.slack.dev/reference/objects/file-object)
    names the identity keys ``id`` and ``name`` and the descriptive keys
    ``mimetype`` (one word) and ``size`` (bytes). Our wire spells the latter two
    ``mime_type`` and ``size_bytes``, so the mapping is explicit here; getting it
    wrong would be silent, since the refs would still arrive with both
    descriptive fields ``None``.

    Every value is type-checked before it is passed to ``Attachment`` rather than
    letting pydantic coerce or reject it. That is what makes the never-raise
    promise structural: construction cannot fail on a payload this function
    accepted. ``id`` or ``name`` missing means no ref at all -- ``Attachment``
    requires both, and inventing either would mint a pointer to nothing. Bad
    *descriptive* metadata costs only itself: the ref is still the useful thing,
    so an unreadable ``mimetype`` or ``size`` degrades to ``None`` instead of
    discarding the file. ``bool`` is excluded from ``size`` explicitly because
    ``isinstance(True, int)`` is True in Python and a size of ``1`` from a
    ``size: true`` is a fabricated number.
    """
    if not isinstance(entry, dict):
        return None
    file_id = _non_blank_str(entry.get("id"))
    name = _non_blank_str(entry.get("name"))
    if file_id is None or name is None:
        return None
    mimetype = entry.get("mimetype")
    size = entry.get("size")
    return Attachment(
        id=file_id,
        name=name,
        mime_type=mimetype if isinstance(mimetype, str) and mimetype.strip() else None,
        size_bytes=size if isinstance(size, int) and not isinstance(size, bool) else None,
    )


def derive_attachments(event: dict[str, Any]) -> list[Attachment]:
    """Return the attachment refs a Slack event carries, in Slack's own order.

    Reads ``files`` and nothing else. A missing, ``None``, or wrongly typed
    ``files`` (a string, a bare dict -- Slack sends an array) yields the empty
    list, and an entry this adapter cannot read is skipped while its well-formed
    neighbours are kept. Never raises on a malformed payload; the empty list is
    the honest answer for "this delivery reported no files I can reference".
    """
    files = event.get("files")
    if not isinstance(files, list):
        return []
    refs: list[Attachment] = []
    for entry in files[:_MAX_ATTACHMENTS]:
        ref = _to_attachment(entry)
        if ref is not None:
            refs.append(ref)
    return refs
