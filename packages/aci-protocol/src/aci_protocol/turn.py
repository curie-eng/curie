"""The queued turn payload: the normalized inbound job an ingress adapter
enqueues and the worker consumes.

This is the channel-neutral promotion of the dispatcher's former
``QueuedSlackEvent`` (issue #7). It lives here, in the frozen ACI package, so the
contract is shared across all three languages (Pydantic source of truth, with
generated TypeScript and Rust derived from the committed JSON Schema) and guarded
by the schema-compat gate, rather than hand-mirrored between the Python producer
and the Rust CLI.

The field names are channel-agnostic so a second ingress adapter (not just Slack)
can produce and route the same payload:

    event_id        idempotency key for the delivery
    conversation_id the conversation/thread key routing keeps one live session per
    author          who authored the message
    text            the message text
    source          what started this turn: a person's message, or a job (see
                    ``TurnSource``)
    reply_handle    the routing pair plus where the reply is delivered (see
                    ``ReplyHandle``)
    received_at     ISO-8601 UTC timestamp of when the adapter received it

For the Slack adapter today, ``event_id`` is the Slack event id, ``conversation_id``
is the thread ts, ``author`` is the Slack user id, and ``reply_handle`` carries the
Slack channel plus the placeholder message ts. The Valkey Stream wire encoding (a
single ``payload`` field holding this model's JSON) is a transport detail and
stays outside this package, in the dispatcher's queue module.
"""

from collections.abc import Sequence
from enum import StrEnum
from typing import Protocol

from pydantic import model_validator

from .events import _AciModel


class TurnSource(StrEnum):
    """What started this turn: a person speaking, or the system doing a job.

    ADR-0079's new event kind. The values are the three the ADR names, and the
    distinction the kernel actually acts on is binary: ``SLACK`` is a person's
    message and may steer a live turn; ``WEBHOOK`` and ``CRON`` are jobs, and a
    job is an OUTPUT, never a steering input. ``is_job`` is that predicate, kept
    here rather than re-derived at each call site so a fourth value cannot be
    added without deciding which side of the line it falls on.

    **This is a different axis from ``ReplyHandle.kind`` and the two do not
    collapse.** ``kind`` answers *where the reply goes* (slack, email); ``source``
    answers *what caused the turn*. A nightly digest posted into a Slack channel
    is ``kind="slack"`` with ``source=CRON``: same transport as a mention, and it
    must not steer whatever conversation is live in that thread. Reading either
    field off the other is the silent misroute both exist to close.

    ``SLACK`` is the ADR's spelling for "a person's chat message", which was the
    only such ingress when ADR-0079 was accepted. A channel port turn from a
    human on another transport (an email that a person actually sent) is that
    same category on a different ``kind``, and giving it its own value is a NEW
    ENUM VALUE -- breaking under this package's rules, so it is a deliberate,
    separately-decided bump rather than something this change assumes.
    """

    SLACK = "slack"
    WEBHOOK = "webhook"
    CRON = "cron"

    @property
    def is_job(self) -> bool:
        """Is this turn a system-generated job rather than a person's message?

        Returns:
            True when the turn must never steer a live session (ADR-0079's
            "jobs are outputs, not steering inputs").
        """
        return self is not TurnSource.SLACK


#: The channel kind whose route is the worker's configured transport rather than
#: an adapter endpoint (ADR-0096 D4.4).
SLACK_KIND = "slack"

#: The identity every Slack route had before ADR-0168: the installation's one
#: Slack app (ADR-0168 decision 1).
DEFAULT_IDENTITY = "default"


def route_identity(kind: str, adapter: str | None) -> str | None:
    """The identity a route's ``adapter`` names (ADR-0168 decision 3).

    A Slack route with no adapter predates the identity: a handle still queued
    across the upgrade, an approval row decision 5 has not backfilled, a write
    from an API pod that has not rolled. It means the default app, so every
    reader compares identities through this function and never on the raw
    column. Any other kind is returned unchanged, because a NULL there is a
    binding whose route is not configured yet, not an identity.
    """

    if kind == SLACK_KIND:
        return adapter or DEFAULT_IDENTITY
    return adapter


class RouteRow(Protocol):
    """The four columns ``matching_routes`` reads off a candidate row.

    Structural on purpose: an ORM row (`apps/api`'s ``AgentChannel``), a raw
    SQLAlchemy ``Row`` from a labeled SELECT, or any other object exposing
    these four attributes satisfies it, so the one matching rule can run over
    whichever shape its caller already holds.
    """

    kind: str
    address: str
    adapter: str | None
    endpoint: str | None


def matching_routes[R: RouteRow](
    rows: Sequence[R], kind: str, address: str, adapter: str | None
) -> list[R]:
    """The rows in ``rows`` that the route triple ``(kind, address, adapter)`` selects.

    ADR-0168 decision 3's one matching rule: the API's
    `apps/api/src/curie_api/crud.py::matching_bindings` and the worker's
    binding resolver both answer "is this the same route" through this
    function, over rows they source differently -- ORM objects, a locked
    per-agent set, or a raw SQL result narrowed in Python.

    For Slack, ``adapter`` names an IDENTITY and the match is on the RESOLVED
    identity (``route_identity``), never the raw column, so an omitted
    adapter means 'default'. For any other kind, an omitted adapter selects
    every row on ``(kind, address)`` -- migration 0023's pair constraint holds
    that to one row until the contract migration for ADR-0168 decision 3
    (#3100), so a caller narrowing to one row sees at most one, and a
    non-Slack ``adapter=other`` selects none.
    """

    wanted = route_identity(kind, adapter)
    same_route = [r for r in rows if r.kind == kind and r.address == address]
    matches = [
        r
        for r in same_route
        if (adapter is None and kind != SLACK_KIND) or route_identity(r.kind, r.adapter) == wanted
    ]
    if not matches and kind == SLACK_KIND and adapter is None:
        # The pre-ADR custom-transport Slack binding (e.g. the offline
        # hook-approval proof rig) stores a
        # CREDENTIAL slug in `adapter`, not an identity, so it never matches
        # the 'default' identity above. Its old callers never send `adapter`
        # either, so the omitted-adapter selector falls back to the one row
        # on this pair that carries an endpoint. Retired by the contract
        # migration for ADR-0168 decision 3 (#3100), which refuses a Slack
        # endpoint outright.
        matches = [r for r in same_route if r.endpoint is not None]
    return matches


class ReplyHandle(_AciModel):
    """Channel-neutral coordinates for where a turn's reply is delivered.

    ``kind`` and ``channel`` are the **routing pair** (ADR-0096): the channel kind
    (``slack``, ``email``, ...) plus the address within that kind. Both halves are
    needed to resolve the binding, because one address can legitimately exist under
    two kinds. ``kind`` is REQUIRED and deliberately has no default: an optional
    kind forces the resolver to invent one, and every honest answer there is an
    address-only fallback or a ``"slack"`` guess -- the silent misroute this field
    exists to close.

    The reply model supports editing an existing reply or carrying no existing
    reply reference. ``placeholder`` is a required, nullable, opaque correlation
    handle minted by the adapter. The worker never parses it and only ever hands
    it back to the adapter that minted it. For the Slack adapter it happens to be
    the ts of the preposted placeholder message. For another kind it is whatever
    that adapter needs to find the same message again. ``None`` means this turn
    has no existing reply to edit.

    ``endpoint`` is the per-turn reply target: the base URL of the channel API the
    worker delivers this turn's reply through. It routes the reply back to the
    ingress that enqueued the turn instead of a worker-global setting, so two
    ingress paths (a real Slack workspace and a no-Slack CLI stub) can coexist on
    one worker (issue #19). ``None`` means "use the worker's configured default"
    (its ``slack_api_base_url``, i.e. real Slack), so a producer that does not set
    it keeps the pre-#19 behavior.

    ``adapter`` names the egress adapter identity whose credential authenticates
    the reply, so a sink call made *before* binding resolution can still select the
    right credential. For non-Slack kinds ``endpoint`` and ``adapter`` are both
    **platform-set from the binding row** (never accepted from an ingress request
    body); ``slack`` carries no endpoint, because its route is the worker's
    configured Slack origin, and names its identity in ``adapter`` (ADR-0168
    decision 3); a NULL there means ``DEFAULT_IDENTITY`` (see ``route_identity``).
    ``adapter`` is optional at the schema so a third-party or pre-upgrade
    producer is not rejected outright, but every first-party mint site sets it
    explicitly.
    """

    kind: str
    channel: str
    placeholder: str | None
    endpoint: str | None = None
    adapter: str | None = None


class Attachment(_AciModel):
    """An inbound attachment REFERENCE a turn carries: never the bytes (#2567).

    ADR-0020's required core for a channel port names attachments alongside text,
    author and correlation key, so a turn has to be able to say "a file came with
    this". This model is what it says it with, and it is deliberately a *pointer*
    rather than a payload.

    ``id`` is **adapter-scoped and opaque**. It is whatever identifier the channel
    that produced the turn uses for the file (a Slack file id today), and the only
    thing that can resolve it back to bytes is *that same adapter*. Nothing
    downstream parses it, and nothing outside the producing adapter should try to:
    two channels may legitimately mint the same-looking id for different files.
    ``name`` is the filename to show a person. Both are REQUIRED and have no
    default -- a ref with no id resolves to nothing and a ref with no name
    describes nothing, so either default would let a producer mint something that
    reads as present while carrying nothing actionable.

    ``mime_type`` and ``size_bytes`` are best-effort channel metadata and default
    to ``None``. A channel that reports neither must still be able to produce a
    usable ref, so the absent case is stated as ``None`` rather than fabricated as
    ``"application/octet-stream"`` or ``0``.

    **There is deliberately no url, download_url, or bytes field, and the absence
    is a decision rather than an omission.** A sandbox could not fetch a channel
    URL in this release even if the wire carried one: ADR-0075's Agent Proxy -- the
    host-bound credential injection a container would need to authenticate such a
    fetch -- is Accepted but UNBUILT, and what is live instead is ADR-0032's
    release-wide CIDR egress, which is default-deny. A url on this model would
    therefore be a promise nothing in the release can keep, and worse, it would
    invite exactly the fetch the egress policy exists to refuse -- a reader who
    found the field would reasonably assume it worked. Resolution is the producing
    adapter's job, through ``id``, when a resolve path is actually built.
    """

    id: str
    name: str
    mime_type: str | None = None
    size_bytes: int | None = None


class HookRunRef(_AciModel):
    """Identity of the scheduled hook run that produced a queued turn.

    ``slot_utc`` is an ISO 8601 timestamp with an explicit UTC offset.
    """

    agent_id: str
    name: str
    slot_utc: str


class QueuedTurn(_AciModel):
    """A normalized inbound turn ready for the worker to route and run.

    The channel-neutral promotion of the dispatcher's former ``QueuedSlackEvent``.
    The Valkey Stream carries this model as a single ``payload`` JSON field; the
    stream-encoding helpers live with the producer (the dispatcher), not on this
    frozen model, so the contract stays transport-agnostic.

    ``source`` defaults to ``SLACK`` so a pre-upgrade producer that does not set
    it still decodes, which is what makes this addition a PATCH under this
    package's change-class table rather than a breaking minor. The default is a
    compatibility affordance and not a licence to omit it: every first-party mint
    site sets it explicitly, exactly as ``ReplyHandle.adapter`` does. Defaulting
    to the non-job value is also the safe direction -- an unset ``source`` reads
    as a person's message, so a job can never be created by omission.

    ``attachments`` defaults to the EMPTY LIST for the same reason and with the
    same consequence: a pre-upgrade producer that omits the key still decodes,
    which is what makes adding it a PATCH under this package's change-class table
    rather than a breaking minor. The default is a compatibility affordance and
    not a licence to omit it -- every first-party mint site that can see files
    passes them explicitly, exactly as ``source`` and ``ReplyHandle.adapter`` are
    set explicitly. The default is the empty collection and not ``None`` so a
    consumer can write ``for ref in turn.attachments`` with no guard; "this
    channel reported no files" and "this producer predates the field" are
    deliberately the same value, because nothing downstream acts differently on
    the two.

    ``hook_run`` carries the scheduled hook identity when a hook produced the
    turn. It defaults to ``None`` so ordinary turns and pre-upgrade producers keep
    decoding unchanged.

    ``reply_handle`` is absent only for a targetless cron turn. That turn must
    carry a complete, nonblank ``hook_run`` so the worker has explicit agent and
    scheduled run identity without inventing a reply route. Every targeted turn,
    including cron, keeps the existing reply handle contract unchanged.
    """

    event_id: str
    conversation_id: str
    author: str
    text: str
    reply_handle: ReplyHandle | None = None
    received_at: str
    source: TurnSource = TurnSource.SLACK
    attachments: list[Attachment] = []
    hook_run: HookRunRef | None = None

    @model_validator(mode="after")
    def _validate_targetless_cron_identity(self) -> "QueuedTurn":
        if self.reply_handle is not None:
            return self
        if self.source is not TurnSource.CRON:
            raise ValueError("only a cron turn may omit reply_handle")
        if self.hook_run is None:
            raise ValueError("a targetless cron turn requires hook_run identity")
        if not all(
            value.strip()
            for value in (
                self.hook_run.agent_id,
                self.hook_run.name,
                self.hook_run.slot_utc,
            )
        ):
            raise ValueError("targetless cron hook_run identity must be nonblank")
        return self
