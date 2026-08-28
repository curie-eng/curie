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

from enum import StrEnum

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
    body); ``slack`` legitimately carries neither, because its route is the
    worker's configured Slack origin. ``adapter`` is optional at the schema so a
    third-party or pre-upgrade producer is not rejected outright, but every
    first-party mint site sets it explicitly.
    """

    kind: str
    channel: str
    placeholder: str | None
    endpoint: str | None = None
    adapter: str | None = None


class DelegationMeta(_AciModel):
    """Provenance of an agent-to-agent delegated turn (ADR-0115).

    ``immediate_caller`` and ``accountable_principal`` are deliberately two
    fields rather than one, even though v1 sets them equal: a cycle/depth
    refusal always checks the immediate caller, and a later multi-hop trace
    back to a human principal must not force a second wire change when it
    lands. They are equal today because depth is capped at 1 (``chain`` is
    always empty for a caller minting a fresh call) -- there is no deeper hop
    to trace a principal *through* yet. Propagating a principal ACROSS a hop
    would need the calling agent's own sandbox to see its own inbound turn's
    attribution, which needs a new boot-env field threaded through the
    single-owner kernel claim path (``apps/worker/CLAUDE.md``); this change
    does not touch that file, so multi-hop accountable-principal tracing is a
    documented follow-up, not something silently approximated here.

    v1 identity is payload-level agent identity, not IdP-backed -- ADR-0115's
    maintainer decision #2, discussion #1049 still open for the IdP-backed
    story.

    ``chain`` lists the agent ids already visited by this call chain, oldest
    first, NOT including the target being asked now -- the API refuses a call
    that would revisit one of them (a cycle) or push ``depth`` past the
    configured maximum. ``depth`` is ``len(chain)`` at call time, carried
    explicitly rather than re-derived, so a consumer that only cares about the
    bound does not have to decode the whole chain to check it.
    """

    immediate_caller: str
    accountable_principal: str
    chain: list[str] = []
    depth: int = 0


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

    ``delegation`` is ``None`` for every turn except one minted by the
    ADR-0115 delegate router, which is what makes it a PATCH: an older
    consumer that has never heard of delegation ignores the field and keeps
    working, exactly as a pre-upgrade producer keeps decoding ``source``.
    """

    event_id: str
    conversation_id: str
    author: str
    text: str
    reply_handle: ReplyHandle
    received_at: str
    source: TurnSource = TurnSource.SLACK
    delegation: DelegationMeta | None = None
