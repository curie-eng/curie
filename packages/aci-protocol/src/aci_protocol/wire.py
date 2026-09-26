"""The shared eval/approval queue payloads: one declaration per wire shape.

These three payloads cross a Valkey Stream between the API (producer) and the
worker (consumer). Each was hand-written once per lane with no shared schema and
no drift gate, and two had already drifted (issue #492). They live here, in the
frozen ACI package, for the same reason ``QueuedTurn`` does: the contract is
shared across all three languages (Pydantic source of truth, with generated
TypeScript and Rust derived from the committed JSON Schema) and guarded by the
schema-compat gate, rather than hand-mirrored between the Python producer and the
Python consumer.

``QueuedTurn`` is the exemplar and is followed literally, including the detail
that these carry **no ``version`` field** -- they are queue payloads, not NDJSON
events. The Valkey Stream encoding (a single ``payload`` field holding this
model's JSON) is a transport detail and stays with the producer, outside these
models.

The per-field optionality and constraint decisions these models resolve:

**``EvalJob.bundle_ref``: REQUIRED (``str | None``, no default).** The API
already requires it at construction; the worker defaulted it to ``None``. Adopt
the API's side: required, nullable. This is a **tightening of the worker's
decoder, not a loosening of anything** -- the field is still ``| None``, so every
payload the API has ever produced still decodes. What changes is that a payload
*omitting the key entirely* now fails on the worker. Since the API is the only
producer and it always emits the key, no live payload is affected. The worker's
default was drift, not a designed tolerance.

**``EvalJob.model`` stays optional (#526).** It lets ``curie eval`` target a
named model instead of only the boot-time default so a suite can sweep N models.
It is the intended forward-compatible evolution of the single-``payload`` seam --
an older consumer ignores the field, a newer one honours it. Making it required
would break that property.

**``ApprovalRequest``: adopt the API's strict side, wholesale.** The worker's
mirror had bare types, so it could construct a payload the API 422-rejects --
the live bug stranding the durable-approval path. Resolved strict:

- ``conversation_id``, ``author``, ``summary``, ``reply_channel``, and
  ``dedupe_key``: ``min_length=1``. An empty string now raises **at the worker**,
  at construction, instead of producing a 422 from the API. Nothing that
  previously *succeeded* now fails -- those payloads were already being rejected
  downstream. The failure just moves to the source with a clear message.
- ``reply_placeholder``: required ``str | None`` with ``min_length=1`` for a
  nonnull value. ``None`` explicitly represents no placeholder to edit; omitting
  the key remains invalid, and an empty nonnull string remains invalid.
- ``gate_kind``: ``str | None`` -> ``GateKind | None``. A worker sending an
  unrecognized string now raises locally rather than 422ing. Per #544/ADR-0046
  this field is **authority-bearing** (it decides whether a gate may grant), so
  rejecting an unknown value is correct and matches the ``SessionStatus``
  precedent in ``packages/CLAUDE.md`` (control-bearing, so an unknown value is
  rejected, never degraded). It is **not** degraded to None.
- ``gate_kind`` and ``granted_tool`` nonetheless stay **nullable**, which is
  load-bearing and must not be "tightened" further: an older runner emits neither
  during a rolling deploy and the durable row's columns stay NULL (the window the
  worker's prefix fallback covers, #544). Making either required would regress
  #544 directly. Decision 2 tightens the *value domain* only.
- ``expires_in_seconds``: ``gt=0``. A worker sending ``0`` or a negative now
  raises. ``None`` is unaffected, which preserves the documented no-SLA path.
"""

import uuid
from enum import StrEnum
from typing import Any

from pydantic import Field

from .events import _AciModel


class GateKind(StrEnum):
    """Which gate raised an approval: a permission gate or a policy gate.

    A ``StrEnum`` rather than a ``Literal`` for two reasons. It is what the Rust
    generator can express (a multi-valued ``Literal`` raises; the ``Enum`` branch
    is the sanctioned path, exactly as ``SessionStatus`` uses). And the field is
    authority-bearing per #544/ADR-0046, so the value domain is a named, exported
    part of the contract rather than an inline annotation.
    """

    PERMISSION = "permission"
    POLICY = "policy"


class EvalJob(_AciModel):
    """One eval job: run ``suite`` against the version built from ``sha``.

    The shared declaration of what the API enqueues onto the eval stream and the
    worker consumes off it (formerly the API's ``EvalJobRequest`` and the
    worker's ``EvalWorkItem``, which had drifted on ``bundle_ref``).
    """

    agent_id: uuid.UUID
    version_id: uuid.UUID
    sha: str
    suite: str
    bundle_ref: str | None
    target_url: str | None = None
    model: str | None = None
    requested_at: str


class EvalReport(_AciModel):
    """An eval run's rollup, reported so the API can post the PR check.

    Byte-identical across both lanes before this promotion, so it carries no
    semantic decisions.
    """

    repo_full_name: str
    sha: str
    passed_count: int
    total: int
    target_url: str | None = None


class ApprovalRequest(_AciModel):
    """A durable approval request (#244), created by the worker when a run ends
    awaiting-approval.

    ``dedupe_key`` is the triggering event id, so a redelivered turn adopts the
    existing record instead of forking a second one. ``route``/``card_channel``
    (#247) record the manifest route the request named and the channel the worker
    routed the card to after binding resolution; the authorizer proves membership
    against ``card_channel``. ``gate_kind``/``granted_tool`` are the durable gate
    provenance (#544, Decision C) written by the runner; both stay optional for
    the rolling-deploy window.

    ``granted_arguments`` is the canonical JSON object of the denied permission
    gated call (#3255), carried independently of the human summary. It is
    optional for older producers, while an empty object remains a real value.

    ``reply_kind``/``reply_channel`` are the durable twin of ``ReplyHandle``'s
    routing pair (ADR-0096), and ``reply_adapter`` the durable twin of its egress
    selector. ``reply_kind`` is REQUIRED, deliberately unlike ``gate_kind`` above:
    an absent kind puts the silent misroute back on the resume path, where it is
    least observable, and ADR-0096 phase 2 buys the right to reject it with a
    quiescent cutover rather than a rolling-deploy window. ``reply_adapter`` is
    nullable because ``slack`` legitimately has no adapter -- its route is the
    worker's configured Slack origin.
    """

    agent_id: uuid.UUID | None = None
    conversation_id: str = Field(min_length=1)
    author: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    reply_kind: str = Field(min_length=1)
    reply_channel: str = Field(min_length=1)
    reply_placeholder: str | None = Field(min_length=1)
    reply_endpoint: str | None = None
    reply_adapter: str | None = None
    dedupe_key: str = Field(min_length=1)
    route: str | None = None
    card_channel: str | None = None
    gate_kind: GateKind | None = None
    granted_tool: str | None = None
    granted_arguments: dict[str, Any] | None = None
    # Optional SLA: seconds from creation after which the record can only
    # expire, never be approved or rejected.
    expires_in_seconds: int | None = Field(default=None, gt=0)
