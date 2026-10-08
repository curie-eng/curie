import uuid
from datetime import datetime
from typing import Any, Literal

from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from .. import adapter_principal
from .channels import (
    CALLER_MAX_CHARS,
    EMAIL_CALLER,
    EMAIL_KIND,
    MAX_ALLOWED_CALLERS,
    SLACK_USER_ID,
    SLACK_USERGROUP_ID,
    ChannelBinding,
    ChannelBindingOut,
    ChannelBindingWrite,
    normalize_caller_id,
)


class _StoredWithoutNulls(BaseModel):
    """Serializes to the stored-JSONB shape: unset keys are absent, not null.

    Route bindings are dumped straight into ``agents.approval_routes`` by every
    persist site, and a plain dump would rewrite every pre-#420 binding with an
    ``approvers: null`` sibling (and every group-only approvers block with a
    ``users: null`` one) on the next write. Making that an invariant of the
    models themselves, rather than asking each caller for ``exclude_none=True``,
    keeps the stored shape from depending on every writer remembering.

    Tripwire for a future reader: subclasses are validation-side only today
    (request bodies), which is why the committed ``openapi.json`` carries one
    schema each. Using one in a RESPONSE model would make FastAPI split it into
    ``-Input``/``-Output`` variants, because the wrap serializer above makes the
    dumped shape differ from the validated one.
    """

    @model_serializer(mode="wrap")
    def _dump_without_nulls(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        return {k: v for k, v in handler(self).items() if v is not None}


class ApprovalApprovers(_StoredWithoutNulls):
    """WHO may resolve a route's approvals (#420), as opposed to the binding's
    ``resolution``, which is only WHERE the interactive card posts.

    Declaring an approvers block is what lets a request sit in a broad channel
    where everyone can see it while only a narrow set may act on it. Omitting it
    keeps the zero-setup default in Slack: the resolution-card channel's members
    are the approvers. Notification recipients never enter this policy.

    ``group`` and ``users`` are Slack's entries; ``emails`` is email's (ADR-0177 amendment).
    Each surface reads only its own: a Slack card never reads ``emails``, and an
    email card never reads ``users`` or ``group``.
    """

    # A typo in an optional key must not be ignored: silently dropping it would
    # leave no approvers block at read time, falling the route back to channel
    # membership and widening the approver set the operator meant to narrow.
    model_config = ConfigDict(extra="forbid")

    # A Slack user group whose current members are the approvers. Membership is
    # resolved by the API against Slack at resolve time, never asserted by the
    # caller. Ignored when ``users`` is set.
    group: str | None = None
    # An explicit allowlist of Slack user IDs. Takes precedence over ``group``
    # (issue #420 settles the precedence rather than refusing the combination),
    # and needs no Slack lookup at all.
    users: list[str] | None = None
    # An explicit list of approver email addresses (ADR-0177 amendment), read only for a
    # card shown in an email thread. Separate from the binding's
    # ``allowed_callers``: being allowed to talk to a bot is not being allowed to
    # approve what it does. Stored lowercase, the form the mail adapter reports a
    # verified sender in.
    emails: list[str] | None = None

    @field_validator("group")
    @classmethod
    def _check_group(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not SLACK_USERGROUP_ID.match(value):
            raise ValueError(
                f"approvers group {value!r} is not a Slack user-group ID: pass "
                "the ID (e.g. S0123ABCD), not a @handle or a name -- a handle "
                "never resolves, and a C-prefixed value is a channel, not a "
                "user group. Find it via the usergroups.list API."
            )
        return value

    @field_validator("users")
    @classmethod
    def _check_users(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        if not value:
            # Neither "unset" (omit the key) nor "nobody may approve": as silent
            # config the latter is a footgun, since the approval could then only
            # ever expire.
            raise ValueError("approvers users, when present, must contain at least one user ID")
        for user in value:
            if not SLACK_USER_ID.match(user):
                raise ValueError(
                    f"approvers user {user!r} is not a Slack user ID: pass the "
                    "ID (e.g. U0123ABCD, or W0123ABCD on enterprise grid), not "
                    "a @handle or a display name."
                )
        return value

    @field_validator("emails")
    @classmethod
    def _check_emails(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        if not value:
            # The same footgun as an empty ``users``: silent config for "nobody",
            # so every email approval on the route could only ever expire.
            raise ValueError("approvers emails, when present, must contain at least one address")
        stored: list[str] = []
        for address in value:
            if len(address) > CALLER_MAX_CHARS or not EMAIL_CALLER.match(address):
                raise ValueError(
                    f"approvers email {address[:CALLER_MAX_CHARS]!r} is not one bare "
                    "email address: list exact addresses like approver@example.com, "
                    "with no display name, no angle brackets and no domain-only or "
                    "wildcard entries."
                )
            normalized = normalize_caller_id(EMAIL_KIND, address)
            if normalized not in stored:
                stored.append(normalized)
        if len(stored) > MAX_ALLOWED_CALLERS:
            raise ValueError(
                f"approvers emails holds {len(stored)} distinct addresses; the limit "
                f"is {MAX_ALLOWED_CALLERS} per route."
            )
        return stored

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ApprovalApprovers":
        if self.group is None and self.users is None and self.emails is None:
            raise ValueError(
                "approvers must declare at least one of group, users or emails; "
                "omit the approvers block entirely to keep channel membership"
            )
        return self

    @property
    def slack_declared(self) -> bool:
        """Whether this block names any Slack approver (``users`` or ``group``)."""

        return self.users is not None or self.group is not None


class ApprovalResolutionTarget(ChannelBinding):
    """A fixed channel for a route's approval card.

    ``kind`` is an explicit extension point, but it is intentionally Slack-only
    until a second adapter can present the scoped verified identity ADR-0096
    requires. Merely teaching an adapter to render buttons cannot widen this
    authority boundary. A card reaches any other channel only through
    ``ApprovalRequestingSurfaceTarget``, and only in the conversation that
    asked (ADR-0177).
    """

    kind: Literal["slack"]


REQUESTING_SURFACE_MODE = "requesting_surface"


class ApprovalRequestingSurfaceTarget(BaseModel):
    """Show the card in the conversation that asked, on any channel (ADR-0177).

    The other form of a route's ``resolution``: instead of a fixed Slack
    channel, the card goes where the request was asked, exactly as a routeless
    approval's card already does. Who may answer then follows the channel the
    card lands on: Slack keeps its approver sets, and any other channel admits
    only an address on the route's approver ``emails`` (``approvers.EmailApprovers``,
    ADR-0177 amendment).

    Strict on purpose. ``mode`` is the whole object: a stray ``kind`` or
    ``address`` beside it is a mix of the two forms, which the ADR refuses
    rather than guessing which half the operator meant.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["requesting_surface"]


class ApprovalNotificationTarget(ChannelBindingWrite, _StoredWithoutNulls):
    """A visibility-only approval ping target and its server-side transport.

    A Slack target names no transport, and may name its identity in
    `adapter`. Every other kind needs the full endpoint/adapter pair at write
    time, so a declared notification cannot persist as a permanently
    undeliverable best-effort branch.
    """

    @model_validator(mode="after")
    def _default_identity_stays_implicit(self) -> "ApprovalNotificationTarget":
        # A stored JSON document, not a route row: keep the default implicit so
        # stored routes and every comparison of them are unchanged.
        if self.kind == SLACK_KIND and self.adapter == DEFAULT_IDENTITY:
            self.__dict__["adapter"] = None
        return self

    @model_validator(mode="after")
    def _require_non_slack_transport(self) -> "ApprovalNotificationTarget":
        if self.kind != "slack" and self.endpoint is None:
            raise ValueError(
                "a non-slack approval notification target requires both endpoint "
                "and adapter; only slack can use the worker's configured default "
                "transport"
            )
        return self


class ApprovalRouteBinding(_StoredWithoutNulls):
    """One strict workspace binding for a declared approval route (#1460).

    ``resolution`` is the single verified-identity action surface: a fixed
    Slack channel, or the conversation that asked (ADR-0177).
    ``notification`` may make the pending request visible elsewhere, but its
    message carries no interaction. ``approvers`` continues to narrow WHO may
    act through the resolution card path and is never inferred from notification
    recipients.
    """

    model_config = ConfigDict(extra="forbid")

    resolution: ApprovalResolutionTarget | ApprovalRequestingSurfaceTarget
    notification: ApprovalNotificationTarget | None = None
    approvers: ApprovalApprovers | None = None

    @model_validator(mode="after")
    def _emails_need_the_requesting_surface(self) -> "ApprovalRouteBinding":
        # ADR-0177 amendment A1: only a requesting_surface route shows its card in
        # an email thread. A fixed target is a Slack channel, where an address
        # can never be verified, so an email list there could only admit nobody.
        if (
            self.approvers is not None
            and self.approvers.emails is not None
            and not isinstance(self.resolution, ApprovalRequestingSurfaceTarget)
        ):
            raise ValueError(
                "approvers emails need a requesting_surface resolution: a fixed "
                "target shows its card in Slack, where an email address cannot be "
                'verified. Use {"mode": "requesting_surface"}, or list Slack users.'
            )
        return self

    @model_validator(mode="after")
    def _targets_must_differ(self) -> "ApprovalRouteBinding":
        if isinstance(self.resolution, ApprovalRequestingSurfaceTarget):
            if self.notification is not None:
                # ADR-0177 decision 1: the card already joins the thread that
                # asked, so there is nobody further to notify.
                raise ValueError(
                    "a requesting_surface resolution cannot carry a notification: "
                    "the card is already shown in the conversation that asked"
                )
            return self
        if self.notification is not None and (
            self.resolution.kind,
            self.resolution.address,
        ) == (self.notification.kind, self.notification.address):
            raise ValueError(
                "approval notification must differ from the resolution target; "
                "a duplicate target adds no notification surface"
            )
        return self


class ApprovalTargetOut(ChannelBindingOut):
    """Display-safe target identity; stored transport is write-only.

    This is deliberately a tolerant read projection. Write models validate the
    channel kind/address pair before persistence, while reads must still expose
    a malformed historical address so an operator can repair it.
    """

    # Stored bindings contain endpoint/adapter. Accept and discard those
    # server-controlled fields so AgentOut never discloses them. `endpoint` is
    # not a declared field at all, so `extra="ignore"` drops it; `adapter` IS
    # declared, inherited from `ChannelBindingOut` (ADR-0168 decision 3), but
    # a notification/resolution target's `adapter` is TRANSPORT (which egress
    # credential authenticates it), not a route identity -- decision 3 only
    # widens the read shape for an agent's own channel bindings, whose
    # `adapter` names the Slack app -- so it is excluded from serialization
    # here to keep this read shape exactly as it always was.
    model_config = ConfigDict(extra="ignore")

    adapter: str | None = Field(default=None, exclude=True)
    # A binding's caller list (ADR 0175), inherited from `ChannelBindingOut`,
    # has no meaning on an approval target, which is where a card is posted,
    # not a place a caller starts a turn; excluded for the same reason as
    # `adapter` above, so this read shape stays exactly as it was.
    allowed_callers: list[str] | None = Field(default=None, exclude=True)


class ApprovalApproversOut(BaseModel):
    """Repair-oriented read projection of the stored approver declaration."""

    model_config = ConfigDict(extra="ignore")

    group: str | None = None
    users: list[str] | None = None
    emails: list[str] | None = None


class ApprovalRequestingSurfaceTargetOut(BaseModel):
    """Read projection of ``ApprovalRequestingSurfaceTarget`` (ADR-0177).

    Tolerant like every read shape here, so a hand-edited row still reads back
    for repair instead of failing the whole agent response.
    """

    model_config = ConfigDict(extra="ignore")

    mode: str


class ApprovalRouteBindingOut(BaseModel):
    """The required resolution plus optional, redacted visibility policy."""

    model_config = ConfigDict(extra="ignore")

    resolution: ApprovalTargetOut | ApprovalRequestingSurfaceTargetOut
    notification: ApprovalTargetOut | None = None
    approvers: ApprovalApproversOut | None = None


class ApprovalResolve(BaseModel):
    """One resolution attempt. Exactly one attempt wins (compare-and-set), and
    the server-side authorizer decides whether the authenticated principal may
    resolve it. Identity and channel evidence come only from that credential;
    this body carries no caller-asserted actor fields (ADR-0106)."""

    decision: Literal["approved", "rejected"]
    note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _reject_retired_approval_identity_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        for field in ("resolved_by", "actor_channel"):
            if field in data:
                raise ValueError(
                    f"{field} is no longer accepted by approval resolution "
                    "(ADR-0106): remove the field and authenticate with an "
                    "approval principal instead"
                )
        return data


class _RecoveryRequest(BaseModel):
    """Shared shape of a break-glass request (#2753).

    ``reason`` is the ENTIRE after-the-fact review surface for an operation that
    bypasses the ordinary approver set, so a blank one is refused rather than
    stored: an unexplained administrative rejection is exactly the thing the
    accepted blast radius relies on being reviewable.

    ``recovery_key`` is caller-supplied and makes the operation idempotent. A
    retried request carrying the same key is a READ of the recorded outcome; a
    different key against an already-recovered record is a second intent and a
    conflict.
    """

    reason: str = Field(min_length=1)
    recovery_key: str = Field(min_length=1)

    @field_validator("reason", "recovery_key")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped


class ApprovalRecover(_RecoveryRequest):
    """One administrative settlement of a stranded approval.

    ``rejected`` is the only disposition there is. There is deliberately no
    approve-on-behalf-of: an administrative path that could grant would let a
    platform-key holder authorize the action a human was asked about, which is
    a different power from settling a record nobody can reach.
    """

    disposition: Literal["rejected"]


class ApprovalRecoveryOut(BaseModel):
    """The RECORDED outcome of an administrative settlement.

    Every field is read back off the row, so a replay of the same
    ``recovery_key`` renders an identical body rather than a fresh one.
    """

    model_config = ConfigDict(from_attributes=True)

    approval_id: uuid.UUID
    status: str
    recovery_key: str | None
    reason: str | None
    actor: str | None
    recovered_at: datetime | None


class ApprovalIdentityFactsOut(BaseModel):
    """Per-row FACTS about one pending approval (#2753).

    Facts, never verdicts: nothing here says a row cannot be resolved. A route
    whose approver set is the card channel's membership is a HEALTHY route that
    any attested chat click resolves, and it is reported like any other row.
    """

    id: uuid.UUID
    agent_id: uuid.UUID | None
    status: str
    route: str | None
    reply_kind: str
    reply_adapter: str | None
    reply_channel: str
    card_channel: str | None
    has_reply_placeholder: bool
    created_at: datetime
    facts: list[str]


class ApprovalReplyIdentityDeclaration(BaseModel):
    """The skeleton the migration workflow consumes.

    Everything but the id is the OPERATOR's to fill in. The report never
    pre-fills provenance it does not have: a guessed reply kind is precisely the
    silent misroute the declaration document exists to prevent.
    """

    approval_id: uuid.UUID
    reply_kind: str | None = None
    reply_adapter: str | None = None
    actor: str | None = None
    reason: str | None = None


class ApprovalIdentityReportOut(BaseModel):
    """A pure read an operator runs on a broken installation.

    It makes no Slack call and reads only ``approvals`` and ``agent_channels``,
    so it still answers against a schema old enough that the fence refuses to
    serve it.
    """

    approvals: list[ApprovalIdentityFactsOut]
    declarations: list[ApprovalReplyIdentityDeclaration]


class ApprovalPrincipalMint(BaseModel):
    """Administrative request to mint one operator approval credential."""

    subject: str = Field(min_length=1)

    @field_validator("subject")
    @classmethod
    def _nonblank_subject(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("subject must not be blank")
        return value


class ApprovalPrincipalOut(BaseModel):
    """One-time delivery of a short-lived operator approval credential."""

    token: str
    subject: str
    kind: Literal["operator"] = "operator"
    expires_at: datetime


class AdapterPrincipalMint(BaseModel):
    """Administrative request to issue one channel adapter credential (ADR-0154)."""

    subject: str = Field(min_length=1)
    binding_ids: list[uuid.UUID] = Field(min_length=1)
    ttl_s: int = Field(
        default=adapter_principal.DEFAULT_TTL_SECONDS,
        gt=0,
        le=adapter_principal.MAX_TTL_SECONDS,
    )

    @field_validator("subject")
    @classmethod
    def _nonblank_subject(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("subject must not be blank")
        return value


class AdapterPrincipalRotate(BaseModel):
    """Self-rotation request: only the next credential's lifetime is chosen."""

    ttl_s: int = Field(
        default=adapter_principal.DEFAULT_TTL_SECONDS,
        gt=0,
        le=adapter_principal.MAX_TTL_SECONDS,
    )


class AdapterPrincipalOut(BaseModel):
    """One-time delivery of a channel adapter credential."""

    token: str
    subject: str
    kind: Literal["adapter"] = "adapter"
    binding_ids: list[uuid.UUID]
    scopes: list[str]
    expires_at: datetime


class ApprovalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID | None
    conversation_id: str
    author: str
    summary: str
    display_summary: str | None = None
    reply_channel: str
    reply_placeholder: str | None
    reply_endpoint: str | None
    dedupe_key: str
    route: str | None
    card_channel: str | None
    # Gate provenance (#544): which gate fired, and the tool a grant is bound to.
    # Both NULL for a pre-#544 row. A permission gate carries granted_tool; a
    # policy gate carries it too when the operator opted the manifest gate into
    # grantability (grantableViaPolicy, #558), and NULL otherwise.
    gate_kind: str | None
    granted_tool: str | None
    status: str
    expires_at: datetime | None
    resolved_by: str | None
    resolution_note: str | None
    created_at: datetime
    resolved_at: datetime | None


class ApprovalCreateOut(ApprovalOut):
    """Display attribution for creation, separate from the durable turn author."""

    requested_by: str | None = None


class ApprovalAuditOut(BaseModel):
    """One audit entry (#247): who attempted what, and the authorizer snapshot
    that counted (or refused) them."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    approval_id: uuid.UUID
    action: str
    actor: str
    actor_channel: str | None
    principal_kind: (
        Literal["chat", "console", "operator", "adapter", "platform", "test_driver"] | None
    )
    authenticated: bool
    # The adapter that transported an `adapter` principal's decision
    # (ADR-0154); `actor` is the sender it authenticated. NULL otherwise.
    principal_subject: str | None
    decision: str
    authorizer: str
    authorized: bool
    reason: str | None
    # The membership facts that decided it (#420): the group and the actor's
    # verdict, the allowlist that counted, or the channels compared. NULL for
    # writers that made no membership decision (the expiry sweeper) and for rows
    # written before the column existed.
    evidence: dict[str, Any] | None
    created_at: datetime
