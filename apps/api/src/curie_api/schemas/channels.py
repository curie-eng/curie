import logging
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from aci_protocol.turn import SLACK_KIND, route_identity
from pydantic import BaseModel, ConfigDict, model_validator

from ..identities import refuse_undeclared

# Slack channel IDs start with C (public/private channel), D (DM), or G (legacy
# private group) followed by uppercase-alphanumeric chars. Allowlist-shaped on
# purpose: unlike the CLI's blocklist (which only rejects a leading '#'), this
# also rejects bare names ("general"), pasted URLs, and lowercase IDs -- none of
# which the worker can route on.
SLACK_CHANNEL_ID = re.compile(r"^[CDG][A-Z0-9]{7,}$")
# Slack user-group (subteam) IDs start with S; user IDs start with U, or W for
# enterprise-grid users. Same allowlist discipline and same reason as channels:
# a @handle or a bare name never resolves, and the S/C prefix is the whole
# distinction between a user group and a channel.
SLACK_USERGROUP_ID = re.compile(r"^S[A-Z0-9]{7,}$")
SLACK_USER_ID = re.compile(r"^[UW][A-Z0-9]{7,}$")

# Channel kinds are lowercase slugs: the value names the owning adapter, and
# `Slack`, `slack ` and `slack` must not be three different kinds. Shape only --
# membership is deliberately unchecked (see `_CHANNEL_ADDRESS_SHAPES`).
CHANNEL_KIND = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")

# Selected only by the worker's built-in cluster-message relay.  Letting an
# operator persist this slug on a binding would shadow that trusted route with
# an arbitrary endpoint, so the write-side schema reserves it explicitly.
BUILTIN_CLUSTER_MESSAGE_ADAPTER = "curie-cluster-message"

# Any whitespace at all in an address. An address is an opaque routing key the
# worker matches on equality, so a stray space is never meaningful and always
# means the value was pasted wrong.
_ADDRESS_WHITESPACE = re.compile(r"\s")


logger = logging.getLogger(__name__)


def _slack_shape_error(value: str) -> str:
    """#143's actionable guidance, in one place so its two callers cannot drift.

    `curie deploy --slack-channel '#name'` stored the literal name, reported
    success, and never routed, because the worker matches on the channel ID. The
    text below -- naming the About tab and the `/archives/` URL form -- IS that
    fix; a validator that keeps the rejection and drops the guidance re-opens
    #143 while the status code stays green.
    """

    return (
        f"slack channel {value!r} is not a Slack channel ID: real Slack "
        "events carry the channel ID (e.g. C0123ABCD) and the worker "
        "routes on it, so a #name or bare-name binding never receives "
        "messages. Pass the channel ID instead -- find it in the channel's "
        "About tab, or the channel URL (.../archives/C0123ABCD)."
    )


# Channel kind -> the address shape that kind requires, paired with the message
# a violation earns (ADR-0096, #1459).
#
# A kind ABSENT from this table is not rejected: it validates on the generic
# rule in `_validate_channel_binding` instead. That fallback is what makes
# "an agent binds a non-Slack channel kind without schema changes" true rather
# than aspirational -- a registry would put a code change in front of every new
# adapter, which is the coupling ADR-0096 removes. Adding an entry here is how a
# kind EARNS a stricter shape once its adapter exists, not a precondition of
# binding one.
#
# The message rides WITH the shape rather than in a second lookup: the shape is
# only half the deliverable (#143), and a kind that gains a rule and no guidance
# tells an operator their value is wrong without telling them what is right.
_CHANNEL_ADDRESS_SHAPES: dict[str, tuple[re.Pattern[str], Callable[[str], str]]] = {
    "slack": (SLACK_CHANNEL_ID, _slack_shape_error),
}


def validate_channel_binding(kind: str, address: str) -> str:
    """Enforce the address shape a channel KIND requires, and return the address.

    The authoritative gate for every caller (UI, API, CLI): the CLI and the
    console keep fast local checks purely for UX, which is only defensible while
    this one is authoritative. Reused by `AgentCreate` and `AgentUpdate` through
    `ChannelBinding`, so create and PATCH validate identically.

    Dispatch, not a chain of `if kind ==`: a registered kind (today only
    `slack`) validates on its own shape, and an UNREGISTERED kind validates on
    the generic rule -- non-empty, no whitespace -- rather than being rejected or
    silently borrowing Slack's `^[CDG][A-Z0-9]{7,}$`. Borrowing it would make
    every new adapter a schema change, which is exactly the coupling ADR-0096
    removes.

    Args:
        kind: the channel kind naming the owning adapter (a lowercase slug).
        address: the routing key the worker matches on equality.

    Returns:
        The validated address, unchanged.

    Raises:
        ValueError: the kind is not a slug, or the address fails its shape.
    """

    if not CHANNEL_KIND.match(kind):
        raise ValueError(
            f"channel kind {kind!r} is not a channel kind: a kind names the "
            "adapter that owns the binding and must be a lowercase slug "
            "(e.g. 'slack', 'webhook', 'ms-teams')."
        )
    registered = _CHANNEL_ADDRESS_SHAPES.get(kind)
    if registered is None:
        # A typo'd kind ('slak') is a well-formed slug with no adapter behind
        # it, so it creates a binding nothing will ever resolve. Nothing can
        # reject it without a kind registry -- deliberately not built (ADR-0096
        # decision 5) -- so say it once, here, at write time, instead of leaving
        # an operator to debug dead routing later.
        logger.info(
            "channel kind %r has no registered address shape; validating %r on the generic rule",
            kind,
            address,
        )
        if not address or _ADDRESS_WHITESPACE.search(address):
            raise ValueError(
                f"channel address {address!r} is not routable: an address is "
                "matched on equality by the worker, so it must be non-empty and "
                "contain no whitespace."
            )
        return address
    shape, explain = registered
    if not shape.match(address):
        raise ValueError(explain(address))
    return address


def validate_channel_endpoint(endpoint: str) -> str:
    """Enforce that a reply endpoint is a URL the worker can actually POST to.

    Unlike an address, an endpoint is not opaque: the worker hands it to
    `aiohttp` with the platform's egress credential attached, so "configured" has
    to mean more than "a non-empty string". An empty value, a bare hostname, a
    `file://` URL or a `mailto:` all pass the both-or-neither pair rule, mint a
    token, pass ingress, and only then fail closed inside the worker -- E17's
    failure one layer later, and the reason this check lives on the write path
    every caller (UI, API, CLI) shares.

    Userinfo is rejected on its own footing: a credential embedded in the stored
    URL is disclosed by every place the route is read back, and `adapter` is the
    field that names an egress credential.

    Neither the endpoint nor any fragment of it appears in the message: it can
    carry a token in its path or query, and this text is returned to the caller
    and written to logs.
    """

    parsed = urlsplit(endpoint)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(
            "channel endpoint is not a reply route: the worker POSTs the reply "
            "to it, so it must be an absolute http:// or https:// URL with a "
            "host (e.g. 'http://curie-mail-adapter:8080/'). The value is not "
            "echoed here because an endpoint can carry a credential."
        )
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "channel endpoint must not embed credentials: a user:password in the "
            "URL is stored on the binding and disclosed everywhere the route is "
            "read back. Name the egress identity in 'adapter' instead, and let "
            "the worker attach its configured credential."
        )
    return endpoint


# The caller list a binding may carry (ADR 0175 decision 1). Exact ids only: no
# wildcards, domains or patterns, because a pattern is a rule an operator has to
# reason about and an id is a fact they can check.
MAX_ALLOWED_CALLERS = 100
# The kind whose caller ids are email addresses, compared lowercase because the
# mail adapter sends the sender that way (`_bare_address` lowercases it).
EMAIL_KIND = "email"
# A Slack caller is a user (U), an enterprise-grid user (W) or a bot (B). Same
# allowlist discipline as `_SLACK_USER_ID`, widened by exactly the bot prefix:
# the dispatcher asks with the bot id when a bot sent the message.
_SLACK_CALLER_ID = re.compile(r"^[UWB][A-Z0-9]{7,}$")
# One bare address: exactly one `@`, something on each side, and none of the
# characters that would make it a display-name form, a list or a wildcard. `*`
# is legal in a mailbox name but refused here: `*@example.com` is how an
# operator spells "the whole domain", and an exact match would silently read it
# as one mailbox literally named `*`.
EMAIL_CALLER = re.compile(r'^[^@\s<>,;"()*]+@[^@\s<>,;"()*]+$')
# Longer than any real address (RFC 5321 caps a path at 256 octets) or provider
# id, so a longer entry is a paste error rather than a caller.
CALLER_MAX_CHARS = 256


def normalize_caller_id(kind: str, caller: str) -> str:
    """The form a caller id is stored and compared in, for this binding kind.

    Email ids are lowercased, exactly as the mail adapter sends a sender; every
    other kind's ids are compared as sent, because Slack ids are uppercase by
    construction and an unknown kind's ids are opaque.

    Args:
        kind: the binding's channel kind.
        caller: one caller id, as sent or as stored.

    Returns:
        The id in its comparison form.
    """

    return caller.lower() if kind == EMAIL_KIND else caller


def validate_allowed_callers(kind: str, callers: list[str] | None) -> list[str] | None:
    """Check a binding's caller list against its kind and return the stored form.

    The authoritative gate for every caller of the list's endpoint (CLI, API,
    console): None means everyone and passes through; an empty list is refused
    because one operator reads it as "no limit" and the next as "nobody"; every
    entry must be an exact id of the binding's kind. Duplicates are dropped
    (after normalizing, so two spellings of one address count once) and the
    order of first appearance is kept, so a read shows the list as written.

    Args:
        kind: the binding's channel kind, which chooses the id shape.
        callers: the list as sent, or None.

    Returns:
        None, or the deduplicated, normalized list.

    Raises:
        ValueError: the list is empty, too long, or holds an id the kind rejects.
    """

    if callers is None:
        return None
    if not callers:
        raise ValueError(
            'allowed_callers must not be empty: an empty list reads as "nobody" '
            'to one operator and "no limit" to the next. Send null to let '
            "everyone talk to the bot through this binding, or list at least one "
            "caller id."
        )
    stored: list[str] = []
    for caller in callers:
        if len(caller) > CALLER_MAX_CHARS:
            raise ValueError(
                f"caller id is longer than {CALLER_MAX_CHARS} characters, which "
                "no real id is; the value is not echoed here."
            )
        if kind == SLACK_KIND:
            if not _SLACK_CALLER_ID.match(caller):
                raise ValueError(
                    f"caller {caller!r} is not a Slack user or bot id: a Slack "
                    "binding's callers are exact ids starting with U, W or B "
                    "(e.g. U0123ABCD), never a @handle, a display name or an "
                    "email. Find a person's id in their profile, under "
                    '"Copy member ID".'
                )
        elif kind == EMAIL_KIND:
            if not EMAIL_CALLER.match(caller):
                raise ValueError(
                    f"caller {caller!r} is not one bare email address: an email "
                    "binding's callers are exact addresses like "
                    "person@example.com, with no display name, no angle "
                    "brackets and no domain-only or wildcard entries."
                )
        elif not caller or _ADDRESS_WHITESPACE.search(caller):
            raise ValueError(
                f"caller {caller!r} is not a caller id: it must be non-empty and "
                "contain no whitespace, because it is matched exactly against "
                "the id the channel reports for the sender."
            )
        normalized = normalize_caller_id(kind, caller)
        if normalized not in stored:
            stored.append(normalized)
    if len(stored) > MAX_ALLOWED_CALLERS:
        raise ValueError(
            f"allowed_callers holds {len(stored)} distinct ids; the limit is "
            f"{MAX_ALLOWED_CALLERS} per binding."
        )
    return stored


class ChannelCallersWrite(BaseModel):
    """The body of `PUT /agents/{agent_id}/channels/callers` (ADR 0175).

    `allowed_callers` is REQUIRED, with null as an explicit value: a body that
    omits it is a 422 rather than a silent "clear", so removing a binding's
    protection is always something the caller wrote down. The kind-specific
    checks run in the handler, since the kind comes from the selected binding
    rather than from this body.
    """

    model_config = ConfigDict(extra="forbid")

    allowed_callers: list[str] | None


class ChannelBinding(BaseModel):
    """Where one agent listens: a channel KIND and an ADDRESS (ADR-0096, #1459).

    An agent holds ONE OR MORE of these (ADR-0118, #1525, amending ADR-0089's
    singular clause). `AgentCreate` still carries one `channel` OBJECT -- the
    first binding, required, since an agent with none cannot receive a turn --
    `AgentOut` carries the `channels` LIST, and every write after the create
    goes through the `/agents/{id}/channels` subresource. `AgentUpdate` carries
    no binding key at all.

    `kind` names the adapter that owns the binding, selects the address-shape
    check, AND routes: since ADR-0096 phase 2 the worker resolves on the
    `(kind, address)` PAIR, so the two together are the routing key the worker
    matches on equality (a Slack channel id for `kind="slack"`). One address can
    therefore be bound twice under two different kinds.
    """

    # A typo'd `adress` or a stray `channels` nested here is a misunderstanding
    # of the contract, not a partially-honored request: accepting it would store
    # a binding the operator did not describe, and an agent bound to the wrong
    # address looks deployed and answers nothing.
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    kind: str
    address: str

    @model_validator(mode="after")
    def _check_binding(self) -> "ChannelBinding":
        # Model-level, not two field validators: the address rule is CHOSEN BY
        # the kind, so neither field can be judged alone.
        validate_channel_binding(self.kind, self.address)
        return self


class ChannelBindingOut(BaseModel):
    """The READ side of a binding: the stored pair, serialized as it is stored.

    Deliberately NOT a subclass of `ChannelBinding`, and that is the whole point.
    `ChannelBinding` carries the address-shape rule three write paths inherit
    (`ChannelBindingWrite`, `ChannelTokenRequest`, `TurnIn`), and it used to be
    the element type of `AgentOut.channels` as well -- so the rule that guards a
    BIND also ran when an existing row was READ, and one row it rejected failed
    the whole response for every agent in it (#1914).

    An install reaches that state by upgrading: migration 0021 backfills
    `agent_channels.address` from `agents.slack_channel` verbatim, and that column
    is exactly where a literal `#name` from before the validator lived. So an
    install that was merely mis-routed became one whose agent list was
    unavailable, reporting a Pydantic error instead of the bad value.

    Serializing a stored row must not re-litigate whether it should have been
    stored. Showing the bad address is also the more useful outcome: an operator
    cannot fix a value the API refuses to tell them.

    The read shape becomes `{kind, address, adapter}` (ADR-0168 decision 3):
    `adapter` is part of the route's identity, not a credential, so it belongs
    on the read side; `endpoint` stays write-only, unchanged from before.
    `ChannelBindingWrite`'s docstring still describes `endpoint`'s absence.

    `allowed_callers` (ADR 0175) is shown as stored: null for everyone, else the
    exact ids that may start a turn through this binding. It is written only by
    `PUT /agents/{agent_id}/channels/callers`, never by a binding write.
    """

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    kind: str
    address: str
    adapter: str | None = None
    allowed_callers: list[str] | None = None

    @model_validator(mode="after")
    def _present_route_identity(self) -> "ChannelBindingOut":
        # Migration 0070 names every Slack row's identity, but a stored
        # approval notification target keeps the default implicit
        # (`ApprovalNotificationTarget`), and `ApprovalTargetOut` reads it
        # through this class. `route_identity` is the one place every reader
        # compares that identity, so the presentation happens here rather than
        # on the raw column, and a non-Slack row with no adapter stays
        # untouched.
        self.__dict__["adapter"] = route_identity(self.kind, self.adapter)
        return self


class ChannelBindingWrite(ChannelBinding):
    """The WRITE side of a binding: the public pair plus its reply ROUTE.

    A separate model from `ChannelBinding` because `ChannelBindingOut`, not this
    one, is the element type of `AgentOut.channels` in RESPONSES (ADR-0096 phase
    2, EB-A18 as relocated; widened to `{kind, address, adapter}` by ADR-0168
    decision 3). `endpoint` is a server-controlled fact an operator configures
    at bind time -- where this kind's replies go back through -- so it is
    durable on the row and ABSENT from every read; `adapter` is now part of the
    route's identity rather than only a credential, so it is present on both
    sides. A write-side policy on the shared model would 422 valid reads and
    leak a write rule into a read contract.

    The rules, stated here because this is the write path every caller (UI,
    API, CLI) passes through; `agent_channels_route_ck` states the first two
    at the database for out-of-band writers:

    - **A Slack route names its identity**, `default` when omitted, and has no
      endpoint (ADR-0168 decision 3): its replies go through the worker's
      configured Slack origin, so an endpoint is refused by field name.
    - **Any other kind is both or neither.** A half-configured route is an
      operator error that would otherwise surface as a fail-closed escalation
      mid-turn, in the worker, far from the request that caused it.
    - **Both absent is legal** for a non-Slack kind: the cutover binds the
      agent first and PATCHes the route in later. The gate for an unroutable
      binding is `POST /channels/token`, which refuses (409) to mint for a
      non-`slack` binding with no route.
    - **`adapter` is a lowercase slug**, on the same pattern as `kind`, because
      it is a CONFIG-MAP KEY on the worker
      (`config.adapter_credentials[route.adapter]`): a value carrying a quote, a
      space or a `:` is a config-injection shape, not a name.
    - **`endpoint` is an absolute http(s) URL with a host and NO userinfo.** The
      worker POSTs the platform's AUTHENTICATED reply to this value, so an empty
      string or a nonsense URL is a binding that mints tokens and passes ingress
      and then fails closed mid-turn in the worker -- the same E17 failure the
      route pair exists to foreclose, arriving one layer later. Userinfo
      (`https://user:pass@host/`) is refused separately: a credential in the
      stored URL is one git-grep, one log line and one error message away from
      disclosure, and the `adapter` field is where a credential belongs.

    Neither the value nor any part of it appears in the messages below. An
    endpoint can carry a token in its path or query, so error text -- which is
    returned to the caller and written to logs -- names the FIELD and states the
    rule instead of echoing what was sent.
    """

    endpoint: str | None = None
    adapter: str | None = None

    @model_validator(mode="after")
    def _check_route(self) -> "ChannelBindingWrite":
        # The reserved-relay literal is refused before anything else reads it,
        # so it never gets the chance to read as an undeclared IDENTITY on a
        # Slack route below: "reserved" is the more specific, more actionable
        # answer, and an operator binding can never legitimately carry this
        # value under either reading.
        if self.adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER:
            raise ValueError(
                f"channel adapter {BUILTIN_CLUSTER_MESSAGE_ADAPTER!r} is reserved "
                "for the platform's built-in disconnected-message relay and "
                "cannot be configured on an operator binding."
            )

        if self.kind == SLACK_KIND:
            if self.endpoint is not None:
                # ADR-0168 decision 3: the custom-transport form is retired.
                raise ValueError(
                    "a Slack route takes no endpoint: its replies go through the "
                    "worker's configured Slack origin, and adapter names the bot "
                    "identity. Remove endpoint."
                )
            # Checked against the RESOLVED identity: an omitted adapter means
            # the default app (`route_identity`), and that is what is stored.
            identity = route_identity(self.kind, self.adapter)
            refuse_undeclared(self.kind, identity)
            # `__dict__`, not setattr: pydantic's `__setattr__` would mark an
            # omitted adapter as SENT, and a PATCH reads `model_fields_set` to
            # decide whether a route was touched at all.
            self.__dict__["adapter"] = identity
        elif (self.endpoint is None) != (self.adapter is None):
            missing = "adapter" if self.endpoint is not None else "endpoint"
            present = "endpoint" if missing == "adapter" else "adapter"
            raise ValueError(
                f"channel route is half-configured: {present} is set but "
                f"{missing} is not. A reply route needs both halves -- where the "
                "reply goes (endpoint) and which egress credential authenticates "
                f"it (adapter) -- so set {missing} too, or send neither and "
                "configure the route later."
            )

        if self.adapter is not None and not CHANNEL_KIND.match(self.adapter):
            raise ValueError(
                f"channel adapter {self.adapter!r} is not an adapter name: an "
                "adapter names the egress identity whose credential authenticates "
                "the reply and is used as a config key by the worker, so it must "
                "be a lowercase slug (e.g. 'agentmail-sandbox', 'ms-teams')."
            )
        if self.endpoint is not None:
            validate_channel_endpoint(self.endpoint)
        return self


class ClusterMessageReplyAck(BaseModel):
    """Acknowledgement for one idempotently stored relay event."""

    model_config = ConfigDict(extra="forbid")

    ref: str


class ClusterMessageReplyPage(BaseModel):
    """Cursor page read by one disconnected ``cluster message`` caller."""

    model_config = ConfigDict(extra="forbid")

    events: list[dict[str, Any]]
    next_cursor: int
    terminal: bool


class ChannelBindingPatch(ChannelBindingWrite):
    """A binding move with partial semantics for the write only reply route.

    `kind` and `address` always describe the replacement routing key. Omitting
    both route fields preserves their stored values because callers cannot read
    them back. Supplying both fields replaces them, including the explicit
    `null` pair that clears the route.
    """

    @model_validator(mode="after")
    def _check_route_presence(self) -> "ChannelBindingPatch":
        endpoint_sent = "endpoint" in self.model_fields_set
        adapter_sent = "adapter" in self.model_fields_set
        if self.kind == SLACK_KIND and adapter_sent and not endpoint_sent:
            # A Slack route is its identity alone (ADR-0168 decision 3).
            return self
        if endpoint_sent != adapter_sent:
            missing = "adapter" if endpoint_sent else "endpoint"
            raise ValueError(
                f"channel route patch must send endpoint and adapter together; "
                f"{missing} was omitted"
            )
        return self
