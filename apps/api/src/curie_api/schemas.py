"""Pydantic v2 request/response models for the API surface."""

import json
import logging
import re
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

# The approval-request and eval-report payloads are declared once in the frozen
# ACI package (#492) and re-exported here, so this module stays the single import
# site for the API's request/response models. ``ApprovalRequest`` is the former
# ``ApprovalCreate``; ``EvalReport`` kept its name.
from aci_protocol import ApprovalRequest as ApprovalRequest
from aci_protocol import EvalReport as EvalReport
from fastapi import HTTPException
from plugin_format import is_reserved_boot_env_name
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from .config import get_settings
from .models import GIT_FLOW_CREATED_BY, Environment

# Slack channel IDs start with C (public/private channel), D (DM), or G (legacy
# private group) followed by uppercase-alphanumeric chars. Allowlist-shaped on
# purpose: unlike the CLI's blocklist (which only rejects a leading '#'), this
# also rejects bare names ("general"), pasted URLs, and lowercase IDs -- none of
# which the worker can route on.
_SLACK_CHANNEL_ID = re.compile(r"^[CDG][A-Z0-9]{7,}$")
# Slack user-group (subteam) IDs start with S; user IDs start with U, or W for
# enterprise-grid users. Same allowlist discipline and same reason as channels:
# a @handle or a bare name never resolves, and the S/C prefix is the whole
# distinction between a user group and a channel.
_SLACK_USERGROUP_ID = re.compile(r"^S[A-Z0-9]{7,}$")
_SLACK_USER_ID = re.compile(r"^[UW][A-Z0-9]{7,}$")

# Channel kinds are lowercase slugs: the value names the owning adapter, and
# `Slack`, `slack ` and `slack` must not be three different kinds. Shape only --
# membership is deliberately unchecked (see `_CHANNEL_ADDRESS_SHAPES`).
_CHANNEL_KIND = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")

# Any whitespace at all in an address. An address is an opaque routing key the
# worker matches on equality, so a stray space is never meaningful and always
# means the value was pasted wrong.
_ADDRESS_WHITESPACE = re.compile(r"\s")

logger = logging.getLogger(__name__)


def _nullable_override_validator(field: str, examples: str) -> Callable[[str | None], str | None]:
    """Build the gate shared by every nullable operator override (#1310, #1355, #1392).

    Refuses a blank and normalizes what it accepts: the returned value is
    stripped, so one paste stores one value no matter which surface it came
    through.

    `model` and `thinking` are the same kind of field consumed the same way, and
    they must answer an empty string the same way. `apply_model_env` reads each as
    `override if override is not None else config.<field>`, so `""` is not None and
    wins the ternary, and then `if value:` is falsy and NO boot key is emitted.
    An empty override therefore skips the platform default an operator configured
    and hands the decision to the model's own built-in -- the opposite of what
    someone typing `""` to clear a value expects. Explicit JSON null is the reset,
    and the error says so.

    Whitespace is worse than empty and is refused by the same predicate: `"  "`
    passes the falsy check downstream and is stored AND forwarded as a garbage
    value.

    #1334 attached this to `thinking` only, and its twin on the adjacent line kept
    accepting what its sibling refused. One factory, two call sites, so the next
    nullable override cannot drift either.

    None passes through: on PATCH that is the clear, on create it is "no override".
    The VOCABULARY of each field is deliberately not checked here -- a model id
    belongs to whatever harness is configured, and thinking's
    `disabled`/`adaptive`/`enabled:<n>` belongs to the runner
    (`curie_runner.thinking`) -- so swapping the harness is not a schema change.

    Args:
        field: the field name, used to open the error message.
        examples: a trailing clause naming valid values for this field.

    Returns:
        A pydantic field validator that refuses empty and whitespace-only values
        and returns every other value stripped.
    """

    def _validate(value: str | None) -> str | None:
        if value is None:
            return value
        if not value.strip():
            raise ValueError(
                f"{field} must not be empty: an empty value skips the platform "
                f"default and selects the model's own behavior, which is not what "
                f"clearing means. Send null to clear the override back to the "
                f"platform default, or {examples}."
            )
        # Normalize here, not in each client (#1392). Leading or trailing
        # whitespace is never meaningful in either override, and storing it
        # verbatim is a bug that surfaces far from its cause: a padded model id
        # is forwarded as CURIE_MODEL and rejected by the provider at the
        # agent's NEXT turn, long after the command that stored it exited 0.
        #
        # This is the API's job because the API is the gate every client passes
        # through -- the same reasoning `_validate_channel_binding` states for
        # itself ("the authoritative gate for every caller (UI, API, CLI)"). The
        # console already trimmed and the CLI did not, so the same paste stored
        # two different values depending on which surface an operator used;
        # fixing only the CLI would have left curl and every future client on
        # the old behavior.
        return value.strip()

    return _validate


def _validate_optional_commit_sha(value: str | None) -> str | None:
    if value is None:
        return None
    if not value.strip():
        raise ValueError("commit_sha must not be empty; omit it when unavailable")
    return value.strip()


_validate_thinking_override = _nullable_override_validator(
    "thinking", "a value like 'disabled', 'adaptive' or 'enabled:2000'"
)
_validate_model_override = _nullable_override_validator(
    "model", "a model id like 'claude-sonnet-5' or 'kimi-k2'"
)


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
    "slack": (_SLACK_CHANNEL_ID, _slack_shape_error),
}


def _validate_slack_channel_id(value: str | None) -> str | None:
    """Enforce a Slack channel-ID shape on an APPROVAL ROUTE's destination.

    Scoped to `ApprovalRouteBinding` since ADR-0096 (#1459). The agent's channel
    binding is validated by `_validate_channel_binding` below instead: an
    approval route's channel is a different concept that merely shared this
    validator (bf717203d), so it keeps a Slack-specific rule of its own rather
    than being dragged through a kind dispatch it has no kind for. Making it
    neutral is #1460's work, not this one's.

    None (an omitted PATCH field) passes through as a no-op.
    """
    if value is None:
        return value
    if not _SLACK_CHANNEL_ID.match(value):
        raise ValueError(_slack_shape_error(value))
    return value


def _validate_channel_binding(kind: str, address: str) -> str:
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

    if not _CHANNEL_KIND.match(kind):
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


def _validate_channel_endpoint(endpoint: str) -> str:
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


class AppConfig(BaseModel):
    """Open app-level config the UI reads before auth (org/workspace name)."""

    org_name: str


class LoadPackConfig(BaseModel):
    """Rotating "working..." load lines for one agent. Mirrors the worker's
    curie_worker.behaviorpacks.LoadPack (packs ride on agent config, not the
    frozen ACI contract, so the shape is duplicated across the layers the way
    BudgetConfig mirrors the ACI Budget)."""

    enabled: bool = False
    lines: list[str] = []


class TipsPackConfig(BaseModel):
    """Rotating capability tips for one agent (mirrors behaviorpacks.TipsPack).
    Separate from LoadPackConfig: a load line is what the agent is doing now, a
    tip advertises what it can do."""

    enabled: bool = False
    tips: list[str] = []


class GreetingPackConfig(BaseModel):
    """The deterministic greeting short-circuit content for one agent."""

    enabled: bool = False
    phrases: list[str] = []
    reply: str = ""


class HelpPackConfig(BaseModel):
    """The deterministic help / "what can you do" short-circuit for one agent."""

    enabled: bool = False
    phrases: list[str] = []
    reply: str = ""


class SettingConfig(BaseModel):
    """One declared user-editable runtime knob (mirrors behaviorpacks.Setting)."""

    key: str
    label: str = ""
    kind: str = "str"
    default: str = ""
    help: str = ""
    choices: list[str] = []
    applies_live: bool = True


class SettingsPackConfig(BaseModel):
    """An agent's declarative allowlist of editable runtime knobs (schema only;
    the override store + edit UI are a deferred runtime)."""

    enabled: bool = False
    settings: list[SettingConfig] = []


class NavPackConfig(BaseModel):
    """The no-dead-ends hub button for one agent (mirrors behaviorpacks.NavPack)."""

    enabled: bool = False
    hub_label: str = ""
    hub_command: str = ""


class BehaviorPacksConfig(BaseModel):
    """An agent's opt-in behavior packs. Validated on write and stored as JSON on
    the agent row; the worker parses the same JSON at bind time."""

    model_config = ConfigDict(from_attributes=True)

    load: LoadPackConfig = LoadPackConfig()
    tips: TipsPackConfig = TipsPackConfig()
    greeting: GreetingPackConfig = GreetingPackConfig()
    help: HelpPackConfig = HelpPackConfig()
    settings: SettingsPackConfig = SettingsPackConfig()
    nav: NavPackConfig = NavPackConfig()


def enforce_behavior_packs_size(config: BehaviorPacksConfig) -> None:
    """Reject a behavior-packs write over the per-agent byte cap (#936).

    Shared by both write paths (the PUT and the create) so the cap is a
    property of the config, not of one endpoint. Size is the serialized-JSON
    byte length of the whole config, the unit ``behavior_packs_max_bytes`` is
    measured in (mirrors the durable-state ``_enforce_caps`` in state.py)."""
    limit = get_settings().behavior_packs_max_bytes
    size = len(json.dumps(config.model_dump(), separators=(",", ":")).encode("utf-8"))
    if size > limit:
        raise HTTPException(
            413,
            f"behavior packs are {size} bytes, over the {limit}-byte cap",
        )


def _validate_tool_names(value: list[str] | None) -> list[str] | None:
    """Approval-required tool names (#245) must be non-empty, comma-free
    strings: the worker forwards the list to the runner as a comma-separated
    CURIE_APPROVAL_REQUIRED_TOOLS value, so a comma inside a name would
    silently split into two wrong gates."""
    if value is None:
        return value
    cleaned = [t.strip() for t in value]
    if any(not t or "," in t for t in cleaned):
        raise ValueError(
            "approval_required_tools entries must be non-empty tool names "
            "without commas (e.g. Bash, mcp__github__create_issue)"
        )
    return cleaned


_SECRET_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def _validate_secret_map(value: dict[str, str] | None) -> dict[str, str] | None:
    """Per-agent connector secrets (ADR-0009, #429): keys are env-var-style
    NAMES, values the secret material the worker forwards into the sandbox env.
    A non-env-var name cannot be forwarded (and would break ``.mcp.json``
    ``${VAR}`` expansion); an empty value is a misconfigure that fails connector
    auth silently, so both are rejected on write."""
    if value is None:
        return value
    for name, secret in value.items():
        if not _SECRET_NAME_RE.match(name):
            raise ValueError(
                f"secret name {name!r} must be an env-var-style name "
                "(uppercase letters, digits, underscore; not starting with a digit)"
            )
        if is_reserved_boot_env_name(name):
            # Reserved names are either CURIE_*-prefixed platform sandbox
            # boot-env keys (budget/session/credential/etc.) or one of the
            # fixed model-credential keys (ANTHROPIC_API_KEY, etc.). A
            # connector secret named that way would either clobber a boot var
            # or be silently dropped by the worker binding's reserved-key
            # guard, so reject it on write.
            raise ValueError(
                f"secret name {name!r} is reserved: it is a platform boot-env, "
                "model-credential, or redirect/capture-capable key and cannot be "
                "used for a connector secret"
            )
        if not secret:
            raise ValueError(f"secret {name!r} has an empty value")
    return value


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
    ``channel``, which is only WHERE the card posts.

    Declaring an approvers block is what lets a request sit in a broad channel
    where everyone can see it while only a narrow set may act on it. Omitting it
    keeps the zero-setup default: the card channel's members are the approvers.
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

    @field_validator("group")
    @classmethod
    def _check_group(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not _SLACK_USERGROUP_ID.match(value):
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
            if not _SLACK_USER_ID.match(user):
                raise ValueError(
                    f"approvers user {user!r} is not a Slack user ID: pass the "
                    "ID (e.g. U0123ABCD, or W0123ABCD on enterprise grid), not "
                    "a @handle or a display name."
                )
        return value

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ApprovalApprovers":
        if self.group is None and self.users is None:
            raise ValueError(
                "approvers must declare at least one of group or users; omit "
                "the approvers block entirely to keep channel membership"
            )
        return self


class ApprovalRouteBinding(_StoredWithoutNulls):
    """One workspace binding for a manifest-declared approval route (#247):
    the Slack channel whose members are that route's approvers (under the
    channel-membership authorizer), and optionally the ``approvers`` block that
    narrows WHO may act (#420), leaving ``channel`` to mean only WHERE the card
    posts.
    """

    # Rejects a typo'd ``approver`` rather than storing a channel-only binding
    # the operator believes narrows authority. Pre-#420 bindings are
    # ``{"channel": ...}`` only, so forbidding extras does not reject them.
    model_config = ConfigDict(extra="forbid")

    channel: str
    approvers: ApprovalApprovers | None = None

    _check_channel = field_validator("channel")(_validate_slack_channel_id)


def _validate_route_names(
    value: dict[str, ApprovalRouteBinding] | None,
) -> dict[str, ApprovalRouteBinding] | None:
    """Route names must be non-empty; they are matched verbatim against the
    manifest's declared route names."""
    if value is None:
        return value
    if any(not name.strip() for name in value):
        raise ValueError("approval_routes keys must be non-empty route names")
    return value


def _reject_retired_binding_keys(data: Any) -> Any:
    """Refuse retired agent-binding keys on an agent write (#1459).

    Two keys are named here, both superseded by the channel-neutral binding:

    - `slack_channel` WAS the agent's binding until migration 0021 replaced it
      with `channel: {kind, address}`.
    - `channels` (plural) is not a CREATE field: a create binds exactly one
      channel and every binding after it is written through the
      `/agents/{id}/channels` subresource (ADR-0116), so the plural key here
      describes a shape this endpoint has never had.

    These models inherit pydantic's `extra="ignore"`, so without this a
    `PATCH /agents/{id}` carrying either key validates into an EMPTY
    `AgentUpdate` and returns 200 having changed nothing -- the caller is told
    its rebind succeeded while the agent still answers on the old binding.
    Silent misrouting is the #38 shadow failure the binding rules exist to
    prevent, so the removal has to be loud.

    Narrow on purpose: the keys are named, not `extra="forbid"`. Forbidding
    ALL unknown keys would also turn every FUTURE field into a hard 422
    against an older platform, and the CLI leans on that tolerance today -- it
    sends `repo_full_name` to platforms that predate #1194 and reads the
    RESPONSE to tell whether the field landed (`cli/src/api.rs`). This model
    rejects only the keys whose meaning was deliberately withdrawn or never
    existed; unknown-key tolerance across releases is untouched.

    Runs `mode="before"`, since by `mode="after"` the extra key is already gone.
    """

    if isinstance(data, dict):
        if "slack_channel" in data:
            raise ValueError(
                "slack_channel is no longer an agent field: it was replaced by "
                "the channel-neutral binding (ADR-0096), so sending it would "
                'leave the agent bound where it already was. Send channel: '
                '{"kind": "slack", "address": "C0123ABCD"} instead.'
            )
        if "channels" in data:
            raise ValueError(
                "channels is not an agent field: a create binds exactly ONE "
                'channel, so send channel: {"kind": "slack", "address": '
                '"C0123ABCD"} here and add the rest through POST '
                "/agents/{id}/channels (ADR-0116). Creating with no binding at "
                "all would leave the agent unable to receive a turn."
            )
    return data


def _reject_retired_update_binding_key(data: Any) -> Any:
    """Refuse `channel` on an agent UPDATE (ADR-0116, #1525).

    Separate from `_reject_retired_binding_keys` rather than a flag on it,
    because `AgentCreate` must keep ACCEPTING `channel` -- a create still binds
    exactly one. Only the update surface withdrew the key.

    `PATCH /agents/{id}` with `channel: {...}` meant "move the agent's only
    binding". With several bindings that sentence has no referent, and widening
    it to "add, or move, depending" would silently turn a redeploy against a
    different channel into an accumulate. Left merely undeclared it would be
    worse still: `extra="ignore"` parses the retired key into an AgentUpdate
    with nothing set, so the caller is told 200 while the agent keeps answering
    on its old address -- #38's silent misroute, reached by a caller who read
    last release's docs.

    Runs `mode="before"`, since by `mode="after"` the extra key is already gone.
    """

    if isinstance(data, dict) and "channel" in data:
        raise ValueError(
            "channel is no longer an agent field: an agent may hold several "
            "bindings (ADR-0116), so moving 'the' binding has no referent. Use "
            "the subresource, where each verb means one thing: POST "
            "/agents/{id}/channels to add a binding, PATCH "
            "/agents/{id}/channels?kind=&address= to move the one that pair "
            "names, DELETE /agents/{id}/channels?kind=&address= to remove it."
        )
    return data


class ChannelBinding(BaseModel):
    """Where one agent listens: a channel KIND and an ADDRESS (ADR-0096, #1459).

    An agent holds ONE OR MORE of these (ADR-0116, #1525, amending ADR-0089's
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
        _validate_channel_binding(self.kind, self.address)
        return self


class ChannelBindingWrite(ChannelBinding):
    """The WRITE side of a binding: the public pair plus its reply ROUTE.

    A separate model from `ChannelBinding` because that one doubles as the
    element type of `AgentOut.channels` in RESPONSES, and the read contract is
    exactly
    `{kind, address}` (ADR-0096 phase 2, EB-A18 as relocated). `endpoint` and
    `adapter` are server-controlled facts an operator configures at bind time --
    where this kind's replies go back through, and which egress credential
    authenticates them -- so they are durable on the row and ABSENT from every
    read. A write-side policy on the shared model would 422 valid reads and
    leak a write rule into a read contract.

    Three rules, stated here because this is the write path every caller (UI,
    API, CLI) passes through; `agent_channels_route_pair_ck` states the first of
    them at the database for out-of-band writers:

    - **Both or neither.** A half-configured route is an operator error that
      would otherwise surface as a fail-closed escalation mid-turn, in the
      worker, far from the request that caused it.
    - **Both absent is legal**, for every kind. The 0024 CHECK permits both-NULL,
      migration 0024 backfills every existing row to exactly that, and the
      cutover binds the agent first and PATCHes the route in later. The gate for
      an unroutable binding is `POST /channels/token`, which refuses (409) to
      mint for a non-`slack` binding with no route -- `slack`'s route is
      legitimately implicit (the worker's configured Slack origin).
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
        if (self.endpoint is None) != (self.adapter is None):
            missing = "adapter" if self.endpoint is not None else "endpoint"
            present = "endpoint" if missing == "adapter" else "adapter"
            raise ValueError(
                f"channel route is half-configured: {present} is set but "
                f"{missing} is not. A reply route needs both halves -- where the "
                "reply goes (endpoint) and which egress credential authenticates "
                f"it (adapter) -- so set {missing} too, or send neither and "
                "configure the route later."
            )
        if self.adapter is not None and not _CHANNEL_KIND.match(self.adapter):
            raise ValueError(
                f"channel adapter {self.adapter!r} is not an adapter name: an "
                "adapter names the egress identity whose credential authenticates "
                "the reply and is used as a config key by the worker, so it must "
                "be a lowercase slug (e.g. 'agentmail-sandbox', 'ms-teams')."
            )
        if self.endpoint is not None:
            _validate_channel_endpoint(self.endpoint)
        return self


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
        if endpoint_sent != adapter_sent:
            missing = "adapter" if endpoint_sent else "endpoint"
            raise ValueError(
                f"channel route patch must send endpoint and adapter together; "
                f"{missing} was omitted"
            )
        return self


class AgentCreate(BaseModel):
    name: str
    # Required, and singular. Every create path supplies exactly one binding
    # today (the CLI defaults to C0LOCALDEV), and an agent with no binding
    # cannot receive a turn -- it would look deployed and healthy while
    # answering nothing, which is #38's silent-shadow failure.
    #
    # The WRITE model: a create may also configure the reply route (ADR-0096
    # phase 2). `AgentOut.channels` stays a list of read-only `{kind, address}`
    # pairs. Additional bindings are added through the subresource, never here.
    channel: ChannelBindingWrite
    repo_full_name: str | None = None
    behavior_packs: BehaviorPacksConfig | None = None
    # Per-agent model id, forwarded as CURIE_MODEL at boot (#254). None uses the
    # platform default model.
    model: str | None = None
    # Per-agent thinking depth, forwarded as CURIE_THINKING at boot (#1182,
    # ADR-0098). None uses the platform default.
    thinking: str | None = None
    # Per-agent permission gates (#245): tool names requiring human approval.
    # None means no gates (the bypass posture).
    approval_required_tools: list[str] | None = None
    # Per-agent approval route bindings (#247): manifest route name -> workspace
    # channel. None means no bindings (unbound routes fall back to the
    # requesting channel).
    approval_routes: dict[str, ApprovalRouteBinding] | None = None
    # Per-agent connector secret VALUES (ADR-0009, #429): env-var-style name ->
    # secret. Stored on the agent row for the local tier and forwarded into the
    # sandbox by the worker binding. None means no connector secrets.
    secrets: dict[str, str] | None = None

    _check_model = field_validator("model")(_validate_model_override)
    _check_thinking = field_validator("thinking")(_validate_thinking_override)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _reject_retired_channel_keys = model_validator(mode="before")(_reject_retired_binding_keys)


class AgentUpdate(BaseModel):
    """Partial update of mutable agent fields. An omitted field is unchanged.

    For the two nullable operator overrides -- `model` and `thinking` -- omitted
    and explicit JSON null are DIFFERENT requests, and the router tells them
    apart with `model_fields_set` (#1310). Omitted leaves the current value;
    explicit null clears the override back to the platform default. Reading
    `None` alone cannot distinguish the two, which is why setting one of these
    used to be a one-way door.

    The repo binding stopped being identity in ADR-0091: one repository builds
    many agents now, so binding is a routing fact, not a name.
    """

    # No binding key, in either number (ADR-0116, #1525). The binding's write
    # surface is `/agents/{id}/channels`, where add, move and remove are three
    # verbs instead of one overloaded field; `_reject_retired_update_binding_key`
    # refuses the withdrawn `channel` key loudly rather than ignoring it.
    #
    # New per-agent model id (#254). OMITTED leaves the current model unchanged;
    # explicit null clears it back to the platform default (#1310).
    model: str | None = None
    # New per-agent thinking depth (#1182, ADR-0098). Same three-way semantics as
    # `model` above: omitted is unchanged, explicit null clears to the platform
    # default.
    thinking: str | None = None
    # New permission gates (#245). Omitted (None) leaves the current gates
    # unchanged; an explicit empty list clears them.
    approval_required_tools: list[str] | None = None
    # New route bindings (#247). Omitted (None) leaves the current bindings
    # unchanged; an explicit empty dict clears them.
    approval_routes: dict[str, ApprovalRouteBinding] | None = None
    # New connector secrets (#429). Omitted (None) leaves current secrets
    # unchanged; an explicit empty dict clears them.
    secrets: dict[str, str] | None = None
    # Which repository's pushes deploy this agent (ADR-0091). PATCHable because
    # an agent created before its repo existed -- or, until migration 0018, the
    # SECOND agent of a repo, which the unique index forbade from carrying it --
    # has no other way to be bound. Without this, git-flow cannot find that
    # agent and a target naming it is rejected as unknown.
    repo_full_name: str | None = None

    _check_model = field_validator("model")(_validate_model_override)
    _check_thinking = field_validator("thinking")(_validate_thinking_override)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _reject_retired_channel_keys = model_validator(mode="before")(_reject_retired_binding_keys)
    # The update-only half: a withdrawn `channel` here is refused, while the
    # same key stays required on `AgentCreate`.
    _reject_retired_channel_key = model_validator(mode="before")(
        _reject_retired_update_binding_key
    )


class AgentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    # Serialized from the plural `Agent.channels` relationship (ADR-0116).
    # Ordering is NOT moot now that there can be more than one: it is
    # `(kind, address)`, enforced on the relationship, because `agent_channels`
    # has no `created_at` and an unordered list makes two identical GETs differ
    # -- which re-renders the console's rows on every poll. The LOADING strategy
    # lives on the relationship too (`models.Agent.channels`, lazy="selectin").
    channels: list[ChannelBinding]
    repo_full_name: str | None
    behavior_packs: dict[str, Any] | None
    model: str | None
    thinking: str | None
    approval_required_tools: list[str] | None
    approval_routes: dict[str, Any] | None
    # Connector secret NAMES only (#429) -- values are never returned. The stored
    # column is a name->value map; expose just the sorted names so an operator can
    # see which secrets an agent has bound without the material leaving the API.
    secrets: list[str] | None
    created_at: datetime

    @field_validator("secrets", mode="before")
    @classmethod
    def _secret_names_only(cls, value: Any) -> Any:
        return sorted(value) if isinstance(value, dict) else value


class GraderOut(BaseModel):
    """A deterministic grader, mirroring the frozen eval-case Grader shape
    (`apps/worker/schema/eval-cases.schema.json`). Do not let this drift from the
    worker's `Grader` model."""

    kind: Literal["exact", "contains", "regex", "tool_called"]
    expected: str
    case_sensitive: bool = False


class EvalCaseOut(BaseModel):
    """An eval case conforming to the frozen eval-case format (#8, ADR-0019):
    an input prompt plus the grader that judges the answer. Emitted by the
    promote-a-trace-to-an-eval-case endpoint (#259).

    ``shared_history`` mirrors the worker's ``EvalCase`` field (#550, ADR-0051):
    a promoted trace is a standalone case, so it emits the ``False`` default
    (fresh conversation). Kept here to satisfy the schema field-parity gate; the
    promote endpoint has no reason to mint a history-chained case.

    ``expect_status`` mirrors the frozen ``ExpectedStatus`` (#262, ADR-0053): the
    terminal session status the case asserts, default ``done``. A promoted trace
    is a completed conversation, so the emitted case keeps the default; a human
    edits it to ``awaiting-approval`` when the case should assert an approval gate
    held. Do not let this literal drift from the schema's ``ExpectedStatus`` enum."""

    id: str
    input: str
    grader: GraderOut
    shared_history: bool = False
    expect_status: Literal["done", "awaiting-approval"] = "done"


class VersionCreate(BaseModel):
    version_label: str
    bundle_ref: str | None = None
    commit_sha: str | None = None
    created_by: str

    _check_commit_sha = field_validator("commit_sha")(_validate_optional_commit_sha)

    @field_validator("created_by")
    @classmethod
    def reject_internal_provenance(cls, value: str) -> str:
        if value == GIT_FLOW_CREATED_BY:
            raise ValueError("created_by is reserved for internal Git flow versions")
        return value


class VersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version_label: str
    bundle_ref: str | None
    bundle_sha256: str | None
    commit_sha: str | None
    created_by: str
    created_at: datetime


class BundleOut(BaseModel):
    """Result of storing a bundle for a version."""

    version_id: uuid.UUID
    bundle_ref: str
    bundle_sha256: str
    size_bytes: int


class BundleFile(BaseModel):
    """One text file inside a stored bundle (path relative to the bundle root)."""

    path: str
    content: str


class BundleFiles(BaseModel):
    """The readable text surfaces of a version's stored bundle."""

    files: list[BundleFile]


class ResolveTargetRequest(BaseModel):
    """A bundle's ``deploy.yaml`` text plus the target to resolve (ADR-0089).

    The CLI sends the file CONTENT rather than parsing it, so there is exactly
    one parser for this format. Two would be a drift hazard on a file whose
    whole job is to be unambiguous about where a deploy lands -- and the Rust
    YAML ecosystem has no maintained successor to serde_yaml to pick from.
    """

    content: str
    target: str


class ResolvedTarget(BaseModel):
    """What a named target resolves to. Pure function of the file."""

    agent: str | None = None
    env: str = "dev"
    slack_channel: str | None = None


class NamedTarget(ResolvedTarget):
    """A resolved target plus the name it is declared under."""

    name: str


class ListedTargets(BaseModel):
    """Every target a ``deploy.yaml`` declares, dev before prod.

    Ordered so a caller onboarding a repository deploys dev first. A run that
    fails part-way then leaves prod BEHIND rather than ahead of a dev that
    never landed -- recoverable in one direction, not the other.
    """

    targets: list[NamedTarget] = []


class ConnectorManifests(BaseModel):
    """Kubernetes objects derived from a version's ``connectors.yaml``.

    The API renders; the caller applies. Rendering is a pure function of the
    bundle plus the deployment context the caller supplies, so producing this
    needs no cluster access and the API's read-only RBAC is untouched
    (ADR-0086, #1063).
    """

    manifests: list[dict[str, Any]] = Field(default_factory=list)
    # The Secret Curie owns for this agent, and the keys the CALLER must
    # resolve values for. Stated explicitly because the caller cannot infer it
    # from the manifests: since #1163 a connector may also reference a Secret
    # provisioned out of band, and those keys must NOT be resolved -- the whole
    # point is that the deploy path never handles them.
    owned_secret_name: str = ""
    owned_secret_keys: list[str] = Field(default_factory=list)
    # name -> the `.mcp.json` entry the agent should use. Derived from the
    # Service in `manifests`, so an author never hand-writes a URL that
    # resolves in one tier and not another.
    mcp_entries: dict[str, Any] = Field(default_factory=dict)


class DeploymentCreate(BaseModel):
    agent_id: uuid.UUID
    version_id: uuid.UUID
    environment: Environment
    commit_sha: str | None = None
    status: str = "active"

    _check_commit_sha = field_validator("commit_sha")(_validate_optional_commit_sha)


class DeploymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version_id: uuid.UUID
    environment: Environment
    commit_sha: str | None
    status: str
    deployed_at: datetime


class ApprovalResolve(BaseModel):
    """One resolution attempt. Exactly one attempt wins (compare-and-set), and
    the server-side authorizer (#246) decides first whether this actor may
    resolve at all: self-approval is blocked, and channel membership is proven
    by ``actor_channel`` -- the channel the resolution attempt was made from
    (the card click's channel, relayed by the dispatcher; asserted explicitly
    by API-key operators)."""

    decision: Literal["approved", "rejected"]
    resolved_by: str = Field(min_length=1)
    note: str | None = None
    actor_channel: str | None = None


class ApprovalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID | None
    conversation_id: str
    author: str
    summary: str
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


class ApprovalAuditOut(BaseModel):
    """One audit entry (#247): who attempted what, and the authorizer snapshot
    that counted (or refused) them."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    approval_id: uuid.UUID
    action: str
    actor: str
    actor_channel: str | None
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


class WebhookResult(BaseModel):
    """The outcome of processing a GitHub webhook event."""

    status: str
    environment: Environment | None = None
    agent_id: uuid.UUID | None = None
    version_id: uuid.UUID | None = None
    deployment_id: uuid.UUID | None = None
    commit_sha: str | None = None
    errors: list[dict[str, str]] | None = None


class ObservationNode(BaseModel):
    """One node in the reconstructed observation tree (Runs view)."""

    id: str
    type: str
    name: str | None = None
    startTime: str | None = None  # noqa: N815 (Langfuse wire field name)
    model: str | None = None
    usageDetails: dict[str, Any] | None = None  # noqa: N815
    children: list["ObservationNode"] = []


class TraceTree(BaseModel):
    """A trace plus its reconstructed observation tree."""

    trace: dict[str, Any]
    tree: list[ObservationNode]
    # The runner's sandbox id (curie.sandbox_id), hoisted out of the trace/
    # observation resource attributes; None when the trace predates the attr.
    sandbox_id: str | None = None
    # The resolved approval-gate decision (approved/rejected/expired) this turn
    # resumed from (ADR-0076 Stone 3, #889), hoisted out of the trace/
    # observation attributes; None for a turn that resumed no approval.
    approval_decision: str | None = None


class MetricsSummary(BaseModel):
    """Scalar totals for the Metrics tab stat row over a time window."""

    start: str
    end: str
    runs: int
    latency_p95_ms: float
    tokens: int
    cost_usd: float
    # False when work happened (tokens > 0) but Langfuse priced it to exactly 0 --
    # a missing model price row, not a genuinely free run (#547). Additive and
    # defaulting True, so `cost_usd` stays a non-nullable float for existing
    # clients; consumers render "unknown" rather than a misleading $0.00.
    cost_known: bool = True
    error_rate: float


class MetricPoint(BaseModel):
    ts: str
    value: float


class MetricSeries(BaseModel):
    """One metric as a time series for the Metrics tab charts."""

    metric: str
    granularity: str
    start: str
    end: str
    points: list[MetricPoint]


class PodLogs(BaseModel):
    """Runner-pod logs for the per-run runner-logs affordance."""

    namespace: str
    pod: str
    container: str | None
    logs: str


class RunnerPods(BaseModel):
    """The runner sandbox pods in a namespace (populates the Logs pod dropdown)."""

    namespace: str
    pods: list[str]


class EvalCell(BaseModel):
    """One cell of the eval matrix: a case's result on a version.

    ``model`` is the model the result was produced under (the matrix's model
    dimension), or ``None`` when the recording run carried no model tag.

    ``detail`` is the scorer's optional explanation for the verdict. It is
    distinct from a trace's ``error``, which identifies a turn that did not
    complete.

    ``plumbing_ok`` means the case ran to completion but no grader judged it (the
    fake-model tier, ADR-0055). It is a distinct status rather than a pass or a
    fail because it is neither: the fake answers from a canned script, so its cell
    carries no comparative information and must never read as a green promotion
    gate.
    """

    version: str
    status: Literal["pass", "fail", "plumbing_ok", "missing"]
    model: str | None = None
    detail: str | None = None
    stream_id: str | None = None
    scorer: Literal["grader", "trajectory"] | None = None
    case_count: int | None = None


class EvalMatrixRow(BaseModel):
    """One row of the eval matrix: a case across every version column."""

    case_id: str
    cells: list[EvalCell]


class EvalModelSummary(BaseModel):
    """A per-model rollup across the suite: pass-rate and total cost.

    The model dimension of the matrix (issue #255): the same suite run across
    models is sliceable here into ``passed/total`` pass-rate and summed
    ``cost_usd`` per model, so BYO-model work can compare which models a use case
    tolerates and at what cost. ``cost_usd`` is ``None`` when no case under this
    model reported a cost (e.g. the fake-model path), rather than a misleading 0.

    ``plumbing`` counts the rows that ran but were never graded (ADR-0055). They
    are excluded from ``passed``/``total``: counted as passes the fake model reads
    100% and counted as fails it reads 0%, both fabricated. The count keeps those
    rows visible instead of silently dropping them, so a model whose only rows are
    plumbing still appears with ``total == 0``.

    ``completed`` counts the graded rows (within ``total``) whose turn actually
    reached a verdict, as opposed to a graded FAIL that never completed at all
    (a classified failure, a turn that ended in the wrong terminal status, or a
    transport/runner exception -- see ``EvalCaseResult.error`` in the worker).
    ``total`` alone cannot tell a real 0% (every case completed and the grader
    said no) apart from a model that never produced one completed turn (issue
    #622, #526 AC4): a model whose id does not resolve, or whose runner boots but
    never answers, drives every case through the SAME classified-failure path a
    real model's bad answer never touches. A sweep row with ``total > 0`` and
    ``completed == 0`` is that distinct outcome, not a real (if unlucky) 0%.
    """

    model: str | None = None
    passed: int
    total: int
    cost_usd: float | None = None
    plumbing: int = 0
    completed: int = 0

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


class EvalModelVersionSummary(BaseModel):
    """A per-(version, model) rollup: the graded aggregates scoped to a single
    version column, not rolled across the whole shown window.

    ``EvalModelSummary`` sums ``completed`` over EVERY in-window version for a
    model. That blend can mask a triggered sha that lands all-incomplete (the
    model boots but never completes a turn on the new code) when a prior in-window
    sha completed cases for that same model: the blended ``completed`` stays ``> 0``
    from the old sha, so the "never completed" outcome the sweep must fail on
    (ADR-0068, #622) is hidden and a blended pass-rate is reported as a real
    comparison (issue #814). This per-version breakdown exposes the
    ``(version, model)`` dimension so a caller -- the CLI ``--model`` sweep, which
    knows the sha it just triggered -- can scope ``completed``/never-completed to
    that one sha instead of the window.

    Fields mirror the graded subset of ``EvalModelSummary`` (``cost_usd`` is not
    sliced per version, since the sweep does not compare cost per sha). It is
    additive and defaulted the way ``completed``/``plumbing`` already are: a caller
    that predates the field reads an empty list and degrades to the blended
    reading rather than misreporting.
    """

    version: str
    model: str | None = None
    passed: int
    total: int
    completed: int = 0
    plumbing: int = 0

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


class EvalMatrix(BaseModel):
    """The eval matrix grid: rows = cases, columns = versions (most recent first).

    ``models`` and ``model_summaries`` add the model dimension: the distinct
    models observed across the fetched traces, and a pass-rate + cost rollup per
    model for BYO-model comparison. ``model_version_summaries`` slices that same
    rollup per ``(version, model)`` so a caller can scope completion to a single
    triggered sha rather than the blended window (#814). They are additive; the
    version grid is unchanged.
    """

    suite: str
    versions: list[str]
    cases: list[str]
    rows: list[EvalMatrixRow]
    models: list[str | None] = []
    model_summaries: list[EvalModelSummary] = []
    model_version_summaries: list[EvalModelVersionSummary] = []


class EvalTriggerRequest(BaseModel):
    """Ask for an on-demand platform eval run for an agent (issue #10).

    Enqueues the same EvalJob the git-push fan-out uses, minus the
    push-only gate. With no version_id the agent's active dev deployment is
    evaluated; suite falls back to Settings.eval_default_suite when omitted.
    """

    agent_id: uuid.UUID
    version_id: uuid.UUID | None = None
    suite: str | None = None
    target_url: str | None = None
    # The model to evaluate under (#526): booted into the eval sandbox and used as
    # the run's matrix model dimension. None uses the worker default. A sweep posts
    # one trigger per model, then reads GET /evals/matrix sliced by model back.
    # Blank and whitespace-only are refused (#1389): "" is not None, so it won the
    # binding's override ternary but was then falsy, CURIE_MODEL was never emitted
    # and the run booted the BYO endpoint's own default while the matrix labelled
    # the row ''. Whitespace was worse -- it passed the falsy check and rode
    # through as a garbage model id. Send null to get the worker default.
    model: str | None = None

    _check_model = field_validator("model")(_validate_model_override)


class EvalTriggerResult(BaseModel):
    """The enqueued eval job's stream id plus the resolved job identity."""

    stream_id: str
    agent_id: uuid.UUID
    version_id: uuid.UUID
    sha: str
    suite: str
    bundle_ref: str | None
    # Echoes the requested model (#526) so a sweep caller can key each enqueued
    # job to the model it will land under in the matrix; None = worker default.
    model: str | None = None


class EvalReportResult(BaseModel):
    """The committed GitHub commit-status state for a reported eval run."""

    state: str
    sha: str


class BudgetConfig(BaseModel):
    """Per-agent budget (L1). Field names match the ACI CURIE_BUDGET so the
    worker passes them straight through; null means platform defaults."""

    model_config = ConfigDict(from_attributes=True)

    max_usd_per_day: Annotated[float, Field(gt=0)] | None = None
    max_output_tokens_per_run: Annotated[int, Field(gt=0)] | None = None


class KillState(BaseModel):
    """Whether an agent is currently killed (kill switch, L1)."""

    killed: bool


class ThreadResetState(BaseModel):
    """Whether a thread has a pending forced-sandbox-release request (#713)."""

    requested: bool


class CostReport(BaseModel):
    """Daily spend series + total for an agent (L1 Cost view)."""

    start: str
    end: str
    total_usd: float
    # False when tokens were spent in the window but Langfuse priced them to 0
    # (a missing model price row, not free usage) -- see MetricsSummary.cost_known
    # (#547). Additive, defaults True; total_usd stays a non-nullable float.
    cost_known: bool = True
    points: list[MetricPoint]


class StateEntryPut(BaseModel):
    """Write a durable state entry (#23). ``expected_version`` opts into
    compare-and-set: the write is rejected with 409 unless it matches the stored
    version (omit it for a blind upsert). ``value`` is any JSON value (object,
    array, or scalar); an array value is what ``append`` grows."""

    value: Any
    expected_version: int | None = None


class StateAppendIn(BaseModel):
    """Append ``item`` to a log-shaped (JSON array) state entry (#248). If the
    entry does not exist it is created as a single-element array; if it exists
    its value must already be an array, else the append is rejected."""

    item: Any


class StateEntryOut(BaseModel):
    """A durable state entry as returned to the caller."""

    model_config = ConfigDict(from_attributes=True)

    namespace: str
    key: str
    value: Any
    version: int
    updated_at: datetime


class StateNamespaceOut(BaseModel):
    """One namespace in an agent's durable state store, for the operator's
    read/inspect surface (#250): the namespace, how many keys it holds, and when
    it was most recently written."""

    namespace: str
    key_count: int
    last_updated: datetime


# --- Agent memory (#266 trace-back; #267 inspect/edit/delete) ---------------


class MemoryProvenanceOut(BaseModel):
    """Where a memory entry was learned from (#264 ``Provenance`` shape)."""

    learned_from_session_id: str | None = None
    source_trace_ids: list[str] = Field(default_factory=list)
    recorded_at: str = ""


class SourceTraceOut(BaseModel):
    """One resolved source trace: its id plus a link to view it in Langfuse."""

    trace_id: str
    trace_url: str


class MemoryEntryOut(BaseModel):
    """One learned memory entry as returned to an operator.

    ``index`` is the entry's current position in the memory log. It is valid for
    mutation only with this response's parent log ``version``. A mutation with
    a stale version conflicts if another change has reordered the log.
    """

    index: int
    content: str
    provenance: MemoryProvenanceOut
    version: int


class MemoryTraceBackOut(BaseModel):
    """The learned-from trace-back for one memory entry (#266).

    Resolves an entry's recorded provenance into the concrete session and source
    traces the lesson was learned from -- the answer to "how did it learn that?".
    """

    index: int
    content: str
    learned_from_session_id: str | None = None
    recorded_at: str = ""
    source_traces: list[SourceTraceOut] = Field(default_factory=list)


class MemoryEntryEdit(BaseModel):
    """Edit one memory entry using its parent log version.

    The required version prevents a stale positional index from changing an
    entry after the log has changed. Provenance is preserved.
    """

    content: str
    expected_version: int


# --- console sessions (ADR-0083, #1044) -------------------------------------


class ConsoleLoginCodeOut(BaseModel):
    """A freshly minted login code, returned exactly once.

    The code is plaintext here because this response IS the one delivery: the CLI
    prints it for the operator to copy. Only its hash is stored.
    """

    code: str
    expires_at: datetime


class ConsoleSessionExchange(BaseModel):
    """The browser's exchange request: a login code and nothing else."""

    code: str = Field(min_length=1)


class ConsoleSessionOut(BaseModel):
    """The result of an exchange. Deliberately carries NO token.

    The session token travels only as an `HttpOnly` cookie, so page script cannot
    read it -- putting it in the body would hand the credential straight back to
    the JavaScript this design exists to keep it away from.
    """

    expires_at: datetime
