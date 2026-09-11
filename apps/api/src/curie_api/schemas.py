"""Pydantic v2 request/response models for the API surface."""

import base64
import binascii
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
from plugin_format.connector_render import agent_forges_join
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
from .hook_partition import HOOK_NAME, validate_pointer_syntax
from .models import GIT_FLOW_CREATED_BY, Environment
from .repo_full_name import RepoFullName
from .workspace_policy import REPOSITORY_FULL_NAME_PATTERN, valid_repository_name

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

# Selected only by the worker's built-in cluster-message relay.  Letting an
# operator persist this slug on a binding would shadow that trusted route with
# an arbitrary endpoint, so the write-side schema reserves it explicitly.
BUILTIN_CLUSTER_MESSAGE_ADAPTER = "curie-cluster-message"

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


def _validate_agent_name(value: str) -> str:
    """Reject an agent name that would forge the connector object-name join.

    A connector's Kubernetes objects are named
    ``{release}-{agent}-mcp-{connector}``
    (``plugin_format.connector_render.object_name``). The ``-mcp-`` is a bare
    substring inside one DNS label rather than a structural separator, so the
    join point is not recoverable from the rendered string: agent ``a-mcp-b``
    with connector ``c`` and agent ``a`` with connector ``b-mcp-c`` render
    byte-identical objects AND the identical ``app.kubernetes.io/name`` pod
    selector. The connector is deliberately unauthenticated (ADR-0086 -- the
    sandbox holds no credential to authenticate WITH, so the network is the
    whole of the access control), which makes that name the only thing binding
    a sandbox to a credential: one agent's sandbox reaches another agent's
    connector holding another agent's production token, and nothing errors
    anywhere (#1446).

    ``connectors.yaml`` names and ``deploy.yaml``'s ``target.agent`` are both
    gated by bundle validation. ``POST /agents`` is the hole -- the stored
    ``Agent.name`` reaches the renderer with no field validator in between --
    and it is the path the CLI's ``resolve_agent`` and the UI's create modal
    both take. Refusing on write keeps the forging name out of the database
    entirely; the render-time 422 in ``routers/agents.py`` only covers rows
    created before this validator existed.

    Deliberately ONLY the delimiter-forging shape. ``AgentCreate.name`` accepts
    spaces, uppercase, and 200-character names today; that is a real but
    SEPARATE pre-existing gap, and tightening it here would refuse names live
    installs already hold. Do not "helpfully" widen this into general
    name-shape validation -- that is its own change, with its own migration
    story.

    The rule itself is imported, never restated: ``agent_forges_join`` asks
    whether ``-mcp-`` appears in ``f"{name}-"``, which catches a TRAILING
    ``-mcp`` (the join supplies the dash that completes it) as surely as an
    outright ``-mcp-``, while leaving a LEADING ``mcp-`` alone -- its only
    alternative split leaves an empty agent, so nothing is ambiguous. A second
    copy of that asymmetry here would be free to drift from the renderer it
    exists to protect.
    """

    if agent_forges_join(value):
        raise ValueError(
            f"agent name {value!r} collides with the connector object-name "
            "delimiter '-mcp-': a connector's Kubernetes objects are named "
            "'{release}-{agent}-mcp-{connector}', so a name that contains "
            "'-mcp-' or ends in '-mcp' makes two different agents render the "
            "same objects and share one connector's credential (#1446). Pick a "
            "name that neither contains '-mcp-' nor ends in '-mcp'."
        )
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
    ``resolution``, which is only WHERE the interactive card posts.

    Declaring an approvers block is what lets a request sit in a broad channel
    where everyone can see it while only a narrow set may act on it. Omitting it
    keeps the zero-setup default: the resolution-card channel's members are the
    approvers. Notification recipients never enter this policy.
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


class HookPartitionConfig(BaseModel):
    """How one hook names the thing each delivery is about (ADR-0134).

    One model serves ``AgentCreate``, ``AgentUpdate`` AND ``AgentOut``, which the
    ``_StoredWithoutNulls`` tripwire above would otherwise argue against: that
    split only happens for models carrying the wrap serializer, and this one has
    neither it nor an optional field, so the dumped and validated shapes are the
    same and no ``-Input``/``-Output`` pair is generated. Do not add a separate
    ``...Out`` variant.
    """

    # A typo'd key must not be silently dropped: here the dropped key would be
    # the whole partition, and the hook would run unpartitioned while its config
    # still looked right in a GET. Same reason as `ApprovalApprovers`.
    model_config = ConfigDict(extra="forbid")

    # An RFC 6901 pointer into the delivery body.
    pointer: str

    @field_validator("pointer")
    @classmethod
    def _check_pointer(cls, value: str) -> str:
        # The ingress's own syntax rule, imported rather than restated, so a
        # pointer the write surface accepts is exactly one the resolver can read.
        return validate_pointer_syntax(value)


def _validate_hook_partitions(
    value: "dict[str, HookPartitionConfig] | None",
) -> "dict[str, HookPartitionConfig] | None":
    """Partition keys are hook NAMES, checked against the shape the ingress
    enforces.

    A key outside that shape can never match a firing, so it configures nothing
    while looking configured -- the operator sees a partition map and gets
    unpartitioned threads.
    """

    if value is None:
        return value
    for name in value:
        if not HOOK_NAME.fullmatch(name):
            raise ValueError(
                f"hook_partitions key {name!r} is not a hook name: 1-63 "
                "characters of lowercase letters, digits, dot, dash or "
                "underscore, beginning with a letter or a digit"
            )
    return value


def _validate_route_names(
    value: "dict[str, ApprovalRouteBinding] | None",
) -> "dict[str, ApprovalRouteBinding] | None":
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
      `/agents/{id}/channels` subresource (ADR-0118), so the plural key here
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
                "leave the agent bound where it already was. Send channel: "
                '{"kind": "slack", "address": "C0123ABCD"} instead.'
            )
        if "channels" in data:
            raise ValueError(
                "channels is not an agent field: a create binds exactly ONE "
                'channel, so send channel: {"kind": "slack", "address": '
                '"C0123ABCD"} here and add the rest through POST '
                "/agents/{id}/channels (ADR-0118). Creating with no binding at "
                "all would leave the agent unable to receive a turn."
            )
    return data


def _reject_retired_update_binding_key(data: Any) -> Any:
    """Refuse `channel` on an agent UPDATE (ADR-0118, #1525).

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
            "bindings (ADR-0118), so moving 'the' binding has no referent. Use "
            "the subresource, where each verb means one thing: POST "
            "/agents/{id}/channels to add a binding, PATCH "
            "/agents/{id}/channels?kind=&address= to move the one that pair "
            "names, DELETE /agents/{id}/channels?kind=&address= to remove it."
        )
    return data


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
        _validate_channel_binding(self.kind, self.address)
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

    The shape stays `{kind, address}`, identical to what `ChannelBinding`
    serialized, so this is not a wire change -- `ChannelBindingWrite`'s docstring
    already describes that as the read contract.
    """

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    kind: str
    address: str


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
        if self.adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER:
            raise ValueError(
                f"channel adapter {BUILTIN_CLUSTER_MESSAGE_ADAPTER!r} is reserved "
                "for the platform's built-in disconnected-message relay and "
                "cannot be configured on an operator binding."
            )
        if self.endpoint is not None:
            _validate_channel_endpoint(self.endpoint)
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
        if endpoint_sent != adapter_sent:
            missing = "adapter" if endpoint_sent else "endpoint"
            raise ValueError(
                f"channel route patch must send endpoint and adapter together; "
                f"{missing} was omitted"
            )
        return self


class ApprovalResolutionTarget(ChannelBinding):
    """The one target permitted to carry an approval-resolving affordance.

    ``kind`` is an explicit extension point, but it is intentionally Slack-only
    until a second adapter can present the scoped verified identity ADR-0096
    requires. Merely teaching an adapter to render buttons cannot widen this
    authority boundary.
    """

    kind: Literal["slack"]


class ApprovalNotificationTarget(ChannelBindingWrite, _StoredWithoutNulls):
    """A visibility-only approval ping target and its server-side transport.

    Slack may use the worker's configured default transport. Every other kind
    needs the full endpoint/adapter pair at write time, so a declared
    notification cannot persist as a permanently undeliverable best-effort
    branch.
    """

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

    ``resolution`` is the single verified-identity action surface.
    ``notification`` may make the pending request visible elsewhere, but its
    message carries no interaction. ``approvers`` continues to narrow WHO may
    act through the resolution card path and is never inferred from notification
    recipients.
    """

    model_config = ConfigDict(extra="forbid")

    resolution: ApprovalResolutionTarget
    notification: ApprovalNotificationTarget | None = None
    approvers: ApprovalApprovers | None = None

    @model_validator(mode="after")
    def _targets_must_differ(self) -> "ApprovalRouteBinding":
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
    # server-controlled fields so AgentOut never discloses them.
    model_config = ConfigDict(extra="ignore")


class ApprovalApproversOut(BaseModel):
    """Repair-oriented read projection of the stored approver declaration."""

    model_config = ConfigDict(extra="ignore")

    group: str | None = None
    users: list[str] | None = None


class ApprovalRouteBindingOut(BaseModel):
    """The required resolution plus optional, redacted visibility policy."""

    model_config = ConfigDict(extra="ignore")

    resolution: ApprovalTargetOut
    notification: ApprovalTargetOut | None = None
    approvers: ApprovalApproversOut | None = None


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
    repo_full_name: RepoFullName | None = None
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
    # Per-agent approval route bindings (#247/#1460): manifest route name -> one
    # verified Slack resolution target and optional visibility-only notification
    # target. None means no bindings; a named unbound route escalates.
    approval_routes: dict[str, ApprovalRouteBinding] | None = None
    # Per-agent connector secret VALUES (ADR-0009, #429): env-var-style name ->
    # secret. Stored on the agent row for the local tier and forwarded into the
    # sandbox by the worker binding. None means no connector secrets.
    secrets: dict[str, str] | None = None
    # Per-hook delivery partitioning (ADR-0134): hook name -> the JSON Pointer
    # into the delivery body that names the thing each delivery is about. None
    # (the default) is the unpartitioned behavior: one thread per hook.
    hook_partitions: dict[str, HookPartitionConfig] | None = None
    # Whether this agent's bindings share one workflow-state namespace (#1525
    # follow-up). False (the default) matches a single-binding agent's existing
    # behavior exactly, since there is nothing yet to share with.
    memory: bool = False

    _check_name = field_validator("name")(_validate_agent_name)
    _check_model = field_validator("model")(_validate_model_override)
    _check_thinking = field_validator("thinking")(_validate_thinking_override)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _check_hook_partitions = field_validator("hook_partitions")(_validate_hook_partitions)
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

    # No binding key, in either number (ADR-0118, #1525). The binding's write
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
    # New per-hook delivery partitioning (ADR-0134). Omitted (None) leaves the
    # partitions unchanged; an explicit empty dict clears them, returning every
    # hook on this agent to one thread per hook. Deliberately `approval_routes`'
    # semantics and NOT the `model`/`thinking` `model_fields_set` three-way:
    # there is no platform default for this field to be cleared back TO, so
    # reading None as "omitted" conflates nothing.
    hook_partitions: dict[str, HookPartitionConfig] | None = None
    # Which repository's pushes deploy this agent (ADR-0091). PATCHable because
    # an agent created before its repo existed -- or, until migration 0018, the
    # SECOND agent of a repo, which the unique index forbade from carrying it --
    # has no other way to be bound. Without this, git-flow cannot find that
    # agent and a target naming it is rejected as unknown.
    repo_full_name: RepoFullName | None = None
    # Whether this agent's bindings share one workflow-state namespace.
    memory: bool | None = None

    _check_model = field_validator("model")(_validate_model_override)
    _check_thinking = field_validator("thinking")(_validate_thinking_override)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _check_hook_partitions = field_validator("hook_partitions")(_validate_hook_partitions)
    _reject_retired_channel_keys = model_validator(mode="before")(_reject_retired_binding_keys)
    # The update-only half: a withdrawn `channel` here is refused, while the
    # same key stays required on `AgentCreate`.
    _reject_retired_channel_key = model_validator(mode="before")(_reject_retired_update_binding_key)


class AgentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    # Serialized from the plural `Agent.channels` relationship (ADR-0118).
    # Ordering is NOT moot now that there can be more than one: it is
    # `(kind, address)`, enforced on the relationship, because `agent_channels`
    # has no `created_at` and an unordered list makes two identical GETs differ
    # -- which re-renders the console's rows on every poll. The LOADING strategy
    # lives on the relationship too (`models.Agent.channels`, lazy="selectin").
    channels: list[ChannelBindingOut]
    repo_full_name: str | None
    behavior_packs: dict[str, Any] | None
    model: str | None
    thinking: str | None
    approval_required_tools: list[str] | None
    approval_routes: dict[str, ApprovalRouteBindingOut] | None
    # Which hooks fan out, and by what (ADR-0134). Null is the unpartitioned
    # posture and the value every pre-existing agent row carries.
    hook_partitions: dict[str, HookPartitionConfig] | None
    # Connector secret NAMES only (#429) -- values are never returned. The stored
    # column is a name->value map; expose just the sorted names so an operator can
    # see which secrets an agent has bound without the material leaving the API.
    secrets: list[str] | None
    # Whether this agent's bindings share one workflow-state namespace (#1525
    # follow-up).
    memory: bool
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


class RoutingCheckRequest(BaseModel):
    """Ask whether a repository's pushes can still be routed to an agent (#1221).

    Migration 0018 (ADR-0091) dropped the unique index on ``repo_full_name``, so
    binding a SECOND agent to a repository is legal -- and silently flips every
    future push for the agent that was already bound from "deploys" to
    "rejected", because nothing says which of the two a branch belongs to. The
    caller sends the bundle's ``deploy.yaml`` TEXT for the same reason
    ``ResolveTargetRequest`` does: the API owns the resolver rule, so a client
    restating it here would drift from the rule actually enforced on a push.
    """

    repo_full_name: str
    # The bundle's deploy.yaml TEXT, or None when the bundle has no such file.
    # None and an empty `targets:` map say the same thing about routing (#1210),
    # and the resolver already treats them identically.
    content: str | None = None


class RoutingCheckProblem(BaseModel):
    """One environment whose pushes this repository can no longer route.

    ``message`` is the resolver's OWN text, carried verbatim so the CLI can
    print it without paraphrasing the rule.
    """

    environment: str
    code: str
    message: str


class RoutingCheck(BaseModel):
    """Whether pushes to a repository still resolve to an agent (#1221).

    ``resolvable`` is false only when the real resolver raised: a branch with no
    matching target resolves to "ignore", which is intended behaviour, not a
    problem. An unbound repository (``agent_count`` 0) stays resolvable too --
    this reports ROUTING, not whether anything is bound.
    """

    repo_full_name: str
    agent_count: int = 0
    agents: list[str] = Field(default_factory=list)
    resolvable: bool = True
    unresolvable: list[RoutingCheckProblem] = Field(default_factory=list)


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
    # Retained as a compatibility-only deployment field; it is not a runtime
    # coding gate. The worker-wide workspace coordinator plus an allowed root
    # GitHub URL determine whether claim-time repository acquisition occurs.
    workspace_enabled: bool | None = None
    status: str = "active"

    _check_commit_sha = field_validator("commit_sha")(_validate_optional_commit_sha)


class DeploymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version_id: uuid.UUID
    environment: Environment
    commit_sha: str | None
    workspace_enabled: bool
    status: str
    deployed_at: datetime


class RepositoryCredentialOut(BaseModel):
    """One server-derived Git credential returned only to the trusted worker."""

    repo_full_name: str
    clone_url: str
    authorization_header: str


class WorkspaceSelectionRequest(BaseModel):
    conversation_id: str = Field(min_length=1)
    author: str = Field(min_length=1)
    repo_full_name: str | None = None

    @field_validator("repo_full_name")
    @classmethod
    def _canonical_repo(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not valid_repository_name(value):
            raise ValueError("repo_full_name must be one canonical owner/repository name")
        return value


class WorkspaceSelectionOut(BaseModel):
    repo_full_name: str | None


class WorkspaceCredentialRequest(BaseModel):
    conversation_id: str = Field(min_length=1)


class PublicationCreate(BaseModel):
    """Trusted snapshot facts used to atomically create approval + publication."""

    deployment_id: uuid.UUID
    conversation_id: str = Field(min_length=1)
    reply_conversation_id: str | None = Field(default=None, min_length=1)
    repo_full_name: str = Field(pattern=REPOSITORY_FULL_NAME_PATTERN)
    author: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    reply_kind: str = Field(min_length=1)
    reply_channel: str = Field(min_length=1)
    reply_placeholder: str | None = None
    reply_endpoint: str | None = None
    reply_adapter: str | None = None
    dedupe_key: str = Field(min_length=1)
    review_origin_key: str | None = Field(default=None, min_length=1, max_length=180)
    base_sha: str
    patch_b64: str = Field(min_length=1)
    changed_paths: list[str] = Field(min_length=1, max_length=4096)
    expires_in_seconds: int | None = Field(default=None, ge=1)
    title: str | None = Field(default=None, max_length=256)
    body: str | None = Field(default=None, max_length=65_536)

    @field_validator("repo_full_name")
    @classmethod
    def _canonical_publication_repo(cls, value: str) -> str:
        if not valid_repository_name(value):
            raise ValueError("repo_full_name must be one canonical owner/repository name")
        return value

    @field_validator("base_sha")
    @classmethod
    def _full_base_sha(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            raise ValueError("base_sha must be one full 40-character hexadecimal commit id")
        return value.lower()

    @model_validator(mode="after")
    def _valid_reply_route(self) -> "PublicationCreate":
        _validate_channel_binding(self.reply_kind, self.reply_channel)
        builtin_relay = self.reply_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER
        if builtin_relay and self.reply_endpoint is not None:
            raise ValueError(
                "the built-in cluster-message publication reply route must not set an endpoint"
            )
        if not builtin_relay and ((self.reply_endpoint is None) != (self.reply_adapter is None)):
            raise ValueError("publication reply route must set endpoint and adapter together")
        if self.reply_adapter is not None and not _CHANNEL_KIND.match(self.reply_adapter):
            raise ValueError("publication reply adapter must be a lowercase slug")
        if self.reply_endpoint is not None:
            _validate_channel_endpoint(self.reply_endpoint)
        return self

    @field_validator("changed_paths")
    @classmethod
    def _safe_changed_paths(cls, value: list[str]) -> list[str]:
        for path in value:
            parts = path.split("/")
            if tuple(part.casefold() for part in parts[:2]) == (
                ".github",
                "workflows",
            ):
                raise ValueError("GitHub workflow changes cannot be published by this capability")
            if (
                not path
                or path.startswith("/")
                or parts[0].casefold() == ".git"
                or any(part in ("", ".", "..") for part in parts)
            ):
                raise ValueError("changed_paths must contain safe repository-relative paths")
        return value

    def decoded_patch(self) -> bytes:
        try:
            return base64.b64decode(self.patch_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("patch_b64 must be canonical base64") from exc


class ReviewRevisionReserve(BaseModel):
    repository_id: int = Field(gt=0, strict=True)
    pr_number: int = Field(gt=0, strict=True)
    expected_lineage_version: int = Field(ge=1, strict=True)
    origin_key: str = Field(min_length=1, max_length=180)


class ReviewRevisionOut(BaseModel):
    revision_id: uuid.UUID
    lineage_id: uuid.UUID
    agent_id: uuid.UUID
    conversation_id: str
    reply_conversation_id: str
    binding_id: uuid.UUID
    binding_generation: int
    repository_id: int
    installation_id: int
    pr_node_id: str
    base_ref: str
    repo_full_name: str
    pr_number: int
    branch: str
    base_sha: str
    expected_head_sha: str
    lineage_version: int
    revision_number: int
    version: int
    status: Literal["reserved", "consumed", "cancelled"]


class ReviewRevisionCancel(BaseModel):
    origin_key: str = Field(min_length=1, max_length=180)
    expected_version: int = Field(ge=1, strict=True)


class PublicationLineageAdvance(BaseModel):
    """Exact compare-and-set facts for one publication revision outcome."""

    expected_version: int = Field(ge=1)
    expected_head_sha: str | None
    state: Literal["open", "merged", "closed"] = "open"
    pr_number: int = Field(gt=0)
    pr_url: str = Field(min_length=1, max_length=2048)
    head_sha: str

    @field_validator("expected_head_sha", "head_sha")
    @classmethod
    def _full_commit_sha(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            raise ValueError("lineage head must be one full 40-character commit id")
        return value.lower()


class PublicationLineageOut(BaseModel):
    """Credential-free pull-request lineage facts safe for the worker."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    deployment_id: uuid.UUID
    conversation_id: str
    repo_full_name: str
    base_sha: str
    branch: str
    pr_number: int | None
    pr_url: str | None
    head_sha: str | None
    state: Literal["open", "merged", "closed"] = Field(validation_alias="status")
    version: int
    latest_revision: int
    # This is intentionally only a boolean. The worker needs to know whether a
    # fenced replacement would race an unresolved revision, but must not learn
    # that revision's identifier or private patch state.
    has_pending_revision: bool = False
    # True while a terminal publication outcome has not yet been acknowledged
    # by the durable transcript outbox. No private patch or error text crosses
    # this read seam.
    has_pending_outcome: bool = False
    # Monotonic within one lineage. The worker stores this on its route so a
    # headless denial/failure causes exactly one cold history rehydrate even
    # though the Git head itself did not move.
    visible_outcome_revision: int = Field(default=0, ge=0)


class PublicationOut(BaseModel):
    """Patch-free publication metadata safe for operator and worker reads."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    approval_id: uuid.UUID
    deployment_id: uuid.UUID
    lineage_id: uuid.UUID | None
    revision_number: int | None
    expected_prior_head: str | None
    lineage_base_sha: str | None
    lineage_head_sha: str | None
    lineage_state: Literal["open", "merged", "closed"] | None
    lineage_version: int | None
    branch: str | None
    pr_number: int | None
    pr_url: str | None
    repo_full_name: str
    status: str
    version: int
    base_sha: str
    changed_paths: list[str]
    title: str
    body: str
    reply_kind: str
    reply_channel: str
    reply_placeholder: str | None
    reply_endpoint: str | None
    reply_adapter: str | None
    result_url: str | None
    error: str | None
    created_at: datetime
    updated_at: datetime
    terminal_at: datetime | None


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


class ActionRecord(BaseModel):
    """The opening frame of a side-effecting call, as the worker forwards it.

    ``dedupe_key`` is the triggering event id and the call id. The worker
    redelivers at least once (ADR-0013), so a replayed turn must adopt the record
    it already wrote rather than mint a second account of one call.
    """

    agent_id: uuid.UUID | None = None
    conversation_id: str
    call_id: str
    tool: str
    arguments: dict[str, Any] | None = None
    detail: str | None = None
    # The approval that gated this call, when one did. The worker knows it
    # because a gated call only executes on an approval-resume turn.
    gate_approval_id: uuid.UUID | None = None
    dedupe_key: str


class ActionComplete(BaseModel):
    """The closing frame: what came back, and what it takes to put it back.

    ``prior_state`` and ``target`` are what a restore replays. Both are optional
    because a connector that answers in prose reports neither, and a record that
    holds neither is not undoable -- which is the honest answer rather than a
    missing one.
    """

    failed: bool = False
    result: dict[str, Any] | None = None
    prior_state: dict[str, Any] | None = None
    post_state: dict[str, Any] | None = None
    target: dict[str, Any] | None = None
    detail: str | None = None


class ActionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID | None
    conversation_id: str
    call_id: str
    tool: str
    arguments: dict[str, Any] | None
    result: dict[str, Any] | None
    prior_state: dict[str, Any] | None
    post_state: dict[str, Any] | None
    target: dict[str, Any] | None
    detail: str | None
    gate_approval_id: uuid.UUID | None
    status: str
    dedupe_key: str
    created_at: datetime
    completed_at: datetime | None
    undone_at: datetime | None
    undone_by: str | None
    # Derived on the model, never stored, so a record cannot claim a
    # reversibility nothing captured the state for (ADR-0117).
    undoable: bool


class ActionUndo(BaseModel):
    """A request to put back what an action changed.

    ``observed_state`` is the resource as it looks NOW, read by whoever will
    perform the restore. The platform cannot read it itself -- nothing here can
    reach a connector -- and it will not assume: an absent observation is refused
    rather than treated as "unchanged".
    """

    actor: str
    actor_channel: str | None = None
    observed_state: dict[str, Any] | None = None


class ActionRestore(BaseModel):
    """The call an authorized undo permits: put this state back on that target."""

    target: dict[str, Any]
    prior_state: dict[str, Any]


class ActionUndoOut(BaseModel):
    """An authorization, not a receipt.

    The API rules and returns; something else performs the restore (ADR-0117
    leaves where that executor lives undecided). So this names the call to make
    rather than claiming it was made.
    """

    action: ActionOut
    restore: ActionRestore


class ActionAuditOut(BaseModel):
    """One entry in an action's audit trail: an authorized undo, or a refused one."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    action_id: uuid.UUID
    action: str
    actor: str
    actor_channel: str | None
    authorizer: str
    authorized: bool
    reason: str | None
    evidence: dict[str, Any] | None
    created_at: datetime


class ApprovalAuditOut(BaseModel):
    """One audit entry (#247): who attempted what, and the authorizer snapshot
    that counted (or refused) them."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    approval_id: uuid.UUID
    action: str
    actor: str
    actor_channel: str | None
    principal_kind: Literal["chat", "console", "operator"] | None
    authenticated: bool
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
    """Where a memory entry was learned from (#264 ``Provenance`` shape).

    ``source`` distinguishes an operator-seeded record (``operator``) from a
    session-learned one. Absent or null means learned/unspecified, matching
    records written before the operator seed path existed.
    """

    learned_from_session_id: str | None = None
    source_trace_ids: list[str] = Field(default_factory=list)
    recorded_at: str = ""
    source: str | None = None


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


class MemoryEntryCreate(BaseModel):
    """Append one operator-authored memory record (#1904).

    Provenance is stamped by the server. Extra body fields, including a
    caller-supplied provenance object, are ignored rather than trusted.
    """

    content: str

    @field_validator("content")
    @classmethod
    def _content_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("content must not be empty")
        return stripped


# --- console sessions (ADR-0083, #1044) -------------------------------------


class ConsoleLoginCodeMint(BaseModel):
    """Administrative request for one immutable subject-bound console login."""

    subject: str = Field(min_length=1)

    @field_validator("subject")
    @classmethod
    def _nonblank_subject(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("subject must not be blank")
        return value


class ConsoleLoginCodeOut(BaseModel):
    """A freshly minted login code, returned exactly once.

    The code is plaintext here because this response IS the one delivery: the CLI
    prints it for the operator to copy. Only its hash is stored.
    """

    code: str
    subject: str
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

    subject: str | None
    expires_at: datetime
