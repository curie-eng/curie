import json
import re
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from curie_internal.sealing_key import custody_reason, is_sealing_key_name
from fastapi import HTTPException
from plugin_format import is_reserved_boot_env_name
from plugin_format.connector_render import agent_forges_join
from plugin_format.connectors import ADMITS_SELF
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

from ..config import get_settings
from ..hook_partition import HOOK_NAME, validate_pointer_syntax
from ..models import (
    MAX_EXECUTION_DEADLINE_SECONDS,
    MAX_MAX_TURNS,
    MIN_EXECUTION_DEADLINE_SECONDS,
    MIN_MAX_TURNS,
)
from ..publication_policy import POLICY_APPROVE, POLICY_AUTO, validate_branch_prefix
from ..repo_full_name import RepoFullName
from ..source_binding import validate_revision, validate_source_binding_keys, validate_workload_key
from ..workspace_policy import valid_repository_name
from .approvals import ApprovalRouteBinding, ApprovalRouteBindingOut
from .channels import ChannelBindingOut, ChannelBindingWrite
from .common import (
    nullable_override_validator,
    validate_model_override,
    validate_runner_resource_value,
    validate_thinking_override,
)


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
    measured in (mirrors the durable-state ``enforce_caps`` in routers/state.py)."""
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
        if is_sealing_key_name(name):
            # @spec ACTION-EXECUTOR-16: the worker forwards these values into
            # the sandbox env, and the sealing key must reach only the hosted
            # connector, as a SecretRef in the bundle.
            raise ValueError(custody_reason(name))
        if not secret:
            raise ValueError(f"secret {name!r} has an empty value")
    return value


def _validate_agent_name(value: str) -> str:
    """Reject an agent name that forges the connector join, or is the sentinel.

    Two independent refusals share this validator:

    ``self`` is reserved (ADR-0168 decision 7): ``admits`` uses it to mean the
    agent a bundle is deployed as, and ``deploy.yaml``'s ``target.agent``
    (``deploy.bad_agent_name``) and the CLI's per-agent secret binding already
    refuse a target genuinely named that, since it would be indistinguishable
    from the sentinel. ``POST /agents`` was the remaining hole, closed here the
    same way #1446 closed the ``-mcp-`` join below.

    A connector's Kubernetes objects are named
    ``{release}-{agent}-mcp-{connector}``
    (``plugin_format.connector_render.object_name``). The ``-mcp-`` is a bare
    substring inside one DNS label rather than a structural separator, so the
    join point is not recoverable from the rendered string: agent ``a-mcp-b``
    with connector ``c`` and agent ``a`` with connector ``b-mcp-c`` render
    byte-identical objects AND the identical ``app.kubernetes.io/name`` pod
    selector. That selector is what the connector's Service and both
    NetworkPolicies bind to, and the Deployment it names carries the caller
    proxy's admits list (ADR-0086, ADR-0168 decision 7), which makes that name
    what binds a sandbox to a credential: one agent's sandbox reaches another
    agent's connector holding another agent's production token, and nothing
    errors anywhere (#1446).

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

    if value == ADMITS_SELF:
        raise ValueError(
            f"agent name {value!r} is reserved: `admits` (ADR-0168 decision 7) "
            "uses it to mean the agent a bundle is deployed as, so a target "
            "genuinely named `self` would be indistinguishable from that "
            "sentinel. Pick a different name."
        )
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


class SourceBindingEntry(BaseModel):
    """One workload's allowlisted repository and deployed revision (#2572)."""

    model_config = ConfigDict(extra="forbid")

    repository: str
    revision: str

    @field_validator("repository")
    @classmethod
    def _check_repository(cls, value: str) -> str:
        if not valid_repository_name(value):
            raise ValueError("repository must be one canonical owner/repository name")
        return value

    @field_validator("revision")
    @classmethod
    def _check_revision(cls, value: str) -> str:
        return validate_revision(value)


class SourceBindingConfig(BaseModel):
    """How one hook maps a workload identity onto a coding target (#2572)."""

    model_config = ConfigDict(extra="forbid")

    workload_pointer: str
    map: dict[str, SourceBindingEntry]

    @field_validator("workload_pointer")
    @classmethod
    def _check_pointer(cls, value: str) -> str:
        return validate_pointer_syntax(value)

    @field_validator("map")
    @classmethod
    def _check_map(cls, value: dict[str, SourceBindingEntry]) -> dict[str, SourceBindingEntry]:
        if not value:
            raise ValueError("source binding map must contain at least one workload")
        for key in value:
            validate_workload_key(key)
        return value


def _validate_source_bindings(
    value: "dict[str, SourceBindingConfig] | None",
) -> "dict[str, SourceBindingConfig] | None":
    if value is None:
        return value
    validate_source_binding_keys(value)
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


def _reject_null_publication_switches(data: Any) -> Any:
    """Policy and draft are not nullable. Prefix null clears the prefix."""

    if not isinstance(data, dict):
        return data
    if "publication_policy" in data and data["publication_policy"] is None:
        raise ValueError("publication_policy must be approve or auto")
    if "publication_draft" in data and data["publication_draft"] is None:
        raise ValueError("publication_draft must be true or false")
    return data


def _validate_publication_policy_value(value: str | None) -> str | None:
    if value is None:
        return None
    if value not in (POLICY_APPROVE, POLICY_AUTO):
        raise ValueError("publication_policy must be approve or auto")
    return value


def _validate_publication_branch_prefix(value: str | None) -> str | None:
    if value is None:
        return None
    return validate_branch_prefix(value)


class AgentCreate(BaseModel):
    name: str
    # Required, and singular. Every create path supplies exactly one binding
    # today (the CLI defaults to C0LOCALDEV), and an agent with no binding
    # cannot receive a turn -- it would look deployed and healthy while
    # answering nothing, which is #38's silent-shadow failure.
    #
    # The WRITE model: a create may also configure the reply route
    # (ADR-0096 phase 2). `AgentOut.channels` stays a list of read-only
    # `{kind, address, adapter}` routes. Additional bindings are added through
    # the subresource, never here.
    channel: ChannelBindingWrite
    repo_full_name: RepoFullName | None = None
    deploy_notifications: bool = False
    behavior_packs: BehaviorPacksConfig | None = None
    # Per-agent model id, forwarded as CURIE_MODEL at boot (#254). None uses the
    # platform default model.
    model: str | None = None
    reviewer_model: str | None = None
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
    # Per-hook workload to repository mapping (#2572). None means no hook on
    # this agent selects a coding target from a delivery.
    source_bindings: dict[str, SourceBindingConfig] | None = None
    # Whether this agent's bindings share one workflow-state namespace (#1525
    # follow-up). False (the default) matches a single-binding agent's existing
    # behavior exactly, since there is nothing yet to share with.
    memory: bool = False
    # ADR 0147. Omitted means human approval. A bundle cannot set this.
    publication_policy: Literal["approve", "auto"] = "approve"
    publication_draft: bool = False
    publication_branch_prefix: str | None = None

    _check_name = field_validator("name")(_validate_agent_name)
    _check_reviewer_model = field_validator("reviewer_model")(
        nullable_override_validator("reviewer_model", "a reviewer model id like 'claude-opus-5-5'")
    )
    _check_model = field_validator("model")(validate_model_override)
    _check_thinking = field_validator("thinking")(validate_thinking_override)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _check_hook_partitions = field_validator("hook_partitions")(_validate_hook_partitions)
    _check_source_bindings = field_validator("source_bindings")(_validate_source_bindings)
    _check_publication_prefix = field_validator("publication_branch_prefix")(
        _validate_publication_branch_prefix
    )
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
    reviewer_model: str | None = None
    # New per-agent thinking depth (#1182, ADR-0098). Same three-way semantics as
    # `model` above: omitted is unchanged, explicit null clears to the platform
    # default.
    thinking: str | None = None
    # New per-agent work-item execution deadline in seconds (#3071). Same
    # three-way semantics as `model`: omitted is unchanged, explicit null clears
    # to the platform default of 1800 s.
    execution_deadline_seconds: (
        Annotated[
            int,
            Field(ge=MIN_EXECUTION_DEADLINE_SECONDS, le=MAX_EXECUTION_DEADLINE_SECONDS),
        ]
        | None
    ) = None
    # New per-agent runner step cap (#4175), forwarded as CURIE_MAX_TURNS in the
    # agent's sandbox claim. Same three-way semantics as `model`: omitted is
    # unchanged, explicit null clears to the installation default. Strict, so a
    # string, fraction or boolean is refused rather than coerced.
    max_turns: Annotated[StrictInt, Field(ge=MIN_MAX_TURNS, le=MAX_MAX_TURNS)] | None = None
    # Per-agent runner resources (#3209). Same three-way semantics as `model`:
    # omitted is unchanged, explicit null clears to the chart block, and an
    # object sets requests and limits.
    runner_resources: dict[str, Any] | None = None
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
    # New per-hook source mapping (#2572). Omitted leaves it unchanged; an
    # explicit empty dict clears it.
    source_bindings: dict[str, SourceBindingConfig] | None = None
    # Which repository's pushes deploy this agent (ADR-0091). PATCHable because
    # an agent created before its repo existed -- or, until migration 0018, the
    # SECOND agent of a repo, which the unique index forbade from carrying it --
    # has no other way to be bound. Without this, git-flow cannot find that
    # agent and a target naming it is rejected as unknown.
    repo_full_name: RepoFullName | None = None
    deploy_notifications: bool | None = None
    # Whether this agent's bindings share one workflow-state namespace.
    memory: bool | None = None
    # Whether the runner mounts its memory tools (#1461). Omitted (None) leaves
    # it unchanged; the column is NOT NULL, so like `memory` there is no
    # default for a null to clear back to.
    memory_writes: bool | None = None
    # Omitted leaves the current publication policy. Explicit null is refused.
    # ``publication_branch_prefix`` null clears the prefix.
    publication_policy: Literal["approve", "auto"] | None = None
    publication_draft: bool | None = None
    publication_branch_prefix: str | None = None

    _check_reviewer_model = field_validator("reviewer_model")(
        nullable_override_validator("reviewer_model", "a reviewer model id like 'claude-opus-5-5'")
    )
    _check_model = field_validator("model")(validate_model_override)
    _check_thinking = field_validator("thinking")(validate_thinking_override)
    _check_runner_resources = field_validator("runner_resources")(validate_runner_resource_value)
    _check_approval_tools = field_validator("approval_required_tools")(_validate_tool_names)
    _check_approval_routes = field_validator("approval_routes")(_validate_route_names)
    _check_secrets = field_validator("secrets")(_validate_secret_map)
    _check_hook_partitions = field_validator("hook_partitions")(_validate_hook_partitions)
    _check_source_bindings = field_validator("source_bindings")(_validate_source_bindings)
    _check_publication_policy = field_validator("publication_policy")(
        _validate_publication_policy_value
    )
    _check_publication_prefix = field_validator("publication_branch_prefix")(
        _validate_publication_branch_prefix
    )
    _reject_retired_channel_keys = model_validator(mode="before")(_reject_retired_binding_keys)
    # The update-only half: a withdrawn `channel` here is refused, while the
    # same key stays required on `AgentCreate`.
    _reject_retired_channel_key = model_validator(mode="before")(_reject_retired_update_binding_key)
    _reject_null_publication = model_validator(mode="before")(_reject_null_publication_switches)


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
    deploy_notifications: bool
    behavior_packs: dict[str, Any] | None
    model: str | None
    reviewer_model: str | None
    thinking: str | None
    # Null means the platform default execution deadline (1800 s) (#3071).
    execution_deadline_seconds: int | None = None
    # Null means the installation's runner step cap (#4175).
    max_turns: int | None = None
    # Null means the chart runner resource block (#3209).
    runner_resources: dict[str, Any] | None = None
    approval_required_tools: list[str] | None
    approval_routes: dict[str, ApprovalRouteBindingOut] | None
    # Which hooks fan out, and by what (ADR-0134). Null is the unpartitioned
    # posture and the value every pre-existing agent row carries.
    hook_partitions: dict[str, HookPartitionConfig] | None
    source_bindings: dict[str, SourceBindingConfig] | None
    # Connector secret NAMES only (#429) -- values are never returned. The stored
    # column is a name->value map; expose just the sorted names so an operator can
    # see which secrets an agent has bound without the material leaving the API.
    secrets: list[str] | None
    # Whether this agent's bindings share one workflow-state namespace (#1525
    # follow-up).
    memory: bool
    # Whether the runner mounts its remember/update/forget tools (#1461).
    memory_writes: bool = False
    publication_policy: Literal["approve", "auto"] = "approve"
    publication_policy_version: int = 1
    publication_draft: bool = False
    publication_branch_prefix: str | None = None
    created_at: datetime

    @field_validator("secrets", mode="before")
    @classmethod
    def _secret_names_only(cls, value: Any) -> Any:
        return sorted(value) if isinstance(value, dict) else value
