"""Pydantic models for the Claude Code plugin bundle shape (verbatim).

Compatibility with the Claude Code plugin format is the distribution wedge, so
these models mirror the real shapes and are deliberately permissive: unknown
keys are allowed, not rejected, because real bundles carry fields this MVP does
not model (and future Claude Code versions will add more). The models capture
the fields the platform reads; validators in ``validate.py`` turn structural
problems into actionable errors.

Shapes covered:
    .claude-plugin/plugin.json   PluginManifest
    skills/**/SKILL.md           SkillFrontmatter (YAML frontmatter)
    .mcp.json                    McpConfig / McpServer
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Forward compatible: accept and keep unknown keys rather than failing a real
# bundle that carries fields we do not model yet.
_LENIENT = ConfigDict(extra="allow", populate_by_name=True)


class Author(BaseModel):
    """The plugin manifest author object (or it may be a plain string)."""

    model_config = _LENIENT

    name: str
    email: str | None = None
    url: str | None = None


class PluginManifest(BaseModel):
    """The `.claude-plugin/plugin.json` manifest.

    Only ``name`` is required by the Claude Code format; everything else is
    optional. ``commands``/``agents`` may be a single path or a list; ``hooks``
    and ``mcpServers`` may be a path string or an inline object.
    """

    model_config = _LENIENT

    name: str
    version: str | None = None
    description: str | None = None
    author: str | Author | None = None
    homepage: str | None = None
    repository: str | None = None
    license: str | None = None
    keywords: list[str] | None = None
    commands: str | list[str] | None = None
    agents: str | list[str] | None = None
    hooks: str | dict[str, Any] | None = None
    mcpServers: str | dict[str, Any] | None = None
    # Curie authoring extension (epic #30): the agent's system prompt, shipped
    # in the bundle and versioned with it. Inline prompt text; applied at runner
    # boot as the sole system-prompt surface (#488) -- no env var shadows it.
    systemPrompt: str | None = None
    # Initial terminal/chat affordances. These are bundle-authored conversation
    # starters, not response actions; adapters discard them after the first turn.
    starterPrompts: list[str] | None = None
    # Curie authoring extension (ADR-0009, #429): the named secrets this
    # bundle's connectors need (e.g. an authed MCP server's API token). Policy
    # only -- the NAMES, never values. Values are supplied per-agent at deploy
    # time and delivered into the sandbox env, where ``.mcp.json`` ``${VAR}``
    # expansion consumes them. Validated env-var-style by ``validate.py``.
    secrets: list[str] | None = None
    # Curie authoring extensions (epic #30 / #29), validated at deploy time by
    # ``validate.py``. Kept loosely typed on the manifest (the models stay
    # lenient); the dedicated ``TriggerDeclaration`` / ``ApprovalPolicy`` models
    # below carry the shape the validator enforces.
    triggers: list[Any] | None = None
    approvalPolicy: dict[str, Any] | None = None
    # Curie authoring extension (ADR-0115): the OTHER agents this bundle
    # intends to call by name. Declaration of intent only -- it names what the
    # bundle author WANTS to reach, never a grant. An operator must still arm
    # each declared pair before a call can succeed (ADR-0115 part 5, "the
    # bundle declares, the operator arms"); a declared-but-unarmed pair is
    # refused exactly like an undeclared one. Validated non-empty-string and
    # deduplicated by ``validate.py``.
    delegatesTo: list[str] | None = None


# Trigger types the manifest may declare beyond inbound chat (epic #29). The
# validator enforces the per-type required field.
_TRIGGER_TYPES = ("cron", "webhook")


class TriggerDeclaration(BaseModel):
    """One trigger that can wake the agent, declared in the bundle (#273/#270).

    ``type`` is ``"cron"`` (requires a ``schedule`` cron expression) or
    ``"webhook"`` (requires a ``path`` the webhook ingress routes to). Declaring
    triggers in the bundle keeps an agent's full behavior in one reviewable
    artifact; the kernel/ingress consumes them (not built here — deploy-time
    validation only, so a malformed declaration is rejected before ship).
    """

    model_config = _LENIENT

    type: str
    schedule: str | None = None
    path: str | None = None


class ApprovalGate(BaseModel):
    """One approval gate declaration: a gate point plus the route it sends to.

    ``gate`` names the point at which the agent pauses for approval (e.g. a tool
    class or lifecycle point); ``route`` names the approval route that decides.
    Both are required — an incomplete gate is rejected at deploy.
    ``grantableViaPolicy`` is the operator opt-in (#558): when true, a policy-gate
    approval on this gate's route MAY mint a one-shot grant for the manifest tool
    ``gate`` names; default false preserves #544's policy-never-grants behavior.
    """

    model_config = _LENIENT

    gate: str
    route: str
    grantableViaPolicy: bool = False


class ApprovalPolicy(BaseModel):
    """The manifest ``approvalPolicy`` object: a list of gate declarations."""

    model_config = _LENIENT

    gates: list[ApprovalGate] = Field(default_factory=list)


class SkillFrontmatter(BaseModel):
    """The YAML frontmatter of a `skills/<name>/SKILL.md` file.

    ``name`` and ``description`` are required. The tool restriction field is the
    verbatim Claude Code key ``allowed-tools`` (see the package README Decisions
    for why this differs from the task's shorthand "tools").
    """

    model_config = _LENIENT

    name: str
    description: str
    allowed_tools: list[str] | None = Field(default=None, alias="allowed-tools")


class HookDefinition(BaseModel):
    """One hook action inside a matcher's ``hooks`` list.

    The Claude Code plugin ``hooks`` shape: a single action of a given ``type``.
    Today only ``type: "command"`` exists (run a shell command); ``command`` is
    required for it. Kept lenient so a future hook type still validates, but the
    deploy-time validator (``validate.py``) enforces that a ``command`` hook
    actually carries a command so a malformed guardrail is caught before ship.
    """

    model_config = _LENIENT

    type: str
    command: str | None = None


class HookMatcherConfig(BaseModel):
    """One matcher entry under a hook event (e.g. ``PreToolUse``).

    ``matcher`` is a tool-name pattern (``"Bash"``, ``"Write|Edit"``; absent or
    empty means "all tools"); ``hooks`` is the list of actions to run when a tool
    call matches. Mirrors the Claude Code hooks structure verbatim.
    """

    model_config = _LENIENT

    matcher: str | None = None
    hooks: list[HookDefinition] = Field(default_factory=list)


class McpServer(BaseModel):
    """One entry in `.mcp.json` mcpServers.

    A single permissive model rather than a strict stdio-vs-remote union, to
    stay forward compatible with Claude Code's evolving server shapes. A stdio
    server carries ``command`` (plus optional ``args``/``env``); a remote server
    carries ``type`` and ``url``. The validator enforces that one of the two is
    present.
    """

    model_config = _LENIENT

    command: str | None = None
    args: list[str] | None = None
    env: dict[str, str] | None = None
    type: str | None = None
    url: str | None = None
    headers: dict[str, str] | None = None


class McpConfig(BaseModel):
    """The `.mcp.json` document."""

    model_config = _LENIENT

    mcpServers: dict[str, McpServer] = Field(default_factory=dict)
