"""Validate a plugin bundle directory against the Claude Code plugin shape.

``validate_bundle(path)`` is the entry point task B2 calls before versioning and
storing a bundle. It returns a ValidationResult with actionable, path-qualified
errors instead of raising, so the caller can surface every problem at once.
"""

import json
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, TypeAdapter, ValidationError

from .approval_policy import (
    connector_server_names,
    connector_tool_prefix,
    declared_mcp_server_names,
    effective_tool_prefix,
    grantable_routes,
)
from .connectors import CONNECTORS_FILE, ConnectorsFile, validate_connectors
from .deploy_targets import validate_deploy_targets
from .manifest import resolve_manifest
from .models import (
    _TRIGGER_TYPES,
    ApprovalPolicy,
    HookMatcherConfig,
    McpConfig,
    PluginManifest,
    SkillFrontmatter,
    TriggerDeclaration,
)
from .reserved_env import is_reserved_boot_env_name
from .yaml_loader import DuplicateKeyError, safe_load_unique

# The hooks field is a mapping of event name -> list of matcher entries. Reused
# to validate both the inline object and a declared hooks file.
_HOOKS_ADAPTER = TypeAdapter(dict[str, list[HookMatcherConfig]])
_TRIGGERS_ADAPTER = TypeAdapter(list[TriggerDeclaration])

# Claude Code plugin names are kebab-case: lowercase alphanumerics and hyphens.
_NAME_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


class ValidationIssue(BaseModel):
    code: str
    message: str
    location: str


class ValidationResult(BaseModel):
    valid: bool
    errors: list[ValidationIssue] = []
    warnings: list[ValidationIssue] = []


class _Collector:
    def __init__(self) -> None:
        self.errors: list[ValidationIssue] = []
        self.warnings: list[ValidationIssue] = []

    def error(self, code: str, message: str, location: str) -> None:
        self.errors.append(ValidationIssue(code=code, message=message, location=location))

    def warn(self, code: str, message: str, location: str) -> None:
        self.warnings.append(ValidationIssue(code=code, message=message, location=location))

    def result(self) -> ValidationResult:
        return ValidationResult(valid=not self.errors, errors=self.errors, warnings=self.warnings)


def validate_bundle(path: str | Path) -> ValidationResult:
    """Validate the plugin bundle at ``path`` and return a ValidationResult."""

    root = Path(path)
    c = _Collector()

    if not root.is_dir():
        c.error("bundle.missing", f"bundle path is not a directory: {root}", str(root))
        return c.result()

    manifest = _validate_manifest(root, c)
    if manifest is not None:
        _validate_skills(root, c)
        mcp_servers = _validate_mcp(root, manifest, c)
        _validate_hooks(root, manifest, c)
        _validate_triggers(manifest, c)
        _validate_approval_policy(manifest, mcp_servers, connector_server_names(root), c)
        _validate_secrets(manifest, c)
        _validate_delegates_to(manifest, c)
        _validate_scripts(root, c)
        _validate_connectors(root, c)
        _validate_deploy_targets(root, c)

    return c.result()


DEPLOY_FILE = "deploy.yaml"


def _validate_deploy_targets(root: Path, c: _Collector) -> None:
    """Validate ``deploy.yaml`` if present (ADR-0089).

    Optional: a bundle that passes routing as flags simply omits it. When
    present it must parse, because every error it can raise is one that would
    otherwise deploy SUCCESSFULLY to the wrong place -- a mistyped agent mints a
    new agent, a mistyped channel binds a channel nobody watches, and neither
    reports anything.
    """

    path = root / DEPLOY_FILE
    if not path.is_file():
        return
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except DuplicateKeyError as exc:
        c.error(
            "deploy.duplicate_target",
            f"{DEPLOY_FILE}: duplicate target key {exc.key!r}",
            DEPLOY_FILE,
        )
        return
    except (OSError, yaml.YAMLError) as exc:
        c.error("deploy.unreadable", f"{DEPLOY_FILE}: {exc}", DEPLOY_FILE)
        return
    for code, message in validate_deploy_targets(data)[1]:
        c.error(code, message, DEPLOY_FILE)


def _validate_connectors(root: Path, c: _Collector) -> None:
    """Validate ``connectors.yaml`` if present (ADR-0086).

    Optional: a bundle with no hosted connectors simply omits it. When present
    it must parse, because every error it can raise would otherwise surface as
    an opaque Kubernetes apply failure long after the author stopped looking.
    """

    path = root / CONNECTORS_FILE
    if not path.is_file():
        return
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except DuplicateKeyError as exc:
        c.error(
            "connectors.duplicate_connector",
            f"{CONNECTORS_FILE}: duplicate connector key {exc.key!r}",
            CONNECTORS_FILE,
        )
        return
    except (OSError, yaml.YAMLError) as exc:
        c.error("connectors.unreadable", f"{CONNECTORS_FILE}: {exc}", CONNECTORS_FILE)
        return
    parsed, errors = validate_connectors(data)
    for code, message in errors:
        c.error(code, message, CONNECTORS_FILE)
    if parsed is not None:
        _reject_connector_name_collisions(root, parsed, c)


def _reject_connector_name_collisions(root: Path, parsed: ConnectorsFile, c: _Collector) -> None:
    """Reject a server name declared in BOTH ``connectors.yaml`` and MCP config.

    Curie injects the connector's entry into the agent's MCP configuration
    (ADR-0086) alongside whatever the bundle declares. When both name the same
    server, which one the agent ends up talking to is decided downstream, and
    the loser is overridden with no diagnostic -- so the author's committed
    ``.mcp.json`` entry could be silently ignored, or could silently win over
    the objects Curie actually created.

    Neither outcome should be discoverable only at turn time, and a precedence
    rule would just be a thing to remember. One name, one owner: say so here,
    where the fix is a one-line edit.
    """

    declared = declared_mcp_server_names(root)
    if declared is None:
        # A declaration exists but is unreadable. `_validate_mcp` already errors
        # on that; cross-checking a partial set would add a confusing second
        # error about a name we cannot actually confirm.
        return
    for name in sorted(set(parsed.connectors) & declared):
        c.error(
            "connectors.duplicate_server",
            f"connectors.{name}: `{name}` is declared in both {CONNECTORS_FILE} and the "
            "bundle's MCP config. Curie derives this server's URL from the Service it "
            f"creates, so remove the `{name}` entry from the MCP config and let "
            f"{CONNECTORS_FILE} own it.",
            CONNECTORS_FILE,
        )


def _validate_manifest(root: Path, c: _Collector) -> PluginManifest | None:
    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        c.error(
            "manifest.missing",
            "no plugin manifest found at .claude-plugin/plugin.json or plugin.json",
            str(root),
        )
        return None

    rel = str(manifest_path.relative_to(root))
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        c.error("manifest.invalid_json", f"manifest is not valid JSON: {exc}", rel)
        return None

    try:
        manifest = PluginManifest.model_validate(data)
    except ValidationError as exc:
        for issue in _explain(exc):
            c.error("manifest.invalid", issue, rel)
        return None

    if not _NAME_RE.match(manifest.name):
        c.error(
            "manifest.name_invalid",
            f"plugin name {manifest.name!r} must be kebab-case "
            "(lowercase letters, digits, hyphens)",
            rel,
        )
    return manifest


def _validate_skills(root: Path, c: _Collector) -> None:
    skills_dir = root / "skills"
    if not skills_dir.is_dir():
        return

    skill_files = sorted(skills_dir.rglob("SKILL.md"))
    if not skill_files:
        c.warn("skills.empty", "skills/ exists but contains no SKILL.md files", "skills")
        return

    for skill_file in skill_files:
        rel = str(skill_file.relative_to(root))
        frontmatter = _read_frontmatter(skill_file, rel, c)
        if frontmatter is None:
            continue
        _check_tools_confusable(frontmatter, rel, c)
        try:
            SkillFrontmatter.model_validate(frontmatter)
        except ValidationError as exc:
            for issue in _explain(exc):
                c.error("skill.frontmatter_invalid", issue, rel)


# Keys an author reaches for instead of the verbatim Claude Code ``allowed-tools``.
# ``tools`` is silently dropped by ``extra="allow"``; ``allowed_tools`` populates
# the field via ``populate_by_name`` but will not survive a round-trip through
# real Claude Code. Both are rejected, but only when ``allowed-tools`` is absent:
# an author who already has the right key is not confused, whatever else the
# frontmatter carries.
_CONFUSABLE_TOOLS_KEYS = ("tools", "allowed_tools", "allowedTools")


def _check_tools_confusable(frontmatter: dict[str, Any], rel: str, c: _Collector) -> None:
    if "allowed-tools" in frontmatter:
        return
    for key in _CONFUSABLE_TOOLS_KEYS:
        if key in frontmatter:
            c.error(
                "skill.tools_confusable",
                f"skill frontmatter key {key!r} is not the Claude Code key: use "
                "'allowed-tools'. As written the skill declares no tools.",
                rel,
            )
            return


def _validate_mcp(root: Path, manifest: PluginManifest, c: _Collector) -> set[str] | None:
    """Validate every MCP declaration: the manifest field and root .mcp.json.

    The manifest ``mcpServers`` must be an inline object. The path-string form
    parses but the real loader ignores it, so the servers never register; it is
    rejected outright rather than validating a file that never loads (#540).

    Returns the set of declared server names across every declaration it read, so
    the approval-policy check can compare a gate name against them. ``None`` means
    a declaration existed but could not be read (invalid JSON, a missing declared
    path, or a config that failed to validate); it poisons the union, because a
    single unreadable source makes the declared-server set unknowable. An empty
    set is a different fact: a declaration was read and named no servers.

    The declaration objects are still walked here for their error layering (the
    ``_Collector`` messages), but the RETURNED set -- which
    ``_validate_approval_policy`` builds gate prefixes from -- comes from the
    shared ``declared_mcp_server_names`` derivation the runtime loader also uses,
    so the deploy validator and the runner normalize gate names identically by
    construction (#453/#703). Deriving both from one function is the anti-drift
    guarantee; the per-source walks below only produce actionable errors.
    """

    declared = manifest.mcpServers
    if isinstance(declared, dict):
        _validate_mcp_object(declared, "plugin.json (mcpServers)", c)
    elif isinstance(declared, str):
        c.error(
            "mcp.declared_pointer",
            f"manifest mcpServers is the path {declared!r}, a form the loader "
            "ignores: the servers never register. Declare them as an inline "
            'object instead: "mcpServers": {"<name>": {"command": "..."}}.',
            "plugin.json",
        )

    root_mcp = root / ".mcp.json"
    if root_mcp.is_file():
        _validate_mcp_file(root_mcp, ".mcp.json", c)

    return declared_mcp_server_names(root)


def _validate_mcp_file(path: Path, location: str, c: _Collector) -> set[str] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        c.error("mcp.invalid_json", f"{location} is not valid JSON: {exc}", location)
        return None
    return _validate_mcp_object(data, location, c)


def _validate_mcp_object(obj: object, location: str, c: _Collector) -> set[str] | None:
    # Accept both a full config object ({"mcpServers": {...}}) and a bare servers
    # map ({name: server}), which is how the manifest carries an inline value.
    payload = obj if isinstance(obj, dict) and "mcpServers" in obj else {"mcpServers": obj}
    try:
        config = McpConfig.model_validate(payload)
    except ValidationError as exc:
        for issue in _explain(exc):
            c.error("mcp.invalid", issue, location)
        return None

    for name, server in config.mcpServers.items():
        if server.command is None and server.url is None:
            c.error(
                "mcp.server_incomplete",
                f"mcp server {name!r} must define either 'command' (stdio) or 'url' (remote)",
                location,
            )

    return set(config.mcpServers)


def _validate_hooks(root: Path, manifest: PluginManifest, c: _Collector) -> None:
    """Validate the manifest ``hooks`` declaration (deploy-time gate, #272).

    ``hooks`` may be an inline object or a path to a hooks JSON file; either form
    must parse as ``{event: [ {matcher?, hooks: [{type, command}]} ]}``. A
    ``command`` hook must carry a non-empty ``command`` so a malformed guardrail
    is rejected before it ships (the runner enforces PreToolUse at run time).
    """

    declared = manifest.hooks
    if declared is None:
        return

    if isinstance(declared, str):
        hooks_path = root / declared
        if not hooks_path.is_file():
            c.error(
                "hooks.declared_missing",
                f"manifest hooks path {declared!r} was not found",
                "plugin.json",
            )
            return
        location = str(Path(declared))
        try:
            data: object = json.loads(hooks_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            c.error("hooks.invalid_json", f"{location} is not valid JSON: {exc}", location)
            return
    elif isinstance(declared, dict):
        location = "plugin.json (hooks)"
        data = declared
    else:
        c.error("hooks.invalid", "hooks must be an object or a path string", "plugin.json")
        return

    try:
        parsed = _HOOKS_ADAPTER.validate_python(data)
    except ValidationError as exc:
        for issue in _explain(exc):
            c.error("hooks.invalid", issue, location)
        return

    # Each command-type hook must actually declare a command.
    for event, matchers in parsed.items():
        for m_i, matcher in enumerate(matchers):
            for h_i, hook in enumerate(matcher.hooks):
                if hook.type == "command" and not (hook.command and hook.command.strip()):
                    c.error(
                        "hooks.command_missing",
                        f"{event}[{m_i}].hooks[{h_i}]: a 'command' hook must define a "
                        "non-empty command",
                        location,
                    )


def _validate_triggers(manifest: PluginManifest, c: _Collector) -> None:
    """Validate the manifest ``triggers`` declarations (deploy-time gate, #273).

    ``triggers`` is a list of ``{type, ...}``. ``type`` must be a known kind
    (``cron``/``webhook``); a ``cron`` trigger requires a non-empty ``schedule``
    and a ``webhook`` trigger a non-empty ``path``. Malformed declarations are
    rejected at deploy so an agent's non-chat wake-ups fail loudly before ship.
    """

    declared = manifest.triggers
    if declared is None:
        return
    if not isinstance(declared, list):
        c.error("triggers.invalid", "triggers must be a list of declarations", "plugin.json")
        return

    try:
        parsed = _TRIGGERS_ADAPTER.validate_python(declared)
    except ValidationError as exc:
        for issue in _explain(exc):
            c.error("triggers.invalid", issue, "plugin.json (triggers)")
        return

    for i, trigger in enumerate(parsed):
        loc = f"plugin.json (triggers[{i}])"
        if trigger.type not in _TRIGGER_TYPES:
            c.error(
                "triggers.unknown_type",
                f"trigger type {trigger.type!r} is not one of {list(_TRIGGER_TYPES)}",
                loc,
            )
            continue
        if trigger.type == "cron" and not (trigger.schedule and trigger.schedule.strip()):
            c.error(
                "triggers.cron_missing_schedule",
                "a 'cron' trigger must define a non-empty 'schedule'",
                loc,
            )
        if trigger.type == "webhook" and not (trigger.path and trigger.path.strip()):
            c.error(
                "triggers.webhook_missing_path",
                "a 'webhook' trigger must define a non-empty 'path'",
                loc,
            )


def _validate_approval_policy(
    manifest: PluginManifest,
    mcp_servers: set[str] | None,
    connector_servers: set[str] | None,
    c: _Collector,
) -> None:
    """Validate the manifest ``approvalPolicy`` declaration (deploy-time, #273).

    Shape ``{gates: [{gate, route}]}``: each gate names a pause point and the
    route that decides. A malformed policy or a gate missing its ``gate``/``route``
    is rejected at deploy.

    ``mcp_servers`` is the set of MCP server names the bundle declares (from
    ``_validate_mcp``). A gate that carries the ``mcp__`` prefix must be a live,
    fully-namespaced tool name (``mcp__plugin_<bundle>_<server>__<tool>``); the
    runner matches a gate by exact string equality, so the natural
    ``mcp__<server>__<tool>`` shape arms nothing and silently never fires. We
    reject it here. ``None`` means the MCP declaration could not be read, so the
    cross-check stays silent rather than stacking a misleading error on top of the
    MCP error that already fired.

    ``connector_servers`` is the ``connectors.yaml`` half of the same tool surface
    (#1495). A connector is mounted on the SDK's ``mcp_servers`` map rather than
    loaded as a plugin, so its live tool name is ``mcp__<connector>__<tool>`` with
    NO ``plugin_<bundle>_`` infix -- a DIFFERENT namespacing rule, which is why the
    two sets stay separate and each builds its own prefix
    (``connector_tool_prefix`` vs ``effective_tool_prefix``). Without this source a
    bundle that declares its whole tool surface through ``connectors.yaml`` has an
    empty accepted set, so every gate it could write is rejected here and no
    connector tool can be gated at all. ``None`` carries the same
    read-it-or-stay-silent meaning as ``mcp_servers``.
    """

    declared = manifest.approvalPolicy
    if declared is None:
        return
    if not isinstance(declared, dict):
        c.error(
            "approval_policy.invalid",
            "approvalPolicy must be an object with a 'gates' list",
            "plugin.json",
        )
        return

    try:
        policy = ApprovalPolicy.model_validate(declared)
    except ValidationError as exc:
        for issue in _explain(exc):
            c.error("approval_policy.invalid", issue, "plugin.json (approvalPolicy)")
        return

    # Construct the valid prefixes from what the bundle declares rather than
    # parsing the gate to extract a server name: a bundle name cannot contain '_'
    # (_NAME_RE) but a server key can, so parsing mcp__plugin_a_b_c__t is
    # ambiguous. Constructing and testing startswith sidesteps that entirely.
    # Each source contributes its OWN prefix form: a plugin-loaded server gets
    # mcp__plugin_<bundle>_<server>__, a connectors.yaml server gets the bare
    # mcp__<connector>__ the SDK produces for a directly-mounted server (#1495).
    # A single unreadable source makes the accepted set unknowable, so either
    # being None suppresses the check rather than asserting against half of it.
    expected_prefixes: set[str] | None = (
        {effective_tool_prefix(manifest.name, s) for s in mcp_servers}
        | {connector_tool_prefix(s) for s in connector_servers}
        if mcp_servers is not None and connector_servers is not None
        else None
    )

    for i, gate in enumerate(policy.gates):
        loc = f"plugin.json (approvalPolicy.gates[{i}])"
        if not (gate.gate and gate.gate.strip()) or not (gate.route and gate.route.strip()):
            c.error(
                "approval_policy.incomplete",
                "an approval gate must define a non-empty 'gate' and 'route'",
                loc,
            )
            continue

        # A gate without the mcp__ prefix names a built-in tool (Bash, Write,
        # PreToolUse); it is armed by raw name and never touched here. Evaluate
        # the STRIPPED value: the runner strips the gate before matching
        # (approval.py load_approval_policy), so a leading-space "mcp__..." that
        # looks built-in on the raw string would arm a mis-namespaced tool at
        # runtime and silently never fire (#453).
        stripped_gate = gate.gate.strip()
        if expected_prefixes is None or not stripped_gate.startswith("mcp__"):
            continue

        # A live tool name needs a non-empty tool suffix after the matched
        # prefix; the bare "mcp__plugin_<bundle>_<server>__" arms nothing (#453).
        if not any(
            stripped_gate.startswith(prefix) and len(stripped_gate) > len(prefix)
            for prefix in expected_prefixes
        ):
            c.error(
                "approval_policy.gate_not_namespaced",
                _gate_not_namespaced_message(
                    stripped_gate,
                    manifest.name,
                    mcp_servers or set(),
                    connector_servers or set(),
                ),
                loc,
            )

    # #558: an operator-opted grantable route claimed by more than one distinct
    # tool would validate green yet arm no grant (the ambiguous route is excluded
    # by the shared normalizer), so reject it here. grantable_routes is the SAME
    # helper the runtime loader uses, so the validator and loader agree on which
    # routes are grantable by construction (#453).
    _, ambiguous = grantable_routes(policy.gates)
    for route in sorted(ambiguous):
        c.error(
            "approval_policy.grant_route_ambiguous",
            f"more than one grantableViaPolicy gate claims route {route!r} with a"
            " different tool; it is ambiguous which tool a policy approval would"
            " grant, so the route would validate but arm no grant. Make the"
            " grantable gates on this route name the same tool, or drop the opt-in.",
            "plugin.json (approvalPolicy)",
        )


def _gate_not_namespaced_message(
    gate: str, bundle: str, mcp_servers: set[str], connector_servers: set[str]
) -> str:
    """Actionable message for a gate whose mcp__ name is not a live tool name."""

    expected = sorted(
        [f"{effective_tool_prefix(bundle, s)}<tool>" for s in mcp_servers]
        + [f"{connector_tool_prefix(s)}<tool>" for s in connector_servers]
    )
    if expected:
        declared = "Expected one of: " + ", ".join(expected)
    else:
        declared = f"This bundle declares no MCP servers and no {CONNECTORS_FILE} connectors"
    return (
        f"approval gate {gate!r} is not a live MCP tool name. A bundle-declared "
        f"MCP tool's live name is mcp__plugin_{bundle}_<server>__<tool>. A "
        f"{CONNECTORS_FILE} connector is mounted directly instead of loaded as a "
        "plugin, so ITS live name is mcp__<connector>__<tool>, with no "
        f"plugin_{bundle}_ infix. "
        f"{declared}. A built-in tool gate (e.g. Bash) carries no mcp__ prefix. "
        "A MANIFEST approvalPolicy gate must be the fully-namespaced live name "
        "(this deploy gate does not normalize it). The per-agent "
        "CURIE_APPROVAL_REQUIRED_TOOLS env knob is more lenient (#703): the runner "
        "normalizes a bare mcp__<server>__<tool> shorthand for a DECLARED server to "
        "its effective name, and also accepts an already-namespaced "
        "mcp__plugin_<bundle>_<server>__<tool> or a built-in name -- but only for a "
        "server the bundle declares; it never arms a name that names no declared "
        "server. Namespace this manifest gate to its live name."
    )


_SECRET_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def _validate_secrets(manifest: PluginManifest, c: _Collector) -> None:
    """Validate the manifest ``secrets`` policy (deploy-time gate, ADR-0009 / #429).

    ``secrets`` is a list of the named connector secrets the bundle needs (the
    NAMES only, never values). Each must be an environment-variable-style name
    (``^[A-Z_][A-Z0-9_]*$``) so it can be forwarded into the sandbox env and
    consumed by ``.mcp.json`` ``${VAR}`` expansion; a malformed name is rejected
    at deploy before an agent ships expecting a secret that can never bind.
    """

    declared = manifest.secrets
    if declared is None:
        return
    if not isinstance(declared, list):
        c.error("secrets.invalid", "secrets must be a list of names", "plugin.json")
        return

    for i, name in enumerate(declared):
        loc = f"plugin.json (secrets[{i}])"
        if not isinstance(name, str) or not _SECRET_NAME_RE.match(name):
            c.error(
                "secrets.name_invalid",
                f"secret name {name!r} must be an env-var-style name "
                "(uppercase letters, digits, underscore; not starting with a digit)",
                loc,
            )
        elif is_reserved_boot_env_name(name):
            # Reserved sandbox boot-env / model-credential keys (#457, #445):
            # the whole CURIE_* namespace plus the runner's non-prefixed
            # credential keys (ANTHROPIC_BASE_URL etc). A connector secret must
            # not declare one -- it would clobber or be dropped by the worker
            # binding at delivery time, or silently redirect the model session.
            c.error(
                "secrets.name_reserved",
                f"secret name {name!r} is reserved: it is a platform boot-env, "
                "model-credential, or redirect/capture-capable key and cannot be "
                "used for a connector secret",
                loc,
            )


def _validate_delegates_to(manifest: PluginManifest, c: _Collector) -> None:
    """Validate the manifest ``delegatesTo`` declaration (ADR-0115).

    A list of the OTHER agents this bundle intends to call, by name. This is
    declaration only -- it never grants anything by itself, an operator still
    has to arm each pair -- so validation is narrow: each entry is a non-empty
    string, no entry names this same bundle (a bundle cannot declare intent to
    call itself; that is a trivial cycle refused for free at deploy rather
    than left for the depth/cycle check at call time), and there are no
    duplicate entries.
    """

    declared = manifest.delegatesTo
    if declared is None:
        return
    if not isinstance(declared, list):
        c.error("delegatesTo.invalid", "delegatesTo must be a list of agent names", "plugin.json")
        return

    seen: set[str] = set()
    for i, name in enumerate(declared):
        loc = f"plugin.json (delegatesTo[{i}])"
        if not isinstance(name, str) or not name.strip():
            c.error(
                "delegatesTo.name_invalid",
                f"delegatesTo entry {name!r} must be a non-empty agent name",
                loc,
            )
            continue
        if name == manifest.name:
            c.error(
                "delegatesTo.self_reference",
                f"delegatesTo cannot name this bundle's own agent {name!r}",
                loc,
            )
        elif name in seen:
            c.error("delegatesTo.duplicate", f"delegatesTo lists {name!r} more than once", loc)
        seen.add(name)


def _validate_scripts(root: Path, c: _Collector) -> None:
    scripts = root / "scripts"
    if scripts.exists() and not scripts.is_dir():
        c.error("scripts.not_a_directory", "scripts must be a directory", "scripts")


def _read_frontmatter(skill_file: Path, rel: str, c: _Collector) -> dict[str, Any] | None:
    text = skill_file.read_text(encoding="utf-8")
    if not text.startswith("---"):
        c.error("skill.frontmatter_missing", "SKILL.md has no YAML frontmatter block", rel)
        return None

    parts = text.split("---", 2)
    if len(parts) < 3:
        c.error(
            "skill.frontmatter_unterminated",
            "SKILL.md frontmatter is not closed by '---'",
            rel,
        )
        return None

    try:
        loaded = yaml.safe_load(parts[1])
    except yaml.YAMLError as exc:
        c.error("skill.frontmatter_invalid_yaml", f"frontmatter is not valid YAML: {exc}", rel)
        return None

    if not isinstance(loaded, dict):
        c.error("skill.frontmatter_invalid", "frontmatter must be a YAML mapping", rel)
        return None
    return loaded


def _explain(exc: ValidationError) -> list[str]:
    out: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "(root)"
        out.append(f"{loc}: {err['msg']}")
    return out
