"""Load and validate the mounted plugin bundle for the SDK.

``CURIE_PLUGIN_DIR`` points at a Claude Code plugin bundle (skills/, .mcp.json,
scripts/, plugin.json). The runner validates it with the frozen
``plugin_format.validate_bundle`` before handing it to the SDK, and translates a
valid bundle into the ``ClaudeAgentOptions.plugins`` shape (a local plugin
config). An invalid bundle is a hard configuration error surfaced at startup, not
a silent skip: a runner that booted with a broken bundle would answer with the
wrong (empty) capability set.

This module is also where the sandbox's bundle STAGING is proved, not just its
content (#2612). ``CURIE_BUNDLE_REF`` is the object key the ``bundle-fetch`` /
``bundle-extract`` init containers fetch and unpack into ``CURIE_PLUGIN_DIR``
before this runner starts. A ``SandboxClaim`` written by hand puts that ref in
``spec.env`` with no ``containerName``, and the substrate injects such an entry
into the RUNNER container only -- the init containers keep the SandboxTemplate's
own empty default, take their documented no-op path, and exit 0. The runner then
boots with the ref set and an empty plugin dir and dies on ``[manifest.missing]``,
which reads like a broken bundle rather than a claim that never staged one. So a
ref that is set while nothing was staged is diagnosed here, by name, before the
frozen validator gets a chance to mis-attribute it.
"""

import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from aci_protocol import BootEnv
from claude_agent_sdk import SdkPluginConfig
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    PluginManifest,
    resolve_manifest,
    validate_bundle,
)

# The SDK's skill-name check is private, and the SDK is pinned only ``>=``, so a
# later release may move or rename it. When it is gone, use the local copy below.
_sdk_validate_skill_name: Callable[[str], None] | None
try:
    from claude_agent_sdk._internal.transport.subprocess_cli import (
        _validate_skill_name as _sdk_validate_skill_name,
    )
except ImportError:
    _sdk_validate_skill_name = None

logger = logging.getLogger(__name__)

# A copy of ``_validate_skill_name`` in claude_agent_sdk 0.2.159
# (``claude_agent_sdk/_internal/transport/subprocess_cli.py``). It rejects the names
# the SDK rejects; a test pins the two together while the SDK still ships its own.
_SKILL_NAME_INVALID_CHARS = re.compile(r"[(),\x00-\x1f\x7f-\x9f\ufeff]")
_SURROGATE_RE = re.compile("[\ud800-\udfff]")


def _local_validate_skill_name(name: str) -> None:
    """Raise ``ValueError`` if ``name`` cannot ride in a ``Skill(name)`` rule."""

    if not name.strip():
        raise ValueError("Skill names must be non-empty strings")
    if _SURROGATE_RE.search(name):
        raise ValueError(f"Invalid skill name {name!r}: contains a surrogate code point")
    if name != name.strip():
        raise ValueError(f"Invalid skill name {name!r}: leading or trailing whitespace")
    if _SKILL_NAME_INVALID_CHARS.search(name):
        raise ValueError(
            f"Invalid skill name {name!r}: parentheses, commas, control characters,"
            " and byte-order marks are not allowed"
        )
    if name == "*":
        raise ValueError("Invalid skill name '*': wildcards are not allowed")
    if name.endswith((":*", " *")):
        raise ValueError(f"Invalid skill name {name!r}: wildcard-suffix names are not allowed")
    if name.startswith("/"):
        raise ValueError(f"Invalid skill name {name!r}: skill names may not start with '/'")
    if "\\\\" in name:
        raise ValueError(f"Invalid skill name {name!r}: consecutive backslashes are not allowed")
    if name.endswith("\\"):
        raise ValueError(f"Invalid skill name {name!r}: names may not end with a backslash")


_validate_skill_name: Callable[[str], None] = _sdk_validate_skill_name or _local_validate_skill_name

BUNDLE_CONFIG_NAME = "curie.bundle.json"

# The boot key the bundle init containers consume. Read through BootEnv rather
# than typed here, so a rename in the one declaration (#488, ADR-0049) cannot
# leave this diagnosis watching a name nobody sets any more.
BUNDLE_REF_ENV = BootEnv.env_key("bundle_ref")

# The SandboxTemplate init containers that consume BUNDLE_REF_ENV
# (charts/curie/templates/agent-sandbox.yaml). Named in the operator-facing
# message below because the fix is to target them explicitly;
# ``curie_worker.sandbox.k8s.BUNDLE_INIT_CONTAINERS`` is the worker's copy of the
# same list and the two are pinned equal by the chart/worker parity test.
BUNDLE_INIT_CONTAINERS = ("bundle-fetch", "bundle-extract")


class PluginBundleError(RuntimeError):
    """Raised when the mounted plugin bundle fails validation."""


def load_bundle_web_search_enabled(plugin_dir: str | None) -> bool:
    """Return the bundle's provider-side web-search choice (ADR-0138).

    ``curie.bundle.json`` is a Curie-owned sidecar beside, not inside, the
    frozen Claude plugin-format contract. An absent sidecar defaults on. A
    present document is strict because a misspelled opt-out would otherwise
    widen the model's capability while appearing to disable it.
    """

    if not plugin_dir:
        return True
    config_path = Path(plugin_dir) / BUNDLE_CONFIG_NAME
    try:
        raw = config_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return True
    except OSError as exc:
        raise PluginBundleError(f"cannot read {config_path}: {exc}") from exc

    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PluginBundleError(
            f"invalid {config_path}: expected JSON object ({exc.msg})"
        ) from exc
    if not isinstance(loaded, dict):
        raise PluginBundleError(f"invalid {config_path}: root must be a JSON object")
    unknown = sorted(set(loaded) - {"webSearch"})
    if unknown:
        raise PluginBundleError(
            f"invalid {config_path}: unknown key(s): {', '.join(unknown)}"
        )
    enabled = loaded.get("webSearch", True)
    if not isinstance(enabled, bool):
        raise PluginBundleError(
            f"invalid {config_path}: webSearch must be a JSON boolean"
        )
    return enabled


def load_bundle_platform_slack_grants(plugin_dir: str | None) -> frozenset[str]:
    """The platform Slack grants the bundle manifest holds (ADR 0100, ADR 0200).

    The grants are ``channelRead``, ``canvasList``, ``canvasRead`` and
    ``canvasEdit``; each is held only as a literal ``true`` and defaults off.
    Any manifest this reader cannot parse grants nothing; ``load_plugins`` is
    the gate that refuses such a bundle at startup.
    """

    if not plugin_dir:
        return frozenset()
    manifest_path = resolve_manifest(plugin_dir)
    if manifest_path is None:
        return frozenset()
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = PluginManifest.model_validate(data)
    except (json.JSONDecodeError, ValueError, OSError):
        return frozenset()
    return manifest.platform_slack_grants()


def load_bundle_system_prompt(plugin_dir: str | None) -> str | None:
    """Return the ``systemPrompt`` declared in the bundle manifest, if any.

    The system prompt travels in the bundle (manifest field, epic #30) so it is
    versioned with the agent, and this is its sole surface: the out-of-band env
    override was removed in #488, so the bundle always wins. Returns ``None``
    when there is no plugin dir, no
    manifest, or no ``systemPrompt`` field. Best-effort and non-fatal: a bundle
    that fails to parse here is caught by ``load_plugins`` at startup, which is
    the authoritative validation gate, so this reader stays quiet.
    """

    if not plugin_dir:
        return None
    manifest_path = resolve_manifest(plugin_dir)
    if manifest_path is None:
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = PluginManifest.model_validate(data)
    except (json.JSONDecodeError, ValueError, OSError):
        return None
    return manifest.systemPrompt


def _is_empty(root: Path) -> bool:
    """True when nothing was staged at ``root`` -- no directory, or no entries."""

    try:
        return not any(root.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return True
    except OSError:
        # Unreadable is not provably empty; leave the verdict to validate_bundle.
        return False


def staging_no_op_detail(root: Path) -> str:
    """The operator-facing diagnosis for a ref that is set over an empty dir.

    Deliberately worded as "the most common cause", not a proven one. What this
    runner observes is missing content, and a mis-shaped claim is the only cause
    of it the sandbox's own loud failures do not already rule out -- but it is
    not the only cause reachable from outside that path. A claim that overrides
    CURIE_PLUGIN_DIR away from the init pair's mount path, or ``run_check``
    pointed at an arbitrary directory, both land here with the init containers
    entirely innocent. Naming the likely cause and the ones to rule out is the
    honest form of that.
    """

    entries = ", ".join(
        f"{{name: {BUNDLE_REF_ENV}, value: <ref>, containerName: {container}}}"
        for container in BUNDLE_INIT_CONTAINERS
    )
    return (
        f"{BUNDLE_REF_ENV} is set but nothing was staged at {root}. The most "
        "common cause is a SandboxClaim whose staging env never reached the "
        "init containers: a spec.env entry that carries no containerName is "
        "injected into the runner container ONLY, so bundle-fetch and "
        "bundle-extract keep the SandboxTemplate's empty default and take their "
        "no-op path. A hand-written claim must repeat each staging entry once "
        f"per init container, e.g. {entries}. Other causes to rule out: a "
        f"CURIE_PLUGIN_DIR that does not name the path the init pair extracts "
        "into, and a plugin dir emptied after extraction. See "
        "docs/operations.md, 'Which claim env reaches which sandbox container'."
    )


def load_plugins(
    plugin_dir: str | None, *, env: Mapping[str, str] | None = None
) -> list[SdkPluginConfig]:
    """Validate the bundle at ``plugin_dir`` and return the SDK plugin config.

    Returns an empty list when no plugin dir is configured. Raises
    ``PluginBundleError`` with the aggregated validation issues when the bundle
    exists but is malformed, and with an explicit staging diagnosis when
    ``CURIE_BUNDLE_REF`` is set but the init containers staged nothing (#2612).

    ``env`` defaults to the process environment; it is a parameter so the
    staging diagnosis is exercisable without mutating the interpreter's env.
    """

    if not plugin_dir:
        return []

    root = Path(plugin_dir)
    # #2612: a set ref over an EMPTY plugin dir is a claim-shape fault, not a
    # bundle fault. Diagnose it before validate_bundle reports [manifest.missing],
    # which blames the bundle author for a directory nobody ever filled.
    #
    # The emptiness test is what keeps this precise, and it is only sound because
    # every other staging failure is already loud: bundle-fetch runs `set -eu` and
    # fails the pod when the object cannot be fetched, and bundle-extract FATALs
    # when a ref is set with no archive present. So a runner that reached boot
    # with the ref set and NOTHING in the dir has exactly one cause -- the init
    # containers never saw the ref. A bundle that did extract but ships no
    # manifest leaves files behind, so it keeps the frozen validator's verdict,
    # which is the correct one for it.
    source = os.environ if env is None else env
    if source.get(BUNDLE_REF_ENV, "").strip() and _is_empty(root):
        raise PluginBundleError(staging_no_op_detail(root))
    # Naming the enforcement contract is a statement that this build applies a
    # declared toolPolicy. Older runners omit it and safely refuse policy-bearing
    # bundles rather than starting them unfenced.
    result = validate_bundle(root, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    if not result.valid:
        detail = "; ".join(f"[{i.code}] {i.location}: {i.message}" for i in result.errors)
        raise PluginBundleError(f"invalid plugin bundle at {root}: {detail}")

    return [SdkPluginConfig(type="local", path=str(root))]


# The CLI's own name for a plugin-loaded MCP server, ``plugin:<bundle>:<server>``.
# The SDK normalizes it to the live prefix ``mcp__plugin_<bundle>_<server>__``,
# which is ``plugin_format.approval_policy.effective_tool_prefix`` -- so a server
# mounted under this key publishes exactly the tool names toolPolicy, the approval
# gates and the capability probe already expect (verified on CLI 2.1.281).
_PLUGIN_ROOT_PLACEHOLDER = "${CLAUDE_PLUGIN_ROOT}"


def bundle_mcp_servers(plugin_dir: str | None) -> dict[str, Any]:
    """The bundle's own declared MCP servers, keyed as the CLI keys plugin servers.

    The runner sets ``strict_mcp_config`` (#2899) so ambient project ``.mcp.json``,
    user and plugin-marketplace servers never load beside the ones Curie mounts.
    The CLI applies that to ``--plugin-dir`` servers too: under strict mode a
    bundle's plugin servers silently stop registering. So the runner mounts them
    itself on ``--mcp-config``, under the same ``plugin:<bundle>:<server>`` name
    the plugin loader would have used, reading the same two declaration surfaces
    (the manifest's inline ``mcpServers`` and the root ``.mcp.json``) that
    ``plugin_format.approval_policy.declared_mcp_server_names`` reads.

    ``${CLAUDE_PLUGIN_ROOT}`` is the one variable the plugin loader supplies that
    the ``--mcp-config`` path does not, so it is substituted here and exported to
    a stdio server's env. An absent optional secret's whole-value stdio env
    reference is omitted before mounting (ADR-0209); the bundle stays immutable.
    Every other ``${VAR}`` is left for the CLI, which
    expands ``--mcp-config`` entries from the session env exactly as it expands a
    plugin's. Call only after ``load_plugins`` has validated the bundle; a
    malformed declaration was already refused there.
    """

    if not plugin_dir:
        return {}
    root = Path(plugin_dir)
    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        return {}
    manifest = PluginManifest.model_validate(json.loads(manifest_path.read_text(encoding="utf-8")))
    declarations: list[object] = []
    if isinstance(manifest.mcpServers, dict):
        declarations.append(manifest.mcpServers)
    root_mcp = root / ".mcp.json"
    if root_mcp.is_file():
        declarations.append(json.loads(root_mcp.read_text(encoding="utf-8")))

    plugin_root = str(root)
    absent_optional_refs = {
        f"${{{name}}}" for name in manifest.optionalSecrets or [] if name not in os.environ
    }
    servers: dict[str, Any] = {}
    for payload in declarations:
        if not isinstance(payload, dict):
            continue
        declared = payload.get("mcpServers", payload)
        if not isinstance(declared, dict):
            continue
        for name, config in declared.items():
            if not isinstance(config, dict):
                continue
            entry = cast("dict[str, object]", _substitute_plugin_root(config, plugin_root))
            if isinstance(entry.get("command"), str):
                raw_env = entry.get("env")
                env = (
                    {
                        key: value
                        for key, value in raw_env.items()
                        if not (isinstance(value, str) and value in absent_optional_refs)
                    }
                    if isinstance(raw_env, dict)
                    else {}
                )
                env.setdefault("CLAUDE_PLUGIN_ROOT", plugin_root)
                entry["env"] = env
            servers[f"plugin:{manifest.name}:{name}"] = entry
    return servers


def bundle_skill_names(plugin_dir: str | None) -> list[str]:
    """The bundle's own skills, named as the CLI names plugin skills (#3766).

    The runner passes this list as the SDK ``skills`` option so the model's
    skill listing holds only the bundle's skills, not the Claude Code CLI's
    built-in ones (ADR-0189). The CLI names a plugin skill
    ``<manifest name>:<dir>`` after its directory, not its frontmatter
    ``name``, and loads only ``skills/<dir>/SKILL.md`` one level down, so the
    list follows the same rule (verified on CLI 2.1.281). Sorted, so the list
    is deterministic. No bundle, or a bundle without skills, gives ``[]``. Call
    only after ``load_plugins`` has validated the bundle.

    The bundle validator only warns about odd folder names, but the SDK raises
    ``ValueError`` while building the CLI command for a name it cannot carry in
    a ``Skill(<name>)`` rule (a comma, a parenthesis, edge whitespace and so
    on). One such folder would stop the whole session at connect, so each name
    goes through the SDK's own check, and a rejected folder is skipped with a
    warning that names it. Calling the SDK's check keeps the accepted set
    exactly what the installed SDK accepts.
    """

    if not plugin_dir:
        return []
    root = Path(plugin_dir)
    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        return []
    manifest = PluginManifest.model_validate(json.loads(manifest_path.read_text(encoding="utf-8")))
    skills_dir = root / "skills"
    if not skills_dir.is_dir():
        return []
    names: list[str] = []
    for entry in sorted(skills_dir.iterdir(), key=lambda path: path.name):
        if not (entry.is_dir() and (entry / "SKILL.md").is_file()):
            continue
        name = f"{manifest.name}:{entry.name}"
        try:
            _validate_skill_name(name)
        except ValueError as exc:
            logger.warning(
                "skipping bundle skill folder %r: the agent SDK cannot pass its name (%s)",
                entry.name,
                exc,
            )
            continue
        names.append(name)
    return names


def _substitute_plugin_root(value: object, plugin_root: str) -> object:
    if isinstance(value, str):
        return value.replace(_PLUGIN_ROOT_PLACEHOLDER, plugin_root)
    if isinstance(value, list):
        return [_substitute_plugin_root(item, plugin_root) for item in value]
    if isinstance(value, dict):
        return {key: _substitute_plugin_root(item, plugin_root) for key, item in value.items()}
    return value
