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
import os
from collections.abc import Mapping
from pathlib import Path

from aci_protocol import BootEnv
from claude_agent_sdk import SdkPluginConfig
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    PluginManifest,
    resolve_manifest,
    validate_bundle,
)

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
