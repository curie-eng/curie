"""Runner entrypoint: build the session from the environment and serve the ACI.

Reads the ACI ``CURIE_*`` / ``OTEL_EXPORTER_OTLP_*`` env into a RunnerConfig,
wires the real claude-agent-sdk session (validated plugin bundle, budget, OTel),
and serves the HTTP channel. The session is started in ``on_startup`` so a plugin
or connect failure fails the process visibly rather than after the port is up.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path

import anyio
from aci_protocol import BootEnv
from aiohttp import web
from claude_agent_sdk import ClaudeAgentOptions, HookMatcher
from curie_telemetry import bootstrap_service_telemetry

from . import __version__
from .adapter import (
    ClaudeAgentSession,
    ModelSession,
    build_options,
    build_structured_resume,
)
from .approval import (
    APPROVAL_SERVER_NAME,
    ApprovalPolicyError,
    assert_gates_not_shadowed,
    build_approval_gate,
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
    include_generic_policy_pager,
    policy_disallowed_tools,
    resolve_approval_policy,
)
from .config import RunnerConfig
from .connectors import (
    build_mcp_servers,
    derive_mcp_servers,
    drop_connector_secret_names,
    materialize_hosted_bearer_headers,
)
from .fake import FakeModelSession
from .harness.contribution import HarnessContribution
from .harness.registry import (
    BUILTIN_HARNESS_CANONICAL_PATHS,
    DEFAULT_HARNESS,
    resolve_harness,
)
from .history import (
    DEFAULT_REPLAY_MAX_BYTES,
    DEFAULT_REPLAY_MAX_TURNS,
    ConversationReplay,
    HistoryCapacityError,
    HistoryConflictError,
    HistoryError,
    StructuredReplayUnsupported,
    TranscriptStore,
    build_conversation_replay,
    resolve_history,
)
from .hooks import build_gated_pre_tool_use_hooks, load_bundle_hooks
from .mcp_tool_capability import (
    ConnectorAvailability,
    ConnectorCapabilityFailure,
    McpToolCapabilityProbe,
    diagnose_derived_connector_headers,
    probe_mcp_tool_capability,
    reprobe_connector_failures,
)
from .memory import MemoryStore, format_memory_preamble, resolve_memory
from .otel import RunTracer, build_tracer_provider
from .plugin import load_bundle_web_search_enabled
from .progress import ProgressActivity, build_progress_tool, resolve_progress
from .publication_precheck import PublicationPrecheck
from .redact import install_stdout_redaction
from .sdk_auth import UnsupportedCredentialError
from .server import bind_status_attestation, create_app
from .session import ConnectorReprobe, SessionRunner
from .side_effects import SideEffectClassifier
from .state import STATE_SERVER_NAME, build_state_server, resolve_state_client
from .workspace_snapshot import WorkspaceSnapshot, capture_workspace_snapshot

logger = logging.getLogger("curie_runner")

# Where the chart's ``attachments-init`` materializes one turn's inbound files
# (#2567). Compiled in rather than read from the env: the presigned reference is
# scoped to that init container precisely so the runner -- which executes
# prompt-injectable model code -- never sees it, which leaves nothing at runtime
# to tell this process where the volume landed. The other side of the seam is
# ``agentSandbox.runner.attachments.mountPath`` in charts/curie/values.yaml, and
# runner/tests/test_attachments_mount.py compares the two.
#
# A SIBLING of /workspace, deliberately not a directory inside it: workspace-init
# deletes every child of its own root on entry, so an attachment under the
# checkout could be silently eaten depending on init order, and anything that
# survived would show up in ``git status`` and could ride into a publication diff.
ATTACHMENTS_DIR = Path("/attachments")


def _discover_attachments(mount: Path | None) -> tuple[Path, ...]:
    """The files a person actually attached, as absolute paths.

    A directory probe, exactly as the managed checkout is discovered: an absent
    mount (the lane switched off) and an empty one (the ordinary "no files this
    turn", since the chart mounts the emptyDir unconditionally) both read as no
    attachments. Hidden entries are skipped -- ``attachments-init`` stages into
    ``.curie-attachments-stage`` -- so its own bookkeeping is never announced to
    the model as if a person had sent it.
    """

    if mount is None or not mount.is_dir():
        return ()
    return tuple(
        sorted(
            child
            for child in mount.iterdir()
            if child.is_file() and not child.name.startswith(".")
        )
    )


def format_attachment_preamble(paths: Sequence[Path]) -> str | None:
    """Tell the model the files are there, by a path that actually resolves.

    The session's cwd is the managed checkout, so a bare filename would resolve
    to ``<workspace>/<name>`` and the read would fail. Naming a file without a
    resolvable path is the same bug as not naming it at all, one step later.
    """

    if not paths:
        return None
    lines = [
        "The message you are answering carried file attachments. They are "
        "already on disk in this sandbox and you can open them with your "
        "ordinary file-reading tools. Your working directory is NOT the "
        "directory holding them, so use these absolute paths exactly as "
        "written:",
    ]
    lines.extend(f"- {path}" for path in paths)
    return "\n".join(lines)


def _resolve_harness(name: str = DEFAULT_HARNESS) -> HarnessContribution:
    """Resolve the active harness's contribution manifest (ADR-0060).

    The built-in Claude harness must always be available, so a built-in name --
    its declared name or any alias in ``BUILTIN_HARNESS_CANONICAL_PATHS`` -- is
    resolved from its direct import and never through entry-point discovery.
    That keeps the critical boot path independent of packaging metadata
    entirely: a malformed, colliding, or import-crashing *sibling* entry point
    makes ``discover_contributions`` raise (a guard error such as
    ``FlatHarnessPackageError``/``HarnessNameCollisionError``/
    ``MalformedHarnessContributionError``, none of them ``UnknownHarnessError``),
    and none of that may take down the built-in (#865). The registry already
    refuses any third party that claims a built-in key, so a built-in name can
    only ever mean the built-in -- resolving it directly is equivalent for a
    well-formed registry and strictly safer for a broken one.

    A non-built-in name goes through the registry and still fails loud (an
    ``UnknownHarnessError`` if unregistered, or a guard error if the registry is
    malformed), so an operator who selects a harness that isn't installed fails
    visibly, not silently.
    """

    if name in BUILTIN_HARNESS_CANONICAL_PATHS:
        from .harness.claude import get_contribution

        return get_contribution()
    return resolve_harness(name)


def format_workspace_preamble(mounted_workspace: Path | None) -> str | None:
    """Render mounted-workspace facts as a system-prompt preamble, or None.

    Hardcodes ``/workspace`` in the text so a caller Path never leaks into the
    prompt. Conversation history is not part of this block (ADR-0119).
    """

    if mounted_workspace is None:
        return None
    return (
        "# Mounted workspace\n"
        "\n"
        "A managed checkout is already at /workspace (complete git working tree, "
        "credential-free origin).\n"
        "Work only in /workspace; edit in place.\n"
        "Do not git clone, git fetch, or git pull this repository over the network.\n"
        "General network egress is unavailable in this sandbox; git hosts including "
        "github.com are unreachable by design.\n"
        "Do not git push; use publish_changes when ready.\n"
        "Python, pip, and venv are already in the image. "
        "Create a virtualenv only under /workspace.\n"
        "Install dependencies only from files already in the checkout, with pip --no-index. "
        "Do not contact a package index.\n"
        "Run only the repository's documented check command.\n"
        "Do not write a substitute test runner or shim.\n"
        "If those checks cannot be installed from files already in the checkout, say that "
        "in-sandbox verification is unavailable and that the published pull request's CI "
        "is the only check."
    )


def _compose_system_prompt(
    base: str | None,
    memory_preamble: str | None,
    *,
    model: str | None,
    workspace_preamble: str | None = None,
    attachment_preamble: str | None = None,
) -> str | None:
    """Compose durable memory, mounted-workspace facts, bundle instructions, and model identity.

    Conversation history is deliberately absent: ADR-0119 requires it to cross
    the harness boundary as ordered messages, never rendered system text.

    This turn's inbound attachments (#2567) come last, closest to the query they
    belong to. Absent -- the overwhelming majority of turns -- the composed
    prompt is byte-identical to what it was before the lane existed.
    """

    model_preamble = f"Configured model: {model}" if model else None
    parts = [
        p
        for p in (
            memory_preamble,
            workspace_preamble,
            base,
            model_preamble,
            attachment_preamble,
        )
        if p
    ]
    return "\n\n".join(parts) if parts else None


def _merge_pre_tool_use_hooks(
    approval_hooks: dict[str, list[HookMatcher]] | None,
    bundle_hooks: dict[str, list[HookMatcher]] | None,
) -> dict[str, list[HookMatcher]] | None:
    """Merge the approval gate's PreToolUse matcher with the bundle's own (#1852).

    Merge, never replace: dropping the bundle's declared PreToolUse guardrails
    (#272) would silently disarm them, and dropping the approval matcher leaves
    the gate bypassable by any permission rule -- the #1852 defect itself. The
    approval matcher is placed first for determinism of our own construction;
    the CLI dispatches matchers on one event CONCURRENTLY
    (claude_agent_sdk/types.py:1956-1961), so list position is not a runtime
    precedence guarantee and nothing here relies on it.

    Returns None when neither side contributes anything: ``ClaudeAgentOptions``
    takes ``hooks=None`` to mean "no hooks declared", which is not the same as
    an empty matcher list, and ``load_bundle_hooks`` returning None for a bundle
    with no hooks is the common case rather than an error.
    """

    merged: dict[str, list[HookMatcher]] = {}
    for source in (approval_hooks, bundle_hooks):
        if not source:
            continue
        for event, matchers in source.items():
            merged.setdefault(event, []).extend(matchers)
    return merged or None


def build_runner(
    config: RunnerConfig,
    *,
    fake_model: bool = False,
    sdk_env: dict[str, str] | None = None,
    memory_store: MemoryStore | None = None,
    memory_preamble: str | None = None,
    history_store: TranscriptStore | None = None,
    conversation_replay: ConversationReplay | None = None,
    mcp_capability: McpToolCapabilityProbe | None = None,
    harness: HarnessContribution | None = None,
    workspace_path: Path | None = None,
    attachments_path: Path | None = None,
    connector_failures: tuple[ConnectorCapabilityFailure, ...] = (),
    history_capacity_exceeded: bool = False,
) -> SessionRunner:
    """Wire a SessionRunner backed by the active harness's model session.

    ``fake_model`` (env ``CURIE_FAKE_MODEL``) swaps in the scripted fake session
    so the image can round-trip a synthetic event with no model credential or
    network -- used for the container smoke and any offline exercise of the wiring
    (OTel export included). It never reaches the Anthropic API.

    ``harness`` is the resolved contribution manifest (ADR-0060) whose fields
    drive the read-only tool set and bundle compile; it defaults to the built-in
    Claude harness so existing callers are unaffected.
    """

    # Resolve the active harness's contribution (ADR-0060): its manifest is the
    # single source for the read-only tool classification and how a bundle
    # compiles into session inputs, replacing the direct module imports these
    # used to be. Defaults to the built-in Claude harness.
    harness = harness or _resolve_harness()
    conversation_replay = conversation_replay or ConversationReplay()
    if conversation_replay.present and not harness.supports_structured_replay and not fake_model:
        raise StructuredReplayUnsupported(
            f"harness {harness.name!r} declares structured replay absent; "
            "refusing recovered history instead of rendering a prompt preamble"
        )
    # The bundle compiles once into this harness's native inputs (compile_bundle):
    # the ``systemPrompt`` shipped in the manifest (versioned with the agent, epic
    # #30) is the declared surface and always wins -- an env override let an
    # operator silently replace the prompt the bundle ships (#488) -- and the
    # bundle's plugins feed the session factory below.
    compiled = harness.compile_bundle(config.session.plugin_dir)
    system_prompt = compiled.system_prompt
    web_search_enabled = load_bundle_web_search_enabled(config.session.plugin_dir)
    mounted_workspace = (
        workspace_path
        if workspace_path is not None
        and workspace_path.is_dir()
        and (workspace_path / ".git").exists()
        else None
    )
    # Prior memory (#264) still leads the system prompt. Workspace facts are a
    # mounted-only boot block after memory. Conversation history (#20)
    # deliberately does not: ADR-0119 sends its ordered messages through the
    # harness adapter below. The configured model identity is appended after
    # the bundle prompt.
    # This turn's inbound attachments (#2567): materialized by the chart's
    # attachments-init into a mount that is a sibling of the checkout. The files
    # are useless unless the model is TOLD about them -- an unannounced file is
    # indistinguishable from one that never arrived, and the agent answers "I
    # don't see an attachment" about a message that visibly carries one.
    attachment_paths = _discover_attachments(attachments_path)
    system_prompt = _compose_system_prompt(
        system_prompt,
        memory_preamble,
        model=config.model,
        workspace_preamble=format_workspace_preamble(mounted_workspace),
        attachment_preamble=format_attachment_preamble(attachment_paths),
    )
    # In-bundle PreToolUse guardrails declared in the manifest hooks field (#272),
    # translated into SDK HookMatcher callbacks. None when the bundle declares none.
    bundle_hooks = load_bundle_hooks(config.session.plugin_dir)
    # The permission gate (#245/#247): approval-required tools come from the
    # union of the bundle manifest's approvalPolicy gates (versioned with the
    # agent, each carrying its route name) and the CURIE_APPROVAL_REQUIRED_TOOLS
    # env override (operator/per-agent config, no route). When either names a
    # tool, a can_use_tool callback replaces the hardcoded bypass and blocks
    # those calls pending approval; the gate object is shared with the
    # SessionRunner so a blocked call flips the turn's final to
    # awaiting-approval. Neither configured keeps the bypass posture.
    # Both halves fail closed (#520): resolve_approval_policy raises rather than
    # degrading a declared-but-unarmable policy to "nothing gated", and
    # build_approval_gate refuses a bundle gate that would redefine the route
    # of a tool the operator already gated. Either raises before the first
    # turn, so a misdeclared policy never boots ungated.
    try:
        resolution = resolve_approval_policy(config.session.plugin_dir)
        approval_gate = build_approval_gate(
            operator_tools=config.approval_required_tools,
            policy_routes=resolution.route_by_tool,
            grant_tool=config.approval_grant_tool,
            grantable_by_route=resolution.grantable_by_route,
            summary_by_tool=resolution.summary_by_tool,
            # Bundle identity so an operator mcp__<server>__<tool> shorthand
            # normalizes to its effective plugin-prefixed runtime name (#703),
            # and the connectors.yaml servers so a gate on a connector tool --
            # whose live name is the bare mcp__<connector>__<tool> the SDK gives
            # a directly-mounted server -- verifies instead of failing closed
            # (#1495).
            bundle_name=resolution.bundle_name,
            mcp_servers=resolution.mcp_servers,
            connector_servers=resolution.connector_servers,
            managed_workspace=mounted_workspace is not None,
            tool_policy=resolution.tool_policy,
        )
        # The third fail-closed boot check (#1852). The two above refuse a policy
        # that cannot be armed as declared; this one refuses a policy that WOULD
        # arm and then be bypassed, because the bundle's own skill permissions
        # preauthorize a gated tool before can_use_tool is ever consulted. It sits
        # here rather than in build_approval_gate because only this scope holds
        # both the assembled gate and the bundle directory.
        assert_gates_not_shadowed(config.session.plugin_dir, approval_gate, resolution)
    except ApprovalPolicyError as exc:
        # Log then re-raise, matching the module's other two fatal boot paths
        # (credential resolution, session start): a bare traceback is the one
        # thing an operator cannot triage from pod logs.
        logger.error("approval policy unusable error_class=%s: %s", type(exc).__name__, exc)
        raise

    # The gate's own PreToolUse matcher (#1852) is merged with the bundle's
    # after the MCP capability probe so a failed connector can front the
    # approval hook (#2634). Built after the boot refusal above has passed, so
    # a bundle Curie is about to refuse never gets a hook registered against it.

    # The durable state store exposed to bundle code (#249): when the worker
    # forwarded CURIE_STATE_URL, mount the platform ``curie-state`` MCP
    # server so a skill can read/write suspend/resume-surviving state without the
    # bundle shipping its own server. Absent (fake/local, or an older worker), no
    # state server is mounted and the agent simply sees no state tools.
    state_client = resolve_state_client(os.environ)
    # The live status card (#3077): a factory execution carries a progress URL
    # and token, and the bundle declares its phases. A malformed phase file is
    # logged and mounts no tool; progress never stops a boot.
    try:
        progress = resolve_progress(os.environ, Path(config.session.plugin_dir))
    except ValueError as exc:
        logger.warning("report_progress not mounted: %s", exc)
        progress = None
    progress_activity = ProgressActivity()
    progress_activity.model = config.model
    # Tell the gate whether the platform's own ``curie-state`` tools exist this
    # session (#2286 adversarial round). The toolPolicy exemption is by exact
    # live tool name, and a name the platform never published is not ours -- an
    # ambient project ``.mcp.json`` can mount a server keyed ``curie-state``,
    # because ``strict_mcp_config`` is off. Set AFTER construction rather than
    # passed to ``build_approval_gate`` deliberately: the gate is built above at
    # the three fail-closed approval boot checks, which must raise before any
    # other boot work happens, and hoisting ``resolve_state_client`` above them
    # would reorder the boot to suit a field. Both this flag and the conditional
    # mount below read the same ``state_client`` local, which nothing rebinds in
    # between, so the exemption and the mount cannot disagree. The mount keeps
    # its own ``is not None`` because ``build_state_server`` needs the narrowed
    # client, not the bool.
    state_mounted = state_client is not None
    if approval_gate is not None:
        approval_gate.state_server_mounted = state_mounted
        approval_gate.publication_precheck = PublicationPrecheck(
            mounted_workspace,
            os.environ.get(BootEnv.env_key("state_url"))
            or os.environ.get(BootEnv.env_key("progress_url")),
            network_enabled=not fake_model,
        )
    workspace_cwd = str(mounted_workspace) if mounted_workspace is not None else None
    derived_mcp_servers = derive_mcp_servers(
        config.session.plugin_dir,
        release=config.connector_release,
        agent=config.connector_agent,
        namespace=config.connector_namespace,
    )
    # Expand hosted Bearer ${NAME} headers in memory and drop NAME so Bash
    # cannot read the PAT from the process env (#2559). The on-disk catalog
    # keeps the placeholder; derive_mcp_servers never sees a value.
    spawn_env = sdk_env if sdk_env is not None else os.environ
    dropped = materialize_hosted_bearer_headers(derived_mcp_servers, spawn_env)
    if spawn_env is not os.environ:
        drop_connector_secret_names(os.environ, dropped)

    real_options: ClaudeAgentOptions | None = None
    observed_readonly_tools: frozenset[str] = frozenset()
    capability = mcp_capability
    connector_availability: ConnectorAvailability | None = None
    connector_reprobe: ConnectorReprobe | None = None
    if not fake_model:
        # The bundle's live MCP ``tools/list`` response is the actual advertised
        # MCP surface. Probe even when an explicit gate already pages: exact
        # readOnlyHint=true observations also drive receipt and retry
        # classification. Missing hints, uninspectable declarations, and probe
        # failures preserve the historical fail-closed behavior. The annotation
        # remains a non-authoritative hint: it never authorizes or denies tool
        # execution.
        if capability is None:
            capability = anyio.run(
                probe_mcp_tool_capability,
                config.session.plugin_dir,
                derived_mcp_servers,
                sdk_env,
            )
        observed_readonly_tools = capability.readonly_tools
        boot_connector_failures = connector_failures or capability.connector_failures
        if boot_connector_failures:
            # A failed declared connector no longer halts every turn (#2634).
            # The model runs with that connector's tools denied by this hook,
            # over an exclusion set the SessionRunner refreshes at each turn
            # start by re-dialing the materialized server config. Re-probe is
            # real-model only: the fake path's failures are expansion-only.
            connector_availability = ConnectorAvailability(boot_connector_failures)
            reprobe_servers = derived_mcp_servers
            reprobe_env = sdk_env

            async def reprobe(
                failures: tuple[ConnectorCapabilityFailure, ...],
            ) -> tuple[ConnectorCapabilityFailure, ...]:
                return await reprobe_connector_failures(
                    failures, reprobe_servers, reprobe_env
                )

            connector_reprobe = reprobe

        # The connector exclusion FRONTS the approval hook in one callback
        # (#2634) so an excluded gated tool never records a pending approval or
        # spends a grant; bundle hooks stay siblings, as before. No gate and no
        # failed connector keeps the wiring byte-identical to before.
        session_hooks = _merge_pre_tool_use_hooks(
            build_gated_pre_tool_use_hooks(
                build_approval_hook(approval_gate) if approval_gate is not None else None,
                connector_availability,
            ),
            bundle_hooks,
        )
        policy_hidden_tools = (
            policy_disallowed_tools(approval_gate, capability.observed_tools)
            if approval_gate is not None
            else ()
        )
        carries_request_approval = include_generic_policy_pager(
            approval_gate,
            has_potential_write_tool=capability.has_potential_write_tool,
        )
        if not carries_request_approval:
            logger.info(
                "request_approval omitted: permission gates already page or"
                " observed MCP surface has no actionable tools tool_count=%d"
                " probe_complete=%s failures=%d",
                capability.tool_count,
                capability.complete,
                len(capability.failures),
            )

        # Publication is a built-in coding protocol and remains discoverable
        # without a mounted workspace; its implementation refuses safely until
        # the platform supplies one. The generic pager is independently omitted
        # when the observed surface has no actionable tools.
        platform_servers = {
            APPROVAL_SERVER_NAME: build_approval_server(
                approval_gate,
                managed_workspace=mounted_workspace is not None,
                include_request_approval=carries_request_approval,
                progress_tool=(
                    build_progress_tool(progress[1], progress[0], progress_activity)
                    if progress is not None
                    else None
                ),
            ),
            **(
                {STATE_SERVER_NAME: build_state_server(state_client)}
                if state_client is not None
                else {}
            ),
        }
        structured_resume = build_structured_resume(
            conversation_replay.messages,
            curie_session_id=config.session.session_id,
            cwd=workspace_cwd,
            harness_replay=conversation_replay.harness_replay,
        )
        real_options = build_options(
            plugins=compiled.plugins,
            model=config.model,
            system_prompt=system_prompt,
            max_turns=config.max_turns,
            max_budget_usd=config.max_usd_per_day,
            # Curie's durable contract is ordered role/content messages. The
            # Claude adapter prefers its optional opaque checkpoint to preserve
            # native cache shape, and otherwise materializes a deterministic
            # process-local SDK resume envelope from the portable messages.
            resume=structured_resume.resume,
            session_id=structured_resume.session_id,
            session_store=structured_resume.session_store,
            # Operator-set thinking depth (#1182, ADR-0098); None omits the SDK
            # option entirely rather than defaulting it.
            thinking=config.thinking,
            task_budget_hint=config.session.budget.task_budget_hint,
            env=sdk_env or {},
            # The approval gate's PreToolUse matcher rides here alongside the
            # bundle's own (#1852): can_use_tool is skipped by any permission
            # rule that already allows the call (claude_agent_sdk/types.py:
            # 1932-1948), and a skill's allowed-tools frontmatter is exactly
            # such a rule, so the hook is the only layer that sees every call.
            hooks=session_hooks,
            # Platform tools and connectors share the SDK MCP channel. The
            # generic policy pager is present only on an actionable surface;
            # state and publication remain independent platform capabilities.
            mcp_servers=build_mcp_servers(
                platform=platform_servers,
                derived=derived_mcp_servers,
            ),
            can_use_tool=(build_can_use_tool(approval_gate) if approval_gate is not None else None),
            cwd=workspace_cwd,
            web_search_enabled=web_search_enabled,
            policy_disallowed_tools=policy_hidden_tools,
            disallowed_tools=config.disallowed_tools,
        )

    sdk_generation = 0

    def factory() -> ModelSession:
        if fake_model:
            # The offline fake honors the same permission gate (#245) the real
            # session does, using the shared approval_gate instance so a blocked
            # call flips the turn to awaiting-approval exactly as the SDK path
            # would. Bundle PreToolUse command hooks (#272) are NOT wired here:
            # they shell out and would break the fake's offline no-op guarantee
            # (the can_use_tool gate is a pure membership check, so it is safe).
            return FakeModelSession(
                can_use_tool=(
                    build_can_use_tool(approval_gate) if approval_gate is not None else None
                ),
                # Share the same gate so a scripted request_approval resolves its
                # route through the real decision table on the offline tier (#561).
                approval_gate=approval_gate,
                replay_messages=conversation_replay.messages,
                disallowed_tools=config.disallowed_tools,
            )
        assert real_options is not None
        nonlocal sdk_generation
        generation = sdk_generation
        sdk_generation += 1
        # Boot keeps the deterministic structured-resume envelope so a
        # CURIE_HISTORY_REF reconnect hits the same SDK session id. Reset is
        # not a process restart: it must mint a new id. Reusing the boot id
        # after a completed turn makes Claude Code refuse reconnect with
        # "Session ID ... is already in use" and POST /v1/reset returns 500
        # (#2221). Native checkpoint entries carry the previous id, so later
        # generations rematerialize portable messages only.
        if generation == 0:
            return ClaudeAgentSession(real_options)
        envelope = build_structured_resume(
            conversation_replay.messages,
            curie_session_id=f"{config.session.session_id}:reset:{generation}",
            cwd=workspace_cwd,
            harness_replay=None,
        )
        return ClaudeAgentSession(
            replace(
                real_options,
                resume=envelope.resume,
                session_id=envelope.session_id if envelope.resume is None else None,
                session_store=envelope.session_store,
                session_store_flush="eager" if envelope.session_store is not None else "batched",
            )
        )

    provider = build_tracer_provider(
        config.session.otel,
        config.session.session_id,
        config.session.sandbox_id,
    )
    return bind_status_attestation(
        SessionRunner(
            session_factory=factory,
            ceiling=config.ceiling,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(
                readonly_tools=harness.readonly_tools
                | (
                    observed_readonly_tools - approval_gate.required
                    if approval_gate is not None
                    else observed_readonly_tools
                )
            ),
            trace_name=f"curie-run:{config.session.session_id}",
            session_id=config.session.session_id,
            model=config.model,
            memory_store=memory_store,
            history_store=history_store,
            approval_gate=approval_gate,
            approval_resumed_kind=config.approval_resumed_kind,
            approval_decision=config.approval_decision,
            false_completion_check=config.false_completion_check,
            history_resumed=conversation_replay.present,
            progress_activity=progress_activity if progress is not None else None,
            connector_failures=connector_failures
            or (
                capability.connector_failures
                if capability is not None
                else ()
            ),
            connector_reprobe=connector_reprobe,
            connector_availability=connector_availability,
            history_capacity_exceeded=history_capacity_exceeded,
        ),
        session_id=config.session.session_id,
        sandbox_id=config.session.sandbox_id,
        cwd=workspace_cwd,
    )


async def _load_memory(config: RunnerConfig) -> tuple[MemoryStore, str | None]:
    """Resolve CURIE_MEMORY_REF and load prior memory into a boot preamble.

    Runs at boot (before the port is up), so a bad ref or an unreachable store
    fails the process visibly rather than after serving. A transient load
    failure degrades to "no memory" and does NOT block boot -- an agent must
    still be able to run when its memory store is briefly unavailable.
    """

    store = resolve_memory(config.session.memory_ref, os.environ)
    try:
        records = await store.load()
    except Exception as exc:  # noqa: BLE001 - degrade to no-memory, never fail boot
        logger.warning(
            "memory load failed session=%s error_class=%s: %s (booting without memory)",
            config.session.session_id,
            type(exc).__name__,
            exc,
        )
        return store, None
    logger.info("memory loaded session=%s records=%d", config.session.session_id, len(records))
    return store, format_memory_preamble(records)


# Boot compaction passes (#2927): each is a compare-and-set rewrite of the value
# boot loaded, retried on a concurrent write with a fresh load.
_BOOT_COMPACTION_PASSES = 3


async def _load_history(
    config: RunnerConfig,
) -> tuple[TranscriptStore, ConversationReplay, bool]:
    """Resolve, compact when needed, and load the structured replay prefix.

    A configured history ref is continuity-critical. Failure is fatal: silently
    answering without the prior tool/approval context can duplicate an operation.

    The replay is windowed to a recent structured tail so a long thread does not
    balloon provider context; the operator's window knobs override the sane
    defaults. They arrive through the declared boot env (parsed defensively, so
    a typo degrades to the default rather than failing boot), which is why the
    defaults are applied here rather than read off the process env at this call.

    When the boot summary append is refused at the transcript cap (or its
    headroom reserve), boot compacts the stored value it loaded with a
    compare-and-set rewrite, then reloads and rebuilds the replay from what is
    stored (#2927). A write since that load conflicts, and the next pass reloads
    instead of writing a stale view over it; there are at most three passes.

    A compaction that still cannot fit (or three passes that never settle) is
    the one exception to fatal (#2820): no cold sandbox can fix that thread, so
    the runner still boots and the returned flag makes it refuse every turn with
    the append path's non-retryable capacity event instead of dying unserved.
    """

    store = resolve_history(config.history_ref, os.environ)
    max_turns = (
        config.history_max_turns
        if config.history_max_turns is not None
        else DEFAULT_REPLAY_MAX_TURNS
    )
    max_bytes = (
        config.history_max_bytes
        if config.history_max_bytes is not None
        else DEFAULT_REPLAY_MAX_BYTES
    )
    capacity_exceeded = False
    compacted = False
    try:
        records = await store.load()
        replay, summary = build_conversation_replay(
            records, max_turns=max_turns, max_bytes=max_bytes
        )
        passes = 0
        while summary is not None:
            try:
                await store.append(summary)
                compacted = True
                break
            except HistoryCapacityError as exc:
                if passes == _BOOT_COMPACTION_PASSES:
                    logger.error(
                        "history capacity exceeded at boot session=%s status=%d "
                        "passes=%d (refusing turns)",
                        config.session.session_id,
                        exc.status,
                        passes,
                    )
                    capacity_exceeded = True
                    break
            passes += 1
            try:
                await store.compact()
                compacted = True
            except HistoryConflictError:
                logger.warning(
                    "history compaction conflicted at boot session=%s pass=%d (reloading)",
                    config.session.session_id,
                    passes,
                )
            except HistoryCapacityError as exc:
                logger.error(
                    "history capacity exceeded at boot session=%s status=%d "
                    "(refusing turns)",
                    config.session.session_id,
                    exc.status,
                )
                capacity_exceeded = True
                break
            records = await store.load()
            replay, summary = build_conversation_replay(
                records, max_turns=max_turns, max_bytes=max_bytes
            )
    except Exception as exc:  # noqa: BLE001 - translate loader failures consistently
        status = (
            exc.args[0]
            if len(exc.args) == 1 and isinstance(exc.args[0], int)
            else None
        )
        if status is None:
            logger.error(
                "history load failed session=%s error_class=%s",
                config.session.session_id,
                type(exc).__name__,
            )
        else:
            logger.error(
                "history load failed session=%s error_class=%s status=%d",
                config.session.session_id,
                type(exc).__name__,
                status,
            )
        raise HistoryError("configured structured history could not be loaded") from None
    logger.info(
        "history loaded session=%s records=%d messages=%d compacted=%s",
        config.session.session_id,
        len(records),
        len(replay.messages),
        compacted and not capacity_exceeded,
    )
    return store, replay, capacity_exceeded


@dataclass(frozen=True)
class _BootFetches:
    """Independent boot-time loads that previously ran as sequential anyio.run calls."""

    memory_store: MemoryStore
    memory_preamble: str | None
    history_store: TranscriptStore
    conversation_replay: ConversationReplay
    mcp_capability: McpToolCapabilityProbe | None
    connector_failures: tuple[ConnectorCapabilityFailure, ...] = ()
    history_capacity_exceeded: bool = False


async def _load_boot_fetches(
    config: RunnerConfig,
    fake_model: bool,
    sdk_env: dict[str, str] | None,
) -> _BootFetches:
    """Load memory, structured history, and (on the real-model path) MCP capability together."""

    resolve_memory(config.session.memory_ref, os.environ)
    resolve_history(config.history_ref, os.environ)

    memory: tuple[MemoryStore, str | None] | None = None
    history: tuple[TranscriptStore, ConversationReplay, bool] | None = None
    capability: McpToolCapabilityProbe | None = None
    derived = derive_mcp_servers(
        config.session.plugin_dir,
        release=config.connector_release,
        agent=config.connector_agent,
        namespace=config.connector_namespace,
    )
    expansion_failures = (
        diagnose_derived_connector_headers(
            derived, {**os.environ, **dict(sdk_env or {})}
        )
        if fake_model
        else ()
    )

    async def load_memory() -> None:
        nonlocal memory
        memory = await _load_memory(config)

    async def load_history() -> None:
        nonlocal history
        history = await _load_history(config)

    async def probe() -> None:
        nonlocal capability
        capability = await probe_mcp_tool_capability(
            config.session.plugin_dir,
            derived,
            sdk_env,
        )

    async with anyio.create_task_group() as tg:
        tg.start_soon(load_memory)
        tg.start_soon(load_history)
        if not fake_model:
            tg.start_soon(probe)

    assert memory is not None
    assert history is not None
    connector_failures = (
        capability.connector_failures if capability is not None else expansion_failures
    )
    return _BootFetches(
        memory_store=memory[0],
        memory_preamble=memory[1],
        history_store=history[0],
        conversation_replay=history[1],
        mcp_capability=capability,
        connector_failures=connector_failures,
        history_capacity_exceeded=history[2],
    )


def _serve() -> None:
    # The NAME comes from the one declaration (#488); the parse deliberately does
    # not. BootEnv reads any non-"0" value as true, while this boot has always
    # required an explicit 1/true/yes -- routing through it would turn
    # CURIE_FAKE_MODEL=false into fake-model ON. The declaration moved; the wire
    # did not.
    fake_model = os.environ.get(BootEnv.env_key("fake_model"), "").lower() in (
        "1",
        "true",
        "yes",
    )
    logger.info("runner starting fake_model=%s", fake_model)
    config = RunnerConfig.from_env(os.environ)
    logger.info(
        "runner configured session=%s model=%s port=%d harness=%s",
        config.session.session_id,
        config.model,
        config.port,
        config.harness,
    )
    # The active harness (ADR-0060), SELECTED by config (CURIE_HARNESS, default
    # the built-in Claude). Its manifest supplies the per-spawn env builder used
    # just below and is threaded into build_runner so the read-only tool set and
    # bundle compile come from the same declaration. An unregistered selection
    # raises here, so a misconfigured harness fails visibly before the port is up.
    harness = _resolve_harness(config.harness)
    # A real session authenticates from the SDK's own credential env; the
    # harness's per-spawn env builder maps the forwarded ACI CURIE_CREDENTIALS
    # reference onto it (a no-op for a fake run, which needs no credential).
    # Raises on an unsupported credential so the process fails visibly before the
    # port is up rather than after a real call.
    override = None
    if not fake_model:
        try:
            override = harness.build_spawn_env(os.environ)
        except UnsupportedCredentialError as exc:
            logger.error("credential resolution failed: %s", exc)
            raise
    fetches = anyio.run(_load_boot_fetches, config, fake_model, override)
    memory_store, memory_preamble = fetches.memory_store, fetches.memory_preamble
    history_store, conversation_replay = fetches.history_store, fetches.conversation_replay
    workspace_candidate = Path("/workspace")
    workspace_path: Path | None = (
        workspace_candidate
        if workspace_candidate.is_dir() and (workspace_candidate / ".git").exists()
        else None
    )
    attachments_candidate = ATTACHMENTS_DIR
    attachments_path: Path | None = (
        attachments_candidate if attachments_candidate.is_dir() else None
    )
    runner = build_runner(
        config,
        fake_model=fake_model,
        sdk_env=override,
        memory_store=memory_store,
        memory_preamble=memory_preamble,
        history_store=history_store,
        conversation_replay=conversation_replay,
        mcp_capability=fetches.mcp_capability,
        harness=harness,
        workspace_path=workspace_path,
        attachments_path=attachments_path,
        connector_failures=fetches.connector_failures,
        history_capacity_exceeded=fetches.history_capacity_exceeded,
    )
    def capture_mounted_workspace() -> WorkspaceSnapshot:
        # The sanitized, credential-free origin in /workspace/.git/config is
        # the repository fact. The proposal is runner-held state from the
        # permission-gated tool input; neither needs another claim env.
        if workspace_path is None:
            raise RuntimeError("managed workspace disappeared before snapshot wiring")
        gate = runner._approval_gate  # noqa: SLF001 - same package wiring
        return capture_workspace_snapshot(
            workspace_path,
            publication_title=gate.publication_title if gate is not None else None,
            publication_body=gate.publication_body if gate is not None else None,
        )

    snapshot_callback = capture_mounted_workspace if workspace_path is not None else None

    app = create_app(runner, token=config.runner_token, snapshotter=snapshot_callback)

    async def _startup(_app: web.Application) -> None:
        try:
            await runner.start()
        except Exception as exc:
            logger.error("session start failed error_class=%s: %s", type(exc).__name__, exc)
            raise
        logger.info("session started session=%s", config.session.session_id)

    app.on_startup.append(_startup)
    web.run_app(app, host="0.0.0.0", port=config.port)


def main() -> None:
    install_stdout_redaction()
    telemetry = bootstrap_service_telemetry(
        "curie-runner",
        service_version=__version__,
        logger=logger,
        environ=os.environ,
    )
    try:
        _serve()
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
