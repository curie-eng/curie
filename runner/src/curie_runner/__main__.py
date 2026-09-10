"""Runner entrypoint: build the session from the environment and serve the ACI.

Reads the ACI ``CURIE_*`` / ``OTEL_EXPORTER_OTLP_*`` env into a RunnerConfig,
wires the real claude-agent-sdk session (validated plugin bundle, budget, OTel),
and serves the HTTP channel. The session is started in ``on_startup`` so a plugin
or connect failure fails the process visibly rather than after the port is up.
"""

from __future__ import annotations

import logging
import os
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
    PUBLISH_TOOL_NAME,
    ApprovalPolicyError,
    assert_gates_not_shadowed,
    build_approval_gate,
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
    policy_disallowed_tools,
    resolve_approval_policy,
)
from .config import RunnerConfig
from .connectors import build_mcp_servers, derive_mcp_servers
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
    HistoryError,
    StructuredReplayUnsupported,
    TranscriptStore,
    build_conversation_replay,
    resolve_history,
)
from .hooks import load_bundle_hooks
from .mcp_tool_capability import McpToolCapabilityProbe, probe_mcp_tool_capability
from .memory import MemoryStore, format_memory_preamble, resolve_memory
from .otel import RunTracer, build_tracer_provider
from .plugin import load_bundle_web_search_enabled
from .redact import install_stdout_redaction
from .sdk_auth import UnsupportedCredentialError
from .server import bind_status_attestation, create_app
from .session import SessionRunner
from .side_effects import SideEffectClassifier
from .state import STATE_SERVER_NAME, build_state_server, resolve_state_client
from .workspace_snapshot import WorkspaceSnapshot, capture_workspace_snapshot

logger = logging.getLogger("curie_runner")


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


def _compose_system_prompt(
    base: str | None,
    memory_preamble: str | None,
    *,
    model: str | None,
) -> str | None:
    """Compose durable memory, bundle instructions, and model identity.

    Conversation history is deliberately absent: ADR-0119 requires it to cross
    the harness boundary as ordered messages, never rendered system text.
    """

    model_preamble = f"Configured model: {model}" if model else None
    parts = [p for p in (memory_preamble, base, model_preamble) if p]
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
    # Prior memory (#264) still leads the system prompt. Conversation history
    # (#20) deliberately does not: ADR-0119 sends its ordered messages through
    # the harness adapter below. The configured model identity is appended after
    # the bundle prompt.
    system_prompt = _compose_system_prompt(
        system_prompt,
        memory_preamble,
        model=config.model,
    )
    # In-bundle PreToolUse guardrails declared in the manifest hooks field (#272),
    # translated into SDK HookMatcher callbacks. None when the bundle declares none.
    bundle_hooks = load_bundle_hooks(config.session.plugin_dir)
    mounted_workspace = (
        workspace_path
        if workspace_path is not None
        and workspace_path.is_dir()
        and (workspace_path / ".git").exists()
        else None
    )
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

    # The gate's own PreToolUse matcher (#1852), MERGED with the bundle's rather
    # than replacing it. Built here, after the boot refusal above has passed, so
    # a bundle Curie is about to refuse never gets a hook registered against it.
    # No gate -> no hook, which is what keeps an unconfigured agent's wiring
    # byte-identical to before.
    session_hooks = _merge_pre_tool_use_hooks(
        build_approval_hook(approval_gate) if approval_gate is not None else None,
        bundle_hooks,
    )

    # The durable state store exposed to bundle code (#249): when the worker
    # forwarded CURIE_STATE_URL, mount the platform ``curie-state`` MCP
    # server so a skill can read/write suspend/resume-surviving state without the
    # bundle shipping its own server. Absent (fake/local, or an older worker), no
    # state server is mounted and the agent simply sees no state tools.
    state_client = resolve_state_client(os.environ)
    workspace_cwd = str(mounted_workspace) if mounted_workspace is not None else None
    derived_mcp_servers = derive_mcp_servers(
        config.session.plugin_dir,
        release=config.connector_release,
        agent=config.connector_agent,
        namespace=config.connector_namespace,
    )

    # A configured permission gate is already positive evidence that the
    # session carries an actionable approval boundary. Publication is excluded:
    # its dedicated tool already raises its own approval and the sandbox cannot
    # execute publication itself, so adding the generic pager beside it would
    # recreate #1444 under a different tool name.
    carries_explicit_action_gate = approval_gate is not None and bool(
        approval_gate.required - {PUBLISH_TOOL_NAME}
    )

    real_options: ClaudeAgentOptions | None = None
    observed_readonly_tools: frozenset[str] = frozenset()
    if not fake_model:
        # The bundle's live MCP ``tools/list`` response is the actual advertised
        # MCP surface. Probe even when an explicit gate already requires the
        # generic pager: exact readOnlyHint=true observations also drive receipt
        # and retry classification. Missing hints, uninspectable declarations,
        # and probe failures preserve the historical fail-closed behavior. The
        # annotation remains a non-authoritative hint: it never authorizes or
        # denies tool execution.
        capability = mcp_capability
        if capability is None:
            capability = anyio.run(
                probe_mcp_tool_capability,
                config.session.plugin_dir,
                derived_mcp_servers,
                sdk_env,
            )
        observed_readonly_tools = capability.readonly_tools
        policy_hidden_tools = (
            policy_disallowed_tools(approval_gate, capability.observed_tools)
            if approval_gate is not None
            else ()
        )
        carries_request_approval = (
            carries_explicit_action_gate or capability.has_potential_write_tool
        )
        if not carries_request_approval:
            logger.info(
                "request_approval omitted: observed MCP surface has no actionable"
                " tools tool_count=%d probe_complete=%s failures=%d",
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


async def _load_history(config: RunnerConfig) -> tuple[TranscriptStore, ConversationReplay]:
    """Resolve, compact when needed, and load the structured replay prefix.

    A configured history ref is continuity-critical. Failure is fatal: silently
    answering without the prior tool/approval context can duplicate an operation.

    The replay is windowed to a recent structured tail so a long thread does not
    balloon provider context; the operator's window knobs override the sane
    defaults. They arrive through the declared boot env (parsed defensively, so
    a typo degrades to the default rather than failing boot), which is why the
    defaults are applied here rather than read off the process env at this call.
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
    try:
        records = await store.load()
        replay, summary = build_conversation_replay(
            records, max_turns=max_turns, max_bytes=max_bytes
        )
        if summary is not None:
            await store.append(summary)
    except Exception as exc:  # noqa: BLE001 - translate loader failures consistently
        logger.error(
            "history load failed session=%s error_class=%s: %s",
            config.session.session_id,
            type(exc).__name__,
            exc,
        )
        raise HistoryError("configured structured history could not be loaded") from exc
    logger.info(
        "history loaded session=%s records=%d messages=%d compacted=%s",
        config.session.session_id,
        len(records),
        len(replay.messages),
        summary is not None,
    )
    return store, replay


@dataclass(frozen=True)
class _BootFetches:
    """Independent boot-time loads that previously ran as sequential anyio.run calls."""

    memory_store: MemoryStore
    memory_preamble: str | None
    history_store: TranscriptStore
    conversation_replay: ConversationReplay
    mcp_capability: McpToolCapabilityProbe | None


async def _load_boot_fetches(
    config: RunnerConfig,
    fake_model: bool,
    sdk_env: dict[str, str] | None,
) -> _BootFetches:
    """Load memory, structured history, and (on the real-model path) MCP capability together."""

    resolve_memory(config.session.memory_ref, os.environ)
    resolve_history(config.history_ref, os.environ)

    memory: tuple[MemoryStore, str | None] | None = None
    history: tuple[TranscriptStore, ConversationReplay] | None = None
    capability: McpToolCapabilityProbe | None = None

    async def load_memory() -> None:
        nonlocal memory
        memory = await _load_memory(config)

    async def load_history() -> None:
        nonlocal history
        history = await _load_history(config)

    async def probe() -> None:
        nonlocal capability
        derived = derive_mcp_servers(
            config.session.plugin_dir,
            release=config.connector_release,
            agent=config.connector_agent,
            namespace=config.connector_namespace,
        )
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
    return _BootFetches(
        memory_store=memory[0],
        memory_preamble=memory[1],
        history_store=history[0],
        conversation_replay=history[1],
        mcp_capability=capability,
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
