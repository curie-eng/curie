"""Runner configuration: the declared BootEnv, read back as the runner's view.

``BootEnv`` (from ``aci-protocol``) is the single declaration of the worker-to-
runner boot env: the frozen ACI ``SessionConfig`` plus the platform-operational
vars. ``RunnerConfig`` is the runner-local shape the boot path consumes, built
from that one parse rather than from its own ``CURIE_*`` reads -- every name
this lane needs is declared once, in the contract, so a rename cannot leave the
sandbox booting fine with a silently dropped feature (#488, ADR-0049).

The parse tolerance is deliberately non-uniform and lives in ``BootEnv``: the
turn cap and the port raise on garbage, the history-window knobs degrade to
their default. Each var keeps the behavior it has.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from aci_protocol import BootEnv, SessionConfig

from .harness.registry import DEFAULT_HARNESS
from .memory_facts import MAX_FACTS_PER_MEMORY
from .thinking import parse_thinking


@dataclass(frozen=True)
class RunnerConfig:
    session: SessionConfig
    model: str | None
    # The SDK thinking config parsed from CURIE_THINKING (#1182, ADR-0098), or
    # None when the operator set nothing -- in which case the option is OMITTED
    # from ClaudeAgentOptions rather than defaulted, so the model's own behavior
    # is untouched. Parsed here at boot so a malformed value fails the boot
    # loudly instead of being silently dropped on every turn. The vocabulary is
    # this lane's (curie_runner.thinking), not the boot contract's.
    thinking: dict[str, Any] | None
    # Internal harness selection (ADR 0140), read from CURIE_HARNESS rather than
    # the frozen BootEnv contract. __main__ admits only Claude and its existing
    # aliases, and refuses every other name before registry discovery.
    harness: str
    max_turns: int
    # Where this sandbox's hosted connectors live (ADR-0086, #1118). Absent as
    # a set on any tier that hosts nothing -- see connectors.derive_mcp_servers.
    connector_release: str | None
    connector_agent: str | None
    connector_namespace: str | None
    history_ref: str | None
    # This turn's channel memory (#1461, ADR-0167): the binding-scoped memory
    # namespace URL. The worker sets it whenever the turn has a binding, so the
    # channel's facts load whether memory writes are on or off (#3621). Whether
    # the remember/update/forget tools mount is ``memory_writes_on`` below.
    channel_memory_ref: str | None
    # Tool names whose calls require human approval (#245, ADR-0010). The
    # runner intercepts these proactively via the SDK can_use_tool callback
    # and ends the turn awaiting-approval instead of executing. Injected
    # per-agent by the worker binding as CURIE_APPROVAL_REQUIRED_TOOLS
    # (comma separated); None/empty means no permission gates and the
    # pre-gate bypass posture is preserved.
    approval_required_tools: list[str] | None
    # One-shot post-approval allowance (#430, ADR-0035): the single tool name a
    # resume-boot grant lets through exactly once on the boot turn. A runner-local
    # knob injected by the worker binding as CURIE_APPROVAL_GRANT_TOOL when it
    # boots the resume claim for a genuinely-approved permission-gate block;
    # None/empty means no grant and the ordinary deny-and-pause posture holds.
    approval_grant_tool: str | None
    # Trusted resume input parsed by BootEnv (#3255): the canonical arguments
    # the approver saw. The gate admits the granted tool only with these exact
    # arguments (#3174).
    approval_grant_arguments: dict[str, Any] | None
    # Turn-end reconciliation marker (#544, Decision A2), authority-free. The
    # worker injects CURIE_APPROVAL_RESUMED_KIND='policy' at resume boot to
    # record that the approval being resumed from was a POLICY gate. Unlike
    # CURIE_APPROVAL_GRANT_TOOL it confers NO authority -- it is a fact about
    # the past, used only to emit an observe-only warning when a resumed policy
    # turn takes no action, and to NARROW a name-only grant to genuine policy
    # resumes (#3174). It must never widen what can_use_tool admits.
    approval_resumed_kind: str | None
    # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
    # (approved/rejected/expired) of the approval this resume boot is resuming
    # from. Authority-free like approval_resumed_kind -- it confers nothing,
    # it is stamped onto the turn's OTel span so an operator can see whether
    # (and how) an approval gate was resolved from the trace, closing the
    # "did an approval get requested" gap ADR-0038 named open.
    approval_decision: str | None
    # Opt-in false-completion check (#517), authority-free and observe-only. When
    # CURIE_FALSE_COMPLETION_CHECK is truthy, a turn that ends DONE with a
    # substantive answer but ZERO tool calls emits a non-terminal warning frame.
    # Default off, exactly like approval_resumed_kind's observe-only pattern; it
    # never influences can_use_tool or the final's status.
    false_completion_check: bool
    port: int
    runner_token: str | None
    # Operator bounds on the rehydrated structured history, reachable through
    # the chart's runner.extraEnv. None hands the consumer its own default, so
    # the defaults live at the call site rather than here.
    history_max_turns: int | None
    history_max_bytes: int | None
    # Runner-local deny list (#2429): comma-separated tool names handed to
    # ClaudeAgentOptions.disallowed_tools. Unset/blank keeps the historical
    # empty list so an unconfigured agent is unchanged. Not a BootEnv field.
    disallowed_tools: tuple[str, ...]
    # The caller token this sandbox presents to its hosted connectors
    # (ADR-0168 decision 7), or None when the worker minted none.
    connector_caller_token: str | None = None
    # The operator's memory-writes switch as the worker sent it (#3659):
    # True or False explicitly alongside a channel ref, None from an older
    # worker that sent no flag and only ever sent the ref with writes on.
    memory_writes: bool | None = None
    # How many facts each memory may hold and boot shows the agent (#3624),
    # the operator's CURIE_MEMORY_MAX_FACTS or the default of 200. One number
    # for both, so a saved fact is never left out of the prompt.
    memory_max_facts: int = MAX_FACTS_PER_MEMORY

    @property
    def memory_writes_on(self) -> bool:
        """Whether this turn may save channel memory (#3621).

        An explicit flag decides. Without one (an older worker), a channel ref
        alone means writes on, which is what that worker meant by sending it.
        """

        if self.memory_writes is not None:
            return self.memory_writes
        return bool(self.channel_memory_ref)

    @property
    def ceiling(self) -> int:
        """The per-run output-token ceiling from the ACI budget."""

        return self.session.budget.max_output_tokens_per_run

    @property
    def max_usd_per_day(self) -> float:
        return self.session.budget.max_usd_per_day

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> RunnerConfig:
        """Parse a RunnerConfig from a process environment mapping.

        ``BootEnv.from_env`` is the single parse; a malformed or missing required
        var raises there. ``history_ref`` is read only from an explicit
        ``CURIE_HISTORY_REF``, the URL of this thread's transcript namespace on
        the state API (ADR-0029, resolved by ``history.py`` into a
        ``TranscriptStore`` and delivered as ordered harness messages). It is deliberately
        NOT derived from ``CURIE_MEMORY_REF``: memory is per-agent durable
        lessons, history is this thread's conversation (ADR-0025 keeps them
        distinct). Both live outside the sandbox and are rehydrated at boot
        (ADR-0003, stateless-first).

        The turn-cap and port defaults are applied here rather than on the model:
        a non-None default on ``BootEnv`` would render keys nobody sends and move
        the wire.
        """

        boot = BootEnv.from_env(env)
        # A runner-local read, not a BootEnv contract key (#517): the false-
        # completion check is observe-only, so it stays a direct env read parsed
        # to an explicit 1/true/yes truthy rather than a declared BootEnv field.
        # The worker's producer lane (WorkerConfig.false_completion_check ->
        # apps/worker/src/curie_worker/binding.py's FALSE_COMPLETION_CHECK_ENV
        # write, #669) forwards this same literal name, so it is no longer only
        # reachable on a hand-run local runner.
        false_completion_raw = env.get("CURIE_FALSE_COMPLETION_CHECK", "")
        false_completion_check = false_completion_raw.strip().lower() in ("1", "true", "yes")
        # Internal harness selection (ADR 0140), not a BootEnv key.
        # Empty or unset selects the supported Claude harness.
        harness = env.get("CURIE_HARNESS", "").strip() or DEFAULT_HARNESS
        disallowed_tools = tuple(
            name.strip()
            for name in env.get("CURIE_DISALLOWED_TOOLS", "").split(",")
            if name.strip()
        )
        return cls(
            session=boot.session,
            model=boot.model,
            thinking=parse_thinking(boot.thinking),
            harness=harness,
            max_turns=boot.max_turns if boot.max_turns is not None else 20,
            connector_release=boot.connector_release,
            connector_agent=boot.connector_agent,
            connector_namespace=boot.connector_namespace,
            history_ref=boot.history_ref,
            channel_memory_ref=boot.channel_memory_ref,
            approval_required_tools=boot.approval_required_tools,
            approval_grant_tool=boot.approval_grant_tool,
            approval_grant_arguments=boot.approval_grant_arguments,
            approval_resumed_kind=boot.approval_resumed_kind,
            approval_decision=boot.approval_decision,
            false_completion_check=false_completion_check,
            port=boot.port if boot.port is not None else 8080,
            runner_token=boot.runner_token,
            history_max_turns=boot.history_max_turns,
            history_max_bytes=boot.history_max_bytes,
            disallowed_tools=disallowed_tools,
            connector_caller_token=boot.connector_caller_token,
            memory_writes=boot.memory_writes,
            memory_max_facts=(
                boot.memory_max_facts
                if boot.memory_max_facts is not None
                else MAX_FACTS_PER_MEMORY
            ),
        )
