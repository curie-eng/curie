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
from dataclasses import dataclass, field
from typing import Any

from aci_protocol import BootEnv, SessionConfig

from .harness.registry import DEFAULT_HARNESS
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
    # The harness whose contribution manifest drives this runner (ADR-0060,
    # #844): read from the runner-local CURIE_HARNESS knob (default the
    # built-in Claude), NOT a BootEnv contract key -- the same runner-local read
    # pattern as false_completion_check below. ``__main__`` resolves this name
    # through the harness registry; an unregistered selection fails the boot.
    harness: str
    max_turns: int
    # Where this sandbox's hosted connectors live (ADR-0086, #1118). Absent as
    # a set on any tier that hosts nothing -- see connectors.derive_mcp_servers.
    connector_release: str | None
    connector_agent: str | None
    connector_namespace: str | None
    history_ref: str | None
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
    # Turn-end reconciliation marker (#544, Decision A2), authority-free. The
    # worker injects CURIE_APPROVAL_RESUMED_KIND='policy' at resume boot to
    # record that the approval being resumed from was a POLICY gate. Unlike
    # CURIE_APPROVAL_GRANT_TOOL it confers NO authority -- it is a fact about
    # the past, used only to emit an observe-only warning when a resumed policy
    # turn takes no action. It must never influence can_use_tool.
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
    # Pool bootstrap credential (ADR-0122), delivered by the substrate's pool
    # template only; ``None`` when absent. ``CredentialAuthority`` gives a
    # present ``runner_token`` precedence, so a pod bound at boot never enters
    # bootstrap mode. Never repr'd: it is secret material.
    runner_bootstrap_token: str | None = field(repr=False)
    # Operator bounds on the rehydrated structured history, reachable through
    # the chart's runner.extraEnv. None hands the consumer its own default, so
    # the defaults live at the call site rather than here.
    history_max_turns: int | None
    history_max_bytes: int | None

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
        # Runner-local harness selection (ADR-0060, #844), not a BootEnv key:
        # empty/unset selects the built-in Claude harness.
        harness = env.get("CURIE_HARNESS", "").strip() or DEFAULT_HARNESS
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
            approval_required_tools=boot.approval_required_tools,
            approval_grant_tool=boot.approval_grant_tool,
            approval_resumed_kind=boot.approval_resumed_kind,
            approval_decision=boot.approval_decision,
            false_completion_check=false_completion_check,
            port=boot.port if boot.port is not None else 8080,
            runner_token=boot.runner_token,
            runner_bootstrap_token=boot.runner_bootstrap_token,
            history_max_turns=boot.history_max_turns,
            history_max_bytes=boot.history_max_bytes,
        )
