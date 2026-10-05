---
seam: Harness in-proc / ModelSession
kind: CLEAN
impls: 1 + fake
grade: A-
vision_row: Harness / runtime
epics:
  - "#25"
order: 2
epic_note: folds into
---
# INTERFACE: Harness in-process (`ModelSession`)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 + fake &nbsp;·&nbsp; **Swap-readiness grade:** A-
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

Inside the runner, the model harness is reached through the `ModelSession`
Protocol. The session loop, ACI translation, budget enforcement, telemetry,
and HTTP layer call that port. Steer and interrupt are explicit operations.
The messages crossing it still use the Claude SDK dataclasses, so the typed
port does not provide a neutral event format.

[ADR 0140](../../adr/0140-curie-supports-one-model-harness-until-a-second-one-exists.md)
limits supported boot to Claude and stops the unfinished process boundary
program in ADR 0061. A real second engine must reopen that decision. The
withdrawn `TurnEvent` model from #307 and #315 never shipped.

The approval policy gate at `runner/src/curie_runner/approval.py` contains no
SDK imports. Its Claude permission callback and hook adapter live at
`runner/src/curie_runner/harness/claude/approval.py`.

17 runner modules import `claude_agent_sdk` today (`check.py`, `session.py`,
`hooks.py`, `adapter.py`, `mcp_argv.py`, `fake.py`, `preflight_blocked.py`,
`approval.py`, `translate.py`,
`plugin.py`, `state.py`, `progress.py`, `turn_progress.py`, `issue_read.py`,
`usage_report.py`, `tool_access.py`, `__main__.py`). In that inventory,
`approval.py` means `runner/src/curie_runner/harness/claude/approval.py`;
the core `runner/src/curie_runner/approval.py` has no SDK import.
`preflight_blocked.py` also lives inside the Claude harness package. The import
rules in `pyproject.toml` forbid SDK dependencies in the gate and ratchet the
legacy edges outside the Claude harness package. Only that package is exempt.

## Current contract

A session supplies an object satisfying `ModelSession`
(`runner/src/curie_runner/adapter.py::ModelSession`), a five-method `Protocol`:

- `async def connect(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.connect`) — start/attach the harness,
  rehydrating if a resume ref is configured.
- `async def query(self, text: str) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.query`) — push a user message;
  a `query` issued while a turn is live is the mid-run **steer**.
- `def receive_turn(self) -> AsyncIterator[Any]` (`runner/src/curie_runner/adapter.py::ModelSession.receive_turn`) — yield the harness
  messages for the current turn and, optionally, the runner-internal payload-free
  `PartialMessageBoundary` (`runner/src/curie_runner/adapter.py::PartialMessageBoundary`)
  and `StreamedToolUseBoundary`
  (`runner/src/curie_runner/adapter.py::StreamedToolUseBoundary`), ending at the
  terminal result. Both boundaries are telemetry yields, not `translate_message` input.
- `async def interrupt(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.interrupt`) — native hard stop at the next
  safe boundary.
- `async def close(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.close`) — tear down.

Optional capabilities sit beside the five-method port. A session may implement
`McpServerReconnector.ensure_mcp_server`
(`runner/src/curie_runner/adapter.py::McpServerReconnector.ensure_mcp_server`) so the runner
can verify and repair the session's own MCP connection before clearing a connector failure.
A harness that omits it keeps side-probe-only connector recovery. A session may also expose
the duck-typed `export_replay_state` hook. It returns `HarnessReplayState` for a full
checkpoint or delta, or `None` when there is nothing new to export
(`runner/src/curie_runner/history.py::HarnessReplayState`), and `SessionRunner` bounds the export to
`_HISTORY_REPLAY_EXPORT_BUDGET_SECONDS` (five seconds) before preserving portable replay
without the harness checkpoint (`runner/src/curie_runner/session.py::SessionRunner`).

Apart from `PartialMessageBoundary` and `StreamedToolUseBoundary`, the values a `receive_turn` iterator yields must be
mappable by `translate_message` (`runner/src/curie_runner/translate.py::translate_message`)
into the ACI outbound union (`TextDelta` / `ToolNote` / `SideEffectFlag` / `ErrorEvent` /
`Final`). `SessionRunner` consumes those internal boundaries only as telemetry evidence; they
never reach `translate_message` or the ACI stream. Today the translatable messages are
the concrete `claude_agent_sdk` dataclasses, and the neutral `TurnEvent` payload that was
to decouple the port from the SDK shape was withdrawn with #307/#315 rather than shipped.
Session options are assembled by `build_options`
(`runner/src/curie_runner/adapter.py::build_options`). Since #245 / ADR-0010 the permission
posture is conditional, not pinned: with an approval `can_use_tool` callback the session
runs in `permission_mode="default"` so each tool call is gated, and only an unconfigured
agent (no callback) keeps the historical `"bypassPermissions"` verbatim
(`runner/src/curie_runner/adapter.py::build_options`).

The package declaration above this port is `HarnessContribution`
(`runner/src/curie_runner/harness/contribution.py::HarnessContribution`). It
contains `name`, `aliases`, `readonly_tools`, `build_spawn_env`,
`compile_bundle`, and `supports_structured_replay`. Registry discovery retains
its checks for malformed paths, reserved names, duplicate keys, and keys that
are not exact strings
(`runner/src/curie_runner/harness/registry.py::discover_contributions`).

Supported boot is narrower than registry discovery. `RunnerConfig.from_env`
(`runner/src/curie_runner/config.py::RunnerConfig.from_env`) reads the internal
`CURIE_HARNESS` value. `_resolve_harness`
(`runner/src/curie_runner/__main__.py::_resolve_harness`) admits only the built
in Claude name and aliases by direct import. Every other name raises
`UnsupportedHarnessError` before discovery, even when another installed
contribution registered it. A broken sibling entry point cannot affect a
supported boot path.

## Implementations today

Two, both in `runner/src/curie_runner/`:

- **Real:** `ClaudeAgentSession` (`runner/src/curie_runner/adapter.py::ClaudeAgentSession`), wrapping `ClaudeSDKClient` in
  streaming-input mode. Its `receive_turn` normalization iterator wraps
  `self._client.receive_response()`: it converts only allowlisted partial-message starts
  into payload-free `PartialMessageBoundary` values, converts an allowlisted
  `content_block_start` tool_use into a payload-free `StreamedToolUseBoundary`,
  discards other SDK `StreamEvent` payloads, and otherwise yields the SDK messages.
  `interrupt` delegates to
  `self._client.interrupt()` (`runner/src/curie_runner/adapter.py::ClaudeAgentSession.interrupt`).
- **Fake:** `FakeModelSession` (`runner/src/curie_runner/fake.py::FakeModelSession`), a scripted
  replayer that constructs real SDK message dataclasses. It does not emit
  `StreamedToolUseBoundary`. It is the reusable acceptance
  harness: `conformance_producer` (`runner/src/curie_runner/conformance.py::conformance_producer`) drives
  a real `SessionRunner` over the fake (`runner/src/curie_runner/conformance.py::_build_runner`), so the ACI conformance gate
  validates the actual translation/final plumbing, not a canned stream.
- **Blocked preflight:** `PreflightBlockedSession`
  (`runner/src/curie_runner/harness/claude/preflight_blocked.py::PreflightBlockedSession`) stands in for
  the model session when a declared verification check is blocked (#3873). It answers
  every turn offline with one `Could not complete:` message and a zero-token result.

At the package layer there is one supported contribution, Claude
(`runner/src/curie_runner/harness/claude/__init__.py::CLAUDE_CONTRIBUTION`). It
supplies environment resolution, bundle compilation, the tool allowlist, and
structured replay support. Another registry contribution does not imply
supported engine selection.

## Known leakage

The port is CLEAN as a code interface but leaks harness shape where the SDK is not yet
walled off, called out in vision-doc Job 1:

1. **SDK message payload.** The values crossing the port remain the
   `claude_agent_sdk` message union. The runner owned `TurnEvent` model was
   withdrawn. The import ratchet records the remaining legacy SDK edges and
   prevents new exceptions outside the Claude package. It does not claim that
   those modules have already moved or that a neutral message contract exists.
2. **Plugin format entanglement, visible in the manifest.**
   `packages/plugin-format` is the Claude Code plugin shape verbatim, so another
   harness must interpret or translate those bundles. `compile_bundle` returns
   `BundleCompileResult`
   (`runner/src/curie_runner/harness/contribution.py::BundleCompileResult`),
   whose `plugins` field is typed `list[Any]`. Claude fills it with SDK
   `SdkPluginConfig` objects
   (`runner/src/curie_runner/plugin.py::load_plugins`).
3. **Replay export by attribute lookup.** `export_replay_state` is discovered
   with `getattr` in `SessionRunner`
   (`runner/src/curie_runner/session.py::SessionRunner`), unlike the declared
   `McpServerReconnector` optional protocol. A session can omit a checkpoint
   capability without a protocol conformance failure.
4. **Read-only enforcement is not a `ModelSession` operation.** A read-only turn is
   refused tool by tool only through the SDK fronts that wrap the session's callbacks,
   `front_pre_tool_use_hooks` (a `HookMatcher` front) and `front_can_use_tool`
   (`runner/src/curie_runner/tool_access.py`), plus the fake's own check against the
   shared `TurnToolAccess`. `SessionRunner.enforced_tool_access`
   (`runner/src/curie_runner/session.py::SessionRunner`) reports read-only as enforced on
   `/status` whenever the runner holds a `TurnToolAccess` and has not yet sent an
   unrestricted prompt, without asking the session. A
   second harness must itself refuse every tool outside the read-only set, or `/status`
   claims enforcement that is not happening.

## Cross links

1. [Architecture vision](../../architecture-vision.md) grades the harness
   runtime as Job 1, grade A minus. This seam folds into #25.
2. [ADR 0005](../../adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md)
   defines the SDK adapter behind the frozen ACI contract.
3. [ADR 0010](../../adr/0010-approval-gates-and-human-in-the-loop.md)
   makes permission mode conditional on a `can_use_tool` callback.
4. [ADR 0140](../../adr/0140-curie-supports-one-model-harness-until-a-second-one-exists.md)
   limits supported engines to Claude and replaces the programs in ADRs 0060,
   0061, and 0062 until a real second engine exists.
