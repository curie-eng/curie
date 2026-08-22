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

Inside the runner the model harness is reached through one in-process port: the
`ModelSession` Protocol. Everything above it (ACI translation, budget, side-effect
flagging, NDJSON, the HTTP layer) is written against the Protocol. The port itself is
CLEAN, but the SDK is not yet confined to one module: 10 runner modules still import
`claude_agent_sdk` today (`check.py`, `session.py`, `hooks.py`, `adapter.py`, `fake.py`,
`approval.py`, `translate.py`, `plugin.py`, `state.py`, `delegate.py`), and the value that crosses the port is
currently the raw SDK message union rather than a runner-owned neutral type. The
runner-owned `TurnEvent` model that was once going to draw that line is withdrawn, not
pending: issue #307 is closed as superseded and its PR #315 was closed unmerged (the
withdrawal of OpenCode-as-second-harness, recorded in ADR-0060/0061), and no `TurnEvent`
type exists anywhere in the tree.
ADR-0061 (Draft) replaces it with an out-of-process harness boundary, tracked by open
#844; until something lands, a second harness must emit objects the SDK-shaped
translation step accepts. What stays opinionated core is the frozen ACI wire contract the runner
serves; the port is how a harness plugs into that runner. Steer and interrupt are
first-class Protocol operations, not emulated.

## Current contract

A second harness must supply an object satisfying `ModelSession`
(`runner/src/curie_runner/adapter.py::ModelSession`), a five-method `Protocol`:

- `async def connect(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.connect`) — start/attach the harness,
  rehydrating if a resume ref is configured.
- `async def query(self, text: str) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.query`) — push a user message;
  a `query` issued while a turn is live is the mid-run **steer**.
- `def receive_turn(self) -> AsyncIterator[Any]` (`runner/src/curie_runner/adapter.py::ModelSession.receive_turn`) — yield the harness
  messages for the current turn, ending at its terminal result.
- `async def interrupt(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.interrupt`) — native hard stop at the next
  safe boundary.
- `async def close(self) -> None` (`runner/src/curie_runner/adapter.py::ModelSession.close`) — tear down.

The messages a `receive_turn` iterator yields must be mappable by
`translate_message` (`runner/src/curie_runner/translate.py::translate_message`) into the ACI
outbound union (`TextDelta` / `ToolNote` / `SideEffectFlag` / `ErrorEvent` /
`Final`). Today those messages are the concrete `claude_agent_sdk` dataclasses, and the
neutral `TurnEvent` payload that was to decouple the port from the SDK shape was
withdrawn with #307/#315 rather than shipped. Session options are assembled by `build_options`
(`runner/src/curie_runner/adapter.py::build_options`). Since #245 / ADR-0010 the permission
posture is conditional, not pinned: with an approval `can_use_tool` callback the session
runs in `permission_mode="default"` so each tool call is gated, and only an unconfigured
agent (no callback) keeps the historical `"bypassPermissions"` verbatim
(`runner/src/curie_runner/adapter.py::build_options`).

The Protocol is only half of what a second harness owes the runner. Since ADR-0060
(Accepted) a harness is also a **declared package**: it ships a `HarnessContribution`
manifest (`runner/src/curie_runner/harness/contribution.py::HarnessContribution`) naming
its image, install packages, accepted credential shapes, read-only tool set,
model-override env keys, spawn-env builder and bundle compiler, and it registers a
`get_contribution` callable under the `curie.harness` entry-point group
(`runner/src/curie_runner/harness/registry.py::ENTRY_POINT_GROUP`, declared for the
built-in in `runner/pyproject.toml`). Discovery is fail-closed: a flat module path, a
key already claimed by a built-in or by an earlier contribution, or a non-`str` key is
refused rather than resolved by scan order
(`runner/src/curie_runner/harness/registry.py::discover_contributions`). The runner
selects one at boot from `CURIE_HARNESS`
(`runner/src/curie_runner/config.py::RunnerConfig.from_env`, surfaced as
`runner/src/curie_runner/config.py::RunnerConfig.harness`) and resolves it through
`runner/src/curie_runner/__main__.py::_resolve_harness`, which short-circuits built-in
names to a direct import so a broken sibling entry point cannot brick the default, and
otherwise goes through `runner/src/curie_runner/harness/registry.py::resolve_harness`
and fails loud on an unregistered name.

## Implementations today

Two, both in `runner/src/curie_runner/`:

- **Real:** `ClaudeAgentSession` (`runner/src/curie_runner/adapter.py::ClaudeAgentSession`), wrapping `ClaudeSDKClient` in
  streaming-input mode; `receive_turn` delegates to `self._client.receive_response()`
  (`runner/src/curie_runner/adapter.py::ClaudeAgentSession.receive_turn`) and `interrupt` to `self._client.interrupt()`
  (`runner/src/curie_runner/adapter.py::ClaudeAgentSession.interrupt`).
- **Fake:** `FakeModelSession` (`runner/src/curie_runner/fake.py::FakeModelSession`), a scripted
  replayer that constructs real SDK message dataclasses. It is the reusable acceptance
  harness: `conformance_producer` (`runner/src/curie_runner/conformance.py::conformance_producer`) drives
  a real `SessionRunner` over the fake (`runner/src/curie_runner/conformance.py::_build_runner`), so the ACI conformance gate
  validates the actual translation/final plumbing, not a canned stream.

At the package layer there is exactly one registered contribution, the built-in Claude
harness (`runner/src/curie_runner/harness/claude.py::CLAUDE_CONTRIBUTION`), which
declares no new behavior and only names what `sdk_auth.py`, `side_effects.py` and
`plugin.py` already did. So the registry is a guarded indirection around this same
adapter until a second contribution exists to teach it.

## Known leakage

The port is CLEAN as a code interface but leaks harness shape where the SDK is not yet
walled off, called out in vision-doc Job 1:

- **SDK-shaped message payload.** The value crossing the port is the concrete
  `claude_agent_sdk` message union, and `claude_agent_sdk` is imported across 10
  runner modules rather than one harness package. The runner-owned `TurnEvent` model that
  was to draw the neutral line is withdrawn (issue #307 closed as superseded, its PR #315
  closed unmerged and kept only as mining material for the package-shaped redesign);
  ADR-0061 (Draft) puts the boundary out of process instead, and
  confining these imports into the Claude harness package is part of open #844. ADR-0062
  (Accepted) already gates the boundaries that are clean with import-linter contracts in
  `pyproject.toml`, but its full ban on importing the SDK outside the Claude harness
  package cannot be turned on until that refactor lands. Until then a second harness
  emits SDK-shaped dataclasses that `translate_message` understands.
- **Plugin-format entanglement, now visible in the manifest.** `packages/plugin-format`
  is the Claude Code plugin shape verbatim, so a non-Claude harness must interpret Claude
  Code plugin bundles or translate them; "implement the ACI server" understates that work.
  ADR-0060's manifest names that cost rather than removing it: `compile_bundle` returns a
  `runner/src/curie_runner/harness/contribution.py::BundleCompileResult` whose `plugins`
  field is typed `list[Any]` and, for the built-in, is filled with the SDK's own
  `SdkPluginConfig` objects (`runner/src/curie_runner/plugin.py::load_plugins`).

## Cross-links

- **Epic(s):** — no standalone epic; folds into #25 (ACI producer / second-harness work).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 1 (Harness / runtime), grade A-.
- **ADR(s):** [ADR-0005](../../adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md) — claude-agent-sdk adapter behind a frozen ACI session contract; [ADR-0010](../../adr/0010-approval-gates-and-human-in-the-loop.md) — approval gates make `permission_mode` conditional on a `can_use_tool` callback; [ADR-0060](../../adr/0060-the-harness-is-a-declared-package.md) (Accepted) — a harness is a declared package with a contribution manifest and an entry point, not a class; [ADR-0061](../../adr/0061-out-of-process-harness-boundary.md) (Draft) — the boundary moves out of process, replacing the withdrawn `TurnEvent` union; [ADR-0062](../../adr/0062-harness-conformance-has-teeth.md) (Accepted) — conformance obligations and the import-linter contracts behind them; [ADR-0011](../../adr/0011-opencode-second-harness.md) — OpenCode as the second harness, **Superseded by ADR-0060** (the steer spike passed, adoption was withdrawn for other reasons).
