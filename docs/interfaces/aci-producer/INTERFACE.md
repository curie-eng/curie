---
seam: ACI producer (frozen protocol)
kind: CLEAN, frozen
impls: 1 + reference
grade: A-
vision_row: Harness / runtime
epics:
  - "#25"
  - "#47"
order: 3
---

# INTERFACE: ACI producer (frozen protocol)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN, frozen &nbsp;·&nbsp; **Implementations today:** 1 + reference &nbsp;·&nbsp; **Swap-readiness grade:** A-
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The frozen, cross-process ACI (Agent Container Interface) protocol — the strongest seam in the
system. It makes the whole **harness** swappable: anything inside the sandbox that speaks this
wire contract (session setup env + NDJSON event union + steer/interrupt endpoints) can replace the
default claude-agent-sdk runner without the worker, CLI, or UI changing. What stays opinionated
core is the protocol itself: the event shapes, the `CURIE_*` env contract, and the
compatibility-checked `version` gate. A second harness produces the same bytes; it does not get to
redefine them.

## Current contract

A second implementation is an **ACI server**, an HTTP process serving eight required authenticated
control routes (seven POSTs and `GET /v1/status`) plus one optional executor route. The eight POST endpoints (`runner/src/curie_runner/server.py`, seven every
server serves plus one optional executor route): `POST /v1/event` opens a
turn, `POST /v1/steer` injects into the live turn (409 if none running),
`POST /v1/interrupt` hard-stops it, `POST /v1/reset` discards the conversation so the
next turn starts fresh (409 while a turn is active), `POST /v1/snapshot` captures a
bounded managed-workspace snapshot for publication, `POST /v1/timeout` stops the
exact open turn named by the event response epoch, and `POST /v1/turn-admit` grants or denies
the exact waiting turn epoch; only in executor mode, the optional `POST /v1/execute` runs one
connector action for the worker's executor. Two GETs sit alongside them and stay
unauthenticated, `GET /healthz` and `GET /status`, because the chart's readiness probe
sends no auth header. The eighth authenticated control route, `GET /v1/status`, returns
the credential-free boot attestation (`session_id`, `sandbox_id`, `managed_workspace`,
`cwd`) plus `history_durable` for the worker's replacement-authority check. `POST /v1/execute` is an optional, runner-private,
bearer-authenticated route served only by a runner booted with `CURIE_RUNNER_MODE=execute`
(ACTION-EXECUTOR-6, -24): it runs one connector action's `list`, `observe` and `call` phases
for the worker's executor, carries no ACI frame (like `/v1/turn-admit`) and changes no frozen
contract (ACTION-EXECUTOR-25). In that mode the runner serves only `/healthz`, `/status`,
`/v1/status` and `/v1/execute`, and answers every other control route `409` naming the mode.
`run_conformance` does not cover it, and a server without it answers `404`, which the worker
maps to `runner_unavailable`. The timeout
and `/v1/status` are runner-private, bearer-authenticated control routes. A server that
omits the epoch response header is simply not notified, and worker timeout classification
remains unaffected. `/v1/reset` carries no ACI wire frame (it is a runner control route,
like the GETs, and takes no body), but it is not optional: it is the per-case isolation
guarantee the worker's eval driver depends on (#550,
`apps/worker/src/curie_worker/eval/runner.py::EvalRunner._isolate` over
`apps/worker/src/curie_worker/runner_client.py::RunnerClient.reset`), and the CLI's eval
path calls the same route (`cli/src/runner.rs`). A second server that omits it fails every
eval case that has not opted into `shared_history`. It streams the outbound
NDJSON discriminated union `OutboundEvent` (`packages/aci-protocol/src/aci_protocol/events.py::OutboundEvent`):
`TextDelta` (`packages/aci-protocol/src/aci_protocol/events.py::TextDelta`, `text_delta`),
`ToolNote` (`packages/aci-protocol/src/aci_protocol/events.py::ToolNote`, `tool_note`),
`Final` (`packages/aci-protocol/src/aci_protocol/events.py::Final`, `final` + `status`),
`ErrorEvent` (`packages/aci-protocol/src/aci_protocol/events.py::ErrorEvent`, `error` + `classification`),
`SideEffectFlag` (`packages/aci-protocol/src/aci_protocol/events.py::SideEffectFlag`, `side_effect_flag`). Every
outbound event carries `version` equal to the producer's exact build `PROTOCOL_VERSION`; a consumer
accepts any `major.minor`-compatible version under 0.x (`major` after 1.0). Inbound frames are the
`InboundMessage` union (`packages/aci-protocol/src/aci_protocol/events.py::InboundMessage`) of
`Event` (`packages/aci-protocol/src/aci_protocol/events.py::Event`) and
`Interrupt` (`packages/aci-protocol/src/aci_protocol/events.py::Interrupt`) on the `kind` tag.
Setup is read from the environment via `SessionConfig.from_env`
(`packages/aci-protocol/src/aci_protocol/session.py::SessionConfig.from_env`), honoring the
`CURIE_*` mapping in `to_env` (`packages/aci-protocol/src/aci_protocol/session.py::SessionConfig.to_env`).
Conformance is proven by `run_conformance(<your producer>)`, which must return `passed=True`.

### Per-turn tool access (TOOL-ACCESS)

A turn producer, the component that enqueues a `QueuedTurn`, can restrict what
that one turn may execute, for example a synthetic availability probe that must
never act. The restriction travels with the turn, not with the sandbox, so
ordinary turns on the same install are untouched.

- **TOOL-ACCESS-1:** `QueuedTurn` and `Event` each carry an optional
  `tool_access`, a `ToolAccess` string or null, defaulting to null. The one
  value is `read-only`. An absent or null value means the turn runs exactly as
  it did before the field existed: no tool is added, removed, denied or gated
  differently, and approvals behave as before.
- **TOOL-ACCESS-2:** `tool_access` is an enum, not free text, and its spelling is
  exact. A consumer decoding the wire rejects an unknown value; it never reads
  one as null. A value added later is therefore a breaking change under the
  change-class table in `packages/CLAUDE.md`, decided on its own.
- **TOOL-ACCESS-3:** On a server that advertises it (TOOL-ACCESS-4),
  `read-only` means that, for the whole turn, including any steer delivered
  into it, only tools the server explicitly classifies as read-only may
  execute. Every other tool, including one the server has no
  classification for, is denied before it executes, and the model is told it
  was denied. The turn never requests an approval and never ends
  `awaiting-approval`: a tool that needs approval is denied even when it is
  classified read-only, and the server's own approval request tool (for the
  reference runner, `mcp__curie__request_approval`) is denied too.
- **TOOL-ACCESS-4:** An ACI server that enforces tool access advertises the
  values it enforces as a JSON list under the key `tool_access` on
  `GET /status` and `GET /v1/status`. A server that omits the key enforces
  none. A consumer must not deliver an `Event` carrying a `tool_access` value
  that the same server it is about to send it to does not advertise, because a
  server that does not enforce the field ignores it and would run the turn
  unrestricted. It refuses that turn instead.
- **TOOL-ACCESS-5:** On a server that advertises a tool access, `POST /v1/steer`
  joins the live turn only when the steer frame's `tool_access` equals the
  live turn's. Otherwise it answers `409`, so
  the caller opens its own turn and neither message runs under the other's
  access.
- **TOOL-ACCESS-6:** A worker that implements this contract forwards
  `QueuedTurn.tool_access` as `Event.tool_access` under TOOL-ACCESS-4, never
  steers a restricted turn into a live turn, and never creates an approval
  from a `read-only` turn: an `awaiting-approval` ending on one is a failed
  turn. A worker that does not implement it ignores or drops the field,
  whatever its protocol version, so a turn producer sets `tool_access` only
  toward workers known to implement TOOL-ACCESS-6, and ones whose runners
  advertise it under TOOL-ACCESS-4.
- **TOOL-ACCESS-7:** A consumer that compares a turn it received with a copy it
  stored earlier compares the decoded models, each read tolerantly, never the
  raw JSON, so a turn stored before `tool_access` existed still matches the
  same turn decoded after it. The comparison covers only the fields the
  comparing consumer models, so a field that grants authority must be modelled
  by that consumer before any worker acts on it.

### Per-turn memory credential (MEMORY-TOKEN)

The worker mints a short-lived write credential for each turn and carries it to
the runner on the `Event`, so the runner's memory tools can present it. The
credential stays out of the sandbox env, `BootEnv`, and hook or subprocess
input, but the ACI server that receives the `Event` runs inside the sandbox, so
it does reach the in-sandbox runner process (ADR-0188 decision 5).

- **MEMORY-TOKEN-1:** `Event` carries an optional `memory_token`, a string or
  null, defaulting to null. Null means the turn carries no memory write
  credential. It is never a `BootEnv` key or an env var.
- **MEMORY-TOKEN-2:** A producer sends `memory_token` only on the runner POST,
  `POST /v1/event` and `POST /v1/steer`.
- **MEMORY-TOKEN-3:** A consumer must not place `memory_token` in env, logs,
  hook or subprocess input, or persisted state. The reference model declares it
  with `repr=False`, so `repr(event)` and a log's `%r` never show it.

## Implementations today

The reference runner enforces and advertises `read-only` (RUNNER-TOOL-ACCESS
in [`runner/README.md`](../../../runner/README.md)), and the worker forwards
`QueuedTurn.tool_access` only to a runner that advertises it (WORKER-TOOL-ACCESS
in [`apps/worker/README.md`](../../../apps/worker/README.md)).

One producer (the runner, `runner/src/curie_runner/adapter.py`, a `ModelSession` wrapping
`ClaudeSDKClient`) plus the in-library `reference_producer` used by the conformance suite. The
contract is tri-language: Pydantic source of truth, committed JSON Schema in
`packages/aci-protocol/schema/`, and generated TS/Rust in `packages/aci-protocol/generated/`,
CI-guarded by `packages/aci-protocol/tests/test_schema_compat.py`.

## Known leakage

Plugin-format entanglement: the ACI server must interpret Claude Code plugin bundles mounted at
`CURIE_PLUGIN_DIR` (see the [bundle-format seam](../bundle-format/INTERFACE.md)), so a genuinely
foreign harness inherits that shape too — the A- is docked for exactly this. Rehydration is no
longer a second dock: ADR-0119 keeps the durable contract provider-neutral as ordered role/content
messages from the state store named by `CURIE_HISTORY_REF`. Each harness must materialize that
prefix structurally (the Claude adapter may consume its optional opaque native checkpoint, or
rebuild an ephemeral SDK resume envelope from portable messages), or declare
the capability absent and fail the resume; rendered system-prompt fallback is forbidden.
Otherwise the line is clean and frozen: a producer constructs strictly (stray
keys are rejected at construction), while a consumer tolerates unknown fields and rejects only an
**incompatible** wire version, raising `ProtocolVersionError` naming both versions (see ADR-0036).

## Cross-links

- **Guide:** [implementing-an-aci-server.md](./implementing-an-aci-server.md) — stand up a conformant second ACI server, driven from the conformance suite (#256).
- **Epic(s):** [#25](https://github.com/curie-eng/curie/issues/25) — write the "implement an ACI server" guide from the conformance suite so the port is documented, not just enforced
- **Epic(s):** [#47](https://github.com/curie-eng/curie/issues/47) — telemetry as part of the ACI (OTEL carried in `SessionConfig`)
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 1 (Harness / runtime), grade A-
- **ADR(s):** [ADR-0005](../../adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md) — claude-agent-sdk adapter behind a frozen ACI session contract
