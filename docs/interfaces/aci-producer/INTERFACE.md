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

A second implementation is an **ACI server**, an HTTP process serving
the six POST endpoints (`runner/src/curie_runner/server.py`): `POST /v1/event` opens a
turn, `POST /v1/steer` injects into the live turn (409 if none running),
`POST /v1/interrupt` hard-stops it, `POST /v1/reset` discards the conversation so the
next turn starts fresh (409 while a turn is active), `POST /v1/snapshot` captures a
bounded managed-workspace snapshot for publication, and `POST /v1/timeout` stops the
exact open turn named by the event response epoch. The timeout is a runner-private,
authenticated control route; a server that omits the epoch response header is simply
not notified, and worker timeout classification remains unaffected. Two GETs sit alongside them and stay
unauthenticated, `GET /healthz` and `GET /status`, because the chart's readiness probe
sends no auth header. `/v1/reset` carries no ACI wire frame (it is a runner control route,
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

## Implementations today

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
