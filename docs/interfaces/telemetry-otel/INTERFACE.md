---
seam: Telemetry / OTEL
kind: SOFT
impls: 1
grade: B+
vision_row: Observability
epics:
  - "#47"
  - "#1817"
  - "#1818"
  - "#1819"
order: 7
---
# INTERFACE: Telemetry / OTEL

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 &nbsp;·&nbsp; **Swap-readiness grade:** B+
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

On the write side, observability is swapped at the OTLP wire, not in code. The API,
dispatcher, worker, eval worker, and runner emit OTLP traces, logs, and metrics to an
OpenTelemetry Collector; services never authenticate to or speak a storage backend
directly. Standard `OTEL_EXPORTER_OTLP_*` endpoint, protocol, and header variables select
the wire transport independently per signal. With no endpoint a process keeps its JSON
stderr diagnostics and uses no-op trace/log exporters rather than inventing a localhost
destination.

The Collector is the ownership boundary for backend authentication, retry, queueing, and
signal routing. Swapping a backend means changing a collector exporter and pipeline, not
application code. The runner's opinionated `agent.run` root with sibling
`llm.generation` and `execute_tool` intervals remains a narrower contract inside that
platform-wide path: since ADR-0076 its attributes are a closed, versioned key set rather
than an open bag of `gen_ai.*` names.

## Current contract

### Shared service signals

- `bootstrap_service_telemetry`
  (`packages/telemetry/src/curie_telemetry/bootstrap.py::bootstrap_service_telemetry`)
  installs bounded batch processors for traces and logs and a periodic metric reader.
  The exporters honor the standard per-signal-over-general precedence for OTLP
  endpoints, protocols (`grpc` or `http/protobuf`), and headers. Each signal is disabled
  when its effective endpoint is absent; shutdown flushes and closes every configured
  provider within a finite bound.
- Every service resource comes from `build_resource`
  (`packages/telemetry/src/curie_telemetry/resource.py::build_resource`) and contains
  `service.namespace`, `service.name`, `service.version`, and one anonymous stable
  `service.instance.id` for the process, plus `deployment.environment.name` when
  configured. Event, run, session, sandbox, user, agent, and deployment identifiers are
  correlation attributes, never resource attributes.
- `operation_span`
  (`packages/telemetry/src/curie_telemetry/tracing.py::operation_span`) provides explicit
  parentage and honest status for platform operations. Its caller vocabulary is closed
  to `service.name`, `operation`, `role`, `source`, `outcome`, and `retry_class`; custom
  keys export only as `curie.operation`, `curie.role`, `curie.source`, `curie.outcome`,
  and `curie.retry_class`. Event mutations are separately closed to `curie.outcome` and
  the standard `error.type`; adding an arbitrary identifier at construction or later
  fails before export. Producers inject
  and consumers extract only W3C `traceparent` through the separate Stream field or
  runner HTTP header using `inject_trace_context` and `extract_trace_context`
  (`packages/telemetry/src/curie_telemetry/context.py::inject_trace_context`). A missing
  or malformed carrier starts a safe new root rather than inheriting ambient context.
- `configure_service_logging`
  (`packages/telemetry/src/curie_telemetry/logging.py::configure_service_logging`) keeps
  redacted structured JSON on stderr and, when configured, adds correlated OTLP
  LogRecords carrying the active trace and span identifiers. Stderr retains a redacted
  traceback for diagnosis; the OTLP copy exports only the exception type, not an
  exception message or stack.
- The metric catalog in `packages/telemetry/schema/metrics.json` is the committed
  contract for operational counters, histograms, and gauges across turn, queue,
  thread-lock, sandbox, runner RPC, approval, reply, HTTP, background-loop, and eval
  work. `record_metric`
  (`packages/telemetry/src/curie_telemetry/metrics.py::record_metric`) rejects undeclared
  instruments, attribute keys, and enum values. Its allowlisted dimensions describe
  operation classes and outcomes, not event, run, session, sandbox, user, agent, or
  deployment identifiers, so the application-defined series space remains bounded.
  Standard resource identity still contributes one anonymous `service.instance.id` per
  running process. It is independent of turn input and adds at most one resource series
  per live or restarted process, never one per event, session, user, or sandbox. The
  1,000-identifier regression exercises this production resource shape as well as the
  point attributes.

### Runner generation spans

The runner uses the shared service bootstrap for OTLP logs and metrics while preserving
its specialized trace provider in `runner/src/curie_runner/otel.py`. Per ADR-0076 those
generation spans use a closed, versioned attribute schema, not an open bag of `gen_ai.*`
keys:

- **The vocabulary is closed and committed.** `SpanAttributeKey`
  (`packages/telemetry-schema/src/curie_telemetry_schema/__init__.py::SpanAttributeKey`) is the only key set the runner
  may attach: `langfuse.trace.name`, `langfuse.session.id`, `langfuse.user.id`,
  `gen_ai.approval.decision`, `gen_ai.request.model`, a bare `model`,
  `gen_ai.usage.input_tokens` / `output_tokens` / `cache_read_input_tokens` /
  `cache_creation_input_tokens`, `gen_ai.tool.name`, `gen_ai.operation.name`, plus the
  resource keys `service.name` and `schema.version`, plus the root-span correlation keys
  `curie.session_id` and `curie.sandbox_id`. Phase telemetry adds nine optional v1 keys:
  `curie.phase`, `curie.phase.start_kind`, `curie.phase.end_kind`,
  `curie.terminal.cause`, `curie.terminal.status`, `curie.generation.ttft_ms`,
  `curie.generation.round`, `curie.tool.call.index`, and `curie.tool.outcome`.
  `SPAN_ATTRIBUTE_VALUE_TYPES`
  (`runner/src/curie_runner/otel.py::SPAN_ATTRIBUTE_VALUE_TYPES`) declares each key's
  value type: the four usage counts, generation TTFT, generation round, and bounded tool
  call index are `int`; every other key is `str`.
- **The schema is versioned.** `SCHEMA_VERSION`
  (`runner/src/curie_runner/otel.py::SCHEMA_VERSION`) is `v1`, stamped on the resource as
  `schema.version`, and bumps only when a key is removed, renamed, or retyped; a new
  optional key is additive. `runner/schema/otel-attributes.schema.json` is the committed
  mirror, and `runner/tests/test_otel_schema_drift.py` fails CI when the mirror, the enum,
  the declared types, or a real run's emitted attributes disagree.
- `build_tracer_provider` (`runner/src/curie_runner/otel.py::build_tracer_provider`) takes
  `(otel, session_id, sandbox_id=None)` and resolves standard trace-specific/general OTLP
  environment first, with the typed `otel` values as fallback. It returns `None` when
  disabled or no endpoint is configured, so offline runs neither export nor fail.
  Otherwise its `Resource` uses the shared stable `curie-runner` service identity and
  `schema.version`; session and sandbox correlation are attached to the `agent.run` root
  span rather than multiplying resource identities.
- **A fail-closed validator runs ahead of the exporter.** The provider registers
  `_SchemaValidatingSpanProcessor`
  (`runner/src/curie_runner/otel.py::_SchemaValidatingSpanProcessor`) first, then a
  bounded `BatchSpanProcessor`. On each span ending the validator strips any attribute
  whose key falls outside the closed set, or whose value (or any element of a sequence
  value) still matches a redaction pattern after the `redact.py` scrub. It drops the
  offending attribute, never the span, so the exporter only ever sees schema-legal
  attributes.
- Endpoint, protocol, and headers use the same standard `OTEL_EXPORTER_OTLP_*`
  precedence as the shared bootstrap; `SessionConfig.otel` is the typed runner view of
  those variables.
- `RunTracer.run_span` (`runner/src/curie_runner/otel.py::RunTracer.run_span`) takes
  `(trace_name, model, session_id=None, user_id=None, approval_decision=None)`, opens only
  the root `agent.run` (`SpanKind.SERVER`), and yields a lazy turn-local phase manager. It
  always stamps `langfuse.trace.name` on the root, and stamps `langfuse.session.id`,
  `langfuse.user.id` and `gen_ai.approval.decision` only when the corresponding value is
  non-empty (the approval decision is present only on a turn resuming a resolved
  approval).
- **The canonical tree records real intervals.** Each observable provider wait/model
  round opens one lazy `llm.generation` direct child of `agent.run`, numbered from one
  with `curie.generation.round`; it ends at the observed SDK tool-use or result boundary.
  This is the provider wait visible through SDK events, not a claim that each interval
  maps one-to-one to an underlying provider API call. A matching tool result starts the
  next round only after all active tool waits have ended. Every `execute_tool` interval
  is also a direct child of `agent.run`, never a generation child, so provider and tool
  phases are root siblings rather than a legacy monolithic generation containing
  zero-duration tool markers.
- **Generation data belongs to its round.** The configured model, or the first non-empty
  model reported by an `AssistantMessage`, stamps `gen_ai.request.model` and the bare
  `model` once on that generation. The four usage attributes accumulate only the
  non-negative integer increments on `AssistantMessage` objects in that round;
  `ResultMessage.usage` is a whole-turn total and is never copied onto the final
  generation. When the real SDK supplies an allowlisted `message_start` or
  `content_block_start` partial boundary, the adapter strips it to payload-free evidence
  and the active generation records `curie.generation.ttft_ms` once. TTFT is omitted when
  no such boundary is available: direct compatibility setters may populate model or
  usage attributes, but never claim TTFT without that allowlisted partial boundary.
  Partial bodies, arguments, provider identifiers, and results never enter telemetry.
- **Boundary confidence is explicit.** Generation start kinds are `query_observed` or
  `tool_result_inferred`; end kinds are `tool_use_observed`, `result_observed`, or
  `terminal_inferred`. An `execute_tool` span starts when the adapter observes either a
  streamed SDK tool-use boundary or an SDK tool-use block, with
  `curie.phase.start_kind=tool_use_inferred`. A streamed boundary still produces this
  payload-free observation when no final `AssistantMessage` tool-use block arrives: tool
  inputs, result bodies, and call IDs are never recorded. The span ends on the matching
  tool result with `curie.phase.end_kind=tool_result_inferred`, or at terminal cleanup
  with `terminal_inferred`. Matching uses the SDK call ID only inside the process; the ID
  is never exported. `curie.tool.call.index` is the bounded numeric correlation value,
  while `gen_ai.tool.name` and `gen_ai.operation.name=execute_tool` describe the
  operation.
- **Terminal state is truthful and bounded.** `curie.phase` marks each interval as
  `provider_wait` or `tool_wait`, and on `agent.run` retains the last active phase.
  The root terminal mapping is closed and ordered:

  | Terminal decision | `curie.terminal.cause` / `curie.terminal.status` | OTel |
  | --- | --- | --- |
  | Runner interrupt (highest precedence) | `interrupt_requested` / `cancelled` | `OK` |
  | ACI final intentionally parked for approval | `approval_required` / `paused` | `OK` |
  | Bare SDK abort | `aborted_streaming` or `aborted_tools` / `failed` | `ERROR` |
  | Generic classified failure | `classified_failure` / `failed` | `ERROR` |
  | Completed turn | `completed` / `succeeded` | `OK` |
  | Abandoned instrumentation | `abandoned` / `abandoned` | `ERROR` |

  An `AWAITING_APPROVAL` final outranks an error-shaped SDK abort because the runner's
  gate intentionally parked that turn; it does not fabricate an operator interrupt.
  Tool results close with `curie.tool.outcome` `success` or `error`; any tool still
  active at terminal cleanup closes as `cancelled`, with its real duration preserved.

### Collector delivery

The Helm Collector receives all three OTLP signals over gRPC and HTTP. Every pipeline
orders `memory_limiter` before `batch`. The built-in Langfuse trace exporter has finite
retry and a bounded `sending_queue` backed by the `file_storage` extension; Helm mounts
that directory on a PVC by default, so queued trace batches survive a Collector pod
restart. Any operator-supplied network exporter is rejected at render time unless it
also enables finite retry and a finite file-backed queue. Logs and metrics deliberately
use explicit no-op exporters until an operator configures their backends, avoiding scope
overlap with #1765. Instrumented workloads take a single chart-owned OTLP destination:
the in-cluster collector while `otelCollector.deploy` is true, `otelCollector.endpoint`
when the operator brings an external collector, or no endpoint when telemetry is
explicitly disabled. The production credential gate refuses a missing destination.

Collector self-metrics remain enabled on the chart's internal metrics port, including
queue size/capacity and exporter accepted, sent, failed, and enqueue-failed counters used
to distinguish backlog from loss. The debug exporter is disabled by default and is an
explicit development opt-in. Queue storage uses synchronous file writes, startup and
rebound compaction, a finite queue capacity, and either the configured persistent volume
or an explicit ephemeral-storage choice.

Collector diagnostics remain JSON/stderr so exporter failure is observable through
cluster container logs even when every OTLP backend is unavailable. The production log
collection path consumes that container stream externally; the Collector deliberately
does not export its own failure logs back through its failing OTLP pipeline, which would
create a recursive failure loop.

## Implementations today

One trace backend: Langfuse, reached through the OTel Collector (which authenticates and
forwards over HTTP because Langfuse OTLP ingest is HTTP-only). Every producer knows only
OTLP. The chart injects one destination into the API, dispatcher, worker, eval worker,
and sandbox runner: the in-cluster collector, a chart-owned external endpoint, or nothing
when telemetry is explicitly disabled. The local Compose profiles do the same for the
services they start. The read side (trace list and tree reconstruction) remains a
separate API concern.

The default chart intentionally has no log or metric storage implementation: those
pipelines terminate in named no-op exporters unless the operator adds durable backends.
This keeps signal production and Collector reliability available without claiming the
installable observability stack tracked by #1765.

## Known leakage

Three vendor-named attributes are set at the source on the root span rather than mapped in
the collector. `RunTracer.run_span` stamps `langfuse.trace.name`, `langfuse.session.id`,
and `langfuse.user.id` (`runner/src/curie_runner/otel.py::RunTracer.run_span`) so Langfuse
maps them to its name/Sessions/Users features. The collector trace pipeline is
receivers/`memory_limiter`/`batch`/`otlphttp` with no attributes processor
(`charts/curie/templates/_helpers.tpl`), so nothing downstream renames them. A clean seam
would emit neutral attributes the collector maps to the vendor names; today all three
vendor names are set at the source, and they are members of the closed schema, so a second
backend inherits them.

A fourth, subtler one: `record_model` stamps the bare `model` key alongside the
semantic-convention `gen_ai.request.model`
(`runner/src/curie_runner/otel.py::_GenerationSpan.record_model`). `model` is not a
`gen_ai` semantic-convention key; it exists because the generation span has to carry a
model attribute the backend recognizes for the span to ingest as a generation rather than
an untyped span, and the Langfuse read-side integration test seeds both spellings
(`apps/api/tests/test_langfuse_integration.py`). A second backend inherits the duplicate.

The read side duplicates two of the keys as its own string literals rather than importing
the closed enum: `_SANDBOX_ATTR` (`apps/api/src/curie_api/langfuse.py::_SANDBOX_ATTR`) and
`_APPROVAL_DECISION_ATTR`
(`apps/api/src/curie_api/langfuse.py::_APPROVAL_DECISION_ATTR`), read by
`hoist_sandbox_id` and `hoist_approval_decision`
(`apps/api/src/curie_api/langfuse.py::hoist_approval_decision`). The drift gate covers
only the runner's enum against its committed mirror, so a rename on the producer side
passes CI while silently breaking these two readers. That is the concrete leak a second
implementation trips over: the schema is closed for the writer and open-coded for the
reader.

## Cross-links

- **Epic(s):** #47 — harness-neutral generation telemetry. #1817, #1818, and
  #1819 extend the write path across platform traces/logs, bounded operational metrics,
  and durable/self-observable Collector delivery without adding the backends tracked by
  #1765.
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 2 (Observability / OTel store), grade B+.
- **ADR(s):** [ADR-0004](../../adr/0004-langfuse-observability-and-eval-backbone.md) — Langfuse as the single observability + eval backbone (OTLP over HTTP/protobuf to the collector).
  [ADR-0076](../../adr/0076-closed-typed-telemetry-attribute-schema.md) (Accepted, epic #512) — the closed, versioned attribute schema, the export-time validator, and the `gen_ai.approval.decision` key that closed the approval-gate observability gap ADR-0038 left open.
