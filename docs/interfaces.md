# Curie Interface Catalog (Swappable Seams)

These files document the "strong black lines" that already exist in the code — the
seams where one curated default component can be swapped without rewriting the system.
The governing restraint is
[**"the second implementation teaches the interface"**](architecture-vision.md): we do not
write speculative adapter layers ahead of a real second implementation. Each `INTERFACE.md`
is documentation of where the code already draws the line, not a new abstraction.

## The seams

<!-- BEGIN GENERATED: seam-table (curie dev docs-lint) -->
| Seam | Kind | Impls | Grade | Epic(s) | INTERFACE.md |
|---|---|---|---|---|---|
| Substrate / SandboxClient | CLEAN | 2 (k8s, docker) | not separately graded | #86, #44 | [Substrate / SandboxClient](interfaces/substrate/INTERFACE.md) |
| Harness in-proc / ModelSession | CLEAN | 1 + fake | A- | (folds into #25) | [Harness in-proc / ModelSession](interfaces/harness-modelsession/INTERFACE.md) |
| ACI producer (frozen protocol) | CLEAN, frozen | 1 + reference | A- | #25, #47 | [ACI producer (frozen protocol)](interfaces/aci-producer/INTERFACE.md) |
| Channel / ingress | SOFT | 2 (Slack, email) | C | #7, #19, #27, #38, #1515 | [Channel / ingress](interfaces/channel-ingress/INTERFACE.md) |
| Channel interaction message | CLEAN | 2 renderers (Slack, terminal) | not separately graded | ADR-0020 | [Channel interaction message](interfaces/channel-interaction/INTERFACE.md) |
| Model provider / credentials | SOFT | 2 prefix-routed (Anthropic, OpenRouter) + base-URL-selected provider-native endpoints (Zhipu, Moonshot, DeepSeek, Ollama) | not separately graded | #24, #46 | [Model provider / credentials](interfaces/model-provider/INTERFACE.md) |
| Telemetry / OTEL | SOFT | 1 | B+ | #47 | [Telemetry / OTEL](interfaces/telemetry-otel/INTERFACE.md) |
| Evals (case + scorer) | SOFT | 2 scorers (grader family + trajectory matcher) | B | #8, #26 | [Evals (case + scorer)](interfaces/evals/INTERFACE.md) |
| Blob storage (S3/RustFS) | CLEAN | 1 backend (S3/RustFS) behind the ObjectStore port | B+ | #83 | [Blob storage (S3/RustFS)](interfaces/blob-storage/INTERFACE.md) |
| Relational DB (Postgres) | SOFT | 1 | A- | #84 | [Relational DB (Postgres)](interfaces/relational-db/INTERFACE.md) |
| Queue / stream (Valkey) | CLEAN | 1 (redis-py) behind the broker port | not separately graded | #85, #7 | [Queue / stream (Valkey)](interfaces/queue-stream/INTERFACE.md) |
| Bundle format | CLEAN, frozen | 1 | not separately graded | #30 | [Bundle format](interfaces/bundle-format/INTERFACE.md) |
| Approval / authorizer | CLEAN | 3 approver sets behind one authorizer (Slack channel, Slack user group, explicit user list) | not separately graded | #22 | [Approval / authorizer](interfaces/approval/INTERFACE.md) |
| Workflow state store | SOFT | 1 (API state router) | not separately graded | #23, #248 | [Workflow state store](interfaces/workflow-state/INTERFACE.md) |
| Memory | CLEAN | 1 loader (StateApiMemoryStore) | not separately graded | #28 | [Memory](interfaces/memory/INTERFACE.md) |
| Conversation history | CLEAN | 1 loader (StateApiTranscriptStore) | not separately graded | #20 | [Conversation history](interfaces/conversation-history/INTERFACE.md) |
| Triggers | SOFT | 3 hardcoded (Slack, GH push, commit poll) | not separately graded | #29 | [Triggers](interfaces/triggers/INTERFACE.md) |
| CLI output (agent-facing `--json`) | CLEAN | 9 outputs behind one trait | not separately graded | #456 | [CLI output (agent-facing `--json`)](interfaces/cli-output/INTERFACE.md) |
<!-- END GENERATED: seam-table -->

## Kind legend

- **CLEAN** — a real `Protocol` / typed port class draws the line in code.
- **SOFT** — the swap happens through env vars, a URL/endpoint, a key prefix, or a wire
  payload; there is no code interface, and that is a deliberate decision, not an omission.
- **NONE** — the seam is not built yet; the file records the intended line and the placement
  constraint a future implementation must honor.

Seams not in the six-job Swap-readiness grade table below (substrate, model provider, queue,
bundle format, approval, workflow-state, memory, triggers) are **not separately graded**.

## Swap-readiness grade

Reproduced verbatim from the "Swap readiness" section of
[architecture-vision.md](architecture-vision.md). Only the six production-platform jobs are
graded here.

| Job | Port contract | Current adapter | Grade | Cheapest next step |
|---|---|---|---|---|
| Harness / runtime | Frozen ACI protocol (`packages/aci-protocol`), tri-language, CI-guarded | claude-agent-sdk runner | A-: strongest seam in the system; docked for the plugin-format entanglement and SDK-shaped resume | Write the "implement an ACI server" guide from the conformance suite so the port is documented, not just enforced |
| Observability | OTLP to collector (write), API DTOs (read) | Langfuse behind `langfuse.py` | B+: write side clean but for three vendor span attributes (`langfuse.trace.name`, `langfuse.session.id`, `langfuse.user.id`); read side spans several API modules plus routers | Map the three `langfuse.*` attributes to neutral names in the collector; rename the `/langfuse/*` API routes |
| Evals | Our stream schema + `EvalMatrix` DTO; store behind recorder | Langfuse traces + `eval_pass` scores | B: schema is ours; the case format converged into one frozen, drift-gated schema (#8, ADR-0019), leaving the `version:`/`suite:` tag convention as the unfrozen part | Freeze the tag convention into the schema, or record it as a deliberate soft contract |
| Blob storage | S3 protocol (boto3 + AWS CLI, path-style, endpoint-configurable) | RustFS | B+: config-only within S3-compatible stores; the client is now built in one shared place (`packages/aci-protocol/src/aci_protocol/s3.py::build_s3_client`, #572), and the `ObjectStore` port (`apps/api/src/curie_api/storage.py::ObjectStore`) names the contract, but the second, non-S3 adapter is deferred by decision until a real demand lands (#282) | None needed until a non-S3 demand exists |
| Relational DB | SQLAlchemy 2.0 + alembic | Postgres | A-: managed-Postgres swap is a DSN change; two Postgres-isms in models | Leave as is; note the `postgresql.UUID` and schema-scoped enum as the two things a non-Postgres target would touch |
| Communication | `QueuedTurn` (channel-neutral, in `aci-protocol`) + `SlackSink` | Slack (Bolt + chat.update) | C: the ingress payload is now the channel-neutral `QueuedTurn` (#7), but egress still assumes Slack's edit-in-place `chat.update` reply shape; service swappable (CLI stub), egress protocol not | Route replies per turn (#19) and define a channel-neutral `ReplySink` post/update port so a second channel can coexist |
