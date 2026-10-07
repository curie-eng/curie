---
seam: Relational DB (Postgres)
kind: SOFT
impls: "1"
grade: A-
vision_row: Relational DB
epics:
  - "#84"
order: 10
---

# INTERFACE: Relational DB (Postgres)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 &nbsp;·&nbsp; **Swap-readiness grade:** A-
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

App state (agents, versions, deployments) lives in a Postgres database, reached
through SQLAlchemy 2.0 (async) with Alembic migrations. The swappable thing is the
**Postgres instance behind the DSN** — compose Postgres, RDS, Cloud SQL, any managed
Postgres — while the SQL/ORM layer and the `curie` schema stay opinionated core.
This is a deliberately un-abstracted seam: a managed-Postgres swap is a **DSN change**
(`DATABASE_URL`), not a code change. There is no repository/DAL port; SQLAlchemy 2.0 +
Alembic *is* the contract for the API, while the worker reaches the same schema through
hand-written SQL over its own engine (reads and writes), so the schema itself (table
and column names included) is the real coupling. A narrower port would be extracted
only if a non-Postgres store is ever demanded.

## Current contract

A second implementation must be PostgreSQL 15 or newer, speaking the async `asyncpg`
dialect and honoring the models/migrations verbatim. Compose and the chart ship
PostgreSQL 16:

- **DSN + schema** (`apps/api/src/curie_api/db.py::SCHEMA`, `apps/api/src/curie_api/db.py::create_engine`): `SCHEMA = get_settings().db_schema` (default `"curie"`, `apps/api/src/curie_api/config.py::Settings`); the engine is built from `database_url` (env `DATABASE_URL`) via `create_async_engine(..., pool_pre_ping=True)`.
- **Schema-scoped metadata** (`apps/api/src/curie_api/db.py::Base`): `Base.metadata = MetaData(schema=SCHEMA)` — every table is qualified into the `curie` schema.
- **Models** (`apps/api/src/curie_api/models.py`): the authoritative model set is every `apps/api/src/curie_api/db.py::Base` subclass in that module, and each one's `__tablename__` is its table. The set grows with the product, so it is deliberately not enumerated here; examples include `apps/api/src/curie_api/models.py::Agent` (table `agents`), `apps/api/src/curie_api/models.py::Approval` (table `approvals`), `apps/api/src/curie_api/models.py::WorkItem` (table `work_items`), and `apps/api/src/curie_api/models.py::ThreadTranscript` (table `thread_transcripts`). The module also defines StrEnums such as `apps/api/src/curie_api/models.py::Environment` and `apps/api/src/curie_api/models.py::ApprovalStatus`.
- **Two engines, one DSN** (`apps/api/src/curie_api/db.py::create_engine`, `apps/worker/src/curie_worker/run.py::build`): the API and the worker each build their own `create_async_engine(...)` from their own `database_url` setting (`apps/api/src/curie_api/config.py::Settings`, `apps/worker/src/curie_worker/config.py::WorkerConfig`), both read from `DATABASE_URL`, so a swap repoints both services. The worker never imports the API's models: it reaches the schema through hand-written SQL, both reading (`apps/worker/src/curie_worker/binding.py::_RESOLVE_SQL`, `apps/worker/src/curie_worker/binding.py::_UNDEPLOYED_BINDING_SQL`, `apps/worker/src/curie_worker/connector_loop.py::_TARGETS_SQL`) and writing (`apps/worker/src/curie_worker/publication_store.py::PostgresPublicationStore` updates `publications` / `approvals` under `FOR UPDATE ... SKIP LOCKED`). Table and column names are a second, ORM-independent coupling a conforming DB must honor.
- **Migrations**: the target DB must apply the **whole Alembic chain in `apps/api/alembic/versions/`**, in revision order, ending at `alembic heads`. The chain grows with the product, so it is deliberately not enumerated here: `ls apps/api/alembic/versions/` is the list, and `alembic heads` is the tip a conforming DB must reach. A single head is the invariant — a fork means two branches each added a migration (rebase and merge the heads before swapping anything). Two recent expand revisions make authenticated review feedback part of this schema contract: `0042_review_lineage_authority.py` adds immutable App-observed authority to publication lineages and the `publication_review_reservations` concurrency table; `0043_github_review_feedback.py` adds the `github_review_deliveries` audit table and the `github_review_feedback` durable feedback/outbox table. The latter stores normalized feedback and a credential-free queued turn, never a raw webhook body or GitHub credential.

The application schema window keeps minimum `0077` and advances its head to
`0082`, as recorded in `apps/api/src/curie_api/schema_compat.json`. The
v0.12.1 release raised the minimum to `0077` (`0077_agent_deploy_notifications.py`,
following hook source policy/operation expansions `0075`/`0076`, which in turn
follow polling cursor migration `0073`). Provider installations and channel
identities migration `0078` follows it, channel canvas edits migration
`0079` follows that, action executions migration `0081` follows `0079`, and
thread attachment ledger migration `0082` follows `0081`.

The `channel_canvas_edits` table (`apps/api/src/curie_api/models.py::ChannelCanvasEdit`,
migration `0079_channel_canvas_edits.py`, ADR 0200) holds one audit row per canvas cell
edit. Each row records the agent, deployment, logical turn, edit kind, channel address,
canvas id, section id, the before and after text, a status of `attempted`, `applied` or
`failed`, an error code, and timestamps, and is indexed on (`canvas_id`, `created_at`).

Migration `0081_action_executions.py` (the connector action executor contract,
#4067) is additive. It adds `post_version`, `connector`, `connector_digest`,
`authority_kind` and `authority_ref` to `agent_actions`, all nullable, so rows
written before it read back NULL and are never undoable. The `action_executions`
table (`apps/api/src/curie_api/models.py::ActionExecution`) holds one row per
restore, forward action or capability probe, with checks on `kind` and `state`,
a unique `idempotency_key`, and a partial unique index allowing one restore that
is not `refused` per recorded action. The `connector_capabilities` table
(`apps/api/src/curie_api/models.py::ConnectorCapability`) records whether a
connector image can restore, keyed on the agent, connector and digest.

Migration `0082_thread_attachment_refs.py` ([ADR 0205](../../adr/0205-an-attachment-belongs-to-its-thread.md)) is additive. The
`thread_attachment_refs` table (`apps/api/src/curie_api/models.py::ThreadAttachmentRef`)
holds one row per file a thread's agent was given, keyed like `thread_transcripts`
(agent, binding scope, thread key) and removed with the transcript. Each row
records the turn's event id, the channel's file id, the name, the on-disk name,
the best-effort mime type and size, the sha256, the arrival order (`seq`, an
identity column) and the route kind, adapter and identity; never an endpoint,
a URL or bytes. (agent, scope, thread, event, file) and (agent, scope, thread,
disk name) are each unique with a NULL scope equal, and the agent foreign key
cascades.

Migration `0087_remediation_policies.py` (automated remediation,
AUTOMATED-REMEDIATION-2) is additive. The `remediation_policies` table
(`apps/api/src/curie_api/models.py::RemediationPolicy`) holds one row per bound
hook, keyed on (agent, hook), with its current positive generation, the
operation that wrote it and the `armed` and `active` flags. The
`remediation_policy_generations` table
(`apps/api/src/curie_api/models.py::RemediationPolicyGeneration`) holds one
immutable row per generation, keyed on (agent, hook, generation), with the
operation id (unique per agent and hook), the canonical intent digest, the whole
policy document as JSONB, the flags and the operator principal that wrote it
(`bound_by`). A trigger refuses any update of a generation row and its deletion
while the agent exists; both tables cascade with the agent.

The candidate application serving window and ordered revision ancestry live in
`packages/protected-hooks/src/curie_protected_hooks/schema_serving.json`,
validated against the actual API migration graph and CLI candidate catalog.
`packages/protected-hooks/src/curie_protected_hooks/schema_serving.py::can_serve`
owns the pure serving decision; API exports delegate to it. The API window
resource and chart metadata are checked generated mirrors. Historical CLI
release windows remain separate and unchanged.

API startup calls
`packages/protected-hooks/src/curie_protected_hooks/schema_serving.py::assert_servable`
under its configured database identity. The read-only probe requires one live
revision plus readable source policy and operation columns with compatible
types, including when an unknown future revision is presumed compatible.
Configured schema selection locates version metadata; source tables remain in
`curie`. This proves readable structure, not runtime authority or qualification.

## Worker reads and writes

The worker's SQLAlchemy `text(...)` statements are a schema contract alongside
the API models. The current modules and table ownership are:

| Worker module | Reads | Writes |
|---|---|---|
| `apps/worker/src/curie_worker/binding.py` | `agents`, `agent_channels`, `deployments`, `agent_versions`, `approvals` | None |
| `apps/worker/src/curie_worker/connector_loop.py` | `agents`, `deployments`, `agent_versions` | None |
| `apps/worker/src/curie_worker/cron_loop.py` | `agents`, `deployments`, `agent_versions`, `agent_channels`, `hook_runs`, `schedule_controls` | `hook_runs`, `schedule_controls` |
| `apps/worker/src/curie_worker/hook_runs.py` | `hook_runs`, `schedule_controls` | `hook_runs` |
| `apps/worker/src/curie_worker/publication_store.py` | `publications`, `approvals`, `thread_publication_lineages`, `execution_requests`, `work_items` | `publications`, `approvals`, `thread_publication_lineages` |

`tests/test_worker_sql_contract.py` creates an isolated database, applies the
actual API Alembic chain to head, and checks every discovered worker text
statement against it through
`packages/curie-internal/src/curie_internal/worker_sql.py::discover_statements` and
`packages/curie-internal/src/curie_internal/worker_sql.py::explain_statements`.
Discovery resolves schema formatting, table attributes assigned by class
initializers, and each constant branch of local SQL fragments, including filtered
and unfiltered publication result queries. An unresolved expression is a failure
with its source location, never a skipped statement. `EXPLAIN` validates table and
column references without executing writes; the gate also proves that a missing
column is rejected and a valid parameterized statement is accepted.

The worker continues to own a separate engine and does not import API models at
runtime. Alembic migrations remain the source of table and column authority; a
test fixture that fabricates those tables cannot substitute for this contract
check.

## Implementations today

One: the compose/dev Postgres. Two SQLAlchemy async engines reach it, the API's
(`apps/api/src/curie_api/db.py::create_engine`) and the worker's
(`apps/worker/src/curie_worker/run.py::build`), each built from its own settings object
but from the same `DATABASE_URL`. Tests point the API engine at the compose Postgres by
overriding `database_url` (per the `apps/api/src/curie_api/db.py` module docstring).

## Known leakage

These Postgres-isms make the "just change the DSN" story leak for a non-Postgres store.
The list is enumerated rather than totalled in prose on purpose: what counts as one ism
is a judgement call, not something derivable from the tree.

1. **`postgresql.UUID` column type** — `apps/api/src/curie_api/models.py::UUID` is imported
   from `sqlalchemy.dialects.postgresql` and used as `UUID(as_uuid=True)` on every primary
   and foreign key (e.g. `apps/api/src/curie_api/models.py::Agent`). This is a dialect-specific type.
2. **Schema-qualified tables + a schema-scoped native enum** — foreign keys are
   written as `f"{SCHEMA}.agents.id"` (`apps/api/src/curie_api/models.py::AgentVersion`,
   `apps/api/src/curie_api/models.py::Deployment`) and the `environment`
   column is a native Postgres `Enum(Environment, name="environment", schema=SCHEMA)`
   (`apps/api/src/curie_api/models.py::Deployment`), which materializes as a `CREATE TYPE` in the `curie` schema.
3. **`JSONB` column type**: `apps/api/src/curie_api/models.py::JSONB` is imported from
   `sqlalchemy.dialects.postgresql` on the same line as `UUID` and used on many columns
   across the models. The inventory is every `JSONB` column in that module, including
   the ones whose `mapped_column(` call wraps onto a second line (for example
   `JSONB(none_as_null=True)` on `allowed_callers` and `granted_arguments`), so a
   single-line `mapped_column(JSONB` search undercounts. Several are
   load-bearing rather than incidental: the workflow-state store exists precisely because
   Postgres JSONB meant no new datastore was needed (see that class's docstring), the
   action ledger's `prior_state` holds a snapshot whose shape belongs to whatever resource
   a connector wrote to, which no column type can know in advance (ADR-0117), and
   `hook_partitions` holds per-hook delivery partitioning — hook name to the JSON Pointer
   into a delivery body naming the thing each delivery is about; NULL means one thread
   per hook (ADR-0134, Draft). `source_bindings` holds the operator-controlled
   workload-to-allowlisted-repository map for inbound hooks (#2572). The review-feedback
   outbox likewise stores a normalized `feedback` object and credential-free serialized
   `turn` as JSONB. `scopes` is `apps/api/src/curie_api/models.py::ChannelIdentity.scopes`,
   the list of provider scopes granted to one channel identity (#2909); `attributes` is
   the same class's `attributes`, provider-specific identity details that don't fit a
   fixed column, such as a Slack identity's extra token reference (ADR-0168 decision 1,
   as scoped by ADR-0193 decision 4).
4. **Raw dialect-specific SQL outside the ORM** — `DISTINCT ON`, which is Postgres-only,
   is written by hand in `apps/api/src/curie_api/commitpoller.py::_DEPLOYED_SQL` (executed
   through `text(...)` in `apps/api/src/curie_api/commitpoller.py::CommitPoller.poll_once`)
   in `apps/worker/src/curie_worker/connector_loop.py::_TARGETS_SQL` and
   `apps/worker/src/curie_worker/cron_loop.py::_TARGETS_SQL`, and in the API's
   `apps/api/src/curie_api/routers/hook_fire.py::_IN_FORCE_SQL`,
   `apps/api/src/curie_api/routers/schedules.py::_IN_FORCE_SQL` and
   `apps/api/src/curie_api/routers/schedules.py::_LATEST_SQL`. The worker's
   read path adds a driver-level dependency on top of the dialect one:
   `apps/worker/src/curie_worker/binding.py::BindingResolver.resolve` decodes the JSONB
   columns with `json.loads` because asyncpg returns JSONB as a `str` for a raw-text
   `SELECT`, and `apps/worker/src/curie_worker/binding.py::_RESOLVE_SQL` orders on
   `(d.environment = 'prod')`, comparing the native enum of item 2 against a string
   literal.
5. **`UNIQUE NULLS NOT DISTINCT` identity constraints**: three constraints use
   `postgresql_nulls_not_distinct`: `agent_channels_route_key` on
   `apps/api/src/curie_api/models.py::AgentChannel` (so route-less non-Slack rows
   collide), `uq_thread_transcripts_agent_scope_thread` on
   `apps/api/src/curie_api/models.py::ThreadTranscript`, and the workflow-state
   identity below. `workflow_state_entries` treats `binding_scope IS NULL` as the one real shared scope
   identity, rather than as an absent value. This preserves the shared state of a
   `memory=True` agent (including a legacy general-state row) and the permanently
   agent-wide `memory` and `transcript` namespaces. The named
   `uq_state_agent_scope_ns_key` constraint therefore makes
   `(agent_id, binding_scope, namespace, key)` unique with `NULLS NOT DISTINCT`;
   ordinary Postgres unique semantics would allow duplicate NULL tuples. This clause is
   PostgreSQL-specific and was added in PostgreSQL 15, which is the minimum server
   version for this relational contract.
6. **`SELECT ... FOR UPDATE ... SKIP LOCKED`** — the worker's
   `apps/worker/src/curie_worker/publication_store.py::PostgresPublicationStore`
   claims publication and approval rows under `FOR UPDATE OF p SKIP LOCKED` (and
   sibling `FOR UPDATE SKIP LOCKED` statements in the same module) so concurrent
   workers do not block on one another's in-flight claim. The API uses the same
   clause on other claim paths
   (`apps/api/src/curie_api/crud/approvals.py::claim_resume_row`,
   `apps/api/src/curie_api/resumereconciler.py::ResumeReconciler`). A second
   engine that honors the models but not this skip-locked claim would serialize
   those loops.
7. **Partial unique indexes** use the Postgres-only `postgresql_where` dialect
   argument to enforce active publication invariants. Two indexes on
   `apps/api/src/curie_api/models.py::ThreadPublicationLineage` permit only one
   open lineage for each agent conversation and repository, whether that repository
   is addressed by full name or GitHub repository id. The index on
   `apps/api/src/curie_api/models.py::PublicationReviewReservation` permits only
   one reserved review per lineage, and the index on
   `apps/api/src/curie_api/models.py::Publication` permits only one live
   publication per lineage. A target without partial unique indexes cannot express
   those database-level concurrency constraints as written.
8. **Advisory locks**: several paths serialize on Postgres advisory locks rather
   than row locks. Transaction-scoped `pg_advisory_xact_lock` is taken per agent and
   hook or schedule name in
   `apps/api/src/curie_api/routers/hook_fire.py::_LOCK_SQL`,
   `apps/api/src/curie_api/routers/schedules.py::_LOCK_SQL`,
   `apps/worker/src/curie_worker/cron_loop.py::_LOCK_SQL` and
   `apps/worker/src/curie_worker/hook_runs.py::HookRunRecorder.start_guard`; per
   agent for the namespace-count cap in
   `apps/api/src/curie_api/routers/state.py::enforce_caps`; per route pair in
   `apps/api/src/curie_api/crud/channels.py::refuse_routeless_pair_sharing`; and per GitHub
   issue in `apps/api/src/curie_api/github_factory.py::lock_issue`. The factory poll
   intake instead holds a session-scoped `pg_try_advisory_lock` on one connection
   (`apps/api/src/curie_api/factory_poll_intake.py::poll_once`). The two-argument
   int4 form and the bigint form are separate lock spaces, so a port must keep the
   key derivation as written.

Items 1 to 3 are cheap within the Postgres family: any managed Postgres speaks all three
natively, so the DSN-only swap is unaffected by them. Item 4 is different in kind: it is
dialect-specific and driver-specific SQL living in application code, so it leaks past the
stated contract as well as past Postgres; swapping the models and migrations would not
carry it, and a reader looking only at `models.py` would not find it. Item 5 is likewise
a database-level contract rather than a model type: it is native on a supported managed
Postgres, but a pre-15 server cannot represent Curie's singular NULL shared identity.
Item 6 is a row-claim concurrency primitive: native on Postgres, not on every
SQLAlchemy target. Item 7 makes publication uniqueness conditional on row status,
which likewise depends on a Postgres index feature. Item 8 is a cross-process mutex
that lives in the database session rather than in a table. All eight items would need rework for a different RDBMS, which is
the marker that a real port should be extracted first.

## Cross-links

- **Swap guide + validation:** [managed-postgres-swap.md](./managed-postgres-swap.md) — the DSN-only swap to RDS/Cloud SQL/Neon, with the `apps/api/tests/test_managed_pg_swap.py` smoke test that proves the migration chain applies against a DSN-selected throwaway database (#283). That test covers leakage items 1 and 2 only; items 3 through 7 have no equivalent managed-swap assertion.
- **Epic(s):** #84 — vision epic for the relational-DB seam (keep the swap a DSN change; extract a port only for a non-Postgres store).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 5 (Relational database), grade A-
- **ADR(s):** [ADR-0007](../../adr/0007-adopt-not-build-boundaries.md) — Adopt-not-build boundaries ("vanilla Postgres" adopted for app state)
