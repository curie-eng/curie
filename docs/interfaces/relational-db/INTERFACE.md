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
Alembic *is* the contract for the API, while the worker reads the same schema through
hand-written SQL over its own engine, so the schema itself (table and column names
included) is the real coupling. A narrower port would be extracted only if a
non-Postgres store is ever demanded.

## Current contract

A second implementation must be a Postgres speaking the async `asyncpg` dialect and
honoring the models/migrations verbatim:

- **DSN + schema** (`apps/api/src/curie_api/db.py::SCHEMA`, `apps/api/src/curie_api/db.py::create_engine`): `SCHEMA = get_settings().db_schema` (default `"curie"`, `apps/api/src/curie_api/config.py::Settings`); the engine is built from `database_url` (env `DATABASE_URL`) via `create_async_engine(..., pool_pre_ping=True)`.
- **Schema-scoped metadata** (`apps/api/src/curie_api/db.py::Base`): `Base.metadata = MetaData(schema=SCHEMA)` — every table is qualified into the `curie` schema.
- **Models** (`apps/api/src/curie_api/models.py`): `apps/api/src/curie_api/models.py::Agent` (table `agents`), `apps/api/src/curie_api/models.py::AgentVersion` (table `agent_versions`), `apps/api/src/curie_api/models.py::Deployment` (table `deployments`), `apps/api/src/curie_api/models.py::Approval` (table `approvals`), `apps/api/src/curie_api/models.py::ApprovalAuditEntry` (table `approval_audit_entries`), `apps/api/src/curie_api/models.py::WorkflowStateEntry` (table `workflow_state_entries`), and `apps/api/src/curie_api/models.py::ConsoleSession` (table `console_sessions`), plus the `apps/api/src/curie_api/models.py::Environment` and `apps/api/src/curie_api/models.py::ApprovalStatus` StrEnums. The authoritative list is the set of `Base` subclasses in that module; read it there rather than trusting a count here.
- **Two readers, one DSN** (`apps/api/src/curie_api/db.py::create_engine`, `apps/worker/src/curie_worker/run.py::build`): the API and the worker each build their own `create_async_engine(...)` from their own `database_url` setting (`apps/api/src/curie_api/config.py::Settings`, `apps/worker/src/curie_worker/config.py::WorkerConfig`), both read from `DATABASE_URL`, so a swap repoints both services. The worker never imports the API's models: it reads the schema read-only through hand-written SQL (`apps/worker/src/curie_worker/binding.py::_RESOLVE_SQL`, `apps/worker/src/curie_worker/connector_loop.py::_TARGETS_SQL`), which makes table and column names a second, ORM-independent coupling a conforming DB must honor.
- **Migrations**: the target DB must apply the **whole Alembic chain in `apps/api/alembic/versions/`**, in revision order, ending at `alembic heads`. The chain grows with the product, so it is deliberately not enumerated here: `ls apps/api/alembic/versions/` is the list, and `alembic heads` is the tip a conforming DB must reach. A single head is the invariant — a fork means two branches each added a migration (rebase and merge the heads before swapping anything).

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
3. **`JSONB` column type** — `apps/api/src/curie_api/models.py::JSONB` is imported from
   `sqlalchemy.dialects.postgresql` on the same line as `UUID` and used on **11** columns:
   `behavior_packs`, `approval_required_tools`, `approval_routes` and `secrets` on
   `apps/api/src/curie_api/models.py::Agent`, `evidence` on
   `apps/api/src/curie_api/models.py::ApprovalAuditEntry`, `value` on
   `apps/api/src/curie_api/models.py::WorkflowStateEntry`, `arguments`, `target`,
   `snapshot` and `post_state` on `apps/api/src/curie_api/models.py::AgentAction`, and
   `evidence` on `apps/api/src/curie_api/models.py::ActionAuditEntry`. The
   workflow-state one is load-bearing rather than incidental: that store exists
   precisely because Postgres JSONB meant no new datastore was needed (see that class's
   docstring). The four on `AgentAction` are the same bet made again: an action's
   arguments and the state it read are shaped by whichever tool ran, so the column that
   holds them cannot have a fixed schema.
4. **Raw dialect-specific SQL outside the ORM** — `DISTINCT ON`, which is Postgres-only,
   is written by hand in `apps/api/src/curie_api/commitpoller.py::_DEPLOYED_SQL` (executed
   through `text(...)` in `apps/api/src/curie_api/commitpoller.py::CommitPoller.poll_once`)
   and in `apps/worker/src/curie_worker/connector_loop.py::_TARGETS_SQL`. The worker's
   read path adds a driver-level dependency on top of the dialect one:
   `apps/worker/src/curie_worker/binding.py::BindingResolver.resolve` decodes the JSONB
   columns with `json.loads` because asyncpg returns JSONB as a `str` for a raw-text
   `SELECT`, and `apps/worker/src/curie_worker/binding.py::_RESOLVE_SQL` orders on
   `(d.environment = 'prod')`, comparing the native enum of item 2 against a string
   literal.

Items 1 to 3 are cheap within the Postgres family: any managed Postgres speaks all three
natively, so the DSN-only swap is unaffected by them. Item 4 is different in kind. It is
dialect-specific and driver-specific SQL living in application code, so it leaks past the
stated contract as well as past Postgres: swapping the models and migrations would not
carry it, and a reader looking only at `models.py` would not find it. All of them would
need rework for a different RDBMS, which is the marker that a real port should be
extracted first.

## Cross-links

- **Swap guide + validation:** [managed-postgres-swap.md](./managed-postgres-swap.md) — the DSN-only swap to RDS/Cloud SQL/Neon, with the `apps/api/tests/test_managed_pg_swap.py` smoke test that proves the migration chain applies against a DSN-selected throwaway database (#283). That test covers leakage items 1 and 2 only; items 3 and 4 have no equivalent assertion.
- **Epic(s):** #84 — vision epic for the relational-DB seam (keep the swap a DSN change; extract a port only for a non-Postgres store).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 5 (Relational database), grade A-
- **ADR(s):** [ADR-0007](../../adr/0007-adopt-not-build-boundaries.md) — Adopt-not-build boundaries ("vanilla Postgres" adopted for app state)
