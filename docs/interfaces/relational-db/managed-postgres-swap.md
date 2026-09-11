# Guide: swapping to managed Postgres (RDS / Cloud SQL / Neon)

> Companion to [INTERFACE.md](./INTERFACE.md). Part of the #84 relational-DB seam
> epic; validates the concrete swap tracked by #283.

Pointing Curie at a managed Postgres is a **connection-string change**, not a code
change. The SQL/ORM layer (SQLAlchemy 2.0 async + Alembic) and the `curie` schema
stay put; only the Postgres instance behind the DSN moves. This guide shows the swap
and the one validation that proves it.

## The swap

1. **Provision PostgreSQL 15 or newer** on your managed provider (RDS, Cloud SQL,
   Neon, or any standard Postgres). No extensions are required. Curie uses
   `UNIQUE NULLS NOT DISTINCT` for shared workflow-state identity, which requires
   PostgreSQL 15+; the shipped compose stack and chart use PostgreSQL 16. Curie
   confines its tables to a dedicated `curie` schema (`config.db_schema`), so it
   can share a database with Langfuse without colliding with Langfuse's
   `public`-schema Prisma baseline.

2. **Point `DATABASE_URL` at it.** Two services read it, each building its own
   engine from it: the API (`apps/api/src/curie_api/config.py`,
   `apps/api/src/curie_api/db.py`) and the worker, which reads the same database
   read-only (`apps/worker/src/curie_worker/config.py::WorkerConfig`). Set it for
   both. Use the async `asyncpg` dialect:

   ```
   DATABASE_URL=postgresql+asyncpg://<user>:<password>@<host>:5432/<database>
   ```

   - **RDS / Cloud SQL:** the endpoint host + port. On the chart path, set
     `postgres.sslMode: require` (see `charts/curie/README.md`) so every DSN
     carries a TLS parameter rather than relying on the driver default of
     `prefer`. For Cloud SQL prefer the Auth Proxy (a localhost endpoint) so the
     DSN stays a plain host/port.
   - **Neon:** use the pooled or direct endpoint host, and set
     `postgres.sslMode: require` on the chart path. That renders `?ssl=require`
     for the api and worker (SQLAlchemy + asyncpg) and
     `?sslmode=require&sslaccept=accept_invalid_certs` for Langfuse (Prisma).
     Neither suffix authenticates the server certificate, so api, worker, and
     Langfuse traffic is encrypted but not authenticated; a `verify-full` mode
     needs a mounted CA bundle and is not this knob.
   - The role must own (or be able to create) the `curie` schema. Migration
     `0001_initial` issues `CREATE SCHEMA IF NOT EXISTS curie`.

3. **Apply migrations** against the target:

   ```bash
   cd apps/api
   DATABASE_URL=<managed-dsn> uv run alembic upgrade head
   ```

   That is the whole swap. The models and the migration chain are applied verbatim.

## Why it just works (and where it would leak)

The Postgres-isms below are the things that make the "just change the DSN" story
leak, and only for a *non*-Postgres store. Every one of them is standard Postgres,
so any managed Postgres is unaffected (the full, cited list lives in
[INTERFACE.md](./INTERFACE.md#known-leakage); this is the swap-relevant summary):

1. **`postgresql.UUID` column type** — every primary/foreign key. Native `uuid` on
   any Postgres.
2. **Schema-qualified tables + a schema-scoped native enum** — the `environment`
   column is a native `CREATE TYPE ... environment` in the `curie` schema. Created
   from the same migration on any Postgres.
3. **`JSONB` columns** — on the `agents`, `approval_audit_entries` and
   `workflow_state_entries` tables (`apps/api/src/curie_api/models.py`), emitted by
   the same migration chain. Native on any Postgres.
4. **Raw `DISTINCT ON` SQL in application code** — the API's commit poller
   (`apps/api/src/curie_api/commitpoller.py::_DEPLOYED_SQL`) and the worker's
   connector reconcile loop
   (`apps/worker/src/curie_worker/connector_loop.py::_TARGETS_SQL`). The worker's
   raw-SQL reads also decode JSONB with `json.loads`, because asyncpg hands back a
   `str` for a raw-text `SELECT`. Postgres-only and asyncpg-specific, but unchanged
   by which Postgres sits behind the DSN.
5. **`UNIQUE NULLS NOT DISTINCT`** — the shared (`NULL`) scope in
   `workflow_state_entries` is one identity rather than PostgreSQL's ordinary
   many-NULLs behavior. This needs PostgreSQL 15+ on every managed target.

Cross-database portability (MySQL, etc.) is explicitly a non-goal
([ADR-0007](../../adr/0007-adopt-not-build-boundaries.md)); if a non-Postgres store
is ever demanded, extract a repository port first.

## Validation (smoke test)

`apps/api/tests/test_managed_pg_swap.py` is the executable proof of this swap. The
test suite's `migrated` fixture provisions a throwaway database reached **purely by
overriding `DATABASE_URL`** and runs `alembic upgrade head` against it — the exact
shape of pointing the app at a managed Postgres. The test then asserts that items 1
and 2 materialized as expected: native `uuid` id columns on `agents`,
`agent_versions` and `deployments`; the `environment` enum type in the `curie`
schema with the right labels; zero tables in `public`. Items 3 and 4 have no
assertion here: the ordinary API and worker suites cover JSONB and raw
`DISTINCT ON`, while the migration suite covers the singular shared-state
identity (item 5).

```bash
cd apps/api
uv run pytest tests/test_managed_pg_swap.py -q   # needs a reachable Postgres (compose is fine)
```

A green run against the compose Postgres is a green run against RDS/Cloud SQL/Neon:
same dialect, same models, same migrations — only the DSN differs.
