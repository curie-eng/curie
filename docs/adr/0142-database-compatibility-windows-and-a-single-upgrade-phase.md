# 142. Database compatibility is a release contract; migrations run in one upgrade phase

Date: 2026-09-04

Status: Accepted

This decision was specified by a maintainer in
[#2300](https://github.com/curie-eng/curie/issues/2300) (milestone v0.9.0) and
is realized by `curie_api.schema_compat`,
`charts/curie/templates/schema-migrate.yaml`, the API `schema-wait` init, and
`curie cluster up --forward-only`.

## Context

Helm revision status does not prove that an application version can start
against the current database. API pods ran Alembic during every startup, so an
older image refused a newer revision even when the change was additive and the
old application would otherwise keep serving. Patch rollback was therefore not
a dependable recovery mechanism.

The observed v0.8.5 to v0.8.4 failure (live database at revision 0039, older
image without that script) is the v0.8.x instance of this gap. The v0.8.x
fail-closed repair is [#2296](https://github.com/curie-eng/curie/issues/2296)
and is not this decision.

## Decision

1. Every released API image declares the schema range it understands in
   machine-readable metadata (`schema_min`, `schema_head`) plus a kind for
   every Alembic revision (`expand`, `contract`, `irreversible`).
2. An upgrade planner compares the live database revision, the serving
   application, and the target application's window before mutation. Pending
   contract or irreversible revisions are refused unless the operator supplies
   the documented forward-only procedure (`--forward-only` /
   `api.migrate.forwardOnly`).
3. Migrations apply through one controlled, observable phase: compose
   `curie-migrate` locally, and a Helm `post-install,pre-upgrade` Job on
   cluster. API pods wait for a servable revision and never run Alembic.
4. Patch migrations use expand/contract sequencing. After an expand, application
   N-1 continues serving: startup allows a newer unknown revision, because the
   old image cannot declare a max it has not seen. Schema downgrade is not the
   rollback mechanism; application rollback onto a compatible expand is.
5. The durable crash-retry boundary is one Postgres transaction per Alembic
   revision (`transaction_per_migration`). A later revision that fails does not
   roll back a revision that already landed. Retry resumes from
   `curie.alembic_version`.
6. The planner records the compatibility decision and migration outcome as
   redacted JSON: revision ids, kinds, action, and reason. No connection
   strings, passwords, or row contents.

## Consequences

- A supported patch upgrade can roll the application back to N-1 against the
  same non-empty database after an expand.
- Irreversible schema change is an explicit operator choice, not a side effect
  of `helm upgrade`.
- Fresh install still applies historical irreversible revisions: there is no
  serving application to protect.
- The v0.8.x fail-closed patch remains a separate, narrower change.

## Alternatives considered

1. Keep Alembic in every API init container. Rejected because that is the
   failure mode: an older image refuses a newer additive revision.
2. Cap serving at `schema_head` as well as `schema_min`. Rejected because an
   already-released N-1 image cannot declare a future expand it does not know.
3. Downgrade the schema on application rollback. Rejected because expand
   rollback is supposed to keep the expanded schema and the previous
   application; contract/irreversible work is forward-only.
4. Wait for [#2301](https://github.com/curie-eng/curie/issues/2301) to own all
   of upgrade. Rejected because compatibility and the single migrate phase are
   load-bearing without a new transactional cluster-upgrade command.
