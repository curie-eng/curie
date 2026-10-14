# Source administration route exposure plan

Slice A2 of [#3603](https://github.com/curie-eng/curie/issues/3603) under the
[parent plan](2026-10-02-protected-hooks.md) and the
[source control plan](2026-10-03-source-control.md). Its contract is the
[administrative route exposure](../specs/2026-10-02-protected-hook-source-policy.md#administrative-route-exposure)
amendment to SOURCE-3, SOURCE-6, SOURCE-7, SOURCE-9 and SOURCE-10, with the
matching LANE-3 sentences in the [lane contract](../specs/2026-10-02-protected-hook-lane.md).
It realizes accepted [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md)
authority and adds no ADR. It targets main from origin/main `bcb2d3161`.

**Goal:** serve the five source administration routes and compose the existing
coordinator with a provisioner supplied writer and reader, so that durable
registration, SQL commit, broker reservation and revocation, and ordinary
tombstone publication run for real. A protected PUT or rotate commits and then
answers 503 `source_publication_deferred` with its committed generation; GET
reports closed; the secret route refuses. Protected publication, its evidence
check, the shared probe evaluation, tombstone and protected ingress admission
and the secret's active path belong to the LANE-4 ingress admission change
tracked with [#4075](https://github.com/curie-eng/curie/issues/4075).

## Ownership and order

Commit the amendment and this plan first, alone. For each task a test author
writes behavioral tests from the contract without implementing it, observes
them fail against the unchanged implementation and commits them alone. A
separate implementer follows in a later commit. Every new test and unit cites
its SOURCE or LANE ID. Independent specification, security and quality reviews
gate each task before its dependents start. One owner edits each file: the
API source owner alone edits the coordinator and admin service modules and
their existing tests, and the integration owner alone edits the new router,
the app composition, the OpenAPI artifact and the operator note.

Files owned by other active work stay untouched:
`apps/api/src/curie_api/auth.py` (open console login and desktop changes) and
`apps/api/src/curie_api/routers/agents.py`, which the routes avoid by using a
new router module. `apps/api/openapi.json` is shared generated output; it is
regenerated only through its generator after rebasing on the current base,
never hand merged. Also leave `apps/api/src/curie_api/routers/hooks.py`,
`apps/api/src/curie_api/hook_source_auth.py`, the worker, kernel, consumer,
thread locks, markers, ACI, plugin format, charts and the ACL recipes in
`packages/protected-hooks/src/curie_protected_hooks/broker_metadata.py`
unchanged. `apps/api/src/curie_api/protected_support.py` changes only its
loader import.

## Tasks

| Task | Owner | Criteria | Depends on |
| --- | --- | --- | --- |
| 1. Runtime file loader module | API bootstrap owner | SOURCE-6, SOURCE-9, SOURCE-10 | Spec commit |
| 2. Writer transport | Protected hooks package owner | SOURCE-6, SOURCE-7, LANE-3 | Spec commit |
| 3. Resolver and coordinator changes | API source owner | SOURCE-3, SOURCE-6, SOURCE-7, SOURCE-10 | Tasks 1, 2 |
| 4. Activation read and secret refusal | API source owner | SOURCE-3, SOURCE-6 | Task 1 |
| 5. Routes, error body, OpenAPI, operator note | Integration owner | SOURCE-3, SOURCE-10 | Tasks 3, 4 |
| 6. Installed integration and ladder | Integration owner | All of the above | Task 5 |

Tasks 1 and 2 may run in parallel on independent files and broker resources.
Task 4 can seed broker records directly, as the probe broker tests do, so it
need not wait for task 3; it runs after task 3 only where both edit the admin
service, under the same owner.

**Task 1.** Create one API module that owns loading the runtime directory:
the existing `bootstrap.json`, `manifest.json` and `ca.pem` grammar moved
verbatim from the probe module, plus `source_writer.json` into a new redacting
`SourceWriterCredential`. It imports no source service or probe module. The
probe module imports its loader from it, which is the only edit there; the
admin and coordinator modules import it too, closing no cycle. Record the move
against [#4076](https://github.com/curie-eng/curie/issues/4076), which later
moves the grammar into the shared package. Failing first tests: extra or
missing member, other version, number or duplicate member, empty fields,
`default` username, username equal to the control reader, oversize, FIFO and
directory all refuse; a valid file parses and its representation redacts; an
import test proves the module graph has no cycle. A FIFO or unreadable writer
file leaves probe, GET and secret answers unchanged, proving they never open
it. The existing `apps/api/tests/test_hook_source_support_broker.py` suite
passes unchanged.

**Task 2.** Add `AuthenticatedSourceWriter` beside the reader in
`packages/protected-hooks/src/curie_protected_hooks/broker_transport.py`,
exporting only `reserve_and_revoke`, `publish_ordinary` and `close` over the
existing `packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence`
scripts. It is a sibling class, not a flag on the reader, and honors
`metadata_reader_budget` with the same watchdog. Failing first tests on the
disposable TLS broker with a `source_writer` principal: connect succeeds
without INFO, which proves none is sent because the role refuses INFO;
reserve, ordinary publish and idempotent republish succeed; floor or operation
mismatch refuses; wrong CA, hostname and pin refuse before any authentication;
failed named authentication never tries `default`; a killed connection is
never reopened; an expired budget refuses the next call; the writer cannot
read or write control keys and the reader cannot reserve or publish;
representations and errors contain no credential, endpoint or certificate
bytes.

**Task 3.** Implement the `SourceAuthorityResolver` for
`apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator`
from the runtime files the route loaded once, open reader then writer under
the gate deadline, supply `read_reconciled_floor`, and bracket each writer
effect with reader confirmations. Coordinator changes, all by this owner:
the protected branch answers `source_publication_deferred` with the committed
generation; rotate applies the reference check to the current row before
registration; DELETE of an absent row without history is 409
`source_not_configured` after the agent lookup, while pending history still
tombstones; tombstone publication opens fresh connections under its own
deadline and reports `source_reservation_lost`; any broker identity change is
`broker_unavailable`; the gate waits for an in flight writer call. Rewrite,
not delete, the four tests the spec lists as starting from an absent row
without history so that each starts from pending history or a committed row
and keeps its mechanic. Failing first tests, all on real stores: protected PUT
and rotate commit with exact SQL, ledger, counter and revoked broker state,
then 503 with the committed generation and no active record; DELETE from a
committed row and from pending history publishes the tombstone and leaves the
counter unchanged; a fault at every boundary (before reserve, after reserve
before commit, after commit before tombstone publish, lost publish response);
exact replay with stale CAS; historical, pending and changed intent conflicts
with no broker call; rotate whose row references no longer match the manifest
is 422 with no history; lost tombstone reservation by owned key deletion and
by an owned restored snapshot; owned broker restart between writer effect and
confirmation is `broker_unavailable`; concurrent coordinators on one agent; a
delayed tombstone publisher losing to a later reservation; budget exhaustion
during registration consuming the pending generation; cancellation during a
reserve releasing the gate only after the call ends; and the documented gate
pool bound, two mutations against a paused owned broker plus waiters, with
ingress for another agent observed waiting no longer than that bound.

**Task 4.** Complete `SourceAdminService` GET activation and the secret
refusal per SOURCE-3. Failing first tests, with a valid provisioned runtime
directory so absence of broker use is meaningful: each tombstone closed reason
including the fingerprint failure, and an active tombstone with null reason;
never configured, pending history and protected rows open no broker
connection, counted from the broker's client sessions; protected rows report
`publication_deferred`; secret is 409 for absent, history only and tombstone
and 503 `source_publication_deferred` for protected, with `no-store` on every
handler response and no source key in any response, error or captured log.
Update `apps/api/tests/test_hook_source_admin.py::test_read_reports_the_route_dto_with_locked_counter`
for the new protected reason; keep its unprovisioned cases.

**Task 5.** Serve the routes in a new router module under the `/agents`
prefix with the existing `require_api_key` dependency and no request session,
and include it in the app. Map every `SourceAdminError` to the specified
detail body; request shape errors keep FastAPI's validation list. Regenerate
OpenAPI with `uv run python -m curie_api.export_openapi`. Add an operator note
to `docs/interfaces/triggers/INTERFACE.md` covering the deferred protected
commit and its legacy counter rotation, reissuing ordinary keys through the
legacy hook secret route, tombstones staying refused until the LANE-4 change,
and the post LANE-4 publication path. Failing first tests over HTTP: platform
key, live console session with valid origin, and refusal of a missing key, a
wrong key, a stale session, a cross origin session and a hook signature; 401
and 422 before any source database read; the SOURCE-3 mutation order including
`runtime_unavailable` before any SQL read; protected PUT, rotate, replay and
DELETE through HTTP with the specified bodies; DELETE of an absent row without
history is 409 and with pending history is 200.

**Task 6.** Run the full affected suites, the full isolated Python baseline,
`uv run pytest apps/api/tests/test_openapi_drift.py -q`, Ruff, mypy, import
boundary and documentation checks, then the tier evidence below on the final
candidate.

## Real backing store strategy

Postgres is real and owned per test through `isolated_migration_db` at head,
as the existing source tests use it. The broker is the owned disposable TLS
Valkey from `packages/protected-hooks/tests/admission_broker.py`, with its
default user disabled, exact container identity and label cleanup, and
ephemeral fixture CA and server certificates. Provision two distinct named
principals with `metadata_acl_rules("source_writer")` and
`metadata_acl_rules("control_reader")`. A third fixture administrator seeds
records, injects faults and inspects state; it never appears in the runtime
directory. Write the runtime directory privately with mode 0700 per test.

Inject faults only through real store operations on owned resources: ACL
changes on owned users, client kills, client pause, key deletion or restore and
owned container restart for the broker; a transparent relay for commit
response loss, following `apps/api/tests/test_hook_source_mutation_commit_loss.py`;
backend termination on the owned database. Product code gains no failure
injection parameter. The existing fake external authority stays only for the
mechanics tests it already covers. Fixtures redact their own diagnostics and
publish only anonymous values.

## Tier classification

| Tier | Classification | Reason or command |
| --- | --- | --- |
| skill | not applicable, all tasks | No runner loop, ACI event, bundle or skill packaging changes. |
| local | required, tasks 5 and 6; tasks 1 to 4 reach it only through task 5 | `curie local up --build`, then `CURIE_E2E_TIERS=local curie dev e2e-ladder`, plus the five routes against that stack: closed GET, unprovisioned mutation 503 with no history, absent DELETE 409, secret 409, console session and platform key both accepted, and the probe unchanged. |
| local-release | not applicable | No migration, version pin, release compose or image identity change; the image reads one more provisioner file at runtime. Promote if the path guard maps a changed file. |
| cluster | not applicable | No chart template, RBAC, secret mount or NetworkPolicy; mounting the runtime directory is LANE-8 and #4076. |
| live provider | not applicable | No model routing, provider credential, MCP, workspace or coding tool path. |
| external integration | not applicable while the signed ingress router and its authentication module stay untouched | The probe module changes only its loader import. Promote and drive a real signed delivery and support probe against the candidate if the path guard maps any changed file to it. |
| factory | not applicable | No factory runtime, CI, progress, publication or work item execution change. |

Reservation and tombstone publication against a provisioned broker on the
local stack need an out of band provisioner that does not exist yet. The real
store suites above prove them; the PR records
`Discovery waiver: no provisioner can supply a protected runtime to the local stack #4076`
and links #3603.

## Prior intent to preserve

* `ce20ade6a` ordered replay and history checks before stale CAS, registration
  commit before reservation, one authoritative commit, gate release before
  publication, and protected publication refusal with the committed
  generation. Change only the codes, absent removal without history,
  tombstone confirmation and fresh publication connections.
* `842dfa773` closed administration boundaries: fresh locked counter in GET,
  gate before work connection, uniform 503 on SQL or gate failure, closed
  protected activation and secret refusal for protected rows.
* `72b2ae23a`, `36dadce65`, `01662c231` and `dab629ff6` built the probe and its
  four slot executor and budgets. Its loader moves verbatim; its answers and
  executor do not change; the administrative executor is separate.
* `fc2eca6ac` authenticated the reader and `84b7463ce` fixed the role recipes.
  Neither changes; the writer is a sibling transport.
* The legacy secret route
  `apps/api/src/curie_api/routers/agents.py::get_hook_secret`, the delivery
  signing bytes and the cron durable claim order from the parent plan stay
  untouched.
* Delivery ingress refusal of every configured hook stays exactly as is until
  LANE-4.

## Review risks

* The writer cannot read run_id. Safety rests on SQL ledger allocation above
  every attempt, CAS on the committed operation and reader confirmation after
  each effect. A reservation can land on a restarted broker before the
  confirmation notices. Review that argument against SOURCE-7.
* Gate pool exhaustion: two slow mutations plus two lock waiters can stall
  gated ingress for every agent for the gate deadline plus SQL time. The spec
  accepts and the tests measure that bound.
* A deferred protected commit bumps the agent counter and invalidates every
  ordinary hook key of that agent, by SOURCE-5 design, while the source stays
  closed. The distinct 503 code and the operator note carry that signal until
  the CLI and console siblings exist.
* A tombstone does not restore ordinary delivery until tombstone ingress
  admission lands with the LANE-4 change.
* Reference checks refuse an exact replay after a manifest change with 422.
* Secret handling: the route refuses every state here, but its handler
  responses still carry `no-store` and no source key reaches logs, errors or
  OpenAPI examples.
* The writer file shares the runtime directory; mounting it outside the API is
  a LANE-8 guard concern.

## Review dispositions

| Finding | Disposition |
| --- | --- |
| H-1 runtime and reference rule | One rule: any foreign runtime or unknown reference is 422 before any broker call, ledger registration or SQL write, in the stated mutation order; base SOURCE-10 says the same. DELETE carries no reference. |
| H-2 unreachable identity reason | Writer path reports a detected identity change as `broker_unavailable`; probe reasons unchanged. |
| H-3 pending history exit | DELETE with only pending history commits a tombstone through SOURCE-10 without rotating the legacy counter; absent without history stays 409. |
| M-1 gate pool bound | True bound stated and measured in task 3. |
| M-2 budgets | Writer honors the watchdog; gate deadline covers broker calls only; publication uses fresh connections and its own deadline; slots release on thread completion. |
| M-3 replay versus fresh | Uniform 422 for replays too, so no replay signal is needed; rotate applies the check to the current row. |
| M-4 committed but unpublished | Distinct `source_publication_deferred` and GET `publication_deferred`, operator note in task 5, post LANE-4 publication path and tombstone ingress owner named. |
| M-5 authentication | Kept `require_api_key` for console parity; claims reworded; cookie and origin cases in task 5. |
| M-6 absent removal tests and ownership | Four tests listed and rewritten by the coordinator owner in task 3. |
| M-7 import cycle | One loader module importing no source or probe module; probe import changed only; recorded against #4076. |
| M-8 SOURCE-9 directory | SOURCE-9 now lists the writer file and says the probe reads only three. |
| L-1 stale commit | Replaced by the file and symbol. |
| L-2 waiver issue | Waiver names #4076. |
| L-3 DELETE 409 order | After the agent lookup, before CAS. |
| L-4 precedence | Unset setting, slot, files, references, gate, in that order; 422 means request shape and reference only. |
| L-5 no-store scope | Scoped to handler responses; 401 and 422 carry no source data and are tested. |
| L-6 vacuous negative | Task 4 requires a valid provisioned directory. |
| L-7 DTO details | Null reason when active; FastAPI list for shape errors; new `SourceWriterCredential`. |

## Completion

Protected publication waits for the LANE-4 ingress admission change by design,
so no protected source becomes active from this slice. Do not close #3603,
#4075, #4076, #4053 or #4054. Name #4053 and #4054 as the CLI and console
siblings in the PR body under the parity seam rule. No provisioning,
deployment, downstream import or external message is part of this plan.
