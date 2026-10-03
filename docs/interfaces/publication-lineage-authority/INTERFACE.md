---
seam: Publication lineage authority
kind: CLEAN
impls: 1 (API authority through PublicationLineageClient)
grade: not separately graded
vision_row: null
epics: ["#2274", "#3923"]
order: 23
---

# INTERFACE: Publication lineage authority

> Part of the Curie swappable seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 (API authority through PublicationLineageClient) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol` or typed port class. SOFT = swap through config or wire without a code interface. NONE = not built yet.

## The black line

The publication reconciler asks an authority to accept the exact outcome of one
approved publication. It does not write the published lineage head directly.
`PublicationLineageAuthority`
(`apps/worker/src/curie_worker/publication_loop.py::PublicationLineageAuthority`)
draws that typed boundary. The current implementation sends the outcome to the
API, which verifies current GitHub identity and atomically advances the lineage
with the publication outcome. GitHub mutations remain the reconciler's separate
responsibility.

This is a built internal authority boundary. One implementation ships; the
port does not establish a supported third party publication service.

## Current contract

The port has one operation, `advance`
(`apps/worker/src/curie_worker/publication_loop.py::PublicationLineageAuthority.advance`),
returning `None` or an awaitable resolving to `None`. It takes a publication UUID
and these named facts:

1. `expected_version` and `expected_head_sha` name the observed lineage version
   and stored head. The expected head may be `None` before the first publication.
2. `expected_publication_version` and `lease_owner` name the worker's claimed
   publication version and lease holder. They fence an old worker after another
   worker reclaims the publication.
3. `pr_number`, `pr_url`, and `head_sha` name the resulting pull request and
   exact commit. `metadata_updated_at` is required for a successful publication
   that changes only metadata and must be absent for a publication carrying a
   patch.

`PublicationLineageClient`
(`apps/worker/src/curie_worker/publication_clients.py::PublicationLineageClient`)
translates those facts into a `PATCH` to
`/v1/internal/publications/{publication_id}/lineage`, with `state` set to `open`.
The HTTP request uses `PublicationLineageAdvance`
(`apps/api/src/curie_api/schemas/publications.py::PublicationLineageAdvance`),
which validates full commit identifiers and the version and identity fields.
The client refuses an empty worker token at construction, sends
`X-Curie-Worker-Token`, and disables redirects. The route authenticates with
`require_internal_worker_token`
(`apps/api/src/curie_api/auth.py::require_internal_worker_token`), independently
of the platform API key.

The API route
(`apps/api/src/curie_api/routers/publications.py::advance_publication_lineage`)
checks the publication outcome before contacting GitHub, then calls
`verify_publication_identity`
(`apps/api/src/curie_api/publication_authority.py::verify_publication_identity`).
The stored writer repeats the outcome checks under row locks
(`apps/api/src/curie_api/crud/lineages.py::publication_lineage_outcome_conflict`,
`apps/api/src/curie_api/crud/lineages.py::advance_publication_lineage`). The
lineage must remain open, its latest revision and pull request identity must
match, and both expected versions, expected head, lease owner, and approved
publication state must still be current. One transaction advances the head and
lineage version, settles the publication, clears its lease, and records the
outcome. A stale comparison refuses the transition.

The client distinguishes refusal from unavailability:

1. HTTP 409 with a verified merged or closed state raises
   `PublicationRemoteTerminalError`
   (`apps/worker/src/curie_worker/publication_loop.py::PublicationRemoteTerminalError`).
   Other 409 responses raise `PublicationLineageRefused`
   (`apps/worker/src/curie_worker/publication_loop.py::PublicationLineageRefused`).
2. An HTTP transport failure or HTTP 503 raises
   `PublicationIdentityUnavailable`
   (`apps/worker/src/curie_worker/publication_loop.py::PublicationIdentityUnavailable`).
   Other responses except HTTP 200 raise `PublicationReconcileError`
   (`apps/worker/src/curie_worker/publication_loop.py::PublicationReconcileError`).

The reconciler handles a refused replay only when the store confirms its
publication is already terminal. A refusal alone is not success
(`apps/worker/src/curie_worker/publication_loop.py::PublicationReconciler._advance_lineage`).

## Implementations today

One production client, `PublicationLineageClient`, implements the worker port.
`_build_publication_loop`
(`apps/worker/src/curie_worker/run.py::_build_publication_loop`)
supplies it to `PublicationReconciler` with the API URL, internal worker token,
and shared HTTP client. The API authority uses GitHub identity reads and
Postgres persistence. Worker doubles in
`apps/worker/tests/test_publication_loop.py` exercise the port without adding a
second production authority. Client wire and error behavior are covered in
`apps/worker/tests/test_publication_clients.py`; API identity checks are covered
in `apps/api/tests/test_publication_lineage_identity.py`.

## Known leakage

The operation deliberately carries GitHub pull request identity, a commit SHA,
and GitHub metadata time. It is not a provider neutral publication abstraction.
The API owns the current repository, installation, pull request node, and base
ref verification, so changing that provider requires more than replacing the
worker client.

The port covers published outcome advancement. Publication leasing, approval,
credential redemption, GitHub writes, and terminal lineage marking have their
own paths. `PostgresPublicationStore` does not write a published head
(`apps/worker/src/curie_worker/publication_store.py::PostgresPublicationStore`),
but the reconciler still uses its store to mark a lineage terminal. Replacing
this one port does not replace the whole publication lifecycle.

## Cross-links

1. **Related work:** #2274 establishes thread lineage; #3923 catalogs this
   authority boundary.
2. **Vision doc:** [architecture-vision.md](../../architecture-vision.md).
   Publication lineage authority is not one of its six graded jobs.
3. **ADR:** [ADR 0143](../../adr/0143-thread-owned-pull-request-lineage.md)
   records one fenced pull request lineage per coding thread.
