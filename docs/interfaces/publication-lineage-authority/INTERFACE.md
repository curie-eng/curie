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
API, which verifies current code-host identity and atomically advances the
lineage with the publication outcome. Code-host writes use separate internal
API endpoints and are outside this authority operation.

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
uses two identity branches. For GitHub it checks the publication outcome before
contacting the host, then calls `verify_publication_identity`
(`apps/api/src/curie_api/publication_authority.py::verify_publication_identity`)
for installation, repository and pull request node identity and base checks.
For other hosts it first calls `verify_code_host_identity`
(`apps/api/src/curie_api/publication_authority.py::verify_code_host_identity`)
through the forge `CodeHost`, then checks the outcome against that read's URL
(`apps/api/src/curie_api/routers/publications.py::_verified_identity`).
On a first publication the route also calls `verify_publication_commit`
(`apps/api/src/curie_api/publication_verification.py::verify_publication_commit`)
through the publishing code host. The writer refuses an advance without that
proof or with a stale one, and records the verified parent as the lineage
`base_sha`.
The stored writer repeats the outcome checks under row locks
(`apps/api/src/curie_api/crud/lineages.py::publication_lineage_outcome_conflict`,
`apps/api/src/curie_api/crud/lineages.py::advance_publication_lineage`). The
lineage must remain open, its latest revision and pull request identity must
match, and both expected versions, expected head, lease owner, and approved
publication state must still be current. One transaction advances the head and
lineage version, records the verified first parent on a first publication,
settles the publication, clears its lease, and records the
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
and shared HTTP client. The API authority uses GitHub App identity reads or
forge `CodeHost` identity reads, plus Postgres persistence. Worker doubles in
`apps/worker/tests/test_publication_loop.py` exercise the port without adding a
second production authority. Client wire and error behavior are covered in
`apps/worker/tests/test_publication_clients.py`; API identity checks are covered
in `apps/api/tests/test_publication_lineage_identity.py`.

## Known leakage

The wire still carries a numeric `pr_number`, commit SHA and metadata-update
time. Those shapes constrain another provider even though the API supports
code hosts beyond GitHub. Installation and node checks belong to its GitHub
branch; the forge branch resolves the stored repository and reads the pull
request through `CodeHost`, checking branch, head, state, URL and base ref.
Replacing the worker client alone does not replace either authority branch.

The port covers published outcome advancement. Publication leasing, approval,
credential redemption, code-host writes, and terminal lineage marking have their
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
