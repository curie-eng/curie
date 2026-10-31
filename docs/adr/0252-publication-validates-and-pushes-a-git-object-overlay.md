# 252. Publication validates and pushes a Git object overlay

Date: 2026-10-31

Status: Draft

This Draft proposes an amendment to
[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md):
publication validates an object-bound source receipt instead of rereading the
private workspace archive, and constructs a revision without a full checkout.
It does not authorize implementation. ADR 0125 is not edited.
[ADR 0147](0147-publication-approval-is-a-per-agent-operator-policy.md),
[ADR 0186](0186-a-factory-ticket-declares-its-base-and-keeps-it.md), and
[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
retain their approval, base identity, credential custody and port boundaries.

## Context

Issues [#5169](https://github.com/curie-eng/curie/issues/5169) and
[#5170](https://github.com/curie-eng/curie/issues/5170) describe publication work
that grows with unrelated repository contents and history. A small change can
exhaust the 120 second Git validation timeout after its implementation and
review have completed.

The worker's `WorkspaceClaimCoordinator.stream_current_base` verifies the
complete retained archive, then streams it again.
`validate_snapshot_against_base` extracts it and stages the complete tree
before and after applying the patch. The publication Job separately clones
full history, checks out the base, applies the patch, commits and pushes.
These steps establish important invariants, but require far more input than
those invariants need.

Partial clone followed by cone checkout still materializes all files under
selected directories and sibling files along their ancestors. It does not
establish cost proportional to the affected files. A small edit in a huge file
also cannot be validated from patch size alone: its old and new blob contents
and the affected directory trees remain necessary inputs.

The prepared-base versus agent-HEAD snapshot defect is addressed separately by
[#5231](https://github.com/curie-eng/curie/pull/5231). This decision preserves
that snapshot contract; it does not reimplement the fix.

## Decision

### Authority and source identity

1. Trusted workspace preparation records a private source receipt binding the
   immutable repository identity, prepared commit, original commit tree,
   materialized source tree, Git object format, archive digest and sanitizer
   version. The receipt is claim-scoped platform state, never an assertion
   accepted from the sandbox. Workspace delivery retains archive verification.
2. Publication validates the existing bounded candidate delta against that
   prepared source. Agent HEAD is not the source authority. The worker retains
   exact repository and prepared-base equality checks and the snapshot contract
   referenced above. A later publication revision uses its prepared prior
   publication head, not a newly selected branch base.
3. Every consumed Git object is verified by type, size and content-addressed
   identity, with a verified commit-to-root-to-path chain. The source receipt
   binds the materialized view to its original commit; entries excluded by
   sanitization stay opaque and cannot become publication edits. Missing,
   malformed, corrupt and mismatched inputs refuse publication.

### One object engine, no complete checkout

1. One shared engine serves worker validation and the trusted publisher. It
   retrieves exact shallow commits without trees, then only ancestor trees
   along affected paths and the required old blobs. It performs no checkout,
   recursive tree listing, complete index construction or full-tree staging.
2. Patch headers and declared paths are untrusted discovery inputs. The engine
   discovers every old and new path, verifies existence and ancestor types
   against the trusted tree, and applies the binary patch through a small Git
   index containing only affected entries. It independently derives the actual
   changes and requires exact equality with the declared paths. Parsing must
   cover quoted names, mode-only and binary changes, deletions, and file versus
   directory transitions; a numstat listing alone is not proof of scope.
3. The engine checks both old and new modes and every affected prefix. It
   preserves protected-path refusals, the 900,000 raw-byte patch ceiling and
   dependency policy. Dependency checks consume complete before and after
   contents of affected manifests and lockfiles. It retains empty-change
   behavior, including the existing separate metadata-only publication path.
4. The engine reconstructs only affected ancestor trees. Unchanged entries
   retain their existing object IDs. A private validation manifest binds the
   repository, prepared source receipt, patch digest, policy inputs, chosen
   parent, result tree and generated object identities. The immutable Job
   contract binds this manifest and verifies its inputs before pushing.
5. The work bound is affected old/new blob bytes plus affected ancestor-tree
   metadata and bounded transport/API overhead. Ancestor tree width is an
   explicit cost. Unrelated blobs and repository history are excluded. This
   does not claim the entire factory run or review metadata scan has that bound.

### Parent selection, replay and push

1. The worker obtains a candidate parent through API-owned CodeHost operations
   before launch. First publication requires the approved base to be an
   ancestor of that parent. The publisher uses that exact parent; later
   revisions keep their exact expected prior head. No timestamp-bounded
   deepening or shallow-history inference substitutes for forge ancestry.
2. A moved-base first publication performs a path-local three-way replay and
   validates the resulting paths, modes and dependency changes against the new
   parent. Conflict, empty, unrelated and unproved outcomes preserve the
   existing refusal categories. Replay that requires repository-wide rename
   discovery is refused rather than expanding its search or scope.
3. The trusted Job pushes one platform-created revision commit and a pack
   containing only new commits, changed blobs and changed ancestor trees. It
   preserves the revision trailer, expected-head precheck and exact
   `--force-with-lease`. Sparse packing and common-parent negotiation must be
   qualified by observed object reads and transfer contents, including a base
   that advances again during publication. Setting Git options is not proof.
4. The API retains post-push revision and single-parent verification. First
   publication proves approved base <= actual parent <= current base tip;
   later revisions require the exact prior head. Forge failure remains
   unproved, not false. A Job marker alone cannot settle a publication.
5. CodeHost adapters remain API-owned. Workers receive transport data and
   scoped credentials, not adapter code. Jobs remain push-only, outside
   sandbox egress restrictions, with the existing tokenless identity and
   resource ownership. Sandboxes receive no forge credentials or wider egress.

### Qualification and cutover

1. Each supported forge/version must prove selective object retrieval and
   bounded push behavior before rollout. Explicitly refused and silently
   ignored filters are named pre-admission refusals. There is no full-clone,
   archive-rehash or history-walk fallback. Support on one forge does not
   establish support on another.
2. The implementation ticket stays blocked until this ADR is Accepted and
   forge qualification evidence exists. This Draft and the ticket do not admit
   factory work or grant live-provider spend.
3. Receipt producers, durable state, validation, Job construction and recovery
   consumers migrate together. Cutover drains active claims and nonterminal
   publications before enabling the new contract. It does not silently
   reinterpret an approved revision or adopt an older Job with a new manifest.
   Existing publication history remains readable. No dual execution path is
   added to keep old callers working.

## Safety properties

| Property | Existing boundary | Object-overlay requirement |
| --- | --- | --- |
| Repository and prepared-base identity | Retained private workspace and exact worker comparison | Claim-scoped source receipt and verified commit/tree chain; agent HEAD cannot select the base |
| Patch applicability | Apply against the extracted sanitized base | Apply against affected entries proven to match the bound prepared source |
| Complete change scope | Independently derived paths equal declared paths | Independently derive paths and verify tree reconstruction, including replay results |
| Protected paths and safe modes | Worker path policy and old/new mode checks | Preserve policy and check affected prefixes as well as old/new entries |
| Dependency restrictions | Parse affected before/after manifests and locks | Retain the same complete-file policy checks |
| Review freshness | Review gate fingerprints and rechecks its candidate | Preserve the snapshot/review contract; a committed tree ID does not replace dirty/untracked freshness checks |
| Integrity and provenance | Archive digest, private patch state and immutable Job resource contract | Verify each consumed object and bind the source receipt, patch, policy, parent, result tree and object manifest |
| Ancestry and revision identity | API revision marker, single parent and forge ancestry checks | Preserve approved base <= parent <= current tip for first publication and exact later parent |
| Concurrent branch updates | Expected-head read and force-with-lease push | Retain both checks, including branch creation races |
| Approval and runtime authority | Atomic approval/publication creation, replay and deadline/lease fences | Preserve all authority checks and refusal behavior |
| Credential isolation and recovery | Trusted push-only Job, immutable resource adoption and owned cleanup | Preserve identity boundaries, exact adoption, deterministic recovery and cleanup |

These are requirements for the future implementation, not claims that this
documentation change has established runtime protection. API commit checks
currently prove provenance and ancestry; they do not independently reconstruct
the approved result tree. The manifest continues to rely on trusted publisher
execution, bound inputs and verified objects.

## Consequences and verification

The platform owns a source receipt and object manifest in addition to private
patch state. It takes on selective-object transport qualification and tree
reconstruction correctness. The estimated implementation is 1,200 to 2,000
production lines plus tests, with medium to high correctness risk. This is a
design estimate, not measured delivery evidence.

The realizing implementation belongs in workspace preparation and ownership,
`publication_validation.py`, `publication_k8s.py`, `publication_loop.py`, their
storage/client callers and a shared internal object engine packaged for both
worker and publisher. CodeHost parent selection stays behind API routes and
`publication_verification.py`. This Draft changes none of those paths.

Qualification must compare complete result trees against a reference apply,
execute every existing refusal, verify bounded reads and transferred object
identities on cold stores, and drive the actual publisher and API settlement.
The replacement implementation ticket linked from this ADR's pull request owns
that evidence. Its scaling fixture adds 50,000 unrelated files, 1 GiB of
unrelated contents and 10,000 historical commits beneath untouched history/tree
boundaries. Publication must fetch no additional unrelated blobs or historical
commits. A proposed 30 second validation-plus-publisher budget for a one-file
change is a qualification target, not a result measured by this ADR.

## Alternatives rejected

1. Raise timeouts or optimize repeated complete archive hashing. These retain
   work proportional to unrelated repository contents and move the failure
   threshold without removing its cause.
2. Shallow full-blob fetch or blobless fetch alone. The former still transfers
   all current blobs; the latter can still transfer every tree. Neither proves
   affected-path cost.
3. Treeless partial clone followed by cone checkout and a sparse index. Cones
   include entire selected directories and sibling files along ancestors;
   root edits can materialize unrelated root files. Repository-wide index or
   tree traversal can remain even when the checkout looks small.
4. Deepen history by the approved commit's timestamp. Timestamps do not prove
   ancestry, and a shallow boundary is unknown rather than unrelated. The
   existing CodeHost ancestry operation answers the required relation.
5. Push from the sandbox or trust a sandbox-produced commit/bundle directly.
   Direct push violates credential and approval custody. A bundle is only
   untrusted transport and still needs independent source, scope, mode, digest
   and parent validation; it does not remove the trusted object engine.
6. Let forge APIs create the revision. Changed-blob/tree APIs are attractive,
   but adapters do not yet establish a common exact branch compare-and-set
   contract. A read followed by an unconditional update loses the lease
   guarantee. Moving commit writes into the API also changes the push-only
   publication boundary of ADR 0197.
7. Retain the preparer's entire Git object store for publication. It can avoid
   refetching the original base, but adds durable storage, retention and garbage
   collection coupling. Moved-base retrieval and bounded pack construction
   still need qualification. It is not required for correctness and is not the
   initial object transport.
8. Replace the source fingerprint with HEAD's tree ID. That omits dirty and
   untracked candidate changes. Tree identity is useful for trusted unchanged
   subtrees, not a substitute for the existing review freshness contract.
