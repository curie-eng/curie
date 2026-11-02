# 256. Publication validates and pushes a Git object overlay

Date: 2026-10-31

Status: Draft

This Draft proposes to partially supersede two Decision clauses of
[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md):
the worker "validates the binary patch, base commit, repository, paths, and the
private base archive", and the Job "clones, branches, safely applies, commits,
and pushes". Publication instead validates an object-bound private source
receipt and constructs a revision without a full checkout. This retains
ADR 0147's "validated against a privately-held base" guardrail: the receipt,
prepared commit/tree and validated manifest remain private platform authority.
It does not authorize implementation. ADR 0125 is not edited by this Draft.
Acceptance must add the partial-supersession back-link required by
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
without rewriting ADR 0125's body or changing its Accepted status.
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
4. Validation now depends on forge access before approval creation. The worker
   redeems a read-scoped `workspace_clone` credential through the API, backed by
   CodeHost's clone credential operation and restricted to repository reads.
   The API resolves the immutable repository and rechecks its allowlist at
   every redemption, not only at preparation. The credential is scoped to the
   verified origin, never persisted in Git config, and never sent to the
   sandbox. Existing redirect, CA, audit and redaction rules remain. A provider
   unable to attenuate a credential requires a separately provisioned read-only
   credential; a push credential is not a substitute.
5. Forge transport failure refuses with `publication_validation_forge_unavailable`;
   a mismatched repository identity or absent allowlist authority refuses with
   `publication_validation_repository_refused`. An authenticated authoritative
   answer proving the prepared commit is unavailable refuses with
   `publication_validation_commit_unreachable`. An ambiguous 404 or fetch error
   is unavailable, not proof of an unreachable commit. None creates an approval
   or falls back to the archive. This deliberately replaces offline validation
   availability with bounded, fail-closed forge reads.

### One object engine, no complete checkout

1. One shared engine serves worker validation and the trusted publisher. It
   retrieves exact shallow commits without trees, then only ancestor trees
   along affected paths and the required old blobs. It performs no checkout,
   recursive tree listing, complete index construction or full-tree staging.
   Set `GIT_NO_LAZY_FETCH=1` for every Git process in validation and the Job.
   Object reads use only verified local objects; a missing object must go
   through the bounded explicit fetcher before the read is retried. Every
   object-fetch invocation requests an exact ID/type set with `--filter=tree:0`
   and `fetch.negotiationAlgorithm=noop`, including explicit tree/blob fetches.
   No Git read, promisor callback or negotiation side path may fetch objects
   implicitly or bypass these settings and limits.
   Each fetch declares an exact expected object-ID/type set, of at most 256
   objects. The received pack's count must not exceed that set, and its final
   object set must equal the requested set. Each ID has a size allowance from
   verified receipt metadata when known, otherwise the existing configured
   workspace checkout-byte ceiling. Expanded bytes cannot exceed the remaining
   workspace checkout-byte budget; wire bytes cannot exceed the lower of the
   remaining configured archive-byte budget and 65,536 plus twice the sum of
   those per-ID allowances. Enforce both limits on every explicit fetch while
   streaming, terminate the transfer immediately on excess, and refuse
   with `publication_object_fetch_limit`. Do not first spool an unbounded pack.
   Unexpected objects refuse with `publication_object_fetch_scope`. Thus a
   server or proxy that starts ignoring filters after qualification cannot
   silently expand publication into a full-repository transfer.
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
   Every newly generated tree, including each replay tree and ancestor root,
   must pass Git's own path rules and fsck-strict tree-object validation before
   any manifest is sealed. Enable HFS/NTFS name protections independently of the
   publisher host. Reject `.git` at any depth, platform lookalikes, duplicate
   entries and invalid ordering. `mktree` success alone is insufficient. Check
   generated objects in bounded quarantine; prove unchanged references through
   the source tree chain without recursively fetching their closure.
4. The engine reconstructs only affected ancestor trees. Unchanged entries
   retain their existing object IDs. Before approval, a private validation
   manifest M0 binds repository, source receipt, prepared parent, patch digest,
   policy inputs, result tree and generated object identities. After approval,
   parent selection and replay produce a second manifest M1 bound to the exact
   approved M0 digest, publication/lineage versions, selected parent and result
   objects. M1 exists even when the parent is unchanged. The immutable Job
   contract binds both manifests, and retry adoption verifies both. A new parent
   cannot mutate M0 or reuse an M1 from another approved revision.
   M1 binds all rebuild inputs, including exact patch bytes, parent/source
   objects, Git object format and commit metadata (author, committer, fixed
   timestamps, message and revision trailer). The Job rebuilds blobs, trees
   and the revision commit from those bound inputs. It refuses before push
   with `publication_manifest_rebuild_mismatch` unless every generated object
   ID, including the root tree and commit ID, equals M1's manifest. Matching
   manifest digests without rebuilding the objects is insufficient.
5. The work bound is affected old/new blob bytes plus affected ancestor-tree
   metadata, one sequential round trip per affected path depth level batched
   across paths at that level, and bounded transport/API overhead. Ancestor tree
   width is an explicit cost. Unrelated blobs and repository history are
   excluded. This does not claim the entire factory run has that bound.
6. Changed blobs contain the unconverted index bytes produced by applying the
   approved patch directly to the index. No checkout, clean/smudge filter,
   text conversion, line-ending normalization or `.gitattributes` conversion
   runs. This deliberately changes the old checkout/apply/`git add` behavior
   when attributes would convert bytes. The reference implementation for tests
   loads the complete trusted base into an isolated index, executes
   `git apply --cached --binary` and writes its tree, with no worktree staging.
   Compare complete tree IDs and raw blob bytes, including CRLF, `text=auto`,
   explicit `eol` and filter attributes. Dependency checks and replay consume
   those same raw bytes.

### Parent selection, replay and push

1. The worker obtains a candidate parent through API-owned CodeHost operations
   after approval and before launch, then seals M1. First publication requires
   the approved base to be an ancestor of that parent. The publisher uses that
   exact parent; later revisions keep their exact expected prior head. No timestamp-bounded
   deepening or shallow-history inference substitutes for forge ancestry.
2. A moved-base first publication performs a path-local three-way replay and
   validates the resulting paths, modes and dependency changes against the new
   parent. Conflict, empty, unrelated and unproved outcomes preserve the
   existing refusal categories. Rename-dependent replay deliberately changes:
   the old `cherry-pick` can follow file and directory renames, but the bounded
   replay refuses such cases as `publication_base_moved_conflict`, without
   pushing. It does not search unrelated paths or claim equivalent success.
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

1. Drop none of ADR 0197's four CodeHost adapters: GitHub, GitLab, Bitbucket
   Cloud and Bitbucket Data Center. Global cutover waits for every row of this
   finite qualification matrix to pass selective fetch and bounded push:

   | Adapter | Named qualification target |
   | --- | --- |
   | GitHub | Hosted API 2022-11-28, GraphQL schema and Git protocol v2, qualification profile v1 |
   | GitHub | Enterprise Server: oldest release with complete passing qualification and newest stable release, Git protocol v2 |
   | GitLab | Hosted API v4 and Git protocol v2, qualification profile v1 |
   | GitLab | CE and EE at the documented 17.0 floor (17.0.0) and newest stable release, Git protocol v2 |
   | Bitbucket Cloud | Hosted API 2.0 and Git protocol v2, qualification profile v1 |
   | Bitbucket Data Center | Documented 9.4.0 floor and newest stable release, Git protocol v2 |

   These are probe targets, not claims of qualified support. A hosted profile
   records the actual probe date, exposed service version/capabilities, client
   Git version and redacted evidence; it does not pretend SaaS builds are
   caller-pinnable. A changed capability profile must requalify. Resolve newest
   stable releases from official vendor sources at qualification time, then
   pin and record exact versions and fixture/image identities for that run.
   Keep the documented supported ranges: GitLab 17.0 or later and Bitbucket
   Data Center 9.4.0 or later. Releases between the floor and newest tested
   release are supported, not an exact-version allowlist; the newest tested
   release is evidence, not an upper version gate. GHES has no documented
   floor today: set its floor to the oldest release with complete passing
   qualification and support the range from that floor. A self-managed version
   below its floor refuses before admission with
   `publication_forge_version_unqualified`, naming the version and required
   minimum. A required endpoint that fails keeps the entire cutover blocked;
   it never removes an adapter or narrows the existing documented ranges.
2. Explicitly refused and silently ignored filters are named pre-admission
   refusals (`publication_partial_clone_unsupported`); streaming limits still
   apply to every actual fetch. There is no full-clone, archive-rehash or
   history-walk fallback. Support on one forge does not qualify another.
3. Implementation is split into
   [engine plus validation](https://github.com/curie-eng/curie/issues/5235), and
   [publisher plus cutover](https://github.com/curie-eng/curie/issues/5236).
   Both require ADR acceptance; publisher cutover also requires the
   complete qualification list. The first part delivers executable engine and
   validation interfaces, receipts, M0 and qualification tooling. Production
   admission switches only with the second part, so raw-byte validation is
   never paired with the old converting publisher. This is staged delivery,
   not a runtime fallback or dual execution path. Neither issue admits factory
   work or grants live-provider spend.
4. Receipt producers, durable state, validation, Job construction and recovery
   consumers migrate together. Cutover drains active claims and nonterminal
   publications before enabling the new contract. It does not silently
   reinterpret an approved revision or adopt an older Job with a new manifest.
   Existing publication history remains readable. No dual execution path is
   added to keep old callers working.
5. The scripted factory Git stub must explicitly enable
   `uploadpack.allowFilter` and `uploadpack.allowAnySHA1InWant` for its isolated
   fixture repository. A companion run disables filtering and proves refusal
   before publication. Do not change those settings on an operator repository.

## Safety properties

| Property | Existing boundary | Object-overlay requirement |
| --- | --- | --- |
| Repository and prepared-base identity | Retained private workspace and exact worker comparison | Claim-scoped source receipt and verified commit/tree chain; agent HEAD cannot select the base |
| Patch applicability | Apply against the extracted sanitized base | Apply against affected entries proven to match the bound prepared source |
| Complete change scope | Independently derived paths equal declared paths | Independently derive paths and verify tree reconstruction, including replay results |
| Protected paths and safe modes | Worker path policy and old/new mode checks | Preserve policy and check affected prefixes as well as old/new entries |
| Git path and tree validity | Git apply/add reject invalid paths and tree entries | Every generated/replayed tree passes Git path rules and fsck-strict checks, including nested `.git`, HFS/NTFS lookalikes, duplicates and ordering |
| Dependency restrictions | Parse affected before/after manifests and locks | Retain the same complete-file policy checks |
| Bundle review freshness | Not a platform property; the platform does not bind the bundle fingerprint to a publication | Unaffected; bundle review policy remains outside this ADR |
| Integrity and provenance | Archive digest, private patch state and immutable Job resource contract | Verify each consumed object; bind M0/M1 and retry adoption, and require the Job's rebuilt blob/tree/commit IDs to equal M1 |
| Ancestry and revision identity | API revision marker, single parent and forge ancestry checks | Preserve approved base <= parent <= current tip for first publication and exact later parent |
| Concurrent branch updates | Expected-head read and force-with-lease push | Retain both checks, including branch creation races |
| Approval and runtime authority | Atomic approval/publication creation, replay and deadline/lease fences | Preserve all authority checks and refusal behavior |
| Credential isolation and recovery | Trusted push-only Job, immutable resource adoption and owned cleanup | Preserve identity boundaries, exact adoption, deterministic recovery and cleanup |
| Allowlist and validation access | Private archive permits offline validation; redemption rechecks repository authority | Read-scoped validation credential with an allowlist recheck at every redemption; named unavailable/unreachable refusals, without archive fallback |
| Transfer bounds | Existing workspace and Git time/size envelopes | Disable lazy fetch; explicit exact-set tree:0/noop fetches enforce streaming byte/object limits even after qualification |

These are requirements for the future implementation, not claims that this
documentation change has established runtime protection. API commit checks
currently prove provenance and ancestry; they do not independently reconstruct
the approved result tree. The manifest continues to rely on trusted publisher
execution, bound inputs and verified objects.

## Consequences and verification

The platform owns a source receipt and object manifest in addition to private
patch state. It takes on selective-object transport qualification and tree
reconstruction correctness. Engine plus validation is estimated at 900 to 1,400
production lines; publisher plus cutover and qualification at 800 to 1,300,
both plus tests. Correctness risk remains medium to high. These are design
estimates, not measured delivery evidence.

Qualification changes neither GitLab's documented 17.0+ range nor Bitbucket
Data Center's 9.4.0+ range into a list of individually tested releases. Floor
and newest-release tests qualify the endpoints, and the intervening releases
remain supported. Newest releases must be resolved and pinned for each
qualification run; that tested endpoint does not impose an upper support
cutoff. GHES gains an explicit minimum only from the oldest release whose
complete qualification passes. Before cutover, #5236 must publish that measured
GHES floor and the below-floor refusal in `docs/capabilities.md` and
`docs/guides/dark-factory-quickstart.md`, alongside the unchanged GitLab and
Bitbucket ranges. This Draft does not invent an untested GHES minimum.

Validation adds read-only credential provisioning and rotation work. The
static-token GitLab, Bitbucket Cloud and Bitbucket Data Center adapters need a
separate read-only repository credential alongside their publishing token.
GitHub, including GHES, needs the same extra credential when using a static
token. GitHub App installations may instead mint a repository-read-only
installation token, but #5235 must implement and verify that attenuation;
today's clone/push scope label alone does not attenuate the returned token.
Provisioning and authenticated read-only checks are dispatch prerequisites;
operators must not reuse a write token as validation authority.

The realizing implementation belongs in workspace preparation and ownership,
`publication_validation.py`, `publication_k8s.py`, `publication_loop.py`, their
storage/client callers and a shared internal object engine packaged for both
worker and publisher. CodeHost parent selection stays behind API routes and
`publication_verification.py`. This Draft changes none of those paths.

Qualification must compare complete result trees against a reference apply,
execute every existing refusal, verify bounded reads and transferred object
identities on cold stores, and drive the actual publisher and API settlement.
Issues #5235 and #5236 own that evidence. The scaling fixture adds 50,000
unrelated files, 1 GiB of unrelated contents and 10,000 historical commits beneath untouched history/tree
boundaries. Publication must fetch no additional unrelated blobs or historical
commits. Pytest verifies these object/read bounds, not a wall-clock threshold.
A dedicated `curie dev` qualification harness must run the candidate's actual
publisher Job in a digest-pinned runner image, enforce a two-core limit and
20 ms RTT at a 100 Mbit/s link, and record those settings with the digest and
base identity. Five cold validation-plus-publisher trials must each finish in
30 seconds, with the large fixture median at most twice the small fixture's.
Fixture creation, workspace materialization and image loading precede timing.
The large fixture explicitly raises its test-only preparation ceilings to
2 GiB for checkout/archive bytes and 100,000 members, without changing product
defaults.
Each trial starts when validation receives the snapshot and ends at the Job's
successful push marker, including Job scheduling. The harness must error when
it cannot enforce CPU/network controls. It copies inputs through the container
API or owned volumes, accepts endpoints/context from its caller, and verifies
cleanup. This is a qualification target, not evidence measured by this ADR.

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
   and GitHub's GraphQL `createCommitOnBranch` provides compare-and-set through
   `expectedHeadOid`. GitHub REST ref creation requires an absent branch, while
   a non-forced ref update enforces fast-forward ancestry, not an exact lease
   after a rewind. Rejection is not a claim that GitHub lacks compare-and-set.
   `createCommitOnBranch` cannot create a branch or set executable file modes,
   fixes authorship to its authenticated identity, and the other forges expose
   different contracts. Git transport preserves modes, author metadata and one
   publication mechanism across all adapters; API commit writes would also
   change ADR 0197's push-only boundary. See the provider's
   [mutation contract](https://docs.github.com/en/graphql/reference/mutations#createcommitonbranch)
   and [Git ref API](https://docs.github.com/en/rest/git/refs).
7. Retain the preparer's entire Git object store for publication. It can avoid
   refetching the original base, but adds durable storage, retention and garbage
   collection coupling. Moved-base retrieval and bounded pack construction
   still need qualification. It is not required for correctness and is not the
   initial object transport.
