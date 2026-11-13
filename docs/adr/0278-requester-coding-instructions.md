# 278. Requester coding instructions are pinned preferences below repository rules

Date: 2026-11-10

Status: Accepted

**Maintainer approval:** The repository owner approved acceptance alongside
implementation on 2026-11-12, recorded in the implementing issue #5590, under [ADR
0102](0102-accepted-alongside-implementation-with-explicit-approval.md). The decision is
realized in three stages. Stage 1 (#5590) realizes the store, owner API and tenant
policy of section 1, the limits, revisions and digest of section 2, and the frozen
selection and context contracts; its realizing code path is the owner profile router and
service under `apps/api/src/curie_api/`, the profile model in
`apps/api/src/curie_api/models.py` and the `curie cluster coding-instructions` command
under `cli/src/commands/`. Stage 2 (#5591) realizes requester resolution in section 1
and atomic selection and pinning in section 2. Stage 3 (#5592) realizes sections 3, 5
and 6 and the behavioral evidence for section 4. Until stage 3 ships, no factory run
applies a stored document and admission records explicit absence.

## Context

Developers using the [Software Factory](../software-factory.md) can have coding
preferences that apply across repositories. Repository guidance remains the
shared contract, but it does not express every requester's preferred style.
Copying personal guidance into every repository confuses those two scopes.

The factory's `examples/dark-factory/skills/implement-issue/SKILL.md` already
directs implementation to read repository guidance, including `AGENTS.md` and
`CONTRIBUTING.md`. `runner/src/curie_runner/adapter.py::build_options` preserves
workspace `CLAUDE.md` loading through explicit SDK setting sources.
`runner/src/curie_runner/__main__.py::_compose_system_prompt` composes memory,
workspace facts, bundle instructions and model identity. None of these seams
currently supplies a versioned requester preference profile.

[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md) and
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md) define
canonical tenant principals and verified identity links.
[ADR 0268](0268-personal-subscription-execution-and-acceptance-contract.md),
accepted through #5371, binds personal execution to an immutable original owner
and an enrollment generation. Its epic is #5370; #5376 and open PR #5441 add
execution ownership, and #5377 owns enrollment lifecycle.
[ADR 0270](0270-hosted-connectors-receive-the-verified-requester-as-a-signed-caller-claim.md)
defines verified original requester attribution. These supply identity, not a
new username, provider account namespace or model credential for this feature.

Source inspection: `origin/next` at
`f975ed30d2b7007c97e09b00123806e0ebcd898a` on 2026-11-10. PR #5441 remains
an implementation proposal; this document does not claim its behavior ships
on that base. This decision covers platform structure only. It changes no bundle skill text.

## Decision

### 1. Store one tenant and owner scoped profile in the platform

Store one optional UTF-8 text document per `(tenant_id, principal_id)` in Postgres,
separate from agent memory, repository contents and subscription Secrets. Only that
authenticated active principal may create, replace, clear or read its current document
through an owner scoped API. Writes compare an expected version and return conflict on
stale versions. A tenant administrator may disable the capability for its tenant, but
cannot edit another principal's preferences or impersonate that principal through the
owner API. The tenant administrator is the existing administrative authority acting on a
tenant scoped policy endpoint; this decision adds no role system. Disabling retains
stored documents and existing pins, lets owners keep managing their stored document,
gives newly admitted work explicit absence, and prevents further implementation model
work that would use an already selected snapshot while the tenant is disabled. A
selection is never silently discarded.

The trusted admission, worker and runner components may read only the profile
or immutable snapshot needed for their authorized execution. Normal operators,
other developers and reviewers receive application metadata, not a profile
directory or raw text. Database administrators remain inside the storage trust
boundary. Content reaches the configured implementation model provider and can
shape generated code; this is not a secret store or a promise against model
disclosure. Do not put credentials or confidential operational details in it.

Resolve the initiating requester through the existing verified principal path and
persist its canonical principal reference at WorkItem admission. Never derive it from an
issue author name, comment text, Git author, approver or the latest steering actor. For
personal execution this reference must equal ADR 0268's persisted owner; tenant, active
enrollment and generation checks still apply. For explicit service execution, a verified
requester may select its profile without creating subscription ownership or changing
credential mode. Ownerless service work receives no personal profile. Service work whose
initiating actor has no verified link to a principal, including work from a tracker with
no identity namespace, continues with explicit absence. Personal admission without its
verified owner is refused. Identity is never inferred from requester text. This adds a
reference to the existing principal, not a separate preference identity or automatic
link.

A profile belongs to the principal, not its subscription enrollment. Grant
replacement or reconnect advances only enrollment generation. Disconnect keeps
the stored profile but stops personal execution under ADR 0268. No profile
operation enrolls a grant, revives an inactive principal or authorizes a run.

### 2. Pin a bounded immutable snapshot for the WorkItem

The limits are one current document per owner, at most 16,384 UTF-8 bytes after
converting CRLF and CR line endings to LF, and one selected snapshot per
WorkItem. Preserve other bytes, including whitespace; reject invalid UTF-8,
NUL and oversized input rather than truncating. Empty text clears the current
document. There are no attachments, executable includes, URL fetches, multiple
profiles or repository selectors.
The owner API PUT body and raw CLI input file have a separate 131,072-byte transport
limit enforced before decoding, so larger raw inputs are refused even if line-ending
normalization would produce a valid document.

Each successful mutation advances a positive, monotonically increasing revision stored
as a PostgreSQL BIGINT in the range 1 to 9223372036854775807, including clear; refuse
overflow. The current row retains its revision after clear, so recreation cannot reuse
an old revision. Store a SHA-256 digest of the exact normalized text and a snapshot
format version of 1. There is no separate archive of every edit: superseded current text
survives only where a WorkItem has pinned it.

Admission atomically records either an immutable text snapshot with principal reference,
revision, digest and format version, or explicit absence. While a deployment has not
enabled application of requester instructions, because its worker and runner cannot yet
deliver, validate and acknowledge them, admission selects no text and records explicit
absence with the reason that application is unavailable. That absence stays pinned after
application is enabled. The snapshot and every ExecutionRequest reference are committed
together with admission. Concurrent profile edits serialize with that selection.
Revisions, CI repair, approval by another principal, recovery and explicit enrollment
revalidation retain the WorkItem's original selection. Updates and clears affect
subsequently admitted WorkItems only; changing preferences for existing work requires a
new tracker issue: stopping work and applying the label again to the same issue keeps
the WorkItem and its pin, until a separate lifecycle decision permits a new WorkItem for
the same issue. Recheck current execution authority before continuing, without rereading
the owner's latest text.

Snapshots share their WorkItem's retention and access controls; they are not shared
agent or channel memory. Retained prompt and checkpoint content that contains the text
follows the same WorkItem retention mechanism. This decision sets no retention duration
and no separate deletion procedure. Clear removes the current text, not retained
execution evidence. An absent profile is valid. An unavailable store, malformed snapshot
or digest mismatch is a visible failure before model work, never an implicit successful
run without the selected preferences.

### 3. Deliver request context independently of repository and bundle files

The trusted worker resolves the pinned snapshot from execution state and sends a
versioned internal context envelope to a worker-authenticated runner endpoint before
starting the execution. Bind it to the WorkItem, ExecutionRequest and current execution
fence, with the selection metadata and text or explicit absence. The runner verifies
bounds, format and digest before composing its implementation prompt. The runner constructs
the preference-bearing session and connects to the model only after that validation,
and model work starts only after the API has accepted the acknowledgment for the current
fence. A missing envelope, mismatched fence or unsupported version refuses start. Existing scoped runner
authentication protects this endpoint; the model cannot supply or replace the envelope.

This is a new internal worker to runner contract, outside frozen ACI messages,
BootEnv and plugin format. It must have explicit matched readers and writers.
Do not overload an existing credential, issue body or memory entry to transport
it. Do not write the text into repository `CLAUDE.md`, `AGENTS.md`, native user
settings, shared home directories or bundle artifacts.

Extend `_compose_system_prompt` with a distinct requester preference section.
Retain workspace guidance loading in `adapter.py`. A fresh execution or resume
reconstructs the same pinned section, and handle reuse requires a matching
requester and selection. Histories and native checkpoints must not restore
another WorkItem's selection. Keep the stable bundle prefix before the new
requester section; do not put raw profiles in ordinary logs, traces or public
artifacts. Prompt and checkpoint storage that retains the text requires the
same access controls and retention as its snapshot.

### 4. Preferences cannot override platform, bundle or repository rules

Conflict precedence, from highest to lowest, is platform safety and operator
controls, mandatory bundle workflow rules, repository guidance, requester
preferences, then ordinary suggestions from remembered context. Repository
guidance includes all applicable scoped `AGENTS.md`, `CONTRIBUTING.md` and
workspace `CLAUDE.md` rules. Compatible personal preferences supplement them;
repository choices win every style conflict. A repository or bundle cannot
relax a platform control.

Textual placement does not create authorization. Personal text cannot add
tools, skills, credentials, egress, grants, permissions or execution budget,
change the task's acceptance criteria, or remove review and publication gates.
Keep existing executable authorization and publication checks authoritative.
The model is instructed to follow this precedence, but style compliance is a
behavior to evaluate, not a deterministic guarantee from prompt ordering.

### 5. Reviewers judge the repository contract independently

Only implementation roles apply the preferences. Platform reviewer sessions
use repository guidance, task acceptance criteria, bundle review policy and
platform controls. A reviewer neither requires personal style compliance nor
accepts it as justification for a repository violation. An otherwise valid
change cannot be blocked solely for missing a personal preference.

Compose reviewer context separately: exclude the personal instruction section, the
implementer's system prompt and inherited native checkpoints carrying it. Reviewers may
see application metadata and review the resulting patch as data. Do not rely on a
sentence telling an inheriting reviewer to ignore preferences. If an execution adapter
cannot separate those contexts, refuse this capability on that adapter until it can.
Separation is established by an executable proof on the adapter's real reviewer path for
fresh, resumed and replayed sessions, not by inspecting SDK options. Where the native
path cannot satisfy it, use a platform-mediated clean reviewer session behind the
existing call contract, or hold the capability unavailable. This is a platform role
boundary, not a change to any bundle's skill text or verdict format. ADR 0268 still
charges implementer and reviewer sessions to the same owner grant in personal mode;
sharing billing authority does not require sharing instruction context.

### 6. Record application through trusted execution and publication metadata

Each ExecutionRequest records requester reference, selected revision, format version and
digest, plus whether the implementation context actually loaded that selection.
Distinguish `none`, `applied` and `failed`, and report a selected but not yet
acknowledged request as `pending`, never as `none` or `applied`; a digest alone is not
evidence the runner used the text. The runner acknowledges verified loading before model
start, and the API fences that acknowledgment to the current request. Authorized run
reads expose these facts without raw profile text.

For an applied selection, trusted publication adds
`Requester preferences: applied; sha256:<digest>` to the PR or merge request
metadata, sourced from the acknowledged execution record. No requester name,
principal id or raw text is published. A failed or unacknowledged selection
cannot publish as applied. This tells repository reviewers that personal
preferences shaped implementation without making them review requirements.

### 7. Amend only the necessary Accepted contracts on acceptance

This decision partially amends
[ADR 0167](0167-agent-and-channel-memory-are-written-by-the-agent-guided-by-editable-guidance.md):
its rejection of per-person memory no longer excludes this explicit owner
written instruction store, and its rejection of version history does not apply
to pinned instruction snapshots. Agent and channel memory remain unchanged.

This decision partially amends
[ADR 0119](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md)
decisions 1 and 2: an execution prefix additionally contains a requester scoped
instruction section, stable for that WorkItem and absent from reviewer context.
Bundle and tool prefix stability, ordered message history and observable cache
accounting remain required; no cache hit is claimed by this decision.

ADRs 0166, 0198, 0268 and 0270 supply identity and authority without amendment. [ADR
0189](0189-deployed-agents-see-only-their-bundles-skills.md) retains its workspace
settings and skill restrictions. [ADR
0222](0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
continues to own the bundle and platform division. The accepting change adds precise
partial amendment back-links immediately below the status lines of ADR 0167 and ADR 0119
under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md), and
changes nothing else in any Accepted ADR.

## Realizing paths and required evidence

These paths realize the decision in the three stages the status note names, tracked by
#5590, #5591 and #5592. The evidence below is required before the capability is claimed;
stage 1 claims only owner storage and management.

1. `apps/api/src/curie_api/models.py`, `routers/work_items.py` and
   `workitems/lifecycle.py`: owner API, versioned profile writes, tenant and
   principal predicates, atomic snapshot selection and per-request evidence.    Prove
   owner writes succeed, stale writes conflict, another owner or tenant    cannot read
   or write, and absent and unavailable profiles differ, against    real Postgres
   through the authenticated API. 2. `apps/worker/src/curie_worker/binding.py`,
   `kernel/claim.py`,    `sandbox/substrate.py`, and
   `runner/src/curie_runner/server.py`: versioned    context delivery, execution
   fencing, acknowledgment and reuse checks.    Prove repair, restart, approval by
   another principal and enrollment    replacement keep the pin, while a second
   requester cannot inherit it. 3.
   `runner/src/curie_runner/__main__.py::_compose_system_prompt` and
   `adapter.py::build_options`: implementation and reviewer context separation,
   workspace loading and bounded parsing. Exercise 16,384 bytes, 16,385 bytes,
   multibyte UTF-8, clear, malformed text and digest tampering. Drive the real    native
   reviewer path; helper output or SDK option inspection is insufficient. 4.
   `apps/api/src/curie_api/workitem_outcomes.py` and
   `apps/worker/src/curie_worker/publication_loop.py`: trusted application    evidence
   and publication metadata. Drive actual factory admission through    implementation,
   independent review and approved publication on an owned    forge fixture and
   throwaway cluster, with the required live provider and    external integration proof.
   Show repository guidance wins a conflicting    preference, a compatible preference
   shapes the diff, and no profile is the    falsifiable control. A request to bypass
   review or widen a tool grant must    fail through the real gate. Record command,
   candidate commit, observed outcome and resource cleanup.

## Consequences

1. A developer can carry bounded preferences across repositories without
   changing their shared rules or the factory bundle. Owner API and trusted
   execution access become an additional platform storage responsibility.
2. Pinning makes results explainable across repairs and restarts. A preference
   edit cannot silently alter an in-flight review; applying it costs a new
   WorkItem. Retained snapshots can outlive a cleared current profile.
3. Role separation can require separate native sessions and extra prompt cost.
   Requester sections reduce cache sharing beyond the common bundle prefix.
4. Preferences remain model guidance. Existing authorization gates provide
   enforceable limits; no amount of prompt precedence proves style compliance
   or prevents every disclosure through generated output.
5. Reviewer metadata reveals that preferences were used and fingerprints the
   text. Even a digest can be recognized from known text; owners must treat
   publication of that fingerprint as part of using the feature.

## Rejected alternatives

1. A file fetched from a user owned repository: adds forge authorization,
   mutable references and availability to every admission for a small document.
2. Agent or channel memory: shares scope across requesters and permits model
   writes, which cannot establish the owner's explicit versioned preferences.
3. Subscription Secret or enrollment generation as the profile store/version:
   couples independent preferences to credential custody and replacement.
4. Inject personal `CLAUDE.md` or native settings: obscures provenance and can
   change configuration authority or override repository guidance.
5. Resolve the latest profile on every repair or resume: makes one change use
   different instructions without a new admission or reviewer-visible selection.
6. Let personal preferences outrank repository rules or bind reviewer verdicts:
   gives an individual control over the shared contract and independent review.
7. Rely on implementer-written provenance: permits an unverified claim about
   what was loaded instead of trusted request evidence.

## Resolved questions

1. Maintainer acceptance is recorded in the status note, and implementation is split
   into the three tracked stages named there. 2. The first owner editing surface is the
   CLI, through the existing principal session client. A console surface, when added,
   uses the same owner API and cannot change these decisions.