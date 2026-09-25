# 174. Publication prechecks use an execution scoped capability

Date: 2026-09-25

Status: Draft

Tracked in [issue #3215](https://github.com/curie-eng/curie/issues/3215), a
prerequisite for [issue #3200](https://github.com/curie-eng/curie/issues/3200).

Acceptance: Pending explicit maintainer approval of this decision. Authorization
to prepare the prerequisite does not establish acceptance of this Draft.

Realization: The intended code paths are named below. Implementation authority
remains pending under [ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md)
and [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).

## Context

[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)
keeps repository credentials and publication effects outside the sandbox.
[ADR 0143](0143-thread-owned-pull-request-lineage.md) gives a coding thread one
durable pull request lineage and makes the API responsible for reconciling that
lineage with GitHub. A later publication request can therefore target the
existing pull request without changing any files.

A request with an empty file diff and unchanged title and body should return an
actionable tool error while the model can still correct its request. Detecting
that condition after the turn ends cannot provide this interaction. The runner
can inspect the managed workspace, but neither its checkout nor conversation
history proves the current GitHub title and body. Metadata can change after the
turn starts.

The existing publication gate has several observation paths. The real SDK can
expose a tool call to the stream observer before the permission hook executes.
If that observer creates a pending approval record before a metadata comparison,
a later hook refusal cannot restore the intended continuing turn. The decision
must precede every writer of pending publication state.

[ADR 0033](0033-scoped-sandbox-state-token.md) establishes a scoped sandbox
credential for state access. That state capability does not authorize repository
queries. Forwarding a platform key or GitHub credential to make the comparison
would undo both that decision and ADR 0125.

## Decision

Add a narrowly scoped publication precheck capability to the versioned ACI
`Event` as optional context. The trusted worker supplies fresh context for each
covered factory turn, including turns that reuse a warm runner. The carrier
includes lineage identity and version, exact accepted head, plaintext observed
title, body digest, observation completion time, the comparison endpoint URL,
and the signed execution capability. These facts describe a platform
observation; they do not establish current GitHub truth at a later tool call.
Neither the observation nor the capability authorizes publication.

The runner accepts the Event endpoint only when its origin matches the trusted
state URL delivered at boot, or the boot delivered progress URL when the state
URL is absent. It rejects a missing trusted origin, a mismatched origin, model
supplied replacement URLs, and redirects. The Event identifies the endpoint;
it cannot expand the runner's trusted API authority.

This decision amends ADR 0143's requirement that GitHub truth operations be
authenticated by a worker: the new comparison operation also accepts the
execution scoped credential defined here. Existing reconciliation remains
authenticated by a worker. It also adds one explicit scoped credential router
exception to `apps/api/CLAUDE.md`; that exception authorizes only this comparison
route, not additional scoped access elsewhere.

This capability covers factory WorkItem `execute` and `ci` turns, including
unpublished continuations, early stop continuations, and approval resumes that
start a new turn under a running request. Review turns have no running execution
request and are excluded. Every
covered turn receives freshly bound context, including warm runner reuse and
attachment routing. The API verifies the durable WorkItem request and
runtime epoch that authorize the request. Ordinary managed publication outside
a factory execution keeps its existing approval path. This prerequisite makes
no promise of metadata refusal within the same model turn for that path; it
does not invent a generic active turn registry.

The ACI addition is its own reviewed prerequisite. Apply the protocol version
rule in `packages/CLAUDE.md` and regenerate the schema, TypeScript, Rust, and wire
lock artifacts together. Optional context preserves decoding of events outside
this factory capability. A first publication with no existing pull request and
nonfactory, review, nonworkspace, and eval requests have no context and retain
the existing approval path. The additive absent field rule also covers older
producers and consumers; it does not claim that an older runner performs this
check. A current worker must supply context for covered factory turns with an
existing pull request. If minting fails, it stops before `start_turn` and uses
the existing runner error handling; provider failure cannot silently turn
a covered request into an uncovered request. A current runner with absent
context retains the existing path because it cannot independently identify a
covered execution. A present but invalid context is a refusal, never absence.

### A capability authorizes one narrow read operation

Use one new sibling signed credential. Do not widen `sandbox_token`.

| Contract | Decision |
| --- | --- |
| Minter | API, only after authenticating the worker and validating its running execution request and existing open lineage |
| Signing key and format | Existing platform API key as the HMAC signing key through the existing signature primitive; distinct `ppc` token prefix and dedicated verifier |
| Scope | Exactly `publication.precheck`; no state, channel, progress, approval, publication, or credential redemption authority |
| Mint route | Worker authenticated `POST /publications/precheck/context` in `routers/publications.py` |
| Comparison route | Scoped credential authenticated `POST /publications/precheck` |
| Comparison router | `apps/api/src/curie_api/routers/publication_precheck.py` |
| Signed identity | Agent, deployment, canonical storage conversation, WorkItem identifier, execution request UUID, runtime epoch, lineage UUID and version, and accepted head |
| Signed observation | SHA256 digests of exact UTF8 observed title and body, with null body normalized to empty, and observation completion time |
| Signed lifetime | Purpose, issued time, and expiry no later than the execution deadline |
| Correlation | Signed `queued_event_id` from `QueuedTurn.event_id`, for correlation and audit only, not proof of a unique or active Event |
| API authority | Event endpoint origin must match the boot delivered state URL, otherwise the boot delivered progress URL when state is absent; reject redirects |

The API resolves repository and pull request identities from durable platform
state through the signed lineage identifier. Caller supplied names, numbers,
or URLs cannot select another resource. The Event has no generic identifier
field; its timestamp is not a turn identity. A continuation may reuse the queue
event identifier. Binding context to the runner's active Event is local lifetime
enforcement, not a claim the API can independently verify.

The API checks that the execution request is running, its WorkItem ownership
and runtime epoch still match, its execution lease remains valid, and its
lineage identity, version and head still match the claims. It repeats the
ownership, epoch, lease and lineage checks after the provider read before
returning a comparison result. The runner binds the context to the active Event
when a turn starts and clears it when that turn ends. The API does not claim to
observe the runner's private turn identifier or prove that a specific model turn
is still active. A replay within the same valid runtime epoch can perform only
the capped read operation. This remaining replay allowance conveys no mutation
authority and does not survive expiry, lease loss, or epoch replacement. An
approval hold may extend the lease to the execution deadline, so this read
allowance can last for that entire execution window, up to three hours.

The endpoint can inspect the bound lineage and compare a proposed title and body
with current GitHub metadata. It cannot reserve a revision, create or resolve an
approval, redeem a repository credential, edit a pull request, or write durable
publication state. State access tokens do not gain this new scope. The new
capability is rejected by state, approval, and credential endpoints.

GitHub truth comes from a pure helper that performs one pull request GET and
validates its facts without updating lineage state. The precheck does not call
the existing reconciliation helper that initializes heads or marks a lineage
terminal. An inflight push makes the comparison unavailable. Validate the pull
request response against the repository and pull request identities resolved
from durable state, including pull request number and node identity, repository
identity, head repository and branch, base branch, open state, and accepted head.
Do not broaden this operation into additional repository reads or migrate
ordinary callers to stronger credential or identity requirements. Missing
required identity is unavailable for this comparison; ordinary publication
keeps its existing credential and reconciliation behavior.

No GitHub credential or platform key enters the sandbox. The API obtains the
GitHub credential and performs the authenticated GitHub read. The sandbox sees
only the scoped capability and the bounded comparison result. Capability values
are excluded from model messages, transcript content, and logs. Sandbox access
to the existing API host does not open GitHub egress.

### Compare when the tool is called

For a covered factory request, validate the proposed body before capturing a
workspace snapshot. A blank or omitted body returns the nonhalting
`body_required` refusal and asks the model to provide a useful description.
The current worker rejects that body after the turn; this decision moves that
refusal into the turn. Omission does not authorize clearing an existing body.
Other malformed calls retain their existing halting behavior.

Then validate the title and capture the managed workspace using the publication
snapshot rules. Snapshot failure is an actionable nonhalting refusal, never an
empty diff. Independently of whether files changed, local HEAD drift from the
prepared head is an actionable refusal because local commits are not part of
the publishable working tree snapshot.

A valid working tree file change follows the existing mandatory approval gate
without contacting the precheck API. Metadata drift, GitHub unavailability,
and precheck rate limits cannot block that file change at the tool boundary.
Only a valid empty working tree snapshot invokes the API precheck.

For each authorized empty snapshot comparison, the API performs a fresh GitHub
pull request GET. It validates identity and head, compares current metadata
with the signed observation, and then compares the proposed title and body with
current metadata. Before this read, it hashes the Event's plaintext observed
title and checks that hash, its carried body digest, and observation time
against the signed claims. Fresh GitHub title and body digests must match that
signed observation. The Event observation never substitutes for this read. Every
covered empty snapshot remains a nonhalting refusal in issue #3215:

| Outcome | Refusal |
| --- | --- |
| Blank or omitted proposed body | `body_required`, before snapshot capture and any API call |
| Metadata differs from the bound observation | `stale_context`; preserve the external edit |
| Invalid authority, unavailable provider, unavailable budget store, timeout, or an inflight push | `precheck_unavailable`; do not infer no change |
| Neither files nor metadata differ | `no_change`; ask for a working tree file change |
| Metadata differs from the existing pull request | `metadata_only_unsupported`; explain that this publication path still requires a working tree file change |

These refusals leave no pending approval summary, publication payload, halt
marker, or approval grant. The model can supply a nonblank body and a working
tree file change in a corrected call during the same turn. If external metadata
has drifted, the runner does not silently refresh the active Event observation.
A new trusted Event supplies any replacement observation.

The runner comparison has a ten second overall timeout, with the API and
provider budget inside it and no retry that exceeds it. This is below the SDK
control request timeout. The runner permits at most five prechecks per turn,
and the API permits at most twenty per lineage in any five minute window.
Count each authorized attempt before spending GitHub quota, including attempts
that fail. Coalesced observations of one call count once. Rate exhaustion is an
actionable nonhalting refusal and never arms pending state. The API cap uses
shared atomic state; a storage failure cannot bypass the cap.

Issue #3200 changes only the `metadata_only_unsupported` branch to permit the
existing mandatory approval gate after adding metadata only publication support.
It must retain the other refusals and independent admission checks. A successful
comparison never authorizes publication. Issue #3215 proves a corrected call
with a working tree file change through the current publication contract.

### All gate paths share one decision

The stream observer, `PreToolUse` hook, and SDK `can_use_tool` permission
callback use one asynchronous publication decision owned by the runner. Key
the decision by the factory execution context and exact tool call identity so
concurrent observation of the same call shares its result. A corrected later
tool call obtains a new decision and, if its snapshot remains empty, a fresh
comparison.

Use `ToolUseBlock.id` from the stream, the `tool_use_id` argument delivered to
the `PreToolUse` hook, and `ToolPermissionContext.tool_use_id` delivered to the
SDK permission callback as that shared call key. The stream translation must
retain both the identifier and input for each publication call; retaining only
the input loses the identity needed to share the decision. A real SDK test must
prove that these identifiers agree for one call and that the shared decision
works when the stream arrives before hooks. Missing or conflicting identifiers
must refuse the covered request rather than infer identity from arguments.
Matching only arguments is insufficient because a later identical call can
observe changed GitHub metadata. The SDK MCP handler receives arguments only
and has no call identifier. It does not participate in the shared decision;
if hooks are skipped, it returns its existing generic refusal. The fake model
path supplies `block.id` in `ToolPermissionContext` so the same decision
identity applies there.

Retain the identifiers of calls refused by this precheck and exclude them from
the final unrecorded publication check. A turn containing only refused calls
may end `DONE` without pending approval or a runner failure. If a factory model
ends after that refusal, the WorkItem can terminate with `no_pull_request`.
The refusal does not automatically start an unpublished continuation.

Only a decision requiring approval may populate pending publication state.
Each of the three decision paths applies the same continuing refusal for
every covered empty snapshot proposal and the local body and HEAD refusals.
The stream observer must await that decision before recording pending state,
even when the real SDK exposes the call before hooks.
The decision preserves the existing prohibition on granting the publication
tool authority to execute a GitHub side effect.

### Admission independently checks again

The precheck improves the model interaction. It is not trusted evidence for
approval admission. After the turn, the worker validates the actual workspace
snapshot and the API retains the existing independent lineage ownership,
expected head, and revision version checks before creating the Approval and
Publication transaction. This prerequisite does not add empty patch admission
or change ordinary publication callers.

The current schema and worker validator require a nonempty patch. Semantic
unchanged proposal rejection and metadata only admission belong to issue #3200;
a schema rejection is not proof of that later behavior. Issue #3200 must also
repeat current metadata checks at admission so an edit between the precheck and
admission cannot be overwritten. This Draft does not claim that behavior exists
on the current base.

### Intended realizing paths

The coordinated implementation must name its final symbols and record explicit
maintainer approval before changing this ADR to Accepted. The intended paths
are:

1. `packages/aci-protocol/src/aci_protocol/events.py` and
   `packages/aci-protocol/src/aci_protocol/version.py` for the optional Event
   context and protocol version, with generated artifacts under
   `packages/aci-protocol/schema` and `packages/aci-protocol/generated`.
2. `apps/worker/src/curie_worker/kernel.py` for trusted turn context delivery
   and its worker authenticated API client for requesting context. The API owns
   minting and verification in `publication_precheck_token.py`; the worker never
   signs this credential.
3. `apps/api/src/curie_api/routers/publications.py` for worker authenticated
   context minting, `apps/api/src/curie_api/routers/publication_precheck.py` for
   scoped comparison, and `apps/api/src/curie_api/publication_truth.py` for pure
   factory truth validation. `apps/api/CLAUDE.md` records the new scoped
   router exception. Existing ordinary reconciliation keeps its auth contract.
4. `runner/src/curie_runner/session.py` and
   `runner/src/curie_runner/approval.py` for per turn context reset and the one
   shared decision consumed by the real stream observer and permission hooks;
   `runner/src/curie_runner/translate.py` for retaining `ToolUseBlock.id` with
   each publication input so it joins the hook identifier and
   `ToolPermissionContext.tool_use_id`, and
   `runner/src/curie_runner/fake.py` for the same identity on the fake path.
5. Existing publication admission remains authoritative. Metadata only
   admission changes in `apps/api/src/curie_api/routers/approvals.py` and
   `apps/api/src/curie_api/crud.py` belong to issue #3200.

The prerequisite introduces the versioned carrier and its security contract.
Dependent behavior in issue #3200 starts only after the prerequisite has landed
through its own review. This Draft claims neither implementation nor acceptance.

## Consequences

The model can correct an empty factory publication request without leaving its
turn. Warm and newly claimed runners receive context for the same bounded
factory execution authority. GitHub credentials remain within trusted platform
components, and approval admission remains authoritative. Ordinary managed
publication retains its existing behavior until a separate decision supplies
an equally verifiable execution boundary.

An empty file proposal adds a bounded API and GitHub read to tool handling. The
implementation must define timeouts and response bounds and return a useful
tool refusal when verification fails. It must not record a pending approval as
an error recovery shortcut.

Verification must exercise the real stream ordering as well as hooks, prove
that an unchanged request leaves no pending state, and prove that a corrected
call can request approval in the same turn. Scope, expiry, cross conversation,
stale execution, replaced runtime epoch, expired lease, warm runner reset,
continuation coverage, and concurrent call tests establish the capability
boundary. Tests also cover the per turn and per lineage caps, refused only
`DONE`, factory termination without an automatic continuation, local HEAD drift,
blank and omitted body refusal, and other unchanged malformed call behavior. A
metadata race between the carried observation and tool call must trigger a
stale refusal after fresh comparison for an empty snapshot. File change tests
prove that this comparison is bypassed while the local HEAD check remains. The
metadata race at admission is a proof obligation for issue #3200. These are
required proof obligations, not completed results.

## Alternatives considered

### Refuse empty snapshots locally until metadata publication lands

Rejected because it cannot prove the scoped credential, runtime authority,
provider read, or stale observation behavior through the live boundary. Issue
#3215 performs the fresh comparison and returns distinct actionable refusals
while the current publication contract still requires files. Issue #3200 later
opens the changed metadata branch through approval.

### Treat the carried observation as current GitHub truth

Rejected because an external edit can invalidate an observation before the tool
call. The versioned Event carries observed metadata and freshness for explicit
identity and audit context, while the API comparison establishes current facts.
An initial checkout or boot environment also cannot refresh every warm turn.

### Infer metadata from transcript history

Rejected because history can be incomplete and records prior outcomes rather
than current GitHub truth. A model statement cannot establish authority.

### Let the runner query GitHub with a platform credential

Rejected because the sandbox would gain repository authority or a platform key.
The scoped API comparison preserves the credential isolation established by
ADRs 0033 and 0125.

### Extend the existing state token

Rejected because state access and publication comparison are distinct
capabilities. Combining them silently widens every existing state credential.

### Reject only when the worker creates approval

Rejected as the sole check because it runs after the model turn has ended. It
remains necessary as the independent authority check.

### Add the refusal only to a permission hook

Rejected because the stream observer can record pending publication state
first. Independent checks at several interception points can also disagree
after a GitHub edit. One decision per exact tool call covers every path.
