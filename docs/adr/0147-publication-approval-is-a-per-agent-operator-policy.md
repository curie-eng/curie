# 147. Publication approval is a per-agent operator policy, not a platform constant

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

**Amends [ADR-0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)**
(managed repository workspaces and approval-gated publication are platform
capabilities). It supersedes in part exactly one clause of 0125's Decision:
that `mcp__curie__publish_changes` "is an additive **mandatory** permission gate
that a bundle or operator policy cannot remove." Everything else in 0125 stands
unchanged — the worker-owned clone, the credential the sandbox never holds, the
remote-URL reset and verification, the snapshot validation, the publication Job
outside the sandbox NetworkPolicy, the repository allowlist checked twice, and
the audited credential redemption. The gate itself is not removed by this
decision. What becomes policy is who resolves it.

Under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
the back-link on ADR-0125 is written when this decision is Accepted, not while
it is Draft.

## Context

ADR-0125 put the most sensitive boundary in the system outside prompt-controlled
code, and it did so on evidence: a clone URL carrying a write-scoped credential
persisted in `.git/config`, code inside the sandbox read it, and a pull request
was opened without passing through the approval plane. Everything 0125 decided
follows from that incident, and the mandatory gate was the belt to the
credential boundary's braces.

The gate is unconditional today, structurally rather than by configuration. The
runner adds `publish_changes` to the gated set whenever a managed workspace is
mounted (`runner/src/curie_runner/approval.py:1409-1414`), a `Publication` row
is created only alongside an `Approval` whose `purpose` is publication, and the
credential endpoint refuses to redeem for a publication that is not approved,
launching, or running (`apps/api/src/curie_api/routers/publications.py:409`).
`Publication.approval_id` is `NOT NULL` and unique
(`apps/api/src/curie_api/models.py:534`). There is no bypass flag anywhere in
the tree, which is exactly what 0125 intended.

Headless ticket-to-pull-request execution needs a pull request to open without a
person clicking a card. The existing escape hatch is a scripted operator
principal: mint one, poll for pending approvals, resolve the publication rows.
CI already does precisely this in the end-to-end demo script. That works, and it
is the wrong shape to ship. It moves the decision out of the platform into a
loop someone runs beside it, and the audit trail then records a principal that
is really a cron job wearing an operator's name. A dark factory does not need a
bypass flag; it needs a decision about what replaces the human, made in the open
and recorded where an auditor will find it.

The premise that has changed since 0125 is not the risk. It is who is exposed to
it. ADR-0125 was written for an interactive agent steered from a Slack thread by
whoever happened to be in the channel, where the approving human is the only
thing standing between a conversational prompt and a push. A headless factory
agent is deployed by an operator, against a repository the operator allowlisted,
from tickets the operator's own team filed, running a bundle the operator
installed. In that configuration the approval card is asking a person to
re-consent, one pull request at a time, to a thing the operator already
consented to at install. That is not a safety property; it is a rate limiter on
a decision already made.

Meanwhile every guardrail that does not depend on a human is untouched by this
and stays: the sandbox holds no credential and cannot push, the patch is
validated against a privately-held base, `.github/workflows` edits are refused,
patches cap at 900 KB, the repository must be on the allowlist at clone *and*
again at redemption, the Job runs outside the sandbox NetworkPolicy with a
tokenless ServiceAccount, and every redemption is audited. The output is a pull
request, which is itself a review object a human must merge.

## Decision

**Whether a publication requires a human decision is per-agent operator policy.
An agent's `publication` policy is `approve` (the default, and today's
behaviour) or `auto`. Under `auto` the platform resolves the publication
approval itself, under a recorded policy, and every other publication guardrail
applies unchanged.**

### The policy lives on the agent row

`publication` joins the operator-owned per-agent policy already on `agents`:
`model`, `thinking`, `approval_required_tools`, `approval_routes`,
`hook_partitions`, `max_usd_per_day`. It is operator state, set at deploy. **A
bundle has no surface for it at any tier**, which is the part of ADR-0125's
clause that this decision keeps rather than supersedes: prompt-controlled code
still cannot reach this, and neither can a bundle author. Only the operator who
installed Curie and allowlisted the repository can turn it on, for one named
agent at a time.

The default is `approve`. An install that changes nothing behaves exactly as it
does today, byte for byte.

### `auto` does not mean "no approval"

`Publication.approval_id` is `NOT NULL`, and this decision does not change that.
Under `auto` the `Approval` row is still created, and it is still the object the
credential endpoint checks; what changes is that the platform resolves it
immediately under the recorded policy instead of posting a card and waiting.

This is deliberate and is the crux of the decision. The approval row is the
audit trail: it carries the purpose, the conversation, the resolving principal,
and the timestamp, and the credential redemption audit entry points at it. An
`auto` publication that skipped the row would be a publication with no record of
why it was allowed. Instead it records that it was allowed *by policy*, naming
the agent's policy as the authorizer, and it is distinguishable in the audit
trail from one a person resolved. An operator reviewing a quarter of
publications can tell the two apart.

The consequence is stated plainly: **under `auto`, the platform is the
approver.**
[ADR-0106](0106-an-approver-is-an-authenticated-principal.md) made an approver an
authenticated principal, and a policy
is not a principal. This decision does not pretend otherwise. It says the
operator's act of setting the policy is the authorization, that it is recorded
as such, and that it is scoped to one agent and one allowlisted repository set.

### Every other guardrail is unchanged, and the gate still runs

The runner still registers `publish_changes` as a gated tool, the turn still
ends at the gate, and the snapshot is still validated against a privately-held
base before any durable row exists. `auto` changes what happens *after* the
`Approval` and `Publication` rows are written, and nothing before it. A patch
that fails validation, touches `.github/workflows`, exceeds the size cap, or
names a repository outside the allowlist is refused under `auto` exactly as it
is under `approve`.

The two additional bounds an `auto` agent may declare are the ones that make an
unattended pull request easier to review rather than harder: the pull request
may be opened as a draft, and its branch may be required to carry a named
prefix. Both are operator-set, both are enforced platform-side, and neither is
required.

### Denial keeps its meaning

Nothing about the denied path changes. A denied publication redeems no
credential and produces no GitHub side effect. Under `auto` there is no denial
path from a human, which is the point; the refusals that remain are the
validation refusals, which are not approvals at all.

## Consequences

An operator can install Curie, allowlist a repository, deploy a factory agent
with `publication: auto`, and receive pull requests without anyone clicking
anything. That is the capability the whole dark-factory proposal rests on, and
it becomes a supported setting rather than a script running beside the platform.

The scripted operator-principal loop stops being necessary, which removes a
pattern where an automated actor holds a human principal's identity. That is a
net improvement to the audit trail even though the decision it enables is a
loosening.

**The blast radius of a compromised bundle grows.** Under `approve`, a bundle
that produced a malicious patch still had to get a human to click. Under `auto`
it does not. What bounds it is the allowlist, the workflow-edit refusal, the
patch cap, the fact that a pull request is not a merge, and the reviewer who
merges it. Anyone turning this on is choosing to rely on that set, and the
setting should say so at the point of use.

**A pull request is now the last human gate, so it has to actually be one.** An
install running `auto` against a repository with permissive merge settings, or
against a branch nothing protects, has no human gate at all. This decision does
not enforce branch protection and cannot; the operator wiring is where that
lives, and the documentation for this setting has to name it.

An `auto` agent produces pull requests at whatever rate its tickets arrive.
Review capacity becomes the constraint the approval card used to be, and it is a
constraint on people rather than on the platform.

The audit trail gains a class of approvals nobody resolved. Any query, report,
or dashboard that assumes an approval has a human principal has to learn about
this class, and the migration adding the policy column has to say what existing
rows mean.

## Alternatives considered

**Keep the gate mandatory and ship the scripted operator-principal loop.**
Rejected as the product answer, though it stays the correct demo answer. It puts
the decision outside the platform where no ADR governs it, records an automated
resolver as a human principal, and gives an operator no way to express "this
agent, this repository, unattended" that the platform can enforce or audit.

**A global install-level setting rather than per-agent.** Rejected. Capacity to
publish unattended should be as narrow as the thing that earned it. Per-agent
means an install can run one factory agent unattended beside interactive agents
that are still gated, which is the configuration a real team wants.

**A bundle-declared setting.** Rejected, and this is the part of ADR-0125's
clause that survives intact. The bundle is prompt-adjacent code; letting it
declare that it needs no approval is exactly the hole 0125 closed.

**Skip the `Approval` row entirely under `auto`.** Rejected. It is simpler and it
destroys the audit trail: a publication with no approval row is a push with no
recorded authorization. Making the row say "authorized by policy" costs one
column and keeps the history answerable.

**A task-class policy — "this kind of ticket needs no approval" — instead of a
per-agent one.** Rejected for v1 because no task-class concept exists in the data
model, and inventing one to carry this would be a larger decision than this one.
Per-agent is the granularity the schema already supports.

**Auto-approve only when the repository is a designated sandbox repository.**
Considered and folded in rather than rejected: the allowlist already expresses
this, and an operator who wants that shape sets the allowlist to that repository.
A second mechanism naming the same thing would drift from the first.

## Realizing code path

Unimplemented. The named paths for the eventual work are the policy column and
its migration in `apps/api/src/curie_api/models.py`, the resolution path in
`apps/api/src/curie_api/routers/publications.py` and `crud.py`, the recorded
authorizer in `apps/api/src/curie_api/authorizer.py` and the approval audit
entries, the deploy surface in `cli/src/main.rs` and `cli/src/commands.rs`, and
the draft-pull-request and branch-prefix bounds in
`apps/worker/src/curie_worker/publication_clients.py`.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act — and for a decision that loosens a boundary
another ADR set on incident evidence, acceptance should be an explicit one.
