# 125. Managed repository workspaces and approval-gated publication are platform capabilities

Date: 2026-08-23

Status: Accepted

**Partially superseded by [ADR 0106](0106-an-approver-is-an-authenticated-principal.md).**
ADR 0106 supersedes only this ADR's publication-only requester exception and
universal self-block premise; publication remains approval-gated under the
selected approver set's authenticated-principal membership rule.

**Superseded in part by [ADR 0256](0256-publication-validates-and-pushes-a-git-object-overlay.md)**
(back-link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the worker clause that validates the binary patch, base commit, repository,
paths, and the private base archive, and the Job clause that clones, branches,
safely applies, commits, and pushes. The privately held base, the approval
gate, and the rest of this decision stand.

Maintainer approval supplied for this decision on 2026-08-20 authorizes ADR
0125 to land Accepted with its realizing code paths in this PR.

## Context

Curie can run a versioned bundle in an isolated sandbox, but a development
session also needs the source repository it is meant to change. Leaving clone
and publication mechanics to each bundle duplicates privileged plumbing and
puts the most sensitive boundary in prompt-controlled code.

A live cluster exercise demonstrated the concrete failure mode. A clone URL
containing a write-scoped credential persisted in `.git/config`; code running
inside the sandbox read it and opened a pull request without passing through the
platform approval plane. Supplying a credential only for `git clone` is not
enough: Git records the authenticated URL unless the platform immediately
replaces and verifies the stored remote.

The same exercise established three operational constraints. Slack dispatch is
asynchronous, so the session thread, not a synchronous command response, is the
result surface. Sandbox egress is fail-closed, so a checkout cannot depend on
direct GitHub access after claim. Publication workloads are outside the sandbox
NetworkPolicy selector and can reach GitHub without widening sandbox egress.
Finally, repository handling must fail at the named stage when `git` is absent;
the shipped worker and runner images therefore include it.

[ADR 0092](0092-a-github-app-gives-the-platform-its-own-repository-identity.md)
already chooses the operator-owned GitHub identity: prefer a short-lived App
installation token and fall back to the configured raw token. This decision
extends that identity to managed development workspaces and approved
publication; v1 deliberately does not map Slack users to GitHub users.

## Decision

**Repository workspace preparation and publication are platform capabilities,
configured on a deployment and consumed by bundles.**

A Deployment stores a nullable canonical `owner/repository` workspace. Deploy
commands expose three intentions: `--workspace` enables it and derives the
repository from explicit `--repo` then the persisted agent binding;
`--no-workspace` disables it; omission carries the prior active deployment's
value. The plugin format is unchanged.

Before claiming or resuming a sandbox, the trusted worker redeems a
deployment-derived credential over a dedicated worker-authenticated, no-store
API endpoint. The API derives the clean URL and repository from durable state;
the caller cannot choose either. The worker performs a bounded, full-blob,
depth-one clone with header-based authentication and redirects disabled. It
immediately runs `git remote set-url origin` with the clean URL, verifies both
the remote and `.git/config`, normalizes and bounds the archive, and streams it
to a private workspace bucket.

The sandbox never receives GitHub or bucket credentials. A claim contains only
an opaque, short-lived, one-object signed read capability and the archive
digest. That capability is necessarily visible in plaintext claim state, which
is an accepted bounded tradeoff: it expires quickly, names one immutable object,
and grants neither listing nor access to another object. Credentialless init
stages stream, hash, and extract the archive into a dedicated bounded
`/workspace` volume. GitHub egress remains closed to sandbox pods.

Curie owns a built-in `mcp__curie__publish_changes` tool. When a repository
workspace is mounted, this tool is an additive mandatory permission gate that a
bundle or operator policy cannot remove. It never performs publication and is
never given a one-shot grant. Its sole successful outcome is a trusted denied
provenance that causes the platform to snapshot the workspace after the turn.

The worker validates the binary patch, base commit, repository, paths, and the
private base archive before one database transaction creates both Approval and
Publication. The Publication row is also a leased approval-card outbox: the
worker posts and records the card independently of the session turn, so a Slack
retry never reruns the repository work that produced the patch. Patch bytes are
private durable state and never appear in list or read responses. Publication
approvals are a distinct server-owned purpose. The requester may approve their
own publication only when the existing authorizer also proves channel
membership; ordinary approval self-blocks are unchanged. The publication
requester exception is audited.

Resolving a publication approval performs durable compare-and-set state changes
only. It never enters the ordinary resume, owed-wake, grant, dead-letter reopen,
or expiry wake paths. This is required because the platform, rather than a new
model turn, reports the outcome into the stored reply route.

After approval, a trusted worker reconciler claims Publication rows with a
lease and version compare-and-set. It creates deterministic, owner-referenced
ConfigMap, Secret, and Job objects. The Job uses the shipped runner image and a
tokenless, no-RBAC ServiceAccount; mounts the patch through ConfigMap
`binaryData`; clones, branches, safely applies, commits, and pushes using a
file-backed askpass credential; then opens the pull request with GitHub REST.
It has `backoffLimit: 0`, never needs `gh`, keeps a clean remote, redacts logs,
and emits one `CURIE_PR_URL=` marker. Deterministic branch lookup adopts an
existing pull request after a lost marker or retry instead of creating a
duplicate. Denial redeems no credential and creates no publication resource.

Publication is cluster-only in v1. A local request fails before durable approval
creation. The default installation supplies the dedicated worker-auth token,
workspace storage and resource bounds, publication RBAC, the tokenless Job
identity, and the publication owner anchor. `api.githubToken` remains the raw
token home and the App settings remain preferred when configured.

The realizing paths are:

- deployment, publication, credential isolation, audit, and approval policy in
  `apps/api/src/curie_api/models.py`, `crud.py`, `repository_credentials.py`,
  `routers/workspaces.py`, `routers/publications.py`, `routers/approvals.py`, and
  `authorizer.py`;
- trusted clone, archive, signed-reference delivery, snapshot validation, and
  publication reconciliation in `apps/worker/src/curie_worker/workspace.py`,
  `sandbox/docker.py`, `sandbox/k8s.py`, `kernel.py`,
  `publication_validation.py`, `publication_store.py`, `publication_k8s.py`,
  and `publication_loop.py`;
- the mandatory tool and snapshot endpoint in
  `runner/src/curie_runner/approval.py`, `workspace_snapshot.py`, and
  `server.py`;
- operator wiring in `cli/src/main.rs`, `cli/src/api.rs`,
  `cli/src/commands.rs`, and `charts/curie`; and
- the minimal consumer in `examples/coder`.

## Consequences

An operator can install Curie, connect Slack, deploy the coder example with a
repository workspace, steer a change in its thread, approve the publication
card, and receive a pull-request URL in that thread. A bundle owns the behavior
of the agent but no privileged repository lifecycle.

The platform becomes responsible for private workspace-object retention,
publication patch retention, GitHub REST compatibility, and crash-safe Job
reconciliation. Clone work has explicit concurrency, time, size, member,
compression, and ephemeral-storage bounds so one repository cannot silently
starve the worker or node.

Requester self-approval is narrower than universal self-approval: it applies
only to server-linked publication records and still requires membership. This
optimizes the development loop without weakening unrelated approval gates.

The operator credential is the pull-request author in v1. Per-user GitHub
identity and attribution can be added later without moving clone or publication
back into the sandbox.

## Alternatives considered

### Put clone and publication scripts in the coder bundle

Rejected. It duplicates orchestration in every adopting bundle and makes a
prompt-controlled sandbox responsible for credential handling and approval
enforcement.

### Give the sandbox a GitHub credential

Rejected. The observed `.git/config` persistence turns a transient clone secret
into a readable write capability. Removing GitHub egress does not make storing
the credential acceptable, and widening egress would weaken the fail-closed
boundary.

### Resume the agent after publication approval

Rejected. Slack dispatch does not wait synchronously, and a resumed turn is both
unnecessary and a source of duplicate effects. The publication worker can report
the deterministic result directly to the original route.

### Require a different person to approve publication

Rejected for the default publication policy. A focused development request is
valuable precisely as a short message-to-pull-request loop. Membership remains
the authorization boundary, and other approval purposes retain their structural
self-block.

### Add `git` only to an operator-maintained derived image

Rejected. Managed workspaces and publication are default platform capabilities;
a plain install must contain their required binary. Both shipped images include
`git` and fail loudly if it is unavailable.

### Use `gh` in the publication Job

Rejected. Git and an HTTPS client already provide the required operations. The
GitHub REST call and deterministic-head recovery keep the Job image and retry
behavior explicit.

### Publish locally in v1

Rejected. The Kubernetes Job, dedicated identity, owner reference, resource
envelope, and egress separation are the first production boundary. A local
equivalent would be a second side-effect implementation before the cluster path
has established the contract.
