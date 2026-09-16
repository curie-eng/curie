# 137. Coding tools are built in and an initial repository URL selects the workspace

Date: 2026-08-31

Status: Draft

This Draft records maintainer direction from 2026-08-31 for discussion. It does
not authorize implementation; acceptance remains a separate maintainer action.

## Context

[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)
made repository preparation and approval-gated publication platform
capabilities, but described those capabilities as configured on a Deployment
and named `examples/coder` as their minimal skill consumer. [ADR
0126](0126-runtime-repository-selection-is-sticky-authorized-thread-state.md)
then made `workspace_enabled` the deployment-time switch for coding enablement
and allowed a thread to select an authorized repository at runtime.

Those choices accidentally present coding as a bundle feature. A deployment
must opt in before the worker considers a repository fact, and the example
bundle ships instructions which appear to provide coding tools that the runner
and control plane already own. Copying that skill into another bundle would
make privileged platform behavior look prompt-defined and create divergent
copies of the publication protocol.

The platform boundary is already stronger than that packaging suggests. Claude
Code file operations execute in the isolated sandbox without a GitHub
credential. Curie's publication tool is only an approval request; trusted
components capture, validate, and publish changes after a human decision. The
worker alone may acquire a repository credential, sanitize the checkout, and
transfer a bounded workspace into the sandbox. Coding enablement can therefore
be a uniform session capability without moving publication authority or a
credential into model-controlled code.

[ADR 0136](0136-a-late-workspace-handoff-replaces-the-sandbox-at-a-fenced-turn-boundary.md)
specifies the larger same-thread case: a conversation which already has a live
generic sandbox may later acquire a workspace only through a cold replacement
at a durable, fenced turn boundary. The initial-claim path does not need that
replacement protocol and should not be coupled to its implementation.

## Decision

**Coding tools are a built-in Curie session capability. An allowed repository
URL in the initial message selects and acquires a managed workspace at claim
time; deployment workspace flags do not enable or disable that capability.**

### Every session exposes one platform-owned coding surface

The runner always enables the Claude Code file-tool preset and always registers
`mcp__curie__publish_changes`. Their tool descriptions state the operating
protocol at the point of use: work happens under `/workspace` when a managed
repository is mounted, existing changes must be preserved, the sandbox must not
push, publication requires a human approval, and the turn ends with publication
pending. The SDK-owned file-tool descriptions remain the authority for each
file operation; the Curie-owned publication description states the boundary
between editing and publication.

Discoverability is not publication authority. Without a managed workspace, a
call to `mcp__curie__publish_changes` returns a useful refusal and creates no
approval or side effect. With a managed workspace, the tool remains an additive
mandatory permission gate which bundle policy cannot remove or preauthorize.
The call still only records denied provenance for trusted post-turn handling.
Publication remains mount-keyed, human-approval-gated, and implemented outside
the sandbox with a tokenless publication identity. Sandbox processes receive no
GitHub, worker, or object-store credential.

### The initial message can select the claim-time workspace

Before an initial sandbox claim, the trusted worker parses at most one canonical
root `https://github.com/owner/repository` URL from the raw message and asks the
worker-authenticated API to establish or read the durable thread selection.
Canonicalization, sticky first-to-establish-wins semantics, author attribution,
and whole-value allowlist enforcement remain those of ADR 0126.

A single allowed repository fact causes the worker to redeem a clone credential,
prepare and verify the sanitized archive, and include the managed workspace in
the initial claim. The sandbox receives only the bounded signed workspace
reference and digest. Ambiguous or unallowlisted repository facts are refused
before model execution, sandbox claim, credential redemption, or workspace
preparation. A malformed or non-root GitHub link is not a repository fact.

When neither the message nor durable thread state contains a repository, the
worker performs an ordinary generic claim. It does not redeem a clone
credential, prepare an archive, or touch workspace object storage. A durable
authorized selection may be reused after route loss without requiring the URL
to be repeated.

The worker-wide workspace coordinator configuration remains the installation
boundary for the trusted acquisition machinery. Per-deployment
`workspace_enabled` is not a coding feature gate. Existing `--workspace` and
`--no-workspace` deploy options remain accepted as deprecated compatibility
no-ops so older automation does not fail at argument parsing, but neither option
changes claim routing, tool discovery, repository authorization, or publication
authority.

### The coder example no longer owns enablement

`examples/coder` remains a valid, skill-less example bundle. Its coder skill is
retired rather than copied, composed, or merged into `examples/sre-bot`.
Bundles may still supply task-specific behavior, but no bundle claims to enable
the platform's file tools, repository acquisition, publication protocol, or
approval boundary.

### Late acquisition remains a fenced handoff

This decision realizes only the initial-claim slice: a repository fact which is
available before the first claim can select and prepare that claim's workspace.
It does not authorize hot mounting, steering a repository-bearing message into
an already-running generic sandbox, or replacing a live route without a fence.
The same-thread late-remount and run-without-workspace successor remain the
workspace-handoff implementation specified by ADR 0136.

Until that implementation exists, a repository-bearing delivery for an
already-running generic route fails closed before selection, credential
redemption, workspace preparation, claim, steer, or model execution. This
preserves the existing route and does not silently create a second logical
conversation.

### Scope of the proposed supersession

If accepted, this ADR supersedes only these parts of the earlier decisions:

- ADR 0125's skill-consumer packaging and deployment-intent language, insofar
  as they make `examples/coder` or a deployment workspace flag the source of
  coding enablement;
- ADR 0126's use of `workspace_enabled` as the deployment capability gate; and
- ADR 0136's statement that `workspace_enabled` declares the capability.

ADR 0125's managed checkout, credential isolation, mandatory mount-keyed
publication gate, approval flow, and tokenless publication boundary remain in
force. ADR 0126's canonical parsing, durable sticky selection, allowlist,
authorization, credential, and publication checks remain in force. ADR 0136's
late same-thread remount, terminal-boundary checks, cold replacement, route
metadata, fencing, and crash recovery remain in force.

The existing ACI and plugin-format interfaces are sufficient. This decision
authorizes no change to `packages/aci-protocol` or `packages/plugin-format`. If
realization requires a frozen-interface change, work must stop under the
frozen-contract rule.

The initial-claim realization accompanying this Draft is tracked by #2154. Its
presence does not accept this ADR or make the Draft an implementation
authority. The late fenced handoff remains separate follow-up work under ADR
0136.

## Consequences

Every agent session presents a consistent editing and publication surface.
Users do not need to know which example skill a bundle contains or whether a
deployment flag was set before naming an authorized repository in an initial
request. Bundles cannot weaken the approval boundary by omitting the tool and
cannot strengthen their own authority by preauthorizing a lookalike skill.

Tool availability and tool usability deliberately differ. Generic sessions can
inspect and modify their ordinary sandbox files, but publication refuses until
a trusted managed workspace is mounted. This makes the security boundary
observable in the tool result rather than hiding the tool based on mutable
bundle or deployment configuration.

An initial request without a usable repository stays cheap and generic. An
unauthorized or ambiguous repository still fails before any credential or
sandbox resource is acquired. Operators continue to control repository scope
through the installation's deny-by-default allowlist and trusted workspace
coordinator, not through deployment metadata.

Keeping deprecated deploy flags temporarily avoids an argument-level breaking
change, but their inert behavior must be documented and tested. They can be
removed in a later explicit CLI compatibility decision.

This change does not solve late workspace acquisition. Until the fenced handoff
lands, an existing generic route cannot consume a later repository-bearing
message. That limitation is explicit and fail-closed rather than approximated
with a hot mount, credential injection, or untracked replacement.

## Alternatives considered

### Keep `examples/coder` as the coding feature switch

Rejected. File operations, workspace acquisition, and publication enforcement
are platform capabilities. Presenting a skill as their source duplicates
protocol prose and lets bundle composition appear to grant privileged behavior.

### Merge the coder skill into `examples/sre-bot`

Rejected. It would preserve the same false ownership boundary in a larger
consumer and couple SRE behavior to a generic platform capability.

### Continue gating tools and selection on `workspace_enabled`

Rejected. It makes deployment configuration, rather than the presence of a
trusted managed workspace and an allowed repository fact, decide whether
sessions can discover the platform surface. Publication authority is already
correctly keyed to the mounted workspace.

### Hide the publication tool on generic sessions

Rejected. Conditional discovery recreates the configuration-dependent tool
surface this decision removes. An explicit, side-effect-free refusal accurately
communicates the missing managed-workspace precondition.

### Treat every GitHub-looking link as a repository request

Rejected. Only one canonical root URL is an actionable repository fact.
Ambiguous and unallowlisted facts must fail closed; malformed and nested links
must not accidentally trigger credential redemption.

### Hot-mount a repository into an already-running sandbox

Rejected. It bypasses claim-time verification and the fenced replacement
boundary. ADR 0136 retains authority over late same-thread acquisition.
