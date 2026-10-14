# 190. Automated hook sources cannot widen their tool access

Date: 2026-10-02

Status: Accepted

**Partially superseded by [ADR 0203](0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md)**
(back link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
its premise that an automated source changes no system no longer holds for a
hook an administrator binds a remediation policy to. The model turn keeps
every HOOK-SOURCE-POLICY invariant.

Tracked in [#3603](https://github.com/curie-eng/curie/issues/3603).
This acceptance establishes the source authority and fail-closed invariants
below. It does not claim that they are implemented. Concrete configuration
and fencing mechanisms remain deferred under #3603; affected implementation
must first publish their reviewed contract.

This ADR supersedes the ordinary-tools authorization in
[ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)
only for hooks an operator explicitly restricts. For credentials used by
restricted sources it also amends the per-agent hook-secret boundary in
[ADR 0079](0079-inbound-triggers-as-a-new-event-kind.md).
It preserves ordinary
unconfigured hooks, human turns and their existing approval flow. It builds
on the TOOL-ACCESS contract in
[the ACI producer interface](../interfaces/aci-producer/INTERFACE.md).

## Context

An automated source should inspect an alert without changing a system or
requesting approval. A prompt that tells the model to avoid those operations
is not a permission boundary.

The v0.12.0 API already accepts a signed caller request for
`tool_access=read-only`, queues it and echoes it in its receipt.
`apps/api/src/curie_api/hook_signing.py` binds the decoded hook name and
requested access to the signature. This prevents changing a captured
request's policy. It does not prevent a source holding the shared secret
from signing a new unrestricted request.

An implementing worker checks the exact target runner's advertised access
before opening a restricted turn. The API does not exclude an older worker
that can consume the same stream and ignore the optional field. An API
schema, version string or echoed receipt proves neither that exclusion
nor runtime enforcement.

The example email intake checks explicit reply coordinates at startup and
in the receipt. Its policy is optional in the hook URL; it does not require
`read-only` or verify that policy in the receipt.

## Decision

### Source authority

<!-- @spec HOOK-SOURCE-POLICY-1 -->
An administrator may bind a mandatory `read-only` policy to a named hook
on an agent. Only the existing platform administrative authority can
configure that binding. The delivery body, author label and source's ingress
credential cannot configure or remove it. A restricted source must
authenticate with authority bound to its permitted hook names; that same
credential must not authorize an alternative unrestricted hook. The shared
per-agent secret alone cannot establish source-specific authority. Existing
unconfigured sources retain their current credential behavior.

<!-- @spec HOOK-SOURCE-POLICY-2 -->
A restricted hook runs with `read-only` even when its caller omits a
restriction. A caller cannot request a wider policy. The API verifies the
signature against the caller's actual requested value, then derives and
queues the effective server policy. Receipts and duplicate-delivery checks
must distinguish requested and effective policy without treating an old
unrestricted delivery as a newly restricted one.

<!-- @spec HOOK-SOURCE-POLICY-3 -->
An unconfigured hook retains today's optional signed caller restriction.
Ordinary human turns keep their existing tools and approval flow. The
restriction follows the turn through queueing, retry, cold boot, resume and
session reuse, without carrying it into a later unrestricted human turn.
Reuse the existing ToolAccess enum and worker/runner enforcement contract.

### Admission and execution

<!-- @spec HOOK-SOURCE-POLICY-4 -->
Before claiming or queueing a restricted hook delivery, the platform must
prove that its consumption boundary excludes incompatible or unregistered
workers. A paused, restarted or newly joined old worker must not gain access
after admission. Missing, stale or inconsistent evidence refuses admission
without a delivery claim or queue entry. Admission must also prove that
every runner artifact eligible for this delivery supports enforcement, and
bind that checked selection so incompatible artifacts cannot be substituted
after enqueue. A worker heartbeat or consumer snapshot alone cannot provide
these guarantees. An unsupported runner combination refuses before enqueue;
the later exact-runner check is defense in depth, not its substitute.

<!-- @spec HOOK-SOURCE-POLICY-5 -->
The worker still checks enforcement on the exact selected runner before
opening the model turn. A protected queue does not imply that its runner
can enforce access. Incompatible artifacts fail without model or tool
execution; no ordinary queue fallback is allowed.

<!-- @spec HOOK-SOURCE-POLICY-6 -->
The intake requests and signs `read-only`, checks supported admission before
its first delivery and verifies the effective policy in every receipt.
The check performs no Slack post, hook claim or turn enqueue. A schema
parameter check may supplement it but cannot stand in for admission proof.

<!-- @spec HOOK-SOURCE-POLICY-7 -->
A read-only automatic turn may inspect and explain. Mutating and unknown
tools, built-in mutation paths and approval requests are refused before an
effect or approval card. Executable Skill calls are not admitted by their
name or purpose. If the SRE instruction path requires Skill execution,
[#3610](https://github.com/curie-eng/curie/issues/3610) must prove static
loading while preserving refusal of dynamic commands and forked work.

<!-- @spec HOOK-SOURCE-POLICY-8 -->
Receipts and telemetry distinguish admission refusal, execution-policy
refusal, a completed investigation and delivery failure. Positive and
negative observations cover the actual ingress, worker and runner, including
unsupported artifact combinations and an ordinary human turn afterward.
No receipt alone closes the execution or delivery criterion.

## Deferred mechanism selection

The accepted boundary is operator-controlled source authority, exclusive
consumption by compatible workers and checked runner selection. The exact
configuration surface, source credential format, worker membership proof,
stream access control and rolling-upgrade protocol remain open design work
under [#3603](https://github.com/curie-eng/curie/issues/3603), owned by its
author jw3329 with API and worker maintainer review. Candidate mechanisms
remain proposals until that contract is reviewed; this ADR selects neither
a guessed marker nor a queue topology.

The interim behavior is unchanged: installations requiring these guarantees
must not install the automated intake. The realizing API admission and worker
consumption paths remain unimplemented. Before their implementation opens,
#3603 must record the chosen configuration and consumption contracts and the
actual ingress, worker and runner paths that realize them. Acceptance of
these invariants does not close the issue or clear the installation gate.

## Alternatives considered

- Source policy keyed only by the requested hook with a shared per-agent
  secret: protects that route, but a restricted source may sign a request to
  another unrestricted hook. Source authority must not span that alternative.
- Signed caller opt-in alone: already supported and useful, but a source
  holding its secret can omit the restriction. It cannot enforce operator
  source policy.
- Prompt instructions: permit a model mistake or injected data to reach
  ordinary tools. They do not enforce the required no-effect boundary.
- Check only the runner after enqueue: protects turns selected by an
  implementing worker, but an old consumer may ignore the optional field.
- Fresh worker capability leases or a fleet snapshot: prove recent presence,
  not exclusive future consumption. A paused or newly joined old worker can
  still acquire a restricted turn.
- Dedicated restricted stream: a possible realizing mechanism, but its name
  alone does not fence consumers. Maintainers must choose and review its
  access boundary and scheduling implications before implementation.
- Restrict every hook: changes existing ordinary hooks and their approvals,
  instead of the sources an operator intentionally restricts.

## Consequences

The intake remains uninstalled where runtime read-only guarantees are
required until the source policy, consumption fence and execution evidence
are complete. Partial producer adoption may be reviewed separately and
must not close #3603 or claim to clear that installation condition.

Adding source-policy state and a consumption fence may require additive
schema and deployment changes. Released schema windows remain immutable.
Existing optional policy and human behavior remain covered by regression
observations throughout a rolling upgrade.
