# 259. A binding requires HTTP identity when its invocation or approver role needs HTTP facts

Date: 2026-11-02

Status: Draft

Proposes a partial supersession of
[ADR 0254](0254-http-identity-applies-to-the-channel-identities-an-operator-registers.md)
decision 2 and of its note that approval continuity still classifies by
tenant, as stated below.

## Context

ADR 0254 moved turn admission from a tenant-wide rule to a per-binding one. A
binding in a tenant with an HTTP selection requires HTTP identity when its
channel identity is registered, or when its agent's invocation role is
migrated or carries an agent-owned filter. Approval continuity was left on the
tenant-wide rule: in an adopted tenant every approval is HTTP-selected, so an
unregistered binding can start a turn but cannot create a provenance-free
approval.

Moving approvals to the per-binding rule is the obvious next step, and it has
a trap. The ADR 0254 test reads only the invocation role. An agent can leave
invocation open and still restrict who may approve, with a migrated approver
role or an approver filter that only HTTP facts can evaluate. Under an
invocation-only test, such an agent's approvals on an unregistered binding
would be classified legacy. The legacy approval checks never read an HTTP
approver filter, so any actor the legacy card rules admit could answer,
including people the filter excludes. Restricting approval is the reason the
filter exists, so this would fail open on the one thing it protects.

## Decision

### 1. One classification, two roles

A binding in a tenant with an HTTP selection requires HTTP identity when any
of these holds:

- its channel identity is registered (ADR 0254 decision 2, unchanged);
- its agent's invocation role is migrated or carries an agent-owned filter
  (ADR 0254 decision 2, unchanged);
- its agent's approver role is migrated or carries an agent-owned filter.

A missing policy row requires nothing extra. A stored policy that cannot be
read counts as requiring HTTP identity. A tenant without an HTTP selection is
unaffected.

### 2. Turns and approvals use the same answer

The API's single per-binding classification serves turn admission, the
worker's question about a context-free turn, and approval continuity. There is
no second approval-only test that could disagree with it.

### 3. Approval continuity classifies by binding

An approval is HTTP-selected when it carries a sealed identity context, or
when any binding its reply route resolves to requires HTTP identity under
decision 1. Every route candidate is classified, so an ambiguous route cannot
hide a selected one. A classification that cannot be completed refuses with
the fixed unavailable response; it never selects the legacy path.

Every other approval in an adopted tenant keeps the legacy approval path,
which replaces ADR 0254's note that approvals stay tenant-wide.

### 4. An approver-only agent needs a registered identity

An agent whose only HTTP policy is on its approver role now requires HTTP
identity for its turns as well. Its approvals can only be answered with HTTP
facts, and a turn admitted on the legacy path would create a provenance-free
approval that decision 3 refuses. Such a binding refuses turns until its
identity is registered or the approver role is changed, the same outcome ADR
0254 gives a migrated or filtered invocation role.

## Supersession

If accepted, this partially supersedes ADR 0254 decision 2 by adding the
approver-role clause to the per-binding test, and replaces ADR 0254's note
that approval continuity still classifies by tenant. ADR 0254 remains Accepted
in every other respect.

## Consequences

An operator can move one agent at a time for approvals as well as for turns,
and an approver filter is enforced on every route that can answer the
agent's approvals.

Setting an approver filter on an agent whose identity is not registered now
stops that agent's turns, not just its approvals. That is visible at once and
is the fail-closed direction; the operator registers the identity or removes
the filter.

The worker and API answer the same question, so the worker's classification
of a context-free turn reads the approver role too. No new call is made for a
tenant without an HTTP selection.

## Alternatives considered

1. **Keep approvals tenant-wide.** Blocks staged rollout for any agent that
   creates approvals on an unregistered binding, which is the gap ADR 0254
   left open.
2. **Classify approvals by the invocation role alone**, mirroring ADR 0254.
   Rejected for the reason in the context: an agent that restricts only who
   may approve would have that restriction skipped by the legacy checks.
3. **Classify approvals by the approver role alone.** A migrated invocation
   role keeps no team requirement, so its approvals' legacy checks would rely
   on rules the migration removed. Both roles matter.
4. **Teach the legacy approval checks to read HTTP approver filters.** Would
   make the legacy path depend on the HTTP provider, which defeats having a
   legacy path for unadopted bindings, and leaves two evaluators of one
   filter.
