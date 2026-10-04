# 193. Cluster up reads a missing RuntimeClass before install

Date: 2026-10-04

Status: Accepted

This ADR supersedes in part [ADR 0114](0114-cluster-up-infers-detected-install-facts.md)
Decision 3 and rejected Alternative 2, only for the case where a direct GET of
the configured RuntimeClass returns NotFound before install. The
admission-rejection retry stays when that lookup is Forbidden. Every other
[ADR 0006](0006-security-rails-as-chart-defaults.md) and ADR 0114 rule is
unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Explicit maintainer approval is the recorded grant `adr-supersede-0114-gvisor`
for issue #3933. The realizing code path is `cli/src/ops/up.rs`.

## Context

ADR 0114 waits for the gVisor preflight admission result before applying
`security.gvisor.mode=off`. On a cluster that has no RuntimeClass, that first
install creates a failed Helm revision and prints a retry warning even though
`kubectl get runtimeclass` would already have returned NotFound.

Admission remains the only evidence when the lookup itself is Forbidden. A
present class, an unreadable lookup, and every other ADR 0114 waiver stay fail
closed.

## Decision

When `curie cluster up` would render the gVisor preflight, it GETs the
configured RuntimeClass first. NotFound applies `security.gvisor.mode=off`
before Helm and prints the existing inference line. Forbidden leaves the chart
default and keeps the one admission retry. Present does not infer off, and a
later not-found admission fails closed. Explicit auto or require plus NotFound
is a usage error before install. `installRuntimeClass` true does not treat
NotFound as absence of the runtime. Apply and dry-run do not infer this fact.
Fake-model auto, using the chart default `fakeModel` true unless
`inference.deploy` is true, does not look up.

## Consequences

1. A cluster whose RuntimeClass GET returns NotFound installs once, with the
   existing inference line and no retry warning.
2. A Forbidden lookup still uses the ADR 0114 admission retry, once.
3. Present, unreadable, and nonmatching results do not apply `off`.
4. Prepared apply and dry-run stay offline for this fact.

## Alternatives considered

1. **Keep retrying every install.** Rejected, that is the bug.
2. **Treat any kubectl error as absence.** Rejected, unreadable evidence must
   fail closed.
3. **Delete the admission retry.** Rejected, a forbidden lookup still has only
   admission as evidence.
