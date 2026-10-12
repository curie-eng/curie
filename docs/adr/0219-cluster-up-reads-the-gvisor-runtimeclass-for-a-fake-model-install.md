# 219. Cluster up reads the gVisor RuntimeClass for a fake-model install

Date: 2026-10-12

Status: Accepted

This ADR supersedes in part the [ADR 0193](0193-cluster-up-reads-a-missing-runtimeclass-before-install.md)
Decision sentence that fake-model auto does not look up the RuntimeClass. It
extends the unschedulable inference in [ADR 0218](0218-cluster-up-infers-gvisor-off-when-the-runtimeclass-matches-no-node.md)
to fake-model auto installs only within the scope below, replacing its
Decision 4 exclusion of those installs. ADR 0218 is the accepted record for
issue #4411, which issue #4416 referred to as ADR 0216 before publication.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The maintainer grant is `adr-supersede-0193-gvisor-fake-model-lookup`, approved
by Brian on 2026-10-12 for issue #4416. The realizing code path is
`cli/src/ops/up.rs`.

## Context

Fake-model auto omits the gVisor preflight Job. The chart still looks up the
configured RuntimeClass and uses a present class for runner pods. A present
class whose `scheduling.nodeSelector` matches no node leaves those pods Pending
even though Helm reports a successful install. The fake model does not need
gVisor, but the selected class still constrains where its pods can schedule.

ADR 0218 checks that selector for a real-model install with a rendered
preflight. The same readable scheduling fact can prevent an unusable fake-model
install. The extra read must preserve the existing install behavior when its
evidence is unavailable.

## Decision

1. Under direct `curie cluster up` fact detection, if the value plan renders no
   gVisor preflight Job, clone that plan and append
   `agentSandbox.runner.fakeModel=false`. Render the same template once more.
   A Job in that second render identifies fake-model auto with the preflight
   enabled, unless the render also creates the RuntimeClass. Helm typing,
   `--set-string`, and the chart's RuntimeClass name remain chart decisions.
2. GET the class named by that second render using ADR 0218's existing lookup
   and node selector check. An Unschedulable result applies
   `security.gvisor.mode=off` before the first Helm install and prints ADR
   0218's inference unchanged. An explicit `security.gvisor.mode=auto` with
   that result is the same usage error before Helm. Present, Absent and
   Forbidden change nothing; the chart already omits an absent class.
3. On this fake-model path, any lookup error, including an unreadable node
   list, is reported on the plumbing channel and the install proceeds with
   the original value plan. A readable empty selector result remains the only
   new authority for reducing isolation.
4. Mode `off`, `gvisorPreflight.enabled=false`, `agentSandbox.deploy=false`,
   and `installRuntimeClass=true` do no fake-model lookup. Explicit `require`
   follows the rendered preflight path unchanged. Prepared `curie apply`
   and dry-run do no lookup or scheduling inference.

## Consequences

1. A fake-model auto install with a present class and no matching node installs
   once with gVisor off and the existing scheduling inference visible before
   Helm starts.
2. An absent or schedulable class, a Forbidden lookup, or unreadable evidence
   preserves the previous fake-model install behavior. The new read cannot
   become a new install failure.
3. The real-model preflight, explicit `require`, admission recovery and failure
   diagnosis retain their existing rules. The selector check remains a
   snapshot rather than a general scheduler check.

## Alternatives considered

1. Copy the chart's fake-model and preflight predicates into the CLI. Rejected
   because Helm typing and chart defaults can disagree with a copied predicate.
2. Fail the fake-model install when the new lookup is unreadable. Rejected
   because the previous install did no CLI lookup and did not fail on that read.
3. Infer off for an absent class or every read failure. Rejected because the
   chart already omits an absent class and unreadable evidence does not prove
   that a present class is unschedulable.
4. Add a chart-side node lookup. Rejected because operators without node list
   access would acquire a rendering failure.
