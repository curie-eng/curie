# Quota observation clock v1

The idle-capacity scenario checks at most two chargeable sandbox Pods holding runtime. It classifies each returned Pod list using the time sampled after that read. An earlier clock correction fixed a checker race in which a pre-read timestamp counted an older deleting Pod whose grace expired during the read. That correction remains in place. The current rule also permits a bounded excess of Pending Pods that have not held runtime, as specified in #4740.

## Fresh observation time

<!-- @spec QUOTA-OBSERVATION-CLOCK-1 -->
In the quota_pods function shared by observe and burst_sixteen, finish the existing single Pod-list read before sampling time.time. Apply the existing counts_against_quota predicate and Pending-age limit to that observed list and this fresh time. A pre-read clock may be retained for diagnostics, but must not classify the returned Pods. This classifies a completed observation and does not establish a continuous quota guarantee.

The existing predicate mirrors the pinned [Kubernetes v1.31.0 QuotaV1Pod implementation](https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/quota/v1/evaluator/core/pods.go): terminal Pods and deleting Pods strictly past their deletion grace do not consume Pod quota. Keep that predicate unchanged, including equality at the grace boundary.

## Preserved limits and outcomes

<!-- @spec QUOTA-OBSERVATION-CLOCK-2 -->
Keep hard Pods quota exactly 2, observed quota usage <=2, priority filtering and the unchanged chargeable predicate. Among chargeable Pods, at most two may hold runtime. Every non-Pending phase counts conservatively as holding runtime, including Unknown. A Pending Pod also holds runtime if any normal containerStatuses entry has running or terminated in its current state or lastState. Init-container status alone does not establish a runtime holder.

When the chargeable count exceeds two, every pre-runtime Pending Pod in that sample must have an age between zero and 30 seconds inclusive, measured from its creationTimestamp to the fresh observation time. Missing, malformed or future creation timestamps fail closed. This bounds all such Pending Pods in an excess sample rather than choosing only the newest Pods. An old Pending Pod remains valid when the total chargeable count is at most two.

The 30-second allowance keeps transient pre-runtime admission lag well below the unchanged 300-second CLI reply budget and 320-second aggregate deadline. It does not guarantee that upstream quota accounting converges within 30 seconds. A third runtime holder or an excess pre-runtime Pending Pod outside that allowance must fail.

Preserve the sixteen launch arguments, polling order, all other assertions, history/finalization checks and cleanup. Keep the existing diagnostic whenever the chargeable count exceeds two, including allowed transient samples, and report the runtime-holder count alongside it. Return the complete priority-filtered observed UID map, including nonchargeable Pods. No timeout, quota or retry increase, new network request, process, thread or worker behavior is introduced.

A failed Pod-list read propagates its existing exception; neither classification rule may mask it. The clock regression and bounded Pending allowance do not explain the separate aggregate runtime timeout or every CI failure.

## Executable regression

<!-- @spec QUOTA-OBSERVATION-CLOCK-3 -->
Execute the actual nested checker and existing quota predicate extracted from the driver heredoc AST, with deterministic external list/clock boundaries. No shell driver setup, cluster, container or external service is needed. The clock control starts with valid quota usage two, advances the clock during Pod-list retrieval across an old Pod's deletion-grace boundary, and returns two active Pods plus the expired deleting Pod. One active Pod may be newly created after that boundary; it must not be combined with a pre-read clock.

Pending controls cover fresh excess Pending Pods with zero or two Running Pods, a series through the same checker from three fresh Pending Pods to two Running Pods and then a rejected third Running Pod, age exactly 30 seconds and just beyond it, all Pending ages in an excess sample, missing/malformed/future creation timestamps, normal container current/last-state runtime evidence, init-container exclusion, Unknown phase and an old Pending Pod when the chargeable count is at most two.

Retain canonical quota limits, three runtime holders with reported usage two, unexpired and exactly-equal deletion grace, terminal/different-priority Pods, the complete observed UID map, one-list-read behavior and original read-error propagation. Tests precede implementation. Real runtime verification belongs to the PR's checks for its current commit; deterministic controls alone do not prove live quota behavior.
