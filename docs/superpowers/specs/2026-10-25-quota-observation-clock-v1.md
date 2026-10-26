# Quota observation clock v1

The existing idle-capacity scenario checks two chargeable sandbox Pods. Its ordinary observation samples time after reading the Pod list; the sixteen-turn checker currently samples before the potentially blocking list read. A returned list can contain a newly admitted Pod plus an older deleting Pod whose grace expired during that read. Classifying this newer list with the older clock can report three chargeable Pods even though the current count is two.

## Fresh observation time

<!-- @spec QUOTA-OBSERVATION-CLOCK-1 -->
In the existing nested quota_pods function inside burst_sixteen, finish the existing single Pod-list read before sampling time.time. Apply the existing counts_against_quota predicate to that observed list and this fresh time, consistent with observe. Do not sample a clock before list I/O and reuse it to classify returned Pods. This is observation-completion classification, not a continuous quota guarantee.

The existing predicate mirrors the pinned [Kubernetes v1.31.0 QuotaV1Pod implementation](https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/quota/v1/evaluator/core/pods.go#L417-L436): terminal Pods and deleting Pods strictly past their deletion grace do not consume Pod quota. Keep that predicate unchanged, including equality at the grace boundary.

## Preserved limits and outcomes

<!-- @spec QUOTA-OBSERVATION-CLOCK-2 -->
Keep hard Pods quota 2, observed quota usage <=2, priority filtering and chargeable count <=2. A genuinely over-limit returned list must still fail. Preserve the sixteen launch arguments, 300-second CLI reply budget, 320-second aggregate deadline, polling order, all other assertions, history/finalization checks and cleanup. No timeout/quota/retry increase, new network request, process, thread, logging or worker behavior is introduced.

A failed Pod-list read propagates its existing exception; the clock correction must not mask it. This corrects a deterministic checker race and does not claim to explain the separate aggregate runtime timeout or every CI failure.

## Executable regression

<!-- @spec QUOTA-OBSERVATION-CLOCK-3 -->
Execute the actual nested checker and existing quota predicate extracted from the driver heredoc AST, with deterministic external list/clock boundaries. No shell driver setup, cluster, container or external service is needed. The principal control starts with valid quota usage two, advances the clock during Pod-list retrieval across an old Pod's deletion-grace boundary, and returns two active Pods plus the expired deleting Pod. One active Pod may be newly created after that boundary; it must not be combined with a pre-read clock. The unchanged checker fails and the corrected checker passes.

Also cover canonical quota limits, genuine three-Pod overage, unexpired and exactly-equal deletion grace, terminal/different-priority Pods, one-list-read behavior and original read-error propagation. Tests precede implementation and remain independently authored. Real runtime verification belongs to the PR's exact-head CI; deterministic controls alone do not prove live quota behavior.
