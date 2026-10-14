# 218. Cluster up infers gVisor off when the RuntimeClass matches no node

Date: 2026-10-12

Status: Accepted

This ADR supersedes in part [ADR 0193](0193-cluster-up-reads-a-missing-runtimeclass-before-install.md)
Decision and Consequence 3, rejected Alternative 2 of
[ADR 0114](0114-cluster-up-infers-detected-install-facts.md), and the final
Consequences clause of [ADR 0006](0006-security-rails-as-chart-defaults.md).
The scope is only a present RuntimeClass whose nonempty `scheduling.nodeSelector`
matches no node in a readable node list at lookup time. Every other security
rail and inference rule remains unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The maintainer grant is `adr-supersede-0193-gvisor-scheduling`, approved by
Brian on 2026-10-12 for issue #4411. The realizing code path is
`cli/src/ops/up.rs`.

## Context

A RuntimeClass can exist without any node that satisfies its scheduling
selector. GKE supplies a `gvisor` RuntimeClass whose selector is
`sandbox.gke.io/runtime=gvisor` even when no GKE Sandbox node pool exists.
The previous lookup treated every present class as usable. A real model
installation then waited for the gVisor preflight Job deadline and reported
`DeadlineExceeded` without explaining that no node could schedule the pod.

The RuntimeClass selector directly identifies the required node pool. A
readable empty list supplies a bounded install fact, while a failed node read
supplies no evidence for reducing isolation. A node inventory is also a
snapshot: clusters can provision sandbox nodes on demand after a pod requests
them.

## Decision

1. When direct `curie cluster up` renders a gVisor preflight Job and the chart
   will not create the RuntimeClass, read the configured class before Helm.
   A present class with a nonempty `scheduling.nodeSelector` is unschedulable
   when `kubectl get nodes -l <selector> -o name` succeeds with no node.
   Check only the selector; ignore tolerations, taints and cordons.
2. For an inferred mode, apply `security.gvisor.mode=off` in the value plan
   before the first Helm install and render the preflight again. Install once
   with no admission retry. Print the RuntimeClass name, the selector with
   keys sorted and joined with commas, the GKE Sandbox remedy, and the
   equivalent override. The recorded `off` value follows the existing
   preservation path on later plain installs.
3. Explicit `security.gvisor.mode=auto` contradicts this scheduling result
   and is a usage error before Helm. Explicit `security.gvisor.mode=require`
   keeps the preflight gate and receives no inference or usage error. A node
   list snapshot cannot prove that a cluster which provisions sandbox nodes
   on demand cannot schedule the pod; `require` asserts that it can, and the
   preflight verifies that assertion.
4. Forbidden or otherwise unreadable node lists keep the class present and
   authorize no posture change. NotFound lookup behavior and the existing
   admission retry remain unchanged. Fake model auto installs, omitted
   preflights, prepared apply and dry runs do not infer scheduling posture.
5. After Helm fails with a rendered gVisor preflight, inspect release namespace
   Events for `FailedScheduling` on Pods whose names start with the rendered
   Job name followed by a hyphen. Both `cluster up` and `curie apply` report
   that the Job was never scheduled and give the invocation's existing
   instruction for disabling gVisor. Read the RuntimeClass again; add its
   selector and the GKE Sandbox explanation only when the selector still
   matches no node in a readable node list.
6. Print the reason code and the selector read directly from the class. Never
   print raw Event messages. With no matching Event or an unreadable Event
   list, preserve the existing installation failure path.

## Consequences

1. A real model auto install with a present class and no matching node installs
   once with reduced isolation, disclosed before Helm starts.
2. Explicit `require` retains the enforcement gate for clusters with node
   provisioning on demand. A scheduling failure still waits for the existing
   Job deadline, then reports `FailedScheduling` and the observed selector.
3. A missing node pool receives a useful diagnosis without exposing arbitrary
   Kubernetes Event messages. Unreadable evidence remains closed.
4. Taints, cordons, capacity, and runtime handler availability remain the
   preflight's responsibility. The lookup is not a general scheduler check.

## Alternatives considered

1. Keep every present RuntimeClass enabled until the preflight fails. Rejected
   because a readable empty selector result already identifies the missing
   node pool and causes an avoidable failed Helm revision.
2. Reject explicit `require` on an empty node list. Rejected because a
   snapshot cannot rule out provisioning sandbox nodes on demand; the
   preflight must verify the operator's assertion.
3. Infer off after a failed node read or any Helm failure. Rejected because
   unreadable or unrelated evidence does not authorize reduced isolation.
4. Reimplement all scheduler constraints or abort Helm with a scheduling
   watcher. Rejected because the scoped fact is the RuntimeClass node pool
   selector, and the existing preflight remains the enforcement point.
