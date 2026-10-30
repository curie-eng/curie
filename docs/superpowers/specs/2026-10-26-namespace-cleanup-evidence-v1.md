# Namespace cleanup evidence v1

Verification tooling for [#5062](https://github.com/curie-eng/curie/issues/5062). A released-upgrade negative control can finish its upgrade assertions and Helm uninstall but time out deleting its disposable namespace. Existing logs do not identify the remaining finalizer or discovery failure. This adds evidence, not a finalization fix or a new cleanup policy.

## 1. Read only collection

<!-- @spec NAMESPACE-CLEANUP-EVIDENCE-1 -->

The released-upgrade `namespace_cleanup_evidence` module owns `collect_cleanup_evidence(context, namespaces, *, budget_s=40) -> dict` and `main(argv=None) -> int`. Import performs no command or I/O. The CLI requires explicit `--context` and one or more `--namespace` arguments; optional `--budget-seconds` defaults to 40. Reject an empty/nonstring context, invalid namespace names, duplicate namespaces, more than four namespaces, and nonfinite, boolean or out of range budgets before any command. Budget range is one through 60 seconds. Use fixed ValueError text `invalid_cleanup_evidence_input` without supplied values.

Every kubectl command names the explicit context. Use only read operations: get Namespace, discover namespaced listable resource kinds, get their projected metadata and get APIService availability. No current-context lookup, mutation, finalizer patch, deletion retry or controller restart. Discovery names must pass a bounded resource-name grammar before being used as an argument. Invoke argument arrays without a shell. Collect once per command; failures do not cause retries.

## 2. Safe metadata protocol

<!-- @spec NAMESPACE-CLEANUP-EVIDENCE-2 -->

Return one JSON object with `schema_version: 1`, `complete: bool`, `namespaces: list`, `api_services: list`, and `errors: list`. Each namespace record has its supplied `namespace`, observed `phase`, `deleting`, `finalizers`, `conditions`, `resources` and `errors`. Conditions contain only type/status/reason. Namespace finalizers include the namespace spec and metadata finalizer lists. Each nonempty resource summary contains `kind`, `count`, `deleting`, `finalizers` counts and `owner_kinds` counts. APIService summaries identify only unavailable/unknown service names and Available condition status/reason. Error objects contain only fixed operation/resource identifiers and a bounded reason code.

Workload queries must project only item finalizers, deletion-timestamp presence and owner-reference kinds. They must not emit object/owner names, UIDs, labels, annotations, resource spec or data. Namespace and APIService parsing likewise selects only the fields above; never copy condition messages, raw command output, stderr, exception text or credential material into the report. Validate projected shapes rather than coercing them. Metadata symbols are ASCII identifiers of at most 256 characters; invalid symbols become `unrecognized`, not raw strings. Unknown/unreadable state is not reported as an empty or deleted namespace.

Pinned SDK observation: kubectl **v1.34.1** was exercised against an owned loopback synthetic Kubernetes discovery and SecretList service. An actual `get secrets` with this Go template returned valid JSON containing only the three permitted fields for two items, including missing lists and a deletion timestamp. Synthetic data/spec/annotation/name/UID sentinels were absent, and the fixture observed GET requests only:

```text
[{{range $i,$item := .items}}{{if $i}},{{end}}{"finalizers":[{{range $j,$f := $item.metadata.finalizers}}{{if $j}},{{end}}{{printf "%q" $f}}{{end}}],"deleting":{{if $item.metadata.deletionTimestamp}}true{{else}}false{{end}},"owner_kinds":[{{range $k,$o := $item.metadata.ownerReferences}}{{if $k}},{{end}}{{printf "%q" $o.kind}}{{end}}]}{{end}}]
```

Equivalent reproduction uses a fixture-owned kubeconfig/context and `kubectl --kubeconfig "$owned_config" --context "$owned_context" get secrets -n synthetic-projection -o="go-template=$template"`. This is projection evidence, not proof of any real namespace's finalization.

## 3. Bounded partial evidence

<!-- @spec NAMESPACE-CLEANUP-EVIDENCE-3 -->

One monotonic deadline covers the whole collection. Every command has a timeout of at most five seconds and no more than remaining budget. Once exhausted, start no new command and mark the report incomplete. Bound discovery to 128 resource kinds, accepted projections to one MiB and 1,024 items per response, and metadata lists to 64 entries per object. Limits, malformed JSON/shapes, command errors and timeouts produce fixed reason codes such as `budget_exhausted`, `resource_limit`, `oversized_projection`, `invalid_projection`, `command_error`, `not_found` or `timeout`. Do not turn partial evidence into a successful cleanup verdict.

The collector uses private `_run(command, timeout_s)` for the actual subprocess boundary. Independent tests may replace this external-command boundary with synthetic Kubernetes responses; they may not read a real kubeconfig or contact a real cluster. The observed real kubectl projection remains separate evidence.

## 4. CI placement and required proof

<!-- @spec NAMESPACE-CLEANUP-EVIDENCE-4 -->

The released-chart negative control invokes the collector only in its failure diagnostics, before destroying that job's own cluster. Pass its named context and the two disposable negative-control namespaces explicitly. Retain safe JSON in the job log and a dedicated diagnostic artifact. Preserve the original upgrade/profile assertions, Helm uninstall, 300-second namespace-deletion wait, required aggregate and always cleanup. A diagnostic command succeeding never makes the failed control pass. Missing evidence is not a waiver, and this change claims no root-cause fix.

Changing the collector's source selects the released-upgrade tier through the existing end-to-end selection registry. The diagnostic is an inline repository input of that control; it must not be silently omitted when edited alone. Preserve the registry's existing minimal selection and its independent inline-input coverage guard.

Independent tests precede implementation. Cover safe namespace/resource/APIService summaries, actual command context/read-only arguments, forbidden-field sentinels, malformed/oversized/partial responses, resource and time limits, no retry, no command after exhaustion, and fixed non-sensitive errors. Structural workflow checks retain the original failure gate/wait/teardown and place collection/artifact before teardown. Source and tests must not create, patch or delete any Kubernetes object. Run focused tests and static/docs/workflow checks locally; exact-head required CI still proves the selected control.
