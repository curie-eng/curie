# 114. Cluster up infers detected install facts

Date: 2026-08-20

Status: Accepted

**Superseded in part by [ADR 0193](0193-cluster-up-reads-a-missing-runtimeclass-before-install.md)**
(back-link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
0193 supersedes Decision 3 and rejected Alternative 2 only when a direct GET
returns NotFound before install.

**Supersedes in part [ADR 0032](0032-explicit-provider-egress.md) and
[ADR 0006](0006-security-rails-as-chart-defaults.md).** This ADR replaces
exactly these clauses:

1. ADR 0032 Decision clause 1, which says that a model credential alone opens
   no egress and requires an explicit provider choice before the model is
   reachable.
2. ADR 0032 rejected Alternative 1, which rejects provider detection from a
   credential prefix.
3. The ADR 0006 final Consequences clause that says the absence of every listed
   rail fails review, only as it applies to gVisor when `curie cluster up`
   observes the exact admission rejection `RuntimeClass "gvisor" not found`.
   The ADR 0006 Decision that keeps gVisor enabled by default remains unchanged.

Every other ADR 0032 egress rule and ADR 0006 security rail remains unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Explicit maintainer approval is recorded in [issue
#1662](https://github.com/curie-eng/curie/issues/1662) on 2026-08-20. The
realizing code path is `cli/src/ops.rs::up`, including its provider, singleton,
and gVisor reconciliation in `cli/src/ops.rs`.

## Context

On a reachable stock cluster, a successful install required five explicit
overrides even though Curie already had direct evidence for every value. The
effective credential selected the runtime provider. Existing singleton objects
recorded their Helm owner. Kubernetes admission reported that the requested
RuntimeClass did not exist.

The explicit provider flag also created an unsafe split. An OpenRouter
credential combined with `--allow-egress-host anthropic` installed
successfully, opened the Anthropic host, and left the runner dialing
OpenRouter. A green install therefore produced a model route that NetworkPolicy
blocked.

Requiring a human to copy facts already held by the credential and the cluster
does not add a security decision. It adds opportunities for contradiction and
turns ordinary cluster discovery into a sequence of failed installs.

## Decision

### 1. Infer provider egress from an unambiguous effective credential

Direct `curie cluster up` reads the effective credential after existing release
values have been reconciled. When no provider flag is present, an `sk-ant-`
prefix applies `--allow-egress-host anthropic` and an `sk-or-` prefix applies
`--allow-egress-host openrouter`.

Other credential shapes do not identify one provider and therefore infer
nothing. They remain sealed unless the operator supplies provider or web
egress. This decision does not expand the provider registry.

An explicit provider list wins only when it includes the detected provider. A
list that omits the detected provider is a usage error. A list that includes it
may include additional providers and is used unchanged.

### 2. Infer reuse from complete Helm ownership

Direct `curie cluster up` inspects both rendered PriorityClasses and the
`agent-sandbox-controller` Deployment. An existing singleton is reusable only
when its metadata states `app.kubernetes.io/managed-by=Helm` and carries both
`meta.helm.sh/release-name` and `meta.helm.sh/release-namespace`.

When complete metadata names another release, Curie applies the matching
`priorityClasses.platform.create=false`,
`priorityClasses.sandbox.create=false`, or
`agentSandbox.controller.deploy=false` value. An explicit creation value that
contradicts the observed owner is a usage error. Other explicit values remain
authoritative.

Missing, malformed, unreadable, or incomplete ownership evidence does not
authorize reuse. Curie fails closed and reports the conflicting object.

### 3. Infer gVisor off from the exact admission result

Direct `curie cluster up` first installs with the chart default intact. If the
gVisor preflight pod receives the exact admission rejection `RuntimeClass
"gvisor" not found`, the cluster has supplied the authoritative fact that it
cannot run that pod with gVisor. Curie then applies
`security.gvisor.mode=off`, prints the inference, and retries the same install
once.

This is the right posture because admission is the enforcement point. It proves
that this cluster cannot satisfy the requested RuntimeClass, while a copied
flag adds no further knowledge or consent. Applying the detected posture makes
the reduced isolation explicit and lets a reachable stock cluster install.

The waiver is narrow. An explicit `security.gvisor.mode=auto` or
`security.gvisor.mode=require` contradicts the admission fact and is a usage
error. Any other rejection, an unavailable event watch, or unreadable evidence
still fails closed. Curie does not retry more than once.

### 4. Make every inference visible

Curie prints one line to standard error for each applied inference, naming the
equivalent override. Structured output remains one object on standard output.
Prepared apply and diff paths do not infer cluster facts.

## Consequences

1. A bare `curie cluster up` succeeds in the stock cluster case from issue
   #1662 without requiring the operator to transcribe five discovered facts.
2. Provider egress cannot silently disagree with an unambiguous bound
   credential.
3. A cluster without gVisor runs with less kernel isolation after the exact
   admission result. The inference line makes that posture visible.
4. Ambiguous credentials, incomplete ownership, unreadable cluster state, and
   nonmatching admission failures retain fail closed behavior.
5. Provider discovery remains limited to prefixes whose runtime routing is
   unambiguous. Adding more providers remains separate work.

## Alternatives considered

1. **Keep all five overrides explicit.** Rejected because the operator would be
   copying detected facts, and a wrong provider value can produce a successful
   but unusable install.
2. **Disable gVisor before admission on clusters that appear not to support
   it.** Rejected because an inventory guess is weaker than the result from the
   enforcement point.
3. **Disable gVisor after any Helm failure.** Rejected because unrelated or
   unreadable failures provide no fact that authorizes a security posture
   change.
4. **Reuse any existing singleton by name.** Rejected because a name alone does
   not prove Helm ownership. Only complete ownership metadata authorizes reuse.
