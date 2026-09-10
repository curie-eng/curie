# 149. Curie pins a model per agent and the provider does the routing

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

**Supersedes [ADR-0037](0037-opt-in-binding-hook-and-pareto-model-routing.md)**
(opt-in binding hook and Pareto model routing) in whole. ADR-0037's Decision
specifies a binding-hook port, a manifest `routing` block, an install-level
model registry, deterministic Pareto selection with an optional LLM floor
estimator, promotion of the routing decision into `SessionConfig`, and
per-decision observability. This decision is that Curie builds none of it.

Under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
ADR-0037's status line and back-link are written when this decision is Accepted,
not while it is Draft.

## Context

ADR-0037 was Accepted on 2026-07-16, driven by a prospective engagement asking
for a model router: static rules-based routing first, a model-made routing
decision second. It is a careful design. It reached the same conclusion
OpenRouter reached independently about pinning a routed session to one model to
protect prompt caches, it kept selection deterministic and auditable, and it
refused the in-path gateway that #253 had already rejected on prompt-cache
grounds.

It has not been built. `BindingDecision` and `binding_hook` return no hits
anywhere in the tree, and `model_registry` and `model_alias` — the vocabulary of
the adjacent Draft [ADR-0128](0128-install-owned-model-gateway-and-agent-aliases.md)
— return none either. What ships instead is a boot-time pin with a per-agent
override: `agents.model` and `agents.thinking`, applied to the sandbox boot
environment in `apps/worker/src/curie_worker/binding.py:838-885`, which the code
itself notes are "the only per-agent knobs here"
(`apps/worker/src/curie_worker/binding.py:861`). The runner resolves the
credential to a provider by prefix and base URL, so a key for an aggregating
provider already reaches that provider's endpoint with no Curie-side work.

Two things have changed since 0037 was written.

The first is that the aggregators shipped the router. A caller can name a
provider-side routing model as the model id and get exactly what ADR-0037's
Pareto selection promised — cheapest at or above a declared floor, chosen by
someone whose whole business is maintaining the price and capability table that
ADR-0037's install-level registry would have required an operator to hand-write
and hand-maintain per install. Building the registry now means competing with a
service already reachable through a field Curie already has.

The second is the workload this decision set is about. A dark-factory agent runs
one kind of task — implement a fully-specified ticket — on a cheap model chosen
once. ADR-0037's premise was a mixed stream where a router earns its keep by
telling simple work from complex; a factory lane has no mixed stream to sort.
And ADR-0037 already decided that a route binds per session and sticks, so even
under 0037 a factory agent would get one model per run. The router's value in
this workload is close to zero, and the design cost is the largest single piece
of platform work in its epic (multi-credential delivery).

Nothing about this is an argument that ADR-0037 was wrong when written. It is an
argument that the thing it would have built is now available on the other side
of a field, and that the workload that would have justified building it anyway
is not the workload in front of us.

## Decision

**Model selection stays a per-agent, boot-time pin. Curie builds no router, no
binding-hook port, no install-level model registry, and no floor estimator.
Where routing is wanted, the operator pins a provider-side routing model id and
the provider routes.**

1. **`agents.model` is the whole surface.** An operator selects one model per
   agent. An install that wants a cheap tier and an expensive tier runs two
   agents, which is a shape the platform already supports and an operator
   already understands.

2. **Routing, where it happens, happens at the provider.** A routing model id in
   `agents.model` is a value, not a code path. Curie does not know the id is a
   router, does not see the candidates, and does not record the choice — the
   provider does, on its own dashboard.

3. **No binding hook.** The port ADR-0037 proposed is not extracted. Under the
   ADR-0026/0027 discipline it would have been extracted ahead of its second
   adapter on the strength of the first; with the first adapter withdrawn there
   is no adapter to extract it for.

4. **`SessionConfig` does not gain routing fields.** ADR-0037's decision 6 was to
   promote model id, provider base URL, and credential ref into the typed ACI as
   a patch bump. That is withdrawn. The model still travels as it does today.

5. **The credential shape is unchanged.** One model credential resolves, as
   ADR-0009 decided. The multi-credential delivery ADR-0037 called for — its
   largest piece of platform work — is not built.

6. **Non-model routing stays out of scope**, as ADR-0037 also decided. Handler
   dispatch above the model seam is a different problem and this decision does
   not touch it.

### What this does not decide

Draft [ADR-0128](0128-install-owned-model-gateway-and-agent-aliases.md) proposes
an install-owned model gateway with operator-selected aliases. That decision is
about *where an install terminates its model credentials and what an operator
calls them*, not about per-task selection, and this ADR neither accepts nor
rejects it. If 0128 is accepted, `agents.model` names an alias instead of a raw
model id and everything above still holds.

## Consequences

An entire Accepted-but-unbuilt epic is closed rather than left open. That is the
main benefit: ADR-0037 currently reads as a live commitment, and a reader
opening it finds a design with no code and no back-link telling them the
thinking moved.

Cost control moves to the provider and to the per-agent budget cap that already
exists. An operator who wants to know what routing chose looks at the provider's
records, not Curie's. That is a real loss of a property ADR-0037 valued —
"every decision is auditable and replayable against the registry" — and it is
traded knowingly for not maintaining a per-install price and capability table.

Curie takes a dependency on a provider's routing behaviour changing underneath
it. A routing model that silently starts choosing a different tier changes cost
and quality with no signal on this side, and the compensating control is the
per-agent budget cap and the bundle's own evals, not a platform mechanism.

A future workload with a genuinely mixed stream — one agent, tasks of wildly
different difficulty, and a reason the provider's router cannot see the
difference — would reopen this. ADR-0037's reasoning stays legible for exactly
that case, which is why it is superseded rather than deleted.

Prompt-cache economics are unaffected. A pinned model per session was ADR-0037's
own conclusion and #253's before it, and pinning per agent is strictly stronger.

## Alternatives considered

**Build ADR-0037 as written.** Rejected. The install-level registry requires an
operator to maintain per-model pricing and capability scores per install, which
is a table that is stale the week it is written, and the selection it feeds is
now purchasable through a field that already exists.

**Keep ADR-0037 Accepted and simply not implement it.** Rejected, and this is
the status quo. An Accepted ADR authorizes implementation; leaving one Accepted
and unbuilt for months is the exact intent-gap failure
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md) was
written to make recordable.

**An in-path gateway or proxy.** Rejected again, for the reason #253 and
ADR-0037 both gave: translation hops in the token path break byte-stable prompt
prefixes and destroy prompt-cache economics.

**A per-task model choice made by the bundle.** Rejected. It would put model
selection in prompt-controlled code and give a bundle a say in which credential
it consumes, which is a credential-redirection surface, and ADR-0037 rejected
the general form of it for the same reason.

**Hard-code one cheap model and drop the pin entirely.** Rejected as too narrow.
`agents.model` already exists, costs nothing, and is what lets one install run a
factory agent beside an interactive one on different models.

## Realizing code path

None required; the pin in `apps/worker/src/curie_worker/binding.py` is the
behaviour this decision keeps. The work this decision authorizes is
documentation: recording ADR-0037's supersession and closing its tracking epic.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act.
