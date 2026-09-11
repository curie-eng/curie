---
seam: Third-party port adapter (deployed service)
kind: NONE
impls: lifecycle unbuilt; generic HTTP edge shipped
grade: not separately graded
epics:
  - "#19"
  - "#158"
order: 22
---
# INTERFACE: Third-party port adapter (deployed service)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** NONE &nbsp;·&nbsp; **Implementations today:** lifecycle unbuilt; generic HTTP edge shipped &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

Every other file in this catalog answers "where does the code already draw a
line for *this* seam". This one answers the question that sits above all of
them, and that ADR-0096 settles: **where does a third party put its code so the
platform uses it instead of the default?**

The answer is a **deployed service**, not a loaded plugin. A third-party
platform adapter speaks the port's versioned wire contract and is selected by
composition — chart values, compose, or endpoint config — rather than by runtime
code loading. Four reasons, in the ADR's order of weight: it is language-neutral,
so an internal-tools team writes its adapter in the stack its tool already lives
in rather than being conscripted into Python; its blast radius is a process, so a
crash or a hang in third-party code is a failed dependency rather than a wedged
worker; it is independently upgradable behind a versioned wire; and it is what
the system already does everywhere a second implementation exists, so it adds no
new operational concept.

The generic authenticated HTTP ingress and neutral authenticated HTTP reply edge
now draw useful portions of that line. They are not an adapter installation or
lifecycle: no third-party adapter is packaged, registered, installed, discovered,
deployed, or proven conformant. `NONE` records that unbuilt lifecycle, not a
denial that the generic edges ship.

## Current contract

The shipped channel edge is an authenticated generic HTTP contract, but it is
not yet a contract a third party can install and conform to. A first promoted
adapter must honor the following ADR-0096 constraints rather than treating the
generic endpoints as a plugin API:

- **Two plugin kinds that do not merge.** An *agent bundle* extends one agent, is
  the Claude plugin shape verbatim, runs inside the sandbox trust domain, and
  versions with the agent. A *platform adapter* implements a platform port, runs
  in the platform trust domain, holds platform-adjacent credentials, and versions
  with the deployment. A request to plug in a new channel, store or harness is
  never answered by the bundle format. The rule is narrow and sharp: a bundle's
  files never execute as platform code, while declaring an implementation for the
  platform to host is allowed and already shipped — see
  [connector-host](../connector-host/INTERFACE.md), which is that same
  declare-and-host split one scope down, agent-scoped instead of
  deployment-scoped.
- **The wire is HTTP or a queue payload.** The shipped channel ingress is
  `POST /channels/token` plus `POST /channels/turns`
  (`apps/api/src/curie_api/routers/channels.py`). The platform mints a scoped
  `chn` token for one binding row and generation
  (`apps/api/src/curie_api/channel_token.py`); minting bumps that generation so
  a remint revokes the previous token, and the credential can enqueue only for
  the current binding. The turn body supplies its channel identity and
  delivery content, while the API loads the binding and supplies `kind`,
  `endpoint`, and `adapter` itself. A caller therefore cannot turn a token into
  an authenticated request to a caller-selected endpoint. Raw broker produce
  access remains first-party only: it could mint a turn or forge its author
  without the ingress API's authentication and dedupe.
- **Binding and reply-route facts are server controlled.** A binding is the
  neutral `{kind, address}` pair (`ChannelBinding`) on `AgentChannel`; its write
  form also records the paired `endpoint` and `adapter` facts an operator sets at
  bind time (`apps/api/src/curie_api/schemas.py::ChannelBindingWrite`). The
  generic reply edge receives neither route fact from adapter input. The worker
  builds its local `TargetRoute` from the resolved binding or a server-minted
  reply handle.
- **The egress is neutral and authenticated.** `ReplySinkRouter` selects
  `HttpReplyAdapter` for a non-Slack kind
  (`apps/worker/src/curie_worker/reply_sink.py::HttpReplyAdapter`). It POSTs a
  neutral `ReplyEvent` to the binding's server-controlled endpoint with only the
  credential selected by that route's adapter slug, fails closed when either is
  absent, and does not follow redirects or fall back to Slack. An adapter is a
  rendering and transport contract, never a trust boundary: approvals still
  resolve through the API authorizer, and an adapter holds its own channel
  credential rather than platform model credentials.
- **A four-rung promotion ladder remains before a port is pluggable.** An
  `INTERFACE.md` documents the line; a contract package makes the line a schema;
  a conformance suite is something a third party runs against its own adapter;
  and an adapter manifest declares its config, secrets, endpoints, and targeted
  contract version. Without packaging, installation and lifecycle behind that
  manifest, a generic HTTP edge is not an installable adapter. A registry proves
  an object was loaded; a conformance kit proves it behaves.
- **In-process entry points are the narrow exception.** Reserved for ports that
  are latency- or transaction-coupled to a turn and would be absurd behind HTTP —
  the binding hook that runs at claim time before the sandbox boots
  (`apps/worker/src/curie_worker/binding.py`), approver-set membership on the
  approval path (`apps/api/src/curie_api/approvers.py::ApproverSet`), and eval
  scorers inside a graded sweep. Those would use per-port `curie.<port>`
  entry-point groups carrying the same fail-closed guard rules as the harness
  registry, described in [harness-package](../harness-package/INTERFACE.md). The
  cost is why it stays the exception: the contribution must be pip-installed into
  a service image, which means a derived image per service, rebuilt on every
  platform release.
- **Ports are promoted one at a time, on demand, and the channel port is
  first** — the only seam with named third-party askers and the only one graded
  `C`. No generic plugin framework is built now, because most seams have one
  implementation and no asking party, and a framework would encode guesses about
  second implementations that do not exist. This is the standing restraint of
  [architecture-vision.md](../../architecture-vision.md) applied to the plugin
  question itself.

## Implementations today

The generic channel edges ship, but no third-party adapter service is supported
or shipped. `POST /channels/token` mints a scoped credential for a binding row;
`POST /channels/turns` accepts the matching authenticated delivery and derives
the reply route from that row, not from the request. A non-Slack resolved turn
uses `HttpReplyAdapter` to deliver neutral JSON reply events to that
server-controlled endpoint under its per-adapter credential. Discord
(`adapters/discord`) and email (`apps/mail-adapter`) are first-party adapters
on that HTTP edge, not a third-party kit: they are platform-owned services that
speak the shipped wire, not installable adapters with a manifest, registry, or
conformance suite. The first-party CLI/no-Slack path is a useful composition
precedent, not a supported third-party adapter.

The deployed-adapter lifecycle remains unbuilt:

- no `curie adapter` verb family or adapter manifest schema declares an adapter's
  image, contract version, config, secrets, and endpoints;
- no package format, registration/discovery mechanism, installer, or deployment
  reconciliation lifecycle turns an adapter service into an installed Curie
  component;
- the only entry-point group declared in the repo is the harness one,
  `ENTRY_POINT_GROUP`
  (`runner/src/curie_runner/harness/registry.py::ENTRY_POINT_GROUP`); no
  `curie.<port>` sibling group exists for the narrow in-process exception; and
- `packages/channel-protocol` provides neutral reply DTOs — `OutboundMessage`
  (`packages/channel-protocol/src/channel_protocol/models.py::OutboundMessage`)
  and `ChannelCapabilities`
  (`packages/channel-protocol/src/channel_protocol/models.py::ChannelCapabilities`)
  — but there is no adapter conformance kit.

The generic edges are necessary seam evidence, not second-implementation proof.
A real, independently supported adapter must use the whole ingress, egress,
credential, install, and lifecycle path before this seam can be promoted from
`NONE`.

## Known leakage

`NONE` now means the lifecycle is absent, not that every underlying edge is
absent. The remaining distance between the shipped generic edge and an adapter
service is material:

- **Fixed — credentialed generic egress.** Egress used to deliver every per-turn
  reply through one client holding a single Slack bot token, presented to
  whatever endpoint the turn named, with an unreachable-endpoint fallback that
  re-sent reply content to the default transport. `HttpReplyAdapter` now uses a
  per-adapter secret selected by the route's `adapter` slug, with no transport
  fallback; `SlackReplyAdapter`
  (`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`) refuses an
  endpoint outside the worker's configured trusted Slack origin rather than
  handing it the platform bot token.
- **Fixed (#1459, ADR-0096) — authenticated ingress and the binding surface.**
  The agents table used to carry a literal `slack_channel` column and the API's
  validators rejected non-Slack-shaped ids. The neutral binding is now a
  `{kind, address}` pair (`apps/api/src/curie_api/schemas.py::ChannelBinding`) on
  `AgentChannel`, and the channel router validates a scoped token against that
  binding's row id and generation before it enqueues a turn. The route facts
  (`endpoint`, `adapter`) come from the binding row rather than ingress input.
- **The interactivity return path has no scoped adapter credential.** Approval
  resolution sits behind the platform-wide key, and the scoped token minted for
  the sandbox is deliberately rejected everywhere but the state router. A
  scoped adapter credential for that return path remains a prerequisite of a
  third-party adapter rather than a follow-up to the first-party HTTP-edge
  adapters already shipped.
- **Packaging, installation, discovery, lifecycle, and conformance remain
  unbuilt.** A binding's configured route is not an adapter registry or an
  install experience, and the generic HTTP edges do not establish a supported
  third-party provider or deployment contract. There is no manifest-driven way
  to install an adapter and no conformance suite with which a third party could
  prove one. Discord and email on the HTTP edge do not close that gap: they are
  first-party services, not the kit this seam is waiting for.

## Cross-links

- **Related seam:** [harness-package](../harness-package/INTERFACE.md) — the only entry-point plugin mechanism in the codebase, and the model ADR-0096 generalizes from while reserving it for the narrow in-process exception.
- **Related seam:** [connector-host](../connector-host/INTERFACE.md) — the declare-and-host split one scope down, and the hosting substrate an operator-run adapter would reuse.
- **Related seam:** [channel-ingress](../channel-ingress/INTERFACE.md) — the graded (`C`) channel ingress/egress seam; its generic authenticated HTTP edges are shipped, and Discord and email already ride them as first-party adapters. That is not the third-party adapter kit this seam is waiting for.
- **Related seam:** [channel-interaction](../channel-interaction/INTERFACE.md) — the neutral interaction primitives used by the reply edge; they are not a third-party adapter conformance contract.
- **Epic(s):** #19 — per-turn reply endpoint routing, which the generic egress edge builds on; #158 — multi-tenancy, deliberately out of scope until it settles what a tenant owns
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — the standing restraint that no speculative adapter layer is written ahead of a real second implementation; this is not one of the six swap-readiness Jobs, so it is not separately graded
- **ADR(s):** [ADR-0096](../../adr/0096-port-adapters-are-deployed-services.md) — a third-party port adapter is a deployed service, not a loaded plugin; [ADR-0060](../../adr/0060-the-harness-is-a-declared-package.md) — the harness registry it generalizes; [ADR-0086](../../adr/0086-bundles-declare-connectors-the-platform-hosts-them.md) — the declare-and-host precedent moved up one scope; [ADR-0040](../../adr/0040-adopt-acp-as-an-edge-projection.md) — the trust rule inherited verbatim: an adapter is a rendering and transport contract, never a trust boundary
