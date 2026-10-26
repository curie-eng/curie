---
seam: Mail provider
kind: CLEAN
impls: 1 in-tree (AgentMail) + out-of-tree providers loaded from the `curie.mail_provider` entry-point group
grade: not separately graded
epics:
  - ADR-0228
order: 30
---

# INTERFACE: Mail provider

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 in-tree (AgentMail) + out-of-tree providers loaded from the `curie.mail_provider` entry-point group &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

[ADR 0228](../../adr/0228-mail-services-plug-into-one-email-adapter-which-owns-approvals-by-email.md)
splits the email adapter in two. The neutral adapter in `apps/mail-adapter` keeps the
channel protocol with the platform, delivery state and idempotency, the inbound gate,
the intake path and approvals by email. A mail provider talks to one mail service:
it lists and reads messages, replies in a thread, looks for the reply marker in a
thread, and reports what the receiving mail system knows about who sent a message.
The line is the `MailProvider` protocol
(`apps/mail-adapter/src/curie_mail_adapter/provider.py::MailProvider`). Nothing changes
on the wire between the adapter and the platform.

## Current contract

- **Declarations**, made once when the provider is built: the interface version
  (`apps/mail-adapter/src/curie_mail_adapter/provider.py::INTERFACE_VERSION`), the one
  bare inbox address, whether it verifies senders, and how soon a sent reply is visible
  (zero for read-after-write, else a bound in seconds).
- **Operations**: `list_messages`, `get_message`, `send_reply`, `thread_contains` and
  `config_problems`. None raises for a service or network failure; each returns a
  result or a neutral `Failure`
  (`apps/mail-adapter/src/curie_mail_adapter/provider.py::Failure`) of kind
  `unreachable`, `egress_refused`, `rate_limited`, `refused`, `server_error` or
  `too_large`, with a bounded cause that holds no credential and no mail content.
- **Sender verdict**: `verified` with the From domain DMARC evaluated, `failed`, or
  `unverifiable`
  (`apps/mail-adapter/src/curie_mail_adapter/provider.py::SenderVerdict`). A provider may
  build `verified` only from results the receiving mail system attaches and the
  service guarantees a sender cannot supply.
- **The gate** (`apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter._listing_rejection`
  before any fetch, `apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter._message_rejection`
  after it): a provider that does not verify senders refuses everything from the
  listing alone; otherwise exactly one From mailbox, on the sender filter, with a
  `verified` verdict whose domain equals the From domain.
- **Selection**: `CURIE_MAIL_PROVIDER`, default `agentmail`, resolved by direct import;
  any other name through the `curie.mail_provider` entry-point group with its guards
  (`apps/mail-adapter/src/curie_mail_adapter/provider.py::resolve_factory`). The state
  file records its provider and refuses a different one
  (`apps/mail-adapter/src/curie_mail_adapter/state.py::MailState.bind_provider`).
- **Versioning**: `major.minor`; the adapter boots the same major with an equal or
  older minor (`apps/mail-adapter/src/curie_mail_adapter/provider.py::check_interface_version`).
  The interface is internal to the adapter, not a frozen contract.
- **Conformance**: `apps/mail-adapter/src/curie_mail_adapter/provider_conformance.py::CHECKS`,
  plain functions an out-of-tree author runs from their own tests.

## Implementations today

One in tree: AgentMail
(`apps/mail-adapter/src/curie_mail_adapter/agentmail.py::AgentMailProvider`). It declares
that it cannot verify senders, because AgentMail supplies no aligned verdict a sender
cannot write, so an AgentMail install refuses every inbound message before any body
fetch. Out of tree, a provider ships as a Python package installed into an image built
`FROM` the published mail adapter image of the same release; the adapter's own tests
also drive a verifying test provider end to end.

## Known leakage

- **The provider is inside the approval trust boundary.** It runs in the process that
  holds the adapter principal. A provider that returns `verified` wrongly can approve
  anything on a route with email approvers, and the conformance kit run by its own
  author cannot rule that out. Installing one is a trust decision.
- **The chart is still AgentMail shaped.** `mailAdapter.agentmail.*` carries the
  provider's settings, secret and egress rules; the neutral `mailAdapter.provider`
  block from the ADR's sketch step 6 is not built yet, so a chart install can select
  only AgentMail.
