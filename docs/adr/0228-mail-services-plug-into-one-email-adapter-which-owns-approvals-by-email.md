# 228. Mail services plug into one email adapter, which owns approvals by email

Date: 2026-10-14

Status: Draft

This Draft records a direction the maintainer, Brian Conn, gave on 2026-10-14: a surface talks to one email adapter, each mail service is an implementation under it, and approvals by email live in that upstream adapter. It is open for discussion and authorizes no implementation until it is published as Accepted.

It builds on [ADR 0096](0096-port-adapters-are-deployed-services.md) (decision 4, in-process entry points), [ADR 0120](0120-durable-first-party-email-state.md) (the adapter's local durable state), [ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md) and its amendment (approvals by email), [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) (caller lists) and [ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md) (who an email sender is). It supersedes nothing and changes no frozen contract.

## Summary

The email adapter in `apps/mail-adapter` does two jobs in one class. It talks to one mail service, AgentMail, and it implements everything Curie needs from an email channel, including approvals answered by reply. This ADR separates those jobs. The adapter keeps every rule that does not depend on the mail service. Under it sits a small, documented **mail provider** interface, and each mail service is one implementation of that interface. AgentMail stays the implementation in this repository. A provider for another mail service, such as Microsoft Graph, is written and shipped outside this repository and plugs in without a fork. Nothing changes on the wire between the adapter and the platform, and nothing changes for an AgentMail install.

## Terms

- **Mail provider**: the code that talks to one mail service's API. It lists and reads messages, sends replies, and reports what the service knows about who sent a message.
- **Neutral adapter**: everything in `apps/mail-adapter` that is not a mail provider.
- **Sender verdict**: the provider's answer to "did the receiving mail system authenticate this sender?", built from evidence the sender cannot write themselves, such as the receiving system's own SPF, DKIM and DMARC results.
- **Out of tree**: code that lives and ships outside this repository.

## Context

The split does not exist in code today. On `next`:

1. `MailAdapter` (`apps/mail-adapter/src/curie_mail_adapter/adapter.py`) builds `AgentMailClient` (`agentmail.py`) directly and reads AgentMail's message shape inline: the `sent` label (`_labels`), `next_page_token` paging (`prime`, `poll_once`), `extracted_text` as a reply's new text without quoted history (`_parse_decision`), the `headers` map for the automatic-sender rules (`_sent_automatically`), `from`, `to` and `cc` (`_participants`), `thread_id` as the conversation id (`MailState.admit` in `state.py`), and the rule that a thread 404 means deletion only when it carries AgentMail's own JSON body (`thread_carries`). The errors that come from AgentMail calls (`ProviderEgressRefusedError`, `ProviderThreadDeletedError`) and AgentMail's egress pinning live in the same two files.
2. The same class implements approvals by email under ADR 0177 and its amendment: issuing references (`record_approval_card`), reading answers (`_handle_approval_reply`), carrying them to `POST /approvals/{id}/resolve` with the adapter principal (`_carry_answer`), and the follow-up (`settle_approval_card`). None of these rules mention AgentMail.
3. Other modules are already neutral: `egress.py` serves the reply wire from `channel_protocol`, `state.py` holds delivery state and idempotency, and `run.py`, `config.py` and `recover.py` handle boot, settings and offline recovery. Only the AgentMail settings in `config.py` are provider specific.
4. The inbound gate refuses every AgentMail message with `authentication_unverifiable` (`_listing_rejection`), because AgentMail supplies no positive sender verdict Curie can trust (`apps/mail-adapter/README.md`, "Inbound security"). No email starts a turn or answers an approval, so the approval reply code has no live inbound caller.
5. A second implementation exists. A downstream deployment runs an email adapter on Microsoft Graph, outside this repository, where the receiving mail system does report sender authentication results. Without a seam it cannot reuse this adapter: it has to reimplement the channel protocol, delivery state and approvals by email on its own.

Curie's standing restraint is that the second implementation teaches the interface (`docs/interfaces.md`, `docs/architecture-vision.md`); [ADR 0140](0140-curie-supports-one-model-harness-until-a-second-one-exists.md) applied it by not finishing a harness registry that had no second harness. Here the second implementation is real and running, so the interface is drawn from two concrete cases rather than guessed.

## Decision

### 1. Three layers

Surface (the platform's channel port) -> email adapter (this repository) -> mail provider (one per mail service).

The neutral adapter keeps, with unchanged behavior:

- **The channel protocol with the platform**: `POST /channels/turns` with the scoped channel token, the reply wire behind `X-Curie-Adapter-Secret`, and the health, readiness and status endpoints.
- **Delivery state and idempotency** (ADR 0120): admission and capacity bounds, reply ownership at `(conversation_id, reply_ref)`, completion leases, the event marker used as the provider-visible dedupe witness, terminal receipts, priming and restart confirmation.
- **The inbound gate**: the sender verdict check, domain alignment and the sender allowlist, in that order.
- **Approvals by email**, exactly as ADR 0177, its amendment and ADR 0220 define them: references, the automatic-sender rules, the first-line decision, the approver wording, the reply-all rules, the resolve call and the follow-up.
- **Polling cadence, backoff, retry budgets, the no-mail-PII logging rule**, and the `CURIE_*` settings for all of the above.

The provider owns talking to its service, its own credentials and settings, its network pinning, mapping its errors to the neutral failure kinds below, and its sender verdict.

### 2. The provider interface

A Python `Protocol`, `MailProvider`, in a new module `curie_mail_adapter/provider.py`. No operation raises for a service or network failure; each returns a result or a neutral failure.

1. **List new mail**, `list_messages(cursor, limit)`: one page of message summaries, newest first, plus the cursor for the next page or none. A summary carries the message id, the thread id and whether this inbox sent it. A provider applies its own safe filters here (AgentMail's `include_*=false` constants move with it), and a filter never counts as authentication.
2. **Read one message**, `get_message(message_id)`: a normalized message with its ids, the claimed From address, the To and Cc addresses, the new text with quoted history removed (none when the service cannot separate it, and then no decision is read from it), the full text, the headers the service returned (none when it returned none, which the adapter treats as automatic, as today), and the sender verdict.
3. **Sender verdict**, carried on every read message: `verified` with the domain that passed aligned DMARC, `failed` with a reason, or `unverifiable`. A provider also declares `verifies_senders`. One that cannot report verified senders declares false and always returns `unverifiable`.
4. **Reply in thread**, `send_reply(message_id, text, reply_all)`: accepted, or a failure. The reply lands in the same thread and goes to the sender only, or to everyone on the message when `reply_all` is true.
5. **Look for a marker in a thread**, `thread_contains(thread_id, marker)`: yes, no, deleted at the service, or unreadable. "Deleted" is returned only on the service's own definitive answer; anything ambiguous is "unreadable".
6. **Check settings**, `config_problems()`: a list of missing or invalid settings, each naming its variable, which the adapter adds to its boot gates.

At this boundary, neutral failure kinds replace HTTP statuses: `unreachable` (transport), `egress_refused` (connection refused, today's 424), `rate_limited` (today's 429, with an optional retry delay from the service), `refused` (the service said no, today's other 4xx), `server_error` (today's 5xx) and `too_large`. The adapter maps each kind to the behavior it has today, including the rule that a 5xx neither arms nor clears a poll backoff. A failure may carry a short cause, which must be bounded and contain no credential and no mail content.

Ids are opaque strings. A message id is stable across reads and restarts, because it stays the `delivery_id`. A thread id is the same for a message and every reply in its thread.

The interface carries a version number; a provider declares the version it implements, and the adapter refuses to boot on a mismatch. The interface is internal to the adapter. It is not a frozen contract and is not generated to other languages, and it changes through ordinary review with the conformance kit as the gate.

### 3. The sender verdict decides whether the channel opens

A message is refused unless its verdict is `verified`, the verified domain aligns with the domain of the claimed From address, and that address passes `CURIE_MAIL_ALLOWED_SENDERS`. After that, ADR 0175's caller list and, for answers, ADR 0177 and ADR 0220 apply as they do today. Two rules from the adapter README carry over unchanged: a provider label, the absence of a rejection label, or any header a sender could have written is never a verdict, and the allowlist never authenticates anyone.

A provider may build `verified` only from results the receiving mail system attaches and the service guarantees a sender cannot supply, for example an `Authentication-Results` header stamped by the receiving system and identified by its own server id, where the service strips or separates sender-written copies. The provider documents that source.

AgentMail keeps returning `unverifiable`, so an AgentMail install refuses every message exactly as today. A provider that can report verified senders makes turns and approvals by email work through the same neutral code, with no approval logic of its own.

### 4. How a provider is selected and loaded

- `CURIE_MAIL_PROVIDER` names the provider, defaulting to `agentmail`. The chart exposes it as `mailAdapter.provider.name`.
- `agentmail` is resolved by a direct import, never through discovery, so a broken installed package cannot take the built-in down. This is the harness registry rule ADR 0096 decision 4 names (`runner/src/curie_runner/harness/registry.py`).
- Any other name is looked up in the Python entry-point group `curie.mail_provider`. Boot fails, naming the variable, when no entry point claims the name, when more than one does, when one claims `agentmail`, or when it points at a flat top-level module. The entry point returns a factory that builds the provider from the environment.
- A provider reads its own settings from its own environment variables, as AgentMail does today with `AGENTMAIL_*`. The neutral `CURIE_MAIL_*` settings do not change.
- An out-of-tree provider ships as a Python package installed into an image built `FROM` the published mail adapter image of the same release. The chart already lets an operator point `mailAdapter.image` at that image.
- The state file records the provider name on first boot, and a later boot with a different provider refuses to start, because ids from two mail services must never mix in one state file. A new volume is a first boot, as today.

This uses ADR 0096 decision 4, the exception for in-process entry points, with its guard rules. The provider runs inside the adapter's process and never inside the platform's. Its calls sit inside the adapter's completion lease and dedupe check, where a network hop would add a second failure point and a second idempotency problem. ADR 0096 decision 2 stays open to anyone who wants a whole email adapter in another language: a deployed service speaking the channel protocol.

### 5. Proving a provider conforms

A conformance kit ships in the adapter package as `curie_mail_adapter.provider_conformance`, shaped like `channel_protocol.conformance`: plain data and plain functions with no test runner, so an out-of-tree author runs it from their own tests against a fake or a sandbox mailbox of their service. The author supplies a subject that can place a message in the inbox. The checks:

1. Paging walks to the end without repeats, and mail this inbox sent is flagged.
2. Ids are stable across reads, and a reply lands in its parent's thread.
3. New text excludes quoted history: a reply that quotes `APPROVE` yields new text without it.
4. A message with no verifiable evidence, and a message carrying forged sender-written authentication headers, is never `verified`. A provider that declares `verifies_senders` false never returns `verified`.
5. Sender-only and reply-all sends reach exactly the expected recipients, and `thread_contains` finds a marker after a send.
6. Each failure kind is reported for its own cause, and "deleted" only for the service's definitive answer.
7. No failure cause contains a configured credential.

The in-tree AgentMail provider passes the kit against the existing fake AgentMail server. The neutral adapter's own tests, approvals included, also run against a test provider that can return `verified`, which drives the approval reply path end to end through the real adapter and the fake platform for the first time. As the repository requires, only the external services are faked.

## Consequences

Good:

- One implementation of approvals by email and one inbound gate serve every mail service, and a fix lands once.
- The approval reply code gets a live caller, in tests and in any deployment whose provider verifies senders.
- A deployment on another mail service needs a provider package and a derived image, not a fork.
- AgentMail installs see no change: the same settings, chart values, behavior and refusal.
- Once the code draws this line, the seam is registered in `docs/interfaces.md` with its own `INTERFACE.md`, counting one in-tree implementation (AgentMail) plus out-of-tree providers. This Draft adds no registry row, because the catalog documents only lines the code already draws.

Bad:

- An out-of-tree provider rebuilds its derived image for every adapter release. The version check turns skew into a boot failure instead of a silent bug, but the rebuild is still a cost.
- The interface and the kit are new surface to keep honest, and any interface change breaks out-of-tree providers.
- The chart's AgentMail values (`mailAdapter.agentmail.*`, including its egress rules) are provider specific. Another provider needs a neutral way to pass its settings, secret and egress rules, which is chart work.
- Upstream cannot run an out-of-tree provider's live tests. Upstream proves the neutral adapter and AgentMail; each out-of-tree author proves their provider with the kit.
- Docs that cite `MailAdapter._listing_rejection` (the adapter README, `apps/mail-adapter/CLAUDE.md`, `docs/interfaces/channel-ingress/INTERFACE.md`) must move to the new gate.

## Rejected alternatives

1. **Bring the Microsoft Graph provider upstream.** Rejected by the maintainer's direction. It serves one deployment's mail service, would bring that service's app registration, consent and token handling into the public repository, and adds tests upstream cannot run. A later ADR can revisit this if several deployments need the same provider.
2. **Keep approvals provider specific, inside each adapter.** Rejected. ADR 0177's reply rules and the inbound gate are security rules. Every copy drifts and needs its own proof, which is the situation this ADR ends.
3. **A separate network service per provider.** Rejected. It adds a process, a credential between adapter and provider, and a network hop inside the completion lease, for code that is a few hundred lines of API calls.
4. **A general plugin framework for every channel adapter.** Rejected. ADR 0096 decision 5 promotes ports one at a time, on demand, and email is the only channel with two mail services today.
5. **Load a provider from an import path in configuration.** Rejected. It turns a setting into an arbitrary code loader without the guards entry points already have in this repository.

## Implementation sketch (not authorized until Accepted)

1. Add `provider.py`: the `MailProvider` protocol, its result and failure types, the interface version, and the loader with its guards.
2. Move `agentmail.py` behind the protocol as the built-in provider, taking the label, paging, `extracted_text`, header map and 404 body rules with it. Its `AGENTMAIL_*` settings and chart values stay as they are.
3. Make `MailAdapter` call only the protocol, and replace the constant refusal with the verdict gate of decision 3.
4. Record the provider name in the state file (a schema version bump) and refuse a mismatch at boot.
5. Add `provider_conformance` and run it against AgentMail and the test provider in `apps/mail-adapter/tests`.
6. Add `mailAdapter.provider` to the chart: the name, plus a neutral settings, secret and egress block.
7. Update the adapter README, `apps/mail-adapter/CLAUDE.md` and the channel-ingress interface doc, and add the seam's `INTERFACE.md`.

The existing adapter suite must pass unchanged, an AgentMail install must still refuse every message, and the approval reply path must pass end to end with a verifying test provider. Tiers follow the tier decision rule in `AGENTS.md`; the chart change makes the cluster tier required.
