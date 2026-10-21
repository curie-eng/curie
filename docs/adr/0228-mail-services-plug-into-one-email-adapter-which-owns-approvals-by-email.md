# 228. Mail services plug into one email adapter, which owns approvals by email

Date: 2026-10-14

Status: Accepted

Accepted 2026-10-16 with explicit maintainer approval from Brian Conn, recorded as an approving review on the publishing pull request ([#4588](https://github.com/curie-eng/curie/pull/4588)).

This ADR records a direction the maintainer, Brian Conn, gave on 2026-10-14: a surface talks to one email adapter, each mail service is an implementation under it, and approvals by email live in that upstream adapter.

It builds on [ADR 0096](0096-port-adapters-are-deployed-services.md) (decision 4, in-process entry points), [ADR 0120](0120-durable-first-party-email-state.md) (the adapter's local durable state), [ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md) and its amendment (approvals by email), [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) (caller lists) and [ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md) (who an email sender is). It supersedes nothing and changes no frozen contract.

## Summary

The email adapter in `apps/mail-adapter` does two jobs in one class. It talks to one mail service, AgentMail, and it implements everything Curie needs from an email channel, including approvals answered by reply. This ADR separates those jobs. The adapter keeps every rule that does not depend on the mail service. Under it sits a small, documented **mail provider** interface, and each mail service is one implementation of that interface. AgentMail stays the implementation in this repository. A provider for another mail service, such as Microsoft Graph, is written and shipped outside this repository and plugs in without a fork. Nothing changes on the wire between the adapter and the platform, and nothing changes for an AgentMail install.

Today no email reaches the platform at all: the intake path was removed, not only gated (Context, item 4). So this ADR also rebuilds that intake path, once, in the neutral adapter, behind a gate that only a provider able to verify senders can open.

## Terms

- **Mail provider**: the code that talks to one mail service's API. It lists and reads messages, sends replies, and reports what the service knows about who sent a message.
- **Neutral adapter**: everything in `apps/mail-adapter` that is not a mail provider.
- **Sender verdict**: the provider's answer to "did the receiving mail system authenticate this sender?", built from evidence the sender cannot write themselves, such as the receiving system's own SPF, DKIM and DMARC results.
- **Intake path**: what turns one listed message into a turn or an approval answer: fetching the message, building the turn, storing it, and delivering it to the platform.
- **Out of tree**: code that lives and ships outside this repository.

## Context

The split does not exist in code today. On `next`:

1. `MailAdapter` (`apps/mail-adapter/src/curie_mail_adapter/adapter.py`) builds `AgentMailClient` (`agentmail.py`) directly and reads AgentMail's message shape inline. The live paths read the `sent` label (`_labels`) and `next_page_token` paging (`prime`, `poll_once`), the thread listing's text fields and the rule that a thread 404 means deletion only when it carries AgentMail's own JSON body (`thread_carries`, the reply dedupe witness), and `from`, `to` and `cc` on the asking message when an approval request is worded (`_participants`, through `_on_thread`). Code with no live caller reads more of that shape: `thread_id` as the conversation id (`MailState.admit` in `state.py`), `extracted_text` as a reply's new text (`_parse_decision`), and the `headers` map (`_sent_automatically`). The errors that come from AgentMail calls (`ProviderEgressRefusedError`, `ProviderThreadDeletedError`) and AgentMail's egress pinning live in the same two files.
2. The same class implements approvals by email under ADR 0177 and its amendment. Issuing references (`record_approval_card`) and the follow-up (`settle_approval_card`) are live: they run when the worker renders or settles an approval in a conversation the adapter already owns. Reading answers (`_handle_approval_reply`) and carrying them to `POST /approvals/{id}/resolve` with the adapter principal (`_carry_answer`) have no caller. None of these rules mention AgentMail.
3. Other modules are already neutral: `egress.py` serves the reply wire from `channel_protocol`, `state.py` holds delivery state and idempotency, and `run.py`, `config.py` and `recover.py` handle boot, settings and offline recovery. One AgentMail setting is used by neutral code: the inbox's own address, `AGENTMAIL_INBOX` (`config.agentmail_inbox`). It is the `address` of every turn, and so part of every conversation's identity; it is the target the reply wire checks every event against (`egress.py`); and it is the address `_participants` removes so the inbox never counts itself.
4. The intake path is gone. Commit 54fdb1132 ("Refuse mail without verifiable sender authentication", #3822) replaced it with a constant refusal. `_listing_rejection` returns `authentication_unverifiable` for every listed message, and `prime` and `poll_once` record an id-only `rejected` receipt without fetching a body. The commit removed the body fetch, the turn assembly (the subject plus the body, falling back from `extracted_text` and `text` to `extracted_html` and `html`), the calls to `store_turn`, `_deliver_turn`, and the retry of `ingress_pending` rows; `_retry_pending` now settles any pending row left from before as `rejected`. On `next`, `post_turn`, `_handle_approval_reply`, `sender_allowed`, `MailState.admit` and `MailState.store_turn` have no callers. No email starts a turn or answers an approval. The reason is in the adapter README ("Inbound security"): AgentMail supplies no positive sender verdict Curie can trust. What still runs is reply egress and approval follow-ups for conversations admitted before that commit.
5. A second implementation exists. A downstream deployment runs an email adapter on Microsoft Graph, outside this repository, where the receiving mail system does report sender authentication results. Without a seam it cannot reuse this adapter: it has to reimplement the channel protocol, delivery state, the intake path and approvals by email on its own.

Curie's standing restraint is that the second implementation teaches the interface (`docs/interfaces.md`, `docs/architecture-vision.md`); [ADR 0140](0140-curie-supports-one-model-harness-until-a-second-one-exists.md) applied it by not finishing a harness registry that had no second harness. Here the second implementation is real and running, so the interface is drawn from two concrete cases rather than guessed.

## Decision

### 1. Three layers

Surface (the platform's channel port) -> email adapter (this repository) -> mail provider (one per mail service).

The neutral adapter keeps, with unchanged behavior:

- **The channel protocol with the platform**: `POST /channels/turns` with the scoped channel token, the reply wire behind `X-Curie-Adapter-Secret`, and the health, readiness and status endpoints.
- **Delivery state and idempotency** (ADR 0120): admission and capacity bounds, reply ownership at `(conversation_id, reply_ref)`, completion leases, the event marker used as the provider-visible dedupe witness, terminal receipts, priming and restart confirmation.
- **The inbound gate**, in the order decision 3 gives.
- **Approvals by email**, exactly as ADR 0177, its amendment and ADR 0220 define them: references, the automatic-sender rules, the first-line decision, the approver wording, the reply-all rules, the resolve call and the follow-up.
- **Polling cadence, backoff, retry budgets, the no-mail-PII logging rule**, and the `CURIE_*` settings for all of the above.

It also gets back **the intake path**, rebuilt once on the provider interface. The turn's text is the subject, a blank line, then the first of the new text, the full text, the new HTML and the full HTML that is present, as before 54fdb1132, and the size limit and the `oversize` settlement apply as they did. The adapter never reads an approval decision from HTML.

The provider owns talking to its service, its own credentials and settings, its network pinning, mapping its errors to the neutral failure kinds below, and its sender verdict.

### 2. The provider interface

A Python `Protocol`, `MailProvider`, in a new module `curie_mail_adapter/provider.py`. No operation raises for a service or network failure; each returns a result or a neutral failure.

A provider declares, once, when it is built:

1. **Its interface version**, as major and minor (see the end of this section).
2. **Its inbox address**, `inbox_address`: the one bare address this mailbox receives at. The neutral adapter uses it as the turn's `address`, as the reply wire's target check, and to remove the inbox from an approval's participants. The provider exposes it, rather than a new neutral `CURIE_MAIL_*` setting, because the provider must already know it to reach the mailbox at all, and two settings for one fact can disagree: a mismatch would answer every reply event 422 and make the inbox count itself as a participant. The built-in provider returns `AGENTMAIL_INBOX`, so an AgentMail install keeps its setting and every conversation keeps its identity. Boot fails when it is empty or not one bare address.
3. **Whether it verifies senders**, `verifies_senders`. One that cannot report verified senders declares false and always returns `unverifiable`.
4. **How soon a sent reply is visible**, `sent_visible_within`: zero for a service with read-after-write visibility (see operation 4), or a bound in seconds.

Its operations:

1. **List new mail**, `list_messages(cursor, limit)`: one page of message summaries, newest first, plus the cursor for the next page or none. A summary carries the message id, the thread id, whether this inbox sent it, and the From mailboxes as listed (claimed, never trusted). A provider applies its own safe filters here (AgentMail's `include_*=false` constants move with it), and a filter never counts as authentication.
2. **Read one message**, `get_message(message_id)`: a normalized message with its ids; the subject; every claimed From mailbox; the To and Cc addresses; the new text with quoted history removed (none when the service cannot separate it, and then no decision is read from it); the full text; the new and full HTML bodies the service returned (none when absent); the headers the service returned (none when it returned none, which the adapter treats as automatic, as today); and the sender verdict.
3. **Sender verdict**, carried on every read message: `verified` with the RFC5322.From domain that DMARC evaluated (the `header.from` of the receiving system's DMARC result), `failed` with a reason, or `unverifiable`.
4. **Reply in thread**, `send_reply(message_id, text, reply_all)`: accepted, or a failure. The reply lands in the same thread and goes to the sender only, or to everyone on the message when `reply_all` is true. "Accepted" means the service has taken the reply for delivery and, when `sent_visible_within` is zero, that `thread_contains` finds the reply's marker from the moment `send_reply` returns. The adapter's send dedupe depends on that: on a retry it sends again only when the marker is not in the thread.
5. **Look for a marker in a thread**, `thread_contains(thread_id, marker)`: yes, no, deleted at the service, or unreadable. "Deleted" is returned only on the service's own definitive answer; anything ambiguous is "unreadable".
6. **Check settings**, `config_problems()`: a list of missing or invalid settings, each naming its variable, which the adapter adds to its boot gates.

A service that queues a send and files the sent copy later (for example, a send call that answers 202) cannot promise read-after-write, so its provider declares the bound within which a sent copy becomes visible. The adapter then records each send attempt durably on the completion event before calling `send_reply`. On a retry of an attempted send, while the bound has not passed since that attempt, it treats "no" from `thread_contains` as "unreadable" and retries later instead of sending; after the bound, "no" means the send did not land, and it sends again. The same rule covers a send whose outcome was lost, such as a timeout after the service took it. A provider that can promise neither read-after-write nor a bound does not conform.

At this boundary, neutral failure kinds replace HTTP statuses: `unreachable` (transport), `egress_refused` (connection refused, today's 424), `rate_limited` (today's 429, with an optional retry delay from the service), `refused` (the service said no, today's other 4xx), `server_error` (today's 5xx) and `too_large`. The adapter maps each kind to the behavior it has today, including the rule that a 5xx neither arms nor clears a poll backoff. A failure may carry a short cause, which must be bounded and contain no credential and no mail content.

Ids are opaque strings. A message id is stable across reads and restarts, because it stays the `delivery_id`. A thread id is the same for a message and every reply in its thread.

The interface version is `major.minor`. An addition that an older provider can safely lack, such as a new optional field or a new operation, raises the minor and names what the adapter does when the provider predates it. Any change to the meaning of something that exists raises the major. The adapter boots a provider with the same major and an equal or older minor, and refuses, naming both versions, a different major or a newer minor. The interface is internal to the adapter. It is not a frozen contract and is not generated to other languages, and it changes through ordinary review with the conformance kit as the gate.

### 3. The sender verdict decides whether the channel opens

The gate runs in this order, and every refusal leaves the same id-only `rejected` receipt it leaves today:

1. **Before any fetch, a provider that does not verify senders.** When `verifies_senders` is false, every listed message is refused with `authentication_unverifiable` from the listing alone, with no body fetch. AgentMail declares false, so an AgentMail install refuses every message exactly as today.
2. **Before any fetch, the listed From.** A summary that does not list exactly one From mailbox, or whose From does not pass `CURIE_MAIL_ALLOWED_SENDERS`, is refused without a fetch. This is a cheap filter, not authentication.
3. **After the fetch, the verdict and the sender.** The message is refused unless it has exactly one From mailbox, that address passes `CURIE_MAIL_ALLOWED_SENDERS`, the verdict is `verified`, and the verdict's domain is equal to the domain of that From address. Equal means the same name, compared case-insensitively: no organizational-domain rule and no public suffix list, so a parent domain or a subdomain is not equal. DMARC evaluates the From domain by definition, so a verdict for any other domain belongs to another header or another message.

After that, ADR 0175's caller list and, for answers, ADR 0177 and ADR 0220 apply as they do today. Two rules from the adapter README carry over unchanged: a provider label, the absence of a rejection label, or any header a sender could have written is never a verdict, and the allowlist never authenticates anyone.

A provider may build `verified` only from results the receiving mail system attaches and the service guarantees a sender cannot supply, for example an `Authentication-Results` header stamped by the receiving system and identified by its own server id, where the service strips or separates sender-written copies. The provider documents that source.

A provider that can report verified senders makes turns and approvals by email work through the same neutral code, with no approval logic of its own.

### 4. How a provider is selected and loaded

- `CURIE_MAIL_PROVIDER` names the provider, defaulting to `agentmail`. The chart exposes it as `mailAdapter.provider.name`.
- `agentmail` is resolved by a direct import, never through discovery, so a broken installed package cannot take the built-in down. This is the harness registry rule ADR 0096 decision 4 names (`runner/src/curie_runner/harness/registry.py`).
- Any other name is looked up in the Python entry-point group `curie.mail_provider`. Boot fails, naming the variable, when no entry point claims the name, when more than one does, when one claims `agentmail`, or when it points at a flat top-level module. The entry point returns a factory that builds the provider from the environment.
- A provider reads its own settings from its own environment variables, as AgentMail does today with `AGENTMAIL_*`. The neutral `CURIE_MAIL_*` settings do not change.
- An out-of-tree provider ships as a Python package installed into an image built `FROM` the published mail adapter image of the same release. The chart already lets an operator point `mailAdapter.image` at that image.
- The state file records the provider name on first boot, and a later boot with a different provider refuses to start, because ids from two mail services must never mix in one state file. A new volume is a first boot, as today.

The published image (`apps/mail-adapter/Dockerfile`) installs the adapter into `/app/.venv`, which uv built and which has no pip of its own, and runs as `USER 1000`, which owns that directory. The base image's pip can install into it, so a derived image needs no user switch:

```dockerfile
FROM ghcr.io/curie-eng/curie-mail-adapter:<same release>
COPY example-mail-provider /tmp/example-mail-provider
RUN pip --python /app/.venv/bin/python freeze | grep '==' > /tmp/adapter-pins.txt \
 && pip --python /app/.venv/bin/python install --no-cache-dir \
      -c /tmp/adapter-pins.txt /tmp/example-mail-provider \
 && pip --python /app/.venv/bin/python check
```

The constraints file pins every third-party package the adapter already has, so installing the provider cannot move a dependency the adapter was locked to, and `pip check` fails the build on a conflict. A provider that lists `curie-mail-adapter` as a dependency finds it already installed. A step that needs root, such as an OS package, switches to `USER root` and must switch back to `USER 1000` before the image ends.

This uses ADR 0096 decision 4, the exception for in-process entry points, with its guard rules. The provider runs inside the adapter's process and never inside the platform's. Its calls sit inside the adapter's completion lease and dedupe check, where a network hop would add a second failure point and a second idempotency problem. ADR 0096 decision 2 stays open to anyone who wants a whole email adapter in another language: a deployed service speaking the channel protocol.

### 5. Proving a provider conforms

A conformance kit ships in the adapter package as `curie_mail_adapter.provider_conformance`, shaped like `channel_protocol.conformance`: plain data and plain functions with no test runner, so an out-of-tree author runs it from their own tests against a fake or a sandbox mailbox of their service. The author supplies a subject that can place a message in the inbox. The checks:

1. Paging walks to the end without repeats, and mail this inbox sent is flagged.
2. Ids are stable across reads, a reply lands in its parent's thread, and `inbox_address` is the address mail to the inbox arrives at.
3. New text excludes quoted history: a reply that quotes `APPROVE` yields new text without it. The subject is reported, and an HTML-only message yields no text and an HTML body.
4. A message with no verifiable evidence, and a message carrying forged sender-written authentication headers, is never `verified`. A `verified` verdict names the From domain DMARC evaluated. A message with two From mailboxes reports both. A provider that declares `verifies_senders` false never returns `verified`.
5. Sender-only and reply-all sends reach exactly the expected recipients. Right after `send_reply` returns accepted, with no wait, `thread_contains` finds the marker when `sent_visible_within` is zero; otherwise it finds it no later than the declared bound.
6. Each failure kind is reported for its own cause, and "deleted" only for the service's definitive answer.
7. No failure cause contains a configured credential.

The in-tree AgentMail provider passes the kit against the existing fake AgentMail server. The neutral adapter's own tests, approvals included, also run against a test provider that can return `verified`, which drives the intake path and the approval reply path end to end through the real adapter and the fake platform. As the repository requires, only the external services are faked.

## Consequences

Good:

- One implementation of the intake path, approvals by email and the inbound gate serves every mail service, and a fix lands once.
- The intake path and the approval reply code get live callers again, in tests and in any deployment whose provider verifies senders.
- A deployment on another mail service needs a provider package and a derived image, not a fork.
- AgentMail installs see no change: the same settings, chart values, behavior and refusal, still before any body fetch.
- Once the code draws this line, the seam is registered in `docs/interfaces.md` with its own `INTERFACE.md`, counting one in-tree implementation (AgentMail) plus out-of-tree providers. This ADR adds no registry row, because the catalog documents only lines the code already draws.

Bad:

- **The provider is inside the approval trust boundary.** It runs in the process that holds the adapter principal, and the resolve call carries whatever sender the adapter names as the actor. A provider that returns `verified` wrongly, by mistake or on purpose, can approve anything on a route that lists email approvers, and the platform cannot tell. The conformance kit does not close this: its author runs it, and check 4 against the author's own fake mostly proves the fake. Installing an out-of-tree provider is a trust decision equal to handing it the adapter principal, and the operator who installs one owns that decision.
- An out-of-tree deployment rebuilds its derived image to take each adapter release. The provider's own code changes only on a major version, and the version check turns skew into a boot failure instead of a silent bug, but the rebuild is still a cost.
- The interface and the kit are new surface to keep honest. Every minor addition needs a defined fallback for older providers, and a major change breaks every out-of-tree provider.
- With a provider that lacks read-after-write visibility, a retried send waits out the declared bound, so a crash during a send delays that reply by up to the bound.
- The chart's AgentMail values (`mailAdapter.agentmail.*`, including its egress rules) are provider specific. Another provider needs a neutral way to pass its settings, secret and egress rules, which is chart work.
- Upstream cannot run an out-of-tree provider's live tests. Upstream proves the neutral adapter and AgentMail; each out-of-tree author proves their provider with the kit.
- Docs that describe the constant refusal must describe the gate instead: the adapter README, `apps/mail-adapter/CLAUDE.md`, `docs/interfaces/channel-ingress/INTERFACE.md` (which cites `MailAdapter._listing_rejection`), `ARCHITECTURE.md`, `docs/operations.md` and `docs/guides/building-a-channel-adapter.md`.

## Rejected alternatives

1. **Bring the Microsoft Graph provider upstream.** Rejected by the maintainer's direction. It serves one deployment's mail service, would bring that service's app registration, consent and token handling into the public repository, and adds tests upstream cannot run. A later ADR can revisit this if several deployments need the same provider.
2. **Keep approvals provider specific, inside each adapter.** Rejected. ADR 0177's reply rules and the inbound gate are security rules. Every copy drifts and needs its own proof, which is the situation this ADR ends.
3. **A separate network service per provider.** Rejected. It adds a process, a credential between adapter and provider, and a network hop inside the completion lease, for code that is a few hundred lines of API calls.
4. **A general plugin framework for every channel adapter.** Rejected. ADR 0096 decision 5 promotes ports one at a time, on demand, and email is the only channel with two mail services today.
5. **Load a provider from an import path in configuration.** Rejected. It turns a setting into an arbitrary code loader without the guards entry points already have in this repository.
6. **A neutral `CURIE_MAIL_*` setting for the inbox address.** Rejected in decision 2: it duplicates a fact the provider already holds, and the two can disagree.
7. **Convert stored AgentMail listings to the normalized summary on upgrade.** Rejected in step 4 of the sketch: it needs AgentMail-specific code in a neutral migration, and today every such row is refused on its first retry anyway.

## Implementation sketch (not authorized until Accepted)

1. Add `provider.py`: the `MailProvider` protocol, its result and failure types, the interface version, and the loader with its guards.
2. Move `agentmail.py` behind the protocol as the built-in provider, taking the label, paging, `extracted_text`, HTML, header map and 404 body rules with it, and returning `AGENTMAIL_INBOX` as its inbox address. Its `AGENTMAIL_*` settings and chart values stay as they are.
3. Make `MailAdapter` call only the protocol. Replace the constant refusal with the gate of decision 3 and rebuild the intake path on it: the body fetch, turn assembly, `store_turn`, delivery and the retry of `ingress_pending` rows. Record send attempts for providers with a visibility bound.
4. Bump the state schema. The migration records `agentmail` as the provider of an existing state file, since every earlier file was written by AgentMail. It settles every row still in `body_pending` or `ingress_pending` as `rejected`, closing the reply slot a stored turn opened, which is what `_retry_pending` already does to those rows on its first pass. It replaces every remaining `summary_json` with the id-only receipt, so no row keeps AgentMail's raw listing and no provider ever reads another service's shape. It adds the send-attempt time to completion events. A boot with a different provider than the recorded one refuses to start.
5. Add `provider_conformance` and run it against AgentMail and the test provider in `apps/mail-adapter/tests`.
6. Add `mailAdapter.provider` to the chart: the name, plus a neutral settings, secret and egress block.
7. Update the docs listed under Consequences, and add the seam's `INTERFACE.md`.

The tests added with 54fdb1132 (`test_authentication_gate.py` and the reworked approval and ingress tests) drive `handle_inbound`, `prime` and `poll_once` and assert the constant refusal, so they change with step 3. What they pin must still hold, now through the provider interface:

- An AgentMail install refuses every listed message with `authentication_unverifiable` before any body fetch, including an allowlisted sender, positive provider labels and forged authentication headers, with an id-only receipt and one warning that names neither the sender nor the message.
- Prime, poll and the pending retry apply the same gate, and a row left pending from before never posts a turn.
- A delivery accepted before the gate keeps its reply ownership and still gets its reply.
- No approval answer is carried without a `verified` verdict, and a sender the allowlist refuses never reaches approval logic.
- The wildcard sender filter needs explicit consent at boot and never authenticates anyone.
- Reply egress, the dedupe witness, the deleted-thread rule, approval wording and follow-ups behave as today.

The approval reply path must also pass end to end with a verifying test provider. Tiers follow the tier decision rule in `AGENTS.md`; the chart change makes the cluster tier required.
