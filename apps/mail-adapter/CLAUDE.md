# CLAUDE.md - apps/mail-adapter

The email channel adapter: an AgentMail inbox bridged to a Curie channel
binding. Full behavior spec lives in `apps/mail-adapter/README.md`; this file is
the enforceable-rule summary.

## Load-bearing invariants

- **The adapter holds no platform API key, no queue credential, and no platform
  database access.** Its credentials are `CURIE_CHANNEL_TOKEN` (presented as
  `X-API-Key` on ingress), `CURIE_EGRESS_SECRET` (checked on every inbound POST),
  `AGENTMAIL_API_KEY`, and the optional `CURIE_ADAPTER_PRINCIPAL` (ADR-0156,
  presented only on `POST /approvals/{id}/resolve` when carrying an answer). Do not add `CURIE_API_KEY`, a Valkey client, or a DB
  session to the platform here; a capability the adapter does not hold cannot be
  stolen from it, and re-minting an expired `chn` token is an operator step for
  exactly that reason. Its local SQLite file is delivery state, not a platform
  capability, and must never contain any of the three credentials.
- **Reply text and its send target are owned at `(conversation_id, reply_ref)`.**
  Every update and completion uses that exact durable pair, and the send target
  is only the event's `target.reply_ref`. Never derive a target or accumulated
  text from conversation-global state: two turns in one thread must not clear,
  inherit, or redirect one another's reply.
- **A progress body is acknowledged and ignored.** `EgressHandler.dispatch`
  returns 200 for a `reply.update` or `reply.post` carrying `progress` before
  it reaches `record_text`, so deliberate progress never replaces, clears or
  appends to the buffered reply. Do not render it into the email.
- **Nothing is recorded as replied until the provider has accepted the send.**
  A TCP connection refusal during the AgentMail witness or send returns 424
  with the fixed body `{"detail":"provider egress refused"}`. The worker stores
  that fixed cause against the matching outbox generation and keeps the
  completion owed. Other retryable witness or send failures return 502; a
  duplicate still in flight returns 503. Acking 200 in any of these cases makes
  the worker clear its durable completion record (`kernel.py`
  `clear_completion`, on any 2xx) and the email is gone with no retry and no
  dead letter. Keep 424, 502 and 503 distinct in the worker's diagnostics.
- **A completion claim is a timed, reclaimable durable lease.** A crash may
  leave a live lease, so restart or expiry must reclaim it and consult the
  provider-visible event witness before deciding whether to send. An unreadable
  witness or a completion with no admitted reply row is 502/no send; an active
  lease owned by this process is 503. Never turn either case into 200 or a
  permanent 503. A confirmed provider thread 404 for an admitted reply is the
  exception: persist a deleted receipt and return 410, including on duplicates
  and restart, so the worker dead-letters it without claiming delivery.
- **Reply all only for the approval request and its outcome** (ADR-0177 amendment A5). Every other send stays sender-only, so the resumed answer reaches the requester. Never mail an address that is not already on the thread: the requester copies approvers in. The card's `Approver` fields word the email and decide nothing.
- **`list_messages` always sends all three `include_*=false`.** They are
  constants in `agentmail.py`, not parameters and not config, so no caller and no
  operator can turn them on. Sending them when they are already the provider's
  default is the point: a changed default cannot silently widen the install.
- **Admission is bounded before state or body allocation.** At the pending or
  state-byte cap, leave provider mail unclaimed and unmodified so later capacity
  can recover it. Never evict an unresolved delivery merely to admit a newer one.
  Terminal `completion_events` (`delivered=1` or `deleted=1`) are compacted
  oldest-first under the derived cap; unresolved and leased completion rows are
  never evicted to make room.
- **Logs carry no raw mail PII.** Do not log sender addresses, subjects, bodies,
  provider message/thread ids, or reply text. Use a one-way correlation token
  and a reason/state label so operators can join retries without copying mail
  content into the cluster log-retention system.
- **Every log record leaves through the shared service logger, and that filter
  is a backstop, not a licence.** `main()` calls `bootstrap_service_telemetry`
  on the *package* logger `curie_mail_adapter`, which installs one redacting
  single-line-JSON stderr handler and sets `propagate=False` there; `run`,
  `adapter` and `egress` each hold a `getLogger(__name__)` child, so one
  bootstrap covers all three and nothing walks past it to a root handler. Do not
  reintroduce `logging.basicConfig` (it installs a root handler and the same
  record is then emitted twice, once unformatted), and do not attach a handler
  that bypasses the service logger -- either move re-opens the unfiltered path
  this closed. The shared `REDACTION_RULES` now match this adapter's credential
  *shapes*: a bare `chn.{payload}.{signature}` token, an AgentMail `am_` key,
  `CURIE_*_TOKEN=` / `*_SECRET=` assignments, and an `X-API-Key: <value>`
  header. An unprefixed `CURIE_EGRESS_SECRET` value still needs that
  assignment or header context -- the filter will not claim to recognize the
  secret as an arbitrary string. The "no raw mail PII" rule above is likewise a
  code-level obligation the filter cannot enforce: no rule matches an address,
  subject, body or provider id. Keep credentials and mail content out of the
  record in the first place; the filter only catches the shapes it knows.
  Deliberately absent for now: this adapter authors **no spans**. The bootstrap
  installs the resource and the exporters, so records and any future spans carry
  `service.name: curie-mail-adapter`, but neither the poll loop nor the egress
  path is instrumented -- traces will show nothing from it until that lands.
- **One SQLite file has one serialized writer and the chart pins one replica.**
  Every poller and egress transaction uses the adapter-owned lock. Adding a
  second replica or changing `Recreate` to rolling update creates two writers;
  horizontal scale needs a separately accepted shared-store design.

## Inbound gate

**Every current email approval answer is refused by the same authentication
gate as a turn**, before body fetch or approval resolution. The existing
reference parser and resolution transport have no current inbound caller.
They cannot authenticate a sender and must never be exposed to unauthenticated
intake.

The parser's ADR-0177 rules remain: an answer is never a turn, its reference
must be live and issued in this thread, it must not be sent automatically
(missing headers count as automatic), and its decision word is on the first
line of `extracted_text` only, never the full body. The reference links a reply
to its approval; it is not proof of identity. **Who may answer is the platform's
decision** (ADR-0177 amendment): the actor
is the sender's bare lowercased address, never a display name, and the
platform checks the binding's `allowed_callers` and the route's approver
`emails`. Do not add a local approver or requester filter. A
`caller_not_allowed` refusal gets nothing back. Never respond to an automatic
message. A settled card spends its reference.

**Inbound mail fails closed before a turn or approval can be admitted.** A
sender must match the allowlist and have a positive authentication verdict that
Curie itself verifies. A provider label, absence of a rejection label, or
provider supplied header never counts as that verdict. AgentMail supplies no
trusted positive aligned verdict, guarantees neither header provenance nor
stripping, and permits DMARC failure under `p=none`; every message is therefore
refused with `authentication_unverifiable`. Never describe the allowlist or
provider filtering as authenticating a sender. See the provider evidence in the
README before proposing a new authentication source.

**`conversations` is written only after sender authentication and allowlist
checks pass.** Never seed it from a poll listing. Every current AgentMail
message fails authentication and must not create a conversation or egress target.

**An empty allowlist fails boot while ingress is enabled.** The error asks
for explicit sender addresses or domains and never suggests `*`. A list
containing `*` fails boot unless `CURIE_MAIL_ALLOW_ALL_SENDERS=true` is explicitly
set. Its default is `false`; the chart key is `mailAdapter.allowAllSenders`.
This opt in permits wildcard configuration only and never bypasses sender
authentication. Every current AgentMail message is still refused.

## Config surface

`MailAdapterConfig()` (a frozen `pydantic_settings.BaseSettings` using
`AliasOnlyEnvSource`) reads `AGENTMAIL_*`, `CURIE_API_URL`,
`CURIE_CHANNEL_TOKEN`, `CURIE_EGRESS_SECRET`, `ADAPTER_INGRESS_ENABLED` and the
`CURIE_MAIL_*` knobs. Full table in `apps/mail-adapter/README.md`, and
`tests/test_config.py` fails if the table and the code drift apart. A new field
means a new README row.

## Verify (AgentMail-free)

```bash
uv run pytest apps/mail-adapter/tests -q
```

Only the two external dependencies are faked, both as real local
`ThreadingHTTPServer` instances: AgentMail's API and the platform's channel
ingress. Nothing inside `curie_mail_adapter` is patched. The fake AgentMail
server exercises the adapter's own admission decision, including unlabelled
spoofed mail, allowlisted senders without verifiable verdicts, and approval
answers. Provider filtering cannot stand in for that decision. A new test that
patches an internal function instead of driving it through those servers does
not meet this package's bar.
