# CLAUDE.md - apps/mail-adapter

The email channel adapter: an AgentMail inbox bridged to a Curie channel
binding. Full behavior spec lives in `apps/mail-adapter/README.md`; this file is
the enforceable-rule summary.

## Load-bearing invariants

- **The adapter holds no platform API key, no queue credential, and no platform
  database access.** Its only credentials are `CURIE_CHANNEL_TOKEN` (presented as
  `X-API-Key` on ingress), `CURIE_EGRESS_SECRET` (checked on every inbound POST)
  and `AGENTMAIL_API_KEY`. Do not add `CURIE_API_KEY`, a Valkey client, or a DB
  session to the platform here; a capability the adapter does not hold cannot be
  stolen from it, and re-minting an expired `chn` token is an operator step for
  exactly that reason. Its local SQLite file is delivery state, not a platform
  capability, and must never contain any of the three credentials.
- **Reply text and its send target are owned at `(conversation_id, reply_ref)`.**
  Every update and completion uses that exact durable pair, and the send target
  is only the event's `target.reply_ref`. Never derive a target or accumulated
  text from conversation-global state: two turns in one thread must not clear,
  inherit, or redirect one another's reply.
- **Nothing is recorded as replied until the provider has accepted the send.**
  A `turn.completed` whose AgentMail send failed acks 502 and one whose duplicate
  is still in flight acks 503. Acking 200 in either case makes the worker clear
  its durable completion record (`kernel.py` `clear_completion`, on any 2xx) and
  the email is gone with no retry and no dead letter. Do not collapse 502 and 503
  into one code: they mean different things in the worker's log.
- **A completion claim is a timed, reclaimable durable lease.** A crash may
  leave a live lease, so restart or expiry must reclaim it and consult the
  provider-visible event witness before deciding whether to send. An unreadable
  witness or a completion with no admitted reply row is 502/no send; an active
  lease owned by this process is 503. Never turn either case into 200 or a
  permanent 503.
- **The inbound gate is two checks, in order: the provider's verdict labels, then
  the allow-list.** They are not equivalent and the ordering is not incidental.
  The provider's filtering is the real control; the label check is defense in
  depth that should never fire in a correct install; the allow-list is a filter on
  an attacker-controlled `From` header. **Never describe the allow-list as
  authenticating a sender** in code, comments, docs or chart values: Curie
  performs no sender authentication.
- **`list_messages` always sends all three `include_*=false`.** They are
  constants in `agentmail.py`, not parameters and not config, so no caller and no
  operator can turn them on. Sending them when they are already the provider's
  default is the point: a changed default cannot silently widen the install.
- **`conversations` is written only after both inbound checks pass.** Pre-seeding
  it from the poll listing, however convenient, silently removes the allow-list's
  protection of egress.
- **Admission is bounded before state or body allocation.** At the pending or
  state-byte cap, leave provider mail unclaimed and unmodified so later capacity
  can recover it. Never evict an unresolved delivery merely to admit a newer one.
- **Logs carry no raw mail PII.** Do not log sender addresses, subjects, bodies,
  provider message/thread ids, or reply text. Use a one-way correlation token
  and a reason/state label so operators can join retries without copying mail
  content into the cluster log-retention system.
- **Empty allow-list plus ingress enabled is a boot failure, not deny-all.** The
  inbox is a public mailbox by construction, so fail-open would make every install
  an open trigger for agent turns. Allow-all must be written as `*`.
- **One SQLite file has one serialized writer and the chart pins one replica.**
  Every poller and egress transaction uses the adapter-owned lock. Adding a
  second replica or changing `Recreate` to rolling update creates two writers;
  horizontal scale needs a separately accepted shared-store design.

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
server reproduces the provider's documented filtering rather than serving
whatever it is handed, because a test built on a fake that serves labeled mail
the real provider would have withheld proves nothing about production. A new test
that patches an internal function instead of driving it through those servers does
not meet this package's bar.
