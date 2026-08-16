# CLAUDE.md - apps/mail-adapter

The email channel adapter: an AgentMail inbox bridged to a Curie channel
binding. Full behavior spec lives in `apps/mail-adapter/README.md`; this file is
the enforceable-rule summary.

## Load-bearing invariants

- **The adapter holds no platform API key, no queue credential, and no database
  access.** Its only credentials are `CURIE_CHANNEL_TOKEN` (presented as
  `X-API-Key` on ingress), `CURIE_EGRESS_SECRET` (checked on every inbound POST)
  and `AGENTMAIL_API_KEY`. Do not add `CURIE_API_KEY`, a Valkey client, or a DB
  session here; a capability the adapter does not hold cannot be stolen from it,
  and re-minting an expired `chn` token is an operator step for exactly that
  reason.
- **The reply target is `target.reply_ref` off the event, never the conversation
  record.** The record is overwritten by every inbound message in the thread, so
  deriving the target from it sends turn one's answer to message two. The record
  exists for two things only: the accumulated reply text, and as the existence
  gate that gives the inbound checks their transitive reach into egress.
- **Nothing is recorded as replied until the provider has accepted the send.**
  A `turn.completed` whose AgentMail send failed acks 502 and one whose duplicate
  is still in flight acks 503. Acking 200 in either case makes the worker clear
  its durable completion record (`kernel.py` `clear_completion`, on any 2xx) and
  the email is gone with no retry and no dead letter. Do not collapse 502 and 503
  into one code: they mean different things in the worker's log.
- **`in_flight` is cleaned up in a `finally`.** The set is added to before an
  outbound call and removed after it, so an exception between the two would leak
  the `event_id` and make every later redelivery of that turn take the in-flight
  branch and never send. The mutation proof is
  `tests/test_egress.py::test_an_unexpected_exception_does_not_poison_the_event_id`.
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
- **`seen` is bounded and every polled id enters it before the allow-list
  decision.** That ordering is what makes the bound necessary: without it anyone
  who can mail a public inbox grows the map until the pod is OOMKilled, with no
  inbound gate in the way.
- **Empty allow-list plus ingress enabled is a boot failure, not deny-all.** The
  inbox is a public mailbox by construction, so fail-open would make every install
  an open trigger for agent turns. Allow-all must be written as `*`.
- **All state is process-local and the chart pins one replica.** Adding a second
  replica splits `reply.update` and `turn.completed` across pods. Horizontal scale
  needs shared state, which is a different ticket.

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
