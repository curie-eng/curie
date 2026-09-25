# CLAUDE.md - apps/dispatcher

The Slack ingestion edge: Bolt for Python in Socket Mode. Full behavior spec
lives in `apps/dispatcher/README.md`; this file is the enforceable-rule
summary.

## Load-bearing invariants

- **Scope discipline: ack, dedupe, placeholder, enqueue -- nothing else.**
  Routing, the finish-race, steer/interrupt, and run orchestration belong to
  the worker (`apps/worker`). If you find yourself adding any
  decision about *how* a message gets answered, that decision belongs one
  layer up -- stop and move it, don't grow the dispatcher's scope.
- **Caller admission is asked, never decided, here (ADR 0175).** Every
  turn-starting lane asks the platform API (`admission.AdmissionGate`) after its
  own filters and BEFORE the dedupe claim, so a refused caller gets no
  placeholder. The dispatcher caches answers and fails closed on a cold miss
  during an outage; it never compares caller ids itself, because the API's
  `admission.admit` is the one decider.
- **The queued-turn shape is the frozen `QueuedTurn` contract in `packages/`.**
  The dispatcher is the producer, but the payload it enqueues was promoted out
  of the dispatcher into the channel-neutral `aci_protocol.QueuedTurn` (issue
  #7). That shared contract is the Pydantic source of truth guarded by the
  schema-compat gate, so the Python producer and the Rust/TS consumers compile
  against one shape instead of a hand-mirrored dispatcher-local model. Do not
  reintroduce a dispatcher-owned payload model. The producer-owns-the-shape
  principle still governs any *new* dispatcher-only field: land a contract
  change in `packages/aci-protocol` via an issue/PR first, exactly as #7 did.
- **Idempotency key is the Slack event id, not the message content.** Dedupe
  is `SET <dedupe_prefix><event_id> 1 NX EX <ttl>` in Valkey -- O(1), TTL-bounded,
  no scan. Order is always claim -> post placeholder -> `XADD`, so a
  duplicate delivery can never produce a second placeholder. Do not reorder
  these three steps.
- **The queue seam is one `payload` field, not a multi-field Stream entry.**
  Each Stream entry carries a single `payload` field holding the
  `QueuedTurn` JSON. This lets fields be added to the payload without
  reshaping the Stream schema itself -- do not add a second top-level Stream
  field for a new piece of data; put it inside the JSON payload.
- **Reconnects are the supervisor's job, not the Slack client's.** The Bolt
  socket client self-heals transient drops; `dispatcher.supervisor.Supervisor`
  is the outer net for failures the client cannot recover from (connect
  failures, unrecoverable exits) and owns graceful shutdown (`request_stop`
  wired to SIGINT/SIGTERM). Do not add ad hoc retry logic elsewhere for a
  connection failure -- it belongs in the supervisor's `BackoffPolicy`.
- **Slack allows a `view_submission` three seconds to be acked, and the ack
  body is load-bearing.** It carries `response_action: errors` on a refusal,
  the only surface a claim-race loser can see behind an open modal. The only
  thing permitted before `ack()` on that path is the resolve call itself;
  both Slack calls (the card read and the card stamp, in
  `render_note_submission`) run after the ack. The enforcement is structural,
  not discipline: `resolve_note_submission` takes no `web_client` parameter
  at all, so the pre-ack half physically cannot call Slack -- a future change
  must preserve that property rather than relying on call ordering. The
  post-ack half runs on Bolt's shared listener executor, which is why the
  Slack client carries an explicit short timeout.

## Config surface

`DispatcherConfig()` (a `pydantic_settings.BaseSettings`) reads `SLACK_APP_TOKEN`,
`SLACK_BOT_TOKEN`, `VALKEY_*`, `CURIE_STREAM` (must match the worker's
stream name), `CURIE_DEDUPE_PREFIX`/`_TTL_SECONDS`, `CURIE_PLACEHOLDER_TEXT`,
and the backoff tunables. Full table in `apps/dispatcher/README.md`.

## Verify (Slack-free)

```bash
docker compose -f compose.dev.yaml up -d valkey
uv run pytest apps/dispatcher/tests -q
```

Only the Slack Web API client and socket transport are faked. Stream and
dedupe assertions run against the real Valkey from `compose.dev.yaml`.
`tests/test_dispatch.py` drives Bolt's real `SocketModeHandler.handle` end to
end; `tests/test_queue.py` covers the seam and dedupe against real Valkey;
`tests/test_supervisor.py` covers backoff/reconnect/shutdown with a fake
connection. A new test that fakes Valkey instead of using the real one from
the compose stack does not meet this repo's testing bar (root AGENTS.md:
never mock Postgres/Valkey/Langfuse).
