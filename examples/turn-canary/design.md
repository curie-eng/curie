# Opt-in read-only turn canary

The operator selects concrete Slack binding routes to probe. This example is not
installed by default and does not discover a target from an agent name alone.
It uses the existing queued-turn stream, disconnected cluster-message reply
relay, and scoped thread-reset API. It does not add a worker, runner, admission,
or channel adapter contract. The checker proves a bounded synthetic turn for
each selected route; it does not prove that Slack ingress or any other channel
adapter is connected.

## Contract

- **TURN-CANARY-1: inventory and selection.** Each cycle reads the live
  `agents`, `deployments`, and `agent_channels` tables. A target is eligible only
  when its agent has an active deployment and its exact `(kind, address,
  adapter)` route exists. The operator supplies a nonempty explicit list of
  these three-part routes. The checker rejects missing, duplicate, ambiguous,
  inactive, or non-Slack selections before enqueue. It does not infer bindings
  from agent names, email addresses, manifests, or any tenant convention.
  An inventory read failure is a failed cycle, never an empty healthy cycle.

- **TURN-CANARY-2: isolated request.** Each probe mints a fresh event ID,
  `eval:`-prefixed conversation ID, canonical UUIDv4 relay reply ref, and random
  nonce. Its prompt asks only for that nonce and explicitly forbids tools,
  actions, approval requests, and durable changes. It sets `tool_access` to
  `read-only`, `source` to `slack`, and empty attachments and hook run. The
  `ReplyHandle` retains the selected Slack address, uses the reserved
  `curie-cluster-message` adapter for internal delivery, and names the selected
  binding identity in `identity`. The placeholder is the reply ref. The
  producer validates and serializes the complete request through the installed
  `aci_protocol.QueuedTurn` schema before one stream enqueue. It rejects a
  platform that cannot honor read-only tool access before sending a turn.

- **TURN-CANARY-3: completion.** The checker polls the authenticated
  `/cluster-message-replies/{reply_ref}` API with a monotonic cursor and a
  finite deadline. Success needs a `turn.completed` event with outcome
  `delivered` and a reply update whose entire text equals the nonce. A reply
  alone, an approval pause, dropped or escalated completion, a mismatched
  nonce, malformed event, or a timeout fails. No reply is posted to a channel.

- **TURN-CANARY-4: owned cleanup.** After every enqueued probe, including
  timeout and error paths, the checker computes the worker thread key with
  `channel_protocol.scoped_conversation_id` and the selected binding identity.
  It submits that key to `POST /agents/{agent_id}/threads/{thread_key}/reset`
  and polls the matching reset status endpoint until `requested` is false and
  `route_existed` is true. It never resets an unowned or guessed conversation.
  A false or unknown `route_existed`, pending reset beyond the deadline, or any
  unknown status leaves
  cleanup unconfirmed. The cycle marks the target degraded and stops before
  another probe. It does not treat absence of a route as proof of release.

- **TURN-CANARY-5: bounded operation.** Probes run serially. Cycle interval,
  turn deadline, cleanup deadline, poll period, and maximum selected targets
  are positive, bounded settings with documented defaults. A cycle is also
  bounded by those limits. A new cycle cannot begin while cleanup from the
  preceding cycle is unconfirmed. An optional ResourceQuota headroom check is
  an observation immediately before enqueue: insufficient or unreadable
  headroom skips the turn with an explicit outcome. It is not an atomic
  reservation and cannot promise a global sandbox cap.

- **TURN-CANARY-6: diagnostics.** Export a portable Prometheus target success
  gauge, last run and last success timestamps, cleanup degraded gauge, capacity
  skip counter, cycle success gauge, and last cycle timestamp. Structured logs
  record outcome codes and phase without credentials, prompts, reply text,
  nonces, raw exception strings, or full HTTP bodies. HTTP failures expose only
  status and error type. Metric labels are bounded to selected route identity
  fields and outcome codes. Values such as namespace, image, resource quota,
  endpoint, and operator identity come from configuration; no installation
  value is compiled into this example.

## Verification

Unit tests under `examples/tests` exercise the selected binding and
`QueuedTurn` wire, relay completion, exact scoped reset key, failure and
cleanup stop cases, limits, and safe diagnostics. Isolated runtime
qualification uses a disposable agent and selected binding with a real model
and protected read-only runner, verifies a delivered nonce, inspects the
runner trace for no action or approval, confirms the reset result, and checks
the negative path with an intentionally mismatched nonce. This proof is
recorded for the exact candidate and costs an explicitly approved budget.
No unit or fake-model test alone establishes that runtime qualification.
