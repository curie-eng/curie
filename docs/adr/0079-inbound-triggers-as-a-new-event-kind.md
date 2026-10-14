# 79. Inbound triggers as a new event kind, ingested by the API

Date: 2026-07-10
Status: Accepted

Amended by [ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md)
for credentials used by operator-restricted hook sources.

Proposes how "triggers beyond chat" (issue
[#29](https://github.com/curie-eng/curie/issues/29)) enter the system: which
service accepts a non-Slack trigger, and how a triggered turn produces output
when there is no Slack placeholder to edit. This ADR is **Accepted**; the
worker/kernel owner's decision authorizes the remaining implementation because
the mechanism touches the sacred kernel/dispatcher seam
(`apps/worker/CLAUDE.md`) and the hand-mirrored queue payload
(`docs/roadmap.md` §2).

## Context

Today the only way to start a turn is a Slack `app_mention`/DM: the dispatcher
(Socket Mode, no inbound HTTP port) dedupes it, **posts a placeholder reply**,
and `XADD`s a `QueuedSlackEvent` onto `curie:runs`; the worker kernel later
edits that placeholder in place as the reply streams. `QueuedSlackEvent`
therefore carries a `placeholder_ts` the kernel assumes exists.

#29 wants agents to also react to external events — the first slice being a
generic HMAC-verified inbound webhook (`POST /hooks/{agent}/{hook}`) mapping a
payload to a turn on the existing stream/kernel path ("no new execution
machinery"). Two decisions block a working slice, and both cross into the
single-owner kernel/dispatcher area, so they are recorded here rather than
guessed in a PR:

1. **Which service accepts the webhook?** The dispatcher is the conceptual
   ingress, but it is Socket-Mode-only with no inbound HTTP server. The API
   already owns HTTP ingress, already verifies an HMAC webhook (the GitHub
   git-flow hook, `routers/github.py` `verify_signature`), and already produces
   to a Valkey stream (`curie:evals`).
2. **How does a triggered turn produce output?** A webhook has no Slack
   placeholder, but the kernel edits `placeholder_ts`. Either the ingress posts
   a placeholder before enqueuing (keeps the kernel unchanged, but spreads Slack
   knowledge/credentials into a second service), or the kernel learns a
   "post-instead-of-edit" output path for placeholder-less events (a kernel
   change).

## Decision

1. **The API accepts inbound triggers.** Add `POST /hooks/{agent}/{hook}` to
   `apps/api`, reusing the existing HMAC verification pattern (per-agent hook
   secret, `x-curie-signature-256` over the raw body, `hmac.compare_digest`) <!-- gitleaks:allow -->
   and dedupe (`SET NX`) already proven on the GitHub webhook. The dispatcher
   stays Slack-only. Rationale: reuse the API's HTTP + HMAC + Valkey wiring
   instead of giving the Socket-Mode dispatcher a new inbound-server role.

2. **A trigger is a new event *kind* with no placeholder; the kernel posts its
   output.** Extend the queued-event contract with a `source`
   (`slack` | `webhook` | `cron`) and make `placeholder_ts` optional. On a
   placeholder-less event the kernel **posts** its reply to the event's
   channel/thread rather than editing a pre-posted message. This keeps Slack
   credentials in one place (the worker's existing `SlackSink`) and avoids the
   API growing a Slack client. Per #29's semantics, a trigger targeting a thread
   with a live interactive session waits for idle (jobs are outputs, not steering
   inputs) — kernel behaviour that only its owner should implement.

3. **Promote the queue payload out of its hand-mirrored form.** Adding `source`
   is the moment to address the roadmap §2 debt: move the turn payload into a
   shared, drift-gated contract (alongside `aci-protocol`) rather than mirroring
   the new field by hand in the dispatcher (Python) and CLI (Rust).

Decisions (2) and (3) are **kernel/contract changes owned by the worker
single-owner** (Yichen); this ADR records the accepted shape for that work.
Decision (1) is API-lane and can proceed under the accepted event shape in (2).

## Alternatives considered

- **Webhook in the dispatcher.** Conceptually the ingress home, but it would add
  an inbound HTTP server to a Socket-Mode process and still needs the same
  event-shape decision. Rejected as a larger change for no contract benefit.
- **API posts the placeholder, kernel unchanged.** Avoids a kernel change but
  duplicates the dispatcher's placeholder+enqueue logic in the API and couples
  the API to Slack credentials — spreading Slack knowledge across two services.
  Rejected in favour of a single Slack-output owner (the worker).

## Consequences

- One HMAC-verified ingress pattern serves both GitHub git-flow and generic
  triggers; cron (the other #29 trigger) reuses the same event kind with a
  scheduler producing the event.
- The kernel gains a placeholder-less output path, exercised by a provoking
  test alongside its existing invariants.
- The turn payload becomes a first-class contract, retiring a known drift risk.
- Until the kernel side lands, the webhook cannot complete a
  turn end to end, so no partial webhook code should merge.
