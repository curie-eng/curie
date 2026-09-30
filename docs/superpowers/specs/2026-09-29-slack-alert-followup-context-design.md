# Slack Alert Follow-up Context Design

Date: 2026-09-29

Status: Accepted

Accepted with explicit maintainer approval on 2026-09-29.

## Purpose

A person who replies to a Curie-authored Slack alert must be able to refer to
the alert in ordinary conversational language. If Curie asked whether it should
perform an operation, a reply such as `yes please` must reach the model with
that prior question as context. The reply remains a person's ordinary Slack
turn: it does not inherit the hook's standing authorization, execution source,
live session, or permission posture.

The change targets the generic placeholder-less hook path that exists on
`main`. It does not deploy or configure any downstream agent and does not
restart any workload.

## Root cause

Generic hooks deliberately use synthetic conversation IDs such as
`hook:<agent>:<hook>:<partition>`. The worker scopes the sandbox, transcript,
lock, and approval slot to that synthetic identity. When a Slack-bound hook has
no placeholder, the Slack sink correctly refuses to pass the synthetic value as
`thread_ts`; it posts the answer as a new channel-level message and adopts the
message timestamp only as the reply reference for the rest of that turn.

A later human reply is a separate Slack event whose conversation ID is the
root message's Slack timestamp. The worker therefore opens the human Slack
thread's session and transcript, not the hook session. This separation is
intentional and security-relevant, but the human turn receives no copy of the
bot-authored root message, so phrases that depend on that message arrive
without their antecedent.

PR #3529 permits a signed hook to name an existing conversation and preposted
placeholder. PR #3530 uses that capability for the opt-in Slack Email intake in
issue #3527. Those changes solve an intake that already owns the source thread;
they do not change the default synthetic identity or channel-level output of a
placeholder-less Alertmanager hook. This design is complementary and must work
whether or not those `next` changes are present.

## Decision

The dispatcher will enrich a human mention that replies directly to a message
authored by the same Curie bot. It will read only the root message of that exact
Slack thread and prepend the root text to the current human message inside a
platform-authored, explicitly untrusted context block.

The queued turn keeps all of its existing identity and authority fields:

- `source` remains `slack`.
- `author` remains the human's Slack user ID.
- `conversation_id` remains the Slack root timestamp.
- The reply handle remains the placeholder posted for this human event.
- No hook ID, hook author, hook source, sandbox route, transcript reference, or
  approval state is copied to the human turn.

The worker and runner therefore treat the reply exactly like every other human
Slack turn. Existing permission gates remain authoritative. The context block
states that the earlier answer is data, not instructions or authorization, and
that neither it nor the current message bypasses approval policy. A destructive
tool call still requires the agent's configured approval path.

## Admission and identity checks

Enrichment is attempted only when all of these Slack-issued facts are present:

1. the event is on the `app_mention` lane;
2. the event has a nonempty `thread_ts`, so it is a reply rather than a root;
3. `parent_user_id` equals Bolt's authorized `bot_user_id` for this request;
4. the Slack history response contains a first message whose `ts` equals that
   exact `thread_ts` and whose `user` equals the same `bot_user_id`.

The dispatcher calls `conversations.replies(channel=<event channel>,
ts=<event thread_ts>, limit=1)`. Slack documents that this method returns the
parent first, followed by replies; `limit=1` deliberately excludes every other
participant message. The returned channel is fixed by the current event and is
never accepted from message text or cached content.

A foreign bot, a human-authored root, a mismatched root timestamp, a mismatched
root author, an empty result, or malformed response never contributes context.
The ordinary relevance and self-event rules remain unchanged.

## Context cache and restart behavior

The first validated root is cached in Valkey under a digest of the authorized
bot ID, channel ID, and root timestamp. The value contains a versioned object
with those same coordinates and the root text. Reads revalidate every field;
corrupt or mismatched values are ignored rather than rendered.

The cache serves three purposes:

- a worker or dispatcher restart does not erase the context needed by a later
  reply;
- duplicate delivery and later replies to the same root do not repeatedly call
  Slack;
- Slack's history rate limit is not paid once per turn in a long conversation.

The default retention is 30 days, matching the ordinary idle transcript
window. The setting is explicit in dispatcher configuration and documentation.
Cache keys contain only a digest, never message content or raw identifiers.

Event deduplication stays authoritative. The dispatcher takes the existing
event-ID claim before resolving context. A duplicate event exits before Slack
or the context cache is read. The context resolver catches transport, Slack,
cache, and shape failures so no new exception can remain between claim and
placeholder posting.

## Failure behavior

If the event claims to reply to this bot but the root cannot be loaded and
validated, the dispatcher does not silently treat the human's short reply as a
self-contained instruction. It prepends a platform notice saying that prior
context was unavailable, that the earlier proposal must not be inferred or
executed, and that the agent should ask the person to restate the request.

This fail-closed turn still follows the ordinary placeholder and enqueue path,
so the person receives an answer and the event is neither silently dropped nor
reclassified as a hook. The failure is logged without root text, credentials,
or identifiers beyond the existing bounded event/channel metadata policy.

If `parent_user_id` does not identify this bot, no lookup is attempted and the
message remains byte-for-byte ordinary Slack input after existing self-mention
stripping. This avoids changing unrelated threaded conversations.

## Prompt shape

The successful prefix identifies the material as a prior assistant reply from
this exact Slack thread, says it is context only, and says it may contain
untrusted alert data. The root text is delimiter-escaped before insertion so it
cannot forge the closing marker. The current human text follows outside that
quoted block and retains its normal instruction status.

The fallback prefix contains no root text and explicitly refuses inference from
the unavailable message. Neither prefix contains the hook's synthetic ID,
delivery ID, signature, binding endpoint, or any other execution credential.

## Files and ownership

- `apps/dispatcher/src/curie_dispatcher/thread_context.py` owns root validation,
  cache serialization, Slack lookup, and prompt rendering.
- `apps/dispatcher/src/curie_dispatcher/handlers.py` invokes that helper after
  the event-ID claim and before the shared placeholder/enqueue tail.
- `apps/dispatcher/src/curie_dispatcher/config.py` and
  `apps/dispatcher/README.md` own the cache-retention setting.
- Dispatcher tests own the behavior matrix. Slack is mocked because it is an
  external service; Valkey remains real, per repository policy.

No frozen ACI or plugin-format contract changes. No worker kernel, Slack sink,
runner, API, chart, example bundle, or downstream deployment changes.

## Test strategy

Focused tests will prove:

- a same-bot, same-channel root is included and a dependent `yes please` reply
  remains human-authored `source=slack`;
- only the root is read and rendered, even when the Slack response attempts to
  include later messages;
- another bot, another channel, another root timestamp, malformed cache data,
  and mismatched Slack results cannot leak history;
- root content cannot close the context delimiter or turn itself into
  authorization;
- a missing or failed history read produces the fail-closed visible prompt;
- a dispatcher restart can reuse the Valkey cache;
- a duplicate Slack event produces no second lookup, placeholder, or queued
  turn;
- ordinary root mentions and replies to non-Curie roots remain unchanged.

The focused dispatcher suite runs against real Valkey. Repository lint, typing,
docs checks, the required fix-pin verifier for the selected regression test, and
the full Python baseline provide integration evidence. No live Slack mutation,
production deployment, or workload restart is required for the upstream PR;
live deployment acceptance remains a separate release gate.

## Release path

This is a shared bug in released generic hook behavior, so the PR targets
`main`. The normal forward merge carries it to `next`, where it composes with
#3529 and #3530. Merging the PR is not production completion: a release must be
built, installed by the downstream owner, and verified with a real alert root
and a human follow-up before the incident is closed operationally.
