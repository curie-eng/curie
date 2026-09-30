# Slack Alert Follow-up Context Design

Date: 2026-09-29

Status: Accepted

Accepted with explicit maintainer approval on 2026-09-29. Revised the same
day after measurements and review: Slack's own event type does not promise
`parent_user_id` on `app_mention`, the worker reads a person's turn text as
trusted input for repository selection, and dispatcher and worker versions can
overlap during a rolling upgrade.

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

The dispatcher will enrich a mention that replies in a thread whose root
message this same Curie bot posted. It will read only that exact root message
and prepend its text to the current message inside a platform-authored,
explicitly untrusted context block.

The queued turn keeps all of its existing identity and authority fields:

- `source` remains `slack`.
- `author` remains the sender's Slack user ID.
- `conversation_id` remains the Slack root timestamp.
- The reply handle remains the placeholder posted for this event.
- `hook_run` stays absent. No hook ID, hook author, hook source, sandbox route,
  transcript reference, or approval state is copied to the turn.

The worker and runner therefore treat the reply exactly like every other human
Slack turn. Existing permission gates remain authoritative. The context block
states that the earlier answer is data, not instructions or authorization, and
that neither it nor the current message bypasses approval policy. A destructive
tool call still requires the agent's configured approval path.

## Admission and identity checks

Enrichment is considered only when all of these hold:

1. the event is on the `app_mention` lane;
2. the event has a nonempty `thread_ts` that differs from its own `ts`, so it
   is a reply rather than a root;
3. Bolt's authorization supplied a `bot_user_id` for this request;
4. the event's `parent_user_id`, when present, equals that `bot_user_id`.

`parent_user_id` is a hint, never the proof. Slack's own `AppMentionEvent` type
in its [Node SDK types package](https://github.com/slackapi/node-slack-sdk/blob/main/packages/types/src/events/app.ts),
read on 2026-09-29, lists `thread_ts` and no `parent_user_id`, and Slack's
[`app_mention` reference](https://docs.slack.dev/reference/events/app_mention/)
shows neither. When the field is present and names
someone else, Slack has already said the root is not ours, so no lookup is
made. When it is absent, the root is looked up. The proof of ownership is
always the root message itself.

The dispatcher calls `conversations.replies(channel=<event channel>,
ts=<event thread_ts>, limit=1)`. Slack's reference for that method says the
parent message is returned first; `limit=1` deliberately excludes every other
participant message. The channel is fixed by the current event and is never
accepted from message text or cached content.

The root is this bot's when the first returned message has a `ts` equal to the
event's `thread_ts` and either its `user` equals `bot_user_id`, or it carries no
`user` and its `bot_id` equals Bolt's authorized `bot_id`. The second form is
the one Bolt's own self-event filter accepts, because a bot message may carry
`bot_id` without `user`. A message with a `user` that is someone else is never
ours, whatever its `bot_id`.

A foreign bot, a human-authored root, a mismatched root timestamp, an empty
result, or a malformed response never contributes context. The ordinary
relevance and self-event rules remain unchanged.

## Size bound

Only the root's derived text is kept, and at most 4,000 characters of it. The
text is derived by the same `derive_text` the dispatcher applies to an inbound
event, so a root whose body lives in Block Kit still yields its content. A
longer root splits the available space around a platform marker naming how many
characters were left out, keeping equal head and tail excerpts because an alert
post usually opens with the alert and ends with the question a reply answers.
The complete excerpt, marker included, is at most 4,000 characters before it is
cached.

## Context cache and restart behavior

The first validated answer about a root is cached in Valkey under a digest of
the authorized bot user ID, authorized bot ID, channel ID, and root timestamp.
The value is a versioned object carrying those same coordinates, whether the
root is this bot's, and, only when it is, the bounded root text. Reads
revalidate every field and the 4,000-character bound; corrupt, oversized, or
mismatched values are ignored rather than rendered. A root that is not this
bot's is cached without any of its text, so a thread rooted by someone else
costs one lookup rather than one per reply.

The cache serves three purposes:

- a worker or dispatcher restart does not erase the context needed by a later
  reply;
- duplicate delivery and later replies to the same root do not repeatedly call
  Slack;
- Slack's history rate limit is not paid once per turn in a long conversation.

The default retention is 30 days, matching the ordinary idle transcript
window. The retention and the key prefix are explicit in dispatcher
configuration and documentation. Cache keys contain only a digest, never
message content or raw identifiers. A lookup that failed is not cached.

Event deduplication stays authoritative. The dispatcher takes the existing
event-ID claim before resolving context. A duplicate event exits before Slack
or the context cache is read. The context resolver catches transport, Slack,
cache, and shape failures so no new exception can remain between claim and
placeholder posting.

## Failure behavior

If Slack said the parent is this bot's (`parent_user_id` equals `bot_user_id`)
but the root cannot be loaded and validated, or the loaded root is not this
bot's after all, the dispatcher does not silently treat the short reply as a
self-contained instruction. It prepends a platform notice saying that prior
context was unavailable, that the earlier proposal must not be inferred or
executed, and that the agent should ask the person to restate the request.

When `parent_user_id` was absent and the lookup fails, nothing says the root is
ours, so the message stays ordinary input. Adding the notice there would put it
on every threaded mention in every thread whenever the history read fails, for
example on an install whose app lacks a history scope.

The notice turn still follows the ordinary placeholder and enqueue path, so the
person receives an answer and the event is neither silently dropped nor
reclassified as a hook. Failures are logged without root text or credentials.

If `parent_user_id` names someone else, no lookup is attempted and the message
remains byte-for-byte ordinary Slack input after existing self-mention
stripping. The same holds when the looked-up root is someone else's.

## Prompt shape

The successful prefix identifies the material as a prior assistant reply from
this exact Slack thread, says it is context only, and says it may contain
untrusted alert data. The root text is XML-escaped so it cannot forge the
closing marker, and every slash in that root is emitted as an XML entity. The
entity keeps GitHub URLs and `owner/name` tokens non-lexical to both old and new
workers during a rolling upgrade. The current message follows outside that
quoted block and retains its normal instruction status.

The fallback prefix contains no root text and explicitly refuses inference from
the unavailable message. Neither prefix contains the hook's synthetic ID,
delivery ID, signature, binding endpoint, or any other execution credential.
Neither contains a slash, so the platform wording itself can never read as a
repository.

## Repository selection

The worker reads a `source=slack` turn's text as trusted input when it selects
a coding repository (`trusted_repository_fact`), and any `owner/name` token or
GitHub URL in it counts. Quoted alert text is full of such tokens. The quoted
root is hook output, so the dispatcher makes every slash in that root
non-lexical before the turn reaches the stream. This is safe by construction:
the old worker's unchanged parser and the new worker both ignore the root's
repository-looking strings, while the person's own words outside the block are
parsed exactly as before. No textual marker is treated as authenticated
provenance, so a person who types the platform header cannot hide a repository
from conflict detection.

Other exact-text readers of a turn, the behavior pack greeting and help
matchers, see the prefix and therefore do not fire on a reply in a thread this
bot rooted; the model answers such a reply instead, with the context. That is
accepted rather than special-cased.

## Files and ownership

- `apps/dispatcher/src/curie_dispatcher/thread_context.py` owns root validation,
  the size bound, cache serialization, Slack lookup, prompt rendering, and
  slash neutralization for mixed-version workers.
- `apps/dispatcher/src/curie_dispatcher/handlers.py` invokes that helper after
  the event-ID claim and before the shared placeholder/enqueue tail, passing
  Bolt's `bot_user_id` and `bot_id`.
- `apps/dispatcher/src/curie_dispatcher/config.py` and
  `apps/dispatcher/README.md` own the cache retention and prefix settings.
- Dispatcher and worker tests own the behavior matrix, including the old
  worker's raw repository parser. Slack is faked because it is an external
  service; Valkey remains real, per repository policy.

No frozen ACI or plugin-format contract changes. No worker kernel, Slack sink,
runner, API, chart, example bundle, or downstream deployment changes.

## Test strategy

Focused tests will prove:

- a same-bot, same-channel root is included and a dependent `yes please` reply
  remains human-authored `source=slack` with no `hook_run`;
- only the root is read and rendered, even when the Slack response attempts to
  include later messages, and a long root is bounded;
- another bot, another channel, another root timestamp, malformed cache data,
  and mismatched Slack results cannot leak history, and one bot's or channel's
  cached root is never served to another;
- a reply without `parent_user_id` is enriched when the root is ours and left
  unchanged when it is not;
- root content cannot close the context delimiter, turn itself into
  authorization, or select a repository;
- a missing or failed history read produces the fail-closed visible prompt when
  the parent was claimed as ours, and ordinary input when it was not, and a
  Valkey outage never raises;
- a dispatcher restart can reuse the Valkey cache;
- a duplicate Slack event produces no second lookup, placeholder, or queued
  turn;
- ordinary root mentions and replies to non-Curie roots remain unchanged.

The focused suites run against real Valkey. Repository lint, typing, docs
checks, the fix-pin verifier for the selected regression test, and the full
Python baseline provide integration evidence. No live Slack mutation,
production deployment, or workload restart is required for the upstream PR;
live deployment acceptance remains a separate release gate.

## Release path

This is a shared bug in released generic hook behavior, so the PR targets
`main`. The normal forward merge carries it to `next`, where it composes with
#3529 and #3530. Merging the PR is not production completion: a release must be
built, installed by the downstream owner, and verified with a real alert root
and a human follow-up before the incident is closed operationally.
