# 174. A test installation may let a listed bot resolve approvals over Slack

Date: 2026-09-24

Status: Draft

This ADR builds on [ADR-0169](0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md)
and on the approval authorizer ([ADR-0034](0034-approval-authorizers-resolve-membership-in-the-api.md)).
It supersedes nothing. This is a platform capability: a second way to resolve
an approval, in the dispatcher and the API. The mean tester is its first
consumer. For that consumer it changes ADR-0169's decision 5, "The tester never
resolves an approval", on test installations only.

## Context

A mean tester that stops at a pending approval card cannot test what an agent
does once a person decides. That is half of every gated path: whether the agent
reports the approved action truthfully, and whether a rejection leaves nothing
behind. Fully automated mean testing needs the tester to resolve the card.

How a card is resolved today (on `next` at `bbd4de1e`):
- A card's Approve and Reject buttons carry the approval id as their `value`
  (`apps/dispatcher/src/curie_dispatcher/approval_actions.py:183`).
- A click reaches the dispatcher, which calls `POST /approvals/{id}/resolve`
  as the clicking Slack user (`apps/api/src/curie_api/routers/approvals.py:291`).
- The server-side authorizer then decides whether that principal is in the
  route's approver set: an explicit user list, a Slack user group, or the card
  channel's members. Principal kinds are `chat`, `console`, `operator` and
  `adapter` (`apps/api/src/curie_api/authorizer.py:45`).

A bot cannot use that path:
- Slack delivers a button click only from a person's client. There is no API
  by which one app presses another app's button.
- The dispatcher admits a bot's message inside a thread only for an
  operator-listed channel and bot pair
  (`apps/dispatcher/src/curie_dispatcher/relevance.py:228`).

Resolving through the target's API instead would give the tester that
installation's platform credential. That breaks ADR-0169's decision 2: no
platform key, and the same verdict from any installation.

Resolving an approval performs the action. On a production installation that
means a real filing, a real send, a real change. A mean tester's probes must
never reach production: ADR-0172's decision 5, accepted in #3055.

## Decision

**An installation that declares itself a test installation may list a bot's
Slack user as an approver on chosen routes. That bot then resolves a card by
replying in the card's thread, and the reply takes the same authorized path a
click takes. Every other installation refuses it. A mean tester is the first
such bot.**

1. **Off unless the installation says it is a test installation.**
   - A chart value, `approvals.botApprovers.enabled`, defaults to `false`.
   - When it is `false`, the dispatcher ignores approval replies. The API also
     refuses any resolution whose principal is a bot, even from a
     correctly formed reply.
   - Turning it on is the operator's statement that this installation's tools
     reach only test systems.
2. **Named per route, never by membership.** A bot counts as an approver only
   when the route's explicit user list names its Slack user id. A channel
   members set or a user group never admits a bot, even on a test
   installation.
3. **A reply, in the card's own thread.**
   - The listed bot resolves by posting `[mean test] approve <approval-id>`,
     or `reject`, in the card's thread.
   - The dispatcher admits that one form from that one bot. It resolves as a
     `chat` principal whose subject is the bot's Slack user id. The authorizer
     and the audit row are the ones a click uses, and the audit records a bot
     resolver.
   - Any other text from the bot is not a resolution.
4. **The tester resolves only on listed test installations.** In the tester's
   skill, a target it may resolve for is listed under Test installations
   (ADR-0172). Against any other target it never resolves, and never asks for
   an action at all. The target side is still the real bound: a production
   installation refuses the reply under decision 1, whatever the tester does.
5. **The report says what was decided and what followed.** For each card it
   resolves, the tester reports the decision it made, the reply the target gave
   after it, and whether that reply matches what the decision should have
   produced.

## Consequences

- A test installation can mean test a gated path end to end, with no person
  clicking and no platform key in the tester.
- Production is untouched by construction: the default is off. A misbehaving
  or prompt-injected tester is refused by the target, not trusted to refrain.
- Test installations become a real thing an operator runs: the same agents,
  wired to test systems. This ADR does not provide them.
- The dispatcher gains a second resolution input. Parsing it, the per-bot
  allowlist and the bot refusal in the API are new code with their own tests.
- A bot resolution is recorded as such, so an audit can tell automated
  decisions from people's.

## Alternatives considered

- **Give the tester the target's platform credential.** It breaks ADR-0169's
  decision 2, and it hands a prompt-injectable sandbox a key that also writes.
- **Keep a person in the loop.** The tester reports the card, a person decides,
  and "continue" checks what followed. That is safe and works today, but it is
  not automation.
- **Let the tester resolve on production.** Every run would change real
  systems. ADR-0172's decision 5 forbids that.
- **Drive a logged-in browser to click the button.** It needs a person's Slack
  account and session, which is fragile. It would also bypass the difference
  the audit should record.

## Tracking

On acceptance, file an issue each for:
- the chart value and the API's bot refusal (decisions 1 and 2);
- the dispatcher's approval reply (decision 3);
- the tester's skill and report (decisions 4 and 5).
