---
name: squawk
description: Push a non-empty message onto one agent-global LIFO stack, and pop the newest entry when the message is empty. Invoke on EVERY turn of this agent; the message text alone decides push or pop.
allowed-tools:
  - mcp__curie-state__get
  - mcp__curie-state__set
  - mcp__curie-state__append
---

# Squawk

One durable stack, shared by every channel bound to this agent. The message
decides the operation and nothing else does.

## The rule

Trim the incoming message first. Then:

- **Non-empty after trimming — push.** Append the trimmed text.
  Reply with exactly `Squawk!`
- **Empty after trimming — pop.** Remove and return the newest entry.
  Reply with exactly `Squawk! <text>`, where `<text>` is the entry you removed.
- **Empty, and the stack is empty — nothing to pop.**
  Reply with exactly `Squawk stack is empty.`

Reply with that line and nothing else. No preamble, no explanation, no
follow-up question. The three responses above are the entire output surface.

## Where the stack lives

Namespace `squawk`, key `stack`. The value is a JSON array, oldest first, so
the newest entry is the LAST element.

Never touch the `memory` or `transcript` namespaces. They are the agent's
learned lessons and its own conversation history, and the state tools refuse
them anyway.

## Push

Call `mcp__curie-state__append` with `namespace: "squawk"`, `key: "stack"`, and
`item` set to the trimmed message. Append is atomic on the server, so two
people pushing at the same moment both land — do not read the array first and
write it back, which would lose one of them.

Then reply `Squawk!`

## Pop

Popping is read-modify-write, so it needs the compare-and-set guard or a
concurrent pop returns the same entry twice.

1. `mcp__curie-state__get` with `namespace: "squawk"`, `key: "stack"`. Keep the
   `version` it returns.
2. If the value is missing, not an array, or an empty array, reply
   `Squawk stack is empty.` and stop. Do not write anything.
3. Take the LAST element. That is the entry to return.
4. `mcp__curie-state__set` with the array minus that last element, passing
   `expected_version` set to the version from step 1.
5. If the set is rejected as a conflict, somebody else changed the stack
   between your read and your write. Go back to step 1 and try again, up to
   five times. If it still conflicts, say
   `Squawk stack is busy, try again.` and stop — do not report an entry you did
   not successfully remove.
6. Reply `Squawk! ` followed by the entry you removed.

## What counts as empty

A message of only whitespace is empty. A message that is only a mention of this
bot is also empty — the platform strips its own mention before the turn reaches
you, so by the time you see it there is nothing left, and that is a pop.
