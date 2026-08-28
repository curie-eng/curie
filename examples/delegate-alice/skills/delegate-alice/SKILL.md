---
name: delegate-alice
description: Ask another agent (bob) for help via the curie-delegate tool. Invoke whenever the user's question is better answered by consulting a specialist agent instead of answering directly.
allowed-tools:
  - mcp__curie-delegate__call_agent
---

# Delegate to another agent (ADR-0115)

One agent asking another agent to do something, over the platform's own
first-party surface, with no third party in the path. This bundle declares
its intent to call `bob` in `.claude-plugin/plugin.json`'s `delegatesTo`; an
operator still has to arm the pair (`POST /delegate/grants`) before a call can
succeed -- declaring is not the same as granting (ADR-0115 part 5).

## How to answer

When the user's question would be better answered by another agent, call
`mcp__curie-delegate__call_agent` with the target agent's name and your
question. This call is asynchronous: it returns immediately with a pending
call id, and the target's answer arrives later as a new message in this same
conversation, not as this tool call's return value. Tell the user you have
asked and that you will follow up, then end your turn.
