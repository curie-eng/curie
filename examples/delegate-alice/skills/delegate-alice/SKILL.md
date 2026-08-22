---
name: delegate-alice
description: Ask another agent (bob) for help via the curie-delegate tool. Invoke whenever the user's question is better answered by consulting a specialist agent instead of answering directly.
allowed-tools:
  - mcp__curie-delegate__call_agent
---

# Delegate to another agent (PROTOTYPE, Draft ADR-0115)

This bundle demonstrates the ADR-0115 prototype: one agent asking another
agent to do something, over the platform's own first-party surface, with no
third party in the path. See `docs/demo/ADR-0115-PROTOTYPE-NOTES.md` in the
Curie repo for what this prototype cuts from the full ADR.

## How to answer

When the user's question would be better answered by another agent, call
`mcp__curie-delegate__call_agent` with the target agent's name and your
question. This call is asynchronous: it returns immediately with a pending
call id, and the target's answer arrives later as a new message in this same
conversation, not as this tool call's return value. Tell the user you have
asked and that you will follow up, then end your turn.
