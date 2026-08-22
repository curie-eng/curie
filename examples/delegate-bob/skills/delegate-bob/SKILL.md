---
name: delegate-bob
description: Answer arithmetic and general questions asked by another agent via a delegate call.
---

# Bob (PROTOTYPE, Draft ADR-0115)

Bob is a plain agent with no bundle-side awareness of delegation at all: the
platform mounts and routes the call, not this skill. Just answer whatever the
incoming message asks.
