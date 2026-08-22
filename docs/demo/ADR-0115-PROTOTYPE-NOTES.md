# ADR-0115 delegate-call prototype: what this is and isn't

This branch (`worktree-prototype-adr-0115-agent-delegate`) is a **demo
prototype** built to produce [`docs/demo/adr-0115-delegate.gif`](adr-0115-delegate.gif)
for discussion on [PR #1793](https://github.com/curie-eng/curie/pull/1793),
which carries Draft [ADR-0115](https://github.com/curie-eng/curie/blob/task/adr-0115-reland/docs/adr/0115-agents-call-each-other-with-no-third-party.md)
("agents call each other with no third party in the path").

**It is not an implementation of ADR-0115 and was never intended to be
merged as one.** Per [`docs/adr/AGENTS.md`](../adr/AGENTS.md) (ADR-0085/ADR-0102),
a Draft ADR authorizes no implementation until a maintainer accepts it. This
prototype exists only because a demo was explicitly requested to make the
ADR's proposal concrete and discussable — not as a claim that the process was
satisfied. If ADR-0115 is accepted, the real implementation should be built
fresh against its full Decision section, not by extending this branch.

## What actually runs, for real

Every call in the demo gif is a real HTTP round trip through real code on
this branch: a new API router (`apps/api/src/curie_api/routers/delegate.py`),
a new auto-mounted `curie-delegate` MCP server (`runner/src/curie_runner/delegate.py`),
a new worker reply-sink adapter (`apps/worker/src/curie_worker/reply_sink.py`'s
`DelegationReplyAdapter`), and two new tables (`delegation_calls`,
`delegate_grants`, migration `apps/api/alembic/versions/0028_delegation_calls.py`).
Nothing is mocked or scripted at the HTTP/DB layer — the gif's `curl` calls and
the `curie local message` call all hit the same real endpoints.

## Documented deviations from the full ADR

1. **Reuses `TurnSource.WEBHOOK`** for the delegate target's turn, instead of
   a new message-lane source value. The real ADR (Decision part 2) wants a
   message-lane source; adding one is a breaking change to the frozen,
   tri-language-generated `aci-protocol` package requiring a minor version
   bump and full codegen regeneration — out of scope for a prototype.
2. **No formal suspend/resume.** The caller's turn ends normally (the model
   says "Asking bob about that." and stops); the reply arrives later as an
   ordinary new `QueuedTurn` on the same `conversation_id`, delivered by
   `DelegationReplyAdapter`. This is still asynchronous and non-blocking (the
   ADR's core requirement), just not implemented via the `Approval`-style
   suspend record the ADR's Decision part 3 describes.
3. **No bundle-manifest declaration and no on-wire call chain/depth.**
   Authorization is a flat `delegate_grants` table (caller, target, armed),
   armed directly by name via `POST /delegate/grants` rather than a
   bundle-declared allowlist. Depth is capped at 1 by refusing any call whose
   own `caller_conversation_id` already starts with `delegate:` — a stand-in
   for the ADR's chain/depth bound (Decision part 6), not that bound itself.
4. **A delegate-callable agent can hold no other channel binding.**
   `AgentChannel` has a real DB constraint of one binding per agent
   (`agent_channels_agent_id_key`); arming a target rebinds its one channel to
   `kind="delegation"`. The demo's target ("bob") is deliberately
   backend-only for this reason.
5. **No new `curie` CLI subcommand.** Arming a grant and inspecting a call are
   raw `curl` calls against two small endpoints
   (`POST /delegate/grants`, `GET /agents/{id}/delegate/calls`), not a real
   clap surface — a real subcommand needs command-manifest and mirror-file
   regeneration that only makes sense for code headed to `main`.
6. **The offline fake model can't reason about tool descriptions.** Since
   this demo runs with no live model credential, `runner/src/curie_runner/fake.py`
   adds a `[fake:delegate:<agent>]` marker (mirroring the existing
   `[fake:request-approval]` marker) that scripts a real
   `mcp__curie-delegate__call_agent` tool call — the decision to call is
   faked, but the call itself is real, the same "fake decision, real side
   effect" shape `request_approval`'s own fake-tier handling already uses.
7. **`GET /agents/{id}/delegate/calls[/{call_id}]` is a demo/ops convenience**
   with no equivalent in the ADR's design — added so the round trip could be
   inspected without a direct database connection.

## How the demo was actually recorded

The recording used a fully isolated stack (disposable Postgres/Valkey/RustFS
containers, `curie-api`/`curie-worker` run as host processes, a distinctly
tagged `curie-runner:adr0115proto` image) rather than `curie local up`,
because this development machine already had a shared Docker Compose project
(`compose.dev.yaml`'s pinned `name: curie`) in use by other work. A clean
single-developer machine can just use `curie local up` and the default
`:28000` port; see [`docs/demo/adr-0115-delegate.tape`](adr-0115-delegate.tape)
for the exact commands (set `$API` to your stack's URL).

## Scope not attempted

No kernel.py changes (the steering-predicate gap the ADR's Consequences
section names), no protocol/schema changes to `QueuedTurn`/`TurnSource`, no
bundle-manifest `delegates` field, no cross-tenant considerations, no
parallel fan-out. All explicitly out of scope for both the real ADR (v1) and
this prototype.
