# Multi Surface Agents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one Curie agent serve multiple Slack or Discord surfaces, with adding a second binding acting as the implicit opt in.

**Architecture:** Keep the worker and queue contract adapter neutral. Persist multiple `(kind, address)` bindings per agent, resolve every turn by that pair, and send replies through the reply handle that arrived with the turn. Add Discord as an external adapter that translates Gateway messages into `/channels/turns` and neutral reply events back into Discord REST calls.

**Tech Stack:** FastAPI, SQLAlchemy, Alembic, PostgreSQL, Valkey, Python asyncio, Rust clap, React, TypeScript, Vitest, Playwright, Discord Gateway and REST APIs.

**Spec:** `docs/superpowers/specs/2026-08-21-multi-surface-design.md`

## Global Constraints

- Multi surface behavior is implicit: one binding is ordinary operation and adding a second binding opts the agent in.
- Product copy and CLI commands say `surface`; existing adapter protocol and API route names may retain `channel`.
- `(kind, address)` remains globally unique while `agent_id` is no longer unique in `agent_channels`.
- Every turn resolves and replies using its own surface kind. No fallback to another surface is allowed.
- Reply endpoints and adapter credentials remain write only.
- Do not change `packages/aci-protocol` or `packages/plugin-format`.
- Committed files must contain only public examples and must not name downstream repositories or deployments.
- Discord v1 supports mentions, text, adapter-created threads, streamed text replies, and no interactive approvals.

---

### Task 1: Multi Binding Persistence

**Files:**
- Create: `apps/api/alembic/versions/0028_agent_channels_multi_binding.py`
- Modify: `apps/api/src/curie_api/models.py`
- Test: `apps/api/tests/test_migration_0028_agent_channels_multi_binding.py`
- Test: `apps/api/tests/test_agent_model_integration.py`

**Interfaces:**
- Produces: `Agent.channels: list[AgentChannel]`, ordered by binding id for stable API serialization.
- Preserves: `UNIQUE(kind, address)` and a nonunique index on `agent_id`.

- [ ] **Step 1: Run the fail first migration and ORM tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/api/tests/test_migration_0028_agent_channels_multi_binding.py apps/api/tests/test_agent_model_integration.py`

Expected: failure because revision `0028` does not exist and `Agent.channel` is singular.

- [ ] **Step 2: Add the widening migration and plural relationship**

Implement `upgrade()` by dropping `agent_channels_agent_id_key` and creating `ix_agent_channels_agent_id`. Implement `downgrade()` by refusing when any agent has more than one row, then restoring the unique constraint and removing the plain index. Replace `Agent.channel` with:

```python
channels: Mapped[list["AgentChannel"]] = relationship(
    back_populates="agent", cascade="all, delete-orphan", lazy="selectin"
)
```

- [ ] **Step 3: Re-run the focused tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/api/tests/test_migration_0028_agent_channels_multi_binding.py apps/api/tests/test_agent_model_integration.py`

Expected: pass.

- [ ] **Step 4: Commit**

Run: `git add apps/api/alembic/versions/0028_agent_channels_multi_binding.py apps/api/src/curie_api/models.py apps/api/tests/test_migration_0028_agent_channels_multi_binding.py apps/api/tests/test_agent_model_integration.py && git commit -m "Allow agents to own multiple surface bindings"`

### Task 2: Binding Subresource and Safe Concurrent Writes

**Files:**
- Modify: `apps/api/src/curie_api/schemas.py`
- Modify: `apps/api/src/curie_api/crud.py`
- Modify: `apps/api/src/curie_api/routers/agents.py`
- Modify: `apps/api/src/curie_api/routers/channels.py`
- Modify: `apps/api/src/curie_api/channel_token.py`
- Test: `apps/api/tests/test_agent_channels_subresource.py`
- Test: `apps/api/tests/test_agent_channels_locking.py`
- Test: `apps/api/tests/test_agents.py`
- Test: `apps/api/tests/test_channels.py`

**Interfaces:**
- Produces: `POST /agents/{agent_id}/channels`, `PATCH /agents/{agent_id}/channels?kind=&address=`, and `DELETE` on the same selector.
- Produces: `AgentOut.channels: list[ChannelBinding]`.
- Produces: `lock_agent_bindings(session, agent_id) -> list[AgentChannel]`, `create_agent_channel`, `update_channel_binding`, and `delete_agent_channel`.
- PATCH semantics: omitted `endpoint` and `adapter` preserve stored values; explicit null for both clears the reply route; half pairs fail validation.

- [ ] **Step 1: Run the fail first API tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/api/tests/test_agent_channels_subresource.py apps/api/tests/test_agent_channels_locking.py apps/api/tests/test_agents.py apps/api/tests/test_channels.py`

Expected: failures because the plural schema and subresource CRUD do not exist.

- [ ] **Step 2: Implement schemas, row locking, savepoint conflict mapping, and routes**

Use `SELECT ... WHERE agent_id = :id ORDER BY id FOR UPDATE` with `populate_existing=True`. Create, move, and delete rows inside a nested transaction so a duplicate pair returns retryable `409` without releasing the outer row locks. Refuse deleting the final binding. Remove `channel` from `AgentUpdate` with `extra="forbid"` and serialize all rows through `AgentOut.channels`.

- [ ] **Step 3: Re-run the API tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/api/tests/test_agent_channels_subresource.py apps/api/tests/test_agent_channels_locking.py apps/api/tests/test_agents.py apps/api/tests/test_channels.py`

Expected: pass, including deterministic lock contention and duplicate pair outcomes.

- [ ] **Step 4: Commit**

Run: `git add apps/api && git commit -m "Add the agent surfaces subresource"`

### Task 3: Resolve and Reply on the Originating Surface

**Files:**
- Modify: `apps/worker/src/curie_worker/binding.py`
- Modify: `apps/worker/src/curie_worker/kernel.py`
- Modify: `apps/dispatcher/tests/test_dispatch.py`
- Test: `apps/worker/tests/binding/test_resolver.py`
- Test: `apps/worker/tests/kernel/test_multi_channel_respond_in_kind.py`

**Interfaces:**
- Consumes: binding identity `(ReplyHandle.kind, ReplyHandle.channel)`.
- Produces: `_thread_key_for(reply: ReplyHandle) -> str`, scoped by kind for all internal state while retaining the adapter address in outbound reply events.

- [ ] **Step 1: Run the fail first worker tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/worker/tests/binding/test_resolver.py apps/worker/tests/kernel/test_multi_channel_respond_in_kind.py apps/dispatcher/tests/test_dispatch.py`

Expected: failure when two kinds share an address or when state uses an unscoped address.

- [ ] **Step 2: Scope resolution and conversation state by kind and address**

Resolve deployments with `WHERE channel.kind = :kind AND channel.address = :address`. Derive the internal conversation key from both values. Keep `ReplyHandle` unchanged through enqueue, resume, approval, status, update, and post paths so one surface can never receive another surface's output.

- [ ] **Step 3: Re-run focused worker and dispatcher tests**

<!-- doclint:ignore-line -->
Run: `uv run pytest -q apps/worker/tests/binding/test_resolver.py apps/worker/tests/kernel/test_multi_channel_respond_in_kind.py apps/dispatcher/tests/test_dispatch.py`

Expected: pass.

- [ ] **Step 4: Commit**

Run: `git add apps/worker apps/dispatcher/tests/test_dispatch.py && git commit -m "Route each turn through its originating surface"`

### Task 4: Surface CLI and Console

**Files:**
- Modify: `cli/src/api.rs`
- Modify: `cli/src/commands.rs`
- Modify: `cli/src/main.rs`
- Modify: `cli/src/message.rs`
- Create: `cli/schema/surfaces.schema.json`
- Modify: `cli/schema/index.json`
- Modify: `cli/README.md`
- Test: `cli/tests/surfaces_verb.rs`
- Modify: `apps/ui/src/api/client.ts`
- Modify: `apps/ui/src/views/wired/WiredAgentDetail.tsx`
- Test: `apps/ui/src/api/api.test.ts`
- Test: `apps/ui/src/views/wired/WiredAgentDetail.test.tsx`
- Test: `apps/ui/e2e/agent-detail-wired.spec.ts`

**Interfaces:**
- Produces: `curie surfaces list|add|move|remove` with structured JSON and noninteractive writes.
- Produces: `addAgentSurface`, `patchAgentSurface`, and `removeAgentSurface` API client methods.
- Consumes: write only route values supplied by the user, never values read back from `AgentOut`.

- [ ] **Step 1: Run fail first CLI and UI tests**

Run: `cd cli && cargo test --test surfaces_verb`

<!-- doclint:ignore-line -->
Run: `cd apps/ui && pnpm test -- src/api/api.test.ts src/views/wired/WiredAgentDetail.test.tsx`

Expected: failures because the `surfaces` verb and plural editor are absent.

- [ ] **Step 2: Implement user facing surface operations**

Expose `surface` terminology in all help and labels. List the plural array, add a binding, move a selected pair with partial PATCH, and remove a selected pair while displaying the API's final-binding refusal. Regenerate `cli/command-manifest.json`, `cli/api-mirrors.json`, `apps/ui/src/generated/commandManifest.ts`, and `apps/api/openapi.json` using repository generators.

- [ ] **Step 3: Run CLI and UI verification**

Run: `cd cli && cargo fmt --check && cargo clippy -- -D warnings && cargo test`

Run: `cd apps/ui && pnpm lint && pnpm typecheck && pnpm test`

Expected: pass.

- [ ] **Step 4: Commit**

Run: `git add cli apps/ui apps/api/openapi.json && git commit -m "Expose multi surface management"`

### Task 5: Discord Adapter

**Files:**
- Create: `adapters/discord/pyproject.toml`
- Create: `adapters/discord/src/curie_discord_adapter/config.py`
- Create: `adapters/discord/src/curie_discord_adapter/state.py`
- Create: `adapters/discord/src/curie_discord_adapter/ingress.py`
- Create: `adapters/discord/src/curie_discord_adapter/egress.py`
- Create: `adapters/discord/src/curie_discord_adapter/main.py`
- Create: `adapters/discord/Dockerfile`
- Create: `adapters/discord/README.md`
- Create: `adapters/discord/tests/test_ingress.py`
- Create: `adapters/discord/tests/test_egress.py`
- Create: `adapters/discord/tests/test_state.py`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: Discord Gateway message events and `POST /channels/turns` with a per-binding bearer token.
- Produces: authenticated `POST /replies` accepting neutral `turn.status`, `reply.update`, and `reply.post` events.
- Persists: SQLite mapping from `(conversation, reply_ref)` to continuation message ids and completed event ids.

- [ ] **Step 1: Write and run fail first adapter tests**

Test the observable transforms: only bot mentions in configured parent channels start turns; adapter-created thread messages continue them; delivery ids equal Discord message ids; reply chunks are at most 2000 Unicode code points; repeated completion events do not post twice; invalid adapter secrets return `401`; allowed mentions are suppressed.

Run: `uv run pytest -q adapters/discord/tests`

Expected: import failure because the adapter package does not exist.

- [ ] **Step 2: Implement the adapter service**

Use the maintained Discord Gateway client for message receipt and Discord REST for create and edit operations. Create a public thread and placeholder for a new mention, then post the normalized turn to Curie. Authenticate neutral reply delivery with a constant-time comparison, edit the placeholder plus durable continuation messages for updates, and make `turn.status` a successful no-op.

- [ ] **Step 3: Re-run adapter tests and static checks**

Run: `uv run pytest -q adapters/discord/tests && uv run ruff check adapters/discord && uv run mypy adapters/discord/src`

Expected: pass.

- [ ] **Step 4: Commit**

Run: `git add adapters/discord pyproject.toml uv.lock && git commit -m "Add the Discord surface adapter"`

### Task 6: Documentation and End to End Verification

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `docs/guides/building-a-channel-adapter.md`
- Modify: `docs/interfaces/channel-ingress/INTERFACE.md`
- Modify: `docs/your-first-slack-agent.md`
- Create: `docs/guides/discord-adapter.md`

**Interfaces:**
- Produces: a reproducible demo showing one agent id and deployment answering in Slack and Discord with surface-specific thread continuity.

- [ ] **Step 1: Update public documentation**

Document implicit opt in, the `surfaces` commands, Discord configuration, supported v1 events, security boundaries, and the fact that failures never cross-fallback. Use only placeholder ids.

- [ ] **Step 2: Run repository checks**

<!-- doclint:ignore-line -->
Run: `uv lock --check && uv run ruff check . && uv run mypy && uv run lint-imports && bash scripts/check-docs.sh`

Run the relevant full Python, Rust, and UI suites after the focused suites pass.

- [ ] **Step 3: Perform the hands-on demo**

Start the core stack and Slack dispatcher, register one Slack and one Discord surface on the same agent id, send a distinct prompt from each, continue each thread, and verify replies return only to their source. Record the agent detail or CLI list plus both conversations in one video. Tear the stack down and confirm no Curie containers remain.

- [ ] **Step 4: Commit**

Run: `git add ARCHITECTURE.md docs && git commit -m "Document multi surface operation"`
