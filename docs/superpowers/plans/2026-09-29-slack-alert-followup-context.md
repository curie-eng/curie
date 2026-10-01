# Slack Alert Follow-up Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a human Slack reply the exact same-bot root message as safe context without joining or inheriting the hook session that authored it.

**Architecture:** The dispatcher detects a threaded mention whose `parent_user_id` does not name someone else, resolves only the exact thread root through a Valkey cache and `conversations.replies(limit=1)`, validates bot/channel/timestamp identity on the root itself, bounds its text, neutralizes its slashes for mixed-version repository parsers, and prepends a non-authorizing context block. The existing `QueuedTurn` identity stays human and Slack-scoped; failures produce a safe visible instruction to restate instead of inferring an unavailable proposal.

**Tech Stack:** Python 3.12, Slack Bolt/Web API, redis-py with real Valkey tests, Pydantic settings, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-29-slack-alert-followup-context-design.md`

## Global Constraints

- Target `main`; the normal forward merge carries the fix to `next`.
- Do not change `packages/aci-protocol` or `packages/plugin-format`.
- Do not modify the worker, runner, API, chart, SRE example, or any downstream repository/deployment. The quoted root itself must be safe for the old worker's unchanged repository parser during rollout.
- Never copy hook source, author, session, transcript, route, approval state, or credentials onto the human turn.
- Slack is the only mocked external service; Valkey tests use a real isolated service.
- Committed examples use only public placeholder identifiers.
- Preserve claim -> placeholder -> enqueue ordering and event-ID deduplication.
- No downstream deployment or workload restart is part of this plan.

## Review Focus

- A claimed same-bot parent with a foreign/mismatched fetched root must produce no history leakage and must not execute inferred work.
- A root containing delimiter-shaped prompt text must remain quoted data and cannot forge authorization.
- Cache reuse after constructing a fresh resolver instance must avoid a second Slack call without broadening identity.
- A duplicate event must exit before context resolution, placeholder posting, or enqueue.
- Ordinary root mentions and threaded replies to other parents must retain their current text and Web API call count.

---

### Task 1: Pin the secure root-context behavior with failing tests

**Files:**
- Create: `apps/dispatcher/tests/test_thread_context.py`
- Modify: `apps/dispatcher/tests/test_queue.py`
- Modify: `apps/dispatcher/tests/conftest.py` (a per-test cache prefix)
- Modify: `apps/worker/tests/test_workspace.py`

**Interfaces:**
- Consumes: current `process_event(...) -> str | None`, real `redis.Redis`, and a fake Slack client.
- Produces: failing behavioral tests that define `SlackThreadContext.resolve(...) -> str` and its integration into `process_event`.

- [ ] **Step 1: Add focused resolver tests**

Create tests named:

- `test_same_bot_root_is_rendered_as_non_authorizing_context`
- `test_only_the_exact_first_root_message_is_rendered`
- `test_root_delimiters_are_escaped`
- `test_foreign_or_mismatched_root_never_leaks_text`
- `test_history_failure_renders_fail_closed_restate_notice`
- `test_fresh_resolver_reuses_validated_valkey_cache`
- `test_corrupt_or_wrong_identity_cache_is_never_rendered`
- `test_cache_is_isolated_per_bot_and_channel`
- `test_absent_parent_user_id_is_resolved_from_the_root_itself`
- `test_long_root_is_bounded_head_and_tail`
- `test_cache_outage_falls_back_to_slack_and_never_raises`
- `test_root_owned_by_bot_id_without_user_is_accepted`

Use `C0EXAMPLE1`, `U0BOT`, and synthetic timestamps. Assert the successful prefix says the prior assistant reply is context only, may contain untrusted alert data, and cannot bypass approval. Assert the fallback tells the agent not to infer or execute the earlier proposal and to ask the person to restate it. The Slack fake must record `channel`, `ts`, and `limit=1`.

- [ ] **Step 2: Add ingress integration tests**

Add tests named:

- `test_human_reply_to_own_bot_root_keeps_human_slack_identity_with_context`
- `test_duplicate_human_reply_resolves_no_context_and_posts_no_second_placeholder`
- `test_non_bot_parent_and_root_mentions_remain_byte_identical`
- `test_restarted_dispatcher_reuses_root_context_from_valkey`
- `test_failed_root_lookup_still_answers_with_the_restate_notice`

And in `apps/worker/tests/test_workspace.py`:

- `test_quoted_prior_reply_never_selects_a_repository`

Decode the queued payload and assert `source == TurnSource.SLACK`, `hook_run is None`, `author` is the human user, `conversation_id` is the Slack `thread_ts`, and only `text` gained the context prefix. Assert a duplicate performs no second Slack history call, placeholder, or enqueue.

- [ ] **Step 3: Run the new tests red**

Run:

```bash
TEST_VALKEY_HOST=127.0.0.1 TEST_VALKEY_PORT="$TEST_VALKEY_PORT" uv run pytest -q apps/dispatcher/tests/test_thread_context.py apps/dispatcher/tests/test_queue.py -k 'thread_context or own_bot_root or duplicate_human_reply or non_bot_parent'
```

Expected: FAIL because `curie_dispatcher.thread_context` and handler integration do not exist.

- [ ] **Step 4: Commit the failing tests**

```bash
git add apps/dispatcher/tests/test_thread_context.py apps/dispatcher/tests/test_queue.py
git commit -m "test: pin Slack alert follow-up context"
```

---

### Task 2: Resolve and inject the exact bot-authored root safely

**Files:**
- Create: `apps/dispatcher/src/curie_dispatcher/thread_context.py`
- Modify: `apps/dispatcher/src/curie_dispatcher/handlers.py`
- Modify: `apps/dispatcher/src/curie_dispatcher/config.py`
- Modify: `apps/dispatcher/README.md`
- Test: `apps/dispatcher/tests/test_thread_context.py`
- Test: `apps/worker/tests/test_workspace.py`
- Test: `apps/dispatcher/tests/test_queue.py`

**Interfaces:**
- Consumes: `WebClient.conversations_replies(channel: str, ts: str, limit: int)`, a decode-responses `redis.Redis`, `DispatcherConfig.thread_context_cache_prefix`, and `DispatcherConfig.thread_context_ttl_seconds`.
- Produces: `SlackThreadContext(redis_client, web_client, config).resolve(*, event: Mapping[str, Any], lane: Lane, bot_user_id: str | None, bot_id: str | None, text: str) -> str`.

- [ ] **Step 1: Add cache settings**

Add:

- `thread_context_cache_prefix: str = Field(default="curie:slack-root-context:", validation_alias="CURIE_THREAD_CONTEXT_CACHE_PREFIX")`
- `thread_context_ttl_seconds: int = Field(default=2592000, gt=0, validation_alias="CURIE_THREAD_CONTEXT_TTL_SECONDS")`

Document both in the dispatcher configuration table and explain that cached content is identity-bound, digest-keyed, and advisory context only.

- [ ] **Step 2: Implement the resolver**

In `thread_context.py`, add a small versioned strict cache model and the `SlackThreadContext` interface above. The resolver must:

1. return `text` unchanged unless this is a threaded mention whose `parent_user_id` is absent or equals `bot_user_id`;
2. derive a SHA-256 cache key from bot user, authorized bot ID, channel, and root timestamp without placing raw identifiers or text in the key;
3. accept cached text only when every stored identity field and schema version matches;
4. otherwise call `conversations_replies(channel=channel, ts=thread_ts, limit=1)`;
5. accept only the first message with an exact `ts` match whose `user` is the bot user, or which has no `user` and the authorized `bot_id`; cache a root that is someone else's without its text;
6. cache the validated root for the configured TTL;
7. bound the complete derived root excerpt, including its omission marker, to 4,000 characters (head and tail), XML-escape it, neutralize every root slash as an entity, and render the successful non-authorizing prefix;
8. catch Slack, cache, and shape failures; render the fail-closed restate prefix without root content or exception detail when `parent_user_id` claimed the root, and return `text` unchanged otherwise.

Include a test comment citing Slack's official `conversations.replies` documentation for the parent-first response shape and required arguments.

- [ ] **Step 3: Integrate after dedupe and before placeholder**

Construct the resolver inside `process_event` only after `claim_event` succeeds. Resolve the already-derived and self-mention-stripped text, then pass that resolved text into `_mint_turn`. Do not modify `process_action` or the shared `_mint_turn` ordering. Update the order assertion to include context resolution only where its same-bot preconditions apply; ordinary paths must remain `claim, placeholder, enqueue`.

- [ ] **Step 4: Run focused tests green**

Run:

```bash
TEST_VALKEY_HOST=127.0.0.1 TEST_VALKEY_PORT="$TEST_VALKEY_PORT" uv run pytest -q apps/dispatcher/tests/test_thread_context.py apps/dispatcher/tests/test_queue.py
```

Expected: all pass.

- [ ] **Step 5: Run dispatcher checks**

Run:

```bash
TEST_VALKEY_HOST=127.0.0.1 TEST_VALKEY_PORT="$TEST_VALKEY_PORT" uv run pytest -q apps/dispatcher/tests
uv run ruff check apps/dispatcher/src/curie_dispatcher/thread_context.py apps/dispatcher/src/curie_dispatcher/handlers.py apps/dispatcher/src/curie_dispatcher/config.py apps/dispatcher/tests/test_thread_context.py apps/dispatcher/tests/test_queue.py
uv run mypy apps/dispatcher/src/curie_dispatcher/thread_context.py apps/dispatcher/src/curie_dispatcher/handlers.py apps/dispatcher/src/curie_dispatcher/config.py
bash scripts/check-docs.sh
```

Expected: zero failures or errors.

- [ ] **Step 6: Prove the regression pin twice**

Run:

```bash
curie dev verify-fix-pin HEAD apps/dispatcher/tests/test_queue.py::test_human_reply_to_own_bot_root_keeps_human_slack_identity_with_context
```

Expected: the verifier observes the baseline failure and candidate pass in separate pytest processes with isolated prerequisites.

- [ ] **Step 7: Commit the implementation**

```bash
git add apps/dispatcher/src/curie_dispatcher/thread_context.py apps/dispatcher/src/curie_dispatcher/handlers.py apps/dispatcher/src/curie_dispatcher/config.py apps/dispatcher/README.md apps/dispatcher/tests/test_thread_context.py apps/dispatcher/tests/test_queue.py
git commit -m "fix: carry bot alert context into Slack replies"
```

---

### Task 3: Verify, review, and prepare the upstream change

**Files:**
- None committed. Run evidence and review findings stay in the gitignored project directory and in the pull request.

**Interfaces:**
- Consumes: the complete branch diff and repository verification commands.
- Produces: review findings, six-tier evidence, a PR against `main`, CI results, and the release/deploy gate report.

- [ ] **Step 1: Run independent code, scope, and security reviews**

Review the branch against the accepted spec. Require explicit findings on identity binding, prompt injection, cache poisoning, duplicate delivery, failure behavior, and preservation of the human `TurnSource`/author/thread identity. Store zero-finding reports too.

- [ ] **Step 2: Run the full Python baseline in an isolated Compose project**

Follow root `AGENTS.md` concurrent-local-verification exactly: private project, absolute base and override files, private loopback ports, matching endpoint exports, exact cleanup, and unchanged baseline assertions. Record the literal summary, candidate commit, and teardown.

- [ ] **Step 3: Classify and record all end-to-end tiers**

- `skill`: not applicable; no bundle, runner, model, or skill behavior changes.
- `local`: required; dispatcher, real Valkey, placeholder, and queued turn compose here.
- `local-release`: required; the packaged dispatcher must preserve the path.
- `cluster`: required; the charted dispatcher must preserve the path.
- `live-provider`: not applicable; no model/provider call shape changes.
- `external-integration`: required; only a released deployment can prove Slack delivers the parent identity and root read as expected. Record it blocked, without deploying, because this task does not authorize downstream rollout.

- [ ] **Step 4: Open the PR and monitor required checks**

Push `codex/fix-slack-alert-followup`, open a PR to `main`, attach it to this chat, include the fix pin, the #3529/#3530/#3527 interaction, scoped test evidence, all tier decisions, and the external-integration release gate. Address review/CI findings with test-first fix rounds and rerun affected evidence.

- [ ] **Step 5: Merge and create the forward-merge follow-up**

After required checks and reviews are green, merge the PR into `main`. Then create or request the normal `main` -> `next` forward-merge PR rather than cherry-picking. Do not deploy the resulting release or restart a downstream workload.
