# Issue 1902 Claude Agent SDK structured replay spike

Date: 2026-08-30

Outcome: **PASS**

This disposable spike closes the SDK prerequisite recorded by
[ADR-0119](../adr/0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md)
and issue #1902. It ran against the repository-pinned `claude-agent-sdk`
0.2.135 and its bundled Claude Code 2.1.227 with a real supported provider.
No provider or gateway credential was written to a manifest or printed in the
evidence.

## Capability observed

The pinned SDK now exposes `ClaudeAgentOptions.session_store`. A first
`ClaudeSDKClient` mirrored its structured session transcript into a
`SessionStore`; a second client with no local transcript materialized that
store and resumed the session by UUID. This is a provider-native restoration
capability. Issue #1902 still needs Curie's durable representation and harness
boundary to remain portable rather than making the Claude transcript format the
platform contract.

The mirrored transcript contained ordered `user` and `assistant` entries plus
an assistant `tool_use` content block and its user `tool_result` content block.
The second client recovered a fixed marker from the prior tool result without
calling the tool again.

Observed positive run:

- session: `22649fa5-4979-438f-b79c-f5caf21fb07c`
- mirrored entries: 8
- structured `tool_use`: present
- structured `tool_result`: present
- runner A result: exact marker
- runner B result: exact marker
- runner B cache creation: 56 input tokens
- runner B cache read: 67,170 input tokens

The SDK's streaming-input API rejects a caller-authored assistant message as a
new query (`Expected message role 'user', got 'assistant'`), so Curie does not
inject history through that surface. A second spike reduced the restored store
to deterministic entries whose only durable inputs were each message's `role`
and `content`; UUIDs, parent links, CLI version, working directory, and timestamp
were reconstructed adapter metadata. A fresh client recovered the exact marker
and observed 67,641 cache-read tokens and 345 cache-creation tokens. This is the
fallback shape implemented by `build_structured_resume`. Provider JSONL is never
Curie's authoritative durable contract. The matching Claude harness does persist
it as an opaque optional checkpoint/delta optimization because reconstructed
minimal entries preserve semantics but do not preserve the provider's
message-cache breakpoint. A different harness ignores that checkpoint and uses
role/content.

## Cache-breakpoint negative arm

A separate run cloned the externally stored transcript twice. One fresh client
resumed the byte-stable history. Before the other fresh client resumed, the
spike changed the stored content of the first structured user message while
leaving the system and tool prefix unchanged.

Observed comparison for session
`7ad5365f-4748-4da4-a8af-34d9b22d6ee5`:

| Resume | Cache read input tokens | Cache creation input tokens |
| --- | ---: | ---: |
| unchanged stored history | 103,169 | 663 |
| changed stored history | 52,933 | 50,905 |

The changed-history arm lost the history-layer hit and rewrote that layer. Its
remaining cache read is expected: the unchanged earlier SDK/system/tool layer
has its own stable breakpoint. This positive/negative pair proves that the
fresh-client resume preserves the stored prefix and that changing the recovered
prefix invalidates the corresponding cache layer.

## SDK boundary found

The Python SDK does not expose caller-authored `cache_control` blocks on
`ClaudeAgentOptions`. Cache placement is owned by the bundled Claude Code
runtime. The observed usage counters prove that runtime's stable breakpoints;
Curie must treat explicit caller placement as unavailable and must not claim
that its portable harness contract controls provider cache directives.

Anthropic's current prompt-caching documentation states that automatic caching
moves the breakpoint forward with a growing conversation, while exact matching
is required for a hit. It also documents independent tools, system, and messages
cache levels. Those properties explain both the native-checkpoint requirement
and why a changed-history negative may retain an earlier system/tool read:
[prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
and [tool-use cache hierarchy](https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-use-with-prompt-caching).

Implementation therefore uses three layers:

1. A harness-neutral durable structured conversation and stable-summary model
   owned by Curie.
2. An optional opaque checkpoint/delta from the matching harness. Claude uses
   this to restore the provider-native structured session through `SessionStore`;
   portable replay remains sufficient when the checkpoint is absent or belongs
   to another harness.
3. A first-resumed-turn metric reporting observed cache-read tokens.

A harness that implements neither structured message replay nor an equivalent
provider-native restore must declare the capability absent and fail the resume;
it must not fall back to the legacy rendered transcript preamble.

## Durable regression evidence

The implementation leaves both offline and opt-in provider regressions in the
repository. On 2026-08-30 the following disposable provider command passed in
30.41 seconds against two selected tests:

```text
CURIE_E2E_LIVE=1 uv run pytest -q runner/tests/test_live.py \
  -k 'structured_replay_cache_hit or cross_runner_approval_exact_once' -vv
```

The cache test captures one tool-bearing portable prefix plus its optional native
checkpoint, primes a continuation, reconstructs that exact checkpoint in a fresh
client, and then changes one recovered native user message in another fresh
client. It asserts exact marker recovery, a positive cache read for the identical
prefix, and either a lower read or renewed creation for the changed arm. The
total read may remain positive because the unchanged SDK/system layer has an
independent breakpoint. The approval test blocks
a disposable Bash append in runner A (the file remains absent), persists the
structured suspension including an explicit non-executed tool result, resumes
runner B with a one-shot grant, and asserts the file has exactly one line. A
second attempt on runner B pauses again and the file remains one line. The same
run asserts exactly one `curie.history.resume.cache_read` observation with a
positive value and `cache_hit=true`.

The two approval-runner identities in that evidence are the distinct trace names
`live-structured-approval-block` (runner A) and
`live-structured-approval-resume` (runner B), both bound to the same portable
thread identity `live-approval-thread-1902`.

The offline companion in `runner/tests/test_structured_replay_e2e.py` proves the
same one-shot/re-arm flow without a provider and proves rejected and expired
resume decisions authorize zero calls.

## Reproduction shape

The spike used two sequential `ClaudeSDKClient` instances with the same working
directory and UUID but separate session stores. The first store's JSON-safe
entries were serialized and copied into a new store before the second client
connected. The negative arm deep-copied those entries and changed the first
structured user message before connecting another fresh client. Each client
consumed a terminal `ResultMessage`; evidence came from the stored content-block
types and `ResultMessage.usage` counters. The exact local transcript created by
the disposable first client was deleted after its mirrored copy had been
verified.
