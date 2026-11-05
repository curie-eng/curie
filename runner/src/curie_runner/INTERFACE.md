# INTERFACE: model-provider base-URL override seam

The seam that lets the runner target any Anthropic-compatible endpoint without
an Anthropic credential. Implemented in `sdk_auth.resolve_base_url_override`.

## Purpose

A generic, provider-agnostic base-URL override. When configured, the runner
points the claude-agent-sdk (and the bundled `claude` CLI it spawns) at a local
or third-party endpoint instead of real Anthropic, using a no-op placeholder in
place of a credential. Local Ollama is the FIRST implementation (issue #46).
OpenRouter (issue #24) is implemented as a worked example of the same seam
rather than a parallel path. This is one seam, many providers.

## Env contract

### `ANTHROPIC_BASE_URL` (input)

When set to a non-empty value, the runner enters override mode. Unset or empty
means normal Anthropic credential resolution (`resolve_model_credential`).

### `CURIE_MODEL_API_BACKEND` (input)

Declares the endpoint's wire protocol instead of leaving it assumed: one of
`messages`, `chat_completions`, `responses` (the `ApiBackend` members). Unset or
empty means `messages`, which is what override mode previously assumed of every
endpoint. Only `messages` is dialable — the runner speaks the Anthropic Messages
wire format via claude-agent-sdk — so `resolve_sdk_env` checks the declaration
FIRST, before any credential or base-URL branching, and rejects an OpenAI-shaped
backend with an actionable error rather than mis-dialing it. Reaching one needs a
translating proxy in front of the runner. See ADR-0048.

### `CURIE_MODEL_ENV_KEY` (input)

Declares which env var(s) carry the credential: a bare name or a JSON array of
names. The keys are walked in declared order and the first that is both set and
non-empty wins; a present-but-empty key is skipped, never wins. Unset or empty
defaults to `CURIE_CREDENTIALS`, the single hardcoded name read before this.

### `CURIE_MODEL_AUTH_MODE` (input)

Selects who authenticates model work: exactly `service` or `personal`. Unset
means `service`, which is every rule on this page. Any other present value,
including the empty string, raises `InvalidAuthModeError` before the CLI starts.
`personal` is checked right after the wire-protocol gate: `resolve_sdk_env`
returns no override and maps nothing, so this seam's base URL override and
`CURIE_CREDENTIALS` mapping never apply, and the owner's `CLAUDE_CODE_OAUTH_TOKEN`
is the only grant. From the `CLAUDE_*` and `ANTHROPIC_*` namespaces the CLI
parent env then keeps only model selection, `CLAUDE_CODE_SHELL`,
`CLAUDE_CODE_DISABLE_TERMINAL_TITLE`, `CLAUDE_CODE_MAX_OUTPUT_TOKENS` and
`CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`. Every other name there is removed,
including `CLAUDE_SECURESTORAGE_CONFIG_DIR` and subagent model forcing
(`CLAUDE_CODE_SUBAGENT_MODEL`, `CLAUDE_CODE_SUBAGENT_MODEL_FORCE`,
`CLAUDE_CODE_WORKFLOW_SUBAGENT_MODEL`), so reviewer roles keep the configured
reviewer model. Workspace settings `env` may set no name from those namespaces
and workspace `availableModels` is refused; managed settings `env` may set only
the kept `CLAUDE_*` names from them, and a managed `availableModels` must list
both the implementer and the reviewer model. The personal policy lives in
`personal_auth.py` (#5373, ADR 0268); see the runner README, "Personal
subscription mode".

### `ANTHROPIC_API_KEY` (output, set by the runner)

A NON-EMPTY no-op placeholder (`not-needed`, the `NO_OP_API_KEY` constant).

Empirical gotcha (verified 2026-07-07): the bundled Claude CLI treats an EMPTY
`ANTHROPIC_API_KEY` as "not logged in" and refuses to call the endpoint before
it ever dials the base URL. The placeholder MUST therefore be non-empty to pass
the CLI auth gate. It is deliberately not `sk-...` shaped so it can never be
mistaken for a real credential, and paired with the overridden base URL it
cannot authenticate against real Anthropic.

### `CLAUDE_CODE_OAUTH_TOKEN` / `ANTHROPIC_AUTH_TOKEN` (output, set by the runner)

Both are blanked to `""` in override mode so an inherited OAuth token or Bearer
token cannot take precedence over the placeholder + overridden base URL, nor
leak as a Bearer header to the overridden (local/third-party) endpoint. This
keeps override mode hermetic. (On the OpenRouter path, `resolve_model_credential`
re-sets `ANTHROPIC_API_KEY` to the real `sk-or-` key (the `x-api-key` header
OpenRouter's Anthropic endpoint reads) after applying this override;
`ANTHROPIC_AUTH_TOKEN` stays blank.)

### `CURIE_MODEL` (input)

The model name (e.g. `qwen3:4b`), passed through to `ClaudeAgentOptions.model`.

## OpenRouter mapping

An `sk-or-...` credential in the declared credential env var (`CURIE_CREDENTIALS`
unless `CURIE_MODEL_ENV_KEY` says otherwise) is auto-detected by
`resolve_model_credential`. The runner sets
`ANTHROPIC_BASE_URL=https://openrouter.ai/api` and places the real key in
`ANTHROPIC_API_KEY` (sent as the `x-api-key` header, which OpenRouter's Anthropic
Messages endpoint authenticates on), overriding this seam's `not-needed`
placeholder, and leaves `ANTHROPIC_AUTH_TOKEN` and `CLAUDE_CODE_OAUTH_TOKEN`
blank.

Prompt caching survives only on this native Anthropic Messages path, and only
for Claude-family OpenRouter models.

## How it arrives

- Docker/compose path: the worker injects it per-claim, mapping
  `CURIE_MODEL_BASE_URL` to `ANTHROPIC_BASE_URL` and `CURIE_MODEL` on the
  runner boot env (`apps/worker/src/curie_worker/binding.py`).
- Cluster path: the chart bakes it into the SandboxTemplate when
  `inference.deploy=true` (`charts/curie/templates/agent-sandbox.yaml`).

## Boundary

Documentation and one env-mapping function only. No speculative multi-provider
abstraction: that arrives with issue #24's second path. Tracked in
`.projects/prototype-review/seam-points-catalog.md`.
