# runner

The runner image and SDK adapter: the productized prototype,
a long-lived streaming session server that implements the full ACI (Agent
Container Interface) v0.1 contract from `packages/aci-protocol`. Built on
`claude-agent-sdk` (Python).
Runs inside a claimed Agent Sandbox; the CLI (`curie skill up`) also runs it
locally in Docker.

## What it does

- One long-lived `claude-agent-sdk` streaming-input session per process (one per
  sandbox) -- the source of prompt-cache affinity across turns.
- Accepts inbound ACI frames (`event` of type message | job | eval_case, and
  `interrupt`) and streams outbound NDJSON (newline-delimited JSON:
  `text_delta` | `tool_note` | `final` | `error` | `side_effect_flag`) with
  protocol-version enforcement.
- Enforces `CURIE_BUDGET.max_output_tokens_per_run` (halts a run with a
  classified-failure final) and hands the daily USD cap to the SDK natively.
- Emits `side_effect_flag` when a non-idempotent tool executes (read-only
  allowlist, deny-by-default; see `side_effects.py`).
- Loads and validates the mounted plugin bundle via `plugin_format.validate_bundle`.
- Exports gen_ai OTel spans: an `agent.run` root with duration-bearing
  `llm.generation` provider-wait and `execute_tool` tool-wait siblings, via
  OTLP-HTTP (the OpenTelemetry Protocol over HTTP) to the collector, which
  forwards to Langfuse.
- Rehydrates from a history ref on start (`resume`), stateless-first
  (ADR-0003, an Architecture Decision Record).

## HTTP surface (ACI channel)

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness. |
| GET | `/status` | Session status (done / idle-awaiting-input / classified-failure), readiness, turn state. |
| POST | `/v1/event` | Open a turn: body is an ACI `event` frame; streams outbound NDJSON, ending in a `final`. |
| POST | `/v1/steer` | Inject a follow-up into the live turn (`{"text": ...}`); 409 when no turn is active. |
| POST | `/v1/interrupt` | Hard-stop the live turn: body is an ACI `interrupt` frame. |

One turn consumes the SDK generator at a time; steer and interrupt are
side-channel injections whose output surfaces on the open `/v1/event` stream (the
proven steering pattern). The finish race (a steer arriving as a turn ends,
409) is owned by the worker.

The three POST routes (`/v1/event`, `/v1/steer`, `/v1/interrupt`) require an
`Authorization: Bearer <token>` header matching `CURIE_RUNNER_TOKEN` when that
env var is set, returning 401 otherwise. This is per-sandbox transport auth
(defense-in-depth on the ACI ingress alongside the NetworkPolicy), not part of
the frozen ACI wire contract. Enforcement is only-when-configured: with the var
unset the app is pass-through (CLI, fake-model CI, and pre-token sandboxes stay
unauthenticated). `GET /healthz` and `GET /status` are never gated (the chart
readinessProbe hits `/healthz`).

## Environment

- **ACI-frozen** (`aci-protocol.SessionConfig`): `CURIE_PLUGIN_DIR`,
  `CURIE_SESSION_ID`, `CURIE_SANDBOX_ID`, `CURIE_BUDGET`, optional
  `CURIE_MEMORY_REF` / `CURIE_CREDENTIALS`, `OTEL_EXPORTER_OTLP_*`.
- **Runner-local**: `CURIE_MODEL`, `CURIE_MAX_TURNS`,
  `CURIE_HISTORY_REF` (rehydrate; falls back to `CURIE_MEMORY_REF`),
  `CURIE_HISTORY_MAX_TURNS` / `CURIE_HISTORY_MAX_BYTES` (bound the rehydrated
  history preamble to a tail window; defaults 40 turns / 16000 bytes, a
  nonpositive value falls back to the default),
  `CURIE_RUNNER_PORT`, `CURIE_RUNNER_TOKEN` (per-sandbox bearer token gating
  the three ACI POST routes; enforced only when set), `CURIE_FAKE_MODEL`
  (offline smoke; no model call), `CURIE_DISALLOWED_TOOLS` (optional
  comma-separated tool names removed from the session and refused even under
  bypassPermissions; unset keeps every tool available). This stops the named
  tools, not the underlying capability (#2429): denying
  `mcp__curie-state__append`/`set`/`delete` does not revoke
  `CURIE_STATE_TOKEN`, which stays in the sandbox environment, so a
  still-permitted shell tool (e.g. `Bash`) can reach the same HTTP state API
  directly. Naming `Bash` alongside the state tools closes that path today;
  removing the token itself needs a code change, not a config knob.

## Build and smoke

The image compiles against the frozen workspace packages, so build from the repo
root with `curie build` (the one build entry point; it wraps the
`docker build -f runner/Dockerfile` under the hood):

```bash
curie build
# Offline round-trip (fake model, no credential), OTel to the dev collector:
docker run -d --name runner-smoke --network curie_default \
  -e CURIE_FAKE_MODEL=1 -e CURIE_PLUGIN_DIR=/unused \
  -e CURIE_SESSION_ID=smoke -e CURIE_SANDBOX_ID=sbx \
  -e 'CURIE_BUDGET={"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}' \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
  -p 18080:8080 curie-runner
curl -sN -X POST http://localhost:18080/v1/event -H 'Content-Type: application/json' \
  -d '{"kind":"event","type":"message","text":"hi","user":"U","ts":"1.0"}'
```

## MCP load check (offline, credential-free)

`python -m curie_runner.check` is a separate, one-shot entrypoint (issue #337)
that answers "do this bundle's MCP (Model Context Protocol) tools actually
load?" without a model turn. It
validates the bundle via the frozen `load_plugins`, then builds a real
`ClaudeSDKClient` and `connect()`s (no query), polls `get_mcp_status()` until the
bundle's own servers settle, and compares the **declared** servers against the
plugin-owned **registered** ones. `declared` is the union of the MCP-config
servers (`plugin.json` `mcpServers` and a bare `.mcp.json`) and the bundle's
`connectors.yaml` connectors, each name counted once; the check mounts the
connector forms this tier can actually reach (a remote `url:` and a hosted
connector's `unhosted_url:` fallback), so those are genuinely exercised, while
a purely hosted connector is declared but not exercisable here. It reads
`CURIE_PLUGIN_DIR` (and optional `CURIE_CHECK_TIMEOUT_S`, default 30); it
forwards and reads **no** credential.

```bash
CURIE_PLUGIN_DIR=/plugin python -m curie_runner.check
```

It prints exactly one JSON object to **stdout** (all logging goes to **stderr**)
and exits with the verdict code:

- `0` green: every declared MCP server registered connected with at least one tool
- `1` red: a declared server failed to load (never registered, connected with zero
  tools, `failed`/`needs-auth`/`pending` at the deadline, or the init timed out),
  including a declared `connectors.yaml` connector this tier cannot host
- `2` invalid_bundle: the bundle fails `plugin_format` validation or the plugin dir
  is missing

The JSON shape (frozen contract) is `{check, version, plugin_dir, declared,
registered, matches, verdict, reasons, hints}`; `reasons` is non-empty iff the
verdict is not green. A manifest `mcpServers` string pointer is now rejected by
`plugin_format` validation (step 1, `invalid_bundle`) before this check ever runs,
so the #336 string-pointer hint fires only when `extract_declared`/`evaluate` are
exercised directly (e.g. in tests), not through this entrypoint.
The `curie skill check` CLI verb wraps this as a one-shot container.

## Verify (from repo root)

```bash
uv run pytest runner/tests -q   # unit + integration + conformance
uv run ruff check . && uv run mypy
```

Live tests (`runner/tests/test_live.py`) run only when `CLAUDE_CODE_OAUTH_TOKEN`
or `ANTHROPIC_API_KEY` is present; otherwise they are skipped.
