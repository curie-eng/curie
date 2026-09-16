# CLAUDE.md - runner

The runner image and SDK adapter: a long-lived `claude-agent-sdk`
streaming session server implementing the full ACI v0.1 contract from
`packages/aci-protocol`. Runs inside a claimed Agent Sandbox, or locally in
Docker via `curie skill up`. Full behavior spec in `runner/README.md`.

## Load-bearing invariants

- **The ACI environment contract is mandatory, not optional.** `SessionConfig.from_env`
  (from `packages/aci-protocol`) reads `CURIE_PLUGIN_DIR`, `CURIE_SESSION_ID`,
  `CURIE_SANDBOX_ID`, `CURIE_BUDGET`, and the optional `CURIE_MEMORY_REF`/
  `CURIE_CREDENTIALS`/`OTEL_EXPORTER_OTLP_*`. Do not invent a parallel
  config path for the same values -- this environment shape is frozen with
  `aci-protocol`; a new required field is a contract change (raise it in an
  issue/PR first).
- **One long-lived SDK session per process.** This is the source of
  prompt-cache affinity across turns (ADR-0003) -- do not spin up a fresh SDK
  session per turn or per request; that throws away the cache-reuse property
  the whole design depends on.
- **Budget enforcement is the runner's job, locally.** `CURIE_BUDGET.max_output_tokens_per_run`
  halts a run with a classified-failure `final`; the daily USD cap is handed
  to the SDK natively. This is per-run/per-process enforcement only --
  end-to-end budget wiring through the API and UI Cost view is separate,
  not-yet-built work; do not assume this runner-local enforcement is the
  whole budget story.
- **`side_effect_flag` uses a deny-by-default, read-only allowlist**
  (`side_effects.py`). A new tool defaults to "not idempotent" until
  explicitly allowlisted -- never flip the default to allow-by-default, since
  the worker's no-retry-after-side-effects rule depends on this flag being
  conservative.
- **Rehydration on start is stateless-first** (ADR-0003): the runner
  rehydrates external state on boot rather than assuming any surviving
  in-process state. Two distinct external refs resolve over the durable state
  API: `CURIE_HISTORY_REF` is this thread's structured conversation transcript
  (`history.py`, ADR-0119/0029), while `CURIE_MEMORY_REF` is the agent's durable
  memory and still enters the system prompt (`memory.py`, ADR-0025).
  `CURIE_HISTORY_REF` is never itself an SDK resume id; the harness adapter may
  consume optional opaque checkpoint/delta data for cache fidelity or reconstruct
  an ephemeral provider resume envelope from its portable messages. The portable
  messages remain authoritative across harnesses.
  Never write a code path that depends on the runner having been "the same
  process" as an earlier turn.
- **One turn consumes the SDK generator at a time.** Steer and interrupt are
  side-channel injections whose output surfaces on the already-open
  `/v1/event` stream -- do not open a second concurrent generator for a steer.
- **`CURIE_FAKE_MODEL` must stay a true offline no-op.** It exists so CI,
  the CLI's `curie skill up --fake-model`, and the chart's default runner pool
  can round-trip ACI events with zero credential and zero network call. Any
  change to the fake-model path must preserve "no model call, no network"
  or it breaks all three of those consumers silently.
- **The runner expects to run non-root.** The chart's security rails
  (`charts/curie/CLAUDE.md`) assume the runner container tolerates
  `runAsNonRoot`, a read-only rootfs, and writable-emptyDir scratch only at
  `/tmp` and `/home/runner`. Do not add a code path that writes anywhere
  else in the container filesystem.

## Verify

```bash
docker build -f runner/Dockerfile -t curie-runner .   # from repo root; compiles against the frozen workspace packages
uv run pytest runner/tests -q                            # unit + integration + conformance
uv run ruff check runner/ && uv run mypy
```

The ordinary provider smokes in `runner/tests/test_live.py` run only when
`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` is present. The disposable
structured-replay/cross-runner cases require an explicit `CURIE_E2E_LIVE=1` and
may use an already authenticated local Claude SDK. Without those gates they are
skipped, so a green default run does not exercise the real model path -- do not
treat it as equivalent to a live pass. The conformance suite
(`run_conformance` from `packages/aci-protocol`) must return `passed=True`
against this runner's producer; a change that breaks conformance is a
contract violation even if the runner's own tests pass.
