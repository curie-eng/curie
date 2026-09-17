# Thinking depth: what a reasoning model costs you

Some models answer. Others think first, at length, and then answer. That second
kind is not slower because something is wrong — the thinking IS the product —
but it is dramatically slower, and until you know that is what you are looking
at, a turn that takes 45 seconds looks like a turn that is stuck.

This page is how to see that cost and how to change it. The decision behind the
shape lives in
[ADR-0098](adr/0098-thinking-depth-is-an-operator-knob-never-a-bundle-one.md).

## The measurement

Same question, same 2048-token ceiling, same endpoint Curie dials (OpenRouter's
Anthropic-compatible `/v1/messages`):

| model | wall clock | output tokens | of which thinking |
|---|---|---|---|
| `anthropic/claude-sonnet-4.5` | 2.4s | 95 | **0** |
| `z-ai/glm-5.2` | 8.1s | 380 | **268** (70%) |
| `z-ai/glm-5.2` with thinking disabled | **1.7s** | 182 | **0** |

`z-ai/glm-5.2` is not an outlier. `deepseek/deepseek-r1` defaults to 259
thinking tokens on that prompt and `qwen/qwen3-235b-a22b-thinking-2507` to 613.
Reasoning-on is simply the default for that class of model, while
`claude-sonnet-4.5` defaults it off — so comparing one of each and concluding
"the provider is slow" is the easy mistake to make (issue #1182).

## Turning it down

Two layers, both operator-owned. Nothing in a bundle can set or influence
either one, at any tier — the same rule that governs which model an agent runs.

Provisioned evals inherit both stored model and thinking settings. An explicit
model selected by an eval sweep takes precedence over the stored model.

**The platform default** is `CURIE_THINKING` on the worker, set wherever you set
the worker's other env (compose, or `worker.extraEnv` in the chart):

```bash
CURIE_THINKING=disabled
```

**Per agent** is the `thinking` column, and it wins over the platform default
for that agent only. Set it with the CLI (#1311); the raw PATCH this used to
document is what the one-entry-point rule exists to avoid:

```bash
curie local overrides deal-desk --thinking adaptive     # or `cluster`
curie local overrides deal-desk                         # read it back
curie local overrides deal-desk --clear-thinking        # back to the platform default
```

Clearing is `--clear-thinking`, never an empty value: an empty override reaches
the worker falsy, emits no boot key, and so skips the platform default instead
of restoring it. The API refuses a blank for that reason, and the CLI refuses it
one step earlier with a message naming the flag that does work.

The same verb carries `--model` / `--clear-model`, which had the identical gap.

### The vocabulary

| value | effect |
|---|---|
| `disabled` | no extended thinking at all |
| `adaptive` | the model decides when and how much |
| `enabled:<tokens>` | a fixed thinking budget, e.g. `enabled:2000` |

**Unset is not a fourth value.** With neither layer set, Curie sends the model
no thinking configuration whatsoever and the model's own default stands — which
is exactly how every install behaved before this knob existed. "No opinion" and
"explicitly adaptive" are different instructions, and only the first one is the
status quo.

A value outside that vocabulary fails the sandbox boot with a message naming
what is accepted. That is deliberate: a knob that silently ignores `disable`
(no trailing `d`) is worse than one that refuses it, because the operator
concludes the feature does not work.

## Before you disable it

Reasoning is what a reasoning model is *for*, and Curie's own workload — multi-
step tool loops, judgement calls, noticing its own mistakes — is where it earns
its cost. The 4.8x above is an argument for having the choice, not for making
it one way everywhere.

A small illustration from the same measurements: asked for the weather, every
model that was allowed to think said it had no real-time data. The fastest
zero-thinking model in the comparison made a number up. One prompt is not an
evaluation, but it is the shape of the trade.

If you have eval cases, you have the honest way to decide: run them at
`disabled`, at `adaptive`, and unset, and compare pass rate against latency on
your own agent instead of on a table in a doc.

## ⚠️ The OpenRouter trap

If you go looking for how to turn reasoning off on OpenRouter, you will find
`reasoning: {"enabled": false}`. **It does nothing on the endpoint Curie
uses**, and it does not error either — the request succeeds and the model keeps
thinking.

The reason is that OpenRouter has two doors, and Curie goes through the second
one:

| | `/v1/chat/completions` (OpenAI-shaped) | `/v1/messages` (Anthropic-shaped) |
|---|---|---|
| the parameter | `reasoning` | `thinking` |
| the response field | `reasoning`, `reasoning_details` | a `thinking` content block |
| the token count | `completion_tokens_details.reasoning_tokens` | `output_tokens_details.thinking_tokens` |
| `reasoning: {enabled: false}` | works | **silently ignored** |

Curie speaks the Anthropic Messages format everywhere (the runner is built on
claude-agent-sdk, and staying on that wire keeps prompt caching intact), so the
Anthropic vocabulary is the one that reaches the model. `CURIE_THINKING` sends
it for you; you should not need to touch the raw parameter at all.

Prompt caching, for the record, is **not** the problem: both `claude-sonnet-4.5`
and `z-ai/glm-5.2` were measured reusing a cached prefix through that endpoint.
Thinking tokens are where the time goes.

## Where the code is

| Concern | Path |
|---|---|
| The vocabulary and its parse | `runner/src/curie_runner/thinking.py` |
| Handing it to the SDK (omitted when unset) | `runner/src/curie_runner/adapter.py` |
| The two operator layers and their precedence | `apps/worker/src/curie_worker/binding.py::apply_model_env` |
| The platform default | `apps/worker/src/curie_worker/config.py` |
| The per-agent column | `apps/api/src/curie_api/models.py` |
| The declared boot key | `packages/aci-protocol/src/aci_protocol/session.py::BootEnv` |
