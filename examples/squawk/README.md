# Squawk

One durable LIFO stack, shared by every channel bound to the agent. Say
something and it is pushed; say nothing and the newest entry is popped.

```
you:     the deploy is blocked on the migration
squawk:  Squawk!
you:     (empty message, or just @squawk)
squawk:  Squawk! the deploy is blocked on the migration
you:     (empty message)
squawk:  Squawk stack is empty.
```

## Why this bundle exists

There are two Squawks and the difference is worth understanding before you pick
one.

The original lives in a separate, private agent repository and is a **model-free
ACI runner**: a Python program that implements Curie's agent protocol directly, with
no model anywhere in it, shipped as its own container image. Its answers are
exact by construction because a program produces them.

That shape cannot be authored in the desktop app's Build tab, and the reason is
structural rather than a missing feature. The runner image is a **cluster-wide**
setting — `agentSandbox.runner.image` in the chart, one `CURIE_RUNNER_IMAGE` in
the worker — so pointing it at a custom runner replaces the runner for *every*
agent on that cluster. There is no per-agent runner image in the API, the schema
or the chart. Deploying the original Squawk means the whole cluster is Squawk.

This bundle is the same behaviour in the shape the platform is actually built
for: an ordinary bundle on the standard runner. It needs no image, no Dockerfile
and no Helm override, and it deploys next to every other agent without changing
any of them.

## How it works with no code

The standard runner auto-mounts a `curie-state` MCP server (`runner/src/
curie_runner/state.py`), which is the same durable store the original Squawk
talks to over `CURIE_STATE_URL`. So the stack does not need a datastore, a
service, or an in-bundle MCP server — it is `namespace: squawk`, `key: stack`,
and three tools:

- `append` is atomic server-side, so a push cannot lose a concurrent push.
- `get` returns a `version`, and `set` takes `expected_version`, so a pop is a
  compare-and-set loop and two simultaneous pops cannot return the same entry.

That is the original's concurrency story, expressed as tool calls instead of
Python.

## What you give up

A model is in the loop, so this is not deterministic the way a program is. The
*stack* is exact — the state store does that work — but the reply is generated,
and a model can be chatty, summarise instead of popping, or add a sentence after
the acknowledgement.

`SKILL.md` pins the output surface to three lines and `evals/cases.json` grades
it, including one LLM-graded case for the failure a regex cannot see: `Squawk!`
alone is a well-formed push acknowledgement and a meaningless pop, and the two
are indistinguishable by pattern.

If you need answers that are exact by construction, you want the runner-image
Squawk and a cluster of its own. If you want a durable stack that lives beside
your other agents, this is it.

## Try it

Open this directory in the desktop app's Build tab, or from a terminal:

```bash
curie skill up --plugin-dir examples/squawk
curie skill message --plugin-dir examples/squawk "the deploy is blocked"
curie skill message --plugin-dir examples/squawk ""
curie skill eval --plugin-dir examples/squawk
```

The skill tier has no platform behind it, so the stack is per-session there.
Deploy it to the local or cluster tier for the durable, agent-global stack the
bundle is actually about.
