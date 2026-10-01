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

## Eval case rationale

### push-acknowledges-and-says-nothing-else

A non-empty message is a push, and the whole reply is the acknowledgement. Anchored at both ends on purpose: the failure this catches is not a wrong word, it is a model that answers correctly and then adds 'I've added that to the stack. Anything else?'. That extra sentence is what makes a deterministic-looking bot read as a chatbot wearing its name.

### pop-answers-in-one-of-the-two-valid-shapes

An empty message pops, and there are exactly two right answers: the entry, or that there is none. The grader accepts both because the stack is agent-global and durable, so what is on it depends on what was said earlier -- a case that demanded an entry would go red on a freshly deployed agent, which is not a bug in the agent. What it REJECTS is the interesting half: a bare 'Squawk!' is a well-formed push acknowledgement and a meaningless pop, and this pattern requires either a payload after the bang or the no-entry sentence, so the ack alone fails. AN EARLIER VERSION GRADED THE TRAJECTORY (tool_called on mcp__curie-state__get) on the belief that no pattern could separate those two. That was wrong, and it was also ungradeable: the falsifiability gate proves each grader can go green against a known-good ANSWER, and a text exemplar carries no tool calls, so a trajectory grader could never be proved to work at all.

### whitespace-only-is-empty-and-pops

Trimming decides push versus pop, so whitespace must take the pop branch. Same two accepted shapes as the case above, for the same reason. What is being asserted here is narrower: that three spaces did NOT get pushed as an entry. The failure mode is a bundle that tests `if message:` instead of `if message.strip():` and quietly accumulates blank rows.
