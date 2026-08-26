# curie-control — the fleet control agent

A console for your agent fleet, in the channel you already work in. It opens
**screens** — pages of live values with buttons — and a human presses them.

![The control agent in a channel: screens, buttons, and the refusals](../../docs/demo/curie-control.gif)

The agent renders. It never presses, and it cannot execute anything at all. That
is the whole point, and it is enforced in three places that do not depend on
each other.

## Why this agent is different

Every other agent is sandboxed to itself: its own memory, its own transcript,
its own state namespace (ADR-0033). This one reads across the fleet. So the
question that shaped it is not "what can it do" but "what stops any other agent
from doing the same".

| Layer | What it stops | Where it lives |
|---|---|---|
| The worker mints a `control`-scoped token **only** for the operator-named agent | Any other sandbox ever holding the credential | `apps/worker/src/curie_worker/binding.py` |
| The API accepts that token only when it names **its** configured control agent, with scope `control` | A stolen or forged token, and a state token replayed as a control token | `apps/api/src/curie_api/routers/fleet.py` |
| The execute route accepts the **platform key only** | This agent, even holding a valid token, running its own proposals | same file, `execute_proposal` |

Two operator settings have to agree before any of it turns on —
`CURIE_CONTROL_AGENT` on the worker and `CONTROL_AGENT` on the API. Both default
to empty, and empty means the control plane authenticates nobody but the
platform key. A bundle cannot opt itself in: the worker compares against the
agent name the *platform* assigned at deploy, not anything `plugin.json` claims.

The tool surface in `.mcp.json` is **not** on that list. This bundle ships six
read tools and one propose tool and no execute tool, which is good ergonomics —
the model never sees an affordance it cannot use — but a bundle is data the
model runs inside, so it is not a boundary. The boundary is the server refusing.

## Screens and buttons

A screen is a titled page of blocks and buttons, fetched from
`GET /fleet/screens/{id}`. It carries no Slack or Discord shape: the adapter
renders semantic blocks, so Discord needs a channel adapter and **no new
authorization decision** (ADR-0020).

Pressing is `POST /fleet/screens/actions`, and it needs two things the server
checks itself: the platform key (so this agent is refused, exactly as it is on
execute), and an actor in `CONTROL_OPERATORS`. Empty operators — the default —
means nobody can press anything, while screens still render.

**The caller names a screen and a button id, never an action.** The API
re-renders that screen and reads the action off its own button, so:

- there is no field in which to send `{"action": "delete_agent"}`;
- the sentence the operator confirmed and the parameters that run are the same
  fact, derived once;
- a stale tap is refused rather than replayed — two operators racing on one
  posted message do not both act, because the second press re-renders and does
  not find its button.

Every press writes an executed proposal row, so a change made from a phone and
one made from the CLI land in the same audit trail.

## Every CLI feature is answered, or says why not

`GET /fleet/coverage` maps **every leaf command in `cli/command-manifest.json`**
to a screen or to a written exemption, and `test_screen_coverage.py` fails if a
new `curie` command appears under neither. Today: **26 with a screen, 59
exempt, of 85.**

The exemptions are the honest half:

| Category | Why a button would have to lie | n |
|---|---|---|
| `local-machine` | Your working directory, Docker daemon, files. The agent has none. | 25 |
| `source-checkout` | Needs a repo clone and dev toolchains. | 17 |
| `credential-entry` | A channel stores, indexes, and shows what you type into it. | 7 |
| `substrate-lifecycle` | Installs or destroys the cluster the agent runs inside. | 5 |
| `cli-introspection` | The CLI describing itself to a coding agent. | 3 |
| `is-the-channel` | `message` stands in for a channel when you have none. | 2 |

Ask the agent "what can you do?" and it answers from this table rather than from
its system prompt.

## Two vocabularies

`ACTIONS` — what the **agent may propose**, all recoverable: `kill`, `resume`,
`rollback`, `set_budget`, `set_model`, `set_thinking`, `reset_thread`,
`run_eval`.

`OPERATOR_ONLY_ACTIONS` — what **only a human's button** may invoke: today
`delete_agent` alone. The agent cannot ask for it at any tier; a request is
refused as an unknown action whatever the message says. It also sits behind a
typed confirmation — you type the agent's name, compared server-side.

The bar for the second table is that the action cannot be walked back. A
proposal is a good gate for "stop the agent" and not a sufficient one for
"destroy it and its history", where the pressure to click is highest exactly
when someone is annoyed at a misbehaving bot.

## The summary is written by the platform

A proposal's `summary` — the sentence the operator reads before executing — is
rendered by the API from what it looked up itself: the agent's real name, the
version actually deployed, the budget actually set. The proposer supplies an
action name, a target id, and parameters. It contributes no prose.

This matters more than it first looks. If the model wrote that sentence, prompt
injection would not need tool access to do damage — only a persuasive summary
attached to different parameters. Rendering server-side makes the displayed
consequence and the stored action the same fact.

The skill tells the agent to relay that line verbatim for the same reason.

## Seeing it work

The GIF above is this, and you can run it yourself. It creates a disposable
database, boots an API, seeds a fleet, plays the conversation against real
HTTP, and drops the database again:

```bash
bash examples/curie-control/demo/run.sh
```

Only the human's lines are scripted; no model runs. Every screen, refusal, and
audit id is a live response.

## Running the agent

```bash
curie skill --plugin-dir examples/curie-control
```

At the `skill` tier there is no platform behind it, so the tools report that no
control credential was issued and nothing is reachable — which is exactly what
this bundle looks like when it is not the control agent.

To run it for real, deploy it and name it on both sides:

```bash
curie cluster deploy --plugin-dir examples/curie-control
```

then set, on the API, `CONTROL_AGENT=curie-control` and `CONTROL_OPERATORS` to
the channel user ids allowed to press buttons; and on the worker,
`CURIE_CONTROL_AGENT=curie-control`.

## Executing a proposal

An operator, holding the platform key:

```bash
curl -X POST "$CURIE_API/fleet/proposals/$ID/execute" \
  -H "X-API-Key: $CURIE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"executed_by": "you@example.com"}'
```

`executed_by` is the audit record, not the authorization — the platform key is
what authorizes. Recording a human on every mutation is what ADR-0046 asks for,
and under this design there is always one.

Proposals expire after 30 minutes. A stale proposal describes a fleet that has
moved, and executing it applies a decision made about a different system.

See [ADR-0125](../../docs/adr/0125-the-control-agent-renders-screens-a-human-presses.md).
