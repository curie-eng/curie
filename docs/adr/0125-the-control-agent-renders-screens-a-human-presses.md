# 125. The control agent renders screens; a human presses the buttons

Date: 2026-08-22

Status: Draft

## Context

Every agent Curie runs today is scoped to itself. Its sandbox token names its
own agent and nothing else ([ADR-0033](0033-scoped-sandbox-state-token.md)), its
state namespace is its own, and the platform key is deliberately kept out of the
box. [ADR-0107](0107-an-agent-reads-its-own-runs-through-a-first-party-scoped-surface.md)
(Draft) extends that self-scoping to observability: an agent may read *its own*
runs. Nothing may read the fleet.

The ask is an agent that operates the platform from a chat thread: "what is
running", "why did the support bot fail that thread", "roll it back". Two
distinct capabilities sit behind that, and they are not the same risk.

**Reading across agents** is a widening of scope. Useful, and cheap to reason
about: the worst outcome is a wrong or over-shared answer in a channel.

**Changing the fleet** is different in kind, because the component asking for
the change reads untrusted input for a living. A control agent lives in a
channel where people paste tickets, forward alerts, and relay other agents'
output. Give it a `kill_agent` tool and the tool is reachable by anything that
can get text in front of it. That is not a hypothetical about this design; it is
the ordinary operating condition of a chat agent.

The obvious mitigation is a permission gate: declare the mutating tools in
`approval_required_tools` and let [ADR-0010](0010-approval-gates-and-human-in-the-loop.md)
pause each call for a human. That machinery works and is already load-bearing
elsewhere — `agents/revenue-leak-agentos` gates its triage writes exactly that
way. It is not sufficient here, for a reason specific to *what* is being
approved.

An approval card shows the tool call the model composed. For a triage write
("resolve leak 1042") the human knows the domain and can sanity-check the
argument. For a fleet mutation the human is checking `agent_id`
`f47ac10b-…` against their memory of which UUID is production. In practice they
will read the surrounding sentence the model wrote. So the model authors both
the action and the text the human uses to evaluate it, and an injection does not
need to defeat the gate — it needs to make the sentence agreeable while the
arguments say something else. The gate holds; the human's judgement is what gets
attacked.

A second problem: approval fatigue is real, and the gate produces one card per
tool call whether or not the call was a good idea. A design that makes every
fleet question potentially card-generating trains people to click.

## Decision

**The control agent's authority ends at a proposal. Execution requires the
platform key, which no sandbox holds.**

Concretely, four parts.

**1. A `control`-scoped sandbox token, minted for exactly one agent.** The
worker mints it only when the resolved agent name equals the operator's
`CURIE_CONTROL_AGENT` (`apps/worker/src/curie_worker/binding.py`). Not the name
the bundle claims in `plugin.json` — the name the platform assigned at deploy,
so a bundle cannot elect itself. Every other sandbox is issued no such token,
which is the primary containment: not a permission another agent fails, a
credential it never receives.

The token reuses the ADR-0033 mint with a new scope string. Scope is inside the
signed payload, so the `state` and `state.app` tokens every sandbox already
holds cannot be replayed against this plane, and the control token cannot be
turned back on the state store.

**2. The API verifies independently, against its own configuration.**
`require_control_access` (`apps/api/src/curie_api/routers/fleet.py`) resolves
*its* `CONTROL_AGENT` setting to an agent id and verifies the token names that
agent with scope `control`. Two operator settings, on two services, must agree.
A worker misconfigured to mint a token opens nothing. Both default to empty, and
empty means the plane authenticates nobody but the platform key — so the feature
ships dark and deploying the bundle grants nothing.

**3. The execute route accepts the platform key only.** The control agent
authenticates on the fleet router, reads it, and creates proposals through it,
and is refused on `POST /fleet/proposals/{id}/execute` by caller kind, server
side. There is no setting that grants it. This is the line the design rests on,
and it is deliberately not expressed as an absent tool: the bundle ships no
execute tool, but a bundle is data the model runs inside, so that is ergonomics.
The guarantee is the server refusing.

**4. The API renders the consequence line; the proposer never does.** A
proposal stores a `summary` computed by `apps/api/src/curie_api/proposals.py`
from what the API looked up itself — the agent's real name, the version actually
deployed, the budget actually set. A caller-supplied summary is ignored, not
honored. The action vocabulary is closed: an unknown action is refused at create
time rather than stored for the executor to interpret.

This is the same split [ADR-0087](0087-the-api-renders-connector-objects-the-cli-applies-them.md)
made for connector objects — the API computes and returns, something else
applies — moved one layer up, and for the same reason: the component most
exposed to hostile input should not also be the component that acts.

There are two vocabularies, and they are disjoint.

`ACTIONS` is what the **control agent may propose**: `kill`, `resume`,
`rollback`, `set_budget`, `set_model`, `set_thinking`, `reset_thread`,
`run_eval`. Every one is recoverable.

`OPERATOR_ONLY_ACTIONS` is what **only a human's button may invoke**, and today
holds `delete_agent` alone. `propose` looks in `ACTIONS` only, so a request to
delete an agent is refused as an unknown action whatever the message said. The
bar for membership is that the action cannot be walked back: a proposal is a
good gate for "stop the agent" and not a sufficient one for "destroy it and its
history", where the mistake has no undo and the pressure to click is highest
exactly when someone is annoyed at a misbehaving bot. Delete additionally sits
on its own screen behind a **typed confirmation** — the operator types the
agent's name, compared server-side against the name just loaded, so a button
rendered before a rename cannot delete the renamed agent.

Adding to either table is a code change under review.

### Screens, and why a button is the right primitive

A chat agent that operates a platform through conversation alone has a specific
failure: the model is the one resolving "roll it back" into an agent id and a
version id, and the human is checking that resolution by reading a sentence the
model wrote. Proposals fix half of that (the summary is the platform's), and
leave the resolution itself in the model's hands.

**A screen is a page of blocks and buttons, addressed by id, rendered from the
store.** The model chooses which screen to open. It does not compose the
buttons, their labels, or their parameters, and it cannot press one.

Four consequences, each load-bearing:

- **A press is a human act.** So it runs immediately rather than becoming a
  proposal. The model is not in the causal chain at all, which is a stronger
  statement than "the model proposed and a human approved".
- **A button carries resolved ids.** No natural-language step sits between what
  the human sees and what runs.
- **The caller names a screen and a button id, never an action.** The API
  re-renders that screen and reads the action off its own button
  (`routers/fleet.py::invoke_button`). There is no field in which to send
  `{"action": "delete_agent"}`, and a button the current screen does not render
  does not exist. This also makes stale taps safe: two operators racing on one
  posted message do not both act, because the second press re-renders, does not
  find its button, and is refused.
- **Screens are channel-neutral.** `screens.py` carries no Block Kit and no
  Discord component JSON; the adapter renders semantic blocks
  ([ADR-0020](0020-message-port-rendering-free-channel-interface.md)). Discord
  therefore needs no new authorization decision — the operator set is opaque
  user ids — only a channel adapter, which is ADR-0020's job and not this one's.

**Who may press.** `require_operator` demands two things, both server side: the
platform key (so the control agent is refused here exactly as it is on execute),
and an actor in `CONTROL_OPERATORS`. The second is what makes the first mean
anything — a platform-key caller is the dispatcher relaying a click, and without
the operator check it would relay anyone's, so "the model cannot press buttons"
would only mean "the model must ask a stranger in the channel to press it". The
set is `approvers.ExplicitUsers`: no upstream lookup, so a button still resolves
while Slack is down, and one list serves every channel. Empty is the default and
admits nobody; screens still render, because reads are not presses.

**A press still writes an audit row.** An executed `ControlProposal` with the
rendered summary and the operator's id. A change made from a phone and one made
from the CLI land in the same trail, and neither can be reconstructed only from
a chat log somebody can delete.

### Every CLI feature is answered, and the rest say why

The goal is that an operator never has to leave the channel for something
`curie` can do. That is worth nothing as a prose claim, because the CLI grows
and prose does not, so it is a checked property:
`screens.SCREEN_FOR_COMMAND` maps **every leaf command in
`cli/command-manifest.json`** — the clap-derived surface the CLI itself emits —
to either a screen id or a written exemption, and
`apps/api/tests/test_screen_coverage.py` fails in both directions: a new command
with no entry, and an entry naming no command.

Today that is 26 commands with a screen and 59 exempt, of 85.

The exemptions are the honest half, and their categories describe the real
boundary between a CLI and a chat agent rather than a backlog:

| Category | Why a button would have to lie |
|---|---|
| `local-machine` | Operates on the operator's working directory, Docker daemon, or files. The agent has none of those. |
| `source-checkout` | Needs a repo clone and dev toolchains; the CLI refuses these from a released binary too. |
| `substrate-lifecycle` | Installs or destroys the platform the agent runs on. The turn dies with the cluster. |
| `credential-entry` | Takes a secret as input. A channel stores, indexes, and displays what you type into it. |
| `cli-introspection` | The CLI describing itself to a coding agent, which is what this agent already is. |
| `is-the-channel` | `message` exists to stand in for a channel when you have none. |

`GET /fleet/coverage` serves the same map, so the agent answers "can you do X?"
from the table the gate checks rather than from its system prompt.

### What this is not

It is not a replacement for ADR-0010. The approval gate remains the right tool
for an agent mutating its *own* domain, where the human can evaluate the
argument. This ADR is for the case where the argument is a platform identifier
and the human is really evaluating prose.

It does not make the control agent safe to point at anything. It makes the blast
radius of a compromised control agent equal to "it can put wrong proposals in
front of a human", which is a bounded, auditable, human-rate-limited failure.

## Consequences

- **Two settings gate the feature, and both must be set.** An operator who sets
  only the worker's gets a token nothing accepts; only the API's, a plane no
  agent can reach. Both empty is the default and is a working, closed system.
- **Provenance names a human on every mutation.** The executed row records who
  clicked, which is what [ADR-0046](0046-converged-approval-gates-and-durable-provenance.md)
  asks for and what a model-executed action could never honestly supply. It also
  distinguishes an agent-proposed action from an operator-created one forever.
- **Proposals expire (30 minutes).** A summary describes the fleet as it was
  when rendered. Executing a stale one applies a decision a human made about a
  system that has since moved, so expiry is a correctness property, not
  hygiene.
- **The closed vocabulary is a maintenance obligation.** The natural drift is
  additive — someone adds the action that would have been convenient once. A
  test asserts specific irreversible actions stay unproposable, which catches
  the common case and not the general one; the general one is review.
- **Adding two declared boot keys bumps the ACI patch version** (0.4.2 →
  0.4.3). Two new optional fields is the compatible class under 0.x per
  `packages/CLAUDE.md`, so old consumers keep decoding.
- **The audit trail outlives the agent.** `control_proposals.target_agent_id` is
  `SET NULL`, not `CASCADE`, and the row denormalizes the agent's name. CASCADE
  was the first implementation and it was wrong in exactly the case that matters
  most: deleting an agent erased every kill, rollback, and budget change ever
  made to it, **including the deletion itself**. The one action with no undo was
  the one leaving no trace. Found by a test, recorded here so it is not
  reintroduced as a tidy-up.
- **Screens are a wire contract.** A button rendered into a Slack message last
  week must still resolve after a redeploy, so screen ids and button ids are not
  renamed casually. A rename is a broken button in somebody's scrollback.
- **The coverage gate is a maintenance obligation with teeth.** Adding a CLI
  command now fails the API suite until someone decides whether it is a screen
  or an exemption. That is the intended cost: it is the only thing that keeps
  "everything is available in chat" true a year from now.
- **A second elevated agent would need a real design.** `control_agent` is one
  name, deliberately. A registry of privileged agents, or per-agent action
  allowlists, is a different decision and should be a different ADR rather than
  a widening of this setting.

## Alternatives considered

**Approval-gated mutating tools (ADR-0010 alone).** Rejected as insufficient,
not as wrong: see Context. The gate cannot protect the one thing under attack,
which is the human's reading of a model-authored sentence about opaque
identifiers. Retained as the right mechanism for domain-scoped writes.

**Give the control agent the platform key and rely on the gate.** Rejected
outright. ADR-0033 removed exactly this from sandboxes after it was found to let
an agent resolve its own approvals, and reintroducing it for the one agent whose
job is fleet mutation inverts that decision at its worst possible point.

**Let the model write the summary and have the human read the parameters.**
Rejected as a misread of what humans do. The parameters are UUIDs and version
ids; the sentence is what gets read. Designing against the actual behavior means
the sentence has to be trustworthy, which means the platform writes it.

**A separate control-plane service with its own credential.** Rejected as cost
without benefit at this size: a new deployable, a new credential to rotate, and
a new trust boundary to document, to obtain a separation the scope claim already
provides inside a router.

**Let the control agent execute low-risk actions directly (`resume` only).**
Tempting, and rejected because it makes the boundary conditional. "The model can
never execute" is a sentence an operator can hold; "the model can execute the
safe subset" invites the subset to grow and requires every future reader to
re-derive which actions are in it.

**Have the client send the action and parameters with the press.** The obvious
REST shape, and rejected: it hands the caller the ability to compose any action
with any arguments, and reduces the confirm text to decoration, since the
parameters that ran need not be the ones the sentence was rendered from.
Re-deriving the button server-side costs one screen render per press and makes
the confirmed sentence and the executed action the same fact.

**Resolve approvals from a control screen too.** Rejected. Approvals already
have a channel surface (ADR-0078) carrying the authorizer, the self-approval
block, and the audit trail. A second resolve button would be a second
implementation of the most safety-critical path in the system, and the two would
drift. The approvals screen reports what is pending and points at the card.

**Skip the coverage map and add screens as they are asked for.** Rejected as the
thing that makes the feature dishonest over time. Without a gate, "the agent can
do everything the CLI can" is true on the day it ships and quietly false a month
later, and nobody finds out until an operator needs the missing one.

**No proposal record — the agent just tells a human what to run.** Rejected:
that is the status quo with extra words, and it puts the command a human pastes
back under the model's authorship, which is the failure this ADR exists to
prevent.

## Realizing code path

- Token scope and mint: `apps/worker/src/curie_worker/binding.py`,
  `apps/worker/src/curie_worker/config.py` (`control_agent`)
- Boot keys: `packages/aci-protocol/src/aci_protocol/session.py`
  (`control_url`, `control_token`)
- Plane and the execute refusal: `apps/api/src/curie_api/routers/fleet.py`
- Vocabulary, validation, server-side rendering:
  `apps/api/src/curie_api/proposals.py`
- Persistence: `apps/api/src/curie_api/models.py` (`ControlProposal`),
  `apps/api/alembic/versions/0028_control_proposals.py`
- Screens: `apps/api/src/curie_api/screens.py` (vocabulary and the CLI map),
  `apps/api/src/curie_api/screenbuild.py` (built from the store)
- Operator gate and the press path: `apps/api/src/curie_api/routers/fleet.py`
  (`require_operator`, `invoke_button`), `CONTROL_OPERATORS` in `config.py`
- The agent: `examples/curie-control/`
- Boundary tests: `apps/api/tests/test_fleet.py`,
  `apps/api/tests/test_screens.py`,
  `apps/api/tests/test_screen_coverage.py`,
  `apps/worker/tests/binding/test_control_token_mint.py`
