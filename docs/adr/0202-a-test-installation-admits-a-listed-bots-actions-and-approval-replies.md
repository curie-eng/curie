# 202. A test installation admits a listed bot's actions and approval replies

Date: 2026-10-05

Status: Accepted

Accepted on 2026-10-06 with explicit maintainer approval from Junwon Jung
(jw3329), before any implementation. The issues under "Realizing code paths"
track the work.

This ADR supersedes in part
[ADR 0181](0181-every-mean-tester-probe-only-reads-or-asks.md): its words "on
every installation" stop applying to a test installation as defined below.
Everywhere else, read-or-ask stays the rule, and decision 2 now enforces it for
marked messages.
[ADR 0172](0172-the-mean-tester-is-one-bundle-on-off-the-shelf-mcp-servers.md)
decision 5's exception for an operator-listed test installation stays removed.
Back-links are on both, and on ADR 0106, under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

This ADR also amends
[ADR 0106](0106-an-approver-is-an-authenticated-principal.md) with one new
principal kind (decision 5). It revives the platform capability proposed in
#3122, which closed only because no test installation was planned to use it.

## Context

An agent that sends, files, changes or deletes things is not tested until those
actions run and someone checks the result. Today no automated tester can run
them:

- ADR 0181 limits every mean tester probe to reading and asking, on every
  installation. Its reason was that an operator's channel list "cannot prove
  isolation of downstream effects".
- **A bot cannot press an approval button.** Slack delivers a block action only
  for a user's interaction with the message. The dispatcher reads the clicking
  user and channel from that event (`resolve_approval_action` in
  `apps/dispatcher/src/curie_dispatcher/approval_actions.py`). It then mints a
  60-second chat principal (`mint_chat_principal` in
  `apps/dispatcher/src/curie_dispatcher/approval_principal.py`).
- **The operator principal does not fit a tester.**
  - It is a 12-hour HMAC token whose claims are only a subject, a kind and an
    expiry (`OPERATOR_TOKEN_TTL_SECONDS` in
    `apps/api/src/curie_api/approval_principal.py`).
  - It names no agent, route, approval or installation.
  - Minting it needs the target's platform key, and the mint is not audited.
  - Handing it to a tester in another installation breaks ADR 0169's rule that
    no installation hands another a credential.
- **The platform has no attested notion that an installation is for testing.**
  - `api.environment` mainly arms the production secrets gate
    (`_refuse_dev_defaults_in_prod` in `apps/api/src/curie_api/config.py`). It
    defaults to `dev`.
  - A deployment's `env: dev` reaches the agent only as
    `CURIE_DEPLOYMENT_ENVIRONMENT`, which the agent can only report about itself.
  - A dev deployment can run inside a production installation.
- **Bot-authored mentions are admitted unevenly.** On the mention lane, a
  bot-authored message in a thread is refused unless the bot is one of this
  installation's own identities
  ([ADR 0168](0168-one-installation-hosts-several-bot-identities.md)) or an exact
  `(channel, bot)` pair is in `dispatcher.threadedBotAllowlist`. That check is
  in `apps/dispatcher/src/curie_dispatcher/relevance.py`. The bot-authorship rule
  does not refuse a bot-authored root mention, though
  [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md)'s caller list still
  applies.

Now there is a consumer. The maintainers run a dev installation whose agents
hold credentials for test accounts only, and they want the mean tester to drive
gated and ungated actions there end to end.

No platform can prove that a downstream account is a test account. What it can
do is make one party state it explicitly, per installation and off by default.
It can then refuse test actions everywhere that statement is absent, before any
agent turn runs, and give a test driver an approval proof that only an explicit
approver list accepts.

## Decision

**An installation's operator may declare it a test installation and list the
bots allowed to drive it. Only there does the platform admit a listed bot's
marked actions and approval replies. Every other installation refuses them
before a turn starts.**

### 1. The declaration belongs to the installation and is off by default

The chart gains `testInstallation.enabled` (default `false`) and
`testInstallation.drivers`. Each driver entry is a channel id, a Slack bot id
(`B…`) and that bot's user id (`U…`). It is validated like a
`threadedBotAllowlist` pair.

A driver may run in another installation, or in this one as a sibling identity
([ADR 0168](0168-one-installation-hosts-several-bot-identities.md)). A sibling
driver's entry also names the one agent that identity serves, for example
`mean-tester`. Running the tester beside its target lets it use that
installation's model credential and repository workspace, instead of carrying
its own.

- **Turning it on is the operator's statement** that every credential this
  installation's agents hold reaches only test accounts. It is also a statement
  that a driver's Slack token is reachable by no agent other than the driver
  itself. For a sibling driver, that token is in its own agent's connector
  secrets and in no other agent's.
- **The chart refuses to render it** in three cases. The check is in
  `values.schema.json` and a template `fail`.
  - when `api.environment` is `prod`, as a tripwire, not as evidence;
  - when any published default secret is in use: the API key, the worker token
    or the approval attester secret. A test installation admits bot-driven
    actions, so it must not be the one installation allowed to boot with a
    shared secret;
  - when a driver has no channel.
- **The API and the dispatcher receive the setting explicitly** and check it at
  boot. Whenever it is on, each one also refuses to boot on a published default
  secret. The chart cannot see a secret supplied through `existingSecret`, and
  a local compose stack never renders the chart.
- **The dispatcher's preflight keeps a driver identity to its own agent.** The
  risk it guards is an agent under test posting as a driver and answering its
  own approval cards. The preflight therefore refuses:
  - a driver that is the installation's authorized identity (`auth.test`);
  - a sibling driver whose entry names no agent;
  - a sibling driver whose identity is bound to any agent other than the one
    its entry names.

  Every other agent replies through its own identity, so none of them can post
  as the driver.
- **The setting comes only from the chart.** It is never derived from a channel
  name, a deployment's `env`, a bundle, or anything an agent or a message says.
- **A test installation owns its Slack app exclusively.** Admission is per
  Slack app, and two releases holding one app (#2248) would make it
  nondeterministic.

### 2. Marked messages are admitted from listed drivers and refused elsewhere

A marked message is a bot-authored mention whose text, after the target's own
mention is stripped (`_strip_self_mention` in `handlers.py`), begins with
`[test action]`. Only the event's `bot_id` identifies the driver, never text.
The driver's user id comes from the configured entry for that `bot_id`, never
from the event's `user` field. A person who types the mark sends ordinary text
on every installation.

- **On a test installation, a marked message from a listed driver in its listed
  channel is admitted**, at root and inside threads, with no
  `threadedBotAllowlist` pair. A sibling driver's thread mentions are already
  admitted as an own identity's. Where an ADR 0175 caller list is configured, the
  driver must be on it as well. The chart sets a per-thread cap on driver-started
  turns. A driver is itself an agent that reads the target's replies, and the
  cap bounds a loop between them.
- **Anywhere else, a marked message is dropped before routing.** This happens
  after ADR 0175's caller check, and no turn starts. The new drop reason is
  `DropReason.TEST_ACTION_REFUSED`.
  - Where the caller list has already refused the bot, the drop stays silent,
    as ADR 0175 requires.
  - Otherwise the dispatcher posts one fixed reply in the thread:
    `This installation does not accept test actions.` A bot that passes the
    caller list could already learn that this bot exists by mentioning it, so
    the reply reveals nothing new.

### 3. The admission reply answers a driver's ping and nothing else

A listed driver sends a root mention, `<@target> [test action] ping`, in its
listed channel. A test installation's dispatcher answers in that ping's thread,
without a turn: `This installation accepts test actions from <@driver>.`
Decision 2 already covers every other installation that has this change: it
refuses the ping.

The ping itself starts no turn. A driver accepts admission only as the first
reply in its own ping's thread, posted by the target's bot user within a short
window the driver sets. An agent on that installation could post there only if
someone else first started a turn in that thread.

### 4. A listed driver may answer an approval in the card's thread

On a test installation, a listed driver replies in a card's thread with
`<@target> [test action] approve <approval-id>` or
`<@target> [test action] reject <approval-id>`.

- **The dispatcher checks that the reply is in that card's thread.** It reads
  the thread's messages and looks for the target's card whose buttons carry that
  approval id, as `_fetch_card_message` reads a card for a click. The card may
  be the thread's root or a reply inside it, as it is for unrouted cards and for
  routed cards shown where the request was asked. If the thread holds no such
  card, the reply resolves nothing.
- **The reply's channel must be one listed for that driver.** That includes a
  routed card's channel.
- **It mints a `test_driver` principal (decision 5) and resolves through the
  API.** It uses the same `ApprovalResolveClient` path a click uses. An approval
  reply never starts a turn.
- **A release that does not own the approval handles the miss as it handles a
  click** (`is_release_ownership_miss`, #2248), not with the refusal text.

### 5. A `test_driver` principal is accepted only by an explicit approver list

This amends ADR 0106 with a new principal kind, `test_driver`. The dispatcher
mints it with the approval attester secret, bound to one approval id and one
channel, for 60 seconds.

- **Only an explicit list accepts it.** `test_driver_eligible` is true only on
  `ExplicitUsers`. It is false on channel-membership and group sets, on email
  sets, and on unbound, invalid and unverifiable routes.
- **The API refuses a `test_driver` principal unless two conditions hold**,
  independently of the dispatcher:
  - its own `testInstallation.enabled` is true;
  - the subject is a configured driver's user id.

  Each refusal is audited.
- **The audit records `principal_kind = "test_driver"`**, so a reader can tell
  a driver's decision from a person's click.

### 6. What stays outside the platform

The platform does not:

- verify that downstream accounts are test accounts;
- classify an unmarked request as an action;
- snapshot or restore state.

A driver can read what a tool reports it changed
([ADR 0117](0117-a-tool-that-changes-the-world-reports-what-it-changed.md)).
Restore is decided in
[ADR 0121](0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md),
and snapshots in
[ADR 0124](0124-a-snapshot-is-sealed-to-the-connector-that-wrote-it.md).

## Consequences

- **A tester can drive the whole flow** on a test installation: ask for an
  action, answer its approval card, and check the result.
- **A sibling driver shares its target's installation.** It uses the same model
  credential, sandbox capacity and platform. It cannot report an outage that
  takes the platform down with it, and its turns compete with real users for
  sandboxes.
- **Production behavior does not change for people.** A marked bot message is
  dropped there before a turn, so a misdirected driver learns at its first ping
  that it may not act.
- **The operator who enables the setting owns the isolation claim.** A wrong
  claim lets a listed bot cause real effects through that installation's
  agents. The chart and preflight refusals catch only the mistakes the platform
  can see.
- **The `prod` environment check is a tripwire, not evidence.** A production
  installation that never set `api.environment` passes it. The secret check and
  the operator's statement are what bind.
- **An installation running an older platform version** starts an ordinary turn
  on a ping, and its agent could imitate the admission reply in the ping's
  thread. A driver's own operator-listed channels are the bound that remains
  there.
- **An unmarked action request from any bot behaves as it does today.** The
  mark is a convention the platform enforces where it is present, not a
  classifier.
- **A driver bundle is expected to:**
  - mark every action it sends;
  - ping before its first action;
  - act only after the admission reply in its own ping's thread;
  - check effects through its own read access to the test accounts.

  `examples/mean-tester` records those rules in its own documentation, not
  here (`AGENTS.md`, "Decisions: ADR vs. GitHub issue").

## Alternatives considered

- **Keep ADR 0181 everywhere.** Gated and ungated actions then stay untested by
  any automated means, and the test accounts that would make them safe go
  unused.
- **Give the tester an operator principal for the target.** Rejected:
  - it is unscoped and lasts 12 hours;
  - its mint is unaudited and needs the platform key;
  - it puts one installation's credential in another installation's sandbox.
- **Resolve with the existing chat principal.** Rejected. Channel-membership
  and group sets accept a chat principal, so on a default route a driver that
  is a channel member would resolve, and its decision would look like a click
  in the audit.
- **Use the tester's own channel list as the permission, as ADR 0172 decision 5
  did.** Rejected, as ADR 0181 rejected it: the receiving side does not enforce
  a list on the sending side. It remains only as the driver's second bound.
- **Use a deployment's `env: dev` as the mark.** Rejected: it is per deployment,
  it is reported by the agent under test, and a dev deployment can run in a
  production installation.
- **Resolve through the API with an adapter principal.** Rejected: adapters are
  never eligible on Slack approver sets, and widening that is a larger change.
- **Trust a target that says it is a test installation.** Rejected, as in ADR
  0181: the claim would come from the component under test. The admission reply
  comes from the dispatcher, without a turn.
- **Forbid every own identity as a driver.** An earlier revision did, to keep the
  agent under test from answering its own cards. That also forbade a tester
  placed beside its target, which is the cheapest way to give it a model
  credential and repository access. What has to be prevented is posting as the
  driver, and binding the driver identity to one agent prevents it.
- **Gate only in the dispatcher.** Rejected: a dispatcher and an API whose
  settings drift apart would let a driver resolve on a non-test installation.
  The API checks independently (decision 5).

## Realizing code paths

Nothing implements this yet. One issue tracks each path, in order: #4133, #4134, #4135 and #4136.

1. **Declaration:**
   - `charts/curie/values.yaml`;
   - `charts/curie/values.schema.json`;
   - a template `fail` for the render refusals;
   - `apps/api/src/curie_api/config.py`;
   - `apps/dispatcher/src/curie_dispatcher/config.py`;
   - the boot-time default-secret refusal in both;
   - the dispatcher's preflight identity check, including the agent binding of
     a sibling driver.
2. **Admission and refusal:**
   - `apps/dispatcher/src/curie_dispatcher/relevance.py` (`TEST_ACTION_REFUSED`
     and the driver admission);
   - `handlers.py` (ordering after the caller check, the ping reply and the
     per-thread cap).
3. **Approval replies:**
   - the dispatcher's thread-root check;
   - `test_driver` minting in `apps/dispatcher/src/curie_dispatcher/approval_principal.py`;
   - its verification in `apps/api/src/curie_api/approval_principal.py`;
   - eligibility in `authorizer.py`, `approvers.py` and `slack_approvers.py`;
   - the API's independent driver check and audit;
   - a migration for the audit table's `principal_kind` constraint.
4. **`examples/mean-tester`:** marked action probes, the ping check, and card
   answers only after admission.
