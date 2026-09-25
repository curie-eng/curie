# Approvals: human-in-the-loop turns

A Curie turn does not have to end in an answer. It can end **paused**, waiting for a
person to approve or reject something, and resume later with their decision. This page
is the one prose home for that plane: how a request is raised, where the card goes, who
is allowed to resolve it, and what your skill has to do when the decision comes back.

The narrative "why" behind the shape lives in the ADRs, chiefly
[ADR-0010](adr/0010-approval-gates-and-human-in-the-loop.md) (the approval primitive)
and [ADR-0034](adr/0034-approval-authorizers-resolve-membership-in-the-api.md) (one
authorizer over swappable approver sets), extended by
[ADR-0106](adr/0106-an-approver-is-an-authenticated-principal.md) (authenticated
resolver identity and authorized self-confirmation). This page is the "how do I use it".

## The one-paragraph version

A run raises an approval. The worker persists a durable record, suspends the sandbox,
and posts one interactive card to the request route's **resolution** target. The route
may also name a **notification** target, which receives a text-only ping directing humans
to the configured approval channel without disclosing its identifier. An authenticated
principal resolves on the card, in the Console, or from the CLI; the platform derives the
person from that credential and authorizes them
**server-side**, wakes the suspended session with a platform-authored turn carrying the
decision, and the agent finishes the job it started. Nothing about the decision is
trusted from inside the sandbox, and nothing holds a worker slot while a human thinks.

## Two ways a turn pauses

Both end the turn with the same status and share the entire downstream lifecycle
(`runner/src/curie_runner/approval.py`).

**Policy gate.** When the session exposes the built-in `request_approval` tool (the SDK
name is `mcp__curie__request_approval`), the agent may use it for a step that genuinely
needs sign-off. It takes a one-line `summary` and an optional `route`. The call executes
nothing; it marks the turn. The skill should say the request is pending and end the turn.
The runner advertises this generic pager when a route is `grantableViaPolicy`, or when
the observed MCP surface has an action that may write (any tool not explicitly
`readOnlyHint=true`, including an unknown or unreachable surface) and no permission
gate already pages. An explicit `approvalPolicy` or `toolPolicy.approvalRequired` gate
is already a pager; keeping `request_approval` beside it raises a second card for the
same action (#2657). A bundle with no MCP tools, or only explicitly read-only MCP tools
and no grantable policy route, does not carry it: explain that the bundle cannot
perform the action instead of fabricating a remediation request. `readOnlyHint` is not authorization and does not change gates or
tool execution. When a live probe explicitly reports it as `true`, Curie also adds that
MCP tool's SDK-visible name to the read-only classifier: it emits no side-effect flag,
does not take the no-retry-after-side-effects path, and creates no receipt line. A
missing or `false` hint, an unknown surface, or a failed probe remains potentially
write-capable and is classified conservatively. Publication's dedicated approval flow
and the state tools remain independent.

**Permission gate.** Configuration marks a tool approval-required, and the runner denies
the call through its unscoped `PreToolUse` hook, with the SDK's `can_use_tool` callback as
the second line, before it runs. The denied call never executes. Two sources name gated
tools and they are unioned, never subtracted: the bundle manifest's `approvalPolicy`, and
the `CURIE_APPROVAL_REQUIRED_TOOLS` operator override. For the platform's own
`publish_changes` tool the runner additionally records every call it sees on the model
stream (#2294): if neither SDK layer recorded the call, the in-sandbox tool body runs and
returns its defensive error (it carries no publication authority), the stream record still
parks the turn awaiting approval, and a publication call that left no record ends the turn
as a `publication-unrecorded` classified failure rather than a clean `done`.

## Declaring routes in the bundle

A **route** is a name for a class of decision, declared in the bundle so it is versioned
with the agent. Today a route is declared as part of a gate
(`packages/plugin-format/src/plugin_format/models.py`):

```json
{
  "name": "deal-desk",
  "version": "0.2.0",
  "approvalPolicy": {
    "gates": [
      { "gate": "mcp__plugin_deal-desk_core__post_invoice", "route": "finance" }
    ]
  }
}
```

`gate` names the tool the runner intercepts; `route` names the approval route the
platform binds to a channel per deployment. Declaring a route this way means the runner
validates the model's `route` argument against it in-turn: an unknown route comes back
as a tool error naming the valid ones, so the model can retry inside the same turn
instead of burning it.

Policy gates do not grant a tool call after approval by default. Set
`"grantableViaPolicy": true` on a gate only when an approval should authorize one call to
that gate's tool on the resume turn. Grantable policy is unambiguous per route: every
grantable gate claiming one route must name the same tool. Repeating that same tool is
valid; assigning two distinct grantable tools to one route is rejected as
`approval_policy.grant_route_ambiguous`. This keeps the route-to-granted-tool mapping
single-valued rather than letting a model-selected request decide which tool receives the
one-shot allowance.

View what a bundle declares, offline and with no credential:

```bash
curie skill approvals
```

## Binding a route to a workspace

Declaring a route says nothing about where it goes. The binding is **operator-owned**,
per agent, and read fresh at resolve time so revoking an approver takes effect on the
next click rather than the next restart:

> **One exception, and it is the one worth knowing.** "Read fresh" holds for the
> binding itself and for `approvers.users`. Slack **user-group** membership sits
> behind a per-group TTL cache in the API (`slack_usergroups.py`, 60s by default
> via `slack_usergroup_cache_ttl_s`), because `usergroups.users.list` is a Slack
> Tier 2 method and a fetch per click would rate-limit a busy channel. So
> removing someone from a group can take up to that TTL to bite, where removing
> them from `approvers.users` bites immediately. Setting the TTL to 0 is the
> operator lever for a fetch per resolve.

```json
{
  "approval_routes": {
    "finance": {
      "resolution": {
        "kind": "slack",
        "address": "C0EXAMPLE3"
      },
      "notification": {
        "kind": "email",
        "address": "finance-approvals@example.com",
        "endpoint": "https://adapter.example.com/replies",
        "adapter": "mail"
      },
      "approvers": { "group": "S0123ABCD" }
    }
  }
}
```

Three independent axes, and keeping them separate is the point:

- **`resolution` is WHERE the one interactive card posts.** It is required and currently
  accepts only `{ "kind": "slack", "address": "C..." }`, because Slack's authenticated
  interaction path supplies the verified resolver identity.
- **`notification` is WHERE else humans are told.** It is optional. Its text includes the
  approval ID and directs humans to the configured approval channel without naming its
  kind or address. It has no interaction, buttons, action values, or other resolving
  affordance.
- **`approvers` is WHO may act on the resolution card.** Omit it and that Slack channel's
  members are the approvers, which is the zero-setup default. Notification recipients never
  become approvers merely by receiving the ping.

A Slack notification can use the worker's default transport. Any other notification kind
must store both `endpoint` and `adapter`; those transport details are write-only and are
redacted from API and `--list-routes` output. The target's `kind` and `address` remain visible.
Resolution deliberately has a channel-neutral shape but remains Slack-only. Making another
channel interactive requires a future adapter-scoped credential that establishes a verified
resolver identity; this split does not add one or accept resolution through a notification.

A route the bundle names but the agent never bound is **escalated to a human**, not
posted to the requesting channel. Silently widening a request to whoever happens to be
in the requesting channel is exactly the failure the gate exists to prevent, so the
platform refuses rather than guesses.

Since #2436, that same gap is also closed a step earlier, at configuration time. A route
the bundle declares with no entry in the agent's `approval_routes` now refuses the write
that would make it live: `POST /deployments` returns 422, a git push deploy or promote is
rejected with code `approval_routes.unbound`, and a bundle uploaded onto a version an
active deployment already references is refused too. The refusal names the unbound
route(s) alongside the ones that are bound. A bound route no bundle declares is still
accepted, so pre-binding ahead of a bundle bump remains supported. The check tests only
whether a binding is present, not whether it is valid; the worker's request-time
escalation above stays the backstop for a binding removed or corrupted after this check
ran. An `approval_routes` write, including `--clear-routes` or an empty routes file, that
would drop a route declared by any active deployment of the agent, in either environment,
is refused on the same terms.

The `curie` verbs check the same join locally first, so the gap is reported before
anything is uploaded. `curie local deploy` and `curie cluster deploy` (including
`--all-targets`, `deploy-local`, and the SRE bot installer) read the routes the bundle
declares. After the agent is found or created, they refuse with exit 2 before a version
or bundle is sent, naming the unbound routes, the bound ones, and the
`curie <tier> approvals <agent> --route-resolution ...` command to run. A first deploy of
a gated bundle therefore creates the agent and stops there: bind the routes, then deploy
again. A route write (`--clear-routes`, `--route-resolution`, `--route-approvers`,
`--routes-from`) likewise refuses before the PATCH when an active deployment declares a
route it would remove. Both checks are advisory: when the CLI cannot read a manifest or a
deployment, it warns and sends the request, and the API decides.

Bind one from the CLI rather than by hand. A write REPLACES the whole map, so name
every route it should keep:

```bash
curie local approvals my-agent --routes-from approval-routes.json

curie local approvals my-agent --list-routes
```

The strict `--routes-from` JSON uses the complete binding shape shown above and is the only
way to declare a notification, including the `endpoint` and `adapter` required for a
non-Slack target. `--route-resolution` and `--route-approvers` can override resolution and
authority from that file; the latter takes `users:U0123ABCD,W0456DEFG` or `group:S0123ABCD`.
`--clear-routes` removes every binding, but only when no active deployment for the agent
declares one of the routes being removed; otherwise the write is refused, naming those
routes. A `--routes-from` file containing `{}` clears every binding too, the same as
`--clear-routes`, and is refused on the same terms. To clear a gated agent's routes, first
end every still-active deployment whose bundle declares one of them (`DELETE
/deployments/{id}`), then clear. Deploying a version that declares no gates is not enough:
deployments append rather than stop earlier ones, so an older active deployment still
declares the route and the preflight still refuses. The retired `--route` flag and
`channel` JSON key are not accepted. Nothing is written unless every entry parses, so a
typo cannot leave a half-written map behind.

## Who may resolve

One authorizer decides, and the swappable part behind it is the approver set
(`apps/api/src/curie_api/authorizer.py`, `apps/api/src/curie_api/approvers.py`). The
precedence is fixed: `users` beats `group` beats channel membership.

| Declared | Set | Lookup | Eligible principal |
|---|---|---|---|
| nothing, and the route is bound (or the approval has no route at all) | `SlackChannelMembers` | none, the attested card channel is the proof | authenticated `chat` only |
| nothing, and the approval names a route that is **not** bound | `UnboundRoute` — refuses everyone | none, there is no set to resolve | none |
| `approvers.group: S...` | `SlackUserGroupMembers` | Slack `usergroups.users.list` | authenticated `chat` or `console` |
| `approvers.users: [U...]` | `ExplicitUsers` | none, pure config | authenticated `chat`, `console`, or `operator` |

Because `users` and `group` ignore the click channel entirely, a card can sit in a room
everyone can read while only a narrow set may act. That unfusing of *where* from *who*
is the whole subject of ADR-0034.

Clicking Approve or Reject opens a short dialog for an **optional** note before the
decision lands. Leave it blank and the decision is recorded with no note; type a reason
and it is stored on the record, stamped onto the card in the approver channel, and
carried to the requester in the resume turn below. Cancelling the dialog resolves
nothing, so a misclick is recoverable.

The dialog is not optional the way the note is: **every** approval card opens one, in
every deployment, with no toggle. That costs an approver who wants no note one extra
click, and it is deliberate. A reason is the half of a rejection the requester actually
needs, and the dialog is what makes leaving one the default rather than something you
remember to do from the CLI. If a deployment turns out to want the opt-out, it would be
operator-written config, never a bundle-declared field: an agent must not get to widen
how its own approvals are collected.

An approver is always an authenticated principal, never a name or channel supplied in the
resolve body. The API derives `resolved_by` and any channel evidence from exactly one of
three credential paths:

- **`chat`** — the dispatcher attests the Slack user, card channel, and approval ID from
  the authenticated Socket Mode interaction. The short-lived attestation is usable only
  for that approval.
- **`console`** — a single-use, subject-bound login code creates an HttpOnly same-origin
  session. The Console shows that immutable subject and never asks who is resolving.
- **`operator`** — an administrator mints a reusable, subject-bound terminal token. It
  carries no channel evidence, so it can resolve only a route whose explicit `users` list
  contains its subject. The stateless token is valid for twelve hours and has no
  per-token revocation; rotate the platform API key to invalidate it before expiry.

The shared platform API key administers principal issuance but is not a person. By itself
it cannot resolve an approval. `POST /approvals/{id}/resolve` accepts policy input only:
`decision` and optional `note`; `resolved_by` and `actor_channel` are rejected.

Four properties worth knowing before you design around this:

- **Requester equality neither grants nor denies.** The authorizer always consults the
  selected set, including when the authenticated principal also requested the turn. A
  requester who belongs may approve; one who does not remains denied. Two-person
  separation of duties requires a distinct future policy and is not implicit in an
  ordinary approval gate.
- **The buttons are visible to everyone in the channel.** Slack cannot hide a button per
  user, so authorization is enforced when the click arrives, not by hiding the control.
  A refused click gets a private, reasoned refusal.
- **Terminal principals are explicit-user only.** A Console session also carries no
  channel, but its authenticated subject may be checked by the API's Slack user-group
  lookup. Neither principal can satisfy the channel-membership set.
- **A lookup that fails denies.** A group route with no bot token, or a Slack
  outage, reports "could not verify approver group membership" and refuses. It never falls back to channel
  membership, because that would silently widen the set an operator narrowed.

Resolving a group-bound route needs `SLACK_BOT_TOKEN` on the API with the
`usergroups:read` scope. Both requirements are stated in
[ADR-0034](adr/0034-approval-authorizers-resolve-membership-in-the-api.md) and
`.env.example`; `apps/api/src/curie_api/slack_usergroups.py` is the lookup that
consumes them and names neither, so it is the wrong place to send a reader
checking the claim. The scope needs the Slack app reinstalled. Without the
token, such a route fails closed: every resolution reports `could not verify approver
group membership`.

## What your skill must handle on resume

This is the part nothing in the command tree tells you. When an approval is resolved,
the platform enqueues a normal turn on the runs stream whose text it authored itself
(`apps/api/src/curie_api/resumequeue.py`):

```text
[approval resolved] The request "<summary>" was approved by <U123>. Note: <text>.
Continue the task accordingly: proceed with the approved action, or acknowledge the
rejection and stop.
```

An expiry produces the sibling `[approval expired]` turn, which states that nobody
decided and the gated action must not be performed.

**Your `SKILL.md` must have an instruction for those prefixes.** A skill that does not
recognize them will treat the resume as an unrelated user message and the verdict is
silently dropped. Give the skill a section that says what to do on each, including for
a rejection.

The reply streams back into the **same message** the "awaiting approval" notice was left
on, because the resume turn replays the original turn's reply handle.

**A rejection is final until a person asks again.** If the resumed turn requests the same
approval again (same agent, thread, route and gated tool, however the summary is worded),
the platform refuses it: no card goes out, the refusal is recorded on the rejected
approval's audit trail as `reraise_refused`, and the thread gets one message naming the
rejected approval, who rejected it and when. A person asking in the thread starts a new
turn, and that turn may raise it again.

## Driving it from the CLI

Approval records live in the platform, so these verbs answer at the `local` and
`cluster` tiers. At `skill` the tier declines with a reason rather than erroring, since
a bare runner keeps no durable store
([ADR-0077](adr/0077-skill-tier-durable-approvals-stay-unavailable.md)).

```bash
# what is pending: each record's id, summary, and the route it named
curie local approvals my-agent --list

# mint a reusable token for a subject named in the route's explicit users list
curie local approvals my-agent --mint-operator-principal U0EXAMPLE1
export CURIE_APPROVAL_PRINCIPAL_TOKEN=<one-time-output>

# settle one as the authenticated token subject
curie local approvals my-agent --resolve <id> --note "approved for Q3"

# reject instead
curie local approvals my-agent --resolve <id> --reject --note "discount too deep"

# create the Console's single-use, subject-bound login code instead
curie local approvals my-agent --mint-console-login-code U0EXAMPLE1

# which tools are gated behind approval
curie local approvals my-agent
```

Resolution is **once-only**: the first authorized resolver wins the compare-and-set, and
a later one is told who won rather than overwriting the decision.

The CLI never asks for an actor or channel. `CURIE_APPROVAL_PRINCIPAL_TOKEN` proves the
operator subject and carries no channel, so terminal resolution works only for an explicit
`approvers.users` route. Use the authenticated Slack card for channel-membership or group
routes. When a turn parks, its resolve hint names the approval's real `card_channel` — the
route-bound resolution address when one was configured, not the requesting-channel stub —
so a human knows where that authenticated card lives. A null or empty `card_channel` is
from an older row or a direct API write that omitted the field; for that compatibility
case, the requesting channel is the card location.

## Operational guarantees

- **A paused turn holds nothing.** The queue entry is acknowledged when the turn
  suspends; the resolution arrives later as its own queued turn. No stream entry, thread
  lock, or worker slot is held across a human's lunch break.
- **A suspended sandbox is deleted, not frozen**
  ([ADR-0003](adr/0003-stateless-first-rehydrate-on-resume.md)). Resume cold-creates a
  fresh one and rehydrates from history. Never design a feature that needs in-process
  state to survive a pause.
- **Unresolved approvals expire.** A sweeper settles records nobody answered and wakes
  the session down its timeout branch (`apps/api/src/curie_api/sweeper.py`), and a
  reconciler re-drives resolutions whose resume turn never landed
  (`apps/api/src/curie_api/resumereconciler.py`).
- **Every attempt is audited.** `GET /approvals/{id}/audit` returns each resolution
  attempt with the authorizer, membership evidence, `principal_kind`, and
  `authenticated` proof state, including refusals. Historical rows remain
  `principal_kind: null` and `authenticated: false`; they are not retroactively trusted.
- **Roll the API before the dispatcher.** The identity contract intentionally has no
  assertion-compatible dual mode. During an upgrade, bring up the verifier first, then
  the attester; the reverse order is rejected rather than reopening asserted identity.
- **Console resolution requires HTTPS.** Its session cookie is `Secure`, `HttpOnly`, and
  same-site. A browser on a plain-HTTP endpoint will not retain the credential.

## Common failures

| Symptom | Cause and fix |
|---|---|
| `401 missing or invalid approval principal` | A platform API key alone cannot resolve. For an explicit-user route, mint an operator principal, export it as `CURIE_APPROVAL_PRINCIPAL_TOKEN`, and retry; otherwise use the authenticated Slack card or a live Console session. |
| `403 operator approval principals can resolve only routes bound to an explicit user list` | Terminal credentials carry no Slack membership evidence. Add the subject to an explicit `approvers.users` binding, or resolve through the authenticated card. |
| `403 console approval principals can resolve only routes bound to an explicit user list or verifiable user group` | The Console session carries no channel evidence. Use an explicit `approvers.users` binding, a group the API can verify, or the authenticated card. |
| Console login succeeds but session inspection or resolve returns `401` | Serve the Console over HTTPS. Its `Secure` session cookie is deliberately not retained on plain HTTP. |
| `403 you are not an approver` | The selected set does not admit the authenticated principal. Check the `approvers` block and current membership. Requester equality does not bypass or veto that check. |
| `403 could not verify approvers` | The declared `approvers` block is malformed and cannot be evaluated. Correct its `users` or `group` value, then replace the complete route map. |
| `403 could not verify approvers: ... route is no longer bound` | The approval named a route whose binding was cleared or rewritten while it was pending — `--clear-routes`, a `--routes-from` file that omits the route, or any `--route` write, since a write is a full replacement. A pending approval is resolvable only through its own route's binding (ADR-0123), so it fails closed rather than widening to card-channel membership. Restore the binding and the approval resolves normally; it is not lost. |
| `403 could not verify approver group membership` | Slack group membership could not be verified. This fails closed and does not name its cause. Check the API `SLACK_BOT_TOKEN`, its `usergroups:read` scope and reinstallation, and Slack availability. It never falls back to channel membership. |
| `409 already resolved by ...` | Someone else won the claim. The decision stands. |
| `410 expired` | The record passed its deadline. The session was already woken down its timeout branch. |
| Thread says "Not requesting approval again: ... was rejected by ..." | The agent re-raised an approval a person rejected in this thread, with nobody asking since (`409 approval.rejected_in_thread`). Expected. To try again, ask for it in the thread. |
| Agent says a request is pending, no card anywhere | The named route is not bound for this agent, so the turn escalated instead of posting. Since #2436 this case is now caught before deploy for a newly declared route; the escalation remains the backstop for a binding removed after deployment. Add the binding. |
| `422`, or a rejected push with code `approval_routes.unbound` | The bundle declares an approval route with no entry in this agent's `approval_routes`. Bind every route the bundle declares, then redeploy. |
| deploy or route write exits 2: "declares approval route(s) ... with no entry in this agent's approval_routes" or "this write removes approval route(s)" | The CLI's local pre-check found the same gap before sending. Run the command in the error's fix, then re-run. |
| Notification arrived, but it has no buttons | Expected: notification is visibility-only. Use its approval ID and go to the configured approval channel to find the verified Slack resolution card; the ping deliberately does not disclose that channel's identifier. |
| Card resolved, but the agent never continued | The skill has no instruction for the `[approval resolved]` prefix. |

## Break-glass recovery

An upgrade can leave an approval nobody can reach: the card is gone, or the row
predates something the resume path now needs, and the suspended session waits
forever. Recovery is the way out. It is off by default and is meant to be turned
on for the length of a recovery and turned off again.

Enable it with `api.approvalRecovery.enabled` (environment
`CURIE_APPROVAL_RECOVERY_ENABLED`).

**Read this before you enable it.** Recovery adds no credential and no new
authentication scheme. It widens what the platform API key you already have can
do. While it is enabled, anyone holding that key can administratively reject an
approval anywhere in the installation, including
approvals the ordinary approver set could have resolved perfectly well. That is
deliberate. A recovery path fenced to rows something has already classified as
unrecoverable is useless in exactly the situation nobody planned for, which is
the only situation recovery exists for.

What makes that acceptable is the audit trail. Every recovery writes its audit
row in the same database transaction as its effect, so there is no such thing as
a recovery that happened without a row explaining it. Read the rows back with
`GET /approvals/{id}/audit`; a recovery carries `authorizer=approval_recovery`
and the operator who acted.

Two routes, both requiring the platform key:

| Route | What it does |
|---|---|
| `GET /approvals/identity-report` | Facts about every pending approval, and the declaration skeleton the upgrade workflow consumes. A pure read. It makes no Slack call, excludes nothing, and never claims a row cannot be resolved. |
| `POST /approvals/{id}/recover` | Settles a stranded approval as `rejected` and wakes its session over the ordinary runs stream. `rejected` is the only disposition; there is no approve-on-behalf-of. |

`recover` requires a reason and a caller-supplied `recovery_key`, and takes the
acting identity from an operator principal (`POST /approvals/principals/operator`)
for attribution. The key is recorded in the recovery's audit row. Replaying the
same key returns the outcome that already happened and changes nothing; a key
already recorded for another approval, or a record settled some other way, is a
`409`. Cancelling an owed resume is not part of this path yet (#2829).

A report entry stating `reply_identity_unreconstructable` means neither the row
nor any binding says how that reply would be authenticated. The report does not
guess: it emits an empty declaration for you to fill in and feed back to the
upgrade, because a guessed reply route is a silent misroute.

## Where the code is

| Concern | Path |
|---|---|
| Raising a request, and the tool gate | `runner/src/curie_runner/approval.py` |
| Pausing, routing the card, suspending | `apps/worker/src/curie_worker/kernel.py` |
| Remembering the card so an expiry can disable it | `apps/worker/src/curie_worker/approval_cards.py` |
| Click to resolve, and rendering the verdict | `apps/dispatcher/src/curie_dispatcher/approval_actions.py` |
| The resolve endpoint, claim, and audit | `apps/api/src/curie_api/routers/approvals.py` |
| Break-glass recovery | `apps/api/src/curie_api/routers/approval_recovery.py` |
| Who may resolve | `apps/api/src/curie_api/authorizer.py`, `apps/api/src/curie_api/approvers.py`, `apps/api/src/curie_api/slack_approvers.py` |
| The resume turn | `apps/api/src/curie_api/resumequeue.py` |
| The manifest shape | `packages/plugin-format/src/plugin_format/models.py` |
