# 223. Curie Manager ships with the platform and holds a scoped credential

Date: 2026-10-13

Status: Draft

## Context

Curie Manager is an agent that manages the Curie installation it runs on, from
chat. It reads freely (health, agents, versions, deployments, schedules, hook
runs, controls, memory, approvals, metrics, traces). It can make four changes:
pause or resume a schedule, and kill or resume an agent. Each of those four
needs an approval click. It deletes nothing. Its bundle and its platform
connector are in [PR #3856](https://github.com/curie-eng/curie/pull/3856) as
`examples/curie-manager`.

It was first deployed to a cluster the way any example is: a bundle copy on an
operator's machine, `curie cluster deploy`, connector secrets set by hand, and
the release values for its Slack identity and sandbox pool kept on a local
branch. That deployment failed in four ways, each of which comes from the agent
living outside the platform rather than from the agent itself:

1. **It was not in the repository.** The bundle, its secrets and its values
   existed only on one machine. No one else could reproduce or review it.
2. **It did not survive an upgrade.** Its Slack identity and its
   `agentSandbox.connectorSecrets` entry were missing from the values the
   installation is upgraded from. The next upgrade removed both.
3. **It fought the release.** `curie cluster deploy` cloned the generic runner
   template over the Helm-owned per-agent template, which dropped the
   connector-secret env (the sandbox then sent no bearer and the connector
   returned 401). Its connector and caller proxy were objects the release did
   not own, and they got in the way of the installation's upgrade procedure.
4. **It held the platform API key.** The connector needs to call the platform
   API, and the only credential that can is the platform key, which can do
   everything. The bundle's tool policy limits what the agent asks for, but
   not what a leaked key can do.

[ADR 0156](0156-adapter-principal-with-a-scoped-credential.md) made a channel
adapter a principal with a credential scoped to what an adapter does, and said
bot identities would get the same shape in a later ADR. This is that ADR for
one agent, the one whose whole job is the platform API.

## Decision

1. **Curie Manager is part of the platform, off by default.** The chart gains a
   `manager` block, `manager.enabled: false` by default. When an operator turns
   it on, the release (not a separate deploy) owns everything the agent needs:
   its bundle, its connector Deployment and caller proxy, its sandbox template
   and warm pool, its connector Secret, and its Slack identity and channel
   binding, all from values in the release. A Helm upgrade carries it forward,
   and turning it off removes it.
2. **Its version is the platform's version.** The connector image is published
   by the platform release workflow at the platform's tag, and the bundle is
   built into a platform image at the same tag. There is no separate version to
   keep in step, and no bundle copy outside the repository.
3. **It holds a manager credential, never the platform key.** A new principal
   kind, `manager`, beside `chat`, `console`, `operator` and `adapter`. Its
   credential carries exactly these scopes:
   - `platform:read`, for the read routes the manager's tools call;
   - `schedules:pause` and `schedules:resume`;
   - `agents:kill` and `agents:resume`.

   Any other route refuses it. Issuance is administrative, behind
   `require_platform_key`, in the same shape as the operator and adapter
   credentials: the release mints it once at install (a hook that presents the
   platform key and writes only the manager credential into the connector
   Secret), and the connector never sees the platform key. The credential
   expires, and the connector rotates it before expiry by presenting the
   current one, as ADR 0156 decision 4 does for adapters.
4. **The API checks the approval too, not only the connector.** A gated tool
   call already reaches the connector only with a one-time signed grant
   (`X-Curie-Connector-Grant`), which the caller proxy verifies and spends. For
   the manager, the proxy also forwards that grant to the server, and the
   server presents it to the API with the write. The API honors a write scope
   only with a valid grant for that exact tool call that it has not seen
   before. A manager credential presented without one is refused. The bundle's
   `approvalRequired` policy and the proxy check stay; the API check means a
   leaked credential alone cannot make a change.
5. **Every audit row names the manager.** Reads and writes performed with the
   manager credential record `principal_kind = manager`, and a write also
   records the approval id and its resolver.
6. **`curie cluster deploy` leaves release-owned sandbox pools alone.** When a
   per-agent template already exists and is owned by the release, a deploy
   neither clones nor applies over it. This holds for every agent, not just the
   manager.

## Consequences

- Turning the manager on is one value in the installation's values file, and
  turning it off removes it. Nothing about it lives on an operator's machine.
- The platform API gains a credential check that accepts the platform key
  (unchanged) or a manager credential with the matching scope. Existing callers
  are unaffected.
- A leaked manager credential can read the installation and pause, resume,
  kill or resume, and only with an approval the API resolved. It cannot delete,
  deploy, change secrets, or mint other credentials.
- Who may approve stays with the approval route, as today. If a decision such as
  [PR #4513](https://github.com/curie-eng/curie/pull/4513)'s identity provider is
  accepted, the manager's routes follow it like any other.
- `examples/curie-manager` stays the source of the bundle and the connector. The
  chart builds from it, so the example and the shipped agent cannot drift.
- Tests to hold this: a manager credential is refused on every route outside its
  scopes; a write without an API-resolved approval is refused; a write with an
  approval for different arguments is refused; rotation works before expiry and
  not after; an upgrade with `manager.enabled` keeps the agent, its pool and its
  binding; `curie cluster deploy` of any agent does not touch a release-owned
  template.

## Alternatives considered

1. **Keep it as an example installed with one CLI command**, as the SRE bot is
   (`curie example sre-bot install`). This fixes reproducibility but not the
   upgrade (the release still does not own the agent's values) and not the key.
2. **Give the connector the platform key and rely on the tool policy.** This is
   what failed. The policy controls the agent, not the key.
3. **Add scopes to the existing API key.** Refused for the same reason ADR 0156
   refused it: it changes what a key may do without giving the holder an
   identity, so audit rows still could not say the manager acted.
4. **Wait for a general per-agent principal (`agent` kind).** That is the
   direction ADR 0156 names. This ADR does not block it: when it lands, the
   manager can become an `agent` principal with these scopes, and approvals,
   audit and routes do not move.
