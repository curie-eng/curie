# 9. Per-agent secrets and connector credentials

Date: 2026-07-09
Status: Accepted

Superseded in part by [ADR 0210](0210-a-bundle-may-declare-a-secret-optional.md): a bundle may also declare secrets as optional, and a deploy no longer refuses one it leaves unbound. Every name in `secrets` stays required.

## Context

An agent's usefulness comes largely from connectors: MCP servers that reach
GitHub, Jira, an internal API. A bundle declares those servers in `.mcp.json`,
and a stdio or remote MCP server almost always needs a secret (an API key, a
bearer token). The platform has no place for that secret today.

The only credential the platform resolves is the single model credential: it
flows from a chart Secret through `CURIE_CREDENTIALS` and is mapped by prefix
onto the SDK's auth env (`runner/src/curie_runner/sdk_auth.py`), and an
explicit SDK credential already in the env wins. An `McpServer.env` field exists
in the plugin format but is static (literal strings baked into `.mcp.json`); the
Claude Code CLI expands `${VAR}` in those values, but the platform injects no
operator-provided variables into the runtime for such an expansion to resolve
against. Egress is default-deny (`charts/curie/templates/security-networkpolicy.yaml`),
so a connector that reaches an external endpoint is dropped unless allowlisted.

Notably the frozen ACI already anticipates this: `SessionConfig.credentials_ref`
is documented as "per-tool secrets via K8s Secret refs ... the contract carries
the reference, not the secret material" (`packages/aci-protocol`). The seam
exists; only the resolution of it to more than the one model credential is
missing. This decision is about how a connector's secret is delivered into the
runtime, and it is independent of which harness runs the bundle.

## Decision

Adopt a **named, per-agent secret model, delivered as environment into the
sandbox, consumed by `.mcp.json` `${VAR}` expansion**, with a co-managed
per-agent egress allowlist.

- A bundle **declares which named secrets it needs** (versioned policy that ships
  and evaluates with the agent). The **values** are supplied as per-agent
  deployment configuration, never in the bundle.
- The platform resolves those bindings into a **per-agent Kubernetes Secret** and
  surfaces them into the runner/sandbox environment as named variables; the
  bundle's `.mcp.json` references them as `${VAR}`. Binding or rotating a secret
  requires a pod rollout, because `secretKeyRef` env resolves once at pod start.
- Reaching an external connector endpoint requires a **per-agent egress
  allowlist** entry; the default-deny posture is preserved.
- OAuth for remote MCP servers (dynamic client registration, token refresh) is
  **explicitly out of scope** here and deferred until a concrete need exists.

## Concrete build (accepted 2026-07-15, issue #429)

The decision above is realized across the parity ladder as follows. The unblocking
trigger was #429: an authed-MCP bundle (`examples/github-issues`) that runs at the
`skill` tier fails at `local`/`cluster` with `Bad credentials` because the worker
never delivers the connector secret to the sandbox it spawns.

**Policy — the bundle declares its needs.** The plugin-format manifest gains an
optional `secrets: [NAME, …]` field: the versioned, evaluable list of named
secrets the bundle requires. It carries **no values**. (`packages/plugin-format`.)

**skill tier (already shipped).** The CLI forwards the value by name into the
runner container with `curie skill up --secret NAME`, resolving it from the
process env or the host secret vault (`cli/src/secrets.rs`). No platform involved.

**local tier — values on the agent record.** `curie local deploy --secret NAME`
resolves each value from the host vault and sends it to the platform API, which
stores it in a nullable `secrets` JSONB column on the agent row (mirroring
`behavior_packs`). The worker, resolving a deployment, reads that column and
injects the `name→value` pairs **by value** into the sandbox boot env
(`binding.boot_env`); the docker substrate forwards them as `-e KEY=VALUE`. The
by-value argv exposure is accepted for the local dev substrate. This is the
minimum that restores parity for authed-MCP bundles.

**cluster tier — per-agent K8s Secret, never on the CR.** The agent's `secrets`
column carries **names only** at this tier; values live in a per-agent Kubernetes
Secret (`<release>-agent-<id>-connector-secrets`), written by
`curie cluster deploy --secret` via `helm upgrade` using the higher-sensitivity
`-f` values-file path (not the argv-masked `--set` the Slack connect verb uses).
Delivery is by **`secretKeyRef`** into the sandbox, through a per-agent
SandboxTemplate/warm pool — the mechanism the chart already uses for the model
credential (`charts/curie/templates/agent-sandbox.yaml`). The `SandboxClaim`
env schema is **value-only (no `secretKeyRef`)**, so connector secret values are
**kept off the claim CR entirely** and supplied only via the template's
`secretKeyRef`. Binding or rotating requires a pod rollout (`secretKeyRef` env
resolves once at pod start), the same `kubectl rollout restart` + `rollout status`
pattern `cluster comms --slack` uses. The pool-per-authed-agent cost is accepted;
on-demand (non-pooled) claims are the future alternative if a CRD `secretRef`
lands.

**Egress stays co-managed (cluster).** A per-agent secret is inert without a
per-agent egress allowlist entry: per-agent pod label + a per-agent
`allow-egress` NetworkPolicy `podSelector`, so one agent's connector host is not
opened for all. Default-deny and the cloud-metadata-IP carve-out are preserved.

**Staging.** Landed as sequential PRs: (1) the plugin-format policy field; (2) the
local tier (API column + worker forwarding + `local deploy --secret`); (3) the
cluster per-agent Secret + `secretKeyRef` + per-agent egress.

## Alternatives considered and rejected

1. **Bake secrets into the bundle's `.mcp.json`.** Rejected. Bundles are
   versioned, evaluable, reviewable artifacts that are often shared; a secret in
   the artifact leaks, appears in traces and git history, and cannot rotate
   without a redeploy. This breaks the "bundle is a reviewable artifact" premise.
2. **Extend the single-credential prefix map to carry N credentials in one blob.**
   Rejected. That path is Anthropic-model-auth-specific; overloading it with
   arbitrary third-party tool secrets conflates model authentication with tool
   authentication and does not scale to arbitrary named secrets.
3. **Put secret values (not just references) in the bundle manifest as deployment
   config.** Rejected. It conflates versioned policy (which secrets an agent
   needs) with workspace-specific values (the actual secret). The established
   behavior-packs split keeps policy in the bundle and bindings in deployment.
4. **A networked secret broker/vault the runner calls at tool-invocation time.**
   Rejected for now. It adds a runtime dependency and a new trust boundary reached
   from inside the untrusted sandbox path, and it is over-built versus a
   Kubernetes Secret plus env for the near term. Revisit only if short-lived or
   dynamically-minted secrets become a real requirement.
5. **Skip the platform-side mechanism and rely on the harness (OpenCode ships
   `{env:}` interpolation and MCP OAuth natively).** Not a rejection but a
   boundary: whatever the harness, the platform still must *deliver* the secret
   into the runtime environment. This decision is that delivery mechanism and is
   harness-independent; the harness only changes the consumption syntax. See the
   harness-strategy ADR.

## Consequences

- Connectors that need a secret become possible, which is most useful connectors.
- Egress must be co-managed with secrets: a credential without an allowlisted
  destination still fails closed, and an allowlist without a credential is inert.
- The rollout-restart requirement on bind is an operational cost to document
  (the same pattern the Slack connect verb already uses).
- OAuth-only remote MCP servers remain unsupported until the deferred OAuth work
  lands; if the harness strategy adopts OpenCode, its native OAuth may satisfy
  this instead of a bespoke broker.
