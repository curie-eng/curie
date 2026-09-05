# Security Policy

Curie is a self-hostable platform for running Slack-based agents. This
document states the trust model plainly, explains how to report a
vulnerability, lists supported versions, and spells out what an operator is
responsible for.

## Trust model

**A bundle is code execution. The control is the sandbox, not the bundle
contents.**

The deployable unit in Curie is a "bundle": a Claude-Code-format plugin made
of skills, tools, and MCP servers. Uploading or deploying a bundle is
equivalent to running arbitrary code. Curie treats it that way by design:

- Bundle validation does not sandbox inputs at the config layer.
  [`packages/plugin-format/src/plugin_format/validate.py`](packages/plugin-format/src/plugin_format/validate.py)
  accepts any MCP server `command` (stdio) or `url` (remote) as-is. It only
  checks that one of the two is present, not what it points at. Model configs
  are `extra="allow"` by mandate.
- The runner executes the agent with `permission_mode="bypassPermissions"`
  ([`runner/src/curie_runner/adapter.py`](runner/src/curie_runner/adapter.py)).
  There is no in-agent permission gate.

Because a bundle is trusted to run arbitrary code, the security boundary is not
input validation. It is the Kubernetes sandbox rails shipped as defaults in the
Helm chart [`charts/curie`](charts/curie). See
[ADR-0006](docs/adr/0006-security-rails-as-chart-defaults.md) and
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the full rationale.

### The sandbox rails

The rails are the actual control. Every agent runs inside them:

- **Default-deny egress** (`security.networkPolicy.enabled`). A NetworkPolicy on
  runner-sandbox pods is fail-closed: an empty `allowedEgress` means the sandbox
  can resolve DNS and ship traces, but reach nothing else. Arbitrary internet
  and the cloud metadata endpoint `169.254.169.254` are denied. Only
  explicitly-declared egress is permitted: DNS, the in-chart collector, RustFS
  (or `rustfs.egress` / `rustfs.stsEgress` when the object store is BYO),
  and inference when deployed, plus the operator's declared model API and MCP
  hosts via `allowedEgress`.
- **gVisor kernel isolation** via a RuntimeClass (`security.gvisor.mode`).
- **Non-root runner containers** with a read-only root filesystem.
- **Per-agent RBAC scoping** so one agent's secrets are isolated from another's. The chart ships a least-privilege baseline (a ServiceAccount with no bound Role and no mounted token); the control plane binds each agent's `resourceNames`-scoped Role when the agent is deployed.

**How to confirm the rails hold.** The chart ships a PT-3 security probe as a
`helm test`
([`charts/curie/templates/security-probe.yaml`](charts/curie/templates/security-probe.yaml))
that asserts all of the above. It includes a before/after control proving the
CNI actually enforces the egress policy, so a NetworkPolicy-unaware CNI cannot
silently false-pass. Run it against your cluster to verify enforcement.

**Caveat.** A NetworkPolicy is only enforced if the cluster's CNI supports it,
and gVisor only isolates if the RuntimeClass is installed. Both are operator
responsibilities (see below).

### Local mode is not the Kubernetes boundary

The local developer loop (`curie local up` / `curie skill up`, the Docker
substrate) **accepts TRUSTED bundles only.** It is a convenience for developing
and demoing your own bundles on a laptop, not a security boundary for running
untrusted code. The Kubernetes sandbox rails above (gVisor + default-deny
NetworkPolicy + per-agent RBAC) are the only supported boundary for untrusted
bundles; do not use local mode to run a bundle you would not run as a plain
script on your machine.

Local mode still applies practical container isolation as defense-in-depth so a
trusted-but-buggy bundle cannot casually escalate on the host: each runner
container boots with a **read-only root filesystem** (writable `tmpfs` for `/tmp`
and `$HOME` only), **all Linux capabilities dropped**, **no-new-privileges**, the
Docker **default seccomp profile**, and **bounded pids/memory/cpu**, and it joins
a **dedicated `curie_runner` network** that carries only its documented
dependencies (telemetry collector, local model, and the API state endpoint) --
never the data tier (Postgres/Valkey/RustFS) and never the Docker daemon socket
(which only the worker holds). These controls reduce blast radius; they are not a
substitute for the Kubernetes boundary.

## Reporting a vulnerability

Please report security issues privately through **GitHub Private Vulnerability
Reporting**, which is enabled on this repository.

- Go to the repository's **Security** tab and click **Report a vulnerability**,
  or open a private advisory directly at
  <https://github.com/curie-eng/curie/security/advisories/new>.
- **Do not** open a public issue or pull request for a vulnerability, and please
  do not disclose it publicly until a fix is available.

Include what you can:

- The affected version or commit.
- Steps to reproduce.
- The impact (what an attacker can do).

Maintainers will acknowledge your report and coordinate a fix and a
coordinated disclosure with you. There is no published security email; GitHub
Private Vulnerability Reporting is the channel.

## Supported versions

Curie is pre-1.0. Only the latest minor release line receives security
fixes; older lines are unsupported. See the
[Releases page](https://github.com/curie-eng/curie/releases) for the current
version rather than a number pinned here (which only goes stale).

## Operator responsibilities

The rails are defaults, but they only protect a deployment the operator
configures and maintains. As an operator you are responsible for:

- **A CNI that enforces NetworkPolicy.** The default-deny egress rail is inert on
  a CNI that ignores NetworkPolicy. Verify with the security probe above.
- **Installing the gVisor RuntimeClass** referenced by `security.gvisor.mode`.
  Without it, the kernel-isolation rail does not isolate.
- **Keeping `allowedEgress` minimal.** Declare only the model API and MCP hosts
  your agents actually need. Every entry widens the sandbox.
- **Treating every bundle as trusted code.** Review what you deploy. A bundle
  can run anything the sandbox permits.
- **Protecting secrets and credentials** you supply to the platform (Slack
  tokens, model API keys), and relying on per-agent RBAC scoping to keep them
  isolated.
- **Running a supported version** and applying security updates promptly.

## Repository security baseline

This documents the supply-chain and merge-control baseline for the public
repository itself, distinct from the product trust model above. It is the
reference for what CI enforces in the tree and for the repository settings that
admins maintain (issue #632).

### Enforced in the tree (CI)

- **Secret scanning on pushes to and pull requests targeting both `main` and
  `next`, plus a weekly full-history sweep.** The
  `Secret Scan` workflow scans pull request commits from base to head, and full
  history on pushes, scheduled runs, and manual runs. Its scanner image is pinned
  to an immutable digest so this sensitive path cannot change under us.
- **Dependency vulnerability audits per ecosystem.** The `Dependency Audit`
  workflow runs `cargo audit` (Rust), `pip-audit` (the uv/Python workspace), and
  `pnpm audit` (the `apps/ui` JavaScript workspace) as advisory checks.
- **CodeQL** static analysis over the Python and JavaScript/TypeScript code.
- **Dependabot** keeps the five ecosystems (GitHub Actions, Cargo, uv, npm,
  Docker) current through grouped weekly PRs (`.github/dependabot.yml`).
- **Third-party actions and scanner images on sensitive workflows are pinned to
  immutable revisions** — a commit SHA for actions, a digest for images;
  first-party `actions/*` are referenced by major-version tag.

### Configured in repository settings (admin-managed)

These live in the repository's GitHub settings rather than the tree and are
maintained by repository admins:

- **Secret scanning** with non-provider patterns, validity checks, and **push
  protection** enabled, so a leaked credential is blocked at push time — not just
  detected after it lands.
- **Dependabot alerts** and **Dependabot security updates** enabled; the open
  alert set is triaged (fixed, or dismissed with a recorded reason).
- **A single repository ruleset (`Default`) covers `main` and `next`, so both
  receive identical protection.** It requires all required status checks to
  pass and all **review conversations resolved**. The `gitleaks (full history)`
  status check is required, and CodeQL is enforced through the code scanning
  rule.

### Bypass path and auditability

The `Default` ruleset applies to everyone, including admins, save its explicit
bypass actor. Only repository or organization admins may bypass it, and
only to recover from a stuck state — e.g. a required check wedged by
infrastructure rather than by the change under review. Every bypass is recorded
in the organization audit log (ruleset-bypass events), and every merge's check
state is visible on its PR, so a bypass is observable after the fact and is
expected to be justified in the PR. Routine merges never bypass; the ruleset is
the default and only path.
