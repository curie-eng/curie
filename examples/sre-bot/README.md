# SRE bot example

This bundle combines Grafana, Tempo, and one pinned upstream Kubernetes MCP
server. The Kubernetes connector runs only the `core` toolset, has config and
multi-cluster disabled, is stateless, and reads one file-mounted kubeconfig.

## Kubernetes authority

The bundle's `toolPolicy` classifies the pinned server's complete 19-tool core
surface by canonical `kubernetes/<tool>` name:

- 13 read tools are allowed immediately;
- `pods_delete`, `pods_exec`, `pods_run`, `resources_create_or_update`,
  `resources_delete`, and `resources_scale` require a fresh human approval;
- unmatched tools are refused by `curie/mcp-tool-policy@1` and never become an
  approval request.

Approval is not authorization. The connector ServiceAccount in
[`manifests/kubernetes-access.yaml`](manifests/kubernetes-access.yaml) can read
enumerated non-secret operational resources cluster-wide, but can write only
workload APIs in the disposable `sre-demo` namespace. It cannot read Secrets or
mutate namespaces, nodes, identities, RBAC, CRDs, admission webhooks, or any
cluster-scoped resource. The general connector has no platform-upgrader grant.

`resources_create_or_update` accepts a raw manifest, so the Role is the real
blast-radius ceiling: within `sre-demo`, an approved call can replace workload
images, commands, and environment. Approval records intent; it does not narrow
arguments. Review the manifest before widening that Role.

## Install

The one-command example path installs the observability stack, applies the
Kubernetes identity, builds its kubeconfig in memory, deploys the bundle, and
stores the credential outside bundle content:

```bash
curie example sre-bot install --observability
```

Runtime repository workspaces need `api.githubRepoAllowlist`. The chart default
is empty and denies every selection, including after `curie cluster deploy
--workspace` (that flag is a deprecated compatibility no-op; the allowlist is
the real control). Pass `--workspace-repo owner/repo` (repeatable; `owner/*`
is also accepted), or set the chart value yourself:

```bash
curie example sre-bot install --observability \
  --workspace-repo acme-corp/acme-bot
curie cluster up --set 'api.githubRepoAllowlist[0]=acme-corp/acme-bot'
```

`curie cluster deploy --workspace` warns when the allowlist is empty.

Inspect the mutation plan first with `--dry-run`. Add `--platform-upgrade` only
after reading `manifests/platform-upgrade-role.yaml`; it creates a separate,
purpose-built upgrade path with much wider authority.

For a manual install, apply `manifests/kubernetes-access.yaml`, assemble a
kubeconfig for `sre-bot-kubernetes`, store it as the connector secret
`K8S_KUBECONFIG`, bind the declared `sre-approvals` route, and deploy the
unchanged bundle. Deploy is refused until that route is bound:

```bash
curie cluster approvals sre-bot --route-resolution sre-approvals=C0EXAMPLE1
curie cluster approvals sre-bot --list-routes
curie cluster deploy --plugin-dir examples/sre-bot
```

With `--observability`, the example also installs the metrics pipeline and
reliability alerts. Follow [METRICS-ROLLOUT.md](docs/METRICS-ROLLOUT.md) for the
staged rollout and runtime proof; rendered configuration is evidence of wiring,
not proof that live samples or alerts reached their destination.

## Operational limits

- Events normally expire quickly and pod logs disappear with the pod. The MCP
  server is live introspection, not historical retention; use the observability
  stack for history.
- Denying a mutation changes no Kubernetes state. An approval is one-shot and
  tool-name-scoped; a second mutation requires a second approval.
- Kubernetes writes have no general rollback. Scaling can be reversed only when
  the prior replica count was observed; deletes, execs, raw manifest updates,
  and pod runs need workload-specific recovery.
- An approved call outside `sre-demo` still receives a Kubernetes 403. Fixing
  that by widening RBAC is an operator security decision, never an approval
  retry.

## Platform upgrades remain separate

The `self-upgrade` connector continues to publish `upgrade_self()` and
`upgrade_platform()` as separate zero-argument actions behind explicit legacy
approval gates. It starts pinned Job templates; it is not part of the general
Kubernetes connector, and its kubeconfig is never shared with it.

## Live demo

The six Slack scenarios (read, approved scale, one-shot re-arm, configuration
denial, RBAC ceiling, coding-agent pull request), the Slack app and GitHub
prerequisites, and the expected evidence for each are in [DEMO.md](DEMO.md).

## Verification

Use the real pinned image and a disposable cluster. A complete pass proves:

1. a read executes with no approval record;
2. a mutation stops before the MCP server sees it;
3. denial leaves state unchanged;
4. approval resumes that exact mutation once and changes state;
5. another mutation blocks again;
6. config, multi-cluster, and unclassified tools are absent or refused;
7. an approved out-of-scope action is rejected by RBAC; and
8. the separate platform-upgrade gate remains armed.
