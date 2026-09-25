# What the tree installs, and how to reproduce it

The supported cluster reproduction is one command:

```bash
curie example sre-bot install --observability --approvers U0EXAMPLE1
```

With no targeting flags, Curie lands as release `curie` in namespace `curie`,
and the observability stack lands in namespace `observability`. To install
beside an existing release:

```bash
curie example sre-bot install --observability \
  --approvers U0EXAMPLE1 \
  --namespace <ns> \
  --release <release> \
  --observability-namespace <obs-ns>
```

`--dry-run` prints the same plan without mutating the cluster. The installer
copies a runtime bundle from the checked-in tree. It does not use `deploy.yaml`
for routing; the agent name comes from `plugin.json`, and Slack binding comes
from `--slack-channel` when supplied.

## Default connector shape

| Connector | Default install |
|---|---|
| `kubernetes` | kept at the pinned upstream digest with exact tool policy |
| `grafana` | kept and pointed at the bundled stack |
| `tempo` | kept at the published immutable digest |
| `self-upgrade` | stripped unless `--platform-upgrade` is selected |

The installer applies `manifests/kubernetes-access.yaml`, waits for the one
ServiceAccount token, constructs one kubeconfig, and reconciles it as
`K8S_KUBECONFIG`. Under that default grant, the credential combines non-secret
operational reads with workload writes only in `sre-demo`. The installer never
applies the opt-in operator grant, `manifests/kubernetes-operator-access.yaml`,
which widens the same credential's reads and writes to every built-in kind
except Secrets and ServiceAccount tokens; see the README before applying it.

There is no write allowlist flag and no separate scale identity. The exact 13
read tools execute immediately, the exact six mutating core tools require
one-shot approval, and unmatched tools deny. The config toolset and
multi-cluster support are disabled at server startup. Under the default grant,
RBAC, not approval, keeps the write blast radius inside the disposable workload
namespace; under the operator grant, approval is what stands in front of Secret
contents.

## Upgrade path: `--platform-upgrade`

Passing the flag keeps the `self-upgrade` connector and installs the platform
upgrade Job path:

```bash
curie example sre-bot install --observability --platform-upgrade \
  --approvers U0EXAMPLE1
```

After the ordinary deploy, the installer:

1. Applies `manifests/upgrade-role.yaml` for `sre-bot-upgrader`, the connector
   identity that can create Jobs but cannot run Helm itself.
2. Applies `manifests/platform-upgrade-role.yaml` for the short-lived
   `curie-platform-upgrader` Job identity that rewrites release objects.
3. Applies the upgrade script ConfigMap and a suspended CronJob. It never fires
   on schedule; an approved `mcp__self-upgrade__upgrade_platform` call creates
   one Job from the template.
4. Mints `SELF_UPGRADE_KUBECONFIG` and reconciles it onto the deployed version.

Read both Role manifests before accepting this flag. Kubernetes lets a holder
of the connector token create a Job selecting the wider platform-upgrader
ServiceAccount; RBAC does not restrict `spec.serviceAccountName` on Job create.
The connector cannot form that request, but a leaked token can bypass it.
Draft ADR-0141 proposes admission that pins those Jobs to the live CronJob
templates. The installer does not apply `manifests/upgrade-job-admission.yaml`;
the residual risk is unchanged until that ADR is Accepted and the policy is
installed with both identities.

The same connector publishes the separate zero-argument `upgrade_self` tool,
which targets `self-upgrade/cronjob.yaml`. The installer does not apply `self-upgrade/cronjob.yaml`;
enabling the platform path does not silently enable a self-upgrade template that
an operator did not install.

`platform-upgrade/upgrade.sh` accepts no target version from the bot. It reads
the newest published release, refuses when that version is already installed,
and does not roll back. Rollback remains an operator action.

## Slack

```bash
curie example sre-bot install --observability --slack-channel <channel-id> \
  --approvers <user-id>[,<user-id>]
```

`--approvers` binds explicit Slack user IDs on the `sre-approvals` route. You
must supply at least one user. The installer refuses omission before reading or
changing cluster state.

Bind by channel ID, never `#name`. `deploy.yaml` carries no active documentation
placeholder binding. Both API and CLI refuse the `C0EXAMPLE<digits>` placeholder
shape before creating or changing an agent, version, or deployment.

## Manual bundle deployment

`curie cluster deploy --plugin-dir examples/sre-bot` is a lower-level path:

- `tempo` and `self-upgrade` declare local build sources. A cluster deploy needs
  immutable registry locks for any connector it keeps.
- The Kubernetes connector is already an immutable upstream image and must keep
  its core-only, stateless, single-cluster arguments.
- `toolPolicy` references and connector approval gates must name connectors that
  remain in the runtime bundle. The exact `mcp__curie__publish_changes` gate is
  platform mounted, has no connector, and remains in both installer modes.
  Removing `self-upgrade` removes its two allow entries and legacy gates; the
  installer performs this transformation.
- `K8S_KUBECONFIG` must be supplied outside the bundle. If self-upgrade remains,
  `SELF_UPGRADE_KUBECONFIG` is also required.

The installer mints one-off overrides and does not persist them to the host
vault, so a later manual deploy still needs its credentials supplied.

## Historical staging cluster

An older staging host ran hand-patched bespoke `k8s-write` and `k8s-scale`
connectors with duplicated allowlists and credentials. That shape is retired.
Do not copy those deltas: the installer, exact tri-state policy, one Kubernetes
identity, and `manifests/kubernetes-access.yaml` are the reproducible source of
truth.

On an older Curie release, the Grafana connector Secret may still be absent
because the chart's `grafanaConnector` hook did not exist. New connector pods
then fail with `CreateContainerConfigError` while an old ReplicaSet can keep the
Deployment at `1/1`; inspect pod names and ages rather than trusting only the
ready column.
