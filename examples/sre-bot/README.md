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

Use this order for a fresh cluster. The example installer creates the platform
on its fake model default, installs the observability stack, applies the
Kubernetes identity, builds its kubeconfig in memory, binds the approval route,
and deploys the bundle. Enter the model credential only after that command
succeeds, then run `curie cluster up` to record it outside bundle content.

```bash
curie example sre-bot install --observability --slack-channel C0EXAMPLE1 \
  --approvers U0EXAMPLE1,U0EXAMPLE2 \
  --workspace-repo acme-corp/acme-bot
read -rsp 'Model credential: ' CURIE_CREDENTIALS
printf '\n'
export CURIE_CREDENTIALS
curie cluster up --set 'api.githubRepoAllowlist[0]=acme-corp/acme-bot'
```

These commands use the current Kubernetes context and the default `curie`
release and namespace. Observability defaults to the `observability` namespace.
Persistent volumes use the cluster's default storage class, including the
default supplied by a stock kind cluster. See the complete executable sequence
in [DEMO.md](DEMO.md#fresh-install).

The installer is a fresh install command. If the selected release already
records a model credential, it refuses before platform mutation because its
declarative platform step would clear the credential and restore the fake model
default. Use the normal cluster lifecycle for an existing release.

The installer binds the `sre-approvals` route that gates the six Kubernetes
mutations and platform publication before it deploys. Terminal resolution
requires an explicit users list. Supply it with the required `--approvers` flag
using comma separated Slack user IDs. The installer refuses an omitted or empty
list before it reads or changes cluster state. A rerun replaces the managed
route channel and user list with the requested values.
If any bound route carries a notification target, the installer refuses to
rewrite the route map (the API does not return the notification transport);
write the full map with `curie cluster approvals sre-bot --routes-from <file>`.

Runtime repository workspaces need `api.githubRepoAllowlist`. The chart default
is empty and denies every selection, including after `curie cluster deploy
--workspace` (that flag is a deprecated compatibility no-op; the allowlist is
the real control). Pass `--workspace-repo owner/repo` to the installer and the
matching `api.githubRepoAllowlist` value to the following `cluster up`, as the
fresh install sequence above does. Both inputs are repeatable, and `owner/*` is
also accepted.

`curie cluster deploy --workspace` warns when the allowlist is empty.

Inspect the mutation plan first with `--dry-run`. Add `--platform-upgrade` only
after reading `manifests/platform-upgrade-role.yaml`; it creates a separate,
purpose-built upgrade path with much wider authority.

For a manual install, apply `manifests/kubernetes-access.yaml`, assemble a
kubeconfig for `sre-bot-kubernetes`, store it as the connector secret
`K8S_KUBECONFIG`, then deploy the unchanged bundle. The bundle declares the
`sre-approvals` route, and the agent cannot bind a route before it exists. On a
fresh install the first deploy creates the `sre-bot` agent and stops with exit
2, before any version is uploaded, printing the binding command. Bind the
route, then deploy again:

```bash
curie cluster deploy --plugin-dir examples/sre-bot   # first run: creates the agent, refuses locally
curie cluster approvals sre-bot --route-resolution sre-approvals=C0EXAMPLE1 \
  --route-approvers sre-approvals=users:U0EXAMPLE1
curie cluster approvals sre-bot --list-routes
curie cluster deploy --plugin-dir examples/sre-bot
```

On an existing agent that already binds the route, the first deploy succeeds and
the bind step is unnecessary. Terminal resolution additionally requires an
explicit users list. A platform API key only mints the operator principal.
`CURIE_APPROVAL_PRINCIPAL_TOKEN` carries the subject used by `--resolve`, and
that subject must appear in the route's users list. Rejection or resolution by
an unbound operator must not publish changes. A route write replaces the whole
map, so repeat the resolution and users in the same invocation.

With `--observability`, the example also installs the metrics pipeline and
reliability alerts. Follow [METRICS-ROLLOUT.md](docs/METRICS-ROLLOUT.md) for the
staged rollout and runtime proof; rendered configuration is evidence of wiring,
not proof that live samples or alerts reached their destination.

Metric labels stay bounded: operation class and outcome only. Run, session,
sandbox, user, and deployment identifiers are correlation attributes on logs
and traces. To diagnose a failed synthetic request:

1. Take the accepted-message timestamp and the W3C `trace_id` from the
   structured log line, without reading the private message body.
2. In Tempo, open that `traceId` and confirm the expected operations on the
   same trace.
3. In Prometheus, check the matching low-cardinality series. Do not add
   `run_id` or `trace_id` as label matchers; those identities are not metric
   labels.
4. Diagnose completion debt from the completion-outbox metrics rather than
   inspecting message bodies or credentials.

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

## Alert source (opt-in)

Alertmanager stays off in the default observability overlay. One supported
signed source is opt-in: apply `observability/alertmanager-webhook.yaml`, run
`observability/alert-signer/server.py` with `CURIE_HOOK_URL`,
`CURIE_HOOK_SECRET`, and `CURIE_SIGNER_TOKEN`, and configure the agent's `source_bindings` for hook
`alertmanager` (workload pointer `/commonLabels/curie_workload`, partition
`/curie_partition`). A genuine signed alert creates one partitioned
investigation. Missing, ambiguous, or unauthorized mappings visibly stop
coding. Invalid signatures and replayed delivery ids do not multiply work.

## What watches the alert path

A broken alert path looks exactly like a quiet cluster: every rule goes quiet
and nothing says so. Healthy means the heartbeat keeps arriving outside the
cluster and no alert is firing; either one alone proves nothing.

`CurieKubeStateMetricsDown` pages when a kube-state-metrics target of this stack
has failed its scrape, or none has been scraped, for 5 minutes. Most workload
rules read kube-state-metrics, and without it they stay quiet whatever the
cluster does.

The heartbeat is opt-in. Set up a check in an external dead man's switch that
alarms when posts stop, with a period of at least 5 minutes (posts arrive about
every two minutes). Store its URL in a Secret in Alertmanager's namespace
(`observability` unless `--observability-namespace` named another):

```bash
read -rsp 'Heartbeat URL: ' HEARTBEAT_URL   # e.g. https://heartbeat.example.com/ping/EXAMPLE
printf '\n'
kubectl -n observability create secret generic alertmanager-heartbeat \
  --from-literal=url="$HEARTBEAT_URL"
```

Then upgrade the Prometheus release with the overlays in this order, the
heartbeat overlay last. `my-alertmanager.yaml` stands for your own overlay, if
you have one:

```bash
helm upgrade prometheus prometheus-community/prometheus --version 29.27.0 \
  -n observability \
  -f examples/sre-bot/observability/prometheus-values.yaml \
  -f examples/sre-bot/observability/alertmanager-webhook.yaml \
  -f my-alertmanager.yaml \
  -f examples/sre-bot/observability/alertmanager-heartbeat.yaml
```

The heartbeat overlay goes after the webhook overlay: the other way round the
config is invalid (`undefined receiver "heartbeat"`). Helm replaces lists, so
the heartbeat overlay's `extraSecretMounts` replaces yours. Add each mount you
already have, such as the alert-signer token the `curie-sre` receiver's
`credentials_file` reads, to that list in `alertmanager-heartbeat.yaml`, or the
bot's receiver loses it while the heartbeat keeps arriving. Re-running
`curie example sre-bot install --observability` upgrades the release with
`prometheus-values.yaml` alone, which removes the overlays and turns
Alertmanager off; run the command above again after it.

The overlay adds `CurieAlertPathHeartbeat`, which always fires, and routes it
only to that URL; it never reaches the bot. Without it, nothing watches the
alert path. Posts stop within about a minute and a half of Prometheus stopping,
so a service with a five-minute period alarms about six to seven minutes after
the stop, plus its grace. The Secret mount is optional: without the Secret or
its `url` key Alertmanager still runs and every alert still reaches the bot;
only the heartbeat posts fail, so the external service alarms.

The heartbeat cannot see the last leg, from Alertmanager to the bot. Check that
with one synthetic alert posted to Alertmanager's API:

```bash
kubectl -n observability exec prometheus-alertmanager-0 -- \
  amtool alert add CurieSyntheticDeliveryCheck \
  --annotation=summary='Synthetic delivery check' \
  --alertmanager.url=http://localhost:9093
```

Then wait for the bot's reply in the bound channel (`C0EXAMPLE1` above). The
reply says coding is stopped, because the alert names no workload, and a
second, resolved delivery follows about five minutes later. A missing reply
means the path is broken somewhere between Alertmanager and the bot.

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
