# ADR 0143 spike: the smallest custom connector, built with what ships

Evidence for
[ADR 0143](../../0143-a-custom-connector-is-a-bundle-built-http-mcp-server-that-holds-its-own-credential.md).
Everything here was run on 2026-09-04 with the **released `curie` 0.8.5
binary and the released 0.8.5 chart**, on a single-node k3s cluster, against a
public container registry. No source build of the platform was involved, so
every friction below is what an author with a released binary meets.

Identifiers are placeholders per the repository's rule: the registry is written
as `registry.example.com/acme`, the operator's LAN address as `<lan-ip>`, the
release and namespace as `curie-adr`, and generated agent, version and deployment ids as `<uuid-N>`. Digests are real.

## What is here

| Path | What it is |
|---|---|
| `bundle/` | The bundle exactly as deployed: `connectors.yaml` with a `build:` connector, the server, its Dockerfile, the skill, one eval case, and the Role the write-back needs. `connectors.lock.yaml` is the lock `curie build` wrote, with the registry name scrubbed. |
| `stub-api/` | The stand-in third-party finance API: bearer-token reads and a token endpoint that rotates the refresh token on every exchange and retires the previous one. Applied by hand as a ConfigMap-backed Deployment; it plays the external system and is not part of the bundle. |
| `runs/build.md` | Four `curie build` runs and the buildx command they issued. |
| `runs/deploy.md` | Four `curie cluster deploy` runs, including the refusal. |
| `runs/turns.md` | Five `curie cluster message` turns, replies verbatim. |
| `runs/logs.md` | The connector's structured stdout, the provider's view, the runner's two relevant lines, and the negative and recovery runs. |
| `runs/probes.md` | `skill check`, the unknown-key refusal, the ENTRYPOINT and read-only-root probes, deployment-row counts, the API route check, and the worker's environment. |
| `runs/rendered-objects.yaml` | The Deployment, Service and two NetworkPolicies the platform rendered for the connector, as read back from the cluster. |

## The sequence

```
curie cluster up --namespace curie-adr --release curie-adr --no-expose \
  --model <openrouter model> --set agentSandbox.controller.deploy=false \
  --set priorityClasses.platform.create=false --set priorityClasses.platform.name=curie-platform \
  --set priorityClasses.sandbox.create=false  --set priorityClasses.sandbox.name=curie-sandbox \
  --set security.gvisor.mode=off --set worker.slackTrustedOrigins=http://<lan-ip>:8157
kubectl -n curie-adr create configmap stubfin-api --from-file=stubfin_api.py=stub-api/stubfin_api.py
kubectl -n curie-adr apply -f stub-api/stubfin-api.yaml
kubectl -n curie-adr create secret generic stubfin-credentials \
  --from-literal=FIN_CLIENT_ID=... --from-literal=FIN_CLIENT_SECRET=... --from-literal=FIN_REFRESH_TOKEN=rt-0001
kubectl -n curie-adr apply -f bundle/manifests/connector-token-role.yaml
curie build --plugin-dir bundle --registry registry.example.com/acme
curie cluster deploy --plugin-dir bundle --namespace curie-adr --release curie-adr
curie cluster message --namespace curie-adr --release curie-adr --listen-host <lan-ip> --listen-port 8157 \
  "List the invoices for 2026-Q2 and total them."
```

The Secret is created with `kubectl create`, not `kubectl apply`, so no copy of
the token rides on the object in a `last-applied-configuration` annotation.

## What worked, with the observation that proves it

| Claim | Observation |
|---|---|
| A source change yields a new digest and the deploy renders exactly it | `runs/build.md` runs 1 and 2: `source_digest` and `image` both change. The pod's `imageID` equals the lock's `image` (`runs/rendered-objects.yaml`). |
| The agent reaches the connector and the connector reaches the provider | Turn 2 lists three invoices with a correct total; the connector logged one `tool_call` with `upstream_status: 200`; the provider logged one `read`. |
| Every tool annotated `readOnlyHint` makes the surface read-only to the runner | Runner: `request_approval omitted: observed MCP surface has no actionable tools tool_count=2 probe_complete=True failures=0`. |
| Refresh-token rotation, single holder, write-back before use | Turn 2: `token_persisted` precedes `token_refresh ok rotated=true`; the Secret holds `rt-0002`; the provider has retired `rt-0001`. A replay of `rt-0001` returns `400 invalid_grant`. |
| A restarted connector boots from the written-back token | Pod restarted, turn 3 succeeds, Secret advances to `rt-0003`. |
| An operator-provisioned Secret survives a redeploy | Deploy run 4 leaves the Secret at `rt-0003`. |
| Without the write-back grant the failure is loud and the credential is not lost | Turn 4: the model relays the 403 verbatim with "do not restart"; the provider is at `rt-0004`, the Secret at `rt-0003`. Grant restored, turn 5: both at `rt-0005`, no restart. |

## What got in the way, in the order it was hit

| # | Friction | Evidence |
|---|---|---|
| 1 | `curie build --registry` on the default Docker builder pushed a plain manifest; `cluster deploy` refused it as covering no platform. Same declaration through a `docker-container` builder produced an index and passed. | `runs/build.md` runs 2 and 3, `runs/deploy.md` run 1. |
| 2 | `tempfile.gettempdir()` raised at startup under the rendered securityContext; the pod crash-looped. | `runs/probes.md` read-only probe; `runs/logs.md` turn 1. |
| 3 | With the connector crash-looping, deploy exited 0 and the turn finalized with "no such connector". One runner `WARNING` was the only platform signal. | `runs/deploy.md` run 2, `runs/turns.md` turn 1, `runs/logs.md`. |
| 4 | Three deploys, three `active` deployment rows. | `runs/probes.md`. |
| 5 | An unknown key is refused as `Error: parse connectors.yaml`; the key name appears only under `--json` or `--debug`. | `runs/probes.md`. |
| 6 | The write-back Role and RoleBinding are `kubectl` state beside the bundle, bound to `default` because the schema has no ServiceAccount field. | `bundle/manifests/`, `runs/probes.md` unknown-key refusal. |
| 7 | `curie skill check` reports `declared: []`; no verb reports connector health or logs. | `runs/probes.md`. |
| 8 | The released worker carries no `CURIE_CONNECTOR_RECONCILE`; the CLI is the only apply path. | `runs/probes.md`. |
| 9 | The rendered pod has no probes and no telemetry environment. | `runs/rendered-objects.yaml`. |
| 10 | The same source built twice gave two registry digests. | `runs/build.md` runs 2 and 3. |

Two frictions from the first adopting repository were reproduced with plain
`docker run` against the pushed image rather than on the cluster, because the
cluster path would have needed a second image: an `ENTRYPOINT` image appends the
declared `args` to its entrypoint and starts anyway (`runs/probes.md`), and the
rendered read-only root filesystem leaves no usable temp directory while
`/dev/shm` is writable.

## Rerunning

Two edits followed the runs, both renames. The connector's `token_persisted`
log line named its Secret under the key `secret`, now `store`, and the two
environment variables naming that Secret and its key were `FIN_TOKEN_SECRET`
and `FIN_TOKEN_SECRET_KEY`, now `FIN_CREDENTIAL_STORE` and
`FIN_CREDENTIAL_STORE_KEY`, because a static scanner read a variable named
`secret` that holds a Secret's name as a credential being logged. The excerpts
in `runs/logs.md` and `runs/rendered-objects.yaml` show the current names, and
`connectors.lock.yaml`'s `source_digest` predates both renames.

The bundle deploys unchanged into any 0.8.5 release with the four hand-applied
objects above in place. `bundle/connectors.lock.yaml` names a registry that does
not exist; run `curie build --plugin-dir bundle --registry <yours>` first, through
a `docker-container` buildx builder, and the lock is rewritten.
