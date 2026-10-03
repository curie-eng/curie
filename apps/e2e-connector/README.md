# End to end connector

A factory bundle declares this platform hosted connector in `connectors.yaml`. The run receives tools. It does not receive a kubeconfig. ADR 0176 fixes the capabilities. This file fixes the tool names and schemas.

The connector runs in the factory cluster. Its credential is a kubeconfig for a separate test cluster, stored with `curie secrets` under `E2E_CLUSTER_KUBECONFIG` and mounted only on the connector at `/secrets/kubeconfig`. An installation with `e2eConnector.enabled` false renders no connector. A bundle that declares `e2e` on that installation is refused at deploy with `e2e_connector_not_configured`.

## Declaration

```yaml
connectors:
  e2e:
    image: curie-e2e-connector
    secret_files:
      E2E_CLUSTER_KUBECONFIG: /secrets/kubeconfig
      E2E_REGISTRY_PUSH_CONFIG: /secrets/registry/config.json
      E2E_BUILD_CACHE_CONFIG: /secrets/registry-cache/config.json
```

The two registry lines are optional. Declare them only when the run uses `image_build`. Each is a docker `config.json` stored with `curie secrets`, mounted only on the connector at the fixed path shown, and withheld from the sandbox at every bind site that withholds `E2E_CLUSTER_KUBECONFIG`. Only basic credentials are supported: an `auth` of base64 `user:password`, or `username` and `password`. An `identitytoken` or `registrytoken` entry, or an `auth` that does not decode, makes `image_build` refuse with `e2e_connector_misconfigured` before any test cluster call, because retention could not delete what crane pushed with it.

The image value is a sentinel. The factory release substitutes the worker image, which contains `python -m curie_e2e_connector`. The release must set `e2eConnector.enabled` plus the test cluster service account, its namespace, the worker ClusterRole name, and the owner label value installed by `curie cluster up --e2e-connector-identity` on that test cluster.

## Caller identity

`env_create`, `env_destroy`, and `image_build` read `X-Curie-Run` and `X-Curie-Work-Item`. The caller proxy sets those headers from the signed caller token. A call with no pair is refused with `e2e_run_identity_required` and does not touch the test cluster.

## Tools served now

### env_create

Arguments:

1. `allow`, optional, at most 8 items. Each item is either `{"cidr": "<network>", "port": <1-65535>, "protocol": "TCP"|"UDP"}` (protocol defaults to TCP) or `{"dns": true}`.
2. `ttl_seconds`, optional. It must be at least 60 and no higher than the installation cap (default 3600).

Result:

```json
{"namespace": "curie-e2e-<run uuid>", "expires_at": "2026-10-02T13:00:00Z", "run": "<uuid>", "work_item": "<uuid>"}
```

The namespace is named with the connector prefix plus the run id. Its labels are `curietech.ai/e2e-owner=<release>`, `curietech.ai/e2e-run`, `curietech.ai/e2e-work-item`, and `pod-security.kubernetes.io/enforce=baseline` (or `restricted` when the installation says so). The annotation `curietech.ai/e2e-expires-at` is the TTL. The connector also creates a ResourceQuota named `sandbox`, a LimitRange named `sandbox`, a default deny NetworkPolicy named `default-deny`, and a RoleBinding named `e2e-connector` to the configured worker ClusterRole. Extra NetworkPolicies exist only for `allow` rules.

### env_destroy

Argument: `namespace`, the name `env_create` returned.

Result: `{"namespace": "<name>", "deleted": true}`.

Any other name, or a namespace whose run label is not the signed run, is refused with `e2e_namespace_not_owned`. The refusal does not send a delete.

Before the namespace delete, `env_destroy` runs the same image retention as the reaper (see Reaper). It sleeps out the 10 second settle itself, so a single call normally completes. While a build pod is still stopping it answers `e2e_build_in_progress` and deletes nothing; call it again.

### image_build

Contract: build a commit fetchable by SHA from an allowlisted https host.

Arguments:

1. `context`, required. A path relative to the repository root, or `.`. Segments match `[A-Za-z0-9._-]+`; no `..`, no leading `/`, no backslash, at most 256 characters.
2. `repository`, required. An https clone URL with no credentials, query, or fragment, on a host in `e2eConnector.sourceHosts` (default `github.com`).
3. `commit`, required. The full 40 character lowercase SHA.
4. `dockerfile`, default `Dockerfile`, relative to `context` under the same path rule.
5. `platforms`, optional, at most one entry such as `linux/arm64`. kaniko does not emulate, so the platform must match the build node.
6. `name`, default `app`. A lowercase DNS label that is also a registry path component.

Result:

```json
{"images": [{"name": "<registry>/<namespace>/<name>", "digest": "sha256:<hex>"}]}
```

The image lives at `<e2eConnector.registry>/<namespace>/<name>`. Deploy it by digest. The digest appears only in this reply.

Refusals:

1. `e2e_run_identity_required`: no signed caller pair.
2. `e2e_build_argument_refused: <field> ...`: an argument breaks the rules above. Nothing is sent to the test cluster.
3. `e2e_registry_not_configured`: the installation has no `e2eConnector.registry`.
4. `e2e_build_needs_baseline`: the installation or the namespace enforces `restricted` Pod Security. kaniko needs root, which `restricted` forbids; rootless BuildKit needs seccomp Unconfined, which `baseline` forbids.
5. `e2e_environment_required`: the run's namespace is missing or Terminating. Call `env_create` first.
6. `e2e_namespace_not_owned`: the namespace is not this run's.
7. `e2e_environment_closing`: teardown has closed the environment to new builds.
8. `e2e_build_failed: <reason>`: the Job failed. The reason is the failing container's message tail, at most 1000 characters, with every credential string from both registry configs redacted.
9. `e2e_build_timeout`: the build passed `e2eConnector.buildTimeoutSeconds` (default 1200, range 60 to 3600).
10. `e2e_build_no_digest`: the push container did not report `<repository>@sha256:<hex>` for this build's repository.

#### Source delivery limitation

A commit that exists only in the sandbox workspace cannot be built. The dark factory sandbox never pushes; publication happens through a separate trusted job. `image_build` can therefore build only commits already published to an allowlisted https host, and only public ones. This does not yet satisfy the pre publication factory case that ADR 0176 decision 6 and #3249 need. A trusted runner to connector workspace snapshot transport is tracked as follow up #3919, and it extends the `repository` and `commit` argument seam rather than replacing it.

#### How the build runs

The connector creates one Job, `e2e-build-<id>`, in the run's own namespace. Its pod sets `automountServiceAccountToken: false`, uses no host namespace or hostPath, and runs every container with `privileged: false`. It has three containers, so repository content never runs beside the push credential:

1. Init `source` (git) fetches the commit into an emptyDir. It holds no credential.
2. Init `build` (kaniko) builds to a tarball with `--no-push`. It mounts only the optional cache credential and applies no labels, because a Dockerfile `LABEL` would overwrite them anyway.
3. Main `push` (crane) runs a fixed script that receives every value through env. It pushes the tarball under the tag `staging-<id>`, applies the three run labels (`curietech.ai/e2e-owner`, `curietech.ai/e2e-run`, `curietech.ai/e2e-work-item`) with `crane mutate` under the tag `build-<id>`, and reports the post mutate digest. It is the only container that mounts the push credential, and it never executes repository content.

Both tags are unique to the build and never name the commit, so a rebuild never untags an earlier image and parallel identical builds never invalidate each other. The push container deletes nothing; teardown retention removes every tag's digest. Before any Secret or the Job exists, the connector records the target repository in the ConfigMap `e2e-images`. The push credential reaches the test cluster only as the per build Secret `e2e-registry-push-<id>`, and the cache credential only as `e2e-registry-cache-<id>`. Both are deleted when the call returns, whatever the outcome. A Secret whose create call failed without an answer, such as a lost response, may still exist, so the connector reads it back and deletes it only when it carries this build's `curietech.ai/e2e-build` label; another build's Secret of the same name is left alone. The NetworkPolicy `e2e-build-egress` lets build pods reach TCP 443, the registry and cache ports, the explicit ports of configured source hosts and registry token hosts, and DNS.

#### Build cache

The remote layer cache is used only when `E2E_BUILD_CACHE_REPO` is set **and** `E2E_BUILD_CACHE_CONFIG` is declared. In every other case the build runs with `--cache=false`, so an authenticated registry needs the cache config for caching.

The cache credential is mounted in the `build` container, where Dockerfile `RUN` steps can read it. Scope it to the cache repository only, never to the image repositories. The shared cache can be poisoned by one run for a later run. That affects only test images, which never ship; CI builds published artifacts.

#### Registry hosts

`e2eConnector.registryInsecure` switches registry calls to plain http and passes `--insecure-registry` per host to kaniko. A Bearer token realm must be https, carry no userinfo, and be on the registry host or a host listed in `E2E_REGISTRY_TOKEN_HOSTS` (`e2eConnector.registryTokenHosts`). An entry without a port matches 443 only, and `:443` on the registry host or an entry is ignored when comparing. Any other realm is refused before a credential is sent, and redirects are never followed.

#### Required of #3247

`deploy` shares the namespace with build Jobs and their Secrets. Before `deploy` ships beside `image_build`, it must refuse:

1. Create, update, or delete of any Job or Pod carrying label `curietech.ai/e2e-build`, judged on both the submitted object and the existing object of the same name.
2. Create, update, or delete of any Secret whose name starts with `e2e-registry-`.
3. Create, update, or delete of ConfigMap `e2e-images`.
4. Create, update, or delete of NetworkPolicy `e2e-build-egress`.
5. Label `curietech.ai/e2e-build` on any workload or pod template.
6. Any Role or RoleBinding that grants access to `secrets`.

## Reaper

Teardown does not depend on `env_destroy`. The factory worker runs a reaper (`curie_e2e_connector.reaper`, driven by `curie_worker.e2e_reaper`) every `e2eConnector.reaperIntervalSeconds` (default 60). It reads the kubeconfig from each agent's connector Secret through the connector reconciler's Secret list grant, so the release needs `worker.connectorReconciler.enabled`.

On each test cluster it lists namespaces carrying the owner label, then checks every item again for the namespace prefix and the exact owner label value. Nothing outside that scope receives a call. A scoped namespace is deleted when either holds:

1. Its `curietech.ai/e2e-expires-at` annotation is in the past, missing, or unreadable.
2. Its `curietech.ai/e2e-run` label names a request whose status is `completed`, `failed`, `expired`, or `cancelled`.

The reaper deletes the namespace's SandboxClaims, Jobs, and PersistentVolumeClaims first, then the namespace.

Image retention runs before those deletes, on every reap and on `env_destroy`, in this order:

1. Close admission. Teardown sets `closing_at` in the `e2e-images` ledger with a resourceVersion conditioned write, keeping an existing value. From then on `image_build` is refused with `e2e_environment_closing`, and a build admitted just before the close sees `closing_at` on its re-read right after its Job POST and deletes its own Job.
2. Wait until `closing_at` is 10 seconds old. The reaper defers the namespace to the next pass; `env_destroy` sleeps the remainder.
3. Delete the build Jobs.
4. If a build pod still runs, defer. The reaper skips the namespace for that pass, and `env_destroy` answers `e2e_build_in_progress`.
5. Read the ledger again, list every tag in each repository it names, and delete each distinct digest once. A 404 on DELETE counts as done.
6. Delete the children, then the namespace.

The residual window is a build that pushes before its own post-POST re-read or the namespace delete. A build takes minutes, so in practice the Job delete wins. A crash between push and reply cannot hide an image, because the repository was recorded before the Job existed.

A ledger that cannot be read on an Active namespace, a malformed ledger or one naming a repository outside `<registry>/<namespace>/`, a registry failure, or a missing registry config while the ledger names images all fail the namespace and keep it, so the pass is unclean and retries. A deferred namespace is neither failed nor unclean; the overdue gauge covers a build that never stops. On a Terminating namespace whose ledger is already unreadable, the images are left to the registry's own retention, and the same holds for a namespace deleted out of band. The registry must allow manifest deletes and run garbage collection; `registry:2` needs `REGISTRY_STORAGE_DELETE_ENABLED=true` and operator GC. The worker sweeps each distinct test cluster kubeconfig once, so agents sharing a cluster count and reap its namespaces once. It reaches the registry with the push config of every connector Secret naming that kubeconfig, trying each distinct credential in turn and moving on only when the registry answers 401 or 403; any other registry error fails the namespace. It uses `e2eConnector.registry` (a trailing `/` is ignored, as in the connector), `registryInsecure`, and `registryTokenHosts`.

A namespace stuck in Terminating gets those deletes again on the next pass. Each delete logs one WARNING naming the namespace and the reason, `ttl` or `terminal`.

A 403 on one of those child deletes does not stop the namespace delete, which is still sent. On a Terminating namespace the 403 is expected, because teardown removes the RoleBinding first, so the namespace counts as reaped. On an Active namespace it means the grant is wrong and the children it could not delete may be what holds the namespace, so the namespace is reported failed and the pass is not clean.

A pass is clean only when every test cluster listed, every run status was read, and every namespace due for deletion was deleted. A connector Secret whose `E2E_CLUSTER_KUBECONFIG` is not base64 UTF-8, or is a kubeconfig the connector would refuse, fails the pass with an error naming the Secret and the reason, never its contents; the other clusters are still swept. If the reaper is enabled but cannot work at all, because `worker.connectorReconciler.enabled` is off or the worker has no internal worker token, the worker logs the reason at startup and still runs the loop, and every pass fails without loading a kube config.

Three worker gauges report its health:

1. `curie.e2e.reaper.last_success` (Prometheus `curie_e2e_reaper_last_success_seconds`) is the unix time of the last clean pass, recorded every pass and 0 until one succeeds, so a misconfigured reaper reads 0.
2. `curie.e2e.namespaces.expired` (Prometheus `curie_e2e_namespaces_expired`) counts scoped namespaces past their TTL.
3. `curie.e2e.namespaces.overdue` (Prometheus `curie_e2e_namespaces_overdue`) counts scoped namespaces at least ten minutes past their TTL, or without a readable one.

Both namespace gauges are recorded only by a pass that listed every test cluster, so a listing failure holds the last full counts. A slow pass keeps reporting its stale timestamp every interval and is never cancelled or overlapped, so it still reaches deletion. The SRE example pages on three alerts: `CurieE2EReaperStalled` when the last clean pass is more than 15 minutes old, `CurieE2EReaperSignalAbsent` when the timestamp was reported in the last 7 days but not in the last 15 minutes, and `CurieE2EExpiredNamespacesAccumulating` when `curie_e2e_namespaces_overdue` stays above 0 for 15 minutes. That last alert keys on overdue rather than expired, because a healthy reaper always sees a few namespaces just past their TTL between sweeps.

## Names reserved for later issues

These tools are not served yet. Their names and result fields stay as written here.

1. `deploy` argument `manifests` (YAML). Result `{"namespace": "<name>", "applied": [{"kind": "<kind>", "name": "<name>"}]}`. A tag reference returns `e2e_image_not_digest`. A cluster scoped object returns `e2e_cluster_scoped_object` and applies nothing.
2. `run` arguments `command` (string array) and `image` (digest). Result `{"exit_code": 0, "stdout": "", "stderr": ""}`.
3. `logs` arguments `pod`, optional `container`, optional `tail`. Result `{"logs": ""}`.
4. `events` optional argument `involved`. Result `{"events": [{"reason": "", "message": "", "type": "", "count": 1}]}`.
