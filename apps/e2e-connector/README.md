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

Every tool reads `X-Curie-Run` and `X-Curie-Work-Item`. The caller proxy sets those headers from the signed caller token. A call with no pair is refused with `e2e_run_identity_required` and does not touch the test cluster.

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

### deploy

Argument: `manifests`, YAML text of at most 1 MiB. It holds one or more documents, or a `kind: List` (any kind ending in `List` with `items`), up to 100 objects in total. Each object needs `apiVersion`, `kind`, and a `metadata.name` that is a DNS subdomain of at most 253 characters. YAML anchors and aliases are refused.

Result:

```json
{"namespace": "curie-e2e-<run uuid>", "applied": [{"kind": "Deployment", "name": "web"}]}
```

`applied` follows manifest order. Each object lands in the run's namespace with the three run labels merged into `metadata.labels`. `status` and the server set metadata (`resourceVersion`, `uid`, `creationTimestamp`, `managedFields`, `generation`, `selfLink`) are dropped. An object that does not exist is created; one that exists is replaced with its current `resourceVersion`.

Every refusal below is decided before the first write, so a refused manifest applies nothing. They are checked in this order:

1. `e2e_run_identity_required`, then `e2e_environment_required` or `e2e_namespace_not_owned` for the run's namespace, as for `image_build`.
2. `e2e_deploy_manifest_refused: <reason>`: the text does not parse, or an object breaks the rules above.
3. `e2e_cluster_scoped_object: <json>`: at least one object is cluster scoped. The whole deploy is refused, whatever else the manifest holds. A cluster scoped object needs CI only proof; this connector never applies one. The connector knows these kinds without asking: Namespace, Node, PersistentVolume, ComponentStatus, CustomResourceDefinition, PriorityClass, ClusterRole, ClusterRoleBinding, Validating and Mutating WebhookConfiguration, Validating and Mutating AdmissionPolicy and their Bindings, StorageClass, CSIDriver, CSINode, VolumeAttachment, APIService, IngressClass, RuntimeClass, CertificateSigningRequest, FlowSchema, and PriorityLevelConfiguration. Every other kind is resolved through API discovery, and a kind discovery reports with `namespaced: false` is refused the same way.
4. `e2e_deploy_object_refused: <json>`: the cluster does not serve the object's kind in its `apiVersion`.
5. `e2e_namespace_not_owned`: an object names a `metadata.namespace` other than the run's.
6. `e2e_image_not_digest: <json>`: an image is not `<ref>@sha256:<64 hex>`, or is missing. Every list named `containers`, `initContainers`, or `ephemeralContainers` anywhere in the object is checked, so Deployments, StatefulSets, Jobs, CronJobs, Pods, and custom resources with pod templates are all covered, and so is `volumes[].image.reference`. A tag followed by a digest is accepted, because the digest pins it.
7. `e2e_deploy_object_refused: <json>`: the object reaches something the connector owns (see Deploy guard).
8. `e2e_deploy_failed: <kind>/<name> (<status>) <message>; applied: <json>`: the test cluster refused a write. The message is the tail of the API server's Status message. `applied` lists the objects written before the failure, which stay in place.

The JSON after the code and `": "` has sorted keys:

1. `e2e_cluster_scoped_object`: `{"objects": [{"apiVersion": "...", "kind": "...", "name": "..."}]}`, every cluster scoped object in manifest order.
2. `e2e_image_not_digest`: `{"images": [{"container": "...", "image": "...", "kind": "...", "name": "..."}]}`. `container` is the container name, or `volume/<name>` for an image volume. `image` is empty when the container has none.
3. `e2e_deploy_object_refused`: `{"objects": [{"kind": "...", "name": "...", "reason": "..."}]}`, one entry per refused object.

#### Deploy guard

`deploy` shares the namespace with `env_create`'s bounds and with build Jobs and their Secrets. It refuses an object, with `e2e_deploy_object_refused`, when:

1. Any mapping key anywhere in it is `curietech.ai/e2e-build`, which covers labels, pod template labels, and selectors, or any string value is exactly that label.
2. Any string value anywhere in it starts with `e2e-registry-`. That covers a Secret of that name and every volume, env, or image pull reference to one.
3. It is the ConfigMap `e2e-images`.
4. It is a NetworkPolicy of any name. `env_create` owns egress through its `allow` rules, and an extra policy would widen the default deny.
5. It is a ResourceQuota or a LimitRange. `env_create` owns the namespace bounds.
6. It is the RoleBinding `e2e-connector`.
7. It is a Role with a rule that reaches `secrets` (resources `secrets` or `*` in API group `""` or `*`), or reaches `roles` or `rolebindings` in `rbac.authorization.k8s.io` or `*`, which could grant Secrets later.
8. It is a RoleBinding whose `roleRef` is not a Role, such as a ClusterRole the connector cannot read to check, or a RoleBinding to a Role refused by the rule above, whether that Role is in the same manifest or already exists in the namespace.
9. An object of the same kind and name already exists and carries `curietech.ai/e2e-build`, such as a build Job or pod.

A Role that grants only other resources, such as `configmaps`, and a Secret with any other name, deploy normally.

### run

Arguments:

1. `command`, required. 1 to 64 strings of at most 4096 characters each. It is the container's argv, so use `["sh", "-c", "..."]` for a shell line.
2. `image`, required. `<ref>@sha256:<64 hex>`.

The connector creates the Job `e2e-run-<id>` in the run's namespace with the run labels, `backoffLimit: 0`, `activeDeadlineSeconds: 1200`, and `ttlSecondsAfterFinished: 600`. Its pod sets `restartPolicy: Never`, `automountServiceAccountToken: false`, and `enableServiceLinks: false`. The single container `run` sets `allowPrivilegeEscalation: false` and the RuntimeDefault seccomp profile, plus `runAsNonRoot` and dropping all capabilities when the installation enforces `restricted`. The connector polls the Job every 5 seconds and deletes it, with Background propagation, on every exit path.

Result:

```json
{"exit_code": 0, "stdout": "...", "stderr": ""}
```

Kubernetes merges a container's stdout and stderr into one log. `stdout` carries that log, its last 64 KiB. `stderr` carries the container's termination message, or its termination reason (such as `OOMKilled`) when the exit is non zero and there is no message. A non zero exit is a result, not a refusal.

Refusals:

1. `e2e_run_identity_required`, `e2e_environment_required`, `e2e_namespace_not_owned`: as for `deploy`.
2. `e2e_image_not_digest`: `image` is not pinned by digest. Nothing is created.
3. `e2e_run_argument_refused: <reason>`: `command` breaks the rules above. Nothing is created.
4. `e2e_run_failed: <reason>`: the container waits on `InvalidImageName`, `ErrImageNeverPull`, or `ImagePullBackOff`, or the Job failed without a container exit. `ContainerCreating`, `PodInitializing`, and `ErrImagePull` keep waiting; the kubelet turns a pull failure that persists into `ImagePullBackOff`.
5. `e2e_run_timeout`: the run passed 1200 seconds.

### logs

Arguments:

1. `pod`, required. A DNS subdomain name.
2. `container`, optional. A DNS label, for a pod with more than one container.
3. `tail`, optional. Lines from the end, 1 to 5000 (default 500).

Result: `{"logs": "..."}`, at most the last 64 KiB.

Refusals:

1. `e2e_run_identity_required`, `e2e_environment_required`, `e2e_namespace_not_owned`: as for `deploy`.
2. `e2e_pod_not_found`: no such pod in the run's namespace, or the name is not a DNS subdomain.
3. `e2e_logs_refused`: the pod carries `curietech.ai/e2e-build`. Build output would bypass `image_build`'s credential redaction, so `image_build` reports build failures itself.

### events

Argument: `involved`, optional. An object name; only events whose `involvedObject.name` matches are returned.

Result:

```json
{"events": [{"reason": "BackOff", "message": "...", "type": "Warning", "count": 4}]}
```

Events are core v1 Events in the run's namespace, oldest first by `lastTimestamp`, `eventTime`, or creation time, at most the last 200. `count` is the event's `count`, else `series.count`, else 1. Refusals are those of `deploy` in item 1.

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
