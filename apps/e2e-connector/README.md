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
```

The image value is a sentinel. The factory release substitutes the worker image, which contains `python -m curie_e2e_connector`. The release must set `e2eConnector.enabled` plus the test cluster service account, its namespace, the worker ClusterRole name, and the owner label value installed by `curie cluster up --e2e-connector-identity` on that test cluster.

## Caller identity

`env_create` and `env_destroy` read `X-Curie-Run` and `X-Curie-Work-Item`. The caller proxy sets those headers from the signed caller token. A call with no pair is refused with `e2e_run_identity_required` and does not touch the test cluster.

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

## Names reserved for later issues

These tools are not served yet. Their names and result fields stay as written here.

1. `image_build` arguments `context` (required), `dockerfile` (default `Dockerfile`), `platforms` (optional). Result `{"images": [{"name": "<name>", "digest": "sha256:<hex>"}]}`.
2. `deploy` argument `manifests` (YAML). Result `{"namespace": "<name>", "applied": [{"kind": "<kind>", "name": "<name>"}]}`. A tag reference returns `e2e_image_not_digest`. A cluster scoped object returns `e2e_cluster_scoped_object` and applies nothing.
3. `run` arguments `command` (string array) and `image` (digest). Result `{"exit_code": 0, "stdout": "", "stderr": ""}`.
4. `logs` arguments `pod`, optional `container`, optional `tail`. Result `{"logs": ""}`.
5. `events` optional argument `involved`. Result `{"events": [{"reason": "", "message": "", "type": "", "count": 1}]}`.
