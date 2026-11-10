# Adapter credential map reconciliation

This opt-in example repairs the worker's copy of existing adapter reply
credentials. It uses the existing `CURIE_ADAPTER_CREDENTIALS` JSON map and
Kubernetes Secret references. It adds no platform authorization contract.

The program never generates credentials, mints `chn` or `adp` tokens, renews
tokens, or revokes principals. Adapters renew their own channel tokens. Immediate
principal revocation remains tracked by
[issue #3841](https://github.com/curie-eng/curie/issues/3841) and awaits a human
ruling; already issued tokens remain valid until expiry under the existing
contract. Changing both halves of a credential pair as one transaction is
outside this example.

## Configuration and command

`observability/adapter-credentials/sync.py` is a standalone standard-library
Python program. Run it inside Kubernetes using a ServiceAccount token and CA
from `/var/run/secrets/kubernetes.io/serviceaccount`. An optional `SA_DIR`
selects the mounted ServiceAccount directory. The API endpoint is
`https://kubernetes.default.svc`; every request has a finite timeout.
Redirects are refused so the ServiceAccount bearer token stays on that
endpoint.

```json
{
  "namespace": "acme-system",
  "workerDeployment": "acme-worker",
  "workerContainer": "worker",
  "adapters": {
    "acme-mail": {"sourceSecret": "acme-mail-source", "sourceKey": "replySecret"},
    "acme-chat": {"sourceSecret": "acme-chat-source", "sourceKey": "replySecret"}
  }
}
```

```sh
python3 examples/sre-bot/observability/adapter-credentials/sync.py --config /config/adapters.json --dry-run
python3 examples/sre-bot/observability/adapter-credentials/sync.py --config /config/adapters.json
python3 examples/sre-bot/observability/adapter-credentials/sync.py --config /config/adapters.json --prune --dry-run
python3 examples/sre-bot/observability/adapter-credentials/sync.py --config /config/adapters.json --repair-rollout
```

`--prune` and `--repair-rollout` are separate opt-ins (SRE-CREDS-9 and
SRE-CREDS-10). Without them the program keeps its default refusal and
no-rollout behavior. Either combines with `--dry-run`.

The namespace, worker Deployment, container, and every adapter source are
explicit. This file contains names and keys only, never credential values.
Installation tooling may mount it from a ConfigMap. The program does not create
or modify that ConfigMap, install a schedule, or grant itself Kubernetes access.
Operators grant `get` on the named source Secrets, `get` and `patch` on the
worker's referenced Secret, and `get` and `patch` on the named worker Deployment.
Dry run needs only `get` and performs no live changes.

## Contract

### SRE-CREDS-1: explicit, valid inventory

Require a readable JSON object containing exactly the configuration fields
above, a nonempty adapter map, unique JSON object keys, valid Kubernetes
namespace and resource names, a nonempty worker container name, and nonempty
adapter identities and Secret data keys. Each adapter entry contains exactly
`sourceSecret` and `sourceKey`. Reject missing, unknown, duplicate, blank, or
wrongly typed fields before making a Kubernetes request. Adapter identities
are opaque JSON map keys; no provider or mail-address convention is imposed.

### SRE-CREDS-2: discover and validate the actual worker target

Read the named Deployment and select exactly one container with the configured
name. That container must have exactly one `CURIE_ADAPTER_CREDENTIALS` entry,
using a non-optional `valueFrom.secretKeyRef` with valid `name` and `key`, and
no literal value or other value source. Read only that target Secret. Never
guess a default target, search other containers, or patch an additional copy.
Both resource responses must have matching names and namespace and nonempty
`metadata.resourceVersion`. The target must be mutable. Missing or malformed
worker, reference, or target responses abort before any PATCH.

### SRE-CREDS-3: complete source map or no writes

Read every configured source Secret and its configured data key. Require a
matching Secret identity, a strictly valid base64 string decoding to nonblank
UTF-8 text, and preserve the decoded credential verbatim. Missing Secrets,
missing keys, blank values, invalid base64, invalid UTF-8, and request failures
abort the entire reconciliation before any PATCH. No partial map is written.

### SRE-CREDS-4: preserve existing adapter identities

Decode the worker target key as a JSON object of nonempty identity keys and
nonblank string values. An absent key is an empty map; a present malformed
value is an error. If an existing identity is absent from the configured
inventory, abort without writing unless the operator passed `--prune`
(SRE-CREDS-9). Without that flag, retirement stays a separate deliberate
operation. Equality compares decoded maps, so JSON formatting or order alone
never triggers a change.

### SRE-CREDS-5: conditional update of one key

Finish all inventory, worker, target, and source validation before any PATCH.
If the decoded desired map equals the target map, return `unchanged` without
PATCH. Otherwise PATCH only the target key, retaining every unrelated Secret
key. Include its observed `metadata.resourceVersion` in the JSON merge patch
(`application/merge-patch+json`). A stale version or any Secret write failure
aborts without a Deployment PATCH or a blind retry. Kubernetes documents
conditional PATCH and stale-version rejection in
[API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/).

### SRE-CREDS-6: rollout follows a successful map change

After a successful target PATCH, PATCH only two worker pod template
annotations, using the Deployment's observed resource version:
`curietech.ai/adapter-credentials-at`, with a new UTC timestamp, and
`curietech.ai/adapter-credentials-sha256`, the rollout marker defined in
SRE-CREDS-10 for the map just written. Preserve other annotations and
Deployment fields. A failed target write or dry run never rolls the worker.
An unchanged map rolls the worker only under SRE-CREDS-10. A Deployment write
failure returns failure with `secretPatched: true` and `workerRolled: false`.
This is a partial operation, not a successful rollout or an atomic
transaction. Without `--repair-rollout`, a later unchanged-map reconciliation
does not repair that partial rollout, and the operator must complete it
separately before claiming runtime recovery.

### SRE-CREDS-7: read-only dry run and safe structured results

`--dry-run` performs the same complete reads and validation as an apply run,
then returns `would-change`, `would-repair`, or `unchanged` with no PATCH.
The ordinary command returns `updated`, `repaired`, `unchanged`, or `failed`.
Emit one JSON result on stdout and no diagnostic body on stderr. Results may
contain only status, namespace, worker Deployment and container names, target
Secret name and key, adapter count, booleans `changed`, `secretPatched`,
`workerRolled`, the sorted `retired` identity list defined in SRE-CREDS-9, and
a stable failure code. Adapter identities are names from the inventory and
the target map keys, never credential values. Never print Secret values, encoded values, request or response
bodies, raw exceptions, ServiceAccount tokens, or tracebacks. Exit zero for a
successful apply or dry run and nonzero for every failure. Failure codes
distinguish configuration, worker inspection, target inspection, source
inspection, inventory shrinkage, Secret write, and worker rollout failures.

### SRE-CREDS-8: bounded public verification

Focused behavioral tests exercise the real program through
`main(argv, *, request=None) -> int`, capturing its JSON output. An injected
`request(method, path, body=None)` substitutes only the external Kubernetes
boundary; PATCH bodies are JSON merge patches. Production uses the
ServiceAccount transport when `request` is absent. Tests use synthetic
credentials and obvious placeholder names, and assert complete reads before
writes, no-write refusal paths, semantic equality, preserved unrelated data,
conditional version conflicts, rollout failures, explicit retirement, and
rollout marker repair.

Before describing runtime reconciliation as verified, run the standalone
program with synthetic Secrets and a disposable worker in an isolated
Kubernetes namespace. Demonstrate read-only dry run under read-only RBAC,
actual Secret conflict rejection, unchanged-map no rollout, changed-map pod
template advancement, preservation of unrelated keys, and a missing-source
run that changes nothing. For the opt-ins, also demonstrate that a retired
identity is refused by default and removed with `--prune`, and that after an
injected worker PATCH denial, an unchanged-map `--repair-rollout` run advances
the pod template while a second run is `unchanged`. Record Kubernetes version, exact commands, candidate
commit, observed results, and owned-resource cleanup. Fake-boundary tests
prove program decisions only; production credential reads and production
mutations are not a test harness.

### SRE-CREDS-9: explicit opt-in retirement

`--prune` permits removing target identities that the configured inventory no
longer lists. It removes exactly those identities: the desired map is the
complete configured inventory, and every retained or added identity still
comes from a validated source under SRE-CREDS-3. All validation in
SRE-CREDS-1 through SRE-CREDS-5 still completes before any PATCH, and a
nonempty inventory remains required, so pruning can never empty the map.
Every result after the target map is decoded under `--prune`, including
`unchanged`, `would-change`, `updated`, and later write failures, reports
`retired`: the sorted list of removed identities, possibly empty. Dry run
reports what would be retired and writes nothing. Without `--prune` the result
has no `retired` field and shrinkage still fails with `inventory-shrinkage`.
Retirement removes only the worker's copy of a credential. It does not revoke
the adapter principal, delete source Secrets, or invalidate issued tokens.

### SRE-CREDS-10: rollout marker and opt-in repair

The rollout marker is the lowercase hexadecimal SHA-256 digest of the UTF-8
bytes `curie-adapter-credentials-v1`, one NUL byte, and the desired map
serialized as JSON with sorted keys, `(",", ":")` separators, and non-ASCII
characters escaped. It depends only on the decoded map, so formatting and key
order never change it. Every successful rollout under SRE-CREDS-6 writes it to
`curietech.ai/adapter-credentials-sha256`. The marker is a fingerprint of
credential material visible to anyone who can read the Deployment, as with the
common checksum annotation pattern. It assumes high-entropy credentials and is
never printed.

When the decoded map is unchanged and `--repair-rollout` is set, read the
marker from the worker's pod template annotations. If it equals the marker of
the current map, return `unchanged`. If it is absent, malformed, or different,
the worker template does not reflect the current map: dry run returns
`would-repair` with no PATCH, and an apply run PATCHes the worker exactly as
SRE-CREDS-6 does, without any Secret PATCH, and returns `repaired` with
`changed: false`, `secretPatched: false`, and `workerRolled: true`. A worker
PATCH failure returns `worker-rollout`. A worker last rolled before this
marker existed is rolled once by its first repair run. Without
`--repair-rollout`, an unchanged map never rolls the worker. The marker shows
that the pod template advanced for this map; it does not prove that the new
pods became ready or that the worker delivered a reply.

## Existing interface observation

At source revision `b84c1bcd1`, the worker chart renders
`CURIE_ADAPTER_CREDENTIALS` from a `secretKeyRef`, including the separately
configured existing-Secret key. The worker's credential parser consumes a
JSON object and refuses malformed JSON. These are the existing interfaces this
example reconciles; no worker, provider, sender, or authorization gate changes
are required.

## Build order

Land this contract first. The retirement and rollout repair opt-ins followed
the same order: contract update, failing behavioral tests, implementation. Commit the failing behavioral tests separately,
then implement the standalone reconciler in a later commit. Required runtime
proof is the isolated Kubernetes exercise above. Skill, local bundle,
local-release, live-model, and external messaging tiers do not exercise this
standalone Kubernetes map reconciler and are not applicable. The example adds
no guarantee of alarm delivery, principal revocation, or complete SRE recovery.
