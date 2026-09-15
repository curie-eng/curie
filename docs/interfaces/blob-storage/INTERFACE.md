---
seam: Blob storage (S3/RustFS)
kind: CLEAN
impls: 1 backend (S3/RustFS) behind the ObjectStore port
grade: B+
vision_row: Blob storage
epics:
  - "#83"
order: 9
---

# INTERFACE: Blob storage (S3/RustFS)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 backend (S3/RustFS) behind the ObjectStore port &nbsp;·&nbsp; **Swap-readiness grade:** B+
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

Immutable plugin bundles are addressed by a deterministic `(agent, version)` key in
an object store, behind the **`ObjectStore` port** (`apps/api/src/curie_api/storage.py`,
#282 / ADR-0026): `ensure_bucket` / `exists` / `put` / `get`, with the
write-once/no-mutation key discipline promoted from convention **into the port's
contract**. The one backing today is S3/RustFS (`BundleStore`); a future non-S3
backend (GCS-native, Azure Blob) is a drop-in that satisfies the Protocol. The
GCS/Azure adapter itself is deliberately **not built** — it stays gated on a real
non-S3 customer (ADR-0007), so only the *port* is extracted now, not a speculative
second implementation.

## Current contract

The **`ObjectStore` port itself does not require S3**: its docstring
(`apps/api/src/curie_api/storage.py::ObjectStore`) states a second backend (GCS-native,
Azure Blob) satisfies the Protocol without being boto3/S3. What speaks boto3 S3 is the one
backing today, and a config-only swap that stays *within* the S3-compatible family (AWS S3 or
Cloudflare R2) needs no code, only env/settings:

- **Env/settings** (`apps/api/src/curie_api/config.py::Settings`, mirrored field for
  field by `apps/worker/src/curie_worker/config.py::WorkerConfig`): `s3_endpoint_url`,
  `s3_access_key`, `s3_secret_key`, `s3_region`, `bundle_bucket` (env vars
  `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `BUNDLE_BUCKET`).
  The two credential fields default to the **empty string** on both sides; see
  credential resolution below for why that default is load-bearing rather than a
  placeholder. The dev static credential lives in `.env.example` and `compose.dev.yaml`.
- **Client construction** (`packages/aci-protocol/src/aci_protocol/s3.py::build_s3_client`,
  #501): the single `boto3.client("s3", endpoint_url=..., config=BotoConfig(s3={"addressing_style": "path"}))`
  in the repo. It takes primitives, not either app's config object, so it couples to
  neither `Settings` nor `WorkerConfig`. Both lanes import it: the API through a thin
  `Settings`-shaped adapter (`apps/api/src/curie_api/storage.py::build_s3_client`,
  kept as a named function because callers import it from that module), the worker
  directly (`apps/worker/src/curie_worker/bundle_store.py::BundleStore`).
- **Credential resolution is part of the contract** (#1325, #1559). An empty
  `access_key`/`secret_key` is not a blank credential, it is the request to resolve an
  identity through the AWS provider chain, and `build_s3_client` is what turns that
  emptiness into `None`, boto3's documented provider-chain signal. An empty *string* is
  an explicit credential to botocore: it stops at the first resolver and signs with an
  empty identity, so passing it straight through made the key-free path inert. Both
  halves are load bearing, which is why the config defaults are empty as well: a non-empty
  default would arrive at the builder as a real credential and the chain would never be
  consulted. Half a pair (one value set, the other empty) still raises
  `PartialCredentialsError` at construction rather than quietly resolving an ambient
  identity.
- **Operations used** (`apps/api/src/curie_api/storage.py::BundleStore`): `head_bucket`, `create_bucket`,
  `head_object`, `put_object` (with `Body`, `ContentType`), `get_object` (reads
  `obj["Body"].read()`). The current S3 backing uses exactly these five calls, path-style;
  a non-S3 backend instead honors the port's four-method contract (`ensure_bucket` /
  `exists` / `put` / `get`), not these wire calls.

## Implementations today

One backend (S3/RustFS) behind the port, reached by one boto3 builder and three
AWS CLI shell sites:

- **`ObjectStore` port** — `apps/api/src/curie_api/storage.py` (`Protocol`: the
  four ops `ensure_bucket` / `exists` / `put` / `get` + the write-once contract). Consumers (`deps`/`gitflow`/`deploy`) type
  against it, so a second backend is a drop-in.
- **API writer** — `apps/api/src/curie_api/storage.py::BundleStore`, the S3/RustFS backing
  (async-offloaded boto3); client built by the shared `build_s3_client` factory.
- **Worker reader** — `apps/worker/src/curie_worker/bundle_store.py::BundleReader`: a local `BundleReader`
  Protocol (the read-only slice of the port; the worker does not import the API
package) with `BundleStore` as its S3/RustFS backing.
- **Chart bundle-fetch init** — `charts/curie/templates/agent-sandbox.yaml`
  uses the AWS CLI (`aws s3 cp` after `aws configure set default.s3.addressing_style path`),
  a second dialect of the same S3 protocol. It has its own key-free branch, gated on
  the same `curie.rustfs.staticCredentials` helper (`charts/curie/templates/_helpers.tpl`)
  the api and worker Deployments use.
- **Chart bucket bootstrap** — `charts/curie/templates/rustfs.yaml`, a post-install
  hook Job running `aws s3api head-bucket`/`create-bucket` against the in-chart store.
  Renders only under `rustfs.deploy: true`, which is the static-credential case by
  construction, so the key-free path never reaches it.
- **Operator store migration** — `curie cluster migrate-store`
  (`cli/src/migrate_store.rs`, its export/import/listing command builders):
  `aws s3 sync`/`mb`/`ls` run by `kubectl exec` inside a staging pod, deliberately
  with no S3 client in the CLI itself. Also in-cluster-store only (it addresses the
  chart's own `minio`/`rustfs` StatefulSet and reads that store's password from the
  release Secret), so it likewise never sees the key-free path.
- **Worker workspace/attachment store** — `WorkspaceObjectPort`
  (`apps/worker/src/curie_worker/workspace.py::WorkspaceObjectPort`;
  `put_stream` / `get_stream` / `presign_get` / `delete` / `list_keys`), backed
  by `WorkspaceObjectStore`
  (`apps/worker/src/curie_worker/workspace.py::WorkspaceObjectStore`) and built
  in `apps/worker/src/curie_worker/run.py::build`. The inbound-attachment lane
  reuses this same port
  (`apps/worker/src/curie_worker/attachments.py::AttachmentCoordinator`).
- **Chart attachments-init** — `charts/curie/templates/agent-sandbox.yaml`
  fetches each parked object through a presigned URL from
  `apps/worker/src/curie_worker/workspace.py::WorkspaceObjectStore.presign_get`,
  not through the AWS CLI bundle-fetch path.

## Known leakage

**Client construction is no longer a leak.** The two hand-aligned boto3 sites (API
and worker, plus a worker eval fixture) collapsed into one shared builder in #501:
`packages/aci-protocol/src/aci_protocol/s3.py::build_s3_client` is the only
`boto3.client("s3", ...)` call in the repo, and every Python consumer imports it. A
second, non-S3 backend has one Python construction site to displace, not three.

**The AWS CLI is the remaining dialect, in three shell sites.** The sandbox
bundle-fetch init (`charts/curie/templates/agent-sandbox.yaml`), the in-chart bucket
bootstrap Job (`charts/curie/templates/rustfs.yaml`), and the operator migration verb
(`cli/src/migrate_store.rs`) each speak S3 as `aws` shell invocations that no Python
port can cover. Only the first of the three is on the hot path of a BYO store: the
other two address the chart's own in-cluster store, which cannot be swapped without
replacing those templates anyway. So a non-S3 backend breaks the bundle-fetch init
container, and `curie cluster migrate-store` is an S3-only operator path that has no
meaning against it at all. Unifying that path is still deferred until a real non-S3
backend lands (ADR-0007, ADR-0026); building the adapter first is out of scope.

**Key-free auth is contract the Protocol does not carry** (#1325, #1559). The
"empty credential means resolve an ambient identity" rule lives in construction and
config, not in `ObjectStore`, which has no credential surface at all. A second backend
therefore inherits the obligation without the port stating it: an operator who clears
`rustfs.auth.accessKey` expects the chart to omit every credential env var and the
backend to fall through to a mounted web-identity token, not to run unauthenticated.
The chart enforces the omission through one helper
(`charts/curie/templates/_helpers.tpl`'s `curie.rustfs.staticCredentials`), which also
refuses the clear-the-key-while-`rustfs.deploy`-is-true combination at render, since
the in-chart RustFS has no web-identity path. The Python side then has to translate
that omission into `None` rather than an empty string, because botocore treats the
empty string as an explicit credential. Both halves are behavior a second backend must
reproduce, and neither is expressible in the four-method Protocol. See
`charts/curie/README.md` ("Key-free object store auth") for the operator-facing shape,
including why the instance role and IMDS are deliberately unavailable.

**Workspace and attachment operations are a third adapter surface.**
`WorkspaceObjectPort` is not a slice of `ObjectStore`: a non-S3 backend must also
provide streaming put/get, delete, list, and presigned reads
(`apps/worker/src/curie_worker/workspace.py::WorkspaceObjectStore.presign_get`).
The Kubernetes `attachments-init` container (`charts/curie/templates/agent-sandbox.yaml`)
redeems those URLs, so a backend that cannot mint a presigned GET also breaks that path.

A second non-S3 backend is **three adapter surfaces, not one**: the API owns the full **async**
`ObjectStore` port (`apps/api/src/curie_api/storage.py::ObjectStore`, `async` methods),
the worker reads bundles through a separate **sync** `BundleReader` slice
(`apps/worker/src/curie_worker/bundle_store.py::BundleReader`, a plain `get`) because it
deliberately does not import the API package, and the worker also talks to the same
store through `WorkspaceObjectPort`
(`apps/worker/src/curie_worker/workspace.py::WorkspaceObjectPort`). A GCS/Azure backend
must therefore supply the async port, the sync bundle reader, and a
streaming/presign/delete/list implementation.

## Cross-links

- **Epic(s):** #83 — vision epic for the blob-storage seam (extract a port only when a non-S3 backend lands).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 4 (Blob storage), grade B+
- **ADR(s):** [ADR-0007](../../adr/0007-adopt-not-build-boundaries.md) — Adopt-not-build boundaries (RustFS adopted under Apache 2.0, "offer BYO-S3")
