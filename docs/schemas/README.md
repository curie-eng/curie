# Factory tool contracts

These schemas define the artifact contracts from ADRs 0216 and 0217.
The pure models in `apps/api/src/curie_api/forges/runner_config.py` are the
source for `tool-manifest.schema.json` and `tool-catalog-entry.schema.json`.
Export them with `manifest_schema()` and `catalog_entry_schema()`, then write
JSON with sorted keys, two-space indentation and a trailing newline.

The CLI produces `/etc/curie/tool-manifest.json` during a repository runner <!-- doclint:ignore-line -->
image build, and the runner will read it from that image. Catalog entries declare pinned
platform source images, cache paths under the writable `/home/runner`, registry
hosts and version probe argv. Catalog v1 lives in `cli/src/tool_catalog_v1.json`,
and `curie build --repo-runner` builds an image from it, runs the probes and
writes the manifest.

Repository runner declarations require an index image pin, a platform runner
base digest and both `linux/amd64` and `linux/arm64` digests. Image pins accept
registry ports and an optional tag before `@sha256:`, while base and platform
identities are bare SHA256 digests. Future image admission compares the SHA256
component. Repeated bindings for one resolved host kind, host and project id
must declare identical runner and effective Docker settings. Missing Docker
settings mean disabled Docker with no egress. GitHub paths are compared without
case for activated declarations; other forge paths retain case and namespaces.

The worker owns the `CURIE_REPO_RUNNER_POOLS` contract in
`apps/worker/schema/repo-runner-pools.schema.json`, exported by
`build_schema()` and `render_schema()` in
`apps/worker/src/curie_worker/repo_runner_pools.py`. It maps each bound repository
path to one opaque Kubernetes pool name, three digests in index, amd64, arm64
order, the base digest, the Docker flag and optional egress. Rendering the
worker environment, naming pools per agent and digest, selecting a pool and
starting a Docker daemon are later work. No worker environment reader or
runtime consumer is added by these schemas.
