# 216. A factory repository binding selects its own runner image, built from a tool catalog

Date: 2026-10-12

Status: Accepted

Accepted 2026-10-12 with explicit maintainer approval from Brian Conn,
given in the design review that produced it, before implementation.

This ADR partially amends [ADR 0173](0173-a-bundle-may-layer-its-own-runner-image.md)
decision 3 ("Only that agent runs it"): a repository binding may now select a
runner image that replaces the agent's for that repository's runs. It realizes
decision 3 of [ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
(the runner capability manifest) for executables only, without the admission
gate. It builds on [ADR 0214](0214-one-factory-agent-serves-every-github-repository-bound-to-it.md)
and [ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md),
whose typed bindings carry per repository `bases` and `ci`. Everything else in
those ADRs stands.

## Context

One factory agent serves every repository bound to it (ADR 0214), and its
sandboxes all run one image: the platform runner, or the agent's layered image
from ADR 0173. A repository whose checks need a tool that image lacks cannot be
finished by the factory. A run on 2026-10-11 stopped for exactly that reason:
its repository's end to end ladder needed a container runtime.

Three places describe what the runner holds, and they drift:

1. the image itself (`examples/dark-factory/runner.Dockerfile`);
2. a hand-written list in `cli/src/factory_toolchain.rs`;
3. fixed prompt and skill text, which today mentions only Python.

The model learns its tools from (3), which is wrong for any image but the
platform runner.

Comparable products declare tools per repository and cache the result as a
snapshot. None gives the agent a machine-checked list of what exists; agents
learn from prose or by probing.

The install operator already owns the runner image: it is built by `curie
build`, pinned by digest, rendered only from Helm values, and the runner
admission policy refuses any image the chart did not render. A repository file
read at run time cannot change the image, and the factory skill already forbids
repository files from authorizing installs or egress.

## Decision

**The install operator gives each bound repository its own runner image, built
by `curie build` from a list of catalog tools plus an optional Dockerfile.
`curie build` probes the built image and writes a tool manifest into it. The
runner shows the model that manifest. A repository with no image of its own
keeps the agent's image.**

1. **Declared on the binding, by the operator.** A repository entry under
   `factory.bindings[].repos[]` may carry a `runner` block: a `tools` list, an
   optional `dockerfile`, a `probes` list naming each executable the
   Dockerfile adds with its version command, and the resulting image digest. The target
   repository carries no tool file; `.curie/verification.json` keeps naming its
   checks only.
2. **Built from a catalog.** Curie ships a catalog of pinned, multi-arch tool
   layers. The first entries are what the dark factory image already carries
   (uv, a Rust toolchain with rustfmt and clippy, pnpm, build-essential) plus
   the Docker CLI with the compose plugin. Each entry records its source digest,
   its cache paths under the writable home, its version probe, and the
   registry hosts it needs. An optional Dockerfile layers anything the catalog
   lacks.
3. **On the platform runner.** A repository image is built `FROM` the
   installation's platform runner, not on an agent's layer, so the ADR 0173
   base check stays one level. Bundle behavior set in a runner Dockerfile today
   moves to bundle or agent configuration.
4. **Probed, and the manifest travels in the image.** After the build, `curie
   build` runs each catalog entry's version probe, and each entry in the
   binding's `probes` list, inside the built image. It fails if any probe
   fails. It writes the results (name, version, path) to a manifest file in a
   final layer at a fixed path. The manifest cannot claim a tool the image
   lacks. Repository checks are not probed at build time. They can name
   scripts in the checkout, which the image does not contain. The runner's
   verification preflight still runs them against the workspace.
5. **Rendered per agent and digest.** The chart renders one SandboxTemplate and
   pool per (factory agent, distinct repository digest), prewarms every digest,
   and adds every digest to the runner admission policy's allowed images.
   Repositories sharing a toolset share a template. The binding records the
   image's index digest, the platform runner digest it was built on, and the
   per-platform manifest digests. Identity checks accept any of them.
6. **Routed by repository, failing closed.** The worker picks the template by
   the run's repository. It refuses a run:
   1. when the repository's binding names an image that has no rendered pool;
   2. when the recorded base is not the installation's platform runner.

   Either refusal names the reason. Neither run is ever served the agent's
   image. A retained sandbox is reused only when its repository, image digest
   and Docker setting all still match the run. Otherwise the worker retires
   it and claims a fresh one.
7. **The model sees the manifest.** The runner reads the manifest at boot and
   adds it to the prompt as declared data, beside the verification preflight
   block. The hand-written list in `factory_toolchain.rs` is removed, and the
   skill describes tools generically.
8. **Upgrades rebuild catalog images.** `curie cluster upgrade` rebuilds and
   pushes, on the new platform runner, each repository image made only from
   catalog entries. An image with a custom Dockerfile is named, and its
   repository's runs are refused with the rebuild command until the operator
   rebuilds it. No repository silently falls back to an image without its
   tools.

## Consequences

1. Different repositories on one agent can have different toolchains without a
   second agent, bundle copy, or budget.
2. Node disk grows with each distinct image. Measured on v0.12.4: the platform
   runner is about 0.67 GiB uncompressed and a Rust-class toolset adds about 1
   GiB. An upgrade briefly holds old and new copies.
3. Per repository images run only on the Kubernetes substrate. The local
   Docker substrate runs one image for every agent, as it does for ADR 0173
   images today.
4. Curie maintains the catalog: pinned digests, version bumps, and release CI
   for every entry.
5. The manifest makes the ADR 0199 executable list real. The 0199 admission
   gate and the ADR 0204 environment checks can read it later. This ADR does
   not build them.

## Alternatives considered

### A second factory agent per toolset

Rejected. It works today (`curie factory quickstart --agent`), but it is
alternative 1 of ADR 0214: a bundle copy, budget, egress policy and agent record
per toolset, and every copy drifts on skill changes.

### A repository file that selects tools

Rejected. The image is an operator artifact pinned in Helm values and enforced
by admission policy. A file read at run time cannot change it. Giving
repository writers a say over the sandbox would widen it beyond what the
operator approved.

### A per repository Dockerfile only, no catalog

Rejected as the only input. It needs no catalog upkeep, but an upgrade could not
rebuild such images on its own, and every operator would rewrite the same
toolchain layers.

### Install tools when the sandbox starts

Rejected, as in ADR 0173. The root filesystem is read only, and a setup step
that fails partway leaves the agent working in a half-installed tree. That is
the main failure mode reported for products that do this.

## Implementation

Acceptance authorizes implementation in follow-up pull requests into `next`.
No code realizes this decision yet.
