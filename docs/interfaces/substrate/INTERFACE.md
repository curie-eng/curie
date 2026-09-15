---
seam: Substrate / SandboxClient
kind: CLEAN
impls: 2 (k8s, docker)
grade: not separately graded
epics:
  - "#86"
  - "#44"
order: 1
---
# INTERFACE: Substrate / SandboxClient

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 2 (k8s, docker) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The substrate is where a conversation thread claims, dials, suspends, and reaps an isolated runner runtime. `SandboxSubstrate` composes the port; everything Kubernetes-shaped (or Docker-shaped) lives behind the `SandboxClient` `Protocol`. The kernel talks in `thread_key` and receives a `SandboxHandle` with a dial target — it never touches a cluster or a container runtime directly. The swap axis is which runtime backs a claim (k8s CRDs vs local Docker containers); the routing, affinity, and rehydrate logic above the line stay opinionated core.

## Current contract

A second implementation must satisfy the `SandboxClient` `Protocol` at `apps/worker/src/curie_worker/sandbox/types.py::SandboxClient`, six methods:

- `create_claim(name, *, pool, env=None, labels=None) -> None` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.create_claim`)
- `get_claim(name) -> ClaimView | None` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.get_claim`)
- `delete_claim(name) -> None` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.delete_claim`)
- `list_claims(*, label_selector) -> list[ClaimView]` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.list_claims`)
- `get_sandbox(name) -> SandboxView | None` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.get_sandbox`)
- `set_sandbox_mode(name, mode: OperatingMode) -> None` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient.set_sandbox_mode`)

The exchanged value types are `ClaimView` (`apps/worker/src/curie_worker/sandbox/types.py::ClaimView`: `name`, `ready`, `sandbox_name`, `created_at`, `quota_rejection`, `ready_reason`, `ready_message`) and `SandboxView` (`apps/worker/src/curie_worker/sandbox/types.py::SandboxView`: `name`, `ready`, `service_fqdn`, `operating_mode`, `port`). Kubernetes must populate `quota_rejection`, `ready_reason`, and `ready_message` from quota rejection and raw Ready condition evidence. Docker must supply `None` for those diagnostic fields. `operating_mode` must report `"Running"` for a claim to be handed back (`apps/worker/src/curie_worker/sandbox/substrate.py::SandboxSubstrate.claim`), and `OperatingMode` is `Literal["Running", "Suspended"]` (`apps/worker/src/curie_worker/sandbox/types.py::OperatingMode`). A second implementation **must** populate `ClaimView.created_at` from the backing object's own creation instant, as tz-aware UTC: `reap_orphans()` (`apps/worker/src/curie_worker/sandbox/substrate.py::SandboxSubstrate.reap_orphans`) reads it as the bind-window grace, sparing any claim younger than `claim_timeout_seconds` plus a fixed margin, because a claim still binding has no route yet and "no live route" alone would read as litter. An adapter that returns `None` for every claim disables orphan reaping on that tier entirely (the substrate spares unknown-age claims and logs a warning), so `None` is reserved for a genuinely unreadable timestamp. The selector reads `CURIE_SANDBOX_SUBSTRATE` (default `"kubernetes"`, else `"docker"`) in `_sandbox_client()` at `apps/worker/src/curie_worker/run.py::_sandbox_client`, which branches on the value inside that same function.

## Implementations today

Two, both under `apps/worker/src/curie_worker/sandbox/`:

- `KubernetesSandboxClient` (`apps/worker/src/curie_worker/sandbox/k8s.py::KubernetesSandboxClient`) — drives agent-sandbox CRDs (`Sandbox` in `agents.x-k8s.io`, `SandboxClaim` in `extensions.agents.x-k8s.io`) via `CustomObjectsApi`.
- `DockerSandboxClient` (`apps/worker/src/curie_worker/sandbox/docker.py::DockerSandboxClient`) — boots runner containers on the local Docker daemon for middle mode (a laptop, no cluster).

## Known leakage

The `SandboxView.port` field (`apps/worker/src/curie_worker/sandbox/types.py::SandboxView`) exists only because the Docker path publishes each runner on its own loopback host port, while the Kubernetes path uses one fleet-wide `runner_port`; `None` means "fall back to `SubstrateConfig.runner_port`". Credential handling also differs across the line: the k8s client strips `CURIE_CREDENTIALS` (`apps/worker/src/curie_worker/sandbox/k8s.py::CREDENTIALS_ENV`) from per-claim env so it is never persisted in plaintext on the claim (`apps/worker/src/curie_worker/sandbox/k8s.py::KubernetesSandboxClient.create_claim`), relying on the chart Secret's `secretKeyRef`; the Docker client has no Secret and forwards at most one model credential by name (`apps/worker/src/curie_worker/sandbox/docker.py::DockerSandboxClient.create_claim`), across four states: nothing at all for a fake-model run, which authenticates against nothing; an explicit non-empty CURIE_CREDENTIALS alone (never an ambient CLAUDE_CODE_OAUTH_TOKEN/ANTHROPIC_API_KEY that would shadow it), kept under a base-URL override only when it is *not* OAuth-shaped, since the runner routes an `sk-or-` OpenRouter key into ANTHROPIC_API_KEY behind a preset base URL but blanks a `sk-ant-oat` Claude Code token there (`runner/src/curie_runner/sdk_auth.py::OAUTH_TOKEN_PREFIX`, `runner/src/curie_runner/sdk_auth.py::_is_forwardable_provider_credential`); an OAuth-shaped explicit credential under that override, which is dropped entirely (#603, `apps/worker/src/curie_worker/sandbox/docker.py::_OAUTH_TOKEN_PREFIX`) so a real token never sits inert in a container that resolves none; otherwise the ambient SDK credentials, each only when present and only absent a base-URL override, since a local endpoint needs no real Anthropic token. That rule is frozen as data in `tests/vectors/model-credential-forwarding.json` and asserted from both the Python worker lane and the Rust CLI lane, so a change to it fails in two places.

Per-agent connector secrets diverge harder, and this one is a behavior difference, not just a shape difference. The binding injects them into the substrate-agnostic boot env by value and marks their names in `CURIE_CONNECTOR_SECRET_KEYS` (`apps/worker/src/curie_worker/binding.py::inject_connector_secrets`). The k8s client strips both that marker and every key it names off the value-only claim (`apps/worker/src/curie_worker/sandbox/k8s.py::CONNECTOR_SECRET_KEYS_ENV`, applied in `apps/worker/src/curie_worker/sandbox/k8s.py::KubernetesSandboxClient.create_claim`), because a claim env entry is plaintext in etcd; delivery is the per-agent SandboxTemplate `secretKeyRef` plus `warmPoolRef` routing (`apps/worker/src/curie_worker/sandbox/types.py::claim_warm_pool`, #1488). The Docker client has nowhere to leak them to and forwards them by value through its generic `-e key=value` loop, since `apps/worker/src/curie_worker/sandbox/docker.py::_WORKER_OWNED_ENV` covers only the bundle ref, the plugin dir, the sandbox id, the model credential, and the attachment ref (`apps/worker/src/curie_worker/attachments.py::ATTACHMENTS_REF_ENV`). Same `SandboxClient` calls and the same env dict go in; Kubernetes resolves values from the template Secret and Docker from argv. A third substrate has to pick a side of that, and the `Protocol` says nothing about which. These are runtime-shaped asymmetries the `Protocol` does not fully hide. Hosted-connector NetworkPolicy isolation still waits on a frozen plugin-format selector change (issue #1488 item 6).

Attachment redemption diverges the same way (#2567). Docker redeems the capability in `apps/worker/src/curie_worker/sandbox/docker.py::DockerSandboxClient._prepare_attachments` and bind-mounts the files read-only. Kubernetes hands the key only to the `attachments-init` container (`apps/worker/src/curie_worker/sandbox/k8s.py::ATTACHMENT_INIT_CONTAINERS`, applied in `apps/worker/src/curie_worker/sandbox/k8s.py::KubernetesSandboxClient.create_claim`) and keeps it off the runner.

## Cross-links

- **Epic(s):** #86 — substrate vision (pluggable runtimes beyond agent-sandbox); #44 — substrate hardening
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — core substrate, not separately graded
- **ADR(s):** [ADR-0002](../../adr/0002-kubernetes-agent-sandbox-as-runtime-substrate.md) — Kubernetes Agent Sandbox as the interactive runtime substrate; [ADR-0008](../../adr/0008-multi-tenancy.md) — multi-tenancy: hard-siloed compute (namespace-per-tenant) rides this seam; [ADR-0009](../../adr/0009-per-agent-connector-auth.md) — per-agent secrets and connector credentials, whose Kubernetes half is the per-agent pool `secretKeyRef` path (#1488); [ADR-0028](../../adr/0028-substrate-is-resilience-fallback-not-product-swap-axis.md) — the "core-with-fallback, not a marketed swap" stance above is now a recorded decision: substrate portability stays a resilience-only fallback, not a product swap axis (settles #86)
