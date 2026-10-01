# 37. Opt-in binding hook and Pareto model routing

Date: 2026-07-16

Status: Accepted

Implementation tracked in epic [#473](https://github.com/curie-eng/curie/issues/473).

## Context

A prospective customer engagement asks for a model router: phase 1 is static
rules-based routing (task type to model -- simple code to a cheap model,
complex code to a frontier model, pluggable targets), phase 2 uses a model to
make the routing decision over free-form chat text. Independent of that engagement, the
same mechanism is the cost-optimization lever for any deployment running more
than one model.

Curie has no routing today. Model selection is a boot-time pin
(`CURIE_MODEL`, per-agent override at the worker binding), exactly one model
credential resolves (ADR-0009), and the provider is swapped through config,
not code (the model-provider seam is deliberately SOFT). Three prior decisions
constrain the design:

- **#253 (closed) rejected a LiteLLM gateway sidecar**: translation/proxy hops
  in the token path break byte-stable prompt prefixes and destroy prompt-cache
  economics. What matters is a direct provider connection with exactly one
  format boundary. Any router here must decide *which provider to pin a
  session to*, not proxy per-call.
- **ADR-0032**: egress is fail-closed and opened per named provider at install
  by resolving IPs. A router can only choose among providers whose egress is
  already open, so its candidate set must be declared at install time.
- **ADR-0031**: no speculative abstraction at the harness seam. The router
  must not grow a shared options layer; it hands each harness a model id and
  environment, nothing more.

ADR-0036 (ACI semver) additionally names the unfrozen env contract that
shadows `SessionConfig` -- which is where `CURIE_MODEL` travels today -- as
a real gap in what "frozen" covers. A routing decision needs a typed home, and
that home closes part of the named gap.

Landscape evidence shaped the selection mechanism. OpenRouter operates two
routers side by side: a black-box trained classifier (`openrouter/auto`,
white-labeled NotDiamond) and the transparent Pareto code router
(`openrouter/pareto-code`) where the caller declares a quality floor and the
router picks the cheapest model at or above it -- no prompt analysis, fully
auditable. OpenRouter also pins routed sessions to one model/provider
specifically to protect prompt caches. RouteLLM (Apache-2.0, ICLR 2025) is the
peer-reviewed reference for learned routing (95% of top-model quality at 85%
cost reduction on MT-Bench) if a learned estimator is ever wanted. The
CurieTech `platform/agents` codebase independently validates the registry
shape: a litellm-backed model registry carrying per-model pricing metadata.

## Decision

**1. A new optional port: the binding hook.** It runs in the worker at
task-to-sandbox claim time, before the sandbox boots. Input: the task
descriptor, the agent's manifest, and the install-level model registry.
Output: a typed `BindingDecision` restricted to model id, provider base URL,
and model-credential ref. It cannot mutate arbitrary sandbox environment --
an open-ended mutation hook declared by a bundle would be a
credential-redirection and egress hole. The default adapter is the static pin
(today's behavior, byte-identical); the port is dormant unless a manifest opts
in. Model routing is the first adapter. Provider arbitrage / load balancing is
the named deferred second adapter that validates the port shape (the
ADR-0026/0027 discipline: extract the port, defer the second adapter).

**2. The manifest declares intent, not models.** An opt-in `routing` block
joins the existing Curie manifest extensions (alongside `secrets`,
ADR-0009): static per-task-type capability floors and constraints (phase 1),
and optional judge configuration (phase 2). Bundles never name
install-specific providers or model ids -- bundles are portable and egress
cannot follow a bundle (ADR-0032).

**3. An install-level model registry supplies capability.** The operator
declares the candidate set at install: model id, provider, base URL, input and
output cost, capability scores, model-credential ref, egress host. Every
candidate's egress opens at install; every candidate's credential is
deliverable to a sandbox (extending the ADR-0009 named-secret pattern from
connector secrets to model credentials). Dynamic provider discovery is out of
scope by construction.

**4. Selection is deterministic Pareto: cheapest at or above the floor.** The
LLM judge, when configured, is a floor *estimator* only -- it outputs a
required-capability score and never picks a model. Score in, deterministic
selection out: every decision is auditable and replayable against the
registry. Judge failure falls back to the static floor; routing is fail-safe
and never blocks a task.

**5. Decisions bind per session and stick.** A route is decided once at claim
and pinned for the session, including across suspend/resume (the
approval-resume path re-injects the same decision). There is no per-turn or
per-call re-routing; that is the #253 prompt-cache lesson, and OpenRouter's
own session pinning is the same conclusion reached independently.

**6. The decision rides the typed ACI.** Model id, provider base URL, and
model-credential ref are promoted into `SessionConfig` as optional fields -- a
patch bump under the ADR-0036 change-class table -- closing the shadow-env gap
for the model axis.

**7. Every decision is observable.** Structured log plus OTEL span attributes
at claim time: chosen model, floor, floor source (static rule or judge), and
candidates considered.

## Alternatives considered

- **In-path gateway/proxy (LiteLLM sidecar or bespoke router service in the
  token path).** Rejected, again: #253's prompt-cache rationale. Per-request
  routing inside a session invalidates the cached prefix every turn and can
  cost more than running the frontier model directly.
- **Routing inside `ModelSession` / the runner.** Rejected: the runner sees
  one session and holds one credential; the claim, the registry, and
  credential delivery are worker-plane concerns. Also ADR-0031.
- **A learned classifier as the v1 decision-maker (NotDiamond-style, or
  RouteLLM).** Deferred, not rejected: start explainable. A learned floor
  estimator can replace the judge behind the identical contract later without
  touching selection.
- **Bundle-carried model registry.** Rejected: bundles are portable across
  installations, and neither credentials nor egress can follow a bundle.
- **A general env-mutation pre-boot hook.** Rejected: the typed
  `BindingDecision` is the whole writable surface. Generality here is a
  security hole, not a feature.

## Consequences

- The worker gains its first LLM-call capability (the judge), with its own
  cheap-model credential and egress registry entry. Opt-in only.
- Multi-credential delivery extends ADR-0009 from connector secrets to model
  credentials; this is the largest single piece of platform work in the epic.
- The router core (registry schema, floor estimation, Pareto selection) is
  built as a separable library. The Curie binding hook is consumer one; a
  standalone licensable facade or other platforms are future consumers.
- Non-LLM route targets (for example OCR pipelines) are the genuinely novel
  slice of that customer request -- no shipped router does this -- and are explicitly
  out of scope for this port: they are handler dispatch above the model seam,
  not model selection.
- Opt-out cost is zero. A bundle without a `routing` block exercises no new
  code path, and an installation with a single registered model behaves
  exactly as today.
