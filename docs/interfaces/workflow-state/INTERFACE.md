---
seam: Workflow state store
kind: SOFT
impls: 1 (API state router)
grade: not separately graded
epics:
  - "#23"
  - "#248"
order: 14
---
# INTERFACE: Workflow state store

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 (API state router) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The durable workflow-state store is built. It shipped (#248, under epic #23) as the API
state router: a scoped KV/document store on Postgres JSONB. The router exposes
twelve state routes: six operations, covering the verbs this doc once said did not exist
(get / put-with-CAS / list / delete / append) plus the namespace listing added by #250,
each doubled by `agents.memory` (#1525 follow-up) into its original path plus a
binding-scoped `.../state/bindings/{kind}/{address}/...` sibling, both implemented by
the same shared function. The swap
axis here is the state backend, and it is a SOFT seam: the store is reached over the HTTP
state API, not a typed in-process port, so a second backend is a persistence change behind
that API rather than a `Protocol` swap. A separate concrete route store, `AffinityStore`,
records one narrow thing (the `thread_key -> sandbox route` binding on Valkey, with atomic
acquire and TTL expiry) and is not the general store. The typed in-process port the kernel
would write arbitrary run state through (#23) is still unextracted, per "the second
implementation teaches the interface."

## Current contract

The state API is the store today. Every route lives in
`apps/api/src/curie_api/routers/state.py`:

- `get_state` (`apps/api/src/curie_api/routers/state.py::get_state`)
- `put_state` — put with compare-and-swap (`apps/api/src/curie_api/routers/state.py::put_state`)
- `list_state` (`apps/api/src/curie_api/routers/state.py::list_state`)
- `delete_state` (`apps/api/src/curie_api/routers/state.py::delete_state`)
- `append_state` (`apps/api/src/curie_api/routers/state.py::append_state`)
- `list_namespaces` — the namespaces an agent has stored, each with its key count and
  last write time (#250) (`apps/api/src/curie_api/routers/state.py::list_namespaces`)

Memory and Conversation history are the CLEAN loaders already built over this store
(`StateApiMemoryStore`, `StateApiTranscriptStore`).

Bundle code reaches the store two ways (#249), without shipping its own server. The
platform mounts an in-process `curie-state` MCP server into every sandbox
(`runner/src/curie_runner/state.py::build_state_server`), carrying get / set /
append / list / delete tools over the per-key and per-namespace routes; the
namespace listing is not exposed as a tool. `memory` and `transcript` are reserved
so a skill cannot corrupt the memory or history namespaces. A bundle
script that talks to the store directly reads the same URL and scoped token from
`CURIE_STATE_URL` / `CURIE_STATE_TOKEN`.

Neither path uses the platform key, and neither carries the same reach as the
runner's own loaders: there are two scoped-token scopes (ADR-0033), minted
side by side at `apps/worker/src/curie_worker/binding.py::BindingResolver.boot_env`.
The broad `state` scope backs `CURIE_MEMORY_TOKEN` / `CURIE_HISTORY_TOKEN` and
reaches every namespace, because the memory and history loaders must read and
write the reserved ones to rehydrate the agent across a suspend/resume. The narrow
`state.app` scope backs the bundle-facing `CURIE_STATE_TOKEN` and is refused on
the reserved namespaces server-side by
`apps/api/src/curie_api/routers/state.py::forbid_reserved_namespace`, so a skill
cannot bypass the MCP tool's own client-side refusal by composing
`CURIE_STATE_URL` itself. `list_namespaces` has no `namespace` path param and so
cannot be gated that way; it filters the reserved namespaces out of the response
for an app-scoped caller instead (#856). Which scope authenticated is carried
through as `apps/api/src/curie_api/routers/state.py::StateCaller`, resolved by
`apps/api/src/curie_api/routers/state.py::require_state_access`.

The worker-side route store is separate. `AffinityStore` at
`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore` records the
`thread_key -> sandbox route` binding, and its methods are the closest thing to a
route-state contract:

- `get(thread_key) -> RouteRecord | None` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.get`)
- `put_if_absent(thread_key, record, ttl_seconds) -> bool` — atomic acquire, the CAS-shaped primitive (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.put_if_absent`, `SET ... nx=True`)
- `replace(thread_key, record, ttl_seconds) -> None` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.replace`)
- `touch(thread_key, ttl_seconds) -> bool` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.touch`)
- `delete_if_claim(thread_key, claim_name) -> bool` — guarded delete via a Lua script (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.delete_if_claim`, script at `apps/worker/src/curie_worker/sandbox/affinity.py::_DELETE_IF_CLAIM`)
- `live_claim_names(...) -> set[str]` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.live_claim_names`)
- `mark_suspended(thread_key, history_ref, ttl_seconds) -> RouteRecord` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore.mark_suspended`)

The stored value is a `RouteRecord` (`apps/worker/src/curie_worker/sandbox/types.py::RouteRecord`) JSON-serialized. This is route affinity, not general workflow state.

## Implementations today

One general store (the API state router over Postgres JSONB) plus one narrow concrete
route store: `AffinityStore` (`apps/worker/src/curie_worker/sandbox/affinity.py::AffinityStore`), bound directly to `redis.Redis` and to the sandbox-routing use case. Neither is abstracted behind a typed workflow-state port yet.

## Known leakage

The placement constraint the future in-process port must honor is already visible in two
shapes. First, it is stateless-first: per ADR-0003 a suspend/resume is a cold pod restart
(the live process never survives), so resume rehydrates from a caller-supplied `history_ref`
injected as `CURIE_HISTORY_REF` rather than assuming any in-process or cache warmth
(`apps/worker/src/curie_worker/sandbox/substrate.py::SandboxSubstrate.resume`). Second,
the route store leans on Valkey TTL-expiry as garbage collection: an idle route record
simply expires, and the reaper protocol depends on that automatic expiry. A durable
(non-TTL) backend for a future workflow-state port would have to add its own sweeper to
reclaim abandoned state, because it cannot inherit Valkey's expiry-as-GC for free.

The store's own seam already leaks in-process on the API side. The HTTP state API is
not the only reader and writer of `apps/api/src/curie_api/models.py::WorkflowStateEntry`:
the memory router queries and mutates that table directly through the ORM in the same
process, rather than calling its own state routes. `list_memory` and `edit_memory`
(`apps/api/src/curie_api/routers/memory.py::list_memory`,
`apps/api/src/curie_api/routers/memory.py::edit_memory`) and
`apps/api/src/curie_api/routers/memory.py::delete_memory` all `select(WorkflowStateEntry)`
against the `memory` namespace, and the router imports the state module's private
`apps/api/src/curie_api/routers/state.py::_enforce_caps` to reuse the size caps rather
than inheriting them from a route. That import is deliberate (the state module documents
it as the reason `RESERVED_NAMESPACES` is a literal there and not an import back from
`routers.memory`), but the consequence for this seam is concrete: a second state backend
placed behind the HTTP state API would not cover these calls, and the memory surface would
keep reading the Postgres table the first backend owned. Anything the API-side seam gains
by being HTTP (auth scope, reserved-namespace guard, size caps at the boundary) is
re-implemented or bypassed on this path. The memory seam's own file covers the same
bypass from the memory side.

## Cross-links

- **Epic(s):** #23 — full workflow state store API spec (get / put-CAS / list / delete / append); the store shipped as the API state router via #248, and the extracted in-process port lands under this epic
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six graded jobs
- **ADR(s):** [ADR-0003](../../adr/0003-stateless-first-rehydrate-on-resume.md) — stateless-first sessions; rehydrate on resume; no cross-hibernation cache assumption
