# 243. Attachments are fetched on demand through scoped platform tools

Date: 2026-10-23

Status: Draft

The maintainer selected the product direction; acceptance of this written
decision is pending. This Draft does not authorize implementation.

The frozen-ACI dependency is tracked in
[#5005](https://github.com/curie-eng/curie/issues/5005).

## Context

The attachment lane already streams, but eagerly resolves current content and
rebuilds the thread's selected file set before runner startup. Numeric byte
limits bound that work and also reject legitimate files before the agent can
decide whether it needs them. Streaming reduces buffering, not eager network
traffic, local storage or boot delay.

The realizing specification is
[on-demand attachment delivery](../superpowers/specs/2026-10-23-on-demand-attachments-design.md).

## Proposed decision

1. Register trusted inbound references before execution without downloading
   content. Catalog availability and a verified local path are distinct states.
2. Platform tools list scoped references and retrieve one requested file
   through an API facade and private worker broker. Only the worker holds
   channel credentials. The sandbox chooses no arbitrary channel ID or URL.
3. Stream with backpressure, finite execution authority and cancellation.
   Publish a local path only after full transfer, actual-byte accounting and
   digest verification. Reuse verified sandbox-local cache entries. Cold starts
   do not eagerly rebuild the thread's complete file set.
4. Lazy mode has no per-file or thread-aggregate application byte ceiling.
   Finite sandbox capacity, time, concurrency, metadata and retention bounds
   remain. Exhaustion fails a fetch explicitly rather than truncating it.
5. Add optional feature availability and per-turn scoped context through a
   separate reviewed compatible ACI change. Require compatible components for
   lazy activation; do not repurpose the old manifest or fall back silently.

If accepted, this partially amends
[ADR 0153](0153-a-channel-port-turn-carries-its-attachments.md)'s eager complete
current-set delivery, and
[ADR 0205](0205-an-attachment-belongs-to-its-thread.md)'s decisions 5 through 8
only where they require eager rebuild, complete-set preparation, a mandatory
byte budget and a disk-present manifest. Thread ownership, durable reference
lifetime, stable filenames and integrity remain. The finite resource envelope
in [ADR 0059](0059-sandbox-is-a-bounded-resource-envelope.md) stands. Accepted
ADR bodies are not rewritten.

## Realizing paths

Reference registration replaces lazy-mode eager work in
`apps/worker/src/curie_worker/attachments.py` and
`apps/worker/src/curie_worker/kernel/attachments.py`. New worker broker and API
facade/catalog modules implement scoped access, streaming and completion.
Runner tools/cache extend `runner/src/curie_runner/attachments.py` and
`runner/src/curie_runner/__main__.py`. Substrate setup changes
`charts/curie/templates/agent-sandbox.yaml` and
`apps/worker/src/curie_worker/sandbox/docker.py`. These are proposed paths,
not implemented or verified claims.

## Consequences

Startup transfers metadata; only requested files incur download/storage costs.
Large files remain subject to real storage and execution capacity. Retrieval
failure is a visible tool outcome. A file deleted before first fetch may be
unavailable: metadata-only intake does not preserve a content snapshot. First
verified fetch binds a digest; later retrieval cannot silently substitute bytes.

The facade, private broker, catalog and cache lifecycle add complexity. Scoped
authority preserves credential boundaries. Acceptance alone does not qualify
runtime behavior; the specification names required production-path evidence.

## Alternatives

- Raise/remove numbers while keeping eager download: rejected because every
  cold boot still pays for the selected set, including unused files.
- Use a filesystem interception layer: rejected for this change because it
  adds runtime/privilege dependencies and obscures retrieval failures.
- Give credentials or arbitrary fetch URLs to the sandbox: rejected because
  authority would exceed the registered files of the current thread.
- Preserve immutable objects at intake: gives a stronger snapshot guarantee
  but requires downloading every file before it is needed. This design binds
  content on first successful fetch instead.
