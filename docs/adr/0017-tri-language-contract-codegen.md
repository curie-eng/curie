# 17. Tri-language frozen contracts: Pydantic as source of truth, generate the rest, gate on drift

Date: 2026-07-09
Status: Accepted

Partially superseded by [ADR 0194](0194-plugin-format-contract-is-the-frozen-schema-plus-a-parity-gate.md): the plugin-format code-generation clause.

Retroactive record of the codegen mechanism behind the frozen contracts.
ADR-0005 established that the ACI and plugin-format are frozen; this ADR records
how that freeze is maintained across three languages and what it closes the door
on. It is the narrowest of the backfill set; keep it if the codegen choice is
worth its own record, otherwise it folds under ADR-0005.

## Context

`packages/aci-protocol` and `packages/plugin-format` are compiled against by the
Python core, the Rust CLI, and the TypeScript UI. An unreviewed change to the
contract in one language silently breaks the others unless something forces them
to agree. The question was which language is authoritative and how the other two
stay in lockstep.

## Decision

Python Pydantic models are the single source of truth. The committed JSON Schema
and the generated TypeScript and Rust are derivatives, regenerated in CI and
checked with `git diff --exit-code` so drift in any of the three languages fails
the build. The compat gate regenerates schema and Rust in-process and fails on
drift
([`packages/aci-protocol/tests/test_schema_compat.py`](../../packages/aci-protocol/tests/test_schema_compat.py));
the repo-root [`scripts/check-contracts.sh`](../../scripts/check-contracts.sh)
runs the full regenerate-and-compile sweep; CI enforces it as the `contracts-ts`
job ([`.github/workflows/ci.yaml`](../../.github/workflows/ci.yaml)).

## Alternatives considered

- **A protobuf / IDL as the source of truth.** Rejected: it adds a separate
  toolchain and loses the Pydantic validation ergonomics the Python core already
  depends on (the same models validate bundles and requests at runtime). Pydantic
  is already load-bearing in the core, so making it authoritative costs nothing
  extra.
- **Hand-maintain the three language copies.** Rejected: it is exactly the
  silent-cross-language-drift failure the freeze exists to prevent. Generation
  plus a diff gate makes drift a build failure instead of a runtime surprise.
- **Runtime schema validation only, no generated types.** Rejected: it gives up
  the compile-time guarantee that the Rust CLI and TS UI agree with the Python
  core before anything ships.
- **Generate the Rust types from the JSON Schema with `typify`** instead of the
  current hand-rolled emitter. Not chosen yet; it is an open refactor
  ([#81](https://github.com/curie-eng/curie/issues/81)) that would change the
  emitter, not this decision (Pydantic stays the source of truth, the gate stays).

## Consequences

- Hand-editing the generated Rust or TypeScript, or switching the source-of-truth
  language, breaks the guarantee silently and is a design error.
- A task that needs either frozen package to change stops and escalates rather
  than working around the gate, per ADR-0005.
- The emitter implementation is free to change (see #81) as long as Pydantic
  remains authoritative and the regenerate-and-diff gate remains the enforcement.
