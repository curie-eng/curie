# 194. The plugin-format contract is the frozen schema plus a parity gate

Date: 2026-10-04

Status: Accepted

Accepted alongside the implementation of [issue 3834](https://github.com/curie-eng/curie/issues/3834) under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md). The maintainer decisions recorded in that issue are the explicit approval. The realizing code paths are named under Decision.

**Supersedes in part [ADR 0017](0017-tri-language-contract-codegen.md)**, exactly one clause: the statement that the generated TypeScript and Rust are derivatives of the Pydantic models for `packages/plugin-format`. The ACI half of ADR 0017 stands unchanged.

## Context

ADR 0017 describes both frozen packages as tri-language: Pydantic source of truth, committed JSON Schema, and generated TypeScript and Rust. That is true only for `packages/aci-protocol`. `packages/plugin-format` exports a committed JSON Schema (`packages/plugin-format/schema/plugin-format.schema.json`), regenerated and drift-checked by `packages/plugin-format/tests/test_schema_compat.py`, and nothing more. No Rust or TypeScript is generated from it. The CLI mirrors the format by hand in `cli/src/spec.rs` and `cli/src/commands/`, and `docs/architecture-vision.md` repeated the ADR 0017 claim as fact.

Two related gaps sat beside it. The UI hand-mirrored API response types in `apps/ui/src/api/client.ts` and had drifted (its `AgentOut` carried 5 of the API's 21 fields). And the CLI field-parity gate for API models (`cli/tests/api_field_parity.rs`) checked field presence only, not whether a field is required or optional, and did not cover request bodies.

## Decision

For `packages/plugin-format`, the cross-language contract is the frozen JSON Schema plus a field-parity gate. Rust structs are not generated.

- The schema stays the committed export of the Pydantic models, drift-checked by `packages/plugin-format/tests/test_schema_compat.py`.
- The gate is `cli/tests/plugin_format_field_parity.rs`, driven by the manifest `cli/plugin-format-mirrors.json` and the shared comparator `cli/tests/support/field_parity.rs`. It fails CI when a schema field is missing from the hand-written mirror in `cli/src/spec.rs` (or the other mirror files the manifest lists) and does not carry a justified, declared omission. `curie dev field-parity` (`cli/scripts/check-field-parity.sh`) is the local entry point.

Companion decisions from the same issue:

- UI TypeScript API types are generated from `apps/api/openapi.json` by `apps/ui/scripts/gen-api-types.mjs` into `apps/ui/src/api/generated.ts`. The `ui` job in `.github/workflows/ci.yaml` runs the generator with `--check`, so changing an API response field without regenerating the types fails CI. The unused `@aci/*` alias is removed.
- The CLI API field-parity gate (`cli/tests/api_field_parity.rs`, using the same comparator) compares optionality, so a required CLI field that maps to an optional API field fails, and it covers request bodies, not only response models.

## Alternatives considered

- **Generate the Rust structs from the schema.** Rejected. The format is lenient by design (`extra="allow"`, per `packages/CLAUDE.md`) and the CLI structs carry behavior and serde attributes the schema cannot express, so generated types would need a hand layer on top and still would not make the CLI own its own shapes. A presence-and-optionality gate catches the drift that matters at a fraction of the machinery.
- **Keep the hand mirrors ungated.** Rejected. This is the status quo that let the mirror and the documentation drift silently. A field added in Python would reach users with the CLI ignoring or rejecting it.
- **Hand-written UI types.** Rejected. The UI had already drifted from the API on field count, and a generated file with a CI diff check turns that drift into a build failure with no ongoing effort.

## Consequences

- ADR 0017 stays Accepted and gains a back-link only; it still governs the ACI. Its plugin-format generation claim is overtaken by this ADR.
- A change to the plugin-format schema now requires a matching `cli/src/spec.rs` change, or a justified omission in `cli/plugin-format-mirrors.json`, in the same pull request.
- A change to `apps/api/openapi.json` requires regenerating `apps/ui/src/api/generated.ts` in the same pull request.
- The gates check field names and optionality. They do not check value types or semantics, so the mirrors remain hand code that a reviewer still owns.
