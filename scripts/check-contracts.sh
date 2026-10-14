#!/usr/bin/env bash
# Regenerate every committed contract artifact from the Pydantic source of truth
# and fail if anything drifted. This is the local mirror of the CI compat gate:
# the pytest drift tests cover the JSON Schema and Rust, the Rust crate is
# compiled by cargo, and the generated TypeScript is compiled by tsc. Run this
# after any intended contract change, then commit the regenerated files.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

aci_schema="packages/aci-protocol/schema/aci-protocol.schema.json"
aci_wire_lock="packages/aci-protocol/schema/wire.lock"
channel_schema="packages/channel-protocol/schema/channel-protocol.schema.json"
plugin_schema="packages/plugin-format/schema/plugin-format.schema.json"
eval_schema="apps/worker/schema/eval-cases.schema.json"
env_example=".env.example"
ts_out="packages/aci-protocol/generated/ts/aci-protocol.ts"
rust_crate="packages/aci-protocol/generated/rust"

echo "== regenerating JSON Schemas =="
uv run python -m aci_protocol.schema_export
uv run python -m channel_protocol.schema_export
uv run python -m plugin_format.schema_export
uv run python -m curie_worker.eval.schema_export

echo "== regenerating the boot-env block in .env.example =="
uv run python -m aci_protocol.env_example_export

echo "== regenerating Rust types =="
uv run python -m aci_protocol.rust_export

echo "== regenerating the ACI wire lock =="
uv run python -m aci_protocol.wire_lock

echo "== regenerating TypeScript types =="
pnpm --dir tools/contract-codegen exec json2ts "$repo_root/$aci_schema" \
  --unreachableDefinitions -o "$repo_root/$ts_out"

echo "== compiling generated Rust =="
cargo check --manifest-path "$rust_crate/Cargo.toml"

echo "== compiling generated TypeScript =="
pnpm --dir tools/contract-codegen exec tsc --noEmit \
  -p "$repo_root/packages/aci-protocol/generated/ts/tsconfig.json"

echo "== checking for drift =="
if ! git diff --exit-code -- \
  "$aci_schema" "$aci_wire_lock" "$channel_schema" "$plugin_schema" "$eval_schema" \
  "$env_example" "$ts_out" "$rust_crate/src/lib.rs"; then
  echo "ERROR: committed contract artifacts drifted from the models." >&2
  echo "The files above were regenerated and differ. Review, then commit them." >&2
  exit 1
fi

echo "OK: all committed contract artifacts are current."
