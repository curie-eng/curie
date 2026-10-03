#!/usr/bin/env bash
# Field-parity gate: every hand-mirror of a frozen contract in the CLI must be
# declared and cover its source's fields.
#  - cli/src/api.rs mirrors of the platform REST DTOs against the committed
#    apps/api/openapi.json (#691, cli/api-mirrors.json,
#    cli/tests/api_field_parity.rs).
#  - cli/src/commands/*.rs + cli/src/spec.rs hand-mirrors of the frozen
#    packages/plugin-format manifest shape (#701, the sibling seam #691
#    explicitly did not cover: a different source of truth, the frozen
#    package's own packages/plugin-format/schema/plugin-format.schema.json
#    export, not openapi.json; cli/plugin-format-mirrors.json,
#    cli/tests/plugin_format_field_parity.rs).
set -euo pipefail

cargo test --manifest-path cli/Cargo.toml --test api_field_parity -- --nocapture
cargo test --manifest-path cli/Cargo.toml --test plugin_format_field_parity -- --nocapture
