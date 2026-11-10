#!/usr/bin/env bash
# Emit-hop field-parity gate (#699): a CliOutput::to_json that hand-projects a
# cli/src/api.rs mirror struct into a serde_json::json! literal must cover
# every wire field of that struct per cli/api-mirrors.json's `emits` array
# (declared projection + allowlisted, justified omissions). One hop downstream
# of the struct-level gate (#691, check-field-parity.sh). See
# cli/tests/api_emit_parity.rs.
set -euo pipefail

cargo test --manifest-path cli/Cargo.toml --test integration api_emit_parity:: -- --nocapture
