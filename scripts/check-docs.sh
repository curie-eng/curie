#!/usr/bin/env bash
# Regenerate the generated regions of the interface catalog from each seam's
# front-matter, fail if anything drifted, then lint every citation under the
# linted root. This is the local mirror of the CI docs gate and the exact shape
# of scripts/check-contracts.sh: regenerate, diff, then check. Run it after any
# intended catalog change, then commit the regenerated docs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

generated_docs=(docs/interfaces.md docs/interfaces docs/adr/README.md)

echo "== regenerating the seam table, the ADR index, and per-doc headers =="
uv run python -m curie_doclint --repo-root "$repo_root" --write

echo "== checking for drift =="
if ! git diff --exit-code -- "${generated_docs[@]}"; then
  echo "ERROR: generated catalog regions drifted from the seam front-matter." >&2
  echo "The files above were regenerated and differ. Review, then commit them." >&2
  exit 1
fi

# The single curie_doclint invocation that follows runs generate, counts,
# commands, lint, and ADR number uniqueness across the working tree,
# origin/main, origin/next, and open PR additions (#541, #1957). A failure in
# any of them surfaces here. Each finding names its own phase, so this script
# does not enumerate them. --require-remote-adr-scan fails on collisions this
# candidate adds or renames into; other remote collisions print as warnings.
# Scan errors fail closed only when this candidate claims a new ADR file.
echo "== verifying generated counts, the verification contract, citations, and ADR numbers =="
uv run python -m curie_doclint --repo-root "$repo_root" --require-remote-adr-scan

echo "OK: the interface catalog is generated and drift-free, every citation resolves, every command the verification contract names is real, and ADR numbers are unique for this candidate across trains and open PRs."
