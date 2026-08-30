#!/usr/bin/env bash
# Assert every Curie-owned skill still validates against the official Agent
# Skills reference validator. This is the INBOUND spec-conformance twin of
# scripts/check-plugin-compat.sh: that script proves a Curie bundle validates
# unmodified as a Claude Code plugin (outbound compatibility), this one proves
# our own SKILL.md files satisfy the Agent Skills spec directly. They are
# different contracts -- the plugin-format loader stays deliberately lenient so
# Claude-Code-shaped bundles keep loading, while this gate holds Curie's own
# skills to the stricter published spec.
set -euo pipefail

# The pin is deliberate. A gate that floats to whatever the newest spec revision
# happens to be is not deterministic: the same commit could pass today and fail
# tomorrow with no change of ours. Bumping this version is a reviewed edit, and
# the migration it forces is the point.
SKILLS_REF_VERSION="0.1.1"
# Pinning the version alone does NOT make the gate deterministic. skills-ref
# declares floating dependencies (`click>=8.0`, `strictyaml>=1.7.3`) and uvx
# resolves the newest compatible transitive versions at run time -- and it is the
# transitive set, strictyaml in particular, that decides verdicts here:
# strictyaml's refusal of JSON-style flow collections is precisely what makes
# `allowed-tools: []` fail. A strictyaml release could therefore flip this gate
# with no change of ours. The resolution cutoff freezes the whole dependency set,
# so moving it is a reviewed edit exactly like the version bump.
SKILLS_REF_EXCLUDE_NEWER="2026-08-29T00:00:00Z"
# The distribution is `skills-ref`, but its console script is named
# `agentskills`, not `skills-ref`.
SKILLS_REF_BIN="agentskills"
# Every uvx invocation in this file shares one flag array. Spelling the pins out
# per call site invites a future bump that lands on some invocations and misses
# others, which is the same silently-inconsistent state the pin exists to prevent.
uvx_args=(--from "skills-ref==$SKILLS_REF_VERSION" --exclude-newer "$SKILLS_REF_EXCLUDE_NEWER")

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v uvx >/dev/null 2>&1; then
  echo "ERROR: 'uvx' is not on PATH, so the Agent Skills reference validator cannot be run." >&2
  echo "Install uv (curl -LsSf https://astral.sh/uv/install.sh | sh) and re-run." >&2
  exit 1
fi

# uvx exits 1 when it cannot resolve or launch the tool -- the same status the
# validator uses to REJECT a skill. Exit status alone therefore cannot separate
# "the malformed fixture was correctly rejected" from "the network was down", so
# the validator has to be proven runnable BEFORE any status is interpreted below.
echo "== resolving the Agent Skills reference validator =="
if ! skills_ref_version_line="$(uvx "${uvx_args[@]}" "$SKILLS_REF_BIN" --version 2>&1)"; then
  echo "ERROR: the Agent Skills reference validator could not be resolved or launched." >&2
  echo "This is NOT 'a skill is invalid': no verdict from this gate can be trusted," >&2
  echo "positive or negative, because a resolution failure is indistinguishable from a" >&2
  echo "validation rejection by exit status alone. uvx said:" >&2
  printf '%s\n' "$skills_ref_version_line" >&2
  echo "Check network access to PyPI, and that skills-ref==$SKILLS_REF_VERSION still" >&2
  echo "resolves under --exclude-newer $SKILLS_REF_EXCLUDE_NEWER." >&2
  exit 1
fi
# Record which validator actually ran, so the CI log says what produced the verdict.
printf 'using %s\n' "$skills_ref_version_line"

# Skills that MUST validate clean against the reference validator. Every path is
# relative to the repo root.
VALID_SKILLS=(
  ".claude/skills/implement"
  ".claude/skills/update-architecture-atlas"
  "examples/github-issues/skills/github-issues"
  "examples/sre-bot/skills/sre-bot"
  "examples/squawk/skills/squawk"
  "examples/text-stats-engine/skills/text-stats"
  "examples/weather/skills/weather"
  "packages/plugin-format/tests/fixtures/valid_bundle/skills/greeter"
  "runner/tests/fixtures/mcp_green/skills/green"
  "runner/tests/fixtures/mcp_red_broken/skills/broken"
  "runner/tests/fixtures/mcp_red_pointer/skills/red"
)

# Skills that MUST be REJECTED, asserted as negatives rather than quietly
# excluded. bad_skill/skills/broken omits the required `description` field by
# construction -- that malformation is the fixture's whole reason to exist, and
# requiring the reference validator to keep rejecting it is what proves this
# gate is not vacuous.
INVALID_SKILLS=(
  "packages/plugin-format/tests/fixtures/bad_skill/skills/broken"
)

if [ "${#VALID_SKILLS[@]}" -eq 0 ]; then
  echo "ERROR: VALID_SKILLS is empty, so this gate would pass vacuously." >&2
  echo "An empty positive set defeats the purpose of the check." >&2
  exit 1
fi

# Curie-owned roots that may contain a SKILL.md. An allowlist alone silently
# stops covering anything added later, so discovery drift is checked explicitly
# below: a new skill must be classified, never merely unlisted.
SKILL_ROOTS=(
  ".claude/skills"
  "examples"
  "packages/plugin-format/tests/fixtures"
  "runner/tests/fixtures"
)

# find(1) runs inside a process substitution below, so its exit status never
# reaches this shell -- `set -euo pipefail` cannot see it. A vanished root would
# just shrink the discovered set, and if the matching list entries were dropped in
# the same change the drift check would reconcile and PASS while checking nothing.
# Requiring each root to exist keeps that failure legible.
missing_roots=()
for root in "${SKILL_ROOTS[@]}"; do
  if [ ! -d "$root" ]; then
    missing_roots+=("$root")
  fi
done
if [ "${#missing_roots[@]}" -gt 0 ]; then
  echo "ERROR: ${#missing_roots[@]} SKILL_ROOTS entry/entries do not exist: ${missing_roots[*]}" >&2
  echo "The roots list is stale, so discovery cannot be trusted: skills under a moved or" >&2
  echo "renamed root are invisible here and this gate would pass without checking them." >&2
  echo "Update SKILL_ROOTS in scripts/check-agent-skills.sh." >&2
  exit 1
fi

echo "== discovering skills under ${SKILL_ROOTS[*]} =="
discovered=()
# The validator's own find_skill_md accepts both SKILL.md and lowercase
# skill.md. Discovery has to match both spellings too, or a skill.md-only
# directory is invisible here while still validating -- the exact escape this
# drift check exists to catch.
while IFS= read -r skill_md; do
  discovered+=("$(dirname "$skill_md")")
done < <(find "${SKILL_ROOTS[@]}" \( -name SKILL.md -o -name skill.md \) | sort)
echo "found ${#discovered[@]} skill(s)"

listed=()
listed+=("${VALID_SKILLS[@]}")
listed+=("${INVALID_SKILLS[@]}")

echo "== checking the allowlist covers exactly the discovered skills =="
# Associative-array lookups turn both set differences into O(n+m): `[[ -v ...
# ]]` tests membership without expanding the value, so it is safe under
# `set -u` even for keys that were never assigned.
declare -A listed_map=()
for known in "${listed[@]}"; do
  listed_map["$known"]=1
done
unlisted=()
for skill in "${discovered[@]}"; do
  [[ -v listed_map[$skill] ]] || unlisted+=("$skill")
done

declare -A discovered_map=()
for skill in "${discovered[@]}"; do
  discovered_map["$skill"]=1
done
stale=()
for known in "${listed[@]}"; do
  [[ -v discovered_map[$known] ]] || stale+=("$known")
done

drifted=0
if [ "${#unlisted[@]}" -gt 0 ]; then
  echo "ERROR: ${#unlisted[@]} skill(s) escaped the gate: ${unlisted[*]}" >&2
  echo "A new skill exists that neither list names, so nothing checks it. Add it to" >&2
  echo "VALID_SKILLS in scripts/check-agent-skills.sh (or to INVALID_SKILLS with a" >&2
  echo "comment naming why it must fail)." >&2
  drifted=1
fi
if [ "${#stale[@]}" -gt 0 ]; then
  echo "ERROR: ${#stale[@]} listed skill(s) no longer exist: ${stale[*]}" >&2
  echo "The list in scripts/check-agent-skills.sh is stale. Drop the entries for" >&2
  echo "skills that were moved or deleted." >&2
  drifted=1
fi
if [ "$drifted" -ne 0 ]; then
  exit 1
fi
echo "allowlist covers exactly the ${#discovered[@]} discovered skill(s)"

echo "== validating each skill against Agent Skills $SKILLS_REF_VERSION =="
failed=()
for skill in "${VALID_SKILLS[@]}"; do
  echo "-- $skill --"
  if ! uvx "${uvx_args[@]}" "$SKILLS_REF_BIN" validate "$skill"; then
    failed+=("$skill")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "ERROR: the Agent Skills reference validator rejected ${#failed[@]} skill(s): ${failed[*]}" >&2
  echo "Curie's own skills are held to the published spec: frontmatter must be" >&2
  echo "strictyaml-parseable and may only carry name, description, license," >&2
  echo "allowed-tools, metadata, and compatibility. Vendor extensions belong under" >&2
  echo "the free-form 'metadata' map. Fix the skill, or update" >&2
  echo "docs/interfaces/bundle-format/INTERFACE.md to record a new contract." >&2
  exit 1
fi

echo "== asserting the deliberately malformed fixture(s) are still rejected =="
passed=()
unverdicted=()
for skill in "${INVALID_SKILLS[@]}"; do
  echo "-- $skill (must be rejected) --"
  # "Nonzero" is not evidence of a rejection: a missing path exits 2 and a uvx
  # resolution failure exits 1, the same as a real rejection. Only status 1
  # carrying the validator's own marker is a verdict; anything else is a broken
  # harness and must not be counted as this gate biting.
  set +e
  reject_output="$(uvx "${uvx_args[@]}" "$SKILLS_REF_BIN" validate "$skill" 2>&1)"
  reject_status=$?
  set -e
  # The rejection reason is printed, not swallowed: the evidence that this gate
  # bites belongs in the CI log.
  printf '%s\n' "$reject_output"
  if [ "$reject_status" -eq 0 ]; then
    passed+=("$skill")
  elif [ "$reject_status" -ne 1 ] || ! printf '%s' "$reject_output" | grep -q 'Validation failed for'; then
    unverdicted+=("$skill")
  fi
done

if [ "${#unverdicted[@]}" -gt 0 ]; then
  echo "ERROR: could not obtain a validation verdict for ${#unverdicted[@]} skill(s): ${unverdicted[*]}" >&2
  echo "The validator neither accepted nor rejected them: it exited with a status that" >&2
  echo "is not a verdict, or exited 1 without printing 'Validation failed for'. That is a" >&2
  echo "HARNESS failure (bad fixture path, resolution or launch problem), not a rejection," >&2
  echo "and treating it as one is exactly how this gate would pass for the wrong reason." >&2
  echo "Fix the environment or the listed path; the output above is the evidence." >&2
  exit 1
fi

if [ "${#passed[@]}" -gt 0 ]; then
  echo "ERROR: ${#passed[@]} asserted-invalid skill(s) now VALIDATE CLEAN: ${passed[*]}" >&2
  echo "These fixtures are malformed on purpose and are what proves this gate is not" >&2
  echo "vacuous. Either the fixture stopped being malformed (restore the defect, or" >&2
  echo "the tests that depend on it are already broken) or the spec now accepts it" >&2
  echo "and the entry belongs in VALID_SKILLS instead." >&2
  exit 1
fi

echo "OK: ${#VALID_SKILLS[@]} skill(s) conform to Agent Skills $SKILLS_REF_VERSION; ${#INVALID_SKILLS[@]} asserted-invalid fixture(s) still rejected."
