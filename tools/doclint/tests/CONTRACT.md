# doclint public API contract (assumed by the test suite)

The tests in this directory were written first, against the public surface
below. The Implementer builds `curie_doclint` to satisfy exactly this. Tests
assert through the CLI exit code and message text, never internal helpers, so
private module/function names are free to change.

## Package

`import curie_doclint`

## Entrypoint

`curie_doclint.main(argv: list[str]) -> int`
- Accepts `["--repo-root", "<path>"]`; `--repo-root` defaults to the git
  top-level when omitted.
- Returns `0` when the linted tree is clean, non-zero otherwise.
- Prints one line per finding (to stdout or stderr; the suite reads both).

## Findings API (documented, not asserted directly)

`curie_doclint.lint(repo_root: Path) -> list[Finding]` runs the generate,
counts, agent-contract command, MCP/workspace live-tier, and citation lint
phases in memory and returns the findings `main` prints. A `Finding` carries
at least the repo-relative doc path, the offending citation/field, and a
human reason. Tests exercise this through `main`.

## Generator surface

`curie_doclint.render_index_table(repo_root: Path) -> str`
- Renders the `docs/interfaces.md` seam-table block from front-matter, ordered
  by the `order` field (ties are a hard error). Byte-stable across runs and
  machines. Globs `docs/interfaces/*/INTERFACE.md` (no hardcoded seam list).

## Constants

`curie_doclint.SOURCE_EXTENSIONS: tuple[str, ...]` — the single recognized
extension list, shared by the path rule and the raw line-ban rule. The test
list parametrizes over this constant so it cannot drift from the tool. Must
include at least: `py rs ts tsx yaml yml json sh toml md`.

## Linted root

`docs/` EXCLUDING `docs/adr/` (E2), PLUS the repo-root docs named in an
explicit allowlist (currently `ARCHITECTURE.md`). ADR docs are never
path/symbol/line checked; a root doc named in the allowlist that does not
resolve is a finding (deletion, not a skip).

## Expected message fragments (the reason the suite asserts)

- Nonexistent path: names the doc, the citation, and contains `does not exist`.
- Unresolvable symbol: names the citation/symbol and contains `does not resolve`.
- Shorthand `::` with no path (rule 2): names the citation and contains
  `full repo-relative path`.
- Line-number / `#L` citation: names the doc and the offending coordinate.
- Missing/unpaired generated marker: contains `marker`.
- Front-matter missing required field: contains the field name and `required`.
- Invalid `kind`: contains `kind` and the offending value.
- Syntax error in a cited `.py` file: names the file and contains `syntax`.
- Missing/unknown `vision_row:` on a graded seam: names the seam and, for a
  missing key, contains `vision_row`; for an unrecognized row, names the row.
- Grade disagreement: names the seam and both the declared grade and the
  vision-row's grade.
- Duplicate ADR number prefix: names both (or all) colliding filenames.
- Missing allowlisted root doc (e.g. `ARCHITECTURE.md`) or missing ADR index
  (`docs/adr/README.md`): names the doc; neither is scaffolded back by
  `--write`, only reported.
- Agent-contract command not a subcommand: names the token and contains
  `is not a subcommand of`.
- Agent-contract flag not declared: names the flag and contains `is not an
  argument of` and `not a global flag`.
- Agent-contract placeholder: names the token and contains `is a placeholder`
  and `placeholders are banned`.
- Agent-contract invocation names no subcommand: contains `names no
  subcommand`.
- Agent-contract names no command at all (vacuity guard): contains `no
  \`curie\` command appears`.
- Missing SRE demo runbook (`examples/sre-bot/DEMO.md`): names the doc.
- SRE demo runbook flag not declared: names the flag, the runbook path, and
  contains `is not an argument of` and `not a global flag`.
- SRE demo runbook names no command at all (vacuity guard): names the runbook
  path and contains `no \`curie\` command appears`.
- MCP/workspace live-tier rule missing a required sentence: names the doc,
  names the missing sentence, and contains `MCP/workspace live-tier rule is
  missing this required sentence`.

## Front-matter schema (per `INTERFACE.md`)

Required: `seam`, `kind` (`CLEAN` / `SOFT` / `NONE`, optional `, qualifier`),
`impls`, `grade`, `epics` (list of `"#N"` / `"ADR-XXXX"`), `order` (int).
Optional: `epic_note`, `vision_row` (required in practice for a graded seam --
see the grade-agreement check above -- but not enforced as a front-matter
field, since an ungraded seam has no row to name).

## Generated regions (marker-delimited)

- Index seam-table: `<!-- BEGIN GENERATED: seam-table (curie dev docs-lint) -->`
  ... `<!-- END GENERATED: seam-table -->`, rows
  `| Seam | Kind | Impls | Grade | Epic(s) | INTERFACE.md |`.
- Per-doc header blockquote: `<!-- BEGIN GENERATED: header ... -->` ...
  `<!-- END GENERATED: header -->`.
- ADR index table (`docs/adr/README.md`):
  `<!-- BEGIN GENERATED: adr-index ... -->` ... `<!-- END GENERATED: adr-index -->`.
  A missing index is never scaffolded by `--write`; only an existing index's
  region is rewritten.
