"""The interface-catalog citation linter and index/header generator (#452).

Four phases run over the linted root (``docs/`` excluding ``docs/adr/``):

1. Generate: regenerate the seam table in ``docs/interfaces.md`` and each seam
   doc's header blockquote from front-matter, comparing to what is on disk.
2. Counts: assert the bare counts a seam doc states about the tree ("nine
   runner modules import ``claude_agent_sdk``") against the tree itself, so
   prose numbers cannot silently re-drift (#938, see ``counts.py``).
3. Commands: resolve every ``curie ...`` invocation the published verification
   contract (``docs/agents.md``) and the SRE demo runbook
   (``examples/sre-bot/DEMO.md``) name against the committed
   ``cli/command-manifest.json``, so those docs cannot name a command that no
   longer exists (#1041, #2247, see ``commands.py``).
4. Lint: walk every citation, assert no line coordinates, every path exists,
   every Python symbol resolves.

``main`` is a pure check by default (drift is a finding, nothing is written);
``main --write`` rewrites the generated regions so ``scripts/check-docs.sh`` can
diff them, mirroring ``scripts/check-contracts.sh``.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from .citation import (
    SOURCE_EXTENSIONS,
    Classification,
    CodeSpan,
    classify,
    find_code_spans,
    find_field_enumerations,
    is_line_suppressed,
    link_target_matches_citation,
    path_exists,
    scan_raw_line_ban,
)
from .commands import check_agent_contract
from .counts import check_counts, check_name_sets
from .finding import Finding
from .frontmatter import SeamMeta, parse_and_validate
from .generate import (
    ADR_INDEX_REL,
    INDEX_REL,
    OrderTieError,
    adr_number,
    extract_region,
    iter_adr_docs,
    iter_seam_docs,
    render_adr_table,
    render_header_block,
    render_index_table,
    render_table,
    replace_region,
    seam_link,
)
from .mcp_workspace_live_tiers import check_mcp_workspace_live_tiers
from .symbols import SymbolCache, SymbolSyntaxError, dataclass_fields, resolve_symbol
from .vision import VISION_REL, read_swap_readiness

__all__ = [
    "SOURCE_EXTENSIONS",
    "Finding",
    "lint",
    "main",
    "render_adr_table",
    "render_index_table",
]

_ADR_REL = "docs/adr"

# Repo-root docs inside the linted root, named one by one (#541, AC A).
#
# Deliberately an allowlist, not ``repo_root.glob("*.md")``: AC A asks for "at
# minimum ARCHITECTURE.md", and a glob would silently pull five unaudited root
# docs into the gate. Adding a root doc is one entry here plus whatever
# corrections that doc then needs -- a deliberate, separately-reviewed decision.
_ROOT_DOCS = ("ARCHITECTURE.md",)

# The sentinel eleven of the seventeen seams carry instead of a grade. They
# have no swap-readiness row by design and stay out of the grade check.
_UNGRADED = "not separately graded"


def _git_top_level() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip())


def lint(repo_root: Path) -> list[Finding]:
    """Run the generate + lint phases in memory and return the findings."""
    findings, _ = _lint_and_count(repo_root)
    return findings


def _lint_and_count(repo_root: Path) -> tuple[list[Finding], int]:
    """Return the findings plus the count of lines silenced by the escape hatch."""
    cache = SymbolCache()
    findings: list[Finding] = []
    findings.extend(_check_generation(repo_root))
    # Prose counts the catalog states about the tree (#938). Not a generated
    # region: the numbers sit mid-sentence, so they are asserted against source
    # rather than rewritten, and `--write` leaves them alone.
    findings.extend(check_counts(repo_root))
    # The same prose, gated on WHICH modules rather than how many (#1028). A
    # count and a name set drift independently: swapping one importing module
    # for another holds the count and makes the list wrong.
    findings.extend(check_name_sets(repo_root))
    # The published verification contract names commands an agent is told to
    # trust (#1041). Same class as check_counts: prose asserted against
    # machine-readable ground truth, never rewritten, so `--write` leaves it
    # alone.
    findings.extend(check_agent_contract(repo_root))
    # Implement-workflow live-tier rule (#2305). Same class as check_counts:
    # required sentences asserted against the workflow docs, never rewritten.
    findings.extend(check_mcp_workspace_live_tiers(repo_root))
    doc_findings, ignored = _check_docs(repo_root, cache)
    findings.extend(doc_findings)
    return findings, ignored


def _collect_seam_rows(
    repo_root: Path,
) -> tuple[list[tuple[Path, str, str, SeamMeta]], list[Finding]]:
    """Parse every seam doc's front-matter once.

    Returns the docs that parsed cleanly, each as ``(doc, rel, text, meta)``,
    plus findings for any doc whose front-matter failed to parse. Shared by
    the check phase and the write phase so their seam-row loops cannot drift
    out of sync with each other.
    """
    findings: list[Finding] = []
    seam_docs: list[tuple[Path, str, str, SeamMeta]] = []

    for doc in iter_seam_docs(repo_root):
        rel = doc.relative_to(repo_root).as_posix()
        text = doc.read_text(encoding="utf-8")
        meta, errors = parse_and_validate(text)
        if meta is None:
            findings.extend(Finding(rel, err.field, err.reason) for err in errors)
            continue
        seam_docs.append((doc, rel, text, meta))

    return seam_docs, findings


def _render_table_or_finding(
    rows: list[tuple[str, SeamMeta]],
) -> tuple[str | None, Finding | None]:
    """Render the seam table, converting an ``OrderTieError`` into a Finding.

    Returns ``(table, None)`` on success or ``(None, finding)`` on a tie, so
    the check phase and the write phase report a tie identically.
    """
    try:
        return render_table(rows), None
    except OrderTieError as exc:
        return None, Finding(INDEX_REL, "seam-table", f"order tie: {exc}")


def _check_generation(repo_root: Path) -> list[Finding]:
    seam_docs, findings = _collect_seam_rows(repo_root)
    rows: list[tuple[str, SeamMeta]] = []

    vision_grades = read_swap_readiness(repo_root)

    for doc, rel, text, meta in seam_docs:
        rows.append((seam_link(doc, repo_root), meta))
        findings.extend(_check_header_region(rel, text, meta))
        findings.extend(_check_grade(rel, meta, vision_grades))

    findings.extend(_check_index_region(repo_root, rows))

    collisions = _check_adr_uniqueness(repo_root)
    findings.extend(collisions)
    # A collided dir cannot produce a meaningful index -- two rows would claim
    # one number -- and reporting drift on top of the collision buries the one
    # finding that has a remedy. Fix the numbers, then the index follows.
    if not collisions:
        findings.extend(_check_adr_index_region(repo_root))
    return findings


def _adr_collisions(repo_root: Path) -> dict[str, list[str]]:
    """Group ADR filenames by their number prefix, keeping only the collisions."""
    by_number: dict[str, list[str]] = {}
    for doc in iter_adr_docs(repo_root):
        number = adr_number(doc)
        if number is not None:
            by_number.setdefault(number, []).append(doc.name)
    return {number: names for number, names in by_number.items() if len(names) > 1}


def _check_adr_uniqueness(repo_root: Path) -> list[Finding]:
    """No two ADRs may claim the same number prefix (#541, AC A).

    Reads **filenames**, deliberately: ``docs/adr/`` is excluded from the
    citation walk because an Accepted ADR is immutable and its citations are
    allowed to rot with the code they described. So this is the one check that
    looks at a directory whose contents the linter otherwise refuses to read.

    Both colliding names are reported. The whole remedy is deciding which file
    moves, and "0029 is duplicated" does not tell you that.
    """
    findings: list[Finding] = []
    for number, names in sorted(_adr_collisions(repo_root).items()):
        findings.append(
            Finding(
                _ADR_REL,
                number,
                f"ADR number {number} is claimed by {len(names)} files: "
                f"{', '.join(sorted(names))}; renumber all but one to the next "
                "free number and update every inbound reference",
            )
        )
    return findings


def _missing_adr_index() -> Finding:
    """The one message for an absent ADR index, shared by the check and write phases.

    Deliberately NOT recreated during generation, in either mode. ``--write``
    regenerates marker-delimited *regions inside* docs that exist; it has never
    authored a doc from scratch, and a scaffold here would replace the index's
    hand-written prose -- the "claiming a number" instructions that are the
    whole reason the file exists -- with a stub, papering over the deletion at
    exactly the moment it should be loudest. Deleting a required artifact is a
    human decision; both modes refuse it and say so.
    """
    return Finding(
        ADR_INDEX_REL,
        "adr-index",
        "the ADR index is missing; it is a required artifact -- without it the "
        "next free ADR number is an `ls` against whatever branch you are on, "
        "which is how numbers collide. Restore the file (its generated region "
        "is rebuilt by `curie dev docs-lint --write`)",
    )


def _missing_region_finding(rel: str, marker: str) -> Finding:
    """The one message for an absent or unpaired generated marker block.

    Six sites (three regions x check-and-write) built this verbatim. Only the
    message is shared: what each caller *does* around it stays put, because the
    surrounding semantics differ deliberately and are load-bearing -- the seam
    table skips a missing index, the ADR index hard-fails on one, and the write
    phase continues past a bad header rather than rewriting it.
    """
    return Finding(rel, marker, "generated marker block is missing or unpaired")


def _missing_root_doc(name: str) -> Finding:
    """The one message for an absent allowlisted repo-root doc.

    Sibling of ``_missing_adr_index``: both name a required artifact whose
    disappearance must be louder than its drift, never quietly dropped from the
    lint list. Unlike the ADR index this doc has no generated region, so there
    is nothing for ``--write`` to rebuild -- the remedy is entirely a restore.
    """
    return Finding(
        name,
        "root-doc",
        f"{name} is missing; it is a required artifact -- README.md and llms.txt "
        "point release readers at it, and it is on the linted-root allowlist, so "
        "its absence silently empties the gate for it rather than failing. "
        "Restore the file, or drop it from the allowlist deliberately",
    )


def _check_adr_index_region(repo_root: Path) -> list[Finding]:
    index_path = repo_root / ADR_INDEX_REL
    if not index_path.is_file():
        return [_missing_adr_index()]
    text = index_path.read_text(encoding="utf-8")
    region = extract_region(text, "adr-index")
    if region.missing:
        return [_missing_region_finding(ADR_INDEX_REL, "adr-index")]
    if region.content.strip() != render_adr_table(repo_root).strip():
        return [Finding(ADR_INDEX_REL, "adr-index", "ADR index is out of date; regenerate")]
    return []


def _check_grade(rel: str, meta: SeamMeta, vision_grades: dict[str, str]) -> list[Finding]:
    """A graded seam's ``grade:`` must equal its declared vision row's grade.

    Both the missing-key and the unknown-row cases are findings, never skips: a
    skip would leave the seam looking checked while its grade rots unguarded,
    which is the exact defect this check exists to catch. A row shared by two
    seams (``aci-producer`` and ``harness-modelsession`` both answer to
    "Harness / runtime") is the authority for each of them independently,
    because every seam is checked on its own here.
    """
    if meta.grade == _UNGRADED:
        return []

    if meta.vision_row is None:
        return [
            Finding(
                rel,
                "vision_row",
                f"seam '{meta.seam}' declares grade '{meta.grade}' but no "
                f"vision_row:; a graded seam must name the {VISION_REL} "
                "swap-readiness row its grade answers to",
            )
        ]

    if meta.vision_row not in vision_grades:
        known = ", ".join(sorted(vision_grades)) or "none"
        return [
            Finding(
                rel,
                "vision_row",
                f"seam '{meta.seam}' names swap-readiness row "
                f"'{meta.vision_row}', which is not in {VISION_REL}; "
                f"known rows: {known}",
            )
        ]

    expected = vision_grades[meta.vision_row]
    if meta.grade != expected:
        return [
            Finding(
                rel,
                "grade",
                f"seam '{meta.seam}' says grade '{meta.grade}' but its named "
                f"authority, the '{meta.vision_row}' row of {VISION_REL}, says "
                f"'{expected}'",
            )
        ]
    return []


def _check_header_region(rel: str, text: str, meta: SeamMeta) -> list[Finding]:
    region = extract_region(text, "header")
    if region.missing:
        return [_missing_region_finding(rel, "header")]
    if region.content.strip() != render_header_block(meta).strip():
        return [Finding(rel, "header", "generated header block is out of date; regenerate")]
    return []


def _check_index_region(repo_root: Path, rows: list[tuple[str, SeamMeta]]) -> list[Finding]:
    index_path = repo_root / INDEX_REL
    if not index_path.is_file():
        return []
    text = index_path.read_text(encoding="utf-8")
    region = extract_region(text, "seam-table")
    if region.missing:
        return [_missing_region_finding(INDEX_REL, "seam-table")]
    expected, tie_finding = _render_table_or_finding(rows)
    if tie_finding is not None:
        return [tie_finding]
    if expected is not None and region.content.strip() != expected.strip():
        return [Finding(INDEX_REL, "seam-table", "index seam-table is out of date; regenerate")]
    return []


def _check_docs(repo_root: Path, cache: SymbolCache) -> tuple[list[Finding], int]:
    docs, findings = _linted_docs(repo_root)
    ignored = 0

    for md in docs:
        rel = md.relative_to(repo_root).as_posix()
        text = md.read_text(encoding="utf-8")
        doc_findings, doc_ignored = _lint_doc(repo_root, rel, text, cache)
        findings.extend(doc_findings)
        ignored += doc_ignored

    return findings, ignored


def _linted_docs(repo_root: Path) -> tuple[list[Path], list[Finding]]:
    """Every doc inside the linted root, plus a finding per absent root doc.

    ``docs/`` (minus ADRs) is a walk: whatever is there is what gets linted, and
    an empty tree is a legitimate state. ``_ROOT_DOCS`` is the opposite -- a
    named allowlist, so every entry is a *required* artifact and a name that
    does not resolve is a deletion, not an absence. Filtering those out with
    ``.is_file()`` is what let ``ARCHITECTURE.md`` be deleted for zero findings
    and exit 0: the gate verified what was written and never what was missing,
    which is the defect species #541 exists to close. It is reported here rather
    than scaffolded, for the same reason as ``_missing_adr_index``: restoring a
    required doc is a human decision.
    """
    docs_root = repo_root / "docs"
    adr_root = repo_root / _ADR_REL

    docs: list[Path] = []
    if docs_root.is_dir():
        docs.extend(md for md in sorted(docs_root.rglob("*.md")) if not _is_under(md, adr_root))

    findings: list[Finding] = []
    for name in _ROOT_DOCS:
        md = repo_root / name
        if md.is_file():
            docs.append(md)
        else:
            findings.append(_missing_root_doc(name))
    return docs, findings


def _lint_doc(
    repo_root: Path, rel: str, text: str, cache: SymbolCache
) -> tuple[list[Finding], int]:
    """Lint one doc, then apply the per-line escape hatch uniformly.

    Every finding type (line-ban, path, symbol, shorthand) is produced first,
    then a finding whose line is silenced by ``<!-- doclint:ignore-line -->``
    (on the preceding line, or inline at the end of that same line) is dropped.
    The count is of distinct suppressed physical lines, so the summary reflects
    genuinely-silenced findings, not bare comments.
    """
    lines = text.splitlines()
    raw: list[Finding] = []

    for hit in scan_raw_line_ban(text):
        raw.append(
            Finding(
                rel,
                hit.coordinate,
                "line-number coordinate citation is banned; cite a symbol instead",
                line=hit.line,
            )
        )

    for span in find_code_spans(text):
        raw.extend(_check_span(repo_root, rel, span, cache))

    raw.extend(_check_field_enumerations(repo_root, rel, text, cache))

    kept: list[Finding] = []
    suppressed_lines: set[int] = set()
    for finding in raw:
        if finding.line is not None and is_line_suppressed(lines, finding.line):
            suppressed_lines.add(finding.line)
        else:
            kept.append(finding)

    return kept, len(suppressed_lines)


def _check_span(repo_root: Path, rel: str, span: CodeSpan, cache: SymbolCache) -> list[Finding]:
    classification = classify(span.content)
    if classification.kind == "not_a_citation":
        return []
    if classification.kind == "shorthand_error":
        return [
            Finding(
                rel,
                span.content,
                "shorthand citation has a symbol but no path; write the full "
                "repo-relative path (citations resolve from the repo root, "
                "never relative to the doc)",
                line=span.line,
            )
        ]
    return _check_citation(repo_root, rel, span, classification, cache)


def _check_citation(
    repo_root: Path,
    rel: str,
    span: CodeSpan,
    classification: Classification,
    cache: SymbolCache,
) -> list[Finding]:
    if not path_exists(repo_root, classification.path):
        return [Finding(rel, span.content, "cited path does not exist", line=span.line)]

    findings: list[Finding] = []
    if span.href is not None and not link_target_matches_citation(
        repo_root, rel, classification.path, span.href
    ):
        findings.append(
            Finding(
                rel,
                span.content,
                f"link target '{span.href}' does not point at the cited path "
                f"'{classification.path}'; a decorated citation's link must "
                "navigate to the file it cites",
                line=span.line,
            )
        )

    if not classification.symbol:
        return findings

    extension = classification.path.rsplit(".", 1)[-1]
    if extension != "py":
        findings.append(
            Finding(
                rel,
                span.content,
                f"symbol resolution is not supported for .{extension}; cite the file only",
                line=span.line,
            )
        )
        return findings

    try:
        resolved = resolve_symbol(repo_root / classification.path, classification.symbol, cache)
    except SymbolSyntaxError:
        findings.append(
            Finding(rel, span.content, "cited file has a syntax error", line=span.line)
        )
        return findings
    if not resolved:
        findings.append(
            Finding(
                rel,
                span.content,
                f"symbol '{classification.symbol}' does not resolve",
                line=span.line,
            )
        )
    return findings


def _check_field_enumerations(
    repo_root: Path, rel: str, text: str, cache: SymbolCache
) -> list[Finding]:
    """A sealed ``(`Class`: `a`, `b`, ...)`` field list must match the cited
    class's actual annotated fields.

    #573 deleted ``ClaimView.labels`` from the dataclass and both adapters but
    left the substrate seam doc's field list intact, so a second implementer
    building to the documented contract gets a ``TypeError`` on construction.
    The citation still resolved, so citation resolution alone never caught the
    stale field (#615). This complements the link-text-vs-target check by gating
    the other described-shape drift: a documented field list. Compared as sets,
    so field REORDERING is allowed; only extra/missing fields are flagged."""
    findings: list[Finding] = []
    for enum in find_field_enumerations(text):
        classification = classify(enum.citation)
        if classification.kind != "citation" or not classification.symbol:
            continue
        if not classification.path.endswith(".py"):
            continue
        if not path_exists(repo_root, classification.path):
            continue  # a missing path is the citation walk's finding, not ours
        try:
            actual = dataclass_fields(
                repo_root / classification.path, classification.symbol, cache
            )
        except SymbolSyntaxError:
            continue  # a syntax error is the citation walk's finding
        if not actual:
            continue  # not a field-bearing dataclass -> nothing to compare
        documented = set(enum.fields)
        real = set(actual)
        if documented == real:
            continue
        extra = sorted(documented - real)
        missing = sorted(real - documented)
        parts: list[str] = []
        if extra:
            parts.append(f"documents field(s) not on the class: {', '.join(extra)}")
        if missing:
            parts.append(f"omits actual field(s): {', '.join(missing)}")
        findings.append(
            Finding(
                rel,
                enum.citation,
                "documented field list does not match the cited class; " + "; ".join(parts),
                line=enum.line,
            )
        )
    return findings


def _is_under(path: Path, ancestor: Path) -> bool:
    try:
        path.relative_to(ancestor)
    except ValueError:
        return False
    return True


def _write_generated(repo_root: Path) -> list[Finding]:
    """Rewrite the generated regions in place; return blocking findings."""
    seam_docs, findings = _collect_seam_rows(repo_root)
    rows: list[tuple[str, SeamMeta]] = []

    for doc, rel, text, meta in seam_docs:
        rows.append((seam_link(doc, repo_root), meta))
        region = extract_region(text, "header")
        if region.missing:
            findings.append(_missing_region_finding(rel, "header"))
            continue
        rewritten = replace_region(text, "header", render_header_block(meta))
        if rewritten != text:
            doc.write_text(rewritten, encoding="utf-8")

    index_path = repo_root / INDEX_REL
    if index_path.is_file():
        text = index_path.read_text(encoding="utf-8")
        region = extract_region(text, "seam-table")
        if region.missing:
            findings.append(_missing_region_finding(INDEX_REL, "seam-table"))
        else:
            table, tie_finding = _render_table_or_finding(rows)
            if tie_finding is not None:
                findings.append(tie_finding)
            elif table is not None:
                rewritten = replace_region(text, "seam-table", table)
                if rewritten != text:
                    index_path.write_text(rewritten, encoding="utf-8")

    findings.extend(_write_adr_index(repo_root))
    return findings


def _write_adr_index(repo_root: Path) -> list[Finding]:
    """Rewrite the ADR index in place; a collided dir is left untouched.

    Regenerating over a collision would silently emit two rows claiming one
    number, which is exactly the state the uniqueness check exists to surface.
    An absent index is reported, never scaffolded -- see ``_missing_adr_index``.
    """
    index_path = repo_root / ADR_INDEX_REL
    if not index_path.is_file():
        return [_missing_adr_index()]

    collisions = _check_adr_uniqueness(repo_root)
    if collisions:
        return collisions

    text = index_path.read_text(encoding="utf-8")
    region = extract_region(text, "adr-index")
    if region.missing:
        return [_missing_region_finding(ADR_INDEX_REL, "adr-index")]
    rewritten = replace_region(text, "adr-index", render_adr_table(repo_root))
    if rewritten != text:
        index_path.write_text(rewritten, encoding="utf-8")
    return []


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="curie_doclint")
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repo root to lint; defaults to the git top-level.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Rewrite the generated regions in place instead of checking drift.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve() if args.repo_root else _git_top_level()

    if args.write:
        findings = _write_generated(repo_root)
        ignored = 0
    else:
        findings, ignored = _lint_and_count(repo_root)

    for finding in findings:
        print(finding.render())

    if ignored:
        print(f"doclint: {ignored} line(s) suppressed by the ignore-line escape hatch")

    return 0 if not findings else 1
