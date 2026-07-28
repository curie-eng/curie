"""Source-derived counts embedded in seam-catalog prose (#938).

ADR-0043's gate keeps the catalog's *citations*, *grades* and *generated
regions* honest, but a seam doc also states bare counts of things in the tree
-- "nine runner modules import ``claude_agent_sdk``", "32 committed schemas".
Those were hand-written prose that nothing recomputed, so the count-drift class
fired twice (#858, then #920's residual) before this module existed: a tenth
importing module or a thirty-third schema left the doc asserting the opposite
of reality while the gate stayed green.

Each claim below pairs a **regex that locates the number in the prose** with a
**counter that derives the truth from the tree**. There are three ways to
fail, all loud:

- the stated count disagrees with the tree -- the drift the class is named for;
- the anchor phrase is gone, so the regex matches nothing;
- (#956) the claim's doc no longer resolves under the catalog root -- moved or
  renamed away, which would otherwise let a false count go unreachable.

The second and third are what keep the gate from going vacuous. A reworded
sentence, or a doc moved out from under its claim, would otherwise silently
stop being checked, leaving a green gate over unverified prose -- which is
exactly the failure mode #938 was filed about, one level up. So rewording one
of these sentences, or renaming one of these docs, is a deliberate edit here
too, and the finding says so.

Counts are matched in either spelling: the prose writes small numbers as words
("nine runner modules") and larger ones as digits ("32 committed schemas"), so
both forms parse and either is accepted for the same value.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .finding import Finding

# Number words the prose plausibly spells out. Past twenty the docs use digits,
# and an unparseable token is reported rather than guessed at, so this map only
# has to cover the range the house style actually writes as words.
_NUMBER_WORDS: dict[str, int] = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}

# A module counts as importing the SDK when it says so at import level, which
# is the claim the prose makes ("imported across N runner modules"). A passing
# mention in a comment or docstring is not an import and must not inflate it.
_SDK_IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+claude_agent_sdk", re.MULTILINE)

# Rust's plain unit-test attribute. `json_contract.rs` uses only this form
# today; an async variant (`#[tokio::test]`) would need adding here as well as
# to whatever the prose then claims.
_RUST_TEST_RE = re.compile(r"#\[test\]")

# The index is a listing OF the schemas, not one of them, so the count the
# prose states ("32 committed schemas ... with an index") excludes it.
_SCHEMA_INDEX_NAME = "index.json"

# This module's own path *within* the repository, used as the marker that
# identifies a linted tree as the catalog.
#
# CLAIMS below are literals about THIS repository -- its own doc paths, its
# own counters -- so they only bind when the tree being linted IS this
# repository. Anywhere else (the doclint fixtures, a test's tmp tree) is a
# miniature tree that legitimately carries none of the claimed docs, and a
# missing doc there is not a defect.
#
# The question is asked of the LINT TARGET -- does the tree being linted carry
# the doclint source? -- not of this module's own `__file__`. The two answers
# differ under a built wheel, where `__file__` resolves into the virtualenv
# rather than into any checkout, so a location-derived root would match no tree
# at all and every claim would silently go back to being skipped: #956 rebuilt
# one level up, with a green gate over unverified prose. Asking the target
# answers identically from an editable workspace and from a wheel.
_CATALOG_MARKER = Path("tools/doclint/src/curie_doclint/counts.py")


def _is_catalog_root(repo_root: Path) -> bool:
    """Report whether the tree being linted is the repository ``CLAIMS`` describe.

    Args:
        repo_root: The repository root being linted.

    Returns:
        ``True`` when that tree carries the doclint source at [`_CATALOG_MARKER`],
        which only this repository does -- so a claimed doc missing there was
        moved or renamed away rather than never present.
    """
    return (repo_root / _CATALOG_MARKER).is_file()


def parse_count(token: str) -> int | None:
    """Read a count written as digits or as an English number word.

    Args:
        token: The matched prose token, e.g. ``"32"`` or ``"nine"``.

    Returns:
        The integer value, or ``None`` when the token is neither digits nor a
        known number word -- which the caller reports rather than guessing.
    """
    if token.isdigit():
        return int(token)
    return _NUMBER_WORDS.get(token.lower())


def _sdk_importing_runner_module_names(repo_root: Path) -> set[str]:
    """The runner modules that import ``claude_agent_sdk`` at import level.

    This is the leakage the harness seam grades itself on: every module here is
    one the SDK is not yet walled off from. The prose names them as bare file
    names (``check.py``), so that is what this returns.

    Args:
        repo_root: The repository root to resolve ``runner/src`` against.

    Returns:
        The set of ``.py`` file names under ``runner/src`` carrying an SDK import.
    """
    src = repo_root / "runner" / "src"
    return {
        path.name
        for path in sorted(src.rglob("*.py"))
        if _SDK_IMPORT_RE.search(path.read_text(encoding="utf-8"))
    }


def _count_sdk_importing_runner_modules(repo_root: Path) -> int:
    """Count runner modules that import ``claude_agent_sdk`` at import level.

    Derived from [`_sdk_importing_runner_module_names`] rather than counted
    separately, so the count claim and the name claim below cannot disagree with
    each other about the same tree (#1019).

    Args:
        repo_root: The repository root to resolve ``runner/src`` against.

    Returns:
        The number of ``.py`` files under ``runner/src`` carrying an SDK import.
    """
    return len(_sdk_importing_runner_module_names(repo_root))


def _count_committed_cli_schemas(repo_root: Path) -> int:
    """Count the committed CLI result schemas, excluding their index.

    Args:
        repo_root: The repository root to resolve ``cli/schema`` against.

    Returns:
        The number of ``*.json`` schema files under ``cli/schema``, not
        counting ``index.json``, which lists them rather than being one.
    """
    schema_dir = repo_root / "cli" / "schema"
    return sum(1 for path in schema_dir.glob("*.json") if path.name != _SCHEMA_INDEX_NAME)


def _count_json_contract_tests(repo_root: Path) -> int:
    """Count the tests validating real ``to_json()`` output against the schemas.

    Args:
        repo_root: The repository root to resolve the test file against.

    Returns:
        The number of ``#[test]`` attributes in ``cli/tests/json_contract.rs``,
        or ``0`` when that file is absent.
    """
    path = repo_root / "cli" / "tests" / "json_contract.rs"
    if not path.is_file():
        return 0
    return len(_RUST_TEST_RE.findall(path.read_text(encoding="utf-8")))


@dataclass(frozen=True)
class CountClaim:
    """One count a seam doc states in prose, tied to its source of truth.

    ``pattern`` must capture the count token in group 1 and may match more than
    once -- a doc that repeats a count (the harness seam states its module
    count twice) has every occurrence checked, so the two cannot disagree.
    """

    doc: str
    label: str
    pattern: re.Pattern[str]
    counter: Callable[[Path], int]
    source: str


# Every count claim the seam catalog makes. Patterns are anchored on enough
# surrounding words to hit only the claim: `committed schemas under` excludes
# the same file's later "enforced by committed schemas and a drift gate", which
# is prose about the schemas rather than a count of them.
CLAIMS: tuple[CountClaim, ...] = (
    CountClaim(
        doc="docs/interfaces/harness-modelsession/INTERFACE.md",
        label="runner modules importing claude_agent_sdk",
        pattern=re.compile(r"(\S+)\s+runner\s+modules"),
        counter=_count_sdk_importing_runner_modules,
        source="`from`/`import claude_agent_sdk` in runner/src/**/*.py",
    ),
    CountClaim(
        doc="docs/interfaces/cli-output/INTERFACE.md",
        label="committed CLI result schemas",
        pattern=re.compile(r"(\S+)\s+committed schemas under"),
        counter=_count_committed_cli_schemas,
        source="cli/schema/*.json excluding index.json",
    ),
    CountClaim(
        doc="docs/interfaces/cli-output/INTERFACE.md",
        label="json_contract output-validation tests",
        pattern=re.compile(r"across\s+(\S+)\s+tests\s+in\s+`cli/tests/json_contract\.rs`"),
        counter=_count_json_contract_tests,
        source="#[test] in cli/tests/json_contract.rs",
    ),
)


@dataclass(frozen=True)
class NameSetClaim:
    """One ENUMERATION a seam doc states in prose, tied to its source of truth.

    A count claim above pins how many; this pins which. #952 gated the harness
    doc's "nine runner modules" but not the nine names listed beside it, so two
    edits kept the gate green while making the doc wrong: renaming one of the
    modules, or moving the import from one module to another. The second is the
    likely one, because confining these imports is #844 Phase 2's refactor and
    moving imports between modules is precisely what it does (#1019).

    ``pattern`` must capture the region holding the list in group 1; the names
    are then read out of it as backticked tokens, so re-ordering or re-wrapping
    the sentence does not matter but changing the membership does.
    """

    doc: str
    label: str
    pattern: re.Pattern[str]
    lister: Callable[[Path], set[str]]
    source: str


# A backticked file name inside the enumerated region.
_BACKTICKED_NAME_RE = re.compile(r"`([^`]+\.py)`")

# Every enumeration claim the seam catalog makes.
NAME_CLAIMS: tuple[NameSetClaim, ...] = (
    NameSetClaim(
        doc="docs/interfaces/harness-modelsession/INTERFACE.md",
        label="the runner modules importing claude_agent_sdk, by name",
        # The parenthesised run after the claim sentence. DOTALL because the
        # list wraps across lines in the prose.
        pattern=re.compile(r"claude_agent_sdk`\s+today\s+\(([^)]*)\)", re.DOTALL),
        lister=_sdk_importing_runner_module_names,
        source="`from`/`import claude_agent_sdk` in runner/src/**/*.py",
    ),
)


def check_name_sets(
    repo_root: Path, claims: tuple[NameSetClaim, ...] = NAME_CLAIMS
) -> list[Finding]:
    """Assert every prose ENUMERATION in the seam catalog against the tree (#1019).

    The count-claim sibling above answers "how many"; this answers "which", so a
    rename or a swap that preserves the count cannot leave the list wrong while
    the gate stays green.

    Args:
        repo_root: The repository root the docs and sources are resolved under.
        claims: The claims to check; defaults to the catalog's own [`NAME_CLAIMS`].

    Returns:
        One finding per claim whose listed names disagree with the tree, or whose
        anchor phrase vanished, and an empty list when every list matches.
    """
    findings: list[Finding] = []
    is_catalog = _is_catalog_root(repo_root)

    for claim in claims:
        doc = repo_root / claim.doc
        if not doc.is_file():
            if is_catalog:
                findings.append(
                    Finding(
                        claim.doc,
                        claim.label,
                        f"the seam doc has moved, so this claim ({claim.source}) is "
                        "no longer verified. Restore the doc at this path, or update "
                        f"this claim's doc path in {_CATALOG_MARKER}.",
                    )
                )
            continue

        regions = claim.pattern.findall(doc.read_text(encoding="utf-8"))
        if not regions:
            # Same anti-vacuity rule the count claims use: a reworded sentence
            # must fail loudly rather than quietly stop being checked.
            findings.append(
                Finding(
                    claim.doc,
                    claim.label,
                    "no sentence enumerates these any more, so the list is no longer "
                    "verified. Restore the phrasing, or update this claim's pattern "
                    f"in {_CATALOG_MARKER} "
                    f"(source of truth: {claim.source}).",
                )
            )
            continue

        expected = claim.lister(repo_root)
        for region in regions:
            listed = set(_BACKTICKED_NAME_RE.findall(region))
            missing = sorted(expected - listed)
            extra = sorted(listed - expected)
            if not missing and not extra:
                continue
            parts = []
            if missing:
                parts.append(f"the tree has {', '.join(missing)} but the prose omits them")
            if extra:
                parts.append(f"the prose lists {', '.join(extra)} but the tree does not")
            findings.append(
                Finding(
                    claim.doc,
                    claim.label,
                    f"{'; '.join(parts)} ({claim.source}). Update the prose to match.",
                )
            )

    return findings


def check_counts(repo_root: Path, claims: tuple[CountClaim, ...] = CLAIMS) -> list[Finding]:
    """Assert every prose count in the seam catalog against the tree (#938).

    Args:
        repo_root: The repository root the docs and sources are resolved under.
        claims: The claims to check; defaults to the catalog's own [`CLAIMS`].

    Returns:
        One finding per claim that drifted, whose anchor phrase vanished, or
        (when ``repo_root`` is the catalog root) whose doc no longer resolves,
        and an empty list when every stated count matches the tree.
    """
    findings: list[Finding] = []
    is_catalog = _is_catalog_root(repo_root)

    for claim in claims:
        doc = repo_root / claim.doc
        if not doc.is_file():
            if is_catalog:
                # CLAIMS are literals about this repository, so when repo_root
                # IS the catalog root a missing doc was moved or renamed away,
                # not legitimately absent -- the #956 defect: the skip below
                # let a renamed seam doc launder a false count past the gate.
                findings.append(
                    Finding(
                        claim.doc,
                        claim.label,
                        f"the seam doc has moved, so this claim ({claim.source}) is "
                        "no longer verified. Restore the doc at this path, or update "
                        f"this claim's doc path in {_CATALOG_MARKER}.",
                    )
                )
            # Not every repo root carries every seam doc -- the doclint
            # fixtures and a test's tmp tree are miniature trees. Whether a
            # seam doc should exist at all is the catalog's own concern, not a
            # count claim's, so outside the catalog root a missing doc is
            # skipped rather than reported here twice over.
            continue

        stated = claim.pattern.findall(doc.read_text(encoding="utf-8"))
        if not stated:
            findings.append(
                Finding(
                    claim.doc,
                    claim.label,
                    "no sentence states this count any more, so it is no longer "
                    "verified. Restore the phrasing, or update this claim's pattern "
                    f"in {_CATALOG_MARKER} "
                    f"(source of truth: {claim.source}).",
                )
            )
            continue

        expected = claim.counter(repo_root)
        # A set: a repeated count that is wrong the same way twice is one
        # drift to fix, not two findings to read.
        wrong = sorted({token for token in stated if parse_count(token) != expected})
        if wrong:
            findings.append(
                Finding(
                    claim.doc,
                    claim.label,
                    f"prose states {', '.join(repr(token) for token in wrong)} but the "
                    f"tree has {expected} ({claim.source}). Update the prose to match.",
                )
            )

    return findings
