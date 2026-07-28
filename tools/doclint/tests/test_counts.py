"""The seam-catalog count gate (#938).

The counts a seam doc states about the tree ("nine runner modules import
``claude_agent_sdk``", "32 committed schemas") were prose nothing recomputed,
so the drift class fired twice -- #858, then #920's residual -- each time fixed
by hand. These drive the real ``CLAIMS`` (real patterns, real counters) over
miniature trees, so a pattern that stops matching the house phrasing fails here
rather than in six months' review.

Three failure modes matter equally and all are asserted: a count that disagrees
with the tree, an anchor phrase that vanished so nothing is checked at all, and
(#956) a seam doc that moved out from under its claim so the claim was skipped
entirely. The last two are the vacuity guards -- a silently-unchecked claim is
the defect one level up from the one #938 reports.
"""

from __future__ import annotations

from pathlib import Path

from curie_doclint import counts
from curie_doclint.counts import (
    CLAIMS,
    _CATALOG_MARKER,
    check_counts,
    check_name_sets,
    parse_count,
)

from .conftest import REPO_ROOT, Regenerate, RunLint, write

_HARNESS_DOC = "docs/interfaces/harness-modelsession/INTERFACE.md"
_CLI_OUTPUT_DOC = "docs/interfaces/cli-output/INTERFACE.md"


def _sdk_module(name: str) -> tuple[str, str]:
    """A runner module that imports the harness SDK at import level."""
    return f"runner/src/curie_runner/{name}.py", "from claude_agent_sdk import Thing\n"


def _harness_names_prose(names: str) -> str:
    """The harness sentence in the form that ENUMERATES the modules (#1019).

    Matches the real doc's shape: the count, then the parenthesised backticked
    list after "today", which is what the name claim reads.
    """
    return (
        "CLEAN, but the SDK is not yet confined to one module: two runner modules\n"
        f"still import `claude_agent_sdk` today ({names}), and the value that crosses.\n"
    )


def _harness_prose(count: str) -> str:
    """The harness seam's sentence, in the house phrasing, stating ``count``."""
    return (
        "CLEAN, but the SDK is not yet confined to one module: "
        f"{count} runner modules\nstill import it today.\n"
    )


def _cli_output_prose(schemas: str, tests: str) -> str:
    """The cli-output seam's two sentences, in the house phrasing.

    Both are written every time: the doc carries two claims, so omitting one
    would (rightly) trip its vacuity guard and bury the claim under test.
    """
    return (
        f"There are {schemas} committed schemas under `cli/schema/` with an index.\n"
        f"All are validated against real `to_json()` output across {tests} tests in\n"
        "`cli/tests/json_contract.rs`.\n"
        "An agent is coupled to shapes enforced by committed schemas and a drift gate.\n"
    )


# --- the count disagrees with the tree: the named drift class --------------


def test_matching_count_passes(tmp_path: Path) -> None:
    # Positive control. Without this the suite could pass by reporting
    # everything, which is no gate either.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, *_sdk_module("b"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("two"))
    assert check_counts(tmp_path) == []


def test_drifted_count_is_reported(tmp_path: Path) -> None:
    # The #858/#920 recurrence itself: a third importing module lands and the
    # prose still says two. Before this gate, doc-lint stayed green here.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, *_sdk_module("b"))
    write(tmp_path, *_sdk_module("c"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("two"))
    findings = check_counts(tmp_path)
    assert len(findings) == 1
    assert "'two'" in findings[0].reason
    assert "3" in findings[0].reason


def test_digits_and_number_words_are_both_accepted(tmp_path: Path) -> None:
    # The house style spells small counts as words and larger ones as digits,
    # so the same value must pass written either way.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, *_sdk_module("b"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("2"))
    assert check_counts(tmp_path) == []
    assert parse_count("nine") == 9
    assert parse_count("32") == 32
    assert parse_count("many") is None


def test_a_repeated_count_must_agree_in_every_place(tmp_path: Path) -> None:
    # The real harness doc states its module count twice, in two different
    # sentences. Checking only the first would let the second rot, which is
    # how a "fixed" doc stays half wrong.
    write(tmp_path, *_sdk_module("a"))
    doc = _harness_prose("one") + "\nElsewhere: imported across seven runner modules.\n"
    write(tmp_path, _HARNESS_DOC, doc)
    findings = check_counts(tmp_path)
    assert len(findings) == 1
    assert "'seven'" in findings[0].reason


# --- the anchor vanished: the vacuity guard --------------------------------


def test_reworded_sentence_fails_rather_than_going_vacuous(tmp_path: Path) -> None:
    # THE most important test here. A reworded sentence must not silently stop
    # being checked -- a green gate over unverified prose is the failure mode
    # #938 exists to close, and a skip would rebuild it one level up.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, _HARNESS_DOC, "The SDK leaks into several modules today.\n")
    findings = check_counts(tmp_path)
    assert len(findings) == 1
    assert "no longer verified" in findings[0].reason
    # The remedy has to name where the claim lives or it is not actionable,
    # and that path is interpolated rather than spelled out -- so assert it,
    # else a dropped interpolation would render a broken sentence in silence.
    assert str(_CATALOG_MARKER) in findings[0].reason


def test_absent_seam_doc_is_skipped(tmp_path: Path) -> None:
    # A repo root that carries no seam docs at all (the miniature fixture tree)
    # has no claims to check. Whether a seam doc should exist is the catalog's
    # own concern, reported there, not duplicated as a count finding.
    write(tmp_path, *_sdk_module("a"))
    assert check_counts(tmp_path) == []


# --- the seam doc moved: the catalog-root guard (#956) ---------------------


def test_a_missing_seam_doc_is_reported_under_the_catalog_root(tmp_path: Path) -> None:
    # THE #956 defect. `CLAIMS` are literals about THIS repository, so when the
    # tree being linted IS that repository a claim whose doc does not resolve
    # was moved or renamed away, not legitimately absent. Skipping it there is
    # how renaming `docs/interfaces/cli-output/` left the gate green over prose
    # counts of 999 and 777.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("one"))
    write(tmp_path, str(_CATALOG_MARKER), "")
    findings = check_counts(tmp_path)
    assert [(finding.doc, finding.citation) for finding in findings] == [
        (_CLI_OUTPUT_DOC, "committed CLI result schemas"),
        (_CLI_OUTPUT_DOC, "json_contract output-validation tests"),
    ]
    for finding in findings:
        assert "moved" in finding.reason
        assert "no longer verified" in finding.reason
        # As above: the remedy names the claim's home by interpolation, so
        # nothing but this assertion stands between a dropped `{}` and a
        # remedy sentence that tells the reader nothing.
        assert str(_CATALOG_MARKER) in finding.reason


def test_the_same_tree_is_silent_as_a_fixture_and_loud_as_the_catalog(
    tmp_path: Path,
) -> None:
    # The escape hatch and the guard are one decision, so they are asserted
    # against one tree. A miniature tree carries none of the seam docs by
    # design and must stay silent; the same tree once it carries the doclint
    # source -- the marker that makes it the catalog -- must report. Losing
    # either half rebuilds #956 or breaks every fixture test in this file.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("one"))
    assert check_counts(tmp_path) == []
    write(tmp_path, str(_CATALOG_MARKER), "")
    assert check_counts(tmp_path) != []


def test_a_renamed_doc_cannot_launder_a_false_count(tmp_path: Path) -> None:
    # #956's own reproduction: the cli-output seam doc renamed to a sibling
    # path, carrying prose that claims 999 schemas and 777 tests over a tree
    # holding one of each. The rename must not be a way to make a false count
    # unreachable -- the gate reports the claim's declared path either way.
    write(tmp_path, *_sdk_module("a"))
    write(tmp_path, _HARNESS_DOC, _harness_prose("one"))
    write(tmp_path, "cli/schema/kill.json", "{}\n")
    write(tmp_path, "cli/tests/json_contract.rs", "#[test]\nfn a() {}\n")
    write(
        tmp_path,
        "docs/interfaces/cli-output-renamed/INTERFACE.md",
        _cli_output_prose(schemas="999", tests="777"),
    )
    write(tmp_path, str(_CATALOG_MARKER), "")
    findings = check_counts(tmp_path)
    assert [finding.doc for finding in findings] == [_CLI_OUTPUT_DOC, _CLI_OUTPUT_DOC]
    # The point being pinned: the gate reports the claim at its declared path
    # rather than silently validating the renamed doc's false prose. If either
    # false count leaked into a reason, the rename would have laundered it.
    for finding in findings:
        assert "999" not in finding.reason
        assert "777" not in finding.reason


def test_the_catalog_marker_names_the_real_doclint_source() -> None:
    # The guard on the guard. A tree counts as the catalog when it carries the
    # doclint source at `_CATALOG_MARKER`, so relocating that source would
    # leave the marker naming a path no tree holds, no tree would ever be
    # recognized, and every claim would silently go back to being skipped --
    # #956 again, this time with a passing test suite. Pinning the marker
    # against the imported module's own on-disk file reds CI on the move
    # instead.
    #
    # This equality holds because the uv workspace installs curie-doclint
    # editable, so counts.__file__ resolves inside the checkout at
    # _CATALOG_MARKER. Under a non-editable install counts.__file__ would
    # resolve into the site-packages copy instead, and this test FAILS LOUDLY
    # rather than passing vacuously -- the correct direction for a guard on
    # the guard. The production predicate _is_catalog_root deliberately does
    # not depend on the install layout: it asks the lint target whether it
    # carries the doclint source, not the installed copy, which is precisely
    # why it survives a wheel install.
    assert (REPO_ROOT / _CATALOG_MARKER).resolve() == Path(counts.__file__).resolve()


def test_every_claim_doc_resolves_in_the_real_repository() -> None:
    # The gate that actually reds CI when someone moves a seam doc: every
    # claim's declared path exists in the real tree, and every count it states
    # matches. This is the assertion the skip was hiding.
    assert [claim.doc for claim in CLAIMS if not (REPO_ROOT / claim.doc).is_file()] == []
    assert check_counts(REPO_ROOT) == []


# --- counter semantics -----------------------------------------------------


def test_only_import_level_uses_count_as_importing_modules(tmp_path: Path) -> None:
    # The prose claims modules that IMPORT the SDK. A module that merely names
    # it in a comment is not one, or the count inflates on documentation edits.
    write(tmp_path, *_sdk_module("real"))
    write(
        tmp_path,
        "runner/src/curie_runner/mentions.py",
        "# claude_agent_sdk is discussed here but never imported.\n",
    )
    write(tmp_path, _HARNESS_DOC, _harness_prose("one"))
    assert check_counts(tmp_path) == []


def test_schema_count_excludes_the_index_and_the_prose_mention(tmp_path: Path) -> None:
    # Two traps in one. `index.json` lists the schemas rather than being one of
    # them, and the same doc later says "enforced by committed schemas and a
    # drift gate" -- prose ABOUT the schemas, not a count of them, which an
    # under-anchored pattern would read as the count and choke on.
    for name in ("kill", "resume", "budget"):
        write(tmp_path, f"cli/schema/{name}.json", "{}\n")
    write(tmp_path, "cli/schema/index.json", "{}\n")
    write(tmp_path, "cli/tests/json_contract.rs", "#[test]\nfn a() {}\n")
    write(tmp_path, _CLI_OUTPUT_DOC, _cli_output_prose(schemas="3", tests="one"))
    assert check_counts(tmp_path) == []


def test_json_contract_test_count_is_read_from_the_test_file(tmp_path: Path) -> None:
    write(tmp_path, "cli/schema/kill.json", "{}\n")
    write(
        tmp_path,
        "cli/tests/json_contract.rs",
        "#[test]\nfn a() {}\n#[test]\nfn b() {}\n",
    )
    write(tmp_path, _CLI_OUTPUT_DOC, _cli_output_prose(schemas="1", tests="5"))
    findings = check_counts(tmp_path)
    assert len(findings) == 1
    assert "'5'" in findings[0].reason
    assert "2" in findings[0].reason


# --- wired into the linter, not dead code ----------------------------------


def test_count_drift_fails_through_the_cli(
    clean_repo: Path, run_lint: RunLint, regenerate: Regenerate
) -> None:
    # The integration proof: the check runs as part of `curie dev docs-lint`,
    # so a drifted count fails the real gate rather than only a unit test.
    write(clean_repo, *_sdk_module("importer"))
    write(
        clean_repo,
        _HARNESS_DOC,
        "---\n"
        "seam: Harness in-proc / ModelSession\n"
        "kind: CLEAN\n"
        "impls: 1 + fake\n"
        "grade: not separately graded\n"
        "epics:\n"
        '  - "#25"\n'
        "order: 99\n"
        "---\n"
        "\n# Harness\n\n"
        "<!-- BEGIN GENERATED: header (curie dev docs-lint) -->\n"
        "<!-- END GENERATED: header -->\n\n" + _harness_prose("six"),
    )
    # Regenerate first: a new seam doc legitimately changes the index and this
    # doc's header, and that drift is a different finding than the one under
    # test here.
    regenerate(clean_repo)
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "runner modules importing claude_agent_sdk" in out
    assert "'six'" in out


# --- the enumeration disagrees with the tree (#1019) ------------------------
#
# #952 gated the harness doc's COUNT but not the names beside it, so a rename or
# a swap kept the count right and the list wrong. These drive the real
# NAME_CLAIMS over miniature trees, same discipline as the count tests above.


def test_matching_name_list_passes(tmp_path: Path) -> None:
    # Positive control: a gate that reported everything would be no gate.
    write(tmp_path, *_sdk_module("check"))
    write(tmp_path, *_sdk_module("session"))
    write(tmp_path, _HARNESS_DOC, _harness_names_prose("`check.py`, `session.py`"))
    assert check_name_sets(tmp_path) == []


def test_renamed_module_is_reported_by_name(tmp_path: Path) -> None:
    # The tree renamed session.py -> runtime.py; the count is still two, so the
    # count claim cannot see this.
    write(tmp_path, *_sdk_module("check"))
    write(tmp_path, *_sdk_module("runtime"))
    write(tmp_path, _HARNESS_DOC, _harness_names_prose("`check.py`, `session.py`"))

    findings = check_name_sets(tmp_path)
    assert len(findings) == 1, findings
    detail = findings[0].reason
    assert "runtime.py" in detail and "session.py" in detail, detail


def test_swapped_import_is_caught_though_the_count_is_unchanged(tmp_path: Path) -> None:
    """The motivating case, and the one #952's count gate provably cannot catch.

    One module drops the SDK import and another gains it. The count is identical,
    so `check_counts` passes; only the enumeration reveals the drift. #844 Phase 2
    moves imports between exactly these modules, so this is the likely edit.
    """
    write(tmp_path, *_sdk_module("check"))
    write(tmp_path, *_sdk_module("plugin"))  # gained it
    write(tmp_path, "runner/src/curie_runner/session.py", "import os\n")  # dropped it
    write(tmp_path, _HARNESS_DOC, _harness_names_prose("`check.py`, `session.py`"))

    # The old gate is blind to it: two modules stated, two modules in the tree.
    assert check_counts(tmp_path) == []

    findings = check_name_sets(tmp_path)
    assert len(findings) == 1, findings
    detail = findings[0].reason
    assert "plugin.py" in detail and "session.py" in detail, detail


def test_removed_enumeration_fails_rather_than_going_vacuous(tmp_path: Path) -> None:
    # The vacuity guard, mirroring the count claims': a reworded sentence must
    # fail loudly rather than quietly stop being checked.
    write(tmp_path, *_sdk_module("check"))
    write(tmp_path, _HARNESS_DOC, "The SDK is imported in some runner modules.\n")

    findings = check_name_sets(tmp_path)
    assert len(findings) == 1, findings
    assert "no longer verified" in findings[0].reason


def test_absent_seam_doc_is_skipped_for_names_too(tmp_path: Path) -> None:
    # A miniature tree that is not the catalog carries no seam docs; that is not
    # a defect, same rule the count claims follow.
    write(tmp_path, *_sdk_module("check"))
    assert check_name_sets(tmp_path) == []
