"""A cited ADR number must name an ADR file (#3844).

The linted docs cite decisions by number in three shapes the tree actually
uses: ``ADR-0007``, ``ADR 0007``, a plural list (``ADRs 0125 and 0126``), and a
link whose target is ``.../adr/0007-<slug>.md``. Before this rule a doc could
cite an ADR number that no file claims and still pass, which is how
ARCHITECTURE.md came to point at decisions that were never written.

``docs/adr/`` itself stays outside the walk: an Accepted ADR is immutable and
may name proposals that were never filed (ADR 0162 does exactly that).

Every test drives the public CLI over a copied fixture tree, asserting through
exit code and message text only.
"""

from __future__ import annotations

from pathlib import Path

from .conftest import RunLint, write


def test_existing_adr_passes(clean_repo: Path, run_lint: RunLint) -> None:
    # False-positive guard: the fixture has ADR 0001 and 0035, so citing them
    # in every recognized shape stays silent.
    write(
        clean_repo,
        "ARCHITECTURE.md",
        "Decided in ADR-0001 and ADR 0035, together ADRs 0001 and 0035.\n"
        "See [the decision](docs/adr/0001-example.md).\n",
    )
    code, out = run_lint(clean_repo)
    assert code == 0, out


def test_missing_adr_fails(clean_repo: Path, run_lint: RunLint) -> None:
    write(clean_repo, "ARCHITECTURE.md", "Intro.\n\nThe queue follows ADR-0999.\n")
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "ARCHITECTURE.md:3" in out
    assert "ADR-0999" in out
    assert "does not exist" in out


def test_space_form_fails(clean_repo: Path, run_lint: RunLint) -> None:
    write(clean_repo, "docs/notes.md", "Per ADR 0998, the lane waits.\n")
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "docs/notes.md" in out
    assert "ADR-0998" in out


def test_each_citation_checked(clean_repo: Path, run_lint: RunLint) -> None:
    # One real citation must not hide the missing ones beside it.
    write(
        clean_repo,
        "ARCHITECTURE.md",
        "ADR-0001 stands; ADR-0997 and ADR 0998 were never written.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "ADR-0997" in out
    assert "ADR-0998" in out
    assert "ADR-0001" not in out


def test_plural_list_fails(clean_repo: Path, run_lint: RunLint) -> None:
    # The ``ADRs 0125 and 0126`` shape: every number in the list is a citation,
    # not only the one beside the word.
    write(
        clean_repo,
        "ARCHITECTURE.md",
        "ADRs 0001, 0035, and 0996 cover it.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "ADR-0996" in out
    assert "ADR-0035" not in out


def test_link_target_fails(clean_repo: Path, run_lint: RunLint) -> None:
    write(
        clean_repo,
        "docs/notes.md",
        "Read [the queue decision](adr/0995-a-queue.md) first.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "ADR-0995" in out


def test_one_finding_per_line(clean_repo: Path, run_lint: RunLint) -> None:
    # A labelled link names the same number twice; one finding is enough.
    write(
        clean_repo,
        "docs/notes.md",
        "See [ADR-0994](adr/0994-ghost.md).\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert out.count("ADR-0994") == 1, out


def test_front_matter_fails(clean_repo: Path, run_lint: RunLint) -> None:
    # A seam's ``epics:`` list cites ADRs by number too, and the generated
    # index copies it, so a dangling number there spreads to two docs.
    seam = clean_repo / "docs/interfaces/approval/INTERFACE.md"
    seam.write_text(
        seam.read_text(encoding="utf-8").replace('"ADR-0035"', '"ADR-0993"'),
        encoding="utf-8",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "docs/interfaces/approval/INTERFACE.md" in out
    assert "ADR-0993" in out


def test_ignore_line_passes(clean_repo: Path, run_lint: RunLint) -> None:
    write(
        clean_repo,
        "docs/notes.md",
        "A fork's own ADR-9004 is not ours. <!-- doclint:ignore-line -->\n",
    )
    code, out = run_lint(clean_repo)
    assert code == 0, out


def test_non_citations_pass(clean_repo: Path, run_lint: RunLint) -> None:
    # A template number has no digits, and another repository's ADR is not a
    # file this tree has to hold.
    write(
        clean_repo,
        "docs/notes.md",
        "Name it ADR-NNNN or ADR-XXXX.\n"
        "Compare https://github.com/example/other/blob/main/docs/adr/0990-x.md.\n",
    )
    code, out = run_lint(clean_repo)
    assert code == 0, out


def test_own_permalink_fails(clean_repo: Path, run_lint: RunLint) -> None:
    write(
        clean_repo,
        "docs/notes.md",
        "Pinned: https://github.com/curie-eng/curie/blob/abc123/docs/adr/0989-gone.md\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "ADR-0989" in out


def test_adr_bodies_skipped(clean_repo: Path, run_lint: RunLint) -> None:
    # ADR 0162 names "the absent proposals ADR 0148 and ADR 0150". An Accepted
    # ADR is immutable, so its history may cite numbers no file claims.
    adr = clean_repo / "docs/adr/0001-example.md"
    adr.write_text(
        adr.read_text(encoding="utf-8") + "\nIt cites the absent ADR 0148.\n",
        encoding="utf-8",
    )
    code, out = run_lint(clean_repo)
    assert code == 0, out
