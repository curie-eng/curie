"""Publication must be preceded by a reported verification of the changes.

The stable train shipped this contract against the coder example's
``SKILL.md``. The feature train made coding tools a built-in session
capability and deleted that file, so the same contract is asserted here
against the built-in ``publish_changes`` description, which is now the only
place the coder reads the publication protocol from.

This is a description-contract test rather than an implementation test: the
coder operates in an arbitrary target repository, so the contract must make it
run that repository's documented check and report the observable result
in-thread. The publication instruction is the final boundary; verification
language after it would be too late to guide the coder.

The assertions below are deliberately **sentence-scoped**: each rule-bearing
regex uses ``[^.]`` spans so it cannot be satisfied by a neighbouring sentence.
An earlier revision used loose cross-sentence spans (``.{0,140}``), and a
mutation sweep showed three rules were not actually pinned -- deleting them left
the test green because an adjacent sentence supplied the matched words. A later
adversarial pass found a further hole: the cleanup sentence's ``otherwise``
fallback (report generated artifacts and do not publish, when no documented
cleanup procedure exists) was unpinned on its own. The mutation classes each
assertion must kill are named alongside it.
"""

import re

from curie_runner.approval import _PUBLISH_DESCRIPTION


def test_publication_description_requires_a_reported_verification_first() -> None:
    description = _PUBLISH_DESCRIPTION

    publication = re.search(
        r"when the changes are ready, use\s+this tool to request human approval",
        description,
        flags=re.IGNORECASE,
    )
    assert publication, "the publication instruction must remain explicit"

    verification = re.search(
        r"before requesting publication.*?(?=When the changes are ready)",
        description,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert verification, "there must be a distinct pre-publication verification instruction"
    verification_text = verification.group(0)
    assert verification.end() <= publication.start(), (
        "verification instructions must appear before publication"
    )

    assert re.search(
        r"repository(?:'s)?\s+own\s+documented\s+test\s+or\s+check\s+command",
        verification_text,
        flags=re.IGNORECASE,
    ), "the coder must run the target repository's own documented test or check command"
    assert re.search(
        r"run\s+it\s+from\s+/workspace",
        verification_text,
        flags=re.IGNORECASE,
    ), "the repository command must explicitly run from /workspace"
    # Kills: weakening the reporting triple to fewer than all three fields.
    assert re.search(
        r"exact command[^.]{0,60}exit status[^.]{0,60}result",
        verification_text,
        flags=re.IGNORECASE,
    ), "the command, its exit status, and its result must all be reported"
    assert "thread" in verification_text.casefold()

    # Kills: dropping the negation ("...report that and do publish"). The span
    # is sentence-local so the neighbouring failure sentence cannot supply the
    # missing "do not publish".
    assert re.search(
        r"cannot\s+identify\s+or\s+run\s+an?\s+appropriate\s+command"
        r"[^.]{0,80}(?:do\s+not|must\s+not|never)\s+publish",
        verification_text,
        flags=re.IGNORECASE,
    ), "inability to identify or run a command must prevent publication"
    # Kills: replacing the whole failure rule with a softer one ("If the command
    # fails, note it."). Sentence-local, so the cannot-identify sentence and the
    # artifacts sentence cannot stand in for it.
    assert re.search(
        r"(?:fail(?:s|ed|ure|ing)?|non[- ]zero)[^.]{0,80}"
        r"(?:do not|must not|never)\s+publish",
        verification_text,
        flags=re.IGNORECASE,
    ), "a failed verification must prevent publication"
    # Kills: dropping the unrequested-artifact prohibition.
    assert re.search(
        r"verification\s+generates\s+artifacts?"
        r"[^.]{0,80}(?:do\s+not|must\s+not|never)\s+publish\s+unrequested\s+artifacts?",
        verification_text,
        flags=re.IGNORECASE,
    ), "verification artifacts must not publish unrequested artifacts"
    # Kills: generalising the cleanup route away from the repository's own
    # documented procedure ("use a cleanup procedure when one exists and ..."),
    # and dropping the only-what-this-verification-created restriction. Both
    # clauses live in one sentence, so they are pinned by one sentence-local
    # regex rather than two independent word searches.
    assert re.search(
        r"use\s+the\s+repository(?:'s)?\s+documented\s+cleanup\s+procedure"
        r"\s+when\s+one\s+exists[^.]{0,120}"
        r"only\s+artifacts?\s+this\s+verification\s+created",
        verification_text,
        flags=re.IGNORECASE,
    ), (
        "cleanup must go through the repository's own documented procedure and "
        "must not remove requested or unrelated work"
    )
    # Kills: dropping the "otherwise" fallback entirely (no documented cleanup
    # procedure exists), and gutting just its consequence so the artifacts are
    # reported but publication is no longer forbidden. Sentence-local, so the
    # cleanup-procedure clause earlier in the same sentence cannot supply the
    # missing fallback.
    assert re.search(
        r"otherwise\s+report\s+the\s+generated\s+artifacts?[^.]{0,60}"
        r"session\s+thread[^.]{0,40}(?:do\s+not|must\s+not|never)\s+publish",
        verification_text,
        flags=re.IGNORECASE,
    ), (
        "when no documented cleanup procedure exists, the generated artifacts must "
        "be reported in the session thread and publication must not proceed"
    )
