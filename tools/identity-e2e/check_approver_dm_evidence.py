"""Check one ``approver-dm`` evidence file (ADR 0272 items 1, 3 and 4).

Usage::

    python3 tools/identity-e2e/check_approver_dm_evidence.py dm.json [--repo <checkout>]

It exits 0 only when the evidence names the ``approver-dm`` scenario, its
overall ``passed`` is true, every required step id is present and passed, and
a candidate commit is recorded. Inside a git checkout it also requires that
``git diff --name-only <candidate> HEAD`` touches nothing under the runtime
paths, so the harness that produced the evidence ran from the build the
evidence names. Outside a checkout that comparison is skipped and the summary
says so. The judging is ``check_answer_evidence.py``'s with this scenario's
step ids. Standard library only; on a mismatch it prints what it observed.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import Any

import check_answer_evidence as base
from approver_dm_driver import SCENARIO, STEP_IDS


def judge(evidence: Any) -> list[str]:
    """Every way the evidence falls short, one line each; empty when it passes."""

    return base.judge(evidence, scenario=SCENARIO, steps=STEP_IDS)


def main(argv: Sequence[str] | None = None, *, git: base.GitRunner = base.run_git) -> int:
    return base.main(
        argv,
        git=git,
        scenario=SCENARIO,
        steps=STEP_IDS,
        complete="ADR 0272 items 1, 3 and 4 evidence is complete",
        description=__doc__.splitlines()[0] if __doc__ else None,
    )


if __name__ == "__main__":
    sys.exit(main())
