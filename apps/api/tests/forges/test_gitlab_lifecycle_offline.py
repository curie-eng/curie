"""AC 8 of #4399: a gitlab+gitlab install, end to end, offline.

The GitLab fake (`forge_fakes.gitlab.GitLabAPI`) is the only fake. It is
served over a socket and the install's ``CURIE_FORGES`` names only a GitLab
tracker, code host and binding on it (`forge_fakes.gitlab_lifecycle`), built
through the production registry. Nothing in the factory core is replaced: unlike
``forge_fakes/bitbucket_lifecycle.py`` no ``forges.hosts`` or
``code_host_for_repository`` seam is patched, so every code host step must
resolve the GitLab code host from the stored WorkItem or lineage itself.

The passes are the real ones: `WorkItemReconciler.run_once` (tracker poll,
CI gate, review feedback) and the status note sync (`_reconcile`), and the
worker's and publication Job's internal routes. The review feedback pass is
expected inside ``run_once``, beside the tracker poll, so a developer's diff
comment becomes a revision on the next reconcile with no GitHub webhook.

The steps are the ``run_*`` functions in `forge_fakes.gitlab_lifecycle`; the
live fixture suite (``gitlab_fixture/test_gitlab_lifecycle_fixture.py``) runs
the same ones against real GitLab CE.

Run with ``uv run pytest apps/api/tests -k gitlab_lifecycle``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes.gitlab_lifecycle import FAST, GitLabInstall

pytestmark = pytest.mark.usefixtures("clean_db")


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path)


@pytest.fixture
def two_surfaces(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path, labels=(None, FAST))


def test_gitlab_lifecycle_label_to_merge_request_ci_review_and_cancel(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_label_to_merge_request_ci_review_and_cancel(install, caplog)


def test_gitlab_lifecycle_a_reporter_label_admits_nothing_and_sets_no_state_label(
    install: GitLabInstall,
) -> None:
    gl.run_reporter_label_admits_nothing(install)


def test_gitlab_lifecycle_two_surfaces_admit_only_their_own_label(
    two_surfaces: GitLabInstall,
) -> None:
    gl.run_two_surfaces_admit_only_their_own_label(two_surfaces)


def test_gitlab_lifecycle_an_issue_mention_links_the_note_and_counts_as_a_follow_up(
    install: GitLabInstall,
) -> None:
    gl.run_issue_mention_links_the_note(install)


def test_gitlab_lifecycle_failures_log_and_answer_no_token(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_failures_log_and_answer_no_token(install, caplog)
