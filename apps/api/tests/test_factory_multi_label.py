"""Two factory agents on one GitHub repository, told apart by label (#4397 AC 10).

The install's GitHub tracker label is ``factory``. Agent ``full`` binds the
repository with no label, so it serves ``factory``; agent ``fast`` binds it with
``factory-fast``. Each admits only its own issues, through the webhook and
through a poll pass, and review comments on each agent's pull request reach the
agent whose label started that run.

GitHub is ``test_factory_label_routing.LabelGitHub``; the review comment
webhook carries the ``pull_request_review_comment`` shape
``test_github_factory_review._feedback_event`` records. Machine fixtures stand in
for GitHub.
"""

from __future__ import annotations

import itertools
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import INSTALLATION_ID, MENTION, REPO, REPO_ID, _post  # noqa: E402
from test_factory_label_routing import (  # noqa: E402
    LabelFactory,
    _owner,
    _requests,
    _work,
    label_factory,
)
from test_factory_poll_intake import _user  # noqa: E402
from test_github_factory_review import _complete, _own_pull_request  # noqa: E402


@pytest.fixture
def factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None, valkey: object
) -> Iterator[LabelFactory]:
    yield from label_factory(monkeypatch, tmp_path)


LABEL = "factory"
FAST = "factory-fast"
_REVIEW_COMMENTS = itertools.count(765000)


def _two_agents(factory: LabelFactory) -> tuple[str, str]:
    factory.reboot(label=LABEL)
    full = factory.agent({"kind": "github", "address": REPO})
    fast = factory.agent({"kind": "github", "address": REPO, "trigger_label": FAST})
    return full, fast


def _admit(factory: LabelFactory, intake: str, label: str) -> int:
    number, response = factory.opened(intake, label)
    if response is not None:
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text
    return number


@pytest.mark.parametrize("intake", ["webhook", "poll"])
def test_factory_multi_label_each_agent_admits_only_its_own_issues(
    factory: LabelFactory, intake: str
) -> None:
    full, fast = _two_agents(factory)

    owned = {
        full: [_admit(factory, intake, LABEL) for _ in range(2)],
        fast: [_admit(factory, intake, FAST) for _ in range(2)],
    }

    for agent_id, numbers in owned.items():
        for number in numbers:
            assert _owner(number) == agent_id, (agent_id, number, _work(number))
    # A label neither binding serves admits nobody.
    stray, response = factory.opened(intake, "factory-slow")
    assert _owner(stray) is None
    if response is not None:
        assert response.json()["status"] == "factory_ignored", response.text


def _review_comment(factory: LabelFactory, intake: str, number: int) -> None:
    """A review comment on the pull request the run on ``number`` opened."""

    first = _work(number)[0]
    pull = factory.github.plant_pull()
    _own_pull_request(first["work_item_id"], pull)
    _complete(factory.client, first["id"])
    comment = factory.github.add_review_comment(
        pull, next(_REVIEW_COMMENTS), f"@{MENTION} please rename the helper"
    )
    if intake == "poll":
        factory.poll()
        return
    payload: dict[str, Any] = {
        "action": "created",
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _user(),
        "pull_request": factory.github.pulls[pull],
        "comment": comment,
    }
    response = _post(factory.client, "pull_request_review_comment", payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted", response.text


@pytest.mark.parametrize("intake", ["webhook", "poll"])
def test_factory_multi_label_each_prs_review_comment_reaches_the_agent_that_opened_it(
    factory: LabelFactory, intake: str
) -> None:
    full, fast = _two_agents(factory)
    runs = {full: _admit(factory, intake, LABEL), fast: _admit(factory, intake, FAST)}
    before = {number: _requests(number) for number in runs.values()}

    for number in runs.values():
        _review_comment(factory, intake, number)

    for agent_id, number in runs.items():
        assert _requests(number) == before[number] + 1, _work(number)
        assert _owner(number) == agent_id
        assert {str(row["agent_id"]) for row in _work(number)} == {agent_id}
