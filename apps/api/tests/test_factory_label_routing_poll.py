"""One poll pass serves every label bound on a repository (#4361 AC 5, decision 14).

A repository with two bound labels costs one repository read and one
installation token per pass, one labelled listing per effective label (ETag key
``issues:<label>``), and one mention listing. The pre-#4361 ``issues`` key is
dropped on the first pass, so that pass lists each label unconditionally once.
The missed-label reconciler walks the same (repository, label) pairs, and both
skip a repository outside ``GITHUB_REPO_ALLOWLIST``.

GitHub is ``test_factory_label_routing.LabelGitHub``; see that module for the
REST shapes it replays. Machine fixtures stand in for GitHub.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_label_reconcile import reconcile_missed_labels
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import REPO, REPO_ID  # noqa: E402
from test_factory_label_routing import (  # noqa: E402
    _REVIEW_IDS,
    FAST,
    INSTALL,
    MENTION,
    LabelFactory,
    _cancelled,
    _owner,
    _requests,
    _work,
    label_factory,
)
from test_github_factory_ingress import _rows  # noqa: E402
from test_github_factory_review import _complete, _execute, _own_pull_request  # noqa: E402


@pytest.fixture
def factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None, valkey: object
) -> Iterator[LabelFactory]:
    yield from label_factory(monkeypatch, tmp_path)


_OLD = timedelta(minutes=30)


def _cursor_etags() -> dict[str, Any]:
    rows = _rows(
        "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'github' "
        "AND tracker_host = 'github.com' AND tracker_scope_id = :scope",
        {"scope": str(REPO_ID)},
    )
    assert len(rows) == 1, rows
    etags: dict[str, Any] = rows[0]["etags"]
    return etags


def _seed_pre_label_cursor() -> None:
    """The cursor a release before #4361 left: one ``issues`` key, in place of
    any keys an earlier pass stored."""

    _execute(
        "INSERT INTO curie.factory_poll_cursors "
        "(tracker_kind, tracker_host, tracker_scope_id, scope_path, etags, updated_at) "
        "VALUES ('github', 'github.com', :scope, :path, CAST(:etags AS jsonb), now()) "
        "ON CONFLICT (tracker_kind, tracker_host, tracker_scope_id) "
        "DO UPDATE SET etags = EXCLUDED.etags",
        {"scope": str(REPO_ID), "path": REPO, "etags": '{"issues": "\\"pre-4361\\""}'},
    )


def _get(github: Any, path: str) -> list[httpx.Request]:
    return [r for r in github.calls if r.method == "GET" and r.url.path == path]


def _reconcile(factory: LabelFactory) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        client = httpx.AsyncClient(transport=httpx.MockTransport(factory.github.handle))
        try:
            await reconcile_missed_labels(sessions, get_settings(), client, now=datetime.now(UTC))
        finally:
            await client.aclose()
            await engine.dispose()

    asyncio.run(go())


def _count_token_mints(factory: LabelFactory) -> list[str]:
    """Record every installation token the App credentials hand out."""

    from curie_api.github_app import GitHubCredentials

    mints: list[str] = []
    for name in ("fresh_installation_token", "token_for_verified_installation", "token_for"):
        original = getattr(GitHubCredentials, name)

        def counted(self: Any, *args: Any, _name: str = name, _original: Any = original) -> Any:
            mints.append(_name)
            return _original(self, *args)

        factory.monkeypatch.setattr(GitHubCredentials, name, counted)
    return mints


def test_label_routing_poll_one_pass_reads_the_repository_once_and_lists_each_label(
    factory: LabelFactory,
) -> None:
    """R1 finding 7: with an actionable issue under each label, a run to
    cancel and actionable review feedback on each agent's pull request, the
    pass still reads the repository once and mints one token; each notice
    re-reads only its issue, and each review its pull request and its author's
    permission."""

    full, fast = factory.two_agents()
    cancelled = factory.github.open_issue(INSTALL)
    reviewed = {factory.github.open_issue(INSTALL): full, factory.github.open_issue(FAST): fast}
    factory.poll()
    assert _owner(cancelled) == full
    assert {number: _owner(number) for number in reviewed} == reviewed
    factory.github.unlabel(cancelled, INSTALL)
    first = factory.github.open_issue(INSTALL)
    second = factory.github.open_issue(FAST)
    pulls = []
    for number in reviewed:
        (request, *_rest) = _work(number)
        pull = factory.github.plant_pull()
        _own_pull_request(request["work_item_id"], pull)
        _complete(factory.client, request["id"])
        factory.github.add_review(pull, next(_REVIEW_IDS), f"@{MENTION} rename it")
        pulls.append(pull)
    _seed_pre_label_cursor()
    factory.github.calls.clear()
    mints = _count_token_mints(factory)

    factory.poll()

    github = factory.github
    assert _owner(first) == full
    assert _owner(second) == fast
    assert _cancelled(cancelled)
    # Each review admitted a revision to its own agent's WorkItem.
    assert {number: _requests(number) for number in reviewed} == dict.fromkeys(reviewed, 2)
    assert len(_get(github, f"/repos/{REPO}")) == 1, [r.url.path for r in github.calls]
    # The pass's repository read stands in for each review's; its pull
    # request and its author's write permission are still read per review.
    for pull in pulls:
        assert _get(github, f"/repos/{REPO}/pulls/{pull}"), pull
    permission_reads = [r for r in github.calls if r.url.path.endswith("/permission")]
    assert len(permission_reads) >= len(pulls), [r.url.path for r in github.calls]
    # One tracker token for the pass; no notice mints its own. Each fresh
    # admission's base branch read goes through the code host connection and
    # its own credential (`token_for`), which is not the pass's tracker token.
    assert mints.count("fresh_installation_token") == 1, mints
    assert "token_for_verified_installation" not in mints, mints
    for number in (first, second, cancelled):
        assert _get(github, f"/repos/{REPO}/issues/{number}"), number
    listings = github.listings()
    assert sorted(r.url.params.get("labels") for r in listings) == sorted([INSTALL, FAST])
    # The first pass after the switch lists each label unconditionally; the
    # dropped key's tag is never sent.
    assert all("if-none-match" not in r.headers for r in listings), listings
    assert len(_get(github, f"/repos/{REPO}/issues/comments")) == 1
    etags = _cursor_etags()
    assert "issues" not in etags, etags
    assert f"issues:{INSTALL}" in etags, etags
    assert f"issues:{FAST}" in etags, etags


def test_label_routing_poll_second_pass_is_conditional_per_label(
    factory: LabelFactory,
) -> None:
    factory.two_agents()
    factory.poll()
    stored = _cursor_etags()
    factory.github.calls.clear()
    factory.github.saw_304 = False

    factory.poll()

    listings = factory.github.listings()
    assert len(listings) == 2, listings
    for request in listings:
        label = request.url.params.get("labels")
        assert request.headers.get("if-none-match") == stored[f"issues:{label}"], label
    assert factory.github.saw_304
    assert len(_get(factory.github, f"/repos/{REPO}")) == 1


def test_label_routing_poll_lists_a_label_once_however_it_is_bound(
    factory: LabelFactory,
) -> None:
    """One labelled listing per effective label, not per binding: a repository
    with one bound label is listed once (the single-agent install)."""

    factory.agent({"kind": "github", "address": REPO})
    factory.github.calls.clear()

    factory.poll()

    assert [r.url.params.get("labels") for r in factory.github.listings()] == [INSTALL]
    assert "issues" not in _cursor_etags()


def test_label_routing_poll_admits_each_labels_issue_in_one_pass(
    factory: LabelFactory,
) -> None:
    full, fast = factory.two_agents()
    first = factory.github.open_issue(INSTALL)
    second = factory.github.open_issue(FAST)

    factory.poll()

    assert _owner(first) == full
    assert _owner(second) == fast


def test_label_routing_poll_reconciler_walks_both_labels(factory: LabelFactory) -> None:
    full, fast = factory.two_agents()
    # Older than the grace period, with no delivery, so the backstop admits them.
    first = factory.github.open_issue(INSTALL, age=_OLD)
    second = factory.github.open_issue(FAST, age=_OLD)
    factory.github.calls.clear()
    mints = _count_token_mints(factory)

    _reconcile(factory)

    assert sorted(r.url.params.get("labels") for r in factory.github.listings()) == sorted(
        [INSTALL, FAST]
    )
    assert _owner(first) == full
    assert _owner(second) == fast
    # Decision 14 holds for the reconciler too: its admissions reuse the pass's
    # one repository read and token, and re-read only their issue.
    github = factory.github
    assert len(_get(github, f"/repos/{REPO}")) == 1, [r.url.path for r in github.calls]
    assert mints.count("fresh_installation_token") == 1, mints
    assert "token_for_verified_installation" not in mints, mints
    for number in (first, second):
        assert _get(github, f"/repos/{REPO}/issues/{number}"), number


def test_label_routing_poll_skips_a_repository_outside_the_allowlist(
    factory: LabelFactory,
) -> None:
    factory.two_agents()
    planted = factory.github.open_issue(FAST, age=_OLD)
    factory.reboot(allowlist='["other-org/*"]')
    factory.github.calls.clear()

    factory.poll()
    _reconcile(factory)

    asked = [r.url.path for r in factory.github.calls if r.url.path.startswith(f"/repos/{REPO}")]
    assert asked == [], asked
    assert _owner(planted) is None

    # Liveness: allowed again, the same passes walk it and admit.
    factory.reboot()
    factory.poll()
    assert factory.github.listings()
    assert _owner(planted) is not None
