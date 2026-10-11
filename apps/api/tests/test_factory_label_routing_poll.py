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
    FAST,
    INSTALL,
    LabelFactory,
    _owner,
    label_factory,
)
from test_github_factory_ingress import _rows  # noqa: E402
from test_github_factory_review import _execute  # noqa: E402


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
    """The cursor a release before #4361 left: one ``issues`` key."""

    _execute(
        "INSERT INTO curie.factory_poll_cursors "
        "(tracker_kind, tracker_host, tracker_scope_id, scope_path, etags, updated_at) "
        "VALUES ('github', 'github.com', :scope, :path, CAST(:etags AS jsonb), now())",
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


def test_label_routing_poll_one_pass_reads_the_repository_once_and_lists_each_label(
    factory: LabelFactory,
) -> None:
    factory.two_agents()
    _seed_pre_label_cursor()
    factory.github.calls.clear()

    factory.poll()

    github = factory.github
    assert len(_get(github, f"/repos/{REPO}")) == 1, [r.url.path for r in github.calls]
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

    _reconcile(factory)

    assert sorted(r.url.params.get("labels") for r in factory.github.listings()) == sorted(
        [INSTALL, FAST]
    )
    assert _owner(first) == full
    assert _owner(second) == fast


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
