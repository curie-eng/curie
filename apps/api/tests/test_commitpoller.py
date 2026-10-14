"""Noticing new commits without a webhook (issue #1239).

The decision -- which branches moved and are worth deploying -- is pure, so it
is tested without HTTP or a database. What matters is not that it spots a moved
branch; that is one comparison. It is that it does not deploy the same commit
twice, does not stop polling when one repository breaks, and hands the deploy
path a payload indistinguishable from a real webhook.
One test, the one that runs the real lifespan (#1250), is the exception and is
deliberate: the wiring the poller receives from the lifespan cannot be checked
without running the real lifespan.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from datetime import datetime, timedelta

import httpx
import pytest
from curie_api.commitpoller import Move, PollTarget, moves_to_deploy
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient

REPO = "octo/agent-bot"
CLONE = "https://github.com/octo/agent-bot.git"
# A sha the real `process_push` will accept: it rejects anything that is not
# 40/64 lowercase hex as "ignored" before it ever reaches the clone (#1309's
# end-to-end tests drive the real deploy path, so "abc123" would never get
# there).
REAL_SHA = "a" * 40


class Tips:
    """Branch tips, and optionally a repository that raises."""

    def __init__(self, shas: dict[tuple[str, str], str | None], explode: set[str] | None = None):
        self._shas = shas
        self._explode = explode or set()
        self.asked: list[tuple[str, str]] = []

    def sha_for(self, repo_full_name: str, branch: str) -> str | None:
        self.asked.append((repo_full_name, branch))
        if repo_full_name in self._explode:
            raise RuntimeError("credential revoked")
        return self._shas.get((repo_full_name, branch))


def target(*branches: str, repo: str = REPO) -> PollTarget:
    return PollTarget(repo_full_name=repo, clone_url=CLONE, branches=branches)


def _poller_with_clock(
    *,
    tips,
    bindings_for_pass=lambda: [(REPO, "agent")],
    deployments_for_pass=lambda: [],
):
    """The poller plus the mutable clock both harnesses drive it with.

    Returns `(poller, now)`, where `now` is the `{"v": seconds}` dict the
    injected clock reads: advancing it is how a test crosses a backoff window
    without sleeping for five minutes.

    The two row sources are callables evaluated per query rather than fixed
    rows, so the harness that varies its answers between passes (`_run_passes`)
    and the one that answers identically for forty passes (the end-to-end
    timeout test, which needs the REAL `process_push` and so cannot use
    `_run_passes`) share one Session, factory and clock.
    """

    from contextlib import asynccontextmanager

    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings

    now = {"v": 0.0}

    class Session:
        async def execute(self, stmt):
            if "FROM curie.agents" in str(stmt):
                return bindings_for_pass()
            # Everything else is the deployments query: its SQL reads
            # `FROM curie.deployments d JOIN curie.agents a`, so it does not
            # match the substring above.
            return deployments_for_pass()

    @asynccontextmanager
    async def factory():
        yield Session()

    poller = CommitPoller(
        session_factory=factory,
        store=object(),
        settings=Settings(github_clone_base="https://github.com"),
        eval_queue=object(),
        tips=tips,
        interval_seconds=60,
        clock=lambda: now["v"],
    )
    return poller, now


@pytest.mark.anyio
async def test_notice_persistence_failure_retries_after_deploy_is_already_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A poller must keep its failed notice obligation when the deploy is settled."""
    from curie_api import gitflow
    from curie_api.schemas.deployments import WebhookResult

    deployed = {"value": False}
    deploy_calls = {"n": 0}

    async def deploy(_session, _store, _settings, _eval_queue, _payload):
        deploy_calls["n"] += 1
        deployed["value"] = True
        return WebhookResult(status="deployed", commit_sha=REAL_SHA)

    class FailingOnceNotice:
        def __init__(self) -> None:
            self.calls = 0

        async def publish(self, _session, _result, _payload, _settings) -> int:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("outbox write unavailable")
            return 1

    notice = FailingOnceNotice()
    monkeypatch.setattr(gitflow, "process_push", deploy)
    poller, _ = _poller_with_clock(
        tips=Tips({(REPO, "dev"): REAL_SHA, (REPO, "main"): None}),
        deployments_for_pass=lambda: [(REPO, "dev", REAL_SHA)] if deployed["value"] else [],
    )
    poller._notice_queue = notice

    await poller.poll_once()
    assert notice.calls == 1
    await poller.poll_once()
    assert notice.calls == 2
    assert deploy_calls["n"] == 1


# --------------------------------------------------------------------------- #
# Not deploying the same commit twice
# --------------------------------------------------------------------------- #
def test_an_unchanged_branch_is_not_redeployed() -> None:
    # The steady state. A poll every minute against an idle repo must produce
    # nothing at all, or the agent is redeployed once a minute forever.
    tips = Tips({(REPO, "dev"): "abc123"})
    assert moves_to_deploy([target("dev")], tips, {(REPO, "dev"): "abc123"}) == []


def test_a_moved_branch_is_deployed() -> None:
    tips = Tips({(REPO, "dev"): "def456"})
    moves = moves_to_deploy([target("dev")], tips, {(REPO, "dev"): "abc123"})
    assert [m.sha for m in moves] == ["def456"]


def test_a_branch_never_deployed_before_is_deployed() -> None:
    tips = Tips({(REPO, "dev"): "abc123"})
    assert len(moves_to_deploy([target("dev")], tips, {})) == 1


def test_a_restart_does_not_redeploy_current_head() -> None:
    # The poller holds no memory of its own; "already deployed" comes from what
    # is recorded against the repository. If it did not, every API restart
    # would redeploy every agent.
    tips = Tips({(REPO, "dev"): "abc123", (REPO, "main"): "zzz999"})
    state = {(REPO, "dev"): "abc123", (REPO, "main"): "zzz999"}
    assert moves_to_deploy([target("dev", "main")], tips, state) == []


def test_dev_and_prod_are_tracked_separately() -> None:
    # Same repository, two branches, two agents (ADR-0091). A dev push must not
    # mark prod as deployed.
    tips = Tips({(REPO, "dev"): "new111", (REPO, "main"): "old222"})
    state = {(REPO, "dev"): "old000", (REPO, "main"): "old222"}
    moves = moves_to_deploy([target("dev", "main")], tips, state)
    assert [(m.branch, m.sha) for m in moves] == [("dev", "new111")]


# --------------------------------------------------------------------------- #
# One broken repository must not stop the rest
# --------------------------------------------------------------------------- #
def test_a_failing_repository_does_not_stop_the_others() -> None:
    # A revoked credential or a deleted repo is a per-repo condition. Letting it
    # propagate would silently halt deploys for every other agent on the
    # cluster -- with no error anyone would look at.
    broken, fine = "octo/broken", "octo/fine"
    tips = Tips({(fine, "dev"): "abc123"}, explode={broken})
    targets = [target("dev", repo=broken), target("dev", repo=fine)]
    moves = moves_to_deploy(targets, tips, {})
    assert [m.repo_full_name for m in moves] == [fine]


def test_a_branch_the_repository_does_not_have_is_skipped() -> None:
    # A deploy.yaml may name a prod branch a repo has not created yet. Normal,
    # not an error.
    tips = Tips({(REPO, "dev"): "abc123", (REPO, "main"): None})
    moves = moves_to_deploy([target("dev", "main")], tips, {})
    assert [m.branch for m in moves] == ["dev"]


def test_polling_is_per_repository_not_per_agent() -> None:
    # Several agents share one repository. Asking once per branch is the
    # difference between one API call and N racing deploys of the same commit.
    tips = Tips({(REPO, "dev"): "abc123"})
    moves_to_deploy([target("dev")], tips, {})
    assert tips.asked == [(REPO, "dev")]


# --------------------------------------------------------------------------- #
# The payload the deploy path receives
# --------------------------------------------------------------------------- #
def test_the_payload_is_shaped_like_a_real_webhook() -> None:
    # Reusing process_push is what keeps polling and the webhook from
    # disagreeing about what a push means, and that only holds if the payload
    # carries the fields it parses.
    payload = Move(REPO, CLONE, "dev", "abc123").as_push_payload()
    assert payload["ref"] == "refs/heads/dev"
    assert payload["after"] == "abc123"
    assert payload["repository"]["full_name"] == REPO
    assert payload["repository"]["clone_url"] == CLONE


@pytest.mark.anyio
async def test_poll_once_sends_the_derived_clone_url_not_an_arbitrary_one(
    monkeypatch,
) -> None:
    """The origin check must hold for what poll_once ACTUALLY sends.

    This asserted the property against a hand-built `Move`, so replacing the
    real derivation inside poll_once with `https://evil.example/x.git` left it
    green (#1263). It was named for a security property and tested a
    constructor. Driving poll_once is the only version that means anything.
    """

    from curie_api.config import Settings
    from curie_api.gitflow import trusted_clone_url

    seen = await _capture_pushes(monkeypatch)
    derived = trusted_clone_url(REPO, Settings(github_clone_base="https://github.com"))
    assert [p["repository"]["clone_url"] for p in seen] == [derived]


@pytest.mark.anyio
async def test_a_cli_deployment_at_the_branch_tip_does_not_suppress_git_flow(
    clean_db: None, monkeypatch
) -> None:
    """A CLI archive at a Git SHA must not settle the Git flow poll baseline.

    Git flow owns the branch deploy and development eval fan out. This drives
    the real deployment baseline query because replacing the session would let
    a provenance filter pass without ever exercising its SQL.
    """

    from curie_api import gitflow
    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings
    from curie_api.models import Agent, AgentVersion, Deployment, Environment
    from curie_api.schemas.deployments import WebhookResult
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    sha = "branch-tip"
    engine = create_async_engine(get_settings().database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessionmaker() as session:
            agent = Agent(name="poller-cli-baseline", repo_full_name=REPO)
            session.add(agent)
            await session.flush()
            version = AgentVersion(
                agent_id=agent.id,
                version_label="cli-working-tree",
                created_by="cli",
                commit_sha=sha,
            )
            session.add(version)
            await session.flush()
            session.add(
                Deployment(
                    agent_id=agent.id,
                    version_id=version.id,
                    environment=Environment.dev,
                    commit_sha=sha,
                )
            )
            await session.commit()

        pushes: list[dict] = []

        async def capture(session, store, settings, eval_queue, payload):
            pushes.append(payload)
            return WebhookResult(status="deployed")

        monkeypatch.setattr(gitflow, "process_push", capture)
        poller = CommitPoller(
            session_factory=sessionmaker,
            store=object(),
            settings=Settings(github_clone_base="https://github.com"),
            eval_queue=object(),
            tips=Tips({(REPO, "dev"): sha, (REPO, "main"): None}),
            interval_seconds=60,
        )
        await poller.poll_once()
    finally:
        await engine.dispose()

    assert [payload["after"] for payload in pushes] == [sha]


@pytest.mark.anyio
async def test_a_git_flow_deployment_at_the_branch_tip_settles_the_poll_baseline(
    clean_db: None, monkeypatch
) -> None:
    """Matching Git flow provenance is the poller's durable baseline."""

    from curie_api import gitflow
    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings
    from curie_api.models import Agent, AgentVersion, Deployment, Environment
    from curie_api.schemas.deployments import WebhookResult
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    sha = "branch-tip"
    engine = create_async_engine(get_settings().database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessionmaker() as session:
            agent = Agent(name="poller-git-flow-baseline", repo_full_name=REPO)
            session.add(agent)
            await session.flush()
            version = AgentVersion(
                agent_id=agent.id,
                version_label="git-flow-branch-tip",
                created_by="git-flow",
                commit_sha=sha,
            )
            session.add(version)
            await session.flush()
            session.add(
                Deployment(
                    agent_id=agent.id,
                    version_id=version.id,
                    environment=Environment.dev,
                    commit_sha=sha,
                )
            )
            await session.commit()

        pushes: list[dict] = []

        async def capture(session, store, settings, eval_queue, payload):
            pushes.append(payload)
            return WebhookResult(status="deployed")

        monkeypatch.setattr(gitflow, "process_push", capture)
        poller = CommitPoller(
            session_factory=sessionmaker,
            store=object(),
            settings=Settings(github_clone_base="https://github.com"),
            eval_queue=object(),
            tips=Tips({(REPO, "dev"): sha, (REPO, "main"): None}),
            interval_seconds=60,
        )
        await poller.poll_once()
    finally:
        await engine.dispose()

    assert pushes == []


@pytest.mark.anyio
async def test_a_console_rollback_does_not_redefine_the_git_flow_poll_baseline(
    clean_db: None, monkeypatch
) -> None:
    """A newer null deployment cannot replace the last Git flow baseline."""

    from curie_api import gitflow
    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings
    from curie_api.models import Agent, AgentVersion, Deployment, Environment
    from curie_api.schemas.deployments import WebhookResult
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    older_sha = "git-flow-commit-a"
    current_sha = "git-flow-commit-b"
    deployed_at = datetime(2026, 1, 1)
    engine = create_async_engine(get_settings().database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessionmaker() as session:
            agent = Agent(name="poller-console-rollback", repo_full_name=REPO)
            session.add(agent)
            await session.flush()
            older = AgentVersion(
                agent_id=agent.id,
                version_label="git-flow-commit-a",
                created_by="git-flow",
                commit_sha=older_sha,
            )
            current = AgentVersion(
                agent_id=agent.id,
                version_label="git-flow-commit-b",
                created_by="git-flow",
                commit_sha=current_sha,
            )
            session.add_all([older, current])
            await session.flush()
            session.add_all(
                [
                    Deployment(
                        agent_id=agent.id,
                        version_id=older.id,
                        environment=Environment.dev,
                        commit_sha=older_sha,
                        deployed_at=deployed_at,
                    ),
                    Deployment(
                        agent_id=agent.id,
                        version_id=current.id,
                        environment=Environment.dev,
                        commit_sha=current_sha,
                        deployed_at=deployed_at + timedelta(seconds=1),
                    ),
                    Deployment(
                        agent_id=agent.id,
                        version_id=older.id,
                        environment=Environment.dev,
                        commit_sha=None,
                        deployed_at=deployed_at + timedelta(seconds=2),
                    ),
                ]
            )
            await session.commit()

        pushes: list[dict] = []

        async def capture(session, store, settings, eval_queue, payload):
            pushes.append(payload)
            return WebhookResult(status="deployed")

        monkeypatch.setattr(gitflow, "process_push", capture)
        poller = CommitPoller(
            session_factory=sessionmaker,
            store=object(),
            settings=Settings(github_clone_base="https://github.com"),
            eval_queue=object(),
            tips=Tips({(REPO, "dev"): current_sha, (REPO, "main"): None}),
            interval_seconds=60,
        )
        await poller.poll_once()
    finally:
        await engine.dispose()

    assert pushes == []


@pytest.mark.anyio
async def test_a_deployment_sha_does_not_forge_a_git_flow_poll_baseline(
    clean_db: None, monkeypatch
) -> None:
    """A Git flow version's commit is the baseline, not a mutable deployment SHA."""

    from curie_api import gitflow
    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings
    from curie_api.models import Agent, AgentVersion, Deployment, Environment
    from curie_api.schemas.deployments import WebhookResult
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    version_sha = "git-flow-commit-a"
    deployment_sha = "deployment-commit-b"
    engine = create_async_engine(get_settings().database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessionmaker() as session:
            agent = Agent(name="poller-forged-deployment-baseline", repo_full_name=REPO)
            session.add(agent)
            await session.flush()
            version = AgentVersion(
                agent_id=agent.id,
                version_label="git-flow-commit-a",
                created_by="git-flow",
                commit_sha=version_sha,
            )
            session.add(version)
            await session.flush()
            session.add(
                Deployment(
                    agent_id=agent.id,
                    version_id=version.id,
                    environment=Environment.dev,
                    commit_sha=deployment_sha,
                )
            )
            await session.commit()

        pushes: list[dict] = []

        async def capture(session, store, settings, eval_queue, payload):
            pushes.append(payload)
            return WebhookResult(status="deployed")

        monkeypatch.setattr(gitflow, "process_push", capture)
        poller = CommitPoller(
            session_factory=sessionmaker,
            store=object(),
            settings=Settings(github_clone_base="https://github.com"),
            eval_queue=object(),
            tips=Tips({(REPO, "dev"): version_sha, (REPO, "main"): None}),
            interval_seconds=60,
        )
        await poller.poll_once()
    finally:
        await engine.dispose()

    assert pushes == []


@pytest.mark.parametrize("branches", [(), ("dev",), ("dev", "main")])
def test_no_targets_or_no_branches_is_quiet(branches: tuple[str, ...]) -> None:
    tips = Tips({})
    assert moves_to_deploy([target(*branches)], tips, {}) == []
    assert moves_to_deploy([], tips, {}) == []


# --------------------------------------------------------------------------- #
# The wiring the lifespan hands it (#1250)
# --------------------------------------------------------------------------- #
def test_the_lifespan_wires_the_poller_to_the_bundle_store(clean_db: None) -> None:
    # The poller was constructed with `store=app.state.store`, an attribute
    # nothing assigns, so enabling it crashed the API at startup and the feature
    # never ran once. mypy cannot see it (State.__getattr__ returns Any) and no
    # test built the poller, so the only thing that catches it is running the
    # real lifespan with the interval on. The identity assertion is the point:
    # `is not None` would pass against a poller wired to the wrong object.
    #
    # clean_db (not _disposable_db) is load-bearing, not a style choice: on
    # entering the TestClient's `with` block, run_forever calls poll_once
    # immediately, before its first sleep, and poll_once queries curie.agents
    # for rows with a repo_full_name. If an agent row from an earlier test
    # survived, this pass would call GitHubBranchTip.sha_for and issue a real
    # httpx request to the GitHub API. clean_db's TRUNCATE is the only reason
    # that pass is a no-op; do not weaken this to _disposable_db.
    prior = os.environ.get("COMMIT_POLL_INTERVAL_S")
    os.environ["COMMIT_POLL_INTERVAL_S"] = "3600"
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            assert client.app.state.commit_poller_task is not None
            poller = client.app.state.commit_poller
            assert poller._store is client.app.state.bundle_store
            # Pins the actual defect: a "fix" that leaves store=app.state.store
            # in place and adds app.state.store = bundle_store as an alias would
            # satisfy the identity assertion above while reintroducing the
            # compatibility path this codebase forbids. Exactly one store name.
            assert not hasattr(client.app.state, "store")
    finally:
        if prior is None:
            os.environ.pop("COMMIT_POLL_INTERVAL_S", None)
        else:
            os.environ["COMMIT_POLL_INTERVAL_S"] = prior
        get_settings.cache_clear()


# --------------------------------------------------------------------------- #
# Rejections are reported, not called "deployed" (#1268)
# --------------------------------------------------------------------------- #
async def _capture_pushes(
    monkeypatch, *, deployed=None, tips=None, bindings=None
) -> list[dict]:
    """Run one real poll_once and return every payload it handed the deploy path.

    The point of driving the real method: every mutation the battery found
    living in poll_once -- a swapped branch mapping, ignored deployed-state, a
    forged clone_url, deploying nothing at all -- is invisible to a test that
    builds a Move by hand (#1263).
    """

    seen: list[dict] = []

    async def capture(session, store, settings, eval_queue, payload):
        from curie_api.schemas.deployments import WebhookResult

        seen.append(payload)
        return WebhookResult(status="deployed")

    await _run_poll_once(
        monkeypatch,
        None,
        on_push=capture,
        deployed=deployed,
        tips=tips,
        bindings=bindings,
    )
    return seen


async def _run_poll_once(
    monkeypatch,
    result,
    *,
    on_push=None,
    deployed=None,
    tips=None,
    bindings=None,
) -> None:
    """Drive the real poll_once with a stubbed deploy that returns `result`."""
    from contextlib import asynccontextmanager

    from curie_api import gitflow
    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings

    class Rows(list):
        def __iter__(self):  # noqa: D105 - the two queries both iterate
            return super().__iter__()

    class Session:
        async def execute(self, stmt):
            # First query lists bindings, second lists deployed state. Both
            # come from the real SQL in the module.
            if "FROM curie.agents" in str(stmt):
                return Rows(bindings if bindings is not None else [(REPO, "agent")])
            return Rows(deployed or [])

    @asynccontextmanager
    async def factory():
        yield Session()

    async def fake_process_push(session, store, settings, eval_queue, payload):
        return result

    monkeypatch.setattr(gitflow, "process_push", on_push or fake_process_push)
    poller = CommitPoller(
        session_factory=factory,
        store=object(),
        settings=Settings(github_clone_base="https://github.com"),
        eval_queue=object(),
        tips=tips or Tips({(REPO, "dev"): "abc123", (REPO, "main"): None}),
        interval_seconds=60,
    )
    await poller.poll_once()


@pytest.mark.anyio
async def test_a_rejected_polled_deploy_warns_from_the_poller(monkeypatch, caplog) -> None:
    # AC3: driven through poll_once, not by calling the logger directly. An
    # earlier version of this test called log_push_outcome itself and passed
    # even with the poller's call deleted -- it proved the logger worked, not
    # that the poller used it.
    from curie_api.schemas.deployments import WebhookResult

    rejected = WebhookResult(
        status="rejected", errors=[{"code": "deploy.unknown_agent", "message": "no such agent"}]
    )
    with caplog.at_level(logging.WARNING):
        await _run_poll_once(monkeypatch, rejected)

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, "a rejected polled deploy must warn"
    text = " ".join(r.getMessage() for r in warnings)
    assert "deploy.unknown_agent" in text, f"codes must be in the record: {text}"
    assert "commit poll" in text, f"the lane must be identifiable: {text}"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_a_rejected_polled_deploy_warns_with_its_codes(caplog) -> None:
    # The poller reported every outcome as "deployed" at INFO and discarded
    # result.errors. That is #1066 again -- the system reporting success for
    # work it did not do -- on the lane with no GitHub delivery UI to fall back
    # on, because polling exists for clusters GitHub cannot reach.
    from curie_api.gitflow import log_push_outcome
    from curie_api.schemas.deployments import WebhookResult

    rejected = WebhookResult(
        status="rejected", errors=[{"code": "deploy.unknown_agent", "message": "no such agent"}]
    )
    with caplog.at_level(logging.WARNING, logger="curie_api.gitflow"):
        log_push_outcome(
            rejected, Move(REPO, CLONE, "dev", "abc123").as_push_payload(), source="commit poll"
        )

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, "a rejected polled deploy must warn"
    text = warnings[0].getMessage()
    assert "deploy.unknown_agent" in text, f"the codes must be in the record: {text}"
    assert "commit poll" in text, f"the lane must be identifiable: {text}"


def test_a_successful_deploy_does_not_warn(caplog) -> None:
    from curie_api.gitflow import log_push_outcome
    from curie_api.schemas.deployments import WebhookResult

    with caplog.at_level(logging.WARNING, logger="curie_api.gitflow"):
        log_push_outcome(
            WebhookResult(status="deployed"),
            Move(REPO, CLONE, "dev", "abc").as_push_payload(),
            source="commit poll",
        )
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


# --------------------------------------------------------------------------- #
# Throttling backs off instead of re-asking (#1269)
# --------------------------------------------------------------------------- #
def _tip_reader(handler):
    """A GitHubBranchTip whose HTTP is answered by `handler`."""
    from curie_api.commitpoller import GitHubBranchTip
    from curie_api.config import Settings

    real = httpx.Client

    class Creds:
        def token_for(self, repo: str) -> str:
            return ""

    tip = GitHubBranchTip(Settings(), Creds())
    tip._client_factory = lambda **kw: real(transport=httpx.MockTransport(handler))  # type: ignore[attr-defined]
    return tip


def test_a_429_with_retry_after_is_respected_on_the_next_attempt(monkeypatch) -> None:
    # GitHub documents Retry-After (seconds) on a secondary rate limit, under
    # "Rate limits for the REST API". The failure this prevents: re-requesting
    # at the next interval, which is what turns a brief throttle into a
    # sustained one. An unauthenticated caller gets 60 requests/hour.
    from curie_api.commitpoller import GitHubBranchTip, RateLimited
    from curie_api.config import Settings

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "120"}, json={})

    real = httpx.Client
    monkeypatch.setattr(
        "curie_api.commitpoller.httpx.Client",
        lambda *a, **kw: real(transport=httpx.MockTransport(handler)),
    )

    class Creds:
        def token_for(self, repo: str) -> str:
            return ""

    tip = GitHubBranchTip(Settings(), Creds())
    with pytest.raises(RateLimited) as first:
        tip.sha_for(REPO, "dev")
    assert first.value.retry_after_s == pytest.approx(120, abs=2)

    # The second attempt must not reach GitHub at all.
    with pytest.raises(RateLimited):
        tip.sha_for(REPO, "dev")
    assert calls["n"] == 1, "a throttled repo was re-requested instead of backing off"


def test_sustained_throttling_is_reported_above_a_per_branch_warning(monkeypatch, caplog) -> None:
    # AC2 of #1269: one 429 is routine, several rounds running means the deploy
    # lane has stopped and must be findable as that rather than as a blip.
    from curie_api.commitpoller import GitHubBranchTip, RateLimited
    from curie_api.config import Settings

    real = httpx.Client
    monkeypatch.setattr(
        "curie_api.commitpoller.httpx.Client",
        lambda *a, **kw: real(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(429, headers={"Retry-After": "0"}, json={})
            )
        ),
    )

    class Creds:
        def token_for(self, repo: str) -> str:
            return ""

    tip = GitHubBranchTip(Settings(), Creds())
    with caplog.at_level(logging.WARNING, logger="curie_api.commitpoller"):
        # The noise that actually broke this test in CI, kept as a fixture. It
        # is what makes the assertion below a claim about THIS logger rather
        # than about whatever happened to log first.
        logging.getLogger("asyncio").error(
            "Future exception was never retrieved: ConnectionError"
        )
        for _ in range(3):
            with pytest.raises(RateLimited):
                tip.sha_for(REPO, "dev")

    # Filtered by logger and searched rather than indexed. `caplog.at_level`
    # raises the level for ONE logger but `caplog.records` holds everything the
    # root handler captured, so an unrelated ERROR from elsewhere in the process
    # lands in this list too -- asyncio's "Future exception was never retrieved"
    # is the one that actually did it, and `errors[0]` then decides the outcome
    # of a test about GitHub throttling.
    errors = [
        r
        for r in caplog.records
        if r.levelno >= logging.ERROR and r.name == "curie_api.commitpoller"
    ]
    assert errors, "sustained throttling must escalate above a per-branch warning"
    assert any("NOT happening" in r.getMessage() for r in errors), [
        r.getMessage() for r in errors
    ]


def test_a_throttled_repository_does_not_stop_the_others(monkeypatch) -> None:
    # RateLimited is an exception, and moves_to_deploy catches per repo/branch.
    # If that ever changed, one throttled repo would halt every other agent.
    from curie_api.commitpoller import RateLimited

    class Throttled:
        def sha_for(self, repo: str, branch: str) -> str | None:
            if repo == "octo/slow":
                raise RateLimited(repo, 120)
            return "abc123"

    moves = moves_to_deploy(
        [target("dev", repo="octo/slow"), target("dev", repo="octo/fine")], Throttled(), {}
    )
    assert [m.repo_full_name for m in moves] == ["octo/fine"]


# --------------------------------------------------------------------------- #
# poll_once, driven for real (#1263)
# --------------------------------------------------------------------------- #
@pytest.mark.anyio
async def test_poll_once_actually_deploys_something(monkeypatch) -> None:
    # The floor. "deploys nothing at all" survived the whole suite, because
    # nothing drove the method.
    assert await _capture_pushes(monkeypatch), "poll_once handed the deploy path nothing"


@pytest.mark.anyio
async def test_poll_once_maps_dev_to_the_dev_branch(monkeypatch) -> None:
    # Swapping the dev/prod branch mapping survived (#1263). The consequence is
    # the worst kind: a dev push deploying to the prod agent, reported as
    # success.
    seen = await _capture_pushes(monkeypatch)
    assert [p["ref"] for p in seen] == ["refs/heads/dev"]


@pytest.mark.anyio
async def test_poll_once_skips_a_commit_already_deployed(monkeypatch) -> None:
    """Ignoring already-deployed state survived, and is a redeploy loop.

    Every pass would redeploy every agent at the poll interval -- forever, with
    each one logged as a success.
    """

    seen = await _capture_pushes(monkeypatch, deployed=[(REPO, "dev", "abc123")])
    assert seen == [], f"redeployed a commit already recorded: {seen}"


@pytest.mark.anyio
async def test_poll_once_deploys_when_the_recorded_sha_differs(monkeypatch) -> None:
    # The other half: "skip everything" must not pass the test above.
    seen = await _capture_pushes(monkeypatch, deployed=[(REPO, "dev", "OLD0000")])
    assert [p["after"] for p in seen] == ["abc123"]


@pytest.mark.anyio
async def test_invalid_repo_full_name_is_skipped_without_blocking_valid_binding(
    monkeypatch, caplog
) -> None:
    invalid = "octo/../escape?token=x"
    valid = "Octo-Corp/repo.name_with-parts"
    tips = Tips(
        {
            (invalid, "dev"): "bad123",
            (valid, "dev"): "good456",
            (invalid, "main"): None,
            (valid, "main"): None,
        }
    )

    with caplog.at_level(logging.WARNING, logger="curie_api.commitpoller"):
        seen = await _capture_pushes(
            monkeypatch,
            tips=tips,
            bindings=[(invalid, "bad-agent"), (valid, "good-agent")],
        )

    assert [payload["repository"]["full_name"] for payload in seen] == [valid]
    assert invalid in " ".join(record.getMessage() for record in caplog.records)


@pytest.mark.anyio
async def test_run_forever_stops_when_cancelled() -> None:
    """Shutdown cancels this task and awaits it; it must actually end.

    Scope, stated honestly: this proves the loop is cancellable. It does NOT
    prove that a CancelledError handler swallowing the cancellation would be
    caught -- an idle poller spends nearly all its time in the interval sleep,
    which sits outside the try, so cancellation there propagates whatever any
    handler does. A version of this test that cancelled mid-pass could catch
    that, but only by creating a task that ignores cancellation, which cannot
    then be stopped and hangs the suite instead of failing it.

    What protects that property is structural instead: run_forever has no
    CancelledError handler at all, because CancelledError derives from
    BaseException and the `except Exception` below cannot catch it. There is no
    live code path to test -- which is why the guard was deleted rather than
    covered with an assertion it would not earn (#1263).
    """

    from curie_api.commitpoller import CommitPoller
    from curie_api.config import Settings

    class Boom:
        def sha_for(self, repo: str, branch: str) -> str | None:
            raise RuntimeError("every pass fails; the loop must still be cancellable")

    poller = CommitPoller(
        session_factory=None,
        store=None,
        settings=Settings(),
        eval_queue=None,
        tips=Boom(),
        interval_seconds=0.01,
    )
    task = asyncio.create_task(poller.run_forever())
    await asyncio.sleep(0.05)
    task.cancel()
    _, pending = await asyncio.wait([task], timeout=2.0)
    assert not pending, "run_forever did not stop when cancelled"
    assert task.cancelled()


# --------------------------------------------------------------------------- #
# GitHubBranchTip's HTTP behaviour (#1263)
# --------------------------------------------------------------------------- #
def _tip(monkeypatch, handler, token: str = "ghs_tok"):
    from curie_api.commitpoller import GitHubBranchTip
    from curie_api.config import Settings

    real = httpx.Client
    monkeypatch.setattr(
        "curie_api.commitpoller.httpx.Client",
        lambda *a, **kw: real(transport=httpx.MockTransport(handler)),
    )

    class Creds:
        def token_for(self, repo: str) -> str:
            return token

    return GitHubBranchTip(Settings(), Creds())


def test_the_tip_reader_authenticates(monkeypatch) -> None:
    # Never sending Authorization survived (#1263). On a private repo that is a
    # 404 -- which this code treats as "branch does not exist" -- so the agent
    # silently stops deploying and nothing reports an auth problem.
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"sha": "abc123"})

    _tip(monkeypatch, handler).sha_for(REPO, "dev")
    assert seen["auth"] == "Bearer ghs_tok"


def test_a_server_error_is_raised_not_read_as_a_missing_branch(monkeypatch) -> None:
    # Treating every status as OK survived (#1263). A 500 would parse as an
    # empty body, yield no sha, and look exactly like a branch that does not
    # exist -- so an outage reads as "nothing to deploy".
    tip = _tip(monkeypatch, lambda r: httpx.Response(500, json={}))
    with pytest.raises(httpx.HTTPStatusError):
        tip.sha_for(REPO, "dev")


def test_a_404_is_still_a_missing_branch(monkeypatch) -> None:
    # The deliberate exception: a deploy.yaml may name a branch a repo has not
    # created. This must stay non-fatal, or the test above would be satisfied
    # by raising on everything.
    tip = _tip(monkeypatch, lambda r: httpx.Response(404, json={}))
    assert tip.sha_for(REPO, "nope") is None


def test_a_valid_repository_is_preserved_in_the_commit_poll_url(monkeypatch) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"sha": "abc123"})

    repo = "Octo-Corp/repo.name_with-parts"
    assert _tip(monkeypatch, handler).sha_for(repo, "dev") == "abc123"
    assert str(captured[0].url) == (
        f"https://api.github.com/repos/{repo}/commits/dev"
    )


def test_invalid_repo_full_name_is_rejected_before_commit_poll_http(monkeypatch) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"sha": "abc123"})

    with pytest.raises(ValueError):
        _tip(monkeypatch, handler).sha_for("octo/../escape?token=x", "dev")

    assert captured == []


# Not re-cloning a commit that already settled (#1267)
# --------------------------------------------------------------------------- #
async def _run_passes(
    monkeypatch,
    outcomes,
    n: int = 2,
    binding_snapshots: list[tuple[str, ...]] | None = None,
    seconds_per_pass: float = 0.0,
    tips=None,
    after_pass=None,
    deployments: list[list[tuple[str, str, str]]] | None = None,
):
    """Run n consecutive poll_once passes; return (deploy-path calls, poller).

    Reaching the deploy path is what costs a full mirror clone -- the clone
    lives inside process_push -- so the count is exactly the thing #1267 and
    #1309 are about.

    `seconds_per_pass` advances the poller's injected clock between passes, so a
    backoff window can be crossed (or deliberately not crossed) without a test
    that sleeps for five minutes. The default of 0.0 keeps every earlier
    caller's timing semantics: passes back to back, no time elapsing.

    `outcomes` is what the stubbed deploy path returns: one WebhookResult for
    every call, or a list giving a different outcome per call -- which is what
    lets a test prove a retry actually reached process_push and what it returned
    when it did. The last entry of a list repeats once the list runs out.

    `deployments` supplies the rows the deployments query returns on each pass,
    as `(repo_full_name, environment, commit_sha)`. It defaults to no rows on
    every pass, which is what every earlier caller already got. Supplying rows is
    how a test stands in for the OTHER lane -- a webhook deploy the poller never
    saw -- since that query is the only place its result is visible here.
    """
    from curie_api import gitflow

    per_call = list(outcomes) if isinstance(outcomes, list) else [outcomes]
    calls = {"n": 0}

    async def counting(session, store, settings, eval_queue, payload):
        outcome = per_call[min(calls["n"], len(per_call) - 1)]
        calls["n"] += 1
        return outcome

    snapshots = binding_snapshots or [("agent",)] * n
    deployment_rows = deployments if deployments is not None else [[]] * n
    pass_index = {"value": 0}

    monkeypatch.setattr(gitflow, "process_push", counting)
    poller, now = _poller_with_clock(
        tips=tips or Tips({(REPO, "dev"): "abc123", (REPO, "main"): None}),
        bindings_for_pass=lambda: [(REPO, name) for name in snapshots[pass_index["value"]]],
        deployments_for_pass=lambda: deployment_rows[pass_index["value"]],
    )
    for _ in range(n):
        await poller.poll_once()
        if after_pass is not None:
            after_pass(poller, now["v"])
        pass_index["value"] += 1
        now["v"] += seconds_per_pass
    return calls["n"], poller


async def _passes(
    monkeypatch,
    outcomes,
    n: int = 2,
    binding_snapshots: list[tuple[str, ...]] | None = None,
    seconds_per_pass: float = 0.0,
) -> int:
    """How many of n passes reached the deploy path. See `_run_passes`."""

    calls, _ = await _run_passes(
        monkeypatch,
        outcomes,
        n=n,
        binding_snapshots=binding_snapshots,
        seconds_per_pass=seconds_per_pass,
    )
    return calls


@pytest.mark.anyio
async def test_an_ignored_branch_is_not_recloned_every_pass(monkeypatch) -> None:
    """AC2. A branch deploy.yaml has no target for is a blessed configuration.

    The poller's memory is the Deployment table, which records only successes,
    so an ignored push looked never-attempted and was re-cloned on every
    interval -- roughly 1,440 full mirror clones a day at the recommended 60s.
    """

    from curie_api.schemas.deployments import WebhookResult

    assert await _passes(monkeypatch, WebhookResult(status="ignored")) == 1


@pytest.mark.anyio
async def test_an_intrinsically_rejected_commit_is_not_recloned(monkeypatch) -> None:
    # An ambiguous environment is fixed only by changing the commit.
    from curie_api.schemas.deployments import WebhookResult

    rejected = WebhookResult(status="rejected", errors=[{"code": "deploy.ambiguous_env"}])
    assert await _passes(
        monkeypatch,
        rejected,
        n=3,
        binding_snapshots=[("agent",), ("agent", "new"), ("agent", "new")],
    ) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "code",
    [
        "deploy.agent_bound_elsewhere",
        "deploy.no_targets",
        "deploy.unknown_agent",
    ],
)
async def test_a_topology_rejection_waits_for_a_binding_change(monkeypatch, code: str) -> None:
    """Stable topology suppresses clones and one binding change reopens it."""
    from curie_api.schemas.deployments import WebhookResult

    rejected = WebhookResult(status="rejected", errors=[{"code": code}])
    assert await _passes(
        monkeypatch,
        rejected,
        n=4,
        binding_snapshots=[
            ("agent",),
            ("agent",),
            ("agent", "repaired"),
            ("agent", "repaired"),
        ],
    ) == 2


def _archive_failure(*extra_codes: str):
    """A rejection carrying `git.archive_failed` plus any extra codes."""
    from curie_api.schemas.deployments import WebhookResult

    codes = ["git.archive_failed", *extra_codes]
    return WebhookResult(status="rejected", errors=[{"code": code} for code in codes])


@pytest.mark.anyio
async def test_a_transient_clone_failure_IS_retried(monkeypatch) -> None:
    """The other side: remembering must not make a network blip permanent.

    An unreachable remote is the one rejection that says nothing about the
    commit, so suppressing its retry would strand a repository until the API
    restarted. #1309 bounds the RATE of those retries; it must not quietly turn
    them into the terminal state #1267 gives every other rejection. With enough
    time elapsing to clear even the hourly ceiling, every pass tries again.
    """

    assert await _passes(monkeypatch, _archive_failure(), n=4, seconds_per_pass=3600.0) == 4


@pytest.mark.anyio
async def test_a_repeatedly_failing_clone_is_not_recloned_every_pass(monkeypatch) -> None:
    """AC2 of #1309. The regression: ~1,440 full clones a day for one sha.

    Before this, `git.archive_failed` was retried on the very next pass and then
    settled -- two clones, then silence. The suppression half of the fix is that
    a second pass arriving before the backoff window has elapsed must not reach
    process_push at all, because the mirror clone lives inside it.
    """

    assert await _passes(monkeypatch, _archive_failure(), n=4, seconds_per_pass=0.0) == 1


@pytest.mark.anyio
async def test_an_archive_failure_recovers_once_the_backoff_has_elapsed(monkeypatch) -> None:
    """AC1 of #1309: one real timeout must stay recoverable.

    The second pass runs exactly at `next_attempt_at`, reaches process_push, and
    deploys. `_archive_backoff` being empty afterwards is the assertion that
    matters: only the deployed branch pops the record, so a run that never got
    past the filter -- or that got a second rejection -- would leave one behind.
    """

    from curie_api.schemas.deployments import WebhookResult

    calls, poller = await _run_passes(
        monkeypatch,
        [_archive_failure(), WebhookResult(status="deployed")],
        n=2,
        # Exactly _RETRY_BASE_DELAY_S: the window is "not before", so landing on
        # the boundary is a retry, not a skip.
        seconds_per_pass=300.0,
    )
    assert calls == 2, "the repaired repository never got its second clone"
    assert poller._archive_backoff == {}, "a successful deploy left failure state behind"
    assert poller._settled == {}, "a successful deploy must leave the database in charge"


@pytest.mark.anyio
async def test_the_backoff_window_holds_until_it_elapses(monkeypatch) -> None:
    """AC1 of #1309, negative control: 299s is still inside the window.

    299 and 300 seconds are the two sides of the same window, and asserting
    both -- through the real filter, with no private state read -- is what pins
    the boundary rather than merely the record's contents.
    """

    assert await _passes(monkeypatch, _archive_failure(), n=2, seconds_per_pass=299.0) == 1


@pytest.mark.anyio
async def test_the_retry_delay_grows_geometrically_and_is_capped(monkeypatch) -> None:
    """AC2 of #1309: increasing, capped delay -- asserted in seconds.

    The numbers are the point, so they are literals rather than recomputed from
    the module's constants: 5m, 10m, 20m, 40m, then an hour that does NOT keep
    doubling. The cap is what keeps a repository repaired later self-healing --
    an uncapped schedule reaches days and is terminal in all but name.
    """

    delays: list[float] = []

    def record(poller, now: float) -> None:
        delays.append(poller._archive_backoff[(REPO, "dev")].next_attempt_at - now)

    calls, _ = await _run_passes(
        monkeypatch,
        _archive_failure(),
        n=6,
        # Two hours clears every window, so all six passes really attempt.
        seconds_per_pass=7200.0,
        after_pass=record,
    )
    assert calls == 6
    assert delays == [300.0, 600.0, 1200.0, 2400.0, 3600.0, 3600.0]


@pytest.mark.anyio
async def test_a_changed_failure_class_restarts_the_backoff(monkeypatch) -> None:
    """AC2 of #1309: the count continues only for the SAME failure.

    A rejection whose codes changed is a different failure -- the repository
    moved from one problem to another -- and has not earned the accumulated
    delay of the previous one. Without the codes in the record, the second
    failure below would wait 600s on the strength of an unrelated first.
    """

    delays: list[float] = []

    def record(poller, now: float) -> None:
        delays.append(poller._archive_backoff[(REPO, "dev")].next_attempt_at - now)

    await _run_passes(
        monkeypatch,
        [_archive_failure(), _archive_failure("deploy.ambiguous_env")],
        n=2,
        seconds_per_pass=7200.0,
        after_pass=record,
    )
    assert delays == [300.0, 300.0], f"a different failure class inherited a delay: {delays}"


@pytest.mark.anyio
async def test_a_sustained_clone_failure_reports_the_lane_as_stalled(
    monkeypatch, caplog
) -> None:
    """AC3 of #1309: an Error record after a defined consecutive threshold.

    Two properties, and the second is the one that gets lost: nothing at ERROR
    for the first two attempts (a blip must not page anyone), and the error
    REPEATS on every attempt after the threshold. A lane parked at the hourly
    ceiling that reported itself once, hours ago, is back to being silent --
    which is the #1309 failure, not the fix for it.
    """

    seen: list[int] = []

    def count_errors(poller, now: float) -> None:
        seen.append(len([r for r in caplog.records if r.levelno >= logging.ERROR]))

    with caplog.at_level(logging.ERROR, logger="curie_api.commitpoller"):
        await _run_passes(
            monkeypatch,
            _archive_failure(),
            n=4,
            seconds_per_pass=7200.0,
            after_pass=count_errors,
        )

    assert seen == [0, 0, 1, 2], f"errors after each attempt: {seen}"
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    text = errors[0].getMessage()
    assert REPO in text, f"the stalled repository must be named: {text}"
    assert "NOT happening" in text, f"the record must say deploys have stopped: {text}"
    assert "credential" in text, f"the record must point at the likely causes: {text}"


@pytest.mark.anyio
async def test_a_new_commit_is_attempted_while_the_old_backoff_is_open(monkeypatch) -> None:
    """AC4 of #1309: failure state is per sha, and a new sha clears it.

    The backoff is a statement about one commit, not about the repository
    forever. Suppressing a NEW commit -- which may be the very push that fixes
    the repository -- would be a worse outage than the one #1309 is about. The
    negative control is `..._is_not_recloned_every_pass` above: same sha, same
    zero elapsed time, one clone.
    """

    class MovingTip:
        """A dev branch whose tip the test moves between passes."""

        def __init__(self, sha: str) -> None:
            self.sha = sha

        def sha_for(self, repo_full_name: str, branch: str) -> str | None:
            return self.sha if branch == "dev" else None

    tips = MovingTip("abc123")

    def push_a_new_commit(poller, now: float) -> None:
        tips.sha = "def456"

    calls, _ = await _run_passes(
        monkeypatch,
        _archive_failure(),
        n=2,
        # No time passes at all: the first sha's window is still wide open.
        seconds_per_pass=0.0,
        tips=tips,
        after_pass=push_a_new_commit,
    )
    assert calls == 2, "a new commit was suppressed by the previous commit's backoff"


@pytest.mark.anyio
async def test_a_successful_deploy_resets_the_attempt_count(monkeypatch) -> None:
    """AC4 of #1309: a deploy clears the record, it does not just pause it.

    If the record survived a success, the next unrelated failure would inherit
    the old attempt count -- backing off for 10 minutes on a first blip and
    reporting a stalled lane one failure early.
    """

    from curie_api.schemas.deployments import WebhookResult

    calls, poller = await _run_passes(
        monkeypatch,
        [_archive_failure(), WebhookResult(status="deployed"), _archive_failure()],
        n=3,
        seconds_per_pass=7200.0,
    )
    assert calls == 3
    assert poller._archive_backoff[(REPO, "dev")].attempts == 1, (
        "the failure after a successful deploy continued the old count"
    )


@pytest.mark.anyio
async def test_a_lane_failing_for_weeks_keeps_retrying_instead_of_crashing(
    monkeypatch,
) -> None:
    """AC2 of #1309: the hourly ceiling must be a ceiling, not a crash loop.

    `_RETRY_BASE_DELAY_S * 2 ** (attempts - 1)` evaluates the exponent in full
    before `min` clamps anything, and at attempt 1,025 that float multiplication
    raises OverflowError. A repository that fails every attempt reaches 1,025 in
    roughly 43 days at the hourly ceiling -- well inside the life of an API pod.
    The exception escapes poll_once before the new record is stored, so the
    already-expired record survives and the lane clones again on every single
    interval: exactly the ~1,440-clones-a-day regression #1309 exists to end,
    now permanent instead of transient.

    Driven through the real poll_once with no private state seeded, so on the
    unfixed code pass 1,025 raises out of poll_once and this errors -- there is
    no run_forever here to swallow it.
    """

    calls, _ = await _run_passes(
        monkeypatch,
        _archive_failure(),
        n=1026,
        # Two hours clears even the hourly ceiling, so every pass really
        # attempts and the attempt count really reaches 1,026.
        seconds_per_pass=7200.0,
    )
    assert calls == 1026, f"the poll pass stopped attempting after {calls}"


@pytest.mark.anyio
async def test_a_deploy_from_the_webhook_lane_clears_the_backoff(monkeypatch) -> None:
    """AC4 of #1309: a successful deploy is a successful deploy, either lane.

    The backoff record was cleared only when the POLLER itself saw a deploy. The
    webhook lane runs the same process_push without going near the poller, so:
    the poller fails on sha A and writes a 5-minute record; the webhook deploys
    sha B on that branch; the branch is rolled back to A. A is a move again, the
    stale record still matches it, its window is still open -- and the rollback
    is suppressed, even though the deploy of B is direct evidence the repository
    is clonable again. The poller must not sit on a stale failure for a branch
    the platform has since deployed.

    The negative control is `..._is_not_recloned_every_pass` above: same sha, no
    time elapsed, NO deployment change, one clone.
    """

    # Two more shas of the same shape as REAL_SHA, because this scenario needs
    # three distinct commits at once -- reusing REAL_SHA for both sides of a
    # rollback would prove nothing.
    polled_sha = "b" * 40
    webhook_sha = "c" * 40

    calls, _ = await _run_passes(
        monkeypatch,
        _archive_failure(),
        n=2,
        # No time passes at all: A's window is still wide open on pass 2, so the
        # only thing that can let it through is the deployment of B.
        seconds_per_pass=0.0,
        tips=Tips({(REPO, "dev"): polled_sha, (REPO, "main"): None}),
        deployments=[[], [(REPO, "dev", webhook_sha)]],
    )
    assert calls == 2, "a rollback was suppressed by a failure the other lane already fixed"


@pytest.mark.anyio
async def test_each_deploy_branch_backs_off_on_its_own_schedule(monkeypatch) -> None:
    """AC2 of #1309: "the same failure" includes the BRANCH, not just the repo.

    Every other test here exercises one branch, because the shared fixture
    gives `main` no tip and so only `dev` ever moves. That leaves the branch
    dimension of the key unpinned: a record keyed on the repository alone would
    have both deploy branches sharing one backoff, and each branch's failure
    would evict the other's record on every single pass -- so neither window
    would ever suppress anything and the repository would be back to a full
    mirror clone per branch per interval, which is the #1309 regression itself.

    Both branches are given a real, distinct tip so both move on every pass,
    and both fail the same way. What proves the isolation is that the third
    pass clones NOTHING: two independent windows, each opened by that branch's
    own second failure, are both still shut.
    """

    dev_sha = "d" * 40
    main_sha = "e" * 40

    attempts: dict[tuple[str, str], list[int]] = {}

    def record(poller, now: float) -> None:
        for key, backoff in poller._archive_backoff.items():
            attempts.setdefault(key, []).append(backoff.attempts)

    calls, poller = await _run_passes(
        monkeypatch,
        _archive_failure(),
        n=3,
        # One base window per pass: pass 2 lands exactly on `next_attempt_at`
        # and retries, and pass 3 lands inside the 600s that second failure
        # earned -- for BOTH branches, if each really has its own record.
        seconds_per_pass=300.0,
        tips=Tips({(REPO, "dev"): dev_sha, (REPO, "main"): main_sha}),
        after_pass=record,
    )

    assert calls == 4, (
        "two branches over three passes should clone twice each and then be "
        f"suppressed together, not {calls} times"
    )
    assert set(poller._archive_backoff) == {(REPO, "dev"), (REPO, "main")}, (
        f"the two deploy branches did not get their own records: {poller._archive_backoff}"
    )
    assert poller._archive_backoff[(REPO, "dev")].sha == dev_sha
    assert poller._archive_backoff[(REPO, "main")].sha == main_sha
    # Each branch failed twice, at t=0 and t=300, so each has earned 600s from
    # its own second failure -- a shared counter would read 1 or 4, never 2 on
    # both.
    assert attempts == {
        (REPO, "dev"): [1, 2, 2],
        (REPO, "main"): [1, 2, 2],
    }, f"the two branches did not count their failures separately: {attempts}"
    assert poller._archive_backoff[(REPO, "dev")].next_attempt_at == 900.0
    assert poller._archive_backoff[(REPO, "main")].next_attempt_at == 900.0


# --------------------------------------------------------------------------- #
# Driving a real subprocess timeout through the whole lane (#1309, AC5)
# --------------------------------------------------------------------------- #
def _timing_out_git(calls: list[list[str]]):
    """A `subprocess.run` that always times out, recording each invocation.

    120s is the timeout `clone_and_archive` actually passes, and both the clone
    and the archive use it. A repository too large to clone inside it fails this
    way on every attempt, forever -- which is the case #1309 exists for, and the
    one PR #1289 classified as transient.

    The received `timeout` keyword is recorded too, on the returned callable's
    `.timeouts` attribute, so a caller that wants to pin the bound (rather than
    just observe argv) can without changing the `calls` list every existing
    caller already asserts on.
    """
    timeouts: list[object] = []

    def run(*args, **kwargs):
        calls.append(list(args[0]) if args else [])
        timeouts.append(kwargs.get("timeout"))
        raise subprocess.TimeoutExpired(cmd=args[0] if args else ["git"], timeout=120)

    run.timeouts = timeouts
    return run


def test_a_clone_timeout_becomes_a_gitflow_error(monkeypatch) -> None:
    """AC5 of #1309, layer one: TimeoutExpired through `clone_and_archive`.

    `clone_and_archive` catches TimeoutExpired alongside CalledProcessError and
    re-raises GitFlowError. That conversion is the reason a timeout arrives at
    the poller as a rejection rather than as a crashed poll pass. The 120s bound
    itself is pinned here too: an unbounded git clone is a poll pass that never
    returns, and the bound is what makes the failure a rejection the backoff can
    classify rather than a hang.
    """

    from curie_api import gitflow
    from curie_api.config import Settings

    calls: list[list[str]] = []
    fake_run = _timing_out_git(calls)
    monkeypatch.setattr("curie_api.gitflow.subprocess.run", fake_run)

    with pytest.raises(gitflow.GitFlowError):
        gitflow.clone_and_archive(
            CLONE,
            REAL_SHA,
            Settings(github_clone_base="https://github.com"),
            repo_full_name=REPO,
            ref="refs/heads/dev",
        )
    assert calls and calls[0][:1] == ["git"], "the timeout must come from the git clone"
    assert fake_run.timeouts[0] == 120, "clone_and_archive must bound the clone at 120s"


@pytest.mark.anyio
async def test_process_push_reports_a_clone_timeout_as_archive_failed(monkeypatch) -> None:
    """AC5 of #1309, layer two: TimeoutExpired through `process_push`.

    This pins the code the poller classifies on. If a timeout ever stopped
    mapping to `git.archive_failed`, the backoff would never engage and the
    rejection would settle terminally instead -- silently, on the lane with no
    GitHub delivery UI. No database: `process_push` reaches the clone after only
    `crud.get_agents_by_repo`.
    """

    from curie_api import gitflow
    from curie_api.config import Settings
    from curie_api.crud import agents as crud_agents

    class StubAgent:
        repo_full_name = REPO
        name = "agent"

    async def one_agent(session, full_name):
        return [StubAgent()]

    monkeypatch.setattr(crud_agents, "get_agents_by_repo", one_agent)
    monkeypatch.setattr("curie_api.gitflow.subprocess.run", _timing_out_git([]))

    result = await gitflow.process_push(
        object(),
        object(),
        Settings(github_clone_base="https://github.com"),
        object(),
        Move(REPO, CLONE, "dev", REAL_SHA).as_push_payload(),
    )
    assert result.status == "rejected"
    assert [e.get("code") for e in (result.errors or [])] == ["git.archive_failed"]


@pytest.mark.anyio
async def test_a_permanently_timing_out_repository_stops_being_recloned(
    monkeypatch, caplog
) -> None:
    """AC5 of #1309, layer three: repeated poll_once over the REAL deploy path.

    The end-to-end proof, and the only test here that counts actual git
    invocations rather than a fake's calls. Forty passes at the recommended 60s
    interval is forty full mirror clones today; under the backoff the same forty
    passes clone at t=0, 300, 900 and 2100 seconds -- four attempts -- and the
    lane reports itself stalled instead of going quiet.
    """

    from curie_api.crud import agents as crud_agents

    class StubAgent:
        repo_full_name = REPO
        name = "agent"

    async def one_agent(session, full_name):
        return [StubAgent()]

    clones: list[list[str]] = []
    monkeypatch.setattr(crud_agents, "get_agents_by_repo", one_agent)
    monkeypatch.setattr("curie_api.gitflow.subprocess.run", _timing_out_git(clones))

    # The same plumbing `_run_passes` builds, minus its counting stub: this test
    # drives the REAL `process_push`, so the clones it counts are real git
    # invocations. The default row sources are exactly what it needs -- one
    # binding, no deployments, on every pass.
    poller, now = _poller_with_clock(tips=Tips({(REPO, "dev"): REAL_SHA, (REPO, "main"): None}))
    with caplog.at_level(logging.ERROR, logger="curie_api.commitpoller"):
        for _ in range(40):
            await poller.poll_once()
            now["v"] += 60.0

    assert len(clones) == 4, f"40 polls produced {len(clones)} clone attempts"
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, "a repository failing every attempt must report the lane as stalled"
    assert "NOT happening" in errors[0].getMessage()


@pytest.mark.anyio
async def test_a_successful_deploy_leaves_the_database_in_charge(monkeypatch) -> None:
    # After a real deploy the Deployment row is the memory. Keeping a private
    # copy too would mean two sources disagreeing after a rollback.
    from curie_api.schemas.deployments import WebhookResult

    assert await _passes(monkeypatch, WebhookResult(status="deployed")) == 2


# --------------------------------------------------------------------------- #
# One raising deploy must not stop the repositories behind it (#3736)
# --------------------------------------------------------------------------- #
BROKEN = "octo/exploding"
FINE = "octo/steady"


def _two_repo_harness(monkeypatch, push) -> tuple[object, dict]:
    """The clock harness with two bound repositories whose dev branches moved."""

    from curie_api import gitflow

    monkeypatch.setattr(gitflow, "process_push", push)
    return _poller_with_clock(
        tips=Tips(
            {
                (BROKEN, "dev"): "f" * 40,
                (FINE, "dev"): "g" * 40,
                (BROKEN, "main"): None,
                (FINE, "main"): None,
            }
        ),
        bindings_for_pass=lambda: [(BROKEN, "broken-agent"), (FINE, "fine-agent")],
    )


@pytest.mark.anyio
async def test_a_move_whose_deploy_raises_does_not_stop_the_pass(
    monkeypatch, caplog
) -> None:
    """AC1 and AC3 of #3736: the rest of the pass runs, then the pass fails.

    `process_push` maps git, archive and bundle validation errors to a rejected
    result, but the steps after them -- a stored bundle's object read, the
    bundle store write, database writes, the Valkey enqueue -- can raise. With
    no per-move handling the exception escaped the loop, so every repository
    queued behind the failing one never deployed, and the pass surfaced as one
    opaque failure rather than one attributed failure plus real deploys.
    """

    calls: list[str] = []

    async def push(session, store, settings, eval_queue, payload):
        from curie_api.schemas.deployments import WebhookResult

        repo = payload["repository"]["full_name"]
        calls.append(repo)
        if repo == BROKEN:
            # Not a rejection: an unguarded step raising out of process_push.
            raise RuntimeError("object store unreachable")
        return WebhookResult(status="deployed")

    poller, _ = _two_repo_harness(monkeypatch, push)
    with caplog.at_level(logging.ERROR, logger="curie_api.commitpoller"):
        # AC3: the pass must still report failure. It re-raises after the loop,
        # which is how poll_once marks the pass failed in span and metric.
        with pytest.raises(RuntimeError, match="object store unreachable"):
            await poller.poll_once()

    # AC1: the second repository's deploy ran, in the same pass, despite the
    # first one raising. On the unfixed code the exception escapes the loop
    # before this repository is ever reached.
    assert calls == [BROKEN, FINE], calls
    # AC1: the failure is attributed to the repository, branch and sha.
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, "a raising deploy must log at ERROR"
    text = errors[0].getMessage()
    assert BROKEN in text, f"the failing repository must be named: {text}"
    assert "dev" in text, f"the failing branch must be named: {text}"
    assert "ffffffff" in text, f"the failing sha must be named: {text}"


@pytest.mark.anyio
async def test_a_raising_move_is_backed_off_not_retried_first_every_pass(
    monkeypatch,
) -> None:
    """AC2 of #3736: an escaped exception must earn the capped backoff.

    Before this, the raising move was recorded in neither `_settled` nor
    `_archive_backoff`, so the next pass attempted it FIRST, with a fresh full
    mirror clone inside process_push, and blocked the same repositories behind
    it again -- the exact every-pass re clone regression #1309 ended for
    rejections, bypassed by the one failure shape that never returned one.
    """

    calls: list[str] = []

    async def push(session, store, settings, eval_queue, payload):
        from curie_api.schemas.deployments import WebhookResult

        repo = payload["repository"]["full_name"]
        calls.append(repo)
        if repo == BROKEN:
            raise RuntimeError("object store unreachable")
        return WebhookResult(status="deployed")

    poller, now = _two_repo_harness(monkeypatch, push)
    with pytest.raises(RuntimeError):
        await poller.poll_once()
    # No time passes, so the base 300s backoff window is wide open on pass 2.
    await poller.poll_once()

    assert calls == [BROKEN, FINE, FINE], calls
    assert (BROKEN, "dev") in poller._archive_backoff, (
        "a raising deploy must earn a backoff record"
    )


@pytest.mark.anyio
async def test_a_raising_deploy_recovers_once_the_backoff_has_elapsed(monkeypatch) -> None:
    """AC4 of #3736: the record is cleared by a successful deploy, as today.

    The backoff a raising move earns is the same record a retryable rejection
    earns, so everything that already clears it must keep working: the window
    opens after the base delay, the repaired repository attempts again, and a
    `deployed` result drops the record rather than pausing it.
    """

    calls: list[str] = []

    async def push(session, store, settings, eval_queue, payload):
        from curie_api.schemas.deployments import WebhookResult

        repo = payload["repository"]["full_name"]
        calls.append(repo)
        if repo == BROKEN and calls.count(BROKEN) == 1:
            raise RuntimeError("object store unreachable")
        return WebhookResult(status="deployed")

    poller, now = _two_repo_harness(monkeypatch, push)
    with pytest.raises(RuntimeError):
        await poller.poll_once()
    # Exactly _RETRY_BASE_DELAY_S: the window is "not before", so the second
    # pass attempts the repaired repository again.
    now["v"] += 300.0
    await poller.poll_once()

    assert calls == [BROKEN, FINE, BROKEN, FINE], calls
    assert poller._archive_backoff == {}, "a successful deploy left failure state behind"
    assert poller._settled == {}, "a successful deploy must leave the database in charge"


@pytest.mark.anyio
async def test_cancellation_from_process_push_propagates(monkeypatch) -> None:
    """AC5 of #3736: the pass is cancellable mid deploy.

    Structural, the same guarantee run_forever documents: CancelledError
    derives from BaseException, so the per-move `except Exception` cannot catch
    it. This pins that a future edit to that clause (say, `except
    BaseException` for symmetry with some later fix) cannot silently swallow
    shutdown, which is the #1263 shutdown hang again.
    """

    calls: list[str] = []

    async def push(session, store, settings, eval_queue, payload):
        from curie_api.schemas.deployments import WebhookResult

        repo = payload["repository"]["full_name"]
        calls.append(repo)
        if repo == BROKEN:
            raise asyncio.CancelledError()
        return WebhookResult(status="deployed")

    poller, _ = _two_repo_harness(monkeypatch, push)
    with pytest.raises(asyncio.CancelledError):
        await poller.poll_once()
    assert calls == [BROKEN], "cancellation must abort the rest of the pass"
    assert poller._archive_backoff == {}, "a cancellation is not a failure record"
