"""ADR 0279 polling envelope through the real task, Postgres and HTTP adapters.

GitHub shapes and short-page pagination follow the REST references in
``test_factory_poll_intake``; pull listings follow GitHub's Link headers:
https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from anyio.to_thread import current_default_thread_limiter
from curie_api import factory_poll_intake as intake
from curie_api.config import get_settings
from curie_api.db import create_engine, create_sessionmaker
from curie_api.factory_base_reconcile import initialize_base_selection
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import body_digest
from curie_api.github_app import GitHubCredentials
from curie_api.github_factory_events import FactoryNotice
from curie_api.main import create_app
from curie_api.models import Agent, ExecutionRequest, FactoryStatusComment, WorkItem
from curie_api.threadkeys import route_thread_key
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_telemetry import record_metric
from fastapi.testclient import TestClient
from forge_fakes.github import (
    APP_SLUG,
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    factory_env,
    fake_installation_tokens,
)
from test_factory_poll_intake import PollGitHub


class SimulatedClock:
    """Advance only after runnable work and real database IO have settled."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleepers: dict[asyncio.Future[None], float] = {}
        self.changed = asyncio.Event()

    async def wait(self, delay: float) -> None:
        future = asyncio.get_running_loop().create_future()
        self.sleepers[future] = self.now + max(0, delay)
        self.changed.set()
        try:
            await future
        finally:
            self.sleepers.pop(future, None)

    @property
    def seam(self) -> intake.IntakeClock:
        return intake.IntakeClock(lambda: self.now, self.wait)

    @staticmethod
    def chain(task: asyncio.Task[Any]) -> list[str]:
        names = []
        frame: Any = task.get_coro()
        while frame is not None:
            names.append(getattr(frame, "__qualname__", ""))
            frame = getattr(frame, "cr_await", getattr(frame, "gi_yieldfrom", None))
        return names

    async def drive(self, until: Any) -> None:
        loop = asyncio.get_running_loop()
        for _ in range(200000):
            if until():
                return
            self.changed.clear()
            await self.turn()
            others = asyncio.all_tasks() - {asyncio.current_task()}
            chains = {task: self.chain(task) for task in others}
            threads = current_default_thread_limiter().borrowed_tokens
            responses = sum(task.get_name() == "credential-response" for task in others)
            runnable = bool(loop._ready)
            blocked = set()
            for task, names in chains.items():
                future = getattr(task, "_fut_waiter", None)
                if future is None or future.done():
                    runnable = True
                elif (
                    future in self.sleepers
                    or isinstance(future, (asyncio.Task, asyncio.tasks._GatheringFuture))
                    or any(
                        n
                        in {
                            "Semaphore.acquire",
                            "Lock.acquire",
                            "Queue.get",
                            "TaskGroup._aexit",
                            "sleep",
                            "Event.wait",
                            "_wait",
                        }
                        for n in names
                    )
                ):
                    continue
                elif any("run_sync_in_worker_thread" in n for n in names) and threads <= responses:
                    continue
                else:
                    blocked.add(future)
            if runnable:
                continue
            if blocked:
                changed = asyncio.create_task(self.changed.wait())
                try:
                    async with asyncio.timeout(5):
                        await asyncio.wait((*blocked, changed), return_when=asyncio.FIRST_COMPLETED)
                except TimeoutError:
                    pending = [
                        (
                            task.get_name(),
                            self.chain(task),
                            type(getattr(task, "_fut_waiter", None)).__qualname__,
                        )
                        for task in others
                        if getattr(task, "_fut_waiter", None) in blocked
                    ]
                    raise AssertionError(
                        f"cadence clock blocked at {self.now}; tasks={pending}"
                    ) from None
                finally:
                    changed.cancel()
                    await asyncio.gather(changed, return_exceptions=True)
            elif self.sleepers:
                self.now = min(self.sleepers.values())
                for future, due in list(self.sleepers.items()):
                    if due <= self.now + 1e-8 and not future.done():
                        future.set_result(None)
        pending = [(t.get_name(), self.chain(t)) for t in others]
        raise AssertionError(f"cadence clock stalled at {self.now}; tasks={pending}")

    @staticmethod
    async def turn() -> None:
        ready = asyncio.Event()
        asyncio.get_running_loop().call_soon(ready.set)
        await ready.wait()


class EnvelopeGitHub(PollGitHub):
    def __init__(self, clock: SimulatedClock, repo: str, repository_id: int) -> None:
        super().__init__()
        self.clock, self.repo, self.repository_id = clock, repo, repository_id
        self.answers: dict[int, list[float]] = {}
        self.refused: set[int] = set()
        self.event_times: dict[int, float] = {}

    def _events(self, issue: Any) -> list[dict[str, Any]]:
        events = super()._events(issue)
        if issue.number in self.refused:
            events[0]["actor"] = {"id": 42, "login": "fixture[bot]", "type": "Bot"}
        return [{"id": n, "event": "renamed"} for n in range(98)] + events

    def _issue_body(self, issue: Any) -> dict[str, Any]:
        body = super()._issue_body(issue)
        body["labels"] += [{"name": f"acme-label-{n}"} for n in range(98)]
        body.update(title=f"Issue {issue.number}", body="Implement the fixture change.")
        return body

    def handle(self, request: httpx.Request) -> httpx.Response:
        path, params = request.url.path, request.url.params
        root = f"/repos/{REPO}"
        if path == root + "/pulls":
            page = int(params.get("page", "1"))
            rows = [
                {
                    "number": n,
                    "state": "closed",
                    "user": {"login": "octocat", "type": "User"},
                    "head": {"ref": f"topic/{n}"},
                    "base": {},
                }
                for n in range((page - 1) * 100, min(page * 100, 2500))
            ]
            headers = (
                {"Link": f'<{request.url.copy_set_param("page", str(page + 1))}>; rel="next"'}
                if page < 25
                else {}
            )
            return httpx.Response(200, json=rows, headers=headers)
        if (
            path.endswith("/comments")
            and "/issues/" in path
            and not path.endswith("/issues/comments")
        ):
            number = int(path.split("/")[-2])
            if request.method == "GET":
                history = [
                    {
                        "id": n + 1,
                        "body": "Earlier discussion",
                        "user": {"id": 1, "login": "octocat", "type": "User"},
                    }
                    for n in range(200)
                ]
                own = [
                    c
                    for c in self.comments.values()
                    if c["issue_url"].endswith(f"/issues/{number}")
                ]
                rows = history[: max(0, 200 - len(own))] + own
                per_page = int(params.get("per_page", "100"))
                page = int(params.get("page", "1"))
                return httpx.Response(200, json=rows[(page - 1) * per_page : page * per_page])
            if request.method == "POST":
                self.answers.setdefault(number, []).append(self.clock.now)
        if path.endswith("/labels") and "/issues/" in path:
            if request.method == "GET":
                return httpx.Response(200, json=[{"name": f"acme-label-{n}"} for n in range(99)])
            return httpx.Response(200, json=[])
        response = super().handle(request)
        return response

    def burst(self, count: int, *, offset: int = 0) -> list[int]:
        numbers = []
        for n in range(count):
            number = self.plant_issue(label=(LABEL, "factory-two")[n % 2])
            self.issues[number].labeled_at = datetime.now(UTC) - timedelta(seconds=1)
            self.event_times[number] = self.clock.now
            if (n + offset) % 2:
                self.refused.add(number)
            numbers.append(number)
        self._etags["issues"] = f'"burst-{len(self.issues)}"'
        return numbers


class World:
    def __init__(self, clock: SimulatedClock) -> None:
        self.clock = clock
        self.fakes = {
            f"acme-corp/acme-{n}": EnvelopeGitHub(clock, f"acme-corp/acme-{n}", REPO_ID + n)
            for n in range(20)
        }
        for fake in self.fakes.values():
            pull = fake.plant_pull()
            fake.add_review_comment(pull, 700001, "An earlier review comment")
            fake.add_review(pull, 700002, "An earlier review")
        self.agents: dict[tuple[str, str], uuid.UUID] = {}
        self.background: list[tuple[str, int, uuid.UUID]] = []
        self.calls: list[tuple[float, str, str, str]] = []
        self.delay = 0.2
        self.slow = False
        self.fail = False
        self.publication_failure = False
        self.recover = False
        self.restarted_at = 0.0
        self.metrics: list[tuple[float, str, float]] = []
        self.started: list[float] = []
        self.completed: list[float] = []
        self.loop: asyncio.AbstractEventLoop | None = None

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        repo = next(
            (r for r in self.fakes if path == f"/repos/{r}" or path.startswith(f"/repos/{r}/")),
            None,
        )
        assert repo is not None, path
        fake = self.fakes[repo]
        tail = path[len(f"/repos/{repo}") :]
        phase = "traffic" if tail in ("/issues/comments", "/pulls/comments") else "labels"
        self.calls.append((self.clock.now, repo, phase, str(request.url)))
        await self.clock.wait(120 if self.slow and phase == "traffic" else self.delay)
        if self.fail and tail.endswith("/events"):
            return httpx.Response(503, json={"message": "Unavailable"})
        inner = httpx.Request(
            request.method,
            request.url.copy_with(path=f"/repos/{REPO}{tail}"),
            headers=request.headers,
            content=request.content,
        )
        if self.publication_failure and tail == "/pulls" and request.url.params.get("page") == "2":
            return httpx.Response(503, json={"message": "Unavailable"})
        response = fake.handle(inner)

        def rename(value: Any) -> Any:
            if isinstance(value, str):
                return value.replace(REPO, repo)
            if isinstance(value, list):
                return [rename(v) for v in value]
            if isinstance(value, dict):
                result = {k: rename(v) for k, v in value.items()}
                if result.get("full_name") == repo and result.get("id") == REPO_ID:
                    result["id"] = fake.repository_id
                return result
            return value

        headers = {
            k: rename(v) for k, v in response.headers.items() if k.lower() in {"etag", "link"}
        }
        return httpx.Response(
            response.status_code,
            json=rename(response.json()) if response.content else None,
            headers=headers,
        )


@pytest.fixture
def cadence_world(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None
) -> Iterator[World]:
    clock = SimulatedClock()
    world = World(clock)
    for key, value in factory_env(tmp_path, intake="poll").items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    class Credentials:
        def charge(self, count: int) -> None:
            if world.loop is not None:

                async def response() -> None:
                    task = asyncio.current_task()
                    assert task is not None
                    task.set_name("credential-response")
                    for _ in range(count):
                        await world.clock.wait(world.delay)

                asyncio.run_coroutine_threadsafe(response(), world.loop).result()

        def fresh_installation_token(
            self, repo: str, expected_installation_id: int | None = None
        ) -> tuple[int, str]:
            assert repo in world.fakes
            self.charge(2)
            return INSTALLATION_ID, "fixture-installation-token"

        def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
            return self.fresh_installation_token(repo, installation_id)[1]

        def token_for(self, repo: str) -> str:
            return self.fresh_installation_token(repo)[1]

    credentials = Credentials()

    fake_installation_tokens(monkeypatch, credentials)

    def app_slug(self: GitHubCredentials) -> str:
        credentials.charge(1)
        return APP_SLUG

    monkeypatch.setattr(GitHubCredentials, "app_slug", app_slug)
    original_metric = getattr(intake, "record_metric", record_metric)

    def metric(name: str, value: float, *, attributes: dict[str, str]) -> None:
        world.metrics.append((clock.now, attributes["repository"], value))
        original_metric(name, value, attributes=attributes)

    monkeypatch.setattr(intake, "record_metric", metric, raising=False)
    original = httpx.AsyncClient

    class Client(original):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(world.handle)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    with TestClient(create_app()) as client:
        headers = {"X-API-Key": get_settings().api_key}
        for repo in world.fakes:
            for label in (LABEL, "factory-two", "factory-three", "factory-four"):
                response = client.post(
                    "/agents",
                    headers=headers,
                    json={
                        "name": "acme-" + repo.rsplit("-", 1)[1] + "-" + label,
                        "repo_full_name": repo,
                        "channel": {"kind": "github", "address": repo, "trigger_label": label},
                    },
                )
                assert response.status_code == 201, response.text
                world.agents[repo, label] = uuid.UUID(response.json()["id"])
        yield world
    get_settings.cache_clear()


async def exercise(
    world: World,
    passes: int = 2,
    *,
    stopped: bool = False,
    concentrated: bool = False,
    slow_reconciler: bool = False,
    require_answers: bool = True,
    stop_at: float | None = None,
    change_selection: bool = False,
) -> list[tuple[str, int]]:
    engine = create_engine()
    maker = create_sessionmaker(engine)
    clock = world.clock
    world.loop = asyncio.get_running_loop()
    started = []
    original = intake.poll_once

    async def observed(*args: Any, **kwargs: Any) -> None:
        started.append(clock.now)
        if world.recover and len(started) == 3:
            world.fail = False
            world.delay = 0.2
        await original(*args, **kwargs)
        world.completed.append(clock.now)

    intake.poll_once = observed
    try:
        targets = []
        selected = list(world.fakes.items())[: 1 if concentrated else 10]
        for index, (repo, fake) in enumerate(selected):
            targets += [(repo, n) for n in fake.burst(10 if concentrated else 1, offset=index)]
        async with maker() as session:
            for fake in world.fakes.values():
                agent = await session.get(Agent, world.agents[fake.repo, LABEL])
                assert agent is not None
                fingerprint = None
                for index in range(50 - len(fake.issues)):
                    number = fake.plant_issue()
                    issue = fake.issues[number]
                    item = WorkItem(
                        id=uuid.uuid4(),
                        tenant_id=agent.tenant_id,
                        agent_id=agent.id,
                        conversation_id=route_thread_key(
                            "github", None, fake.repo, f"issue-{number}"
                        ),
                        tracker_kind="github",
                        tracker_host="github.com",
                        tracker_scope_id=str(fake.repository_id),
                        tracker_issue_id=str(number),
                        code_host_kind="github",
                        code_host_host="github.com",
                        repository_project_id=str(fake.repository_id),
                        repository_path=fake.repo,
                        code_host_installation_id=INSTALLATION_ID,
                        trigger_label=LABEL,
                        next_sequence=2,
                        base_selection_fingerprint=fingerprint,
                    )
                    session.add(item)
                    if fingerprint is None:
                        await session.flush()
                        await initialize_base_selection(
                            session,
                            item_id=item.id,
                            labels={label["name"] for label in fake._issue_body(issue)["labels"]},
                            default_branch=fake.default_branch,
                            settings=get_settings(),
                        )
                        fingerprint = item.base_selection_fingerprint
                    notice = FactoryNotice(
                        uuid.uuid4(),
                        "issues",
                        "labeled",
                        "admit",
                        INSTALLATION_ID,
                        fake.repository_id,
                        fake.repo,
                        number,
                        6601,
                        "octocat",
                        label_event_at=issue.labeled_at,
                        label=LABEL,
                        label_event_id=issue.event_id,
                    )
                    request = ExecutionRequest(
                        id=notice.request_id,
                        work_item_id=item.id,
                        work_item=item,
                        tenant_id=agent.tenant_id,
                        sequence=1,
                        status="waiting",
                        wait_deadline=datetime.now(UTC) + timedelta(hours=1),
                        objective=f"https://github.com/{fake.repo}/issues/{number}",
                        requester="github:6601:octocat",
                        reply_kind="github",
                        reply_address=fake.repo,
                        reply_conversation_id=f"issue-{number}",
                    )
                    session.add(request)
                    if index % 2:
                        item.base_branch = fake.default_branch
                        item.base_source = "default"
                        item.base_commit = fake.branches[fake.default_branch]
                        item.base_resolved_request_id = request.id
                        item.base_selection_fingerprint = None
                    await session.flush()
                    comment = fake.add_issue_comment(
                        number, next(fake._comment_ids), "Queued work\n" + marker_for(request.id)
                    )
                    session.add(
                        FactoryStatusComment(
                            execution_request_id=request.id,
                            work_item_id=item.id,
                            posted_at=datetime.now(UTC),
                            comment_id=comment["id"],
                            comment_list="issue",
                            rendered_digest=body_digest(comment["body"]),
                            applied_label="curie-factory:queued",
                        )
                    )
                    world.background.append((fake.repo, number, request.id))
            await session.commit()
        if change_selection:
            for repo, number, _ in world.background[:2]:
                world.fakes[repo].issues[number].base_labels = ("base:main",)

        async def drive() -> None:
            def finished() -> bool:
                if slow_reconciler and started:
                    assert started[0] <= 60, "The reconciler delayed the first label pass"
                return (stop_at is not None and clock.now > stop_at) or (
                    len(world.completed) >= passes - 1
                    and len(started) >= passes
                    and (
                        not require_answers
                        or all(world.fakes[r].answers.get(n) for r, n in targets)
                    )
                )

            await clock.drive(finished)

        async def running(until: Any) -> None:
            task = asyncio.create_task(
                intake.run_forever(maker, get_settings(), clock=clock.seam), name="intake"
            )
            try:
                await until()
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        if stopped:
            for repo, number in targets:
                world.fakes[repo].issues[number].labeled = False

            async def first_pass() -> None:
                await clock.drive(lambda: len(world.completed) == 1)

            await running(first_pass)
            for repo, number in targets:
                fake = world.fakes[repo]
                fake.issues[number].labeled = True
                fake.event_times[number] = clock.now + 1
                fake._etags["issues"] = '"after-restart"'
            clock.now += 200
            world.restarted_at = clock.now
            passes += 1

        if slow_reconciler:
            app = create_app()
            async with app.router.lifespan_context(app):
                await drive()
        else:
            await running(drive)
        world.started = started
        return targets
    finally:
        intake.poll_once = original
        world.loop = None
        await engine.dispose()


@pytest.mark.parametrize("concentrated", [False, True])
def test_cadence_envelope_order_listing(
    cadence_world: World,
    concentrated: bool,
    record_property: Callable[[str, object], None],
) -> None:
    world = cadence_world
    targets = asyncio.run(exercise(world, concentrated=concentrated))
    label_end = max(at for at, _, _ in world.metrics[:20])
    record_property("first_label_pass_seconds", label_end)
    assert label_end <= 15
    assert world.started[:2] == pytest.approx([0, 45])
    for repo, number in targets:
        assert len(world.fakes[repo].answers[number]) == 1
        assert world.fakes[repo].answers[number][0] - world.fakes[repo].event_times[number] <= 60
    traffic = [at for at, _, phase, _ in world.calls if phase == "traffic" and at < 45]
    answers = [world.fakes[r].answers[n][0] for r, n in targets]
    assert traffic and min(traffic) >= max(answers)
    assert max(answers) <= 15
    assert min(traffic) >= label_end
    assert max(value for _, _, value in world.metrics) <= 60
    assert any(world.fakes[r].refused for r, n in targets)
    for repo in world.fakes:
        first_pages = [
            url
            for at, r, _, url in world.calls
            if at < 45
            and r == repo
            and "/pulls?" in url
            and "page=" not in url.replace("per_page=", "")
        ]
        assert len(first_pages) <= 1


def test_cadence_answered_admissions_reconcile_changed_selection(cadence_world: World) -> None:
    world = cadence_world
    asyncio.run(exercise(world, change_selection=True))

    async def selections() -> list[tuple[str | None, str | None, str | None, int]]:
        engine = create_engine()
        try:
            async with create_sessionmaker(engine)() as session:
                selected = []
                for _, _, request_id in world.background[:2]:
                    request = await session.get(ExecutionRequest, request_id)
                    assert request is not None
                    item = await session.get(WorkItem, request.work_item_id)
                    assert item is not None
                    selected.append(
                        (
                            item.base_branch,
                            item.base_source,
                            item.base_selection_fingerprint,
                            item.version,
                        )
                    )
                return selected
        finally:
            await engine.dispose()

    unresolved, pinned = asyncio.run(selections())
    assert unresolved[:2] == (None, None)
    assert unresolved[2] is not None
    assert unresolved[3] == 2
    assert pinned == ("main", "label", None, 2)


def test_cadence_restart(cadence_world: World) -> None:
    world = cadence_world
    targets = asyncio.run(exercise(world, stopped=True))
    assert world.started[-2:] == pytest.approx([world.restarted_at, world.restarted_at + 45])
    assert all(
        len(world.fakes[r].answers[n]) == 1
        and world.fakes[r].answers[n][0] - world.restarted_at <= 60
        for r, n in targets
    )


def test_cadence_slow_phase_two_rotates(cadence_world: World) -> None:
    world = cadence_world
    world.slow = True
    asyncio.run(exercise(world, passes=3))
    assert world.started[:3] == pytest.approx([0, 45, 90])
    traffic = [(at, repo) for at, repo, phase, _ in world.calls if phase == "traffic"]
    assert len(traffic) >= 2
    assert traffic[0][1] != traffic[1][1]
    assert traffic[0][0] < 45 <= traffic[1][0] < 90


def test_cadence_envelope_serial_negative_control(
    cadence_world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(intake, "REPOSITORY_CONCURRENCY", 1)
    monkeypatch.setattr(intake, "ISSUE_CONCURRENCY", 1)
    targets = asyncio.run(exercise(cadence_world, stop_at=15))
    assert cadence_world.clock.now > 15
    assert not cadence_world.completed
    assert any(not cadence_world.fakes[r].answers.get(n) for r, n in targets)


def test_cadence_age_beyond_envelope_and_retry(cadence_world: World) -> None:
    world = cadence_world
    world.delay = 2
    world.fail = True
    world.recover = True
    asyncio.run(exercise(world, passes=5, require_answers=False))
    ages = [value for _, _, value in world.metrics]
    assert max(ages) > 60
    first = world.metrics[0][1]
    values = [value for _, repo, value in world.metrics if repo == first]
    assert len(values) >= 4 and values[1] > values[0]
    assert values[-1] <= 60


def test_cadence_slow_reconciler_lifespan(
    cadence_world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api import main

    world = cadence_world
    settings = get_settings().model_copy(update={"work_item_reconciler_enabled": True})
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(intake.IntakeClock, "production", classmethod(lambda cls: world.clock.seam))
    began = []

    async def slow(self: WorkItemReconciler) -> None:
        began.append(world.clock.now)
        await world.clock.wait(120)

    monkeypatch.setattr(WorkItemReconciler, "_settle_publications", slow)
    asyncio.run(exercise(world, slow_reconciler=True))
    assert began and world.started[:2] == pytest.approx([0, 45])
    assert max(at for at, _, _ in world.metrics[:20]) <= 15


def test_cadence_listing_failure_is_shared_and_retried(cadence_world: World) -> None:
    world = cadence_world
    world.publication_failure = True
    asyncio.run(exercise(world, concentrated=True, passes=3, require_answers=False))
    repo, fake = next(iter(world.fakes.items()))
    assert len(fake.answers) == 5
    listings = [url for _, r, _, url in world.calls if r == repo and "/pulls?" in url]
    first = [url for url in listings if "page=" not in url.replace("per_page=", "")]
    assert len(first) == 2
    assert len(listings) == 4
    ages = [value for _, r, value in world.metrics if r == repo]
    assert ages[1] > ages[0]
