"""Production polling against real Postgres, substituting GitHub HTTP only."""

from __future__ import annotations

import asyncio
import copy
import json
import re
from datetime import timedelta
from typing import Any

import pytest
from curie_api.factory_facts_exchange import _answer_facts
from curie_api.factory_facts_schema import FactoryFacts
from forge_fakes.github import LABEL, REPO_ID


def facts(world: Any, number: int = 42) -> FactoryFacts:
    answers = world.answers(number)
    assert len(answers) == 1
    answer = _answer_facts(answers[0]["body"])
    assert answer is not None
    return answer


def assert_no_work(world: Any) -> None:
    assert asyncio.run(world.rows("work_items")) == []
    assert asyncio.run(world.rows("execution_requests")) == []
    assert asyncio.run(world.rows("publication_review_reservations")) == []
    assert asyncio.run(world.rows("remediation_reservations")) == []
    assert not [
        r
        for r in world.github.calls
        if r.method in {"POST", "PATCH", "DELETE"} and "/labels" in r.url.path
    ]


@pytest.mark.parametrize("app", [None, {"id": 8001}])
@pytest.mark.parametrize("permission", ["write", "maintain", "admin"])
def test_writer_answer_layout_schema_and_no_admission(
    facts_world: Any, app: Any, permission: str
) -> None:
    world = facts_world
    world.permissions["dana"] = permission
    request = world.add(app=app)
    ordinary = world.github.add_issue_comment(42, 100, "@curie-factory-bot ordinary comment")
    ordinary["performed_via_github_app"] = {"id": 8001}
    world.cycle()
    answer = facts(world)
    assert answer.request.comment_id == "101"
    assert answer.subject.repository_id == str(REPO_ID)
    assert answer.label_route.route == ("github_user_via_app" if app else "github_user")
    assert answer.label_route.state == "eligible"
    assert answer.admission == "not_requested"
    assert answer.deployment_readiness == "unvalidated"
    assert answer.expires_at - answer.observed_at == timedelta(seconds=120)
    assert [e.name for e in answer.executables] == ["uv", "g++", "python3.13"]
    assert all(
        e.state == "unknown" and e.reason == "observation_missing"
        for e in [*answer.executables, *answer.services, answer.model_credit]
    )
    body = world.answers()[0]["body"]
    assert (
        body.index("**")
        < body.index("<details>")
        < body.index("```json")
        < body.index("</details>")
        < body.index("<!-- curie-factory-facts:v1 ")
    )
    assert body.count("<details>") == body.count("```json") == 1
    assert re.search(
        rf"<!-- curie-factory-facts:v1 label={LABEL} request=101 digest=sha256:[0-9a-f]{{64}} -->",
        body,
    )
    import jsonschema
    from test_factory_facts import VECTORS

    jsonschema.validate(
        answer.model_dump(mode="json"),
        json.loads((VECTORS / "factory-facts-v1.schema.json").read_text()),
    )
    assert request["body"] == world.github.comments[101]["body"]
    assert_no_work(world)


@pytest.mark.parametrize(
    "user,app,permission",
    [
        ({"id": 77, "login": "bot", "type": "Bot"}, None, "write"),
        ({"id": 77, "login": "install", "type": "Organization"}, None, "write"),
        ({"id": 77, "login": "dana", "type": "User"}, {"id": 51}, "write"),
        ({"id": 77, "login": "ghost", "type": "User"}, None, "write"),
        ({"id": 77, "login": "dana", "type": "User"}, None, "read"),
        ({"id": 77, "login": "dana", "type": "User"}, None, "triage"),
        ({"id": None, "login": "dana", "type": "User"}, None, "write"),
    ],
)
def test_ineligible_actors_read_no_observations_or_delivery_records(
    facts_world: Any, user: Any, app: Any, permission: str
) -> None:
    world = facts_world
    world.permissions[user["login"]] = permission
    world.add(user=user, app=app)
    world.cycle()
    assert world.answers() == []
    assert world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []
    assert_no_work(world)


def test_failed_permission_retries_comment_without_replaying_page(facts_world: Any) -> None:
    world = facts_world
    world.add()
    world.permission_failure = True
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []
    world.permission_failure = False
    world.cycle()
    assert facts(world).label_route.state == "eligible"
    pages = [r for r in world.github.calls if r.url.path.endswith("/issues/comments")]
    assert "if-none-match" in pages[-1].headers
    cursor = asyncio.run(world.rows("factory_poll_cursors"))[0]
    assert cursor["comments_since"] is not None
    assert not any(key.startswith("facts-retry:") for key in cursor["etags"])


def test_permission_failure_at_delivery_keeps_owed_for_next_cycle(facts_world: Any) -> None:
    world = facts_world
    world.add()
    world.mutate_on_read = lambda w: setattr(w, "permission_failure", True)
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "owed"
    world.mutate_on_read = None
    world.permission_failure = False
    world.cycle()
    assert facts(world).request.comment_id == "101"


@pytest.mark.parametrize("failure", ["duplicate", "failed_write", "lost_write", "restart"])
def test_delivery_recovery_keeps_exactly_one_answer(facts_world: Any, failure: str) -> None:
    world = facts_world
    world.add()
    world.fail_write = failure in {"failed_write", "restart"}
    world.lose_write = failure == "lost_write"
    world.cycle()
    if world.fail_write or failure == "lost_write":
        assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "owed"
    if failure == "restart":
        # A fresh process has no comment page to replay; only SQL survives.
        world.github.comments.pop(101)
        world.add()
    world.fail_write = False
    world.cycle()
    world.cycle()
    assert facts(world).request.comment_id == "101"
    assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "answered"
    assert_no_work(world)


def test_newer_request_wins_and_spoofed_marker_is_untouched(facts_world: Any) -> None:
    world = facts_world
    spoof = world.github.add_issue_comment(
        42,
        90,
        "<!-- curie-factory-facts:v1 label=curie-factory request=999 digest=sha256:"
        + "a" * 64
        + " -->",
    )
    old = copy.deepcopy(spoof)
    world.add(101)
    world.fail_write = True
    world.cycle()
    world.add(102, executables=["jq"])
    world.fail_write = False
    world.cycle()
    assert facts(world).request.comment_id == "102"
    assert world.github.comments[90] == old
    assert len(world.answers()) == 1
    assert all(
        r["request_comment_id"] != 101 for r in asyncio.run(world.rows("factory_facts_requests"))
    )


def test_identical_unexpired_requests_do_not_rewrite(facts_world: Any) -> None:
    world = facts_world
    world.add()
    world.cycle()
    original = world.answers()[0]["body"]
    world.add(102)
    world.cycle()
    assert world.answers()[0]["body"] == original
    assert len(asyncio.run(world.rows("factory_facts_requests"))) == 2


@pytest.mark.parametrize("edit", ["digest", "invalid", "marker_removed", "deleted", "actor"])
def test_owed_request_changed_before_write_is_dropped(facts_world: Any, edit: str) -> None:
    world = facts_world
    world.add()
    world.fail_write = True
    world.cycle()
    assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "owed"
    if edit == "deleted":
        world.github.comments.pop(101)
    elif edit == "actor":
        world.github.comments[101]["user"]["id"] = 88
    else:
        body = world.github.comments[101]["body"]
        world.github.comments[101]["body"] = (
            body.replace('"uv"', '"jq"')
            if edit == "digest"
            else body.replace('"schema_version": 1', '"schema_version": 2')
            if edit == "invalid"
            else "ordinary"
        )
    world.fail_write = False
    world.cycle()
    assert world.answers() == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []


def test_answered_request_edit_never_rewrites_old_answer(facts_world: Any) -> None:
    world = facts_world
    world.add()
    world.cycle()
    original = world.answers()[0]["body"]
    world.github.comments[101]["body"] = world.github.comments[101]["body"].replace('"uv"', '"jq"')
    world.github._etags["issue-comments"] = '"edited"'
    world.cycle()
    assert world.answers()[0]["body"] == original


def test_rate_limit_drops_seventh_request(facts_world: Any, caplog: Any) -> None:
    world = facts_world
    for i in range(7):
        world.add(101 + i, number=42 + i)
    world.cycle()
    assert sum(len(world.answers(42 + i)) for i in range(7)) == 6
    assert len(asyncio.run(world.rows("factory_facts_requests"))) == 6


def test_lost_success_is_counted_before_the_next_rate_decision(facts_world: Any) -> None:
    world = facts_world
    for index in range(5):
        world.add(101 + index, number=42 + index)
    world.cycle()
    world.add(106, number=47)
    world.lose_write = True
    world.cycle()
    assert sum(len(world.answers(42 + i)) for i in range(7)) == 6
    world.add(107, number=48)
    world.cycle()
    assert sum(len(world.answers(42 + i)) for i in range(7)) == 6


@pytest.mark.parametrize("status", [404, 410])
def test_gone_owed_issue_is_discarded_without_blocking_other_answers(
    facts_world: Any, status: int
) -> None:
    world = facts_world
    world.add()
    world.add(102, number=43)
    world.fail_write = True
    world.cycle()
    world.github.failures["/repos/acme-corp/acme-bot/issues/42/comments"] = status
    world.fail_write = False
    world.cycle()
    assert world.answers(42) == []
    assert len(world.answers(43)) == 1
    rows = asyncio.run(world.rows("factory_facts_requests"))
    assert [(r["issue_number"], r["state"]) for r in rows] == [(43, "answered")]


def test_failed_writes_reserve_the_six_request_window(facts_world: Any) -> None:
    world = facts_world
    world.fail_write = True
    for index in range(12):
        world.add(101 + index, number=42 + index)
    world.cycle()
    rows = asyncio.run(world.rows("factory_facts_requests"))
    assert len(rows) == 6
    assert all(row["state"] == "owed" for row in rows)
    assert not [
        request
        for request in world.github.calls
        if re.search(r"/issues/comments/10[7-9]$|/issues/comments/11[0-2]$", request.url.path)
    ]
    world.fail_write = False
    world.cycle()
    assert sum(len(world.answers(42 + index)) for index in range(12)) == 6
    assert all(
        row["state"] == "answered" for row in asyncio.run(world.rows("factory_facts_requests"))
    )


def test_over_cap_owed_row_is_dropped_before_github_reads(facts_world: Any) -> None:
    from curie_api.models import FactoryFactsRequest

    world = facts_world
    world.fail_write = True
    for index in range(6):
        world.add(101 + index, number=42 + index)
    world.cycle()

    async def overflow() -> None:
        async with world.sessions() as session:
            session.add(
                FactoryFactsRequest(
                    tracker_host="github.com",
                    repository_id=str(REPO_ID),
                    issue_number=48,
                    trigger_label=LABEL,
                    request_comment_id=107,
                    digest="sha256:" + "a" * 64,
                    actor_id="77",
                    state="owed",
                )
            )
            await session.commit()

    asyncio.run(overflow())
    world.github.calls.clear()
    world.fail_write = False
    world.cycle()
    assert not [
        r
        for r in world.github.calls
        if "/issues/48/" in r.url.path or r.url.path.endswith("/issues/comments/107")
    ]
    assert len(asyncio.run(world.rows("factory_facts_requests"))) == 6
    assert sum(len(world.answers(42 + index)) for index in range(6)) == 6


def test_unserved_label_is_silent_and_never_scans_answers(facts_world: Any) -> None:
    world = facts_world
    world.add(label="other-factory")
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert not [r for r in world.github.calls if r.url.path.endswith("/issues/42/comments")]


def test_disabled_factory_does_not_answer(facts_world: Any) -> None:
    world = facts_world
    world.settings = world.settings.model_copy(update={"factory_enabled": False})
    world.add()
    world.cycle()
    assert world.answers() == world.observation_reads == []


def test_request_on_pull_request_is_silent_before_review_handling(facts_world: Any) -> None:
    world = facts_world
    world.pull_requests.add(42)
    world.add()
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []
    assert_no_work(world)


@pytest.mark.parametrize("failure", ["crash", "deadline", "unparseable"])
def test_authority_guard_fails_closed(
    facts_world: Any, monkeypatch: Any, caplog: Any, failure: str
) -> None:
    import curie_api.factory_facts_exchange as exchange
    import httpx

    world = facts_world
    world.add()
    original = world.handle
    assert 0 < exchange.FACTS_DEADLINE_SECONDS < 60
    monkeypatch.setattr(exchange, "FACTS_DEADLINE_SECONDS", 0.25)

    async def broken(request: httpx.Request) -> httpx.Response:
        if "/collaborators/" in request.url.path:
            if failure == "crash":
                raise RuntimeError("private-guard-crash-sentinel")
            if failure == "deadline":
                await asyncio.Event().wait()
            return httpx.Response(200, content=b"{broken-json")
        return original(request)

    world.handle = broken
    asyncio.run(asyncio.wait_for(world.poll(), timeout=3))
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []
    assert "private-guard-crash-sentinel" not in caplog.text
    assert_no_work(world)


@pytest.mark.parametrize("failure", ["crash", "deadline"])
def test_delivery_guard_failure_leaves_owed_and_recovers(
    facts_world: Any, monkeypatch: Any, failure: str
) -> None:
    import curie_api.factory_facts_exchange as exchange
    import httpx

    world = facts_world
    world.add()
    world.fail_write = True
    world.cycle()
    assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "owed"
    world.fail_write = False
    original = world.handle
    monkeypatch.setattr(exchange, "FACTS_DEADLINE_SECONDS", 0.25)

    async def broken(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/issues/comments/101"):
            if failure == "crash":
                raise RuntimeError("private-delivery-crash-sentinel")
            await asyncio.Event().wait()
        return original(request)

    world.handle = broken
    asyncio.run(asyncio.wait_for(world.poll(), timeout=3))
    assert world.answers() == []
    assert asyncio.run(world.rows("factory_facts_requests"))[0]["state"] == "owed"
    world.handle = original
    monkeypatch.setattr(exchange, "FACTS_DEADLINE_SECONDS", 45.0)
    world.cycle()
    assert facts(world).request.comment_id == "101"


@pytest.mark.parametrize("failure", ["crash", "deadline", "unparseable"])
def test_recovery_guard_isolates_one_unreadable_request(
    facts_world: Any, monkeypatch: Any, failure: str
) -> None:
    import curie_api.factory_facts_exchange as exchange
    import httpx

    world = facts_world
    world.add()
    world.fail_write = True
    world.cycle()
    world.fail_write = False
    world.add(102, number=43)
    original = world.handle
    monkeypatch.setattr(exchange, "FACTS_DEADLINE_SECONDS", 0.25)

    async def broken(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/issues/42/comments"):
            if failure == "crash":
                raise RuntimeError("private-recovery-crash-sentinel")
            if failure == "deadline":
                await asyncio.Event().wait()
            return httpx.Response(200, content=b"{broken-json")
        return original(request)

    world.handle = broken
    asyncio.run(asyncio.wait_for(world.poll(), timeout=3))
    assert world.answers(42) == []
    assert len(world.answers(43)) == 1
    assert len(asyncio.run(world.rows("factory_facts_requests"))) == 2
    assert_no_work(world)
    world.handle = original
    monkeypatch.setattr(exchange, "FACTS_DEADLINE_SECONDS", 45.0)
    world.cycle()
    assert len(world.answers(42)) == len(world.answers(43)) == 1


def test_invalid_requests_get_no_reply_and_log_only_reason(facts_world: Any, caplog: Any) -> None:
    from test_factory_facts import REQUESTS, request_body

    world = facts_world
    for index, vector in enumerate(REQUESTS["invalid"]):
        comment = world.add(101 + index, number=42 + index)
        comment["body"] = request_body(vector["request"])
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []
    events = [r for r in caplog.records if r.name == "curie_api.factory_facts_exchange"]
    assert len(events) == len(REQUESTS["invalid"])
    assert all(r.getMessage().startswith("factory facts request ignored reason=") for r in events)
    assert_no_work(world)


def test_two_install_databases_touch_only_their_label(
    facts_world: Any, isolated_migration_db: Any
) -> None:
    import dataclasses
    import uuid

    from curie_api.config import get_settings
    from curie_api.models import Agent, AgentChannel
    from forge_fakes.github import REPO
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    world = facts_world
    isolated_migration_db.at("0166")
    other_engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    other_sessions = async_sessionmaker(other_engine, expire_on_commit=False)
    other_id = uuid.uuid4()

    async def seed() -> None:
        async with other_sessions() as session:
            session.add(Agent(id=other_id, name="acme-other-install"))
            await session.flush()
            session.add(
                AgentChannel(
                    agent_id=other_id, kind="github", address=REPO, trigger_label=LABEL + "x"
                )
            )
            await session.commit()

    asyncio.run(seed())
    other = dataclasses.replace(
        world, sessions=other_sessions, engine=other_engine, agent_id=other_id, observation_reads=[]
    )
    try:
        world.add()
        world.cycle()
        original = copy.deepcopy(world.answers())
        before = len(world.github.calls)
        other.cycle()
        assert world.answers() == original
        assert other.observation_reads == []
        assert asyncio.run(other.rows("factory_facts_requests")) == []
        assert not [
            r for r in world.github.calls[before:] if r.url.path.endswith("/issues/42/comments")
        ]
        other.add(102, number=42, label=LABEL + "x")
        other.cycle()
        assert len(other.answers(42)) == 2
        before = copy.deepcopy(other.answers(42))
        world.cycle()
        assert other.answers(42) == before
    finally:
        asyncio.run(other_engine.dispose())


def test_ambiguous_binding_never_reads_or_writes_an_answer(facts_world: Any) -> None:
    import uuid

    from curie_api.models import Agent, AgentChannel
    from forge_fakes.github import REPO

    world = facts_world

    async def collide() -> None:
        async with world.sessions() as session:
            second = uuid.uuid4()
            session.add(Agent(id=second, name="acme-ambiguous"))
            await session.flush()
            session.add(
                AgentChannel(agent_id=second, kind="github", address=REPO, trigger_label=LABEL)
            )
            await session.commit()

    asyncio.run(collide())
    world.add()
    world.cycle()
    assert world.answers() == world.observation_reads == []
    assert asyncio.run(world.rows("factory_facts_requests")) == []


def test_webhook_request_is_ignored_before_mention_and_revision(facts_world: Any) -> None:
    import hashlib
    import hmac
    import uuid

    import httpx
    from curie_api.config import get_settings
    from curie_api.main import create_app
    from forge_fakes.github import _comment_event

    world = facts_world
    comment = world.add()
    app = create_app()
    app.state.sessionmaker = world.sessions
    app.state.bundle_store = object()
    app.state.eval_queue = object()
    payload = _comment_event(42, comment_id=101, body=comment["body"])
    body = json.dumps(payload).encode()
    signature = (
        "sha256="
        + hmac.new(get_settings().github_webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    )

    async def send() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers={
                    "X-GitHub-Event": "issue_comment",
                    "X-GitHub-Delivery": str(uuid.uuid4()),
                    "X-Hub-Signature-256": signature,
                },
            )
            assert response.status_code == 200
            assert response.json()["status"] == "ignored"

    asyncio.run(send())
    assert world.answers() == world.observation_reads == []
    assert_no_work(world)
