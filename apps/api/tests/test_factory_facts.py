"""ADR 0280's frozen request, schema and cached observation boundary."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from curie_api.factory_facts_schema import REQUEST_MARKER, InvalidFactsRequest, parse_request

VECTORS = Path(__file__).resolve().parents[3] / "tests/vectors"
REQUESTS = json.loads((VECTORS / "factory-facts-requests.json").read_text())
REPLIES = json.loads((VECTORS / "factory-facts-replies.json").read_text())


@pytest.mark.parametrize("vector", REPLIES["cases"], ids=lambda v: v["id"])
def test_shared_reply_vectors_use_the_real_marker_reader_and_parser(vector: dict[str, Any]) -> None:
    import httpx
    from curie_api.factory_facts_exchange import _answer_facts
    from curie_api.factory_facts_schema import marker_prefix
    from curie_api.forges.github.comments import GitHubMarkedComments, static_token
    from curie_api.forges.types import ReplyTarget, TrackerIssueRef

    async def read() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json=[] if vector["comment"] is None else [vector["comment"]]
                )
            )
        ) as client:
            adapter = GitHubMarkedComments(
                client,
                api="https://api.github.com",
                host="github.com",
                repo_full_name="acme-corp/acme-bot",
                repository_id=4401,
                token=static_token("example-installation-token"),
                app_id=REPLIES["factory_app_id"],
            )
            found = await adapter.find_facts(
                ReplyTarget.on_issue(TrackerIssueRef("github", "github.com", "4401", "42")),
                marker_prefix(REPLIES["request"]["trigger_label"]),
            )
            parsed = _answer_facts(found.body) if found is not None else None
            now = datetime.fromisoformat(REPLIES["now"])
            accepted = parsed is not None and parsed.expires_at > now
            assert accepted == vector["accepted"]
            if accepted:
                assert (
                    parsed.request.digest
                    == parse_request(request_body(REPLIES["request"])).digest()
                )
                assert parsed.admission == "not_requested"
                assert parsed.deployment_readiness == "unvalidated"
            if vector["reason"] == "forged":
                assert found is None
            if vector["reason"] == "stale":
                assert parsed is not None and parsed.expires_at == now

    asyncio.run(read())


@pytest.mark.parametrize(
    "vector", [v for v in REPLIES["cases"] if v["accepted"]], ids=lambda v: v["id"]
)
def test_reader_produces_the_shared_valid_replies(facts_world: Any, vector: dict[str, Any]) -> None:
    import redis.asyncio as redis
    from curie_api.factory_facts import read_facts
    from curie_api.factory_facts_exchange import _answer_facts
    from curie_api.factory_facts_schema import RequestIdentity
    from curie_api.killswitch import KillSwitch
    from curie_api.models import AgentChannel, FactoryFactObservation
    from sqlalchemy import select

    expected = _answer_facts(vector["comment"]["body"])
    assert expected is not None
    world = facts_world
    settings = (
        world.settings.model_copy(update={"factory_model_auth_mode": "personal"})
        if expected.label_route.state == "ineligible"
        else world.settings
    )

    async def read() -> None:
        valkey = redis.Redis.from_url(settings.valkey_dsn())
        try:
            if expected.factory.upgrade_pause == "paused":
                await valkey.set(settings.upgrade_quiesce_key(), "1")
            async with world.sessions() as session:
                binding = await session.scalar(
                    select(AgentChannel).where(AgentChannel.agent_id == world.agent_id)
                )
                binding.trigger_label = expected.subject.trigger_label
                for kind, entries in (
                    ("tool", [*expected.executables, *expected.services]),
                    ("model_credit", [expected.model_credit]),
                ):
                    for item in entries:
                        if item.reason == "observation_missing":
                            continue
                        session.add(
                            FactoryFactObservation(
                                kind=kind,
                                agent_id=world.agent_id,
                                scope=expected.subject.repository_id if kind == "tool" else "",
                                item=item.name if kind == "tool" else "",
                                subject_digest="sha256:" + "b" * 64,
                                state=item.state,
                                reason=item.reason,
                                observed_at=item.observed_at,
                                expires_at=expected.expires_at,
                            )
                        )
                await session.commit()
                actual = await read_facts(
                    session,
                    settings,
                    valkey,
                    repository="acme-corp/acme-bot",
                    repository_id=expected.subject.repository_id,
                    query=parse_request(request_body(REPLIES["request"])),
                    issue_number=expected.subject.issue_number,
                    request=RequestIdentity(
                        comment_id=expected.request.comment_id, digest=expected.request.digest
                    ),
                    route=expected.label_route.model_copy(
                        update={"state": "eligible", "reason": None}
                    ),
                    now=expected.observed_at,
                )
                actual_json, expected_json = (
                    actual.model_dump(mode="json"),
                    expected.model_dump(mode="json"),
                )
                actual_json.pop("context_digest")
                expected_json.pop("context_digest")
                assert actual_json == expected_json
        finally:
            await KillSwitch(valkey).resume(world.agent_id)
            if expected.factory.upgrade_pause == "paused":
                await valkey.delete(settings.upgrade_quiesce_key())
            await valkey.aclose()

    asyncio.run(read())


def request_body(value: dict[str, Any], prose: str = "") -> str:
    return (
        (prose + "\n\n" if prose else "")
        + REQUEST_MARKER
        + "\n```json\n"
        + json.dumps(value)
        + "\n```"
    )


@pytest.mark.parametrize("run", ["`", "```", "````"])
def test_answer_label_cannot_escape_its_code_span_or_json_fold(run: str) -> None:
    from curie_api.factory_facts_exchange import _answer_facts
    from curie_api.factory_facts_schema import answer_body
    from markdown_it import MarkdownIt

    source = next(case for case in REPLIES["cases"] if case["accepted"])
    facts = _answer_facts(source["comment"]["body"])
    assert facts is not None
    label = run + " @acme-reviewers <https://example.com> " + run
    facts.subject.trigger_label = label
    body = answer_body(facts, "dana")
    parsed = _answer_facts(body)
    assert parsed is not None and parsed.subject.trigger_label == label
    renderer = MarkdownIt("commonmark")
    tokens = renderer.parse(body)
    spans = [
        child for token in tokens for child in (token.children or []) if child.type == "code_inline"
    ]
    fences = [token for token in tokens if token.type == "fence"]
    assert [token.content for token in spans] == [label]
    assert len(fences) == 1
    assert json.loads(fences[0].content)["subject"]["trigger_label"] == label
    assert len(fences[0].markup) > len(run)
    assert "<a " not in renderer.render(body)


@pytest.mark.parametrize("vector", REQUESTS["valid"], ids=lambda v: v["name"])
def test_shared_valid_request_vectors(vector: dict[str, Any]) -> None:
    parsed = parse_request(request_body(vector["request"], "Check tools. This starts no work."))
    assert parsed.digest() == vector["digest"]
    assert parsed.executables == vector["request"]["executables"]


@pytest.mark.parametrize("vector", REQUESTS["invalid"], ids=lambda v: v["name"])
def test_shared_invalid_request_vectors(vector: dict[str, Any]) -> None:
    with pytest.raises(InvalidFactsRequest):
        parse_request(request_body(vector["request"]))


@pytest.mark.parametrize(
    "body",
    [
        REQUEST_MARKER,
        REQUEST_MARKER + "\n```json\n{}\n```\nextra",
        "```json\n{}\n```",
        REQUEST_MARKER + "\n```json\n" + " " * 4097 + "{}\n```",
    ],
)
def test_invalid_layout_and_size(body: str) -> None:
    with pytest.raises(InvalidFactsRequest):
        parse_request(body)


@pytest.mark.parametrize("vector", REQUESTS["invalid_bodies"], ids=lambda v: v["name"])
def test_request_parser_denies_unparseable_input_and_decoder_crash(vector: dict[str, Any]) -> None:
    with pytest.raises(InvalidFactsRequest):
        parse_request(vector["body"])


def test_fenced_request_byte_limit_has_a_live_boundary() -> None:
    raw = json.dumps(REQUESTS["valid"][0]["request"])
    fence = "```json\n" + raw + "\n```"
    padding = " " * (4096 - len(fence.encode()))
    valid = REQUEST_MARKER + "\n```json\n" + raw + padding + "\n```"
    assert parse_request(valid).digest() == REQUESTS["valid"][0]["digest"]
    with pytest.raises(InvalidFactsRequest):
        parse_request(valid.replace(padding, padding + " "))


def test_committed_response_schema_matches_model() -> None:
    from curie_api.factory_facts_schema import FactoryFacts

    assert (
        json.loads((VECTORS / "factory-facts-v1.schema.json").read_text())
        == FactoryFacts.model_json_schema()
    )


def test_response_schema_examples_reject_unlisted_fields_and_states() -> None:
    import copy

    import jsonschema
    from curie_api.factory_facts_schema import FactoryFacts
    from pydantic import ValidationError

    schema = json.loads((VECTORS / "factory-facts-v1.schema.json").read_text())
    for example in json.loads((VECTORS / "factory-facts-v1.examples.json").read_text()):
        jsonschema.validate(example, schema)
        assert FactoryFacts.model_validate_json(json.dumps(example)).admission == "not_requested"
        extra = copy.deepcopy(example)
        extra["amount"] = 12345.67
        with pytest.raises(ValidationError):
            FactoryFacts.model_validate_json(json.dumps(extra))
        boolean = copy.deepcopy(example)
        boolean["schema_version"] = True
        with pytest.raises(ValidationError):
            FactoryFacts.model_validate_json(json.dumps(boolean))
        wrong = copy.deepcopy(example)
        wrong["factory"]["kill_switch"] = "healthy"
        with pytest.raises(ValidationError):
            FactoryFacts.model_validate_json(json.dumps(wrong))


def test_observation_reader_returns_live_row_and_expires_it(facts_world: Any) -> None:
    from curie_api.factory_facts_store import read_observation
    from curie_api.models import FactoryFactObservation

    async def go() -> None:
        now = datetime.now(UTC)
        async with facts_world.sessions() as session:
            missing = await read_observation(
                session,
                kind="tool",
                agent_id=facts_world.agent_id,
                scope="4401",
                item="uv",
                now=now,
            )
            assert (missing.state, missing.reason) == ("unknown", "observation_missing")
            session.add(
                FactoryFactObservation(
                    kind="tool",
                    agent_id=facts_world.agent_id,
                    scope="4401",
                    item="uv",
                    subject_digest="sha256:" + "a" * 64,
                    state="present",
                    reason=None,
                    observed_at=now,
                    expires_at=now + timedelta(seconds=30),
                )
            )
            await session.commit()
            live = await read_observation(
                session,
                kind="tool",
                agent_id=facts_world.agent_id,
                scope="4401",
                item="uv",
                now=now,
            )
            assert (live.state, live.reason, live.observed_at) == ("present", None, now)
            expired = await read_observation(
                session,
                kind="tool",
                agent_id=facts_world.agent_id,
                scope="4401",
                item="uv",
                now=now + timedelta(seconds=30),
            )
            assert (expired.state, expired.reason) == ("unknown", "observation_expired")

    asyncio.run(go())


@pytest.mark.parametrize("state", ["clear", "killed", "paused", "unreadable", "personal"])
def test_field_truth_from_current_state(facts_world: Any, state: str) -> None:
    import redis.asyncio as redis
    from curie_api.killswitch import KillSwitch
    from test_factory_facts_intake import facts

    world = facts_world

    async def set_state() -> None:
        client = redis.Redis.from_url(world.settings.valkey_dsn())
        try:
            if state == "killed":
                await KillSwitch(client).kill(world.agent_id)
            if state == "paused":
                await client.set(world.settings.upgrade_quiesce_key(), "1")
        finally:
            await client.aclose()

    asyncio.run(set_state())
    original = world.settings
    if state == "unreadable":
        world.settings = world.settings.model_copy(update={"valkey_url": "redis://127.0.0.1:1"})
    if state == "personal":
        world.settings = world.settings.model_copy(update={"factory_model_auth_mode": "personal"})
    try:
        world.add()
        world.cycle()
        answer = facts(world)
        assert answer.factory.kill_switch == (
            "engaged" if state == "killed" else "unknown" if state == "unreadable" else "clear"
        )
        assert answer.factory.upgrade_pause == (
            "paused" if state == "paused" else "unknown" if state == "unreadable" else "clear"
        )
        assert answer.factory.code_host_credential.state == "unknown"
        assert answer.factory.code_host_credential.reason == "not_probed"
        assert answer.label_route.state == ("ineligible" if state == "personal" else "eligible")
        assert answer.label_route.reason == (
            "personal_owner_required" if state == "personal" else None
        )
    finally:

        async def cleanup() -> None:
            client = redis.Redis.from_url(original.valkey_dsn())
            try:
                await KillSwitch(client).resume(world.agent_id)
                if state == "paused":
                    await client.delete(original.upgrade_quiesce_key())
            finally:
                await client.aclose()

        asyncio.run(cleanup())


@pytest.mark.parametrize("binding_state", ["bound", "unbound", "label_not_served", "ambiguous"])
def test_operator_api_auth_binding_states_and_privacy(
    facts_world: Any, binding_state: str, caplog: Any, monkeypatch: Any
) -> None:
    import uuid

    import httpx
    import redis.asyncio as redis
    from curie_api.config import get_settings
    from curie_api.main import create_app
    from curie_api.models import Agent, AgentChannel, AgentVersion, Deployment, Environment
    from forge_fakes.github import LABEL, REPO
    from sqlalchemy import delete

    world = facts_world

    async def configure() -> None:
        async with world.sessions() as session:
            version = AgentVersion(
                agent_id=world.agent_id,
                version_label="facts-v1",
                created_by="test",
                bundle_ref="private-credential-sentinel",
                bundle_sha256="private-image-sentinel",
            )
            session.add(version)
            await session.flush()
            session.add(
                Deployment(
                    agent_id=world.agent_id,
                    version_id=version.id,
                    environment=Environment.prod,
                    commit_sha="private-host-sentinel",
                )
            )
            if binding_state == "unbound":
                await session.execute(delete(AgentChannel))
            if binding_state == "ambiguous":
                second = uuid.uuid4()
                session.add(Agent(id=second, name="acme-second"))
                await session.flush()
                # Distinct configured routes can collide after a tracker-label change.
                session.add(
                    AgentChannel(agent_id=second, kind="github", address=REPO, trigger_label=LABEL)
                )
            await session.commit()

    asyncio.run(configure())
    monkeypatch.setenv("LANGFUSE_HOST", "https://private-host-sentinel.example.com")
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", "private-image-sentinel")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY_FILE", "private-credential-sentinel")
    get_settings.cache_clear()
    world.settings = get_settings()
    if binding_state == "bound":
        world.add()
        world.cycle()
    app = create_app()
    app.state.sessionmaker = world.sessions
    app.state.valkey = redis.Redis.from_url(world.settings.valkey_dsn())
    app.state.http_client = httpx.AsyncClient(transport=httpx.MockTransport(world.handle))

    async def read_api() -> None:
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        )
        try:
            params = {
                "repository": REPO,
                "trigger_label": "other" if binding_state == "label_not_served" else LABEL,
                "executables": "uv,g++",
                "services": "postgres",
            }
            assert (await client.get("/factory/facts", params=params)).status_code == 401
            response = await client.get(
                "/factory/facts", params=params, headers={"X-API-Key": get_settings().api_key}
            )
            assert response.status_code == 200, response.text
            answer = response.json()
            assert answer["binding"] == {
                "state": binding_state,
                "reason": None if binding_state == "bound" else binding_state,
            }
            assert answer["request"] is answer["label_route"] is None
            assert answer["admission"] == "not_requested"
            assert answer["deployment_readiness"] == "unvalidated"
            assert len(answer["executables"]) == 2
            for sentinel in (
                "private-credential-sentinel",
                "private-host-sentinel",
                "private-image-sentinel",
                "12345.67",
            ):
                assert sentinel not in response.text + caplog.text + json.dumps(world.answers())
        finally:
            await client.aclose()
            await app.state.http_client.aclose()
            await app.state.valkey.aclose()

    asyncio.run(read_api())


def test_disabled_factory_is_silent(facts_world: Any) -> None:
    world = facts_world
    world.settings = world.settings.model_copy(update={"factory_enabled": False})
    world.add()
    world.cycle()
    assert world.answers() == world.observation_reads == []


def test_malformed_cached_observation_is_unknown_without_disclosing_it(
    facts_world: Any, caplog: Any
) -> None:
    from curie_api.models import FactoryFactObservation
    from forge_fakes.github import REPO_ID
    from test_factory_facts_intake import facts

    world = facts_world

    async def plant() -> None:
        now = datetime.now(UTC)
        async with world.sessions() as session:
            session.add(
                FactoryFactObservation(
                    kind="tool",
                    agent_id=world.agent_id,
                    scope=str(REPO_ID),
                    item="uv",
                    subject_digest="sha256:" + "b" * 64,
                    state="present",
                    reason="private-credential-sentinel",
                    observed_at=now,
                    expires_at=now + timedelta(seconds=120),
                )
            )
            await session.commit()

    asyncio.run(plant())
    world.add()
    world.cycle()
    answer = facts(world)
    assert answer.executables[0].state == "unknown"
    assert answer.executables[0].reason == "unreadable"
    assert "private-credential-sentinel" not in json.dumps(world.answers()) + caplog.text


@pytest.mark.parametrize("failure", ["crash", "deadline", "unparseable"])
def test_operator_guard_denies_failure_with_a_fixed_error(
    facts_world: Any, monkeypatch: Any, failure: str, caplog: Any
) -> None:
    import httpx
    import redis.asyncio as redis
    from curie_api.main import create_app
    from curie_api.routers import factory_facts as route
    from forge_fakes.github import LABEL, REPO

    world = facts_world
    assert 0 < route.FACTS_DEADLINE_SECONDS < 60
    monkeypatch.setattr(route, "FACTS_DEADLINE_SECONDS", 0.25)

    async def broken(request: httpx.Request) -> httpx.Response:
        if failure == "crash":
            raise RuntimeError("private-operator-crash-sentinel")
        if failure == "deadline":
            await asyncio.Event().wait()
        return httpx.Response(200, content=b"{broken-json")

    app = create_app()
    app.state.sessionmaker = world.sessions
    app.state.http_client = httpx.AsyncClient(transport=httpx.MockTransport(broken))
    app.state.valkey = redis.Redis.from_url(world.settings.valkey_dsn())

    async def read() -> None:
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                response = await asyncio.wait_for(
                    client.get(
                        "/factory/facts",
                        params={"repository": REPO, "trigger_label": LABEL},
                        headers={"X-API-Key": world.settings.api_key},
                    ),
                    timeout=3,
                )
                assert response.status_code == 503
                assert response.json() == {"detail": {"code": "facts_unavailable"}}
                assert "private-operator-crash-sentinel" not in response.text + caplog.text
        finally:
            await app.state.http_client.aclose()
            await app.state.valkey.aclose()

    asyncio.run(read())
    assert world.observation_reads == []
