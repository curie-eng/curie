"""A bot may limit who can talk to it (ADR 0175, #3241).

Every layer of the per-binding caller list, driven against the real Postgres
and the real Valkey the compose stack provides; nothing of ours is mocked:

- migration 0059 and the model: the nullable column, its CHECK, and a Python
  None stored as SQL NULL;
- the entry checks (`schemas.validate_allowed_callers`) per binding kind;
- the one admission function (`admission.admit`);
- `PUT /agents/{agent_id}/channels/callers`, which sets the list without
  bumping the binding generation, so an adapter token survives the edit;
- `POST /channels/turns`, which refuses an unlisted author with 403 before any
  claim or queue write;
- `POST /channels/admission`, the dispatcher's platform-key-only question.

Every guard here is proven by a test that shows it refusing a disallowed
caller, not only admitting a listed one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from alembic import command
from alembic.config import Config
from curie_api.admission import AdmissionReason, admit
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import AgentChannel
from curie_api.schemas import MAX_ALLOWED_CALLERS, validate_allowed_callers
from curie_telemetry import build_resource, configure_meter_provider
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
INBOX = "assistant@example.test"
LISTED = "owner@example.test"
STRANGER = "stranger@example.test"
SLACK_CHANNEL = "C0EXAMPLE1"
LISTED_USER = "U0EXAMPLE1"
OTHER_USER = "U0EXAMPLE2"
BOT = "B0EXAMPLE1"
SECRET_TEXT = "the quarterly numbers are in the attached sheet"


# --- fixtures and helpers -------------------------------------------------------


@pytest.fixture
def channels_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """An app built after the per-test stream override, so turns stay isolated."""

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def refused_metrics(
    channels_client: TestClient,
) -> Iterator[tuple[MeterProvider, InMemoryMetricReader]]:
    """Capture metric points from the app for the duration of one test."""

    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-api",
            service_version="0.0.0-test",
            service_instance_id="acme-api-admission-test",
            deployment_environment="test",
        ),
    )
    original = channels_client.app.state.telemetry.meter_provider  # type: ignore[attr-defined]
    configure_meter_provider(provider)
    try:
        yield provider, reader
    finally:
        configure_meter_provider(original)
        provider.shutdown()


def _refused_points(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> list[tuple[int, dict[str, str]]]:
    provider, reader = metrics
    assert provider.force_flush(timeout_millis=5000)
    data = reader.get_metrics_data()
    if data is None:
        return []
    return [
        (int(point.value), dict(point.attributes))
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == "curie.turn.refused"
        for point in metric.data.data_points
    ]


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _email_agent(client: TestClient, headers: dict[str, str], name: str = "mail-bot") -> str:
    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {
                "kind": "email",
                "address": INBOX,
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _slack_agent(client: TestClient, headers: dict[str, str], name: str = "slack-bot") -> str:
    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": SLACK_CHANNEL}},
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _set_callers(
    client: TestClient,
    headers: dict[str, str],
    agent_id: str,
    *,
    kind: str,
    address: str,
    callers: Any,
) -> Any:
    return client.put(
        f"/agents/{agent_id}/channels/callers",
        params={"kind": kind, "address": address},
        json={"allowed_callers": callers},
        headers=headers,
    )


def _generation(agent_id: str) -> int:
    rows = _sql(
        "SELECT generation FROM curie.agent_channels WHERE agent_id = :aid",
        {"aid": agent_id},
    )
    return int(rows[0]["generation"])


def _mint(client: TestClient, headers: dict[str, str]) -> str:
    minted = client.post(
        "/channels/token",
        json={"kind": "email", "address": INBOX},
        headers=headers,
    )
    assert minted.status_code == 200, minted.text
    return str(minted.json()["token"])


def _turn(author: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "email",
        "address": INBOX,
        "delivery_id": f"msg-{uuid.uuid4().hex}",
        "conversation_id": f"thread-{uuid.uuid4().hex[:8]}",
        "author": author,
        "text": SECRET_TEXT,
        "reply_ref": f"ref-{uuid.uuid4().hex[:8]}",
    }
    body.update(overrides)
    return body


def _row(kind: str, allowed_callers: list[str] | None) -> AgentChannel:
    return AgentChannel(kind=kind, address="x", allowed_callers=allowed_callers)


# --- migration 0059 and the model -------------------------------------------------


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _has_column() -> bool:
    rows = _sql(
        "SELECT count(*) AS n FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'agent_channels' AND column_name = 'allowed_callers'"
    )
    return int(rows[0]["n"]) == 1


def test_0059_round_trip_adds_and_drops_the_nullable_column(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _has_column()
    try:
        command.downgrade(config, "0058")
        assert not _has_column()
    finally:
        command.upgrade(config, "head")
    assert _has_column()


def test_the_database_refuses_an_empty_list_and_a_json_null(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The CHECK is the out-of-band writer's gate: `[]` and JSON `null` are both
    values two operators could read two ways, so neither can be stored."""

    agent_id = _email_agent(client, auth_headers)
    for bad in ("'[]'::jsonb", "'null'::jsonb", "'\"owner@example.test\"'::jsonb"):
        with pytest.raises(IntegrityError, match="agent_channels_allowed_callers_ck"):
            _sql(
                f"UPDATE curie.agent_channels SET allowed_callers = {bad} WHERE agent_id = :aid",
                {"aid": agent_id},
            )
    rows = _sql(
        "SELECT allowed_callers IS NULL AS is_null FROM curie.agent_channels "
        "WHERE agent_id = :aid",
        {"aid": agent_id},
    )
    # A binding created through the ORM stores SQL NULL, not the JSON value
    # `null`: `none_as_null` on the column is what makes the insert pass the CHECK.
    assert rows == [{"is_null": True}]


# --- the entry checks -------------------------------------------------------------


def test_null_means_everyone_and_an_empty_list_is_refused() -> None:
    assert validate_allowed_callers("slack", None) is None
    with pytest.raises(ValueError, match="must not be empty"):
        validate_allowed_callers("slack", [])


@pytest.mark.parametrize("caller", ["U0EXAMPLE1", "W0EXAMPLE1", "B0EXAMPLE1"])
def test_slack_accepts_user_enterprise_and_bot_ids(caller: str) -> None:
    assert validate_allowed_callers("slack", [caller]) == [caller]


@pytest.mark.parametrize(
    "caller", ["@owner", "owner", "u0example1", "C0EXAMPLE1", "S0EXAMPLE1", "U0EX", ""]
)
def test_slack_refuses_anything_but_an_exact_user_or_bot_id(caller: str) -> None:
    with pytest.raises(ValueError, match="not a Slack user or bot id"):
        validate_allowed_callers("slack", [caller])


def test_email_stores_one_bare_lowercase_address() -> None:
    assert validate_allowed_callers("email", ["Owner@Example.TEST"]) == [LISTED]


@pytest.mark.parametrize(
    "caller",
    [
        "Owner <owner@example.test>",
        "example.test",
        "*@example.test",
        "@example.test",
        "owner@",
        "a@b@example.test",
        "owner@example.test, other@example.test",
    ],
)
def test_email_refuses_display_names_domains_and_lists(caller: str) -> None:
    # `*@example.test` is the wildcard spelling an operator reaches for; an
    # exact match would read it as a mailbox literally named `*`, so it is
    # refused rather than stored as something it does not mean.
    with pytest.raises(ValueError, match="not one bare email address"):
        validate_allowed_callers("email", [caller])


@pytest.mark.parametrize("caller", ["", "two words", "tab\there"])
def test_other_kinds_need_a_non_empty_id_with_no_whitespace(caller: str) -> None:
    with pytest.raises(ValueError, match="not a caller id"):
        validate_allowed_callers("discord", [caller])
    assert validate_allowed_callers("discord", ["123456789012345678"]) == ["123456789012345678"]


def test_duplicates_are_dropped_in_first_seen_order() -> None:
    assert validate_allowed_callers(
        "email", ["b@example.test", "A@example.test", "a@example.test", "b@example.test"]
    ) == ["b@example.test", "a@example.test"]


def test_at_most_one_hundred_distinct_entries() -> None:
    hundred = [f"U0EXAMPLE{i:03d}" for i in range(MAX_ALLOWED_CALLERS)]
    assert validate_allowed_callers("slack", hundred) == hundred
    with pytest.raises(ValueError, match="the limit is 100"):
        validate_allowed_callers("slack", [*hundred, "U0EXAMPLEXX"])
    # Duplicates do not count toward the limit.
    assert validate_allowed_callers("slack", [*hundred, hundred[0]]) == hundred


# --- the admission function -------------------------------------------------------


def test_admit_lets_everyone_in_with_no_binding_or_no_list() -> None:
    unbound = admit(None, [STRANGER])
    assert (unbound.allowed, unbound.restricted, unbound.reason) == (
        True,
        False,
        AdmissionReason.UNBOUND,
    )
    open_route = admit(_row("email", None), [STRANGER])
    assert (open_route.allowed, open_route.restricted, open_route.reason) == (
        True,
        False,
        AdmissionReason.OPEN,
    )


def test_admit_refuses_a_caller_not_on_the_list() -> None:
    decision = admit(_row("email", [LISTED]), [STRANGER])
    assert (decision.allowed, decision.restricted, decision.reason) == (
        False,
        True,
        AdmissionReason.CALLER_NOT_ALLOWED,
    )


def test_admit_lets_in_a_listed_caller_on_any_of_its_ids() -> None:
    row = _row("slack", [BOT])
    assert admit(row, [OTHER_USER, BOT]).allowed is True
    assert admit(row, [OTHER_USER]).allowed is False
    assert admit(_row("slack", [LISTED_USER]), [LISTED_USER]).reason is AdmissionReason.LISTED


def test_admit_compares_email_in_its_stored_lowercase_form() -> None:
    assert admit(_row("email", [LISTED]), ["Owner@Example.Test"]).allowed is True
    # Slack ids are compared as sent: case is part of the id.
    assert admit(_row("slack", [LISTED_USER]), [LISTED_USER.lower()]).allowed is False


def test_admit_ignores_blank_ids() -> None:
    assert admit(_row("discord", ["x"]), ["", ""]).allowed is False


# --- PUT /agents/{agent_id}/channels/callers ----------------------------------------


def test_setting_the_list_shows_it_on_read_and_null_clears_it(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _email_agent(client, auth_headers)
    set_resp = _set_callers(
        client,
        auth_headers,
        agent_id,
        kind="email",
        address=INBOX,
        callers=["Owner@Example.test", LISTED],
    )
    assert set_resp.status_code == 200, set_resp.text
    assert set_resp.json()["channels"][0]["allowed_callers"] == [LISTED]
    read = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert read["channels"][0]["allowed_callers"] == [LISTED]

    cleared = _set_callers(
        client, auth_headers, agent_id, kind="email", address=INBOX, callers=None
    )
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["channels"][0]["allowed_callers"] is None


def test_editing_the_list_leaves_the_generation_and_the_token_alone(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Decision 4: who may use a route is not the route, so a list edit must
    not revoke the adapter's `chn` token (#2379). Proven by the token still
    enqueuing after two edits, not only by the counter."""

    agent_id = _email_agent(channels_client, auth_headers)
    token = _mint(channels_client, auth_headers)
    before = _generation(agent_id)
    for callers in ([LISTED], None):
        resp = _set_callers(
            channels_client, auth_headers, agent_id, kind="email", address=INBOX, callers=callers
        )
        assert resp.status_code == 200, resp.text
    assert _generation(agent_id) == before
    posted = channels_client.post(
        "/channels/turns", json=_turn(STRANGER), headers={"X-API-Key": token}
    )
    assert posted.status_code == 200, posted.text
    assert valkey.xlen(runs_stream) == 1


def test_a_binding_move_still_bumps_the_generation(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The sibling write keeps its rotation semantics: only the list is exempt."""

    agent_id = _email_agent(client, auth_headers)
    before = _generation(agent_id)
    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": INBOX},
        json={"kind": "email", "address": INBOX},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    assert _generation(agent_id) == before + 1


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ({"allowed_callers": []}, "must not be empty"),
        ({"allowed_callers": ["Owner <owner@example.test>"]}, "not one bare email address"),
        ({}, "Field required"),
        ({"allowed_callers": None, "extra": 1}, "Extra inputs are not permitted"),
    ],
)
def test_a_bad_list_is_a_422_and_stores_nothing(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    body: dict[str, Any],
    fragment: str,
) -> None:
    agent_id = _email_agent(client, auth_headers)
    assert (
        _set_callers(
            client, auth_headers, agent_id, kind="email", address=INBOX, callers=[LISTED]
        ).status_code
        == 200
    )
    resp = client.put(
        f"/agents/{agent_id}/channels/callers",
        params={"kind": "email", "address": INBOX},
        json=body,
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    assert fragment in json.dumps(resp.json())
    stored = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert stored["channels"][0]["allowed_callers"] == [LISTED]


def test_the_list_is_checked_against_the_selected_bindings_kind(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _slack_agent(client, auth_headers)
    refused = _set_callers(
        client, auth_headers, agent_id, kind="slack", address=SLACK_CHANNEL, callers=[LISTED]
    )
    assert refused.status_code == 422, refused.text
    assert "not a Slack user or bot id" in refused.text
    accepted = _set_callers(
        client,
        auth_headers,
        agent_id,
        kind="slack",
        address=SLACK_CHANNEL,
        callers=[LISTED_USER, BOT],
    )
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["channels"][0]["allowed_callers"] == [LISTED_USER, BOT]


def test_an_unknown_binding_or_agent_is_404_and_the_key_is_required(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _email_agent(client, auth_headers)
    missing = _set_callers(
        client,
        auth_headers,
        agent_id,
        kind="email",
        address="other@example.test",
        callers=[LISTED],
    )
    assert missing.status_code == 404, missing.text
    no_agent = _set_callers(
        client, auth_headers, str(uuid.uuid4()), kind="email", address=INBOX, callers=[LISTED]
    )
    assert no_agent.status_code == 404, no_agent.text
    unauthenticated = _set_callers(
        client, {}, agent_id, kind="email", address=INBOX, callers=[LISTED]
    )
    assert unauthenticated.status_code == 401, unauthenticated.text


def test_moving_a_listed_binding_to_another_kind_is_refused(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _email_agent(client, auth_headers)
    assert (
        _set_callers(
            client, auth_headers, agent_id, kind="email", address=INBOX, callers=[LISTED]
        ).status_code
        == 200
    )
    across = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": INBOX},
        json={"kind": "slack", "address": SLACK_CHANNEL},
        headers=auth_headers,
    )
    assert across.status_code == 409, across.text
    assert "caller list" in across.text
    within = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": INBOX},
        json={"kind": "email", "address": "desk@example.test"},
        headers=auth_headers,
    )
    assert within.status_code == 200, within.text
    assert within.json()["channels"][0]["allowed_callers"] == [LISTED]


# --- POST /channels/turns -----------------------------------------------------------


def test_an_unlisted_author_is_refused_before_any_claim_or_queue_write(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    refused_metrics: tuple[MeterProvider, InMemoryMetricReader],
    caplog: pytest.LogCaptureFixture,
) -> None:
    agent_id = _email_agent(channels_client, auth_headers)
    token = _mint(channels_client, auth_headers)
    assert (
        _set_callers(
            channels_client, auth_headers, agent_id, kind="email", address=INBOX, callers=[LISTED]
        ).status_code
        == 200
    )
    binding_id = _sql(
        "SELECT id FROM curie.agent_channels WHERE agent_id = :aid", {"aid": agent_id}
    )[0]["id"]

    body = _turn(STRANGER)
    with caplog.at_level(logging.INFO, logger="curie_api.routers.channels"):
        refused = channels_client.post(
            "/channels/turns", json=body, headers={"X-API-Key": token}
        )
    assert refused.status_code == 403, refused.text
    assert STRANGER not in refused.text
    # Nothing was queued and nothing was claimed: a later listed retry of the
    # same delivery is not blocked by a receipt the refusal left behind.
    assert valkey.xlen(runs_stream) == 0
    assert list(valkey.scan_iter(match=f"curie:channel:delivery:{binding_id}:*")) == []
    # The log names the binding and the reason, never the caller or the text.
    refusals = [r.getMessage() for r in caplog.records if "caller refused" in r.getMessage()]
    assert len(refusals) == 1
    assert str(binding_id) in refusals[0] and "reason=caller_not_allowed" in refusals[0]
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert SECRET_TEXT not in logged and STRANGER not in logged
    assert _refused_points(refused_metrics) == [
        (1, {"service.name": "curie-api", "reason": "caller_not_allowed"})
    ]

    # The same delivery, now written by a listed author, is admitted: the
    # refusal claimed nothing.
    admitted = channels_client.post(
        "/channels/turns",
        json={**body, "author": "OWNER@example.test"},
        headers={"X-API-Key": token},
    )
    assert admitted.status_code == 200, admitted.text
    assert admitted.json()["duplicate"] is False
    assert valkey.xlen(runs_stream) == 1


def test_the_platform_key_is_not_exempt_from_the_list(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    agent_id = _email_agent(channels_client, auth_headers)
    assert (
        _set_callers(
            channels_client, auth_headers, agent_id, kind="email", address=INBOX, callers=[LISTED]
        ).status_code
        == 200
    )
    refused = channels_client.post("/channels/turns", json=_turn(STRANGER), headers=auth_headers)
    assert refused.status_code == 403, refused.text
    assert valkey.xlen(runs_stream) == 0


def test_a_binding_with_no_list_admits_everyone_as_before(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    _email_agent(channels_client, auth_headers)
    token = _mint(channels_client, auth_headers)
    posted = channels_client.post(
        "/channels/turns", json=_turn(STRANGER), headers={"X-API-Key": token}
    )
    assert posted.status_code == 200, posted.text
    assert valkey.xlen(runs_stream) == 1


def _post(client: TestClient, headers: dict[str, str]) -> Any:
    return client.post("/channels/turns", json=_turn(STRANGER), headers=headers)


def test_a_list_change_applies_on_the_next_message(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Decision 7: the channel port reads the list per request, so a removal
    applies to the very next delivery."""

    agent_id = _email_agent(channels_client, auth_headers)
    token = _mint(channels_client, auth_headers)
    headers = {"X-API-Key": token}
    assert _post(channels_client, headers).status_code == 200
    _set_callers(
        channels_client, auth_headers, agent_id, kind="email", address=INBOX, callers=[LISTED]
    )
    assert _post(channels_client, headers).status_code == 403
    _set_callers(
        channels_client,
        auth_headers,
        agent_id,
        kind="email",
        address=INBOX,
        callers=[LISTED, STRANGER],
    )
    assert _post(channels_client, headers).status_code == 200
    assert valkey.xlen(runs_stream) == 2


# --- POST /channels/admission ---------------------------------------------------------


def _ask(
    client: TestClient,
    headers: dict[str, str],
    callers: list[str],
    *,
    address: str = SLACK_CHANNEL,
    **extra: Any,
) -> Any:
    return client.post(
        "/channels/admission",
        json={"kind": "slack", "address": address, "callers": callers, **extra},
        headers=headers,
    )


def test_admission_answers_for_the_slack_dispatcher(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    agent_id = _slack_agent(client, auth_headers)
    open_answer = _ask(client, auth_headers, [OTHER_USER])
    assert open_answer.status_code == 200, open_answer.text
    assert open_answer.json() == {"allowed": True, "restricted": False}

    assert (
        _set_callers(
            client,
            auth_headers,
            agent_id,
            kind="slack",
            address=SLACK_CHANNEL,
            callers=[LISTED_USER, BOT],
        ).status_code
        == 200
    )
    with caplog.at_level(logging.INFO, logger="curie_api.routers.channels"):
        refused = _ask(client, auth_headers, [OTHER_USER])
    assert refused.json() == {"allowed": False, "restricted": True}
    assert any("reason=caller_not_allowed" in r.getMessage() for r in caplog.records)
    assert _ask(client, auth_headers, [LISTED_USER]).json() == {
        "allowed": True,
        "restricted": True,
    }
    # A bot-sent message is asked with the sender AND the bot id; either admits.
    assert _ask(client, auth_headers, [OTHER_USER, BOT]).json()["allowed"] is True
    # No ids at all can never match a list.
    assert _ask(client, auth_headers, []).json()["allowed"] is False


def test_admission_on_an_unbound_route_is_open(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    answer = _ask(client, auth_headers, [OTHER_USER], address="C0EXAMPLE9")
    assert answer.status_code == 200, answer.text
    assert answer.json() == {"allowed": True, "restricted": False}


def test_admission_takes_the_platform_key_and_nothing_else(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A `chn` token scoped to one binding must not be able to ask about
    another binding's list, so only the platform key is accepted."""

    _email_agent(client, auth_headers)
    token = _mint(client, auth_headers)
    assert _ask(client, {}, [OTHER_USER]).status_code == 401
    assert _ask(client, {"X-API-Key": token}, [OTHER_USER]).status_code == 401
    assert _ask(client, {"X-API-Key": "not-the-key"}, [OTHER_USER]).status_code == 401


def test_admission_validates_the_route_and_bounds_the_ids(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    assert _ask(client, auth_headers, [OTHER_USER], address="#general").status_code == 422
    assert _ask(client, auth_headers, [OTHER_USER] * 11).status_code == 422
    assert _ask(client, auth_headers, ["U" * 257]).status_code == 422
    assert _ask(client, auth_headers, [OTHER_USER], unexpected=True).status_code == 422
