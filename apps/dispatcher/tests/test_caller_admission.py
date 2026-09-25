"""The dispatcher's half of "a bot may limit who can talk to it" (ADR 0175).

The dispatcher asks the platform API (`POST /channels/admission`) whether a
caller may start a turn, after its own filters and before it claims the event,
on mentions, direct messages and turn-starting button clicks. A refused caller
gets no placeholder and no reply. Answers are cached per route: 30 seconds
fresh by default, usable up to 5 minutes while the API cannot answer, and a
cold miss during an outage is refused.

Driven through real Bolt, the real dispatcher handlers and the real Valkey.
Only Slack's transport and Web API are faked, plus the platform API, which is
another service to the dispatcher and is stood in by a real loopback HTTP
server (`conftest.fake_admission_api`). The cache is exercised through the
production gate over that server with an injected clock, so no test sleeps.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import redis
from curie_dispatcher.admission import AdmissionClient, AdmissionGate, build_admission
from curie_dispatcher.approval_actions import APPROVE_ACTION_ID
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.handlers import process_action
from curie_dispatcher.queue import from_stream_fields
from curie_dispatcher.relevance import DropReason
from curie_telemetry import build_resource, configure_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import ValidationError

from .conftest import FakeAdmissionApi, fake_admission_api
from .test_dispatch import _drain, _events_api_request
from .test_inbound_relevance import (
    _block_action_body,
    _build_harness,
    _dm,
    _drop_reasons_logged,
    _interactive_request,
    _mention,
    _stream_entries,
)

LISTED = "U0EXAMPLE1"
STRANGER = "U123"  # the sender `_mention` stamps
DM_SENDER = "U9"  # the sender `_dm` stamps
CHANNEL = "C123"  # the channel `_mention` and `_block_action_body` stamp
DM_CHANNEL = "D1"  # the channel `_dm` stamps
TTL = 30.0
STALE = 300.0


class _Clock:
    """A settable monotonic clock, so cache expiry is driven without sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def refused_metrics() -> Iterator[tuple[MeterProvider, InMemoryMetricReader]]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-dispatcher",
            service_version="0.0.0-test",
            service_instance_id="acme-dispatcher-admission-test",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)
    try:
        yield provider, reader
    finally:
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


def _gate(api: FakeAdmissionApi, clock: _Clock) -> AdmissionGate:
    return AdmissionGate(
        AdmissionClient(api_base_url=api.url, api_key="curie-dev-key"),
        ttl_s=TTL,
        stale_s=STALE,
        clock=clock,
    )


def _deliver(
    config: DispatcherConfig, redis_client: redis.Redis, request: Any
) -> Any:
    harness = _build_harness(config, redis_client)
    harness.handler.handle(harness.sock, request)
    _drain(harness.app)
    assert harness.errors == []
    return harness


# --- the three turn-starting lanes, end to end through Bolt ---------------------


def test_a_listed_mention_is_admitted_and_the_question_names_the_route(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    admission_api.lists[(CHANNEL, None)] = {STRANGER}
    harness = _deliver(
        config,
        redis_client,
        _events_api_request("env-ok", "Ev-ok", _mention(text="<@U0BOT> hi")),
    )
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).author == STRANGER
    assert harness.web_client.chat_postMessage.call_count == 1  # type: ignore[attr-defined]
    # The platform key rides the question; the route is (slack, default, C123)
    # and the ids are exactly the event's sender.
    assert admission_api.requests == [
        {"kind": "slack", "address": CHANNEL, "callers": [STRANGER]}
    ]
    assert admission_api.headers == [config.api_key]


def test_an_unlisted_mention_gets_no_placeholder_no_turn_and_no_claim(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    admission_api: FakeAdmissionApi,
    refused_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    harness = _deliver(
        config,
        redis_client,
        _events_api_request(
            "env-no", "Ev-no", _mention(text="<@U0BOT> read me the private notes")
        ),
    )
    assert _stream_entries(redis_client, config) == []
    assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert redis_client.exists(config.dedupe_key("Ev-no")) == 0
    assert _drop_reasons_logged(harness.records) == [DropReason.CALLER_NOT_ALLOWED]
    logged = "\n".join(record.getMessage() for record in harness.records)
    assert "private notes" not in logged
    assert _refused_points(refused_metrics) == [
        (1, {"service.name": "curie-dispatcher", "reason": "caller_not_allowed"})
    ]


def test_the_direct_message_lane_asks_too(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    admission_api.lists[(DM_CHANNEL, None)] = {LISTED}
    refused = _deliver(config, redis_client, _events_api_request("env-dm", "Ev-dm", _dm()))
    assert _stream_entries(redis_client, config) == []
    assert refused.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert _drop_reasons_logged(refused.records) == [DropReason.CALLER_NOT_ALLOWED]
    assert admission_api.requests[-1]["callers"] == [DM_SENDER]

    admission_api.lists[(DM_CHANNEL, None)] = {LISTED, DM_SENDER}
    # A fresh app is a fresh cache, as a fresh dispatcher pod would be.
    _deliver(config, redis_client, _events_api_request("env-dm-2", "Ev-dm-2", _dm()))
    assert len(_stream_entries(redis_client, config)) == 1


def test_a_button_click_is_judged_on_the_clicking_user(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    click = _block_action_body(
        actions=[{"type": "button", "action_id": "status", "value": "status", "action_ts": "1.5"}],
        trigger_id="trig-click",
    )
    refused = _deliver(config, redis_client, _interactive_request("env-click", click))
    assert _stream_entries(redis_client, config) == []
    assert refused.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert redis_client.exists(config.dedupe_key("action-trig-click")) == 0
    assert _drop_reasons_logged(refused.records) == [DropReason.CALLER_NOT_ALLOWED]
    assert admission_api.requests[-1]["callers"] == [STRANGER]

    listed_click = {**click, "trigger_id": "trig-click-2", "user": {"id": LISTED}}
    _deliver(config, redis_client, _interactive_request("env-click-2", listed_click))
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).author == LISTED


def test_an_approval_click_never_starts_a_turn_and_is_not_asked(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    """ADR-0106 approval buttons resolve through the API and never mint a turn,
    so the caller list does not apply to them (ADR 0175, "not covered")."""

    body = _block_action_body(
        actions=[{"type": "button", "action_id": APPROVE_ACTION_ID, "value": "appr-1"}],
        trigger_id="trig-approve",
    )
    result = process_action(
        body=body,
        web_client=_build_harness(config, redis_client).web_client,
        redis_client=redis_client,
        config=config,
        slack_identity="default",
        admission=build_admission(config),
    )
    assert result is None
    assert admission_api.requests == []


def test_a_bot_sender_is_asked_with_its_bot_id_and_either_id_admits(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    admission_api.lists[(CHANNEL, None)] = {"B0EXAMPLE2"}
    event = _mention(text="<@U0BOT> disk alert", bot_id="B0EXAMPLE2")
    event["user"] = "U0EXAMPLEBOT"
    _deliver(config, redis_client, _events_api_request("env-bot", "Ev-bot", event))
    assert len(_stream_entries(redis_client, config)) == 1
    assert sorted(admission_api.requests[-1]["callers"]) == ["B0EXAMPLE2", "U0EXAMPLEBOT"]


def test_the_threaded_bot_allowlist_still_runs_first(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    admission_api: FakeAdmissionApi,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Decision 5: the deploy-time thread bot list is checked before the API is
    asked, and a bot it admits must ALSO be on the binding's list."""

    thread_event = _mention(text="<@U0BOT> revise", bot_id="B2", thread_ts="1700.0000")
    not_trusted = _deliver(
        config, redis_client, _events_api_request("env-t1", "Ev-t1", thread_event)
    )
    assert _drop_reasons_logged(not_trusted.records) == [DropReason.BOT_AUTHORED_THREAD_REPLY]
    assert admission_api.requests == []

    monkeypatch.setenv(
        "CURIE_SLACK_THREADED_BOT_ALLOWLIST",
        json.dumps([{"channel_id": CHANNEL, "bot_id": "B2"}]),
    )
    trusted = DispatcherConfig(**config.model_dump(exclude={"slack_threaded_bot_allowlist"}))
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    refused = _deliver(
        trusted, redis_client, _events_api_request("env-t2", "Ev-t2", thread_event)
    )
    assert _drop_reasons_logged(refused.records) == [DropReason.CALLER_NOT_ALLOWED]
    assert _stream_entries(redis_client, config) == []


def test_a_cold_miss_while_the_api_is_down_is_refused_even_with_no_list(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    admission_api: FakeAdmissionApi,
    refused_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    admission_api.down = True
    harness = _deliver(
        config, redis_client, _events_api_request("env-down", "Ev-down", _mention(text="hi"))
    )
    assert _stream_entries(redis_client, config) == []
    assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert redis_client.exists(config.dedupe_key("Ev-down")) == 0
    assert _drop_reasons_logged(harness.records) == [DropReason.ADMISSION_UNAVAILABLE]
    assert _refused_points(refused_metrics) == [
        (1, {"service.name": "curie-dispatcher", "reason": "admission_unavailable"})
    ]


# --- the cache ------------------------------------------------------------------


def test_an_open_route_is_asked_once_per_ttl_not_once_per_message(
    admission_api: FakeAdmissionApi,
) -> None:
    clock = _Clock()
    gate = _gate(admission_api, clock)
    for caller in ("U0EXAMPLE1", "U0EXAMPLE2", "U0EXAMPLE3"):
        assert gate.refusal(address=CHANNEL, adapter=None, callers=[caller]) is None
    assert len(admission_api.requests) == 1

    clock.now += TTL
    assert gate.refusal(address=CHANNEL, adapter=None, callers=["U0EXAMPLE4"]) is None
    assert len(admission_api.requests) == 2


def test_a_restricted_route_caches_each_caller_separately(
    admission_api: FakeAdmissionApi,
) -> None:
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is None
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is None
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )
    assert len(admission_api.requests) == 2


def test_a_list_change_applies_once_the_ttl_expires(admission_api: FakeAdmissionApi) -> None:
    """Decision 7: a change applies within 30 seconds, additions and removals
    alike, and never sooner than the cache allows."""

    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None

    admission_api.lists[(CHANNEL, None)] = {LISTED}
    clock.now += TTL - 1
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None
    clock.now += 1
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )

    admission_api.lists[(CHANNEL, None)] = {LISTED, STRANGER}
    clock.now += TTL
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None


def test_a_restricted_answer_retires_the_routes_open_entry(
    admission_api: FakeAdmissionApi,
) -> None:
    """Once one caller's answer shows the route has a list, an older open
    entry must not keep admitting other callers through the stale window."""

    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    clock.now += TTL
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is None
    # The route's open entry is expired but still inside the stale window, so
    # during an outage it would admit STRANGER, whom the list refuses. The
    # restricted answer above retired it, so STRANGER now has nothing usable
    # cached and is refused.
    admission_api.down = True
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.ADMISSION_UNAVAILABLE
    )


def test_stale_answers_count_while_the_api_is_down_until_five_minutes(
    admission_api: FakeAdmissionApi,
) -> None:
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is None
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )
    asked = len(admission_api.requests)

    admission_api.down = True
    clock.now += STALE - 1
    # Expired, so the API is asked; it cannot answer, so the stale answers
    # stand, the refusal as much as the admission.
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is None
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )
    assert len(admission_api.requests) == asked + 2

    clock.now += 1
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[LISTED]) is (
        DropReason.ADMISSION_UNAVAILABLE
    )


def test_a_stale_open_route_still_admits_during_an_outage(
    admission_api: FakeAdmissionApi,
) -> None:
    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None
    admission_api.down = True
    clock.now += TTL + 1
    assert gate.refusal(address=CHANNEL, adapter=None, callers=["U0EXAMPLE9"]) is None
    clock.now = 1000.0 + STALE
    assert gate.refusal(address=CHANNEL, adapter=None, callers=["U0EXAMPLE9"]) is (
        DropReason.ADMISSION_UNAVAILABLE
    )


def test_the_route_key_carries_the_slack_identity(admission_api: FakeAdmissionApi) -> None:
    """ADR-0168 decision 3: two identities on one channel are two routes, so an
    answer for one must never be served for the other."""

    admission_api.lists[(CHANNEL, "ops")] = {LISTED}
    clock = _Clock()
    gate = _gate(admission_api, clock)
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is None
    assert gate.refusal(address=CHANNEL, adapter="ops", callers=[STRANGER]) is (
        DropReason.CALLER_NOT_ALLOWED
    )
    assert admission_api.requests[-1] == {
        "kind": "slack",
        "address": CHANNEL,
        "callers": [STRANGER],
        "adapter": "ops",
    }


def test_the_cache_is_bounded(admission_api: FakeAdmissionApi) -> None:
    admission_api.lists[(CHANNEL, None)] = {LISTED}
    clock = _Clock()
    gate = AdmissionGate(
        AdmissionClient(api_base_url=admission_api.url, api_key="k"),
        ttl_s=TTL,
        stale_s=STALE,
        clock=clock,
        max_entries=2,
    )
    for caller in ("U0EXAMPLE1", "U0EXAMPLE2", "U0EXAMPLE3"):
        gate.refusal(address=CHANNEL, adapter=None, callers=[caller])
    asked = len(admission_api.requests)
    # The oldest entry was evicted, so asking about it again is a new call.
    gate.refusal(address=CHANNEL, adapter=None, callers=["U0EXAMPLE1"])
    assert len(admission_api.requests) == asked + 1


# --- the client -----------------------------------------------------------------


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(401, json={"detail": "missing or invalid API key"}),
        httpx.Response(422, json={"detail": []}),
        httpx.Response(200, text="not json"),
        httpx.Response(200, json={"allowed": "yes", "restricted": False}),
        httpx.Response(200, json={"restricted": True}),
    ],
    ids=["401", "422", "not-json", "not-bool", "missing-field"],
)
def test_any_unusable_answer_counts_as_the_api_being_unable_to_answer(
    response: httpx.Response,
) -> None:
    """A wrong key or a malformed body must fail closed like an outage, never
    be read as "allowed". The transport is httpx's own mock: this pins the
    client's parsing, not the API."""

    client = AdmissionClient(
        api_base_url="http://api.example.com",
        api_key="k",
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: response)),
    )
    assert client.ask(address=CHANNEL, adapter=None, callers=(STRANGER,)) is None
    gate = AdmissionGate(client, ttl_s=TTL, stale_s=STALE, clock=_Clock())
    assert gate.refusal(address=CHANNEL, adapter=None, callers=[STRANGER]) is (
        DropReason.ADMISSION_UNAVAILABLE
    )


def test_an_unreachable_api_is_unavailable_not_an_exception() -> None:
    with fake_admission_api() as api:
        url = api.url
    # The server is gone; the port refuses.
    client = AdmissionClient(api_base_url=url, api_key="k")
    assert client.ask(address=CHANNEL, adapter=None, callers=(STRANGER,)) is None


# --- configuration --------------------------------------------------------------


def test_the_cache_defaults_are_thirty_seconds_and_five_minutes(
    config: DispatcherConfig,
) -> None:
    assert config.admission_cache_ttl_s == 30.0
    assert config.admission_stale_s == 300.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"admission_cache_ttl_s": 0},
        {"admission_stale_s": float("inf")},
        {"admission_cache_ttl_s": 60, "admission_stale_s": 30},
    ],
)
def test_a_cache_setting_that_would_disable_failing_closed_refuses_boot(
    config: DispatcherConfig, overrides: dict[str, float]
) -> None:
    with pytest.raises(ValidationError):
        DispatcherConfig(**{**config.model_dump(), **overrides})
