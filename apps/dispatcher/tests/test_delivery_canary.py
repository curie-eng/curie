"""Delivery-canary channels through real Bolt and Valkey (#4960).

A mention in a channel listed in ``CURIE_SLACK_DELIVERY_CANARY_CHANNELS``
proves Slack still delivers to that identity and nothing more: it is counted on
``curie.slack.delivery_canary`` and refused before admission, dedupe,
placeholder and enqueue. Only Slack's transport and Web API are faked.
"""

import json
import logging
from collections.abc import Callable, Iterator

import httpx
import pytest
import redis
from curie_dispatcher import identity as identity_module
from curie_dispatcher.app import _stop_session_runner
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identity import IdentityResolveClient
from curie_dispatcher.relevance import DROP_RATIONALES, DropReason
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import ValidationError
from pydantic_settings.exceptions import SettingsError

from .conftest import FakeAdmissionApi
from .test_dispatch import _drain, _events_api_request
from .test_inbound_relevance import (
    _build_harness,
    _dm,
    _drop_reasons_logged,
    _mention,
    _stream_entries,
)
from .test_test_actions import DRIVER
from .test_test_actions import _configured as _driver_configured

_CANARY = "curie.slack.delivery_canary"
_CANARY_CHANNEL = "C0EXAMPLE1"

Counts = Callable[[], dict[str, float]]


@pytest.fixture
def canaries(monkeypatch: pytest.MonkeyPatch) -> Iterator[Counts]:
    """Read the canary counter as ``{slack_identity: value}``."""
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    telemetry_metrics.reset_bounded_labels()
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-dispatcher",
            service_version="0.0.0-test",
            service_instance_id="acme-dispatcher-canary-test",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)

    def read() -> dict[str, float]:
        assert provider.force_flush(timeout_millis=5000)
        data = reader.get_metrics_data()
        points: dict[str, float] = {}
        if data is None:
            return points
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name != _CANARY:
                        continue
                    for point in metric.data.data_points:
                        attributes = dict(point.attributes or {})
                        assert set(attributes) == {"service.name", "slack_identity"}
                        assert attributes["service.name"] == "curie-dispatcher"
                        points[str(attributes["slack_identity"])] = float(point.value)
        return points

    yield read
    provider.shutdown()
    telemetry_metrics.reset_bounded_labels()


def _configured(
    config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch, channels: list[str]
) -> DispatcherConfig:
    monkeypatch.setenv("CURIE_SLACK_DELIVERY_CANARY_CHANNELS", json.dumps(channels))
    return DispatcherConfig(**config.model_dump(exclude={"slack_delivery_canary_channels"}))


@pytest.mark.parametrize("bot_id", [None, "B9POSTER"], ids=["person", "bot"])
def test_a_canary_mention_is_counted_and_never_becomes_a_turn(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
    bot_id: str | None,
) -> None:
    """Acked, counted, logged at DEBUG; no API call, claim, placeholder or entry."""
    config = _configured(config, monkeypatch, [_CANARY_CHANNEL])
    harness = _build_harness(config, redis_client)
    event = _mention(text="<@U0BOT> delivery canary", bot_id=bot_id)
    event["channel"] = _CANARY_CHANNEL
    harness.handler.handle(harness.sock, _events_api_request("env-can", "Ev-can", event))
    _drain(harness.app)

    assert harness.errors == []
    assert harness.sock.acked_envelope_ids == ["env-can"]
    assert _stream_entries(redis_client, config) == []
    assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert redis_client.exists(config.dedupe_key("Ev-can")) == 0
    assert admission_api.requests == []
    assert _drop_reasons_logged(harness.records) == [DropReason.DELIVERY_CANARY]
    (record,) = harness.records
    assert record.levelno == logging.DEBUG
    assert canaries() == {"default": 1}


def test_a_threaded_canary_mention_is_still_a_canary(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The channel decides, before the loop guard: a canary never reaches relevance."""
    config = _configured(config, monkeypatch, [_CANARY_CHANNEL])
    harness = _build_harness(config, redis_client)
    event = _mention(text="<@U0BOT> canary", bot_id="B9POSTER", thread_ts="1700.0000")
    event["channel"] = _CANARY_CHANNEL
    harness.handler.handle(harness.sock, _events_api_request("env-t", "Ev-t", event))
    _drain(harness.app)

    assert _drop_reasons_logged(harness.records) == [DropReason.DELIVERY_CANARY]
    assert canaries() == {"default": 1}


@pytest.mark.parametrize("canary", [True, False], ids=["canary", "ordinary-mention"])
def test_a_canary_never_submits_a_principal_lookup(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
    canary: bool,
) -> None:
    """Global principal middleware must skip canaries while retaining ordinary lookups."""
    config = _configured(config, monkeypatch, [_CANARY_CHANNEL])
    requests: list[httpx.Request] = []

    def api(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"status": "unresolved", "reason": "unknown_user"})

    with httpx.Client(transport=httpx.MockTransport(api)) as http:
        lookup = IdentityResolveClient("http://api.example.com", "example-key", client=http)
        monkeypatch.setattr(
            "curie_dispatcher.handlers.build_identity_client", lambda _config: lookup
        )
        harness = _build_harness(config, redis_client)
        event = _mention(text="<@U0BOT> delivery canary")
        event["channel"] = _CANARY_CHANNEL if canary else "C0EXAMPLE2"
        try:
            harness.handler.handle(
                harness.sock, _events_api_request("env-lookup", "Ev-lookup", event)
            )
            _drain(harness.app)
        finally:
            # shutdown_identity_lookups cancels queued work without waiting.
            # Drain the production pool first so an empty request list proves
            # no lookup was submitted, rather than winning a scheduling race.
            identity_module._shared_pool().shutdown(wait=True)
            identity_module.shutdown_identity_lookups()
            harness.handler.close()
            _stop_session_runner(harness.handler.client)

    assert harness.errors == []
    assert harness.sock.acked_envelope_ids == ["env-lookup"]
    if canary:
        assert requests == []
        assert admission_api.requests == []
        assert _stream_entries(redis_client, config) == []
        assert redis_client.exists(config.dedupe_key("Ev-lookup")) == 0
        assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
        assert _drop_reasons_logged(harness.records) == [DropReason.DELIVERY_CANARY]
        assert canaries() == {"default": 1}
    else:
        assert [(request.method, request.url.path) for request in requests] == [
            ("POST", "/identity/resolve")
        ]
        assert len(_stream_entries(redis_client, config)) == 1
        assert canaries() == {}


def test_a_driver_approval_canary_bypasses_admission_even_during_an_outage(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pre-ack ownership middleware must not intercept a canary approval reply."""
    config = _driver_configured(_configured(config, monkeypatch, [_CANARY_CHANNEL]))
    admission_api.down = True
    harness = _build_harness(config, redis_client)
    event = _mention(
        text="<@U0BOT> [test action] approve 00000000-0000-4000-8000-000000000135",
        bot_id=DRIVER.bot_id,
        thread_ts="1600.0001",
    )
    event["channel"] = _CANARY_CHANNEL
    try:
        harness.handler.handle(
            harness.sock, _events_api_request("env-driver-canary", "Ev-driver-canary", event)
        )
        _drain(harness.app)
    finally:
        harness.handler.close()
        _stop_session_runner(harness.handler.client)

    assert harness.errors == []
    assert harness.sock.acked_envelope_ids == ["env-driver-canary"]
    assert admission_api.requests == []
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("Ev-driver-canary")) == 0
    assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert harness.web_client.conversations_replies.call_count == 0  # type: ignore[attr-defined]
    assert _drop_reasons_logged(harness.records) == [DropReason.DELIVERY_CANARY]
    assert canaries() == {"default": 1}


def test_a_mention_elsewhere_is_an_ordinary_turn(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _configured(config, monkeypatch, [_CANARY_CHANNEL])
    harness = _build_harness(config, redis_client)
    event = _mention(text="<@U0BOT> what changed?")
    harness.handler.handle(harness.sock, _events_api_request("env-real", "Ev-real", event))
    _drain(harness.app)

    assert harness.errors == []
    assert len(_stream_entries(redis_client, config)) == 1
    assert _drop_reasons_logged(harness.records) == []
    assert canaries() == {}


def test_a_direct_message_is_never_a_canary(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    canaries: Counts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the mention lane carries canaries; a DM still starts a turn."""
    config = _configured(config, monkeypatch, [_CANARY_CHANNEL])
    harness = _build_harness(config, redis_client)
    event = _dm(text="hello")
    harness.handler.handle(harness.sock, _events_api_request("env-dm", "Ev-dm", event))
    _drain(harness.app)

    assert len(_stream_entries(redis_client, config)) == 1
    assert canaries() == {}


def test_no_canary_channels_by_default() -> None:
    config = DispatcherConfig(approval_chat_attester_secret="dispatcher-attester-test-secret")
    assert config.slack_delivery_canary_channels == ()


def test_canary_channels_parse_from_a_json_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_SLACK_DELIVERY_CANARY_CHANNELS", '["C0EXAMPLE1", "G0EXAMPLE2"]')
    config = DispatcherConfig(approval_chat_attester_secret="dispatcher-attester-test-secret")
    assert config.slack_delivery_canary_channels == ("C0EXAMPLE1", "G0EXAMPLE2")


@pytest.mark.parametrize(
    "raw",
    ["null", '"C0EXAMPLE1"', '["D0DIRECT"]', '["c0lower"]', '[""]', "not json"],
    ids=["null", "bare-string", "dm-id", "lowercase", "blank", "not-json"],
)
def test_malformed_canary_channels_refuse_boot(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """A typo must not silently disable the canary or swallow a DM."""
    monkeypatch.setenv("CURIE_SLACK_DELIVERY_CANARY_CHANNELS", raw)
    with pytest.raises((ValidationError, SettingsError, ValueError)):
        DispatcherConfig(approval_chat_attester_secret="dispatcher-attester-test-secret")


def test_the_canary_drop_has_a_rationale() -> None:
    assert DropReason.DELIVERY_CANARY.value == "delivery_canary"
    assert DROP_RATIONALES[DropReason.DELIVERY_CANARY]
