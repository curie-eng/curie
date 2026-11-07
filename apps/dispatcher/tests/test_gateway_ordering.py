"""The stock Slack admission, claim, placeholder and append contract."""

from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher.config import DispatcherConfig

from .conftest import FakeAdmissionApi
from .test_dispatch import _drain, _events_api_request
from .test_inbound_relevance import _build_harness, _mention, _stream_entries


def test_gateway_slack_admits_then_claims_then_posts_then_enqueues(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    admission_api: FakeAdmissionApi,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _build_harness(config, redis_client)
    steps: list[str] = []
    claim_key = config.dedupe_key("Ev-gateway-order")
    real_set = redis_client.set
    real_xadd = redis_client.xadd

    def claim(name: Any, *args: Any, **kwargs: Any) -> Any:
        if name == claim_key:
            assert len(admission_api.requests) == 1
            assert steps == []
            steps.extend(["admission", "claim"])
        return real_set(name, *args, **kwargs)

    def placeholder(**kwargs: Any) -> dict[str, str]:
        assert steps == ["admission", "claim"]
        assert redis_client.exists(claim_key) == 1
        assert _stream_entries(redis_client, config) == []
        steps.append("placeholder")
        return {"ts": "1700.000200"}

    def append(name: Any, *args: Any, **kwargs: Any) -> Any:
        if name == config.stream:
            assert steps == ["admission", "claim", "placeholder"]
            steps.append("enqueue")
        return real_xadd(name, *args, **kwargs)

    monkeypatch.setattr(redis_client, "set", claim)
    monkeypatch.setattr(redis_client, "xadd", append)
    harness.web_client.chat_postMessage = MagicMock(side_effect=placeholder)
    event = _mention(text="<@U0BOT> hello")
    harness.handler.handle(
        harness.sock, _events_api_request("env-gateway-order", "Ev-gateway-order", event)
    )
    _drain(harness.app)
    assert harness.errors == []
    assert steps == ["admission", "claim", "placeholder", "enqueue"]
    assert len(_stream_entries(redis_client, config)) == 1
