"""``run.build`` wires the sibling limit only where a sibling can exist (ADR-0168 decision 6).

Drives the real ``run.build`` and reads what the kernel was handed, as
``test_attachment_wiring.py`` does, with the same two outside-world seams
stubbed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

import boto3
import pytest
from curie_worker import run
from curie_worker.config import WorkerConfig
from curie_worker.sibling_turns import SiblingTurnLimit, SlackSenderIdentities

_DECLARED = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": None,
        },
        {
            "name": "ops-bot",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
            "signing_secret_env": None,
        },
    ]
)


class _KernelSpy:
    instances: list[_KernelSpy] = []
    real: Any = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.inner = _KernelSpy.real(**kwargs)
        _KernelSpy.instances.append(self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(_KernelSpy, "real", run.Kernel)
    monkeypatch.setattr(run, "Kernel", _KernelSpy)
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(run, "_sandbox_client", lambda config, env, sub: object())
    monkeypatch.setattr(run, "_build_publication_loop", lambda *_a, **_k: None)

    def _build(env: Mapping[str, str], **config_overrides: Any) -> dict[str, Any]:
        _KernelSpy.instances.clear()

        async def _drive() -> dict[str, Any]:
            runtime = run.build(WorkerConfig(**config_overrides), env)
            try:
                assert len(_KernelSpy.instances) == 1
                return _KernelSpy.instances[0].kwargs
            finally:
                await runtime.runner.close()
                await runtime.sink.aclose()
                await runtime.eval_http.aclose()
                await runtime.async_redis.aclose()
                await runtime.pressure_async_redis.aclose()
                await runtime.eval_redis.aclose()
                await runtime.engine.dispose()

        return asyncio.run(_drive())

    return _build


def test_a_stock_worker_builds_no_sibling_limit(built: Any) -> None:
    assert built({})["sibling_limit"] is None


def test_one_mail_adapter_builds_no_sibling_limit(built: Any) -> None:
    assert built({}, adapter_credentials={"mail-adapter": "secret"})["sibling_limit"] is None


def test_two_slack_identities_wire_a_limit_over_both_tokens(built: Any) -> None:
    kwargs = built(
        {"CURIE_SLACK_BOT_TOKEN__1": "xoxb-ops-placeholder"},
        slack_bot_token="xoxb-default-placeholder",
        slack_identities=_DECLARED,
    )
    limit = kwargs["sibling_limit"]
    assert isinstance(limit, SiblingTurnLimit)
    assert isinstance(limit.slack, SlackSenderIdentities)
    assert limit.slack.identities == ("default", "ops-bot")
    assert limit.channel is None


def test_two_adapters_wire_the_binding_resolver_as_the_address_lookup(built: Any) -> None:
    kwargs = built({}, adapter_credentials={"mail-a": "sa", "mail-b": "sb"})
    limit = kwargs["sibling_limit"]
    assert isinstance(limit, SiblingTurnLimit)
    assert limit.channel is kwargs["binding"]
    assert limit.slack is None
