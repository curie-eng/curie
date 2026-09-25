"""Slack preflight per identity (ADR-0168 decision 2).

Discovery runs once. Each identity then probes only the destinations it owns,
with its own token. One that fails is named and skipped while the others
connect, and several probe at once against the one shared deadline. The API is
faked at the httpx seam and Slack at its provider client seam, as in
test_preflight.py, whose helpers this reuses.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx
import pytest
from curie_dispatcher.identities import SlackBotIds, SlackIdentityCredentials
from curie_dispatcher.preflight import (
    PreflightedIdentity,
    SlackChannelPreflightError,
    check_slack_channel_capabilities,
)
from slack_sdk.errors import SlackApiError

from .test_preflight import (
    CHANNEL_A,
    CHANNEL_B,
    CHANNEL_C,
    MISSING_SCOPE_MESSAGE,
    SLACK_TIMEOUT_MESSAGE,
    _agent,
    _client,
    _config,
    _RecordingSlackClient,
)

CHANNEL_D = "C0EXAMPLE4"
CHANNEL_E = "C0EXAMPLE5"
DEFAULT = SlackIdentityCredentials(
    name="default", app_token="xapp-default", bot_token="xoxb-default"
)
OPS = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")
EVERY_IDENTITY_FAILED = (
    "Slack channel capability preflight failed for every declared Slack "
    "identity; each identity's reason is logged above."
)


class _IdentityClient(_RecordingSlackClient):
    """A provider fake that also answers auth.test, and can be slow."""

    def __init__(
        self,
        *,
        auth_response: object | None = None,
        auth_side_effect: Exception | None = None,
        info_delay_s: float = 0.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.auth_calls = 0
        self.auth_response = auth_response
        self.auth_side_effect = auth_side_effect
        self.info_delay_s = info_delay_s

    def auth_test(self) -> object:
        self.auth_calls += 1
        if self.auth_side_effect is not None:
            raise self.auth_side_effect
        return self.auth_response

    def conversations_info(self, *, channel: str) -> Any:
        if self.info_delay_s:
            time.sleep(self.info_delay_s)
        return super().conversations_info(channel=channel)


def _missing_channels_read() -> SlackApiError:
    return SlackApiError(
        "missing-scope-sentinel",
        {"ok": False, "error": "missing_scope", "needed": "channels:read"},
    )


def _two_agents_api() -> httpx.Client:
    agents = [
        _agent(
            channels=[
                {"kind": "slack", "address": CHANNEL_A, "adapter": "default"},
                # A pre-ADR custom-transport binding stores a credential slug,
                # which no identity is named; `default` probes it, as today.
                {"kind": "slack", "address": CHANNEL_E, "adapter": "hook-proof"},
            ],
            approval_routes={"security": {"resolution": {"kind": "slack", "address": CHANNEL_C}}},
        ),
        _agent(
            channels=[{"kind": "slack", "address": CHANNEL_B, "adapter": "ops-bot"}],
            approval_routes={"operations": {"resolution": {"kind": "slack", "address": CHANNEL_D}}},
        ),
    ]
    return _client(lambda _request: httpx.Response(200, json=agents))


def test_each_identity_probes_only_the_destinations_it_owns() -> None:
    default_client = _IdentityClient(auth_response={"ok": True, "bot_id": "B0DEFAULT"})
    ops_client = _IdentityClient(auth_response={"ok": True, "bot_id": "B0OPS"})

    admitted = check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-owned"),
        identities=(DEFAULT, OPS),
        web_clients={"default": default_client, "ops-bot": ops_client},
        api_client=_two_agents_api(),
    )

    assert [identity.name for identity in admitted] == ["default", "ops-bot"]
    assert sorted(default_client.channels) == sorted([CHANNEL_A, CHANNEL_C, CHANNEL_E])
    assert sorted(ops_client.channels) == sorted([CHANNEL_B, CHANNEL_D])


def test_an_identity_missing_channels_read_is_skipped_and_the_other_connects(
    caplog: pytest.LogCaptureFixture,
) -> None:
    default_client = _IdentityClient(auth_response={"ok": True})
    ops_client = _IdentityClient(
        auth_response={"ok": True}, list_side_effect=_missing_channels_read()
    )
    logger = logging.getLogger("test-preflight-identities-skip")

    with caplog.at_level(logging.INFO, logger=logger.name):
        admitted = check_slack_channel_capabilities(
            _config(),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={"default": default_client, "ops-bot": ops_client},
            api_client=_two_agents_api(),
        )

    assert [identity.credentials for identity in admitted] == [DEFAULT]
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert errors == [
        f"Slack identity ops-bot did not pass preflight and will not connect: "
        f"{MISSING_SCOPE_MESSAGE}"
    ]
    summaries = [
        r.getMessage() for r in caplog.records if "preflight public-channel" in r.getMessage()
    ]
    assert summaries == [
        "Slack identity default: Slack channel capability preflight public-channel "
        "capability verified; attachment download capability verified; checked 3 "
        "configured destinations; unverified 0"
    ]
    emitted = " ".join(r.getMessage() for r in caplog.records)
    for private in ("xoxb-default", "xoxb-ops", "missing-scope-sentinel", CHANNEL_B):
        assert private not in emitted


def test_when_every_identity_fails_the_boot_is_refused(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test-preflight-identities-all-fail")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(),
                logger=logger,
                identities=(DEFAULT, OPS),
                web_clients={
                    "default": _IdentityClient(list_side_effect=_missing_channels_read()),
                    "ops-bot": _IdentityClient(list_side_effect=_missing_channels_read()),
                },
                api_client=_two_agents_api(),
            )

    assert str(excinfo.value) == EVERY_IDENTITY_FAILED
    assert sorted(r.getMessage() for r in caplog.records) == sorted(
        f"Slack identity {name} did not pass preflight and will not connect: "
        f"{MISSING_SCOPE_MESSAGE}"
        for name in ("default", "ops-bot")
    )


def test_a_slow_identity_does_not_spend_another_identitys_budget(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Real clock. Run in sequence, `default`'s slow first destination would
    exhaust the shared deadline before `ops-bot` probed anything."""

    default_client = _IdentityClient(auth_response={"ok": True}, info_delay_s=0.8)
    ops_client = _IdentityClient(auth_response={"ok": True})
    logger = logging.getLogger("test-preflight-identities-slow")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        admitted = check_slack_channel_capabilities(
            _config(api_preflight_timeout_s=0.5),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={"default": default_client, "ops-bot": ops_client},
            api_client=_two_agents_api(),
        )

    assert [identity.name for identity in admitted] == ["ops-bot"]
    assert sorted(ops_client.channels) == sorted([CHANNEL_B, CHANNEL_D])
    assert [r.getMessage() for r in caplog.records] == [
        f"Slack identity default did not pass preflight and will not connect: "
        f"{SLACK_TIMEOUT_MESSAGE}"
    ]


def test_bot_ids_are_collected_per_identity_when_several_are_declared(
    caplog: pytest.LogCaptureFixture,
) -> None:
    default_client = _IdentityClient(
        auth_response={"ok": True, "team_id": "T1", "user_id": "U0DEFAULT", "bot_id": "B0DEFAULT"}
    )
    ops_client = _IdentityClient(auth_side_effect=TimeoutError("slow"))
    logger = logging.getLogger("test-preflight-identities-bot-ids")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        admitted = check_slack_channel_capabilities(
            _config(),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={"default": default_client, "ops-bot": ops_client},
            api_client=_two_agents_api(),
        )

    assert admitted == (
        PreflightedIdentity(
            DEFAULT,
            SlackBotIds(team_id="T1", app_id=None, bot_id="B0DEFAULT", bot_user_id="U0DEFAULT"),
        ),
        PreflightedIdentity(OPS, None),
    )
    assert default_client.auth_calls == 1 and ops_client.auth_calls == 1
    assert any(
        "Slack identity ops-bot" in r.getMessage() and "auth.test" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )


def test_a_single_identity_makes_no_auth_test_call_and_returns_default() -> None:
    client = _IdentityClient(auth_response={"ok": True, "bot_id": "B0DEFAULT"})

    admitted = check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-single"),
        web_client=client,
        api_client=_client(lambda _request: httpx.Response(200, json=[])),
    )

    assert [identity.name for identity in admitted] == ["default"]
    assert admitted[0].bot_ids is None
    assert client.auth_calls == 0


def test_production_clients_carry_each_identitys_own_bot_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_dispatcher import preflight

    probed: list[tuple[str, str]] = []

    class ProductionClient(_IdentityClient):
        def __init__(self, token: str) -> None:
            super().__init__(auth_response={"ok": True})
            self.token = token

        def conversations_info(self, *, channel: str) -> Any:
            probed.append((self.token, channel))
            return super().conversations_info(channel=channel)

    def construct(**kwargs: Any) -> ProductionClient:
        return ProductionClient(kwargs["token"])

    monkeypatch.setattr(preflight, "WebClient", construct)

    check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-tokens"),
        identities=(DEFAULT, OPS),
        api_client=_two_agents_api(),
    )

    assert sorted(channel for token, channel in probed if token == "xoxb-ops") == sorted(
        [CHANNEL_B, CHANNEL_D]
    )
    assert sorted(channel for token, channel in probed if token == "xoxb-default") == sorted(
        [CHANNEL_A, CHANNEL_C, CHANNEL_E]
    )
