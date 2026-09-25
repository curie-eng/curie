"""Slack preflight per identity (ADR-0168 decision 2).

Discovery runs once. Each identity then probes only the destinations it owns,
with its own token. One that fails is named and skipped while the others
connect, and several probe at once against the one shared deadline. The API is
faked at the httpx seam and Slack at its provider client seam, as in
test_preflight.py, whose helpers this reuses.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from collections import Counter
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
    """A provider fake that also answers auth.test, and can be slow.

    With ``meet``, its capability probe waits there for the other identity's,
    which passes only while both probes are in flight at once.
    """

    def __init__(
        self,
        *,
        auth_response: object | None = None,
        auth_side_effect: Exception | None = None,
        info_delay_s: float = 0.0,
        meet: threading.Barrier | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.auth_calls = 0
        self.auth_response = auth_response
        self.auth_side_effect = auth_side_effect
        self.info_delay_s = info_delay_s
        self.meet = meet

    def conversations_list(self, *, types: str, exclude_archived: bool, limit: int) -> object:
        if self.meet is not None:
            self.meet.wait()
        return super().conversations_list(
            types=types, exclude_archived=exclude_archived, limit=limit
        )

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
                # which no identity is named; `default` probes it.
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
        (r.levelno, r.getMessage())
        for r in caplog.records
        if "preflight public-channel" in r.getMessage()
    ]
    assert summaries == [
        (
            logging.INFO,
            "Slack identity default: Slack channel capability preflight public-channel "
            "capability verified; attachment download capability verified; checked 3 "
            "configured destinations; unverified 0",
        )
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


IDENTITY_PROBE_FAULT_MESSAGE = (
    "Slack channel capability preflight failed: this identity's probe raised "
    "an unexpected error."
)


class _PerThreadClock:
    """A monotonic clock each probe thread can expire for itself alone.

    Discovery reads it on the calling thread, so the shared deadline is fixed
    before any probe starts; a fake that calls ``expire`` moves only the clock
    its own identity's probe reads. That holds only while each probe has a
    thread of its own, since a pool reuses an idle one, so the fakes that use
    it also ``meet``.
    """

    def __init__(self) -> None:
        self._local = threading.local()

    def expire(self) -> None:
        self._local.expired = True

    def monotonic(self) -> float:
        return 1000.0 if getattr(self._local, "expired", False) else 0.0


def _no_destinations_api() -> httpx.Client:
    return _client(lambda _request: httpx.Response(200, json=[]))


def _summaries(caplog: pytest.LogCaptureFixture) -> dict[str, tuple[int, str]]:
    """Each identity's summary line, keyed by the identity it names."""
    found: dict[str, tuple[int, str]] = {}
    for record in caplog.records:
        message = record.getMessage()
        if "preflight public-channel" not in message:
            continue
        name = message.removeprefix("Slack identity ").split(":", 1)[0]
        found[name] = (record.levelno, message)
    return found


def test_every_identity_probes_against_discoverys_one_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh deadline per identity would stretch the startup envelope the
    chart checks from one Slack budget to one per identity."""
    from curie_dispatcher import preflight

    original = preflight._probe_identity
    deadlines: dict[str, float] = {}

    def record(identity: SlackIdentityCredentials, addresses: Any, **kwargs: Any) -> Any:
        deadlines[identity.name] = kwargs["deadline"]
        return original(identity, addresses, **kwargs)

    monkeypatch.setattr(preflight, "_probe_identity", record)
    # Every read advances the clock, so a deadline taken after discovery differs.
    ticks = itertools.count(100.0, 0.001)

    check_slack_channel_capabilities(
        _config(api_preflight_timeout_s=0.2),
        logger=logging.getLogger("test-preflight-identities-one-deadline"),
        identities=(DEFAULT, OPS),
        web_clients={"default": _IdentityClient(), "ops-bot": _IdentityClient()},
        api_client=_two_agents_api(),
        monotonic=lambda: next(ticks),
    )

    assert deadlines == {"default": 100.0 + 0.2, "ops-bot": 100.0 + 0.2}


def test_identities_probe_at_the_same_time(caplog: pytest.LogCaptureFixture) -> None:
    """Each capability probe waits for the other's; only concurrent probes pass."""
    barrier = threading.Barrier(2, timeout=1)

    logger = logging.getLogger("test-preflight-identities-concurrent")
    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(api_preflight_timeout_s=5.0),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={
                "default": _IdentityClient(meet=barrier),
                "ops-bot": _IdentityClient(meet=barrier),
            },
            api_client=_two_agents_api(),
        )

    summaries = _summaries(caplog)
    assert sorted(summaries) == ["default", "ops-bot"]
    for _level, message in summaries.values():
        assert "public-channel capability verified" in message


def test_capability_probes_carry_each_identitys_own_bot_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_dispatcher import preflight

    calls: list[tuple[str, str]] = []

    class ProductionClient(_IdentityClient):
        def __init__(self, token: str) -> None:
            super().__init__(auth_response={"ok": True})
            self.token = token

        def conversations_list(self, **kwargs: Any) -> object:
            calls.append((self.token, "conversations.list"))
            return super().conversations_list(**kwargs)

        def files_list(self, *, count: int, **unexpected: object) -> object:
            calls.append((self.token, "files.list"))
            return super().files_list(count=count, **unexpected)

        def auth_test(self) -> object:
            calls.append((self.token, "auth.test"))
            return super().auth_test()

    def construct(**kwargs: Any) -> ProductionClient:
        return ProductionClient(kwargs["token"])

    monkeypatch.setattr(preflight, "WebClient", construct)

    check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-capability-tokens"),
        identities=(DEFAULT, OPS),
        api_client=_two_agents_api(),
    )

    each_once = {"conversations.list": 1, "files.list": 1, "auth.test": 1}
    for token in ("xoxb-default", "xoxb-ops"):
        assert Counter(method for owner, method in calls if owner == token) == each_once
    assert {owner for owner, _method in calls} == {"xoxb-default", "xoxb-ops"}


def test_an_expired_deadline_before_auth_test_records_no_bot_ids_and_skips_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _PerThreadClock()

    class _ExpiringAfterFiles(_IdentityClient):
        def files_list(self, *, count: int, **unexpected: object) -> object:
            response = super().files_list(count=count, **unexpected)
            clock.expire()
            return response

    meet = threading.Barrier(2, timeout=1)
    default_client = _ExpiringAfterFiles(
        auth_response={"ok": True, "bot_id": "B0DEFAULT"}, meet=meet
    )
    ops_client = _ExpiringAfterFiles(auth_response={"ok": True, "bot_id": "B0OPS"}, meet=meet)
    logger = logging.getLogger("test-preflight-identities-auth-deadline")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        admitted = check_slack_channel_capabilities(
            _config(),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={"default": default_client, "ops-bot": ops_client},
            api_client=_no_destinations_api(),
            monotonic=clock.monotonic,
        )

    assert admitted == (PreflightedIdentity(DEFAULT, None), PreflightedIdentity(OPS, None))
    assert default_client.auth_calls == 0 and ops_client.auth_calls == 0
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    for name in ("default", "ops-bot"):
        assert any(f"Slack identity {name}" in m and "auth.test" in m for m in warned)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_a_slow_auth_test_cannot_leave_a_destination_unattempted() -> None:
    clock = _PerThreadClock()

    class _SlowAuth(_IdentityClient):
        def auth_test(self) -> object:
            response = super().auth_test()
            clock.expire()
            return response

    meet = threading.Barrier(2, timeout=1)
    ops_client = _SlowAuth(auth_response={"ok": True, "bot_id": "B0OPS"}, meet=meet)

    admitted = check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-slow-auth"),
        identities=(DEFAULT, OPS),
        web_clients={
            "default": _IdentityClient(auth_response={"ok": True}, meet=meet),
            "ops-bot": ops_client,
        },
        api_client=_two_agents_api(),
        monotonic=clock.monotonic,
    )

    assert [identity.name for identity in admitted] == ["default", "ops-bot"]
    assert admitted[1].bot_ids is not None and admitted[1].bot_ids.bot_id == "B0OPS"
    assert sorted(ops_client.channels) == sorted([CHANNEL_B, CHANNEL_D])


def test_an_agent_bound_through_two_identities_gives_both_its_approval_target() -> None:
    agents = [
        _agent(
            channels=[
                {"kind": "slack", "address": CHANNEL_A, "adapter": "default"},
                {"kind": "slack", "address": CHANNEL_B, "adapter": "ops-bot"},
            ],
            approval_routes={"security": {"resolution": {"kind": "slack", "address": CHANNEL_C}}},
        )
    ]
    default_client = _IdentityClient(auth_response={"ok": True})
    ops_client = _IdentityClient(auth_response={"ok": True})

    check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-dual-bound"),
        identities=(DEFAULT, OPS),
        web_clients={"default": default_client, "ops-bot": ops_client},
        api_client=_client(lambda _request: httpx.Response(200, json=agents)),
    )

    assert sorted(default_client.channels) == sorted([CHANNEL_A, CHANNEL_C])
    assert sorted(ops_client.channels) == sorted([CHANNEL_B, CHANNEL_C])


@pytest.mark.parametrize(
    "other_ingress",
    [
        pytest.param(
            {"channels": [{"kind": "email", "address": "ops@example.com", "adapter": "mail"}]},
            id="mail-binding",
        ),
        pytest.param({"hook_partitions": {"alerts": {"pointer": "/alert/id"}}}, id="hook"),
        pytest.param(
            {
                "source_bindings": {
                    "alerts": {
                        "workload_pointer": "/workload",
                        "map": {"payments": {"repo_full_name": "acme/payments"}},
                    }
                }
            },
            id="hook-source-binding",
        ),
    ],
)
def test_default_also_probes_the_approval_target_of_an_agent_with_other_ingress(
    other_ingress: dict[str, Any],
) -> None:
    """A turn that did not arrive on Slack has its card posted by `default`."""
    with_other = _agent(
        channels=[{"kind": "slack", "address": CHANNEL_B, "adapter": "ops-bot"}],
        approval_routes={"security": {"resolution": {"kind": "slack", "address": CHANNEL_C}}},
    )
    extra_channels = other_ingress.get("channels", [])
    with_other.update({k: v for k, v in other_ingress.items() if k != "channels"})
    with_other["channels"] = [*with_other["channels"], *extra_channels]
    slack_only = _agent(
        channels=[{"kind": "slack", "address": CHANNEL_D, "adapter": "ops-bot"}],
        approval_routes={"operations": {"resolution": {"kind": "slack", "address": CHANNEL_E}}},
    )
    default_client = _IdentityClient(auth_response={"ok": True})
    ops_client = _IdentityClient(auth_response={"ok": True})

    check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-identities-other-ingress"),
        identities=(DEFAULT, OPS),
        web_clients={"default": default_client, "ops-bot": ops_client},
        api_client=_client(lambda _request: httpx.Response(200, json=[with_other, slack_only])),
    )

    assert sorted(default_client.channels) == [CHANNEL_C]
    assert sorted(ops_client.channels) == sorted([CHANNEL_B, CHANNEL_C, CHANNEL_D, CHANNEL_E])


def test_an_unexpected_probe_error_is_redacted_and_skips_only_that_identity(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from curie_dispatcher import preflight

    original = preflight._slack_probe_client

    def faulty(bot_token: str, **kwargs: Any) -> Any:
        if bot_token == "xoxb-ops":
            raise RuntimeError("xoxb-secret-sentinel")
        return original(bot_token, **kwargs)

    monkeypatch.setattr(preflight, "_slack_probe_client", faulty)
    logger = logging.getLogger("test-preflight-identities-fault")

    with caplog.at_level(logging.INFO, logger=logger.name):
        admitted = check_slack_channel_capabilities(
            _config(),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={
                "default": _IdentityClient(auth_response={"ok": True}),
                "ops-bot": _IdentityClient(auth_response={"ok": True}),
            },
            api_client=_two_agents_api(),
        )

    assert [identity.name for identity in admitted] == ["default"]
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert errors == [
        "Slack identity ops-bot did not pass preflight and will not connect: "
        f"{IDENTITY_PROBE_FAULT_MESSAGE}"
    ]
    assert "xoxb-secret-sentinel" not in " ".join(r.getMessage() for r in caplog.records)
    assert all(r.exc_info is None for r in caplog.records)


def test_an_unverified_destination_raises_only_its_own_identitys_summary_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    unreachable = SlackApiError(
        "not-found-sentinel", {"ok": False, "error": "channel_not_found"}
    )
    logger = logging.getLogger("test-preflight-identities-levels")

    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(),
            logger=logger,
            identities=(DEFAULT, OPS),
            web_clients={
                "default": _IdentityClient(auth_response={"ok": True}),
                "ops-bot": _IdentityClient(auth_response={"ok": True}, side_effect=unreachable),
            },
            api_client=_two_agents_api(),
        )

    summaries = _summaries(caplog)
    assert summaries["default"][0] == logging.INFO
    assert summaries["ops-bot"] == (
        logging.WARNING,
        "Slack identity ops-bot: Slack channel capability preflight public-channel "
        "capability verified; attachment download capability verified; checked 0 "
        "configured destinations; unverified 2",
    )


def test_a_provider_call_that_ignores_its_timeout_cannot_hold_every_identity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Real clock. `ops-bot`'s listing hangs far past any timeout it was given;
    the wait for it ends one Slack call's timeout after the shared deadline."""
    release = threading.Event()

    class _Hung(_IdentityClient):
        def conversations_list(self, **kwargs: Any) -> object:
            release.wait(timeout=8.0)
            return super().conversations_list(**kwargs)

    logger = logging.getLogger("test-preflight-identities-hung")
    started = time.monotonic()
    try:
        with caplog.at_level(logging.ERROR, logger=logger.name):
            admitted = check_slack_channel_capabilities(
                _config(api_preflight_timeout_s=0.2),
                logger=logger,
                identities=(DEFAULT, OPS),
                web_clients={
                    "default": _IdentityClient(auth_response={"ok": True}),
                    "ops-bot": _Hung(auth_response={"ok": True}),
                },
                api_client=_two_agents_api(),
            )
        elapsed = time.monotonic() - started
    finally:
        release.set()

    assert elapsed < 4.0
    assert [identity.name for identity in admitted] == ["default"]
    assert [r.getMessage() for r in caplog.records] == [
        f"Slack identity ops-bot did not pass preflight and will not connect: "
        f"{SLACK_TIMEOUT_MESSAGE}"
    ]


def test_a_name_missing_from_web_clients_raises_rather_than_reaching_slack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_dispatcher import preflight

    constructed: list[object] = []

    def construct(**kwargs: Any) -> _IdentityClient:
        constructed.append(kwargs)
        return _IdentityClient(auth_response={"ok": True})

    monkeypatch.setattr(preflight, "WebClient", construct)

    with pytest.raises(KeyError):
        check_slack_channel_capabilities(
            _config(),
            logger=logging.getLogger("test-preflight-identities-missing-seam"),
            identities=(DEFAULT, OPS),
            web_clients={"default": _IdentityClient(auth_response={"ok": True})},
            api_client=_two_agents_api(),
        )

    assert constructed == []
