"""#3767: the sandbox state credential reaches only its own channel's transcripts.

A transcript key is a thread key (``kernel._thread_key_for``). The API maps it
back to its binding with ``threadkeys.transcript_binding`` and holds a sandbox
(``state``) credential to the binding its claim names, the way ADR-0188 holds
memory (plan section 4):

- a bound credential reaches a key whose binding is its own, whatever identity
  segment the key carries, and on a binding path only its own binding;
- the unbound credential (a targetless cron's) reaches only
  ``@cron:<its agent>:...`` keys;
- a key no producer builds is refused for a sandbox, kept for the platform key;
- a legacy token (no ``memory`` claim) keeps its reach for now, with a warning
  and a metric, because refusing it would cut off every warm sandbox's history
  at API deploy.

The riskiest failure is a legitimate key being refused: the runner's history
read then fails and the conversation silently loses its past. So every key
form the worker produces is exercised against its own credential here, and
``tests/test_thread_key_binding_parity.py`` checks the worker's real producers.

Real router, real Postgres, nothing mocked except the metrics sink.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import pytest
from channel_protocol import hook_conversation_id, scoped_conversation_id
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time

CHANNEL_A = "C0EXAMPLE6"
CHANNEL_B = "C0EXAMPLE7"
BINDING_A = f"slack:{CHANNEL_A}"
BINDING_B = f"slack:{CHANNEL_B}"
TS = "1700000000.000100"
HISTORY = [{"role": "user", "content": "hello"}]
SECRET = [{"role": "user", "content": "a DM secret"}]

MAIL_ADDRESS = "agent@example.test"
MAIL_ADAPTER = "agentmail-sandbox"
MAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
MAIL_THREAD = "thread/9"
MAIL_BINDING = f"email:{MAIL_ADDRESS}"
MAIL_OLD_KEY = scoped_conversation_id("email", MAIL_ADDRESS, MAIL_THREAD)
MAIL_NEW_KEY = scoped_conversation_id("email", MAIL_ADDRESS, MAIL_THREAD, identity=MAIL_ADAPTER)


def _slack_key(channel: str, conversation: str = TS, identity: str | None = None) -> str:
    return scoped_conversation_id("slack", channel, conversation, identity=identity)


def _cron_key(agent_id: str, conversation: str = "cron-nightly") -> str:
    return scoped_conversation_id("@cron", agent_id, conversation)


def _q(key: str) -> str:
    # The worker quotes the key into the history ref once (binding.boot_env).
    # Starlette's TestClient unquotes the path twice where a real server
    # decodes once, so quote twice here (see test_transcript_identity._url).
    return quote(quote(key, safe=""), safe="")


def _url(aid: str, key: str) -> str:
    return f"/agents/{aid}/state/transcript/{_q(key)}"


def _binding_url(aid: str, kind: str, address: str, key: str) -> str:
    return f"/agents/{aid}/state/bindings/{kind}/{address}/transcript/{_q(key)}"


def _agent(client: Any, headers: dict[str, str], channel: dict[str, Any]) -> str:
    resp = client.post(
        "/agents",
        json={"name": f"transcript-scope-{uuid.uuid4().hex[:6]}", "channel": channel},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _agent_with_two_channels(client: Any, auth_headers: dict[str, str]) -> str:
    aid = _agent(client, auth_headers, {"kind": "slack", "address": CHANNEL_A})
    second = client.post(
        f"/agents/{aid}/channels",
        json={"kind": "slack", "address": CHANNEL_B},
        headers=auth_headers,
    )
    assert second.status_code == 201, second.text
    return aid


def _mail_agent(client: Any, auth_headers: dict[str, str]) -> str:
    return _agent(
        client,
        auth_headers,
        {
            "kind": "email",
            "address": MAIL_ADDRESS,
            "endpoint": MAIL_ENDPOINT,
            "adapter": MAIL_ADAPTER,
        },
    )


def _token(aid: str, claims: Mapping[str, str | None] | None) -> dict[str, str]:
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def _reader(aid: str, binding: str | None = BINDING_A) -> dict[str, str]:
    """The long-lived boot credential: the one the runner's history loader holds."""
    return _token(aid, {"binding": binding, "memory": "read"})


def _writer(aid: str, binding: str = BINDING_A) -> dict[str, str]:
    """The per-turn credential."""
    return _token(aid, {"binding": binding, "memory": "write", "sender": "U0S", "turn": "evt-1"})


def _legacy(aid: str) -> dict[str, str]:
    """What a pre-ADR-0188 worker mints: three claims, no memory claim."""
    return _token(aid, None)


def _seed(client: Any, headers: dict[str, str], url: str, value: Any) -> None:
    resp = client.put(url, json={"value": value}, headers=headers)
    assert resp.status_code == 200, resp.text


def _assert_full_reach(client: Any, url: str, headers: dict[str, str]) -> None:
    """The history loader's whole cycle on its own thread: read, replace, append."""
    put = client.put(url, json={"value": HISTORY}, headers=headers)
    assert put.status_code == 200, (url, put.text)
    got = client.get(url, headers=headers)
    assert got.status_code == 200, (url, got.text)
    appended = client.post(
        f"{url}/append", json={"item": {"role": "assistant", "content": "hi"}}, headers=headers
    )
    assert appended.status_code == 200, (url, appended.text)


def _assert_no_reach(client: Any, url: str, headers: dict[str, str]) -> None:
    """Every verb refused, and nothing of the stored thread leaks."""
    got = client.get(url, headers=headers)
    assert got.status_code == 403, (url, got.status_code, got.text)
    assert "a DM secret" not in got.text
    put = client.put(url, json={"value": HISTORY}, headers=headers)
    assert put.status_code == 403, (url, put.status_code, put.text)
    appended = client.post(f"{url}/append", json={"item": {"role": "user"}}, headers=headers)
    assert appended.status_code == 403, (url, appended.status_code, appended.text)
    deleted = client.delete(url, headers=headers)
    assert deleted.status_code == 403, (url, deleted.status_code, deleted.text)


def _thread_rows(agent_id: str, key: str) -> int:
    async def run() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(
                    text(
                        "SELECT count(*) FROM curie.thread_transcripts "
                        "WHERE agent_id = :agent AND thread_key = :key"
                    ),
                    {"agent": agent_id, "key": key},
                )
                return int(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_channel_a_credential_is_refused_channel_b_transcript(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The #3767 probe: the sandbox serving channel A reads a channel B thread
    # by editing the key in its CURIE_HISTORY_REF.
    aid = _agent_with_two_channels(client, auth_headers)
    b_url = _url(aid, _slack_key(CHANNEL_B))
    _seed(client, auth_headers, b_url, SECRET)

    for headers in (_reader(aid), _writer(aid)):
        _assert_no_reach(client, b_url, headers)
    # Channel B's thread is untouched.
    stored = client.get(b_url, headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["value"] == SECRET

    # Its own thread keeps the history loader's full cycle, with the read
    # credential the boot env hands it ("read" narrows memory, not history).
    _assert_full_reach(client, _url(aid, _slack_key(CHANNEL_A)), _reader(aid))
    # Channel B's own credential still reaches channel B.
    assert client.get(b_url, headers=_reader(aid, BINDING_B)).status_code == 200


def test_named_identity_key_maps_to_its_binding(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Every key form the worker builds for channel A's route must stay
    # reachable to channel A's credential; refusing one silently drops that
    # conversation's history. The identity segment never changes the binding.
    aid = _agent_with_two_channels(client, auth_headers)
    own = {
        "plain": _slack_key(CHANNEL_A),
        "named-identity": _slack_key(CHANNEL_A, identity="first-bot"),
        "named-relay": _slack_key(CHANNEL_A, identity="sre-bot"),
        "hook": _slack_key(CHANNEL_A, hook_conversation_id(uuid.UUID(aid), "deploys")),
        "hook-partition": _slack_key(
            CHANNEL_A, hook_conversation_id(uuid.UUID(aid), "pr-review", "curie-eng/curie#12")
        ),
        "eval-isolate": _slack_key(CHANNEL_A, "eval:1720000000.000100"),
    }
    for form, key in own.items():
        url = _url(aid, key)
        _assert_full_reach(client, url, _reader(aid))
        assert client.get(url, headers=_writer(aid)).status_code == 200, form

    # The same identity on another channel is that channel's thread.
    other = _url(aid, _slack_key(CHANNEL_B, identity="first-bot"))
    _seed(client, auth_headers, other, SECRET)
    _assert_no_reach(client, other, _reader(aid))

    # A non-Slack named route: the address is percent-encoded inside the key
    # ("%40"), and the binding claim carries it unquoted.
    mail = _mail_agent(client, auth_headers)
    mail_url = _url(mail, MAIL_NEW_KEY)
    _assert_full_reach(client, mail_url, _reader(mail, MAIL_BINDING))
    _assert_no_reach(client, mail_url, _reader(mail, BINDING_A))


def test_cron_key_only_for_unbound_credential_of_same_agent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A targetless cron's boot env names no binding, so its credential's
    # binding claim is null; its thread is ``@cron:<agent>:<conv>``.
    aid = _agent_with_two_channels(client, auth_headers)
    unbound = _reader(aid, binding=None)
    cron_url = _url(aid, _cron_key(aid, f"cron-{uuid.uuid4().hex}"))
    _assert_full_reach(client, cron_url, unbound)

    # A bound credential does not reach the cron thread...
    _seed(client, auth_headers, cron_url, SECRET)
    _assert_no_reach(client, cron_url, _reader(aid))
    # ...the unbound one reaches no channel's thread...
    a_url = _url(aid, _slack_key(CHANNEL_A))
    _seed(client, auth_headers, a_url, SECRET)
    _assert_no_reach(client, a_url, unbound)
    # ...and not a cron thread naming another agent.
    other_cron = _url(aid, _cron_key(str(uuid.uuid4())))
    _seed(client, auth_headers, other_cron, SECRET)
    _assert_no_reach(client, other_cron, unbound)


@pytest.mark.parametrize(
    "key",
    [
        "not-a-thread-key",
        # The bare eval conversation id: the kernel always scopes it, so a
        # sandbox never reads its transcript under this form.
        "eval:1720000000.000100",
        # A lowercase escape is not the canonical form, so it does not parse.
        f"slack:{CHANNEL_A}:hook%3aabc",
        f"slack:{CHANNEL_A}:a:b:c",
    ],
)
def test_unparseable_key_refused_for_sandbox_allowed_for_platform(
    client: Any, auth_headers: dict[str, str], clean_db: None, key: str
) -> None:
    aid = _agent_with_two_channels(client, auth_headers)
    url = _url(aid, key)
    _assert_full_reach(client, url, auth_headers)
    _seed(client, auth_headers, url, SECRET)

    for headers in (_reader(aid), _writer(aid), _reader(aid, binding=None)):
        _assert_no_reach(client, url, headers)
    assert client.get(url, headers=auth_headers).json()["value"] == SECRET


def test_transcript_listing_filters_to_own_binding(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent_with_two_channels(client, auth_headers)
    own = {_slack_key(CHANNEL_A), _slack_key(CHANNEL_A, "1700000000.000200", "first-bot")}
    cron = _cron_key(aid)
    others = {_slack_key(CHANNEL_B), cron, "not-a-thread-key"}
    for key in own | others:
        _seed(client, auth_headers, _url(aid, key), HISTORY)

    def listed(headers: dict[str, str]) -> set[str]:
        resp = client.get(f"/agents/{aid}/state/transcript", headers=headers)
        assert resp.status_code == 200, resp.text
        return {row["key"] for row in resp.json()}

    assert listed(_reader(aid)) == own
    assert listed(_writer(aid)) == own
    assert listed(_reader(aid, binding=None)) == {cron}
    assert listed(auth_headers) == own | others

    # The namespace summary would count every channel's threads, so a
    # sandbox credential does not get it; the platform key does.
    def namespaces(headers: dict[str, str]) -> set[str]:
        resp = client.get(f"/agents/{aid}/state", headers=headers)
        assert resp.status_code == 200, resp.text
        return {row["namespace"] for row in resp.json()}

    assert "transcript" in namespaces(auth_headers)
    assert "transcript" not in namespaces(_reader(aid))
    assert "transcript" not in namespaces(_reader(aid, binding=None))


def test_binding_path_checks_path_and_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # On a binding path both the path's binding and the key's must be the
    # credential's: neither can launder the other.
    aid = _agent_with_two_channels(client, auth_headers)
    a_key, b_key = _slack_key(CHANNEL_A), _slack_key(CHANNEL_B)
    for address in (CHANNEL_A, CHANNEL_B):
        for key in (a_key, b_key):
            _seed(client, auth_headers, _binding_url(aid, "slack", address, key), SECRET)

    _assert_full_reach(client, _binding_url(aid, "slack", CHANNEL_A, a_key), _reader(aid))
    for address, key in ((CHANNEL_A, b_key), (CHANNEL_B, a_key), (CHANNEL_B, b_key)):
        _assert_no_reach(client, _binding_url(aid, "slack", address, key), _reader(aid))

    # Listing another binding's transcripts yields none of them.
    other = client.get(
        f"/agents/{aid}/state/bindings/slack/{CHANNEL_B}/transcript", headers=_reader(aid)
    )
    assert other.status_code == 403 or other.json() == [], other.text
    own = client.get(
        f"/agents/{aid}/state/bindings/slack/{CHANNEL_A}/transcript", headers=_reader(aid)
    )
    assert own.status_code == 200, own.text
    assert {row["key"] for row in own.json()} == {a_key}


def test_pre_identity_adoption_within_own_binding(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A named mail route's thread written before ADR-0168 decision 4 sits
    # under its pre-identity key (no identity segment). Its own credential must
    # still read it, under both keys, and the first read adopts it. Another
    # binding's credential must not, and must not trigger the adoption either.
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, _url(aid, MAIL_OLD_KEY), HISTORY)
    new_url = _url(aid, MAIL_NEW_KEY)

    refused = client.get(new_url, headers=_reader(aid, BINDING_B))
    assert refused.status_code == 403, refused.text
    assert "hello" not in refused.text
    assert _thread_rows(aid, MAIL_NEW_KEY) == 0, "a refused read adopted the old thread"

    own = _reader(aid, MAIL_BINDING)
    read = client.get(new_url, headers=own)
    assert read.status_code == 200, read.text
    assert read.json()["key"] == MAIL_NEW_KEY
    assert read.json()["value"] == HISTORY
    old = client.get(_url(aid, MAIL_OLD_KEY), headers=own)
    assert old.status_code == 200, old.text
    reply = {"role": "assistant", "content": "hi"}
    appended = client.post(f"{new_url}/append", json={"item": reply}, headers=own)
    assert appended.status_code == 200, appended.text
    assert appended.json()["value"] == [*HISTORY, reply]


class _MetricProbe:
    def __init__(self) -> None:
        self.points: list[tuple[str, dict[str, str]]] = []

    def record_metric(
        self, name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        del value
        self.points.append((name, dict(attributes or {})))


def test_legacy_token_transcript_allowed_with_warning(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Plan section 4: a token from a pre-ADR-0188 worker keeps its transcript
    # reach until it expires (at most 24h after the worker upgrade); refusing
    # it would cut off every warm sandbox's history at API deploy. Each use is
    # logged and counted so the window is visible.
    import curie_telemetry
    from curie_api import state_mutation
    from curie_api.routers import state as state_router

    probe = _MetricProbe()
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (state_router, state_mutation):
        monkeypatch.setattr(module, "record_metric", probe.record_metric, raising=False)

    aid = _agent_with_two_channels(client, auth_headers)
    legacy = _legacy(aid)
    with caplog.at_level(logging.WARNING):
        for key in (_slack_key(CHANNEL_A), _slack_key(CHANNEL_B), _cron_key(aid)):
            _assert_full_reach(client, _url(aid, key), legacy)
        listed = client.get(f"/agents/{aid}/state/transcript", headers=legacy)
        assert listed.status_code == 200, listed.text

    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    legacy_lines = [m for m in warnings if "legacy sandbox token" in m]
    assert legacy_lines, warnings
    assert any(aid in m and "transcript" in m for m in legacy_lines), legacy_lines
    assert not any(legacy["X-API-Key"] in m for m in warnings), "the token was logged"
    assert any(name == "curie.state.legacy_token" for name, _ in probe.points), probe.points


def test_legacy_token_warning_logged_once_per_agent(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A warm legacy sandbox makes several transcript requests per turn for up
    # to 24h. The warning names the agent once per API process; the metric
    # still counts every request, so the window stays visible.
    import curie_telemetry
    from curie_api import state_mutation
    from curie_api.routers import state as state_router

    probe = _MetricProbe()
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (state_router, state_mutation):
        monkeypatch.setattr(module, "record_metric", probe.record_metric, raising=False)

    first = _agent_with_two_channels(client, auth_headers)
    second = _agent(client, auth_headers, {"kind": "slack", "address": "C0EXAMPLE8"})
    requests = 0
    with caplog.at_level(logging.WARNING):
        for aid in (first, second):
            legacy = _legacy(aid)
            url = _url(aid, _slack_key(CHANNEL_A if aid == first else "C0EXAMPLE8"))
            for _ in range(3):
                _assert_full_reach(client, url, legacy)
                requests += 3
            listed = client.get(f"/agents/{aid}/state/transcript", headers=legacy)
            assert listed.status_code == 200, listed.text
            requests += 1

    legacy_lines = [
        r.getMessage()
        for r in caplog.records
        if r.levelno >= logging.WARNING and "legacy sandbox token" in r.getMessage()
    ]
    assert len(legacy_lines) == 2, legacy_lines
    assert sum(first in m for m in legacy_lines) == 1, legacy_lines
    assert sum(second in m for m in legacy_lines) == 1, legacy_lines
    counted = [name for name, _ in probe.points if name == "curie.state.legacy_token"]
    assert len(counted) == requests, (len(counted), requests)
