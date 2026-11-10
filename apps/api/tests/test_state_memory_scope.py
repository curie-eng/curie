"""ADR-0188: the sandbox memory credential is scoped to its own channel (#3623).

The API, not the sandbox, decides what a ``state`` credential may do on the
``memory`` namespace. Each check below runs against the real state router and
the disposable Postgres the conftest provisions; nothing is mocked. Tokens are
minted with the shared ``sandbox_token`` module used by the worker and API.

The two credential shapes (plan section 2):

- long-lived: ``{binding, memory: "read"}``, handed to the sandbox at boot;
- per-turn: ``{binding, memory: "write", sender, turn}``, carried on the turn's
  ACI ``Event``.

A token with no ``memory`` claim is a legacy (pre-ADR-0188) token and fails
closed: read-only, agent memory only.
"""

import logging
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint

_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time

CHANNEL_A = "C000000S01"
CHANNEL_B = "C000000S02"
BINDING_A = f"slack:{CHANNEL_A}"
BINDING_B = f"slack:{CHANNEL_B}"
SENDER = "U0000SENDER"
FACT = "fact-" + "0123456789abcdef" * 2
OTHER_FACT = "fact-" + "fedcba9876543210" * 2

OTHER_CHANNEL = "credential is scoped to another channel"
READ_ONLY = "memory credential is read-only"
FACT_KEYS_ONLY = "only fact keys are writable with a sandbox credential"


def _agent_with_two_channels(client: Any, auth_headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={"name": "memory-scope", "channel": {"kind": "slack", "address": CHANNEL_A}},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    second = client.post(
        f"/agents/{aid}/channels",
        json={"kind": "slack", "address": CHANNEL_B},
        headers=auth_headers,
    )
    assert second.status_code == 201, second.text
    return aid


def _token(aid: str, claims: dict[str, str | None] | None = None) -> dict[str, str]:
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def _reader(aid: str, binding: str | None = BINDING_A) -> dict[str, str]:
    return _token(aid, {"binding": binding, "memory": "read"})


def _writer(
    aid: str, binding: str | None = BINDING_A, *, sender: str = SENDER, turn: str = "evt-1"
) -> dict[str, str]:
    return _token(aid, {"binding": binding, "memory": "write", "sender": sender, "turn": turn})


def _legacy(aid: str) -> dict[str, str]:
    # Exactly what a pre-ADR-0188 worker mints: three claims, no memory claim.
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE)
    return {"X-API-Key": token}


def _channel(aid: str, address: str) -> str:
    return f"/agents/{aid}/state/bindings/slack/{address}/memory"


def _agent_memory(aid: str) -> str:
    return f"/agents/{aid}/state/memory"


def _fact_value(
    statement: str = "the deploy window is Tuesday", author: str = SENDER
) -> dict[str, str]:
    return {
        "statement": statement,
        "author": author,
        "stated_at": "2026-10-01T00:00:00+00:00",
        "session_id": "sess-1",
    }


def _seed(client: Any, auth_headers: dict[str, str], url: str, value: Any) -> None:
    resp = client.put(url, json={"value": value}, headers=auth_headers)
    assert resp.status_code == 200, resp.text


def _detail(resp: Any) -> str:
    return str(resp.json().get("detail", ""))


def test_channel_a_credential_is_refused_channel_b_memory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The #3623 probe: editing the channel in CURIE_CHANNEL_MEMORY_REF reached
    # another channel's memory. Channel A's credential must get 403 on every
    # verb against channel B's memory, and keep full reach on its own.
    aid = _agent_with_two_channels(client, auth_headers)
    b_fact = f"{_channel(aid, CHANNEL_B)}/{FACT}"
    _seed(client, auth_headers, b_fact, _fact_value("a DM secret"))

    for headers in (_writer(aid), _reader(aid)):
        got = client.get(b_fact, headers=headers)
        assert got.status_code == 403, got.text
        assert OTHER_CHANNEL in _detail(got)
        assert "a DM secret" not in got.text
        listed = client.get(_channel(aid, CHANNEL_B), headers=headers)
        assert listed.status_code == 403, listed.text
        assert OTHER_CHANNEL in _detail(listed)
    put = client.put(b_fact, json={"value": _fact_value("planted")}, headers=_writer(aid))
    assert put.status_code == 403, put.text
    assert OTHER_CHANNEL in _detail(put)
    deleted = client.delete(b_fact, headers=_writer(aid))
    assert deleted.status_code == 403, deleted.text
    assert OTHER_CHANNEL in _detail(deleted)
    # Channel B's fact is untouched.
    stored = client.get(b_fact, headers=auth_headers)
    assert stored.status_code == 200
    assert stored.json()["value"]["statement"] == "a DM secret"

    # Own channel: full reach with the write credential.
    a_fact = f"{_channel(aid, CHANNEL_A)}/{FACT}"
    own_put = client.put(a_fact, json={"value": _fact_value()}, headers=_writer(aid))
    assert own_put.status_code == 200, own_put.text
    assert client.get(a_fact, headers=_writer(aid)).status_code == 200
    assert client.get(a_fact, headers=_reader(aid)).status_code == 200
    own_list = client.get(_channel(aid, CHANNEL_A), headers=_reader(aid))
    assert own_list.status_code == 200, own_list.text
    assert [e["key"] for e in own_list.json()] == [FACT]
    assert client.delete(a_fact, headers=_writer(aid)).status_code == 204


def test_cross_binding_refusal_precedes_binding_lookup(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The claim check runs before _binding_scope's database lookup, so a 403
    # vs 404 difference cannot be used to probe which bindings exist.
    aid = _agent_with_two_channels(client, auth_headers)
    unheld = f"/agents/{aid}/state/bindings/slack/C0EXAMPLE9/memory"
    # The platform key sees the lookup's 404 (no such binding).
    assert client.get(f"{unheld}/{FACT}", headers=auth_headers).status_code == 404
    for method, url in (
        ("GET", f"{unheld}/{FACT}"),
        ("GET", unheld),
        ("PUT", f"{unheld}/{FACT}"),
        ("DELETE", f"{unheld}/{FACT}"),
    ):
        kwargs: dict[str, Any] = {"headers": _writer(aid)}
        if method == "PUT":
            kwargs["json"] = {"value": _fact_value()}
        resp = client.request(method, url, **kwargs)
        assert resp.status_code == 403, f"{method} {url}: {resp.status_code} {resp.text}"
        assert OTHER_CHANNEL in _detail(resp)


def test_unbound_credential_reaches_agent_memory_only(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A credential minted with no binding (an eval, or a turn with no channel)
    # reaches agent memory and no channel's memory.
    aid = _agent_with_two_channels(client, auth_headers)
    _seed(client, auth_headers, f"{_agent_memory(aid)}/guidance", {"text": "be brief"})
    _seed(client, auth_headers, f"{_channel(aid, CHANNEL_A)}/{FACT}", _fact_value())

    reader = _reader(aid, binding=None)
    writer = _writer(aid, binding=None)
    assert client.get(f"{_agent_memory(aid)}/guidance", headers=reader).status_code == 200
    assert client.get(_agent_memory(aid), headers=reader).status_code == 200
    put = client.put(f"{_agent_memory(aid)}/{FACT}", json={"value": _fact_value()}, headers=writer)
    assert put.status_code == 200, put.text

    for headers in (reader, writer):
        for address in (CHANNEL_A, CHANNEL_B):
            got = client.get(f"{_channel(aid, address)}/{FACT}", headers=headers)
            assert got.status_code == 403, got.text
            assert OTHER_CHANNEL in _detail(got)
            assert client.get(_channel(aid, address), headers=headers).status_code == 403
    refused = client.put(
        f"{_channel(aid, CHANNEL_A)}/{OTHER_FACT}", json={"value": _fact_value()}, headers=writer
    )
    assert refused.status_code == 403, refused.text


def test_read_credential_refuses_memory_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #3623 probe item 3: with memory writes off the sandbox only holds the
    # long-lived read credential, and that must be read-only on memory.
    aid = _agent_with_two_channels(client, auth_headers)
    agent_fact = f"{_agent_memory(aid)}/{FACT}"
    channel_fact = f"{_channel(aid, CHANNEL_A)}/{FACT}"
    _seed(client, auth_headers, agent_fact, _fact_value("seeded"))
    reader = _reader(aid)

    for url in (agent_fact, channel_fact, f"{_agent_memory(aid)}/{OTHER_FACT}"):
        put = client.put(url, json={"value": _fact_value("planted")}, headers=reader)
        assert put.status_code == 403, f"{url}: {put.text}"
        assert READ_ONLY in _detail(put)
    deleted = client.delete(agent_fact, headers=reader)
    assert deleted.status_code == 403, deleted.text
    assert READ_ONLY in _detail(deleted)

    # Nothing changed, and reads still work with the same credential.
    got = client.get(agent_fact, headers=reader)
    assert got.status_code == 200, got.text
    assert got.json()["value"]["statement"] == "seeded"
    assert got.json()["version"] == 1
    assert client.get(channel_fact, headers=auth_headers).status_code == 404


def test_sandbox_credential_cannot_write_guidance_or_log(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Only fact keys (fact- + 32 lowercase hex, the shape the tools mint) are
    # writable with a sandbox credential; guidance and the legacy log are not.
    aid = _agent_with_two_channels(client, auth_headers)
    _seed(client, auth_headers, f"{_agent_memory(aid)}/guidance", {"text": "operator text"})
    writer = _writer(aid)

    not_fact_keys = (
        "guidance",
        "log",
        "k",
        "fact-" + "0" * 31,
        "fact-" + "0" * 33,
        "fact-" + "A" * 32,
        "fact-" + "g" * 32,
        "xfact-" + "0" * 32,
    )
    for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
        for key in not_fact_keys:
            put = client.put(f"{base}/{key}", json={"value": {"text": "sneaky"}}, headers=writer)
            assert put.status_code == 403, f"{base}/{key}: {put.text}"
            assert FACT_KEYS_ONLY in _detail(put)
    deleted = client.delete(f"{_agent_memory(aid)}/guidance", headers=writer)
    assert deleted.status_code == 403, deleted.text
    assert FACT_KEYS_ONLY in _detail(deleted)

    # Guidance is unchanged, and the sandbox can still read it (the runner
    # loads it at boot).
    got = client.get(f"{_agent_memory(aid)}/guidance", headers=_reader(aid))
    assert got.status_code == 200, got.text
    assert got.json()["value"] == {"text": "operator text"}
    assert got.json()["version"] == 1


def test_platform_key_writes_guidance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The platform key (``curie cluster memory``) keeps full reach on memory:
    # guidance, the legacy log, appends, deletes, any binding.
    aid = _agent_with_two_channels(client, auth_headers)
    guidance = f"{_agent_memory(aid)}/guidance"
    put = client.put(guidance, json={"value": {"text": "operator"}}, headers=auth_headers)
    assert put.status_code == 200, put.text
    log = client.post(
        f"{_agent_memory(aid)}/log/append", json={"item": {"n": 1}}, headers=auth_headers
    )
    assert log.status_code == 200, log.text
    channel = client.put(
        f"{_channel(aid, CHANNEL_B)}/guidance", json={"value": {"text": "x"}}, headers=auth_headers
    )
    assert channel.status_code == 200, channel.text
    assert client.delete(guidance, headers=auth_headers).status_code == 204


def test_append_on_memory_refused_for_sandbox(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Facts are written with PUT; POST append on memory is always refused for a
    # sandbox credential, whatever the key and whichever credential.
    aid = _agent_with_two_channels(client, auth_headers)
    for headers in (_writer(aid), _reader(aid), _legacy(aid)):
        for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
            for key in ("log", FACT):
                resp = client.post(f"{base}/{key}/append", json={"item": {"n": 1}}, headers=headers)
                assert resp.status_code == 403, f"{base}/{key}: {resp.text}"
    assert client.get(f"{_agent_memory(aid)}/log", headers=auth_headers).status_code == 404
    # Append on general state is unaffected.
    ok = client.post(
        f"/agents/{aid}/state/workflow/log/append", json={"item": {"n": 1}}, headers=_writer(aid)
    )
    assert ok.status_code == 200, ok.text


def test_write_stamps_author_from_sender_claim(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #3623 probe item 2: a raw PUT stored any author. The API now ignores the
    # body's author and stores the per-turn credential's sender claim, on
    # create and on update, for agent and channel memory alike.
    aid = _agent_with_two_channels(client, auth_headers)
    for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
        url = f"{base}/{FACT}"
        created = client.put(
            url, json={"value": _fact_value("v1", author="U0FORGED01")}, headers=_writer(aid)
        )
        assert created.status_code == 200, created.text
        assert created.json()["value"]["author"] == SENDER
        assert created.json()["value"]["statement"] == "v1"
        assert client.get(url, headers=auth_headers).json()["value"]["author"] == SENDER

        updated = client.put(
            url,
            json={"value": _fact_value("v2", author="U0FORGED02")},
            headers=_writer(aid, sender="U0SECOND01", turn="evt-2"),
        )
        assert updated.status_code == 200, updated.text
        stored = client.get(url, headers=auth_headers).json()
        assert stored["value"]["author"] == "U0SECOND01"
        assert stored["value"]["statement"] == "v2"
        assert stored["version"] == 2

        # A body with no author at all is stamped too.
        no_author = {k: v for k, v in _fact_value("v3").items() if k != "author"}
        third = client.put(url, json={"value": no_author}, headers=_writer(aid))
        assert third.status_code == 200, third.text
        assert client.get(url, headers=auth_headers).json()["value"]["author"] == SENDER

    # A job or eval turn's sender is the no-person marker, stamped verbatim.
    job = client.put(
        f"{_agent_memory(aid)}/{OTHER_FACT}",
        json={"value": _fact_value(author="U0FORGED03")},
        headers=_writer(aid, sender="<no person>"),
    )
    assert job.status_code == 200, job.text
    assert job.json()["value"]["author"] == "<no person>"


def test_platform_key_keeps_body_author(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The operator is trusted to say who they are.
    aid = _agent_with_two_channels(client, auth_headers)
    for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
        url = f"{base}/{FACT}"
        put = client.put(
            url, json={"value": _fact_value(author="operator-alex")}, headers=auth_headers
        )
        assert put.status_code == 200, put.text
        assert client.get(url, headers=auth_headers).json()["value"]["author"] == "operator-alex"


def test_non_object_fact_value_is_422(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The author is stamped into the value, so a sandbox fact write must be a
    # JSON object.
    aid = _agent_with_two_channels(client, auth_headers)
    url = f"{_agent_memory(aid)}/{FACT}"
    for value in ("a string", ["a", "list"], 42, True, None):
        resp = client.put(url, json={"value": value}, headers=_writer(aid))
        assert resp.status_code == 422, f"{value!r}: {resp.status_code} {resp.text}"
    assert client.get(url, headers=auth_headers).status_code == 404
    # The platform key is not held to the fact shape.
    assert (
        client.put(url, json={"value": "operator string"}, headers=auth_headers).status_code == 200
    )


def test_write_token_without_sender_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A write credential with no sender claim cannot be stamped, so it cannot
    # write. The worker never mints one; a 403 keeps a bug from storing
    # unattributed facts.
    aid = _agent_with_two_channels(client, auth_headers)
    headers = _token(aid, {"binding": BINDING_A, "memory": "write", "turn": "evt-1"})
    for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
        resp = client.put(f"{base}/{FACT}", json={"value": _fact_value()}, headers=headers)
        assert resp.status_code == 403, resp.text
        assert client.get(f"{base}/{FACT}", headers=auth_headers).status_code == 404


def test_legacy_state_token_is_read_only_agent_memory_and_warns(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: pytest.LogCaptureFixture
) -> None:
    # ADR-0188 Consequences: a token with no memory claim (a pre-upgrade
    # worker's) fails closed on memory -- agent memory read-only, no channel
    # memory -- and the API logs a warning naming the agent, the path and
    # "legacy sandbox token". General state and transcripts keep their reach.
    aid = _agent_with_two_channels(client, auth_headers)
    _seed(client, auth_headers, f"{_agent_memory(aid)}/{FACT}", _fact_value("seeded"))
    _seed(client, auth_headers, f"{_channel(aid, CHANNEL_A)}/{FACT}", _fact_value("channel"))
    legacy = _legacy(aid)

    assert client.get(f"{_agent_memory(aid)}/{FACT}", headers=legacy).status_code == 200
    assert client.get(_agent_memory(aid), headers=legacy).status_code == 200

    channel_path = f"{_channel(aid, CHANNEL_A)}/{FACT}"
    with caplog.at_level(logging.WARNING):
        put = client.put(
            f"{_agent_memory(aid)}/{FACT}", json={"value": _fact_value("planted")}, headers=legacy
        )
        assert put.status_code == 403, put.text
        assert client.delete(f"{_agent_memory(aid)}/{FACT}", headers=legacy).status_code == 403
        channel = client.get(channel_path, headers=legacy)
        assert channel.status_code == 403, channel.text
        assert client.get(_channel(aid, CHANNEL_A), headers=legacy).status_code == 403

    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    legacy_lines = [m for m in warnings if "legacy sandbox token" in m]
    assert legacy_lines, warnings
    assert any(aid in m for m in legacy_lines), legacy_lines
    assert any(f"bindings/slack/{CHANNEL_A}/memory" in m for m in legacy_lines), legacy_lines
    # The token itself is never logged.
    assert not any(legacy["X-API-Key"] in m for m in warnings)

    stored = client.get(f"{_agent_memory(aid)}/{FACT}", headers=auth_headers).json()
    assert stored["value"]["statement"] == "seeded"
    # General state and transcripts are unchanged for a legacy token.
    assert (
        client.put(f"/agents/{aid}/state/workflow/k", json={"value": 1}, headers=legacy).status_code
        == 200
    )
    assert (
        client.put(
            f"/agents/{aid}/state/transcript/thread-1", json={"value": {"n": 1}}, headers=legacy
        ).status_code
        == 200
    )


def test_namespace_listing_for_other_binding_hides_memory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The enumeration route has no namespace param. For a sandbox credential on
    # another binding (or a legacy token on any binding) it drops the memory
    # row rather than 403ing, so general state stays reachable as the ADR
    # keeps it.
    aid = _agent_with_two_channels(client, auth_headers)
    for address in (CHANNEL_A, CHANNEL_B):
        _seed(client, auth_headers, f"{_channel(aid, address)}/{FACT}", _fact_value())
        _seed(
            client,
            auth_headers,
            f"/agents/{aid}/state/bindings/slack/{address}/workflow/k",
            {"n": 1},
        )

    def names(address: str, headers: dict[str, str]) -> set[str]:
        resp = client.get(f"/agents/{aid}/state/bindings/slack/{address}", headers=headers)
        assert resp.status_code == 200, resp.text
        return {row["namespace"] for row in resp.json()}

    assert names(CHANNEL_B, _reader(aid)) == {"workflow"}
    assert names(CHANNEL_B, _writer(aid)) == {"workflow"}
    assert names(CHANNEL_A, _reader(aid, binding=None)) == {"workflow"}
    assert names(CHANNEL_A, _legacy(aid)) == {"workflow"}
    # Own binding and the platform key see memory.
    assert names(CHANNEL_A, _reader(aid)) == {"memory", "workflow"}
    assert names(CHANNEL_B, auth_headers) == {"memory", "workflow"}


def test_general_state_reach_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # ADR-0188 narrows the memory namespace only. Every sandbox state
    # credential keeps its reach on general (non-reserved) state, plain and
    # binding-scoped, including another binding's general state.
    aid = _agent_with_two_channels(client, auth_headers)
    for headers in (_reader(aid), _writer(aid), _reader(aid, binding=None), _legacy(aid)):
        for url in (
            f"/agents/{aid}/state/workflow/step",
            f"/agents/{aid}/state/bindings/slack/{CHANNEL_A}/workflow/step",
            f"/agents/{aid}/state/bindings/slack/{CHANNEL_B}/workflow/step",
        ):
            put = client.put(url, json={"value": "any json, not an object"}, headers=headers)
            assert put.status_code == 200, f"{url}: {put.text}"
            assert client.get(url, headers=headers).json()["value"] == "any json, not an object"
            assert client.get(url.rsplit("/", 1)[0], headers=headers).status_code == 200
            assert client.delete(url, headers=headers).status_code == 204
        # And guidance/log keys are free outside the memory namespace.
        g = client.put(f"/agents/{aid}/state/notes/guidance", json={"value": 1}, headers=headers)
        assert g.status_code == 200, g.text


def test_sandbox_credential_cannot_write_a_non_canonical_fact_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Review L1: the key must be exactly fact- + 32 lowercase hex. A trailing
    # newline (sent URL-encoded as %0A) slipped past a ``$``-anchored match. A
    # trailing space and uppercase hex are refused too.
    aid = _agent_with_two_channels(client, auth_headers)
    writer = _writer(aid)
    encoded_keys = (
        FACT + "%0A",
        FACT + "%20",
        "fact-" + "0123456789ABCDEF" * 2,
        "fact-" + "0" * 31 + "A",
    )
    for base in (_agent_memory(aid), _channel(aid, CHANNEL_A)):
        for key in encoded_keys:
            put = client.put(f"{base}/{key}", json={"value": _fact_value()}, headers=writer)
            assert put.status_code == 403, f"{base}/{key}: {put.status_code} {put.text}"
            assert FACT_KEYS_ONLY in _detail(put)
        listed = client.get(base, headers=auth_headers)
        stored = [] if listed.status_code == 404 else [e["key"] for e in listed.json()]
        assert stored == [], stored


def _stamped_at(value: dict[str, Any]) -> Any:
    from datetime import datetime

    return datetime.fromisoformat(str(value["stated_at"]).replace("Z", "+00:00"))


def test_sandbox_write_stamps_stated_at_from_the_server_clock(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Review L3: a sandbox write cannot backdate a fact. The server stamps
    # ``stated_at`` from its own clock; ``session_id`` stays the body's value.
    from datetime import UTC, datetime, timedelta

    aid = _agent_with_two_channels(client, auth_headers)
    url = f"{_channel(aid, CHANNEL_A)}/{FACT}"
    body = {**_fact_value(), "stated_at": "1999-01-01T00:00:00+00:00", "session_id": "s-body"}
    skew = timedelta(seconds=5)

    before = datetime.now(UTC)
    put = client.put(url, json={"value": body}, headers=_writer(aid))
    after = datetime.now(UTC)
    assert put.status_code == 200, put.text
    stored = client.get(url, headers=auth_headers).json()["value"]
    stamped = _stamped_at(stored)
    assert stamped.tzinfo is not None, stored
    assert before - skew <= stamped <= after + skew, stored
    assert stored["session_id"] == "s-body"

    # An update (with expected_version) is stamped the same way.
    again = {**body, "statement": "changed", "stated_at": "2000-01-01T00:00:00+00:00"}
    before = datetime.now(UTC)
    upd = client.put(url, json={"value": again, "expected_version": 1}, headers=_writer(aid))
    after = datetime.now(UTC)
    assert upd.status_code == 200, upd.text
    stored = client.get(url, headers=auth_headers).json()["value"]
    assert before - skew <= _stamped_at(stored) <= after + skew, stored
    assert stored["session_id"] == "s-body"


def test_platform_key_keeps_body_stated_at(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The platform key (``curie cluster memory``) writes the body as given.
    aid = _agent_with_two_channels(client, auth_headers)
    url = f"{_channel(aid, CHANNEL_A)}/{FACT}"
    body = {**_fact_value(), "stated_at": "1999-01-01T00:00:00+00:00", "session_id": "s-op"}
    put = client.put(url, json={"value": body}, headers=auth_headers)
    assert put.status_code == 200, put.text
    stored = client.get(url, headers=auth_headers).json()["value"]
    assert stored["stated_at"] == "1999-01-01T00:00:00+00:00"
    assert stored["session_id"] == "s-op"
