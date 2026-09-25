"""A named non-Slack route keeps its history across the key change (ADR-0168 decision 4).

Under the ruling for decision 4, a route whose adapter names a non-default
identity gains an identity segment in its thread key. A mail thread's
transcript written before the upgrade sits under the old key. The first read
or write under the new key adopts it, as ``transcripts._adopt_legacy`` adopts
pre-0053 rows, and leaves the old row for a worker that has not rolled.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from channel_protocol import scoped_conversation_id

ADDRESS = "agent@example.test"
ADAPTER = "agentmail-sandbox"
ENDPOINT = "http://curie-mail-adapter:8080/"
THREAD = "thread/9"
OLD_KEY = scoped_conversation_id("email", ADDRESS, THREAD)
NEW_KEY = scoped_conversation_id("email", ADDRESS, THREAD, identity=ADAPTER)
HISTORY = [{"role": "user", "content": "hello"}]


def _url(agent_id: str, key: str) -> str:
    # The worker quotes the key into the history ref the same way (binding.boot_env),
    # a single ``quote``. Starlette's TestClient (pinned 1.6.0) unquotes the path
    # twice -- once via ``httpx.URL.path``, which is already decoded, and again in
    # ``testclient.py``'s own ``scope["path"]`` build -- where a real ASGI server
    # decodes once. Quoting twice here cancels that extra decode so the key this
    # test's requests deliver matches what a real server delivers from one quote.
    return f"/agents/{agent_id}/state/transcript/{quote(quote(key, safe=''), safe='')}"


def _agent(client: Any, headers: dict[str, str], name: str, channel: dict[str, Any]) -> str:
    resp = client.post("/agents", json={"name": name, "channel": channel}, headers=headers)
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _mail_agent(client: Any, headers: dict[str, str]) -> str:
    return _agent(
        client,
        headers,
        "mail-history",
        {"kind": "email", "address": ADDRESS, "endpoint": ENDPOINT, "adapter": ADAPTER},
    )


def _seed(client: Any, headers: dict[str, str], agent_id: str, key: str, value: Any) -> None:
    put = client.put(_url(agent_id, key), json={"value": value}, headers=headers)
    assert put.status_code == 200, put.text


def test_a_named_mail_route_reads_its_pre_identity_transcript(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)

    read = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["key"] == NEW_KEY
    assert read.json()["value"] == HISTORY
    # Left in place for a worker that still builds the old key.
    kept = client.get(_url(aid, OLD_KEY), headers=auth_headers)
    assert kept.status_code == 200, kept.text
    assert kept.json()["value"] == HISTORY


def test_the_first_append_under_the_new_key_continues_the_old_history(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    reply = {"role": "assistant", "content": "hi"}

    appended = client.post(
        f"{_url(aid, NEW_KEY)}/append", json={"item": reply}, headers=auth_headers
    )
    assert appended.status_code == 200, appended.text
    assert appended.json()["value"] == [*HISTORY, reply]


def test_a_later_write_under_the_old_key_is_adopted_again(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """An old worker still serving during the rollout writes the old key."""
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    assert client.get(_url(aid, NEW_KEY), headers=auth_headers).status_code == 200
    later = [*HISTORY, {"role": "assistant", "content": "from an old worker"}]
    _seed(client, auth_headers, aid, OLD_KEY, later)

    read = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["value"] == later


def test_another_identity_on_the_pair_never_adopts_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    other = scoped_conversation_id("email", ADDRESS, THREAD, identity="other-inbox")

    assert client.get(_url(aid, other), headers=auth_headers).status_code == 404


def test_another_agents_route_never_adopts_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    owner = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, owner, OLD_KEY, HISTORY)
    stranger = _agent(client, auth_headers, "stranger", {"kind": "slack", "address": "C0EXAMPLE2"})

    assert client.get(_url(stranger, NEW_KEY), headers=auth_headers).status_code == 404


def test_a_named_slack_identity_never_adopts_the_default_apps_history(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A named Slack key never existed before the ADR, so it has no old form."""
    aid = _agent(client, auth_headers, "slack-history", {"kind": "slack", "address": "C0EXAMPLE1"})
    default_key = scoped_conversation_id("slack", "C0EXAMPLE1", "1700000000.000100")
    named_key = scoped_conversation_id(
        "slack", "C0EXAMPLE1", "1700000000.000100", identity="second-bot"
    )
    _seed(client, auth_headers, aid, default_key, HISTORY)

    assert client.get(_url(aid, named_key), headers=auth_headers).status_code == 404
