"""A named non-Slack route keeps its history across the key change (ADR-0168 decision 4).

Under ADR-0168 decision 4, a route whose adapter names a non-default
identity gains an identity segment in its thread key. A mail thread's
transcript written before the upgrade sits under the old key. The first read
or write under the new key adopts it, as ``transcripts._adopt_legacy`` adopts
pre-0053 rows, and leaves the old row for a worker that has not rolled.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from urllib.parse import quote

from channel_protocol import scoped_conversation_id
from curie_api.config import get_settings
from curie_api.threadkeys import pre_identity_thread_key_for
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

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


def _sql(statement: str, **params: Any) -> None:
    # Reaches under the API for a row no HTTP route writes directly: an
    # already-expired transcript, or a pre-0053 legacy row with no
    # ``thread_transcripts`` counterpart yet.
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(run())


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
    first = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert first.status_code == 200
    later = [*HISTORY, {"role": "assistant", "content": "from an old worker"}]
    _seed(client, auth_headers, aid, OLD_KEY, later)

    read = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["value"] == later
    # The re-adoption is itself a write. A runner still holding the version
    # from before it must not be able to compare-and-set over the history the
    # old worker just wrote, so the version must move, and then hold still
    # until the next real change.
    assert read.json()["version"] > first.json()["version"]
    again = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert again.json()["version"] == read.json()["version"]


def test_a_stale_expected_version_after_readoption_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Without the version bump on re-adoption, a compare-and-set holding the
    pre-readoption version would overwrite the history the old worker just
    wrote back in, instead of conflicting."""
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    first = client.get(_url(aid, NEW_KEY), headers=auth_headers).json()
    later = [*HISTORY, {"role": "assistant", "content": "from an old worker"}]
    _seed(client, auth_headers, aid, OLD_KEY, later)

    stale = client.put(
        _url(aid, NEW_KEY),
        json={
            "value": [*HISTORY, {"role": "user", "content": "stale"}],
            "expected_version": first["version"],
        },
        headers=auth_headers,
    )
    assert stale.status_code == 409, stale.text


def test_an_unbound_identity_on_the_pair_never_adopts_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """This covers an identity with NO binding on the pair. A second BOUND
    identity on the pair cannot exist until #3100 (migration 0023 holds the
    pair to one row); that path is pinned instead by the stub unit test on
    ``pre_identity_thread_key_for`` below."""
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    other = scoped_conversation_id("email", ADDRESS, THREAD, identity="other-inbox")

    assert client.get(_url(aid, other), headers=auth_headers).status_code == 404


def test_another_agents_route_never_adopts_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """OLD_KEY is seeded under the STRANGER itself, not the owner. Seeding it
    under the owner leaves the stranger with nothing to adopt from either
    way, so dropping the agent-id filter in ``pre_identity_thread_key_for``
    would go unnoticed; seeding it here means only that filter stands
    between the stranger and its neighbor's binding."""
    _mail_agent(client, auth_headers)
    stranger = _agent(client, auth_headers, "stranger", {"kind": "slack", "address": "C0EXAMPLE2"})
    _seed(client, auth_headers, stranger, OLD_KEY, HISTORY)

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


def test_a_deleted_transcript_is_not_readopted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A delete under the new key must end the thread, not merely hide it
    behind the pre-identity row adoption keeps copying back from. Before the
    fix, the old row outlived the delete and the next read resurrected it."""
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    assert client.get(_url(aid, NEW_KEY), headers=auth_headers).status_code == 200

    deleted = client.delete(_url(aid, NEW_KEY), headers=auth_headers)
    assert deleted.status_code == 204, deleted.text

    read_again = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert read_again.status_code == 404, read_again.text


def test_an_expired_old_row_is_not_adopted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The old row's own TTL has to be honored the same way ``_adopt_legacy``
    honors it, or a thread whose history the TTL already ended comes back
    from a row nothing else is reading anymore."""
    aid = _mail_agent(client, auth_headers)
    _seed(client, auth_headers, aid, OLD_KEY, HISTORY)
    _sql(
        "UPDATE curie.thread_transcripts SET expires_at = now() - interval '1 hour' "
        "WHERE thread_key = :k",
        k=OLD_KEY,
    )

    assert client.get(_url(aid, NEW_KEY), headers=auth_headers).status_code == 404


def test_a_pre_0053_row_under_the_old_key_is_adopted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Only a pre-0053 ``workflow_state_entries`` row sits under OLD_KEY here
    -- no ``thread_transcripts`` counterpart yet -- so reading NEW_KEY must
    chain through ``_adopt_legacy`` on the old key before there is anything
    to adopt from it."""
    aid = _mail_agent(client, auth_headers)
    _sql(
        "INSERT INTO curie.workflow_state_entries "
        "(id, agent_id, binding_scope, namespace, key, value, version) "
        "VALUES (:id, :a, NULL, 'transcript', :k, CAST(:v AS jsonb), 1)",
        id=uuid.uuid4(),
        a=uuid.UUID(aid),
        k=OLD_KEY,
        v=json.dumps(HISTORY),
    )

    read = client.get(_url(aid, NEW_KEY), headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["value"] == HISTORY


def test_pre_identity_thread_key_for_refuses_a_second_binding_on_the_pair() -> None:
    """Pins the ``==`` (not ``in``) in ``pre_identity_thread_key_for``.
    Migration 0023 holds ``(kind, address)`` to one row per agent until
    #3100, so a second BOUND identity on the pair cannot be seeded through
    the HTTP API today -- a stub session is enough here, because only the
    list comparison is under test."""

    class _StubAdapters:
        async def scalars(self, query: Any) -> list[str | None]:
            del query
            return [ADAPTER, "other-inbox"]

    result = asyncio.run(
        pre_identity_thread_key_for(_StubAdapters(), uuid.uuid4(), NEW_KEY)  # type: ignore[arg-type]
    )
    assert result is None
