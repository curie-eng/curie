"""Channel read against a real Slack workspace (ADR 0100, #2877, AC7).

Opt in with CURIE_LIVE_CHANNEL_READ=1. CI never sets it. Once opted in, the
setup is required, never skipped: SLACK_BOT_TOKEN, SLACK_TEST_CHANNEL (a
channel the bot is in) and SLACK_TEST_FOREIGN_CHANNEL (a channel the bot is
NOT in). Postgres, Valkey and the object store are the compose services, as
in the rest of this suite. Nothing is faked: the API's own HTTP client talks
to slack.com.

Slack methods used directly by the test for seeding and preconditions:
https://docs.slack.dev/reference/methods/chat.postMessage
https://docs.slack.dev/reference/methods/conversations.info
https://docs.slack.dev/reference/methods/auth.test

Failure messages name what was wrong, never a channel id, a ts or a token.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient

from apps.api.tests.test_channel_read import (
    CAPABILITY_HEADER,
    MINT_URL,
    READ_URL,
    WORKER_HEADERS,
    WORKER_TOKEN,
    _bundle_archive,
    _rfc3339,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_LIVE_CHANNEL_READ") != "1",
    reason="live Slack channel read runs only with CURIE_LIVE_CHANNEL_READ=1",
)

REQUIRED = ("SLACK_BOT_TOKEN", "SLACK_TEST_CHANNEL", "SLACK_TEST_FOREIGN_CHANNEL")
UNBOUND = "C0NEVERBOUND9"


@dataclass(frozen=True)
class LiveSlack:
    token: str
    channel: str
    foreign: str
    workspace_url: str


def _slack(method: str, token: str, **params: Any) -> dict[str, Any]:
    response = httpx.post(
        f"https://slack.com/api/{method}",
        headers={"Authorization": f"Bearer {token}"},
        data=params,
        timeout=20.0,
    )
    if response.status_code != 200:
        pytest.fail(f"Slack {method} answered HTTP {response.status_code}")
    body = response.json()
    if not isinstance(body, dict):
        pytest.fail(f"Slack {method} answered a non object body")
    return body


@pytest.fixture(scope="session")
def live_slack() -> LiveSlack:
    missing = [name for name in REQUIRED if not os.environ.get(name, "").strip()]
    if missing:
        pytest.fail(
            "CURIE_LIVE_CHANNEL_READ=1 needs " + ", ".join(missing) + "; this is a setup failure"
        )
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_TEST_CHANNEL"]
    foreign = os.environ["SLACK_TEST_FOREIGN_CHANNEL"]
    if channel == foreign:
        pytest.fail("SLACK_TEST_CHANNEL and SLACK_TEST_FOREIGN_CHANNEL must differ")
    identity = _slack("auth.test", token)
    if not identity.get("ok"):
        pytest.fail(f"SLACK_BOT_TOKEN is not usable: {identity.get('error', 'unknown')}")
    info = _slack("conversations.info", token, channel=foreign)
    if info.get("ok") and info.get("channel", {}).get("is_member"):
        pytest.fail("the bot is a member of SLACK_TEST_FOREIGN_CHANNEL; it must not be")
    own = _slack("conversations.info", token, channel=channel)
    if not (own.get("ok") and own.get("channel", {}).get("is_member")):
        pytest.fail("the bot is not a member of SLACK_TEST_CHANNEL; invite it first")
    return LiveSlack(token, channel, foreign, str(identity["url"]))


@dataclass
class Seeded:
    marker: str
    parent_ts: str
    reply_ts: str
    reply_text: str


@pytest.fixture(scope="module")
def seeded(live_slack: LiveSlack) -> Seeded:
    marker = f"curie-2877-{uuid.uuid4()}"
    posted = _slack("chat.postMessage", live_slack.token, channel=live_slack.channel, text=marker)
    if not posted.get("ok"):
        pytest.fail(f"chat.postMessage failed: {posted.get('error', 'unknown')}")
    reply_text = f"{marker} reply"
    reply = _slack(
        "chat.postMessage",
        live_slack.token,
        channel=live_slack.channel,
        thread_ts=posted["ts"],
        text=reply_text,
    )
    if not reply.get("ok"):
        pytest.fail(f"threaded chat.postMessage failed: {reply.get('error', 'unknown')}")
    # Slack's history index is eventually consistent for a moment after a post.
    time.sleep(2)
    return Seeded(marker, str(posted["ts"]), str(reply["ts"]), reply_text)


@dataclass
class LiveAgent:
    client: TestClient
    agent_id: str
    deployment_id: str
    token: str


@pytest.fixture
def live_agent(
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    auth_headers: dict[str, str],
    live_slack: LiveSlack,
) -> Iterator[LiveAgent]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", live_slack.token)
    monkeypatch.setenv("RUNS_STREAM", f"test:curie:channel-read-live:{uuid.uuid4().hex}")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        agent = client.post(
            "/agents",
            json={
                "name": f"live-reader-{uuid.uuid4().hex[:8]}",
                "channel": {"kind": "slack", "address": live_slack.channel},
            },
            headers=auth_headers,
        )
        assert agent.status_code == 201, "agent create failed"
        agent_id = agent.json()["id"]
        bound = client.post(
            f"/agents/{agent_id}/channels",
            json={"kind": "slack", "address": live_slack.foreign},
            headers=auth_headers,
        )
        assert bound.status_code == 201, "binding the foreign channel failed"
        version = client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": "live", "created_by": "operator"},
            headers=auth_headers,
        )
        assert version.status_code == 201, "version create failed"
        version_id = version.json()["id"]
        upload = client.put(
            f"/agents/{agent_id}/versions/{version_id}/bundle",
            files={"file": ("reader-bot.tar.gz", _bundle_archive(True))},
            headers=auth_headers,
        )
        assert upload.status_code == 201, "bundle upload failed"
        deployment = client.post(
            "/deployments",
            json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
            headers=auth_headers,
        )
        assert deployment.status_code == 201, "deployment create failed"
        deployment_id = deployment.json()["id"]
        minted = client.post(
            MINT_URL,
            json={
                "agent_id": agent_id,
                "deployment_id": deployment_id,
                "event_id": f"live-{uuid.uuid4().hex}",
                "mode": "open",
                "owner": f"owner-{uuid.uuid4().hex}",
                "default_channel": {"kind": "slack", "address": live_slack.channel},
                "ttl_s": 3600,
            },
            headers=WORKER_HEADERS,
        )
        assert minted.status_code == 200, f"mint answered {minted.status_code}"
        yield LiveAgent(client, agent_id, deployment_id, minted.json()["token"])
    get_settings.cache_clear()


def _read(agent: LiveAgent, **body: Any) -> httpx.Response:
    return agent.client.post(READ_URL, json=body, headers={CAPABILITY_HEADER: agent.token})


def _page(response: httpx.Response) -> dict[str, Any]:
    code = response.json().get("detail", {}).get("code") if response.status_code != 200 else None
    assert response.status_code == 200, f"read answered {response.status_code} {code}"
    return dict(response.json())


def _code(response: httpx.Response) -> str | None:
    detail = response.json().get("detail")
    return detail.get("code") if isinstance(detail, dict) else None


def _around(posted_ts: str) -> dict[str, str]:
    posted = datetime.fromtimestamp(float(posted_ts), UTC)
    return {"oldest": _rfc3339(posted - timedelta(seconds=60))}


def test_live_marker_message_and_reply_are_read_back(
    live_agent: LiveAgent, seeded: Seeded, live_slack: LiveSlack
) -> None:
    history = _page(_read(live_agent, operation="history", **_around(seeded.parent_ts)))
    parents = [m for m in history["messages"] if m["text"] == seeded.marker]
    assert len(parents) == 1, "the marker message was not read back from history"
    parent = parents[0]
    # Comparisons are reduced to booleans first so a failure never prints an id.
    id_matches = parent["id"] == seeded.parent_ts
    assert id_matches, "the marker record id is not its message id"
    in_workspace = parent["provenance"].startswith(live_slack.workspace_url)
    assert in_workspace, "provenance does not point into the workspace auth.test names"
    assert all(seeded.reply_text != m["text"] for m in history["messages"]), (
        "history returned a thread reply"
    )

    thread = _page(
        _read(
            live_agent,
            operation="thread",
            thread_id=seeded.parent_ts,
            **_around(seeded.parent_ts),
        )
    )
    replies = [m for m in thread["messages"] if m["text"] == seeded.reply_text]
    assert len(replies) == 1, "the threaded reply was not read back"
    names_thread = replies[0]["thread_id"] == seeded.parent_ts
    assert names_thread, "the reply names the wrong thread"
    assert all(m["text"] != seeded.marker for m in thread["messages"]), (
        "the thread page included the parent"
    )


def test_live_reply_id_dereferences(live_agent: LiveAgent, seeded: Seeded) -> None:
    reply_id = f"{seeded.parent_ts}:{seeded.reply_ts}"
    page = _page(_read(live_agent, operation="message", message_id=reply_id))
    is_reply = [m["text"] for m in page["messages"]] == [seeded.reply_text]
    assert is_reply, "the reply id did not dereference to the reply"
    parent = _page(_read(live_agent, operation="message", message_id=seeded.parent_ts))
    is_marker = [m["text"] for m in parent["messages"]] == [seeded.marker]
    assert is_marker, "the parent id did not dereference to the marker"


def test_live_unbound_and_non_member_channels_are_refused(
    live_agent: LiveAgent, seeded: Seeded, live_slack: LiveSlack
) -> None:
    window = _around(seeded.parent_ts)
    unbound = _read(
        live_agent,
        operation="history",
        channel={"kind": "slack", "address": UNBOUND},
        **window,
    )
    assert (unbound.status_code, _code(unbound)) == (403, "channel_read.not_bound"), (
        "an unbound channel was not refused as not_bound"
    )
    foreign = _read(
        live_agent,
        operation="history",
        channel={"kind": "slack", "address": live_slack.foreign},
        **window,
    )
    assert (foreign.status_code, _code(foreign)) == (403, "channel_read.not_member"), (
        "a bound channel the bot is not in was not refused as not_member"
    )
