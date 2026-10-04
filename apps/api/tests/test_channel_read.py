"""The agent's bounded read of its own channel (ADR 0100, #2877).

Everything here drives the real app: agents, versions, bundles, deployments and
bindings are created through the real routes, Postgres, Valkey and the object
store are the compose services, and the capability is minted by the internal
route the worker calls. Only Slack is replaced, and only at the HTTP seam,
with response shapes taken from Slack's own reference pages:

conversations.history
https://docs.slack.dev/reference/methods/conversations.history
(newest first, `has_more`, `response_metadata.next_cursor`, `oldest`/`latest`
with the `inclusive` flag applying to both ends, `not_in_channel`,
`channel_not_found`; bot messages carry `bot_id` and `subtype: bot_message`
instead of `user`; a thread parent carries `thread_ts` and `reply_count`).

conversations.replies
https://docs.slack.dev/reference/methods/conversations.replies
(the parent is the first element, every reply carries `thread_ts`, an unknown
thread in the named channel is `thread_not_found`).

auth.test
https://docs.slack.dev/reference/methods/auth.test
(`url` is the workspace URL permalinks are built from).

Rate limits
https://docs.slack.dev/apis/web-api/rate-limits
(HTTP 429 with a `Retry-After` header and `{"ok": false, "error": "ratelimited"}`).
"""

from __future__ import annotations

import asyncio
import base64
import calendar
import hashlib
import hmac
import io
import json
import logging
import tarfile
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest
import redis.asyncio as aioredis
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

MINT_URL = "/v1/internal/channel-read/context"
READ_URL = "/channel-read"
CAPABILITY_HEADER = "X-Curie-Channel-Read"
WORKER_TOKEN = "channel-read-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
BOT_TOKEN = "xoxb-channel-read-fixture"
WORKSPACE_URL = "https://example.slack.com/"

CHAN_A = "C0EXAMPLE1"
CHAN_B = "C0EXAMPLE2"
CHAN_UNBOUND = "C0EXAMPLE3"
CHAN_OTHER_AGENT = "C0EXAMPLE4"
CHAN_NOT_MEMBER = "C0EXAMPLE5"
EMAIL = "ops@example.test"

TODAY = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
YESTERDAY = TODAY - timedelta(days=1)


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _ts(moment: datetime) -> str:
    """The Slack `ts` string for a moment: epoch seconds, a dot, six digits."""

    return f"{calendar.timegm(moment.utctimetuple())}.{moment.microsecond:06d}"


def _moment(slack_ts: str) -> datetime:
    seconds, _, fraction = slack_ts.partition(".")
    micros = int((fraction + "000000")[:6])
    return datetime.fromtimestamp(int(seconds), UTC) + timedelta(microseconds=micros)


# The seeded surface. Parents in channel A yesterday, one thread with two
# replies, one bot message, one message at exactly midnight (the exclusive end
# of yesterday's window) and one just before the window opens.
PARENT_USER = _ts(YESTERDAY + timedelta(hours=9))
PARENT_BOT = _ts(YESTERDAY + timedelta(hours=12))
THREAD_A = _ts(YESTERDAY + timedelta(hours=15))
REPLY_A1 = _ts(YESTERDAY + timedelta(hours=15, minutes=5))
REPLY_A2 = _ts(YESTERDAY + timedelta(hours=15, minutes=10))
AT_MIDNIGHT = _ts(TODAY)
BEFORE_WINDOW = _ts(YESTERDAY - timedelta(seconds=1))
THREAD_B = _ts(YESTERDAY + timedelta(hours=10))
REPLY_B1 = _ts(YESTERDAY + timedelta(hours=10, minutes=1))
SECRET_TEXT = "the release decision was to hold 0.13 until Friday"


# --------------------------------------------------------------------------- #
# Fake Slack, at the HTTP seam only
# --------------------------------------------------------------------------- #


def _params(request: httpx.Request) -> dict[str, str]:
    """Slack accepts arguments as a query string or a form body."""

    merged = dict(request.url.params)
    content = request.content.decode() if request.content else ""
    if content:
        if request.headers.get("content-type", "").startswith("application/json"):
            merged.update({k: str(v) for k, v in json.loads(content).items()})
        else:
            merged.update(dict(parse_qsl(content)))
    return merged


def _encode_cursor(offset: int) -> str:
    return base64.b64encode(f"next_ts:{offset}".encode()).decode()


def _decode_cursor(cursor: str) -> int:
    return int(base64.b64decode(cursor).decode().split(":", 1)[1])


@dataclass
class FakeSlack:
    token: str = BOT_TOKEN
    # channel -> top level messages exactly as conversations.history returns them
    channels: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # (channel, thread_ts) -> replies exactly as conversations.replies returns them
    threads: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)
    members: set[str] = field(default_factory=set)
    requests: list[httpx.Request] = field(default_factory=list)
    # method -> responses to return instead of the modeled answer, in order
    scripted: dict[str, list[httpx.Response]] = field(default_factory=dict)
    # method -> a transform applied to the modeled JSON body
    tamper: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = field(default_factory=dict)
    # Time based pagination: `has_more` without `response_metadata.next_cursor`.
    # conversations.history documents paging by moving `latest` to the oldest
    # `ts` already seen when no cursor is offered; such a page ignores `cursor`.
    time_paginated: bool = False
    # conversations.replies counts the parent toward `limit`: the parent is
    # always the first element, so `limit=1` returns the parent alone with
    # `has_more` true (https://docs.slack.dev/reference/methods/conversations.replies).
    # Paired with time_paginated there is no provider cursor to follow.
    replies_parent_in_limit: bool = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        method = request.url.path.rsplit("/", 1)[-1]
        queued = self.scripted.get(method)
        if queued:
            return queued.pop(0)
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(200, json={"ok": False, "error": "invalid_auth"})
        params = _params(request)
        if method == "auth.test":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "url": WORKSPACE_URL,
                    "team": "Example",
                    "user": "curie",
                    "team_id": "T0EXAMPLE1",
                    "user_id": "U0CURIEBOT",
                    "bot_id": "B0CURIEBOT",
                },
            )
        channel = params.get("channel", "")
        if channel not in self.channels:
            return httpx.Response(200, json={"ok": False, "error": "channel_not_found"})
        if channel not in self.members:
            return httpx.Response(200, json={"ok": False, "error": "not_in_channel"})
        if method == "conversations.history":
            body = self._page(
                sorted(self.channels[channel], key=lambda m: Decimal(m["ts"]), reverse=True), params
            )
            if self.time_paginated:
                body = self._page(
                    sorted(self.channels[channel], key=lambda m: Decimal(m["ts"]), reverse=True),
                    {k: v for k, v in params.items() if k != "cursor"},
                )
                body.pop("response_metadata")
        elif method == "conversations.replies":
            thread_ts = params.get("ts", "")
            parent = next((m for m in self.channels[channel] if m["ts"] == thread_ts), None)
            if parent is None:
                return httpx.Response(200, json={"ok": False, "error": "thread_not_found"})
            replies = sorted(
                self.threads.get((channel, thread_ts), []), key=lambda m: Decimal(m["ts"])
            )
            if self.replies_parent_in_limit:
                limit = int(params.get("limit") or 1000)
                window = self._page(replies, {**params, "limit": "1000", "cursor": ""})["messages"]
                start = (
                    _decode_cursor(params["cursor"])
                    if params.get("cursor") and not self.time_paginated
                    else 0
                )
                taken = window[start : start + max(limit - 1, 0)]
                more = start + len(taken) < len(window)
                body = {"ok": True, "messages": [parent, *taken], "has_more": more}
                if not self.time_paginated:
                    body["response_metadata"] = {
                        "next_cursor": _encode_cursor(start + len(taken)) if more else ""
                    }
            else:
                body = self._page(replies, params)
                # Slack always leads a replies page with the thread parent.
                body["messages"] = [parent, *body["messages"]]
        else:
            return httpx.Response(200, json={"ok": False, "error": "unknown_method"})
        transform = self.tamper.get(method)
        return httpx.Response(200, json=transform(body) if transform else body)

    @staticmethod
    def _page(messages: list[dict[str, Any]], params: dict[str, str]) -> dict[str, Any]:
        inclusive = params.get("inclusive", "").lower() in {"true", "1"}
        oldest = Decimal(params.get("oldest") or "0")
        latest = Decimal(params["latest"]) if params.get("latest") else None

        def inside(ts: str) -> bool:
            value = Decimal(ts)
            low = value >= oldest if inclusive else value > oldest
            high = latest is None or (value <= latest if inclusive else value < latest)
            return low and high

        window = [m for m in messages if inside(m["ts"])]
        limit = int(params.get("limit") or 100)
        start = _decode_cursor(params["cursor"]) if params.get("cursor") else 0
        page = window[start : start + limit]
        more = start + limit < len(window)
        return {
            "ok": True,
            "messages": page,
            "has_more": more,
            "pin_count": 0,
            "response_metadata": {"next_cursor": _encode_cursor(start + limit) if more else ""},
        }

    def calls(self, method: str | None = None) -> list[httpx.Request]:
        return [r for r in self.requests if method is None or r.url.path.endswith("/" + method)]

    def reads(self) -> list[httpx.Request]:
        """Requests that read conversation content (auth.test excluded)."""

        return [r for r in self.requests if "/conversations." in r.url.path]

    def fail_next(self, method: str, response: httpx.Response) -> None:
        self.scripted.setdefault(method, []).append(response)


def _user(ts: str, text: str, user: str = "U0ALICE001") -> dict[str, Any]:
    return {"type": "message", "user": user, "text": text, "ts": ts, "team": "T0EXAMPLE1"}


def _seeded_slack() -> FakeSlack:
    slack = FakeSlack()
    parent_a = _user(THREAD_A, "should we ship 0.13 this week?")
    parent_a.update({"thread_ts": THREAD_A, "reply_count": 2, "latest_reply": REPLY_A2})
    parent_b = _user(THREAD_B, "channel B planning thread")
    parent_b.update({"thread_ts": THREAD_B, "reply_count": 1, "latest_reply": REPLY_B1})
    slack.channels = {
        CHAN_A: [
            _user(BEFORE_WINDOW, "too early"),
            _user(PARENT_USER, SECRET_TEXT),
            {
                "type": "message",
                "subtype": "bot_message",
                "bot_id": "B0DEPLOYER",
                "text": "deploy finished",
                "ts": PARENT_BOT,
            },
            parent_a,
            _user(AT_MIDNIGHT, "exactly at the exclusive end"),
        ],
        CHAN_B: [parent_b],
        CHAN_NOT_MEMBER: [_user(PARENT_USER, "the bot was never invited here")],
        CHAN_OTHER_AGENT: [_user(PARENT_USER, "another agent's channel")],
        CHAN_UNBOUND: [_user(PARENT_USER, "nobody bound this")],
    }
    slack.threads = {
        (CHAN_A, THREAD_A): [
            {
                **_user(REPLY_A1, "no, hold until Friday", "U0BOB00002"),
                "thread_ts": THREAD_A,
                "parent_user_id": "U0ALICE001",
            },
            {
                **_user(REPLY_A2, "agreed", "U0CAROL003"),
                "thread_ts": THREAD_A,
                "parent_user_id": "U0ALICE001",
            },
        ],
        (CHAN_B, THREAD_B): [
            {
                **_user(REPLY_B1, "reply in B", "U0BOB00002"),
                "thread_ts": THREAD_B,
                "parent_user_id": "U0ALICE001",
            },
        ],
    }
    # The bot is a member of every seeded channel except CHAN_NOT_MEMBER.
    slack.members = {CHAN_A, CHAN_B, CHAN_OTHER_AGENT, CHAN_UNBOUND}
    return slack


# --------------------------------------------------------------------------- #
# Real stack
# --------------------------------------------------------------------------- #


def _bundle_archive(grant: bool | None) -> bytes:
    manifest: dict[str, Any] = {"name": "reader-bot", "version": "0.1.0", "description": "t"}
    if grant is not None:
        manifest["channelRead"] = grant
    files = {
        "reader-bot/.claude-plugin/plugin.json": json.dumps(manifest).encode(),
        "reader-bot/skills/reader-bot/SKILL.md": (
            b"---\nname: reader-bot\ndescription: t\n---\nhi\n"
        ),
    }
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _execute(query: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(query), params or {})
        finally:
            await engine.dispose()

    asyncio.run(run())


def _valkey_run(work: Callable[[aioredis.Redis], Any]) -> Any:
    async def run() -> Any:
        client = aioredis.from_url(get_settings().valkey_dsn())
        try:
            return await work(client)
        finally:
            await client.aclose()

    return asyncio.run(run())


def _prefix() -> str:
    return get_settings().worker_key_prefix


def _ledger_keys_of(agent_id: str) -> list[str]:
    async def scan(client: aioredis.Redis) -> list[str]:
        found = []
        async for key in client.scan_iter(match=f"{_prefix()}:channel-read:*{agent_id}*"):
            found.append(key.decode() if isinstance(key, bytes) else key)
        return found

    return _valkey_run(scan)


@dataclass
class Deployed:
    agent_id: str
    version_id: str
    deployment_id: str


class Stack:
    def __init__(self, auth_headers: dict[str, str]) -> None:
        self.auth = auth_headers
        self.slack = _seeded_slack()
        self.client: TestClient | None = None
        self._http: httpx.AsyncClient | None = None
        self.agents: list[str] = []

    # Lifecycle. `restart` models the API process going away and coming back.
    def start(self) -> None:
        self.client = TestClient(create_app())
        self.client.__enter__()
        self._http = httpx.AsyncClient(transport=httpx.MockTransport(self.slack.handler))
        self.client.app.state.http_client = self._http

    def stop(self) -> None:
        assert self.client is not None and self._http is not None
        self.client.__exit__(None, None, None)
        asyncio.run(self._http.aclose())
        self.client = None

    def restart(self, while_down: Callable[[], None]) -> None:
        self.stop()
        while_down()
        self.start()

    @property
    def http(self) -> TestClient:
        assert self.client is not None
        return self.client

    # Setup through the real routes.
    def deploy(
        self,
        *,
        grant: bool | None = True,
        channel: dict[str, str] | None = None,
        extra: tuple[dict[str, str], ...] = (),
    ) -> Deployed:
        agent = self.http.post(
            "/agents",
            json={
                "name": f"reader-{uuid.uuid4().hex[:8]}",
                "channel": channel or {"kind": "slack", "address": CHAN_A},
            },
            headers=self.auth,
        )
        assert agent.status_code == 201, agent.text
        agent_id = agent.json()["id"]
        self.agents.append(agent_id)
        for binding in extra:
            self.bind(agent_id, binding)
        version_id = self.version(agent_id, grant)
        return Deployed(agent_id, version_id, self.deployment(agent_id, version_id))

    def version(self, agent_id: str, grant: bool | None) -> str:
        version = self.http.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "operator"},
            headers=self.auth,
        )
        assert version.status_code == 201, version.text
        version_id = version.json()["id"]
        upload = self.http.put(
            f"/agents/{agent_id}/versions/{version_id}/bundle",
            files={"file": ("reader-bot.tar.gz", _bundle_archive(grant))},
            headers=self.auth,
        )
        assert upload.status_code == 201, upload.text
        return str(version_id)

    def deployment(self, agent_id: str, version_id: str) -> str:
        deployment = self.http.post(
            "/deployments",
            json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
            headers=self.auth,
        )
        assert deployment.status_code == 201, deployment.text
        return str(deployment.json()["id"])

    def bind(self, agent_id: str, binding: dict[str, str]) -> None:
        response = self.http.post(f"/agents/{agent_id}/channels", json=binding, headers=self.auth)
        assert response.status_code == 201, response.text

    # The two routes under test.
    def mint_response(
        self,
        deployed: Deployed,
        *,
        event_id: str,
        mode: str = "open",
        owner: str | None = None,
        default: dict[str, str] | None = None,
        no_default: bool = False,
        headers: dict[str, str] | None = None,
        **extra: Any,
    ) -> httpx.Response:
        body: dict[str, Any] = {
            "agent_id": deployed.agent_id,
            "deployment_id": deployed.deployment_id,
            "event_id": event_id,
            "mode": mode,
            "default_channel": None
            if no_default
            else (default or {"kind": "slack", "address": CHAN_A}),
            "ttl_s": 3600,
            **extra,
        }
        if mode == "open" and owner is None:
            owner = f"owner-{uuid.uuid4().hex}"
        if owner is not None:
            body["owner"] = owner
        return self.http.post(
            MINT_URL, json=body, headers=WORKER_HEADERS if headers is None else headers
        )

    def mint(self, deployed: Deployed, **kwargs: Any) -> dict[str, Any]:
        response = self.mint_response(deployed, **kwargs)
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        context = response.json()
        assert context["token"].startswith("chr.")
        return dict(context)

    def read(self, token: str | None, **body: Any) -> httpx.Response:
        headers = {} if token is None else {CAPABILITY_HEADER: token}
        return self.http.post(READ_URL, json=body, headers=headers)


@pytest.fixture
def stack(
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    auth_headers: dict[str, str],
    request: pytest.FixtureRequest,
) -> Iterator[Stack]:
    env = {
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "SLACK_BOT_TOKEN": BOT_TOKEN,
        "RUNS_STREAM": f"test:curie:channel-read-runs:{uuid.uuid4().hex}",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "RESUME_RECONCILER_ENABLED": "false",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        **getattr(request, "param", {}),
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    built = Stack(auth_headers)
    built.start()
    try:
        yield built
    finally:
        if built.client is not None:
            built.stop()
        for agent_id in built.agents:
            keys = _ledger_keys_of(agent_id)
            if keys:
                _valkey_run(lambda client, keys=keys: client.delete(*keys))
        get_settings.cache_clear()


# --------------------------------------------------------------------------- #
# Small assertion helpers
# --------------------------------------------------------------------------- #


def _refused(response: httpx.Response, status: int, code: str) -> dict[str, Any]:
    assert response.status_code == status, response.text
    assert response.headers["cache-control"] == "no-store"
    detail = response.json()["detail"]
    assert detail["code"] == code
    return dict(detail)


def _ok(response: httpx.Response) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    return dict(response.json())


def _yesterday(**extra: Any) -> dict[str, Any]:
    return {
        "operation": "history",
        "oldest": _rfc3339(YESTERDAY),
        "latest": _rfc3339(TODAY),
        **extra,
    }


def _thread(thread_id: str = THREAD_A, **extra: Any) -> dict[str, Any]:
    return {
        "operation": "thread",
        "thread_id": thread_id,
        "oldest": _rfc3339(YESTERDAY),
        "latest": _rfc3339(TODAY),
        **extra,
    }


def _message(message_id: str, **extra: Any) -> dict[str, Any]:
    return {"operation": "message", "message_id": message_id, **extra}


def _event() -> str:
    return f"evt-{uuid.uuid4().hex}"


def _spend(stack: Stack, token: str, pages: int) -> None:
    for _ in range(pages):
        _ok(stack.read(token, **_yesterday()))


def _resign(token: str, **changes: Any) -> str:
    """A validly signed `chr` capability with changed claims, as a key holder would forge."""

    from curie_internal import sandbox_token

    prefix, payload, _ = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    encoded = (
        base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )
    signed = f"{prefix}.{encoded}"
    return f"{signed}.{sandbox_token.signature(get_settings().api_key, signed)}"


def _insert_approval(agent_id: str, dedupe_key: str) -> str:
    approval_id = uuid.uuid4()
    _execute(
        "INSERT INTO curie.approvals (id, agent_id, conversation_id, author, summary, "
        "reply_kind, reply_channel, reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :agent, :conv, 'U0ALICE001', 'merge it', 'slack', :ch, 'p-1', "
        ":dedupe, 'approved')",
        {
            "id": approval_id,
            "agent": uuid.UUID(agent_id),
            "conv": f"th-{approval_id.hex[:8]}",
            "ch": CHAN_A,
            "dedupe": dedupe_key,
        },
    )
    return f"approval-{approval_id}-resolved"


def _revoke_owner(agent_id: str, turn_key: str, owner: str) -> bool:
    """The worker's terminal revoke: a direct owner checked delete in the shared Valkey."""

    from curie_internal.channel_read_ledger import revoke_owner

    return bool(
        _valkey_run(
            lambda client: revoke_owner(client, _prefix(), uuid.UUID(agent_id), turn_key, owner)
        )
    )


# --------------------------------------------------------------------------- #
# Mint
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("grant", [None, False])
def test_mint_refuses_a_bundle_without_the_grant(stack: Stack, grant: bool | None) -> None:
    deployed = stack.deploy(grant=grant)
    response = stack.mint_response(deployed, event_id=_event())
    _refused(response, 409, "channel_read.grant_absent")
    assert "token" not in response.text
    assert _ledger_keys_of(deployed.agent_id) == []
    # Liveness: the same agent on a granted version mints.
    granted = stack.version(deployed.agent_id, True)
    live = Deployed(deployed.agent_id, granted, stack.deployment(deployed.agent_id, granted))
    stack.mint(live, event_id=_event())


def test_mint_refuses_a_stopped_or_foreign_deployment(stack: Stack) -> None:
    mine = stack.deploy()
    other = stack.deploy(channel={"kind": "slack", "address": CHAN_OTHER_AGENT})
    foreign = Deployed(mine.agent_id, mine.version_id, other.deployment_id)
    _refused(
        stack.mint_response(foreign, event_id=_event()), 409, "channel_read.deployment_inactive"
    )
    stack.mint(mine, event_id=_event())
    assert (
        stack.http.delete(f"/deployments/{mine.deployment_id}", headers=stack.auth).status_code
        == 204
    )
    _refused(stack.mint_response(mine, event_id=_event()), 409, "channel_read.deployment_inactive")
    assert stack.slack.requests == []


def test_mint_requires_the_worker_token(stack: Stack) -> None:
    deployed = stack.deploy()
    for headers in ({}, stack.auth, {"X-Curie-Worker-Token": "not-the-token"}):
        response = stack.mint_response(deployed, event_id=_event(), headers=headers)
        assert response.status_code == 401, response.text
    assert _ledger_keys_of(deployed.agent_id) == []
    stack.mint(deployed, event_id=_event())


def test_mint_owner_is_required_to_open_and_forbidden_to_steer(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    body = {
        "agent_id": deployed.agent_id,
        "deployment_id": deployed.deployment_id,
        "event_id": event,
        "mode": "open",
        "default_channel": {"kind": "slack", "address": CHAN_A},
        "ttl_s": 3600,
    }
    assert stack.http.post(MINT_URL, json=body, headers=WORKER_HEADERS).status_code == 422
    stack.mint(deployed, event_id=event)
    steer = stack.mint_response(
        deployed, event_id=event, mode="steer", owner=f"owner-{uuid.uuid4().hex}"
    )
    assert steer.status_code == 422, steer.text


# --------------------------------------------------------------------------- #
# Reads that succeed
# --------------------------------------------------------------------------- #


def test_granted_bound_member_reads_yesterdays_history(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    page = _ok(stack.read(token, **_yesterday()))

    by_id = {record["id"]: record for record in page["messages"]}
    # [oldest, latest): the message at exactly midnight and the one before the
    # window are both excluded.
    assert set(by_id) == {PARENT_USER, PARENT_BOT, THREAD_A}
    assert page["has_more"] is False
    user = by_id[PARENT_USER]
    assert user["text"] == SECRET_TEXT
    assert user["author"] == "U0ALICE001"
    assert user["truncated"] is False
    assert datetime.fromisoformat(user["timestamp"].replace("Z", "+00:00")) == _moment(PARENT_USER)
    assert user["timestamp"].endswith(("Z", "+00:00"))
    assert user["provenance"] == f"{WORKSPACE_URL}archives/{CHAN_A}/p{PARENT_USER.replace('.', '')}"
    # A bot message is authored by its bot id.
    assert by_id[PARENT_BOT]["author"] == "B0DEPLOYER"
    assert by_id[THREAD_A]["reply_count"] == 2
    allowed = {
        "id",
        "thread_id",
        "timestamp",
        "author",
        "text",
        "truncated",
        "provenance",
        "reply_count",
    }
    for record in page["messages"]:
        assert set(record) <= allowed, "no Slack field leaks into the channel neutral record"

    (call,) = stack.slack.calls("conversations.history")
    params = _params(call)
    assert call.headers["authorization"] == f"Bearer {BOT_TOKEN}"
    assert params["channel"] == CHAN_A
    assert Decimal(params["oldest"]) == Decimal(_ts(YESTERDAY))
    assert Decimal(params["latest"]) == Decimal(_ts(TODAY))
    assert params["inclusive"].lower() in {"true", "1"}
    assert params["limit"] == "50"


def test_thread_replies_exclude_the_parent_and_stay_in_the_window(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    page = _ok(stack.read(token, **_thread()))
    ids = [record["id"] for record in page["messages"]]
    assert sorted(ids) == sorted([f"{THREAD_A}:{REPLY_A1}", f"{THREAD_A}:{REPLY_A2}"])
    assert all(record["thread_id"] == THREAD_A for record in page["messages"])
    reply = next(r for r in page["messages"] if r["id"] == f"{THREAD_A}:{REPLY_A1}")
    assert reply["provenance"] == (
        f"{WORKSPACE_URL}archives/{CHAN_A}/p{REPLY_A1.replace('.', '')}"
        f"?thread_ts={THREAD_A}&cid={CHAN_A}"
    )
    (call,) = stack.slack.calls("conversations.replies")
    assert _params(call)["ts"] == THREAD_A
    assert _params(call)["channel"] == CHAN_A

    # A window that closes before the replies returns none of them.
    early = _ok(
        stack.read(
            token,
            **_thread(latest=_rfc3339(_moment(REPLY_A1) - timedelta(minutes=1))),
        )
    )
    assert early["messages"] == []


def test_empty_history_page_is_success(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    quiet = TODAY - timedelta(days=5)
    page = _ok(
        stack.read(
            token,
            operation="history",
            oldest=_rfc3339(quiet),
            latest=_rfc3339(quiet + timedelta(hours=1)),
        )
    )
    assert page["messages"] == []
    assert page["has_more"] is False


def test_message_by_id_and_missing_message(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    hit = _ok(stack.read(token, **_message(PARENT_USER)))
    assert [record["id"] for record in hit["messages"]] == [PARENT_USER]
    assert hit["messages"][0]["text"] == SECRET_TEXT
    # A well formed id that is not in the channel is an error, unlike an empty page.
    missing = _ts(YESTERDAY + timedelta(hours=11, seconds=7))
    _refused(stack.read(token, **_message(missing)), 404, "channel_read.message_not_found")


def test_reply_ids_dereference_through_message(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    reply_id = f"{THREAD_A}:{REPLY_A2}"
    listed = _ok(stack.read(token, **_thread()))
    assert reply_id in [record["id"] for record in listed["messages"]]

    # History omits replies, so only the thread form of the id can reach one.
    history = _ok(stack.read(token, **_yesterday()))
    assert reply_id not in [record["id"] for record in history["messages"]]

    stack.slack.requests.clear()
    page = _ok(stack.read(token, **_message(reply_id)))
    assert [record["id"] for record in page["messages"]] == [reply_id]
    assert page["messages"][0]["text"] == "agreed"
    replies = stack.slack.calls("conversations.replies")
    assert replies, "a reply id is looked up inside its thread"
    params = _params(replies[0])
    assert params["channel"] == CHAN_A
    assert params["ts"] == THREAD_A
    assert Decimal(params["oldest"]) == Decimal(params["latest"]) == Decimal(REPLY_A2)

    # A reply ts that is not in that thread.
    stray = f"{THREAD_A}:{_ts(YESTERDAY + timedelta(hours=15, minutes=7))}"
    _refused(stack.read(token, **_message(stray)), 404, "channel_read.message_not_found")

    # A parent id goes to history.
    stack.slack.requests.clear()
    _ok(stack.read(token, **_message(THREAD_A)))
    assert stack.slack.calls("conversations.history")
    assert not stack.slack.calls("conversations.replies")


def test_long_text_is_truncated_with_a_marker(stack: Stack) -> None:
    long_ts = _ts(YESTERDAY + timedelta(hours=20))
    long_text = "x" * 3990 + "y" * 1010
    stack.slack.channels[CHAN_A].append(_user(long_ts, long_text))
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    page = _ok(stack.read(token, **_message(long_ts)))
    (record,) = page["messages"]
    assert record["truncated"] is True
    assert record["text"].endswith("[truncated]")
    assert record["text"].startswith("x" * 3990)
    assert len(record["text"]) <= 4000 + len(" [truncated]")
    assert "y" * 1000 not in record["text"]
    short = _ok(stack.read(token, **_message(PARENT_USER)))["messages"][0]
    assert short["truncated"] is False


# --------------------------------------------------------------------------- #
# Which channel: binding, kind, membership
# --------------------------------------------------------------------------- #


def test_unbound_channel_is_refused_before_slack(stack: Stack) -> None:
    deployed = stack.deploy()
    stack.deploy(channel={"kind": "slack", "address": CHAN_OTHER_AGENT})
    token = stack.mint(deployed, event_id=_event())["token"]
    for address in (CHAN_UNBOUND, CHAN_OTHER_AGENT):
        response = stack.read(token, **_yesterday(channel={"kind": "slack", "address": address}))
        _refused(response, 403, "channel_read.not_bound")
    # The right address under the wrong kind is not this binding either.
    _refused(
        stack.read(token, **_yesterday(channel={"kind": "email", "address": CHAN_A})),
        403,
        "channel_read.not_bound",
    )
    assert stack.slack.requests == []
    # Liveness: the bound channel named explicitly reads.
    _ok(stack.read(token, **_yesterday(channel={"kind": "slack", "address": CHAN_A})))


def test_a_channel_named_without_kind_or_no_default_is_refused(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    _refused(
        stack.read(token, **_yesterday(channel={"address": CHAN_A})),
        400,
        "channel_read.channel_required",
    )
    no_default = stack.mint(deployed, event_id=_event(), no_default=True)["token"]
    _refused(stack.read(no_default, **_yesterday()), 400, "channel_read.channel_required")
    assert stack.slack.requests == []
    # Liveness: a turn with no default that names a bound pair reads.
    _ok(stack.read(no_default, **_yesterday(channel={"kind": "slack", "address": CHAN_A})))


def test_a_multi_binding_agent_reads_its_second_binding_by_name(stack: Stack) -> None:
    deployed = stack.deploy(extra=({"kind": "slack", "address": CHAN_B},))
    token = stack.mint(deployed, event_id=_event())["token"]
    page = _ok(stack.read(token, **_yesterday(channel={"kind": "slack", "address": CHAN_B})))
    assert [record["id"] for record in page["messages"]] == [THREAD_B]
    assert _params(stack.slack.calls("conversations.history")[0])["channel"] == CHAN_B


@pytest.mark.parametrize("slack_error", ["not_in_channel", "channel_not_found"])
def test_bot_not_in_channel_becomes_not_member(stack: Stack, slack_error: str) -> None:
    deployed = stack.deploy(extra=({"kind": "slack", "address": CHAN_NOT_MEMBER},))
    if slack_error == "channel_not_found":
        del stack.slack.channels[CHAN_NOT_MEMBER]
    token = stack.mint(deployed, event_id=_event())["token"]
    named = {"kind": "slack", "address": CHAN_NOT_MEMBER}
    _refused(stack.read(token, **_yesterday(channel=named)), 403, "channel_read.not_member")
    assert len(stack.slack.reads()) == 1, "membership is the one refusal the provider decides"
    # The refusal consumed no page: eight reads still fit, the ninth does not.
    _spend(stack, token, 8)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")


def test_thread_from_another_channel_is_refused(stack: Stack) -> None:
    deployed = stack.deploy(extra=({"kind": "slack", "address": CHAN_B},))
    token = stack.mint(deployed, event_id=_event())["token"]
    # Channel B's thread named under channel A: Slack looks it up in A only.
    response = stack.read(token, **_thread(THREAD_B))
    _refused(response, 404, "channel_read.thread_not_in_channel")
    (call,) = stack.slack.calls("conversations.replies")
    assert _params(call)["channel"] == CHAN_A, "the thread id never selects a channel"
    assert "reply in B" not in response.text

    # Defence in depth: a reply whose thread_ts is not the requested thread is
    # never returned, whatever the provider sent.
    def foreign_reply(body: dict[str, Any]) -> dict[str, Any]:
        body["messages"].append(
            {**_user(REPLY_B1, "reply in B", "U0BOB00002"), "thread_ts": THREAD_B}
        )
        return body

    stack.slack.tamper["conversations.replies"] = foreign_reply
    leaked = stack.read(token, **_thread())
    _refused(leaked, 502, "channel_read.provider_error")
    assert "reply in B" not in leaked.text
    assert "hold until Friday" not in leaked.text


def test_non_slack_binding_is_capability_unsupported(stack: Stack) -> None:
    deployed = stack.deploy(extra=({"kind": "email", "address": EMAIL},))
    token = stack.mint(deployed, event_id=_event())["token"]
    _refused(
        stack.read(token, **_yesterday(channel={"kind": "email", "address": EMAIL})),
        409,
        "channel_read.capability_unsupported",
    )
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


# --------------------------------------------------------------------------- #
# Shape: window, limit, identifiers, body
# --------------------------------------------------------------------------- #


def _window_case(case: str) -> tuple[dict[str, Any], str]:
    now = datetime.now(UTC)
    if case == "missing_oldest":
        return {"operation": "history", "latest": _rfc3339(TODAY)}, "channel_read.window_invalid"
    if case == "seven_days_plus_one_second":
        return (
            {
                "operation": "history",
                "oldest": _rfc3339(TODAY - timedelta(days=7, seconds=1)),
                "latest": _rfc3339(TODAY),
            },
            "channel_read.window_too_wide",
        )
    if case == "oldest_in_the_future":
        return {
            "operation": "history",
            "oldest": _rfc3339(now + timedelta(hours=1)),
        }, "channel_read.window_invalid"
    if case == "latest_equals_oldest":
        return (
            {"operation": "history", "oldest": _rfc3339(YESTERDAY), "latest": _rfc3339(YESTERDAY)},
            "channel_read.window_invalid",
        )
    if case == "latest_before_oldest":
        return (
            {"operation": "history", "oldest": _rfc3339(TODAY), "latest": _rfc3339(YESTERDAY)},
            "channel_read.window_invalid",
        )
    if case == "naive_timestamp":
        return (
            {"operation": "history", "oldest": YESTERDAY.replace(tzinfo=None).isoformat()},
            "channel_read.window_invalid",
        )
    if case == "junk":
        return {"operation": "history", "oldest": "yesterday"}, "channel_read.window_invalid"
    if case == "message_with_oldest":
        return {
            **_message(PARENT_USER),
            "oldest": _rfc3339(YESTERDAY),
        }, "channel_read.window_invalid"
    raise AssertionError(case)


@pytest.mark.parametrize(
    "case",
    [
        "missing_oldest",
        "seven_days_plus_one_second",
        "oldest_in_the_future",
        "latest_equals_oldest",
        "latest_before_oldest",
        "naive_timestamp",
        "junk",
        "message_with_oldest",
    ],
)
def test_window_refusals_name_their_reason(stack: Stack, case: str) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    body, code = _window_case(case)
    _refused(stack.read(token, **body), 422, code)
    assert stack.slack.requests == []


def test_window_liveness_exactly_seven_days_and_yesterday(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    week = _ok(
        stack.read(
            token,
            operation="history",
            oldest=_rfc3339(TODAY - timedelta(days=7)),
            latest=_rfc3339(TODAY),
        )
    )
    assert PARENT_USER in [record["id"] for record in week["messages"]]
    _ok(stack.read(token, **_yesterday()))
    # A first page with no latest runs up to now and is valid too.
    _ok(stack.read(token, operation="history", oldest=_rfc3339(YESTERDAY)))


def test_limit_bounds(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    for limit in (0, 101, -1):
        _refused(stack.read(token, **_yesterday(limit=limit)), 422, "channel_read.limit_invalid")
    _refused(stack.read(token, **_message(PARENT_USER, limit=1)), 422, "channel_read.limit_invalid")
    assert stack.slack.requests == []
    for limit in (1, 100):
        _ok(stack.read(token, **_yesterday(limit=limit)))
        assert _params(stack.slack.calls("conversations.history")[-1])["limit"] == str(limit)


def test_identifier_refusals(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    for body in (
        _thread("not-a-ts"),
        _thread("1512085950.21"),
        {"operation": "thread", "oldest": _rfc3339(YESTERDAY), "latest": _rfc3339(TODAY)},
        _yesterday(message_id=PARENT_USER),
        _yesterday(thread_id=THREAD_A),
        {"operation": "message"},
        _message("12345"),
        _message(f"{THREAD_A}:junk"),
        _message(f"{THREAD_A}/{REPLY_A1}"),
    ):
        _refused(stack.read(token, **body), 422, "channel_read.invalid_identifier")
    assert stack.slack.requests == []
    _ok(stack.read(token, **_thread(THREAD_A)))
    _ok(stack.read(token, **_message(f"{THREAD_A}:{REPLY_A1}")))


@pytest.mark.parametrize(
    "body",
    [
        {**_yesterday(), "operation": "search"},
        {**_yesterday(), "query": "release"},
        {**_yesterday(), "channel": {"kind": "slack"}},
        {**_yesterday(), "limit": "ten"},
    ],
    ids=["unknown_operation", "extra_field", "channel_without_address", "limit_not_int"],
)
def test_malformed_bodies_are_request_invalid(stack: Stack, body: dict[str, Any]) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    detail = _refused(stack.read(token, **body), 422, "channel_read.request_invalid")
    assert "release" not in json.dumps(detail), "the message names the location, never the input"
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


def test_no_operation_and_no_cursor_is_operation_required(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    _refused(
        stack.read(token, oldest=_rfc3339(YESTERDAY), latest=_rfc3339(TODAY)),
        422,
        "channel_read.operation_required",
    )
    assert stack.slack.requests == []


# --------------------------------------------------------------------------- #
# Cursors
# --------------------------------------------------------------------------- #


def _first_page_with_cursor(stack: Stack, token: str) -> dict[str, Any]:
    page = _ok(stack.read(token, operation="history", oldest=_rfc3339(YESTERDAY), limit=2))
    assert page["has_more"] is True
    assert page["next_cursor"]
    return page


def test_cursor_only_continuation_keeps_the_window(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    first = _first_page_with_cursor(stack, token)
    first_call = _params(stack.slack.calls("conversations.history")[0])
    assert "cursor" not in first_call

    second = _ok(stack.read(token, cursor=first["next_cursor"]))
    second_call = _params(stack.slack.calls("conversations.history")[1])
    assert second_call["cursor"] == _encode_cursor(2), "the provider cursor is passed through"
    assert Decimal(second_call["oldest"]) == Decimal(first_call["oldest"])
    assert Decimal(second_call["latest"]) == Decimal(first_call["latest"]), (
        "latest is fixed on page 1"
    )
    assert second_call["limit"] == "2"
    ids = [r["id"] for r in first["messages"]] + [r["id"] for r in second["messages"]]
    assert sorted(ids) == sorted({PARENT_USER, PARENT_BOT, THREAD_A, AT_MIDNIGHT})

    # The same continuation with matching bounds supplied is also accepted.
    latest = _rfc3339(_moment(first_call["latest"]))
    _ok(
        stack.read(
            token,
            operation="history",
            oldest=_rfc3339(YESTERDAY),
            latest=latest,
            cursor=first["next_cursor"],
        )
    )


def test_tampered_or_foreign_cursor_is_refused(stack: Stack) -> None:
    deployed = stack.deploy(extra=({"kind": "slack", "address": CHAN_B},))
    event = _event()
    token = stack.mint(deployed, event_id=event)["token"]
    cursor = _first_page_with_cursor(stack, token)["next_cursor"]
    stack.slack.requests.clear()

    middle = len(cursor) // 2
    flipped = cursor[:middle] + ("A" if cursor[middle] != "A" else "B") + cursor[middle + 1 :]
    other_turn = stack.mint(deployed, event_id=_event())["token"]
    for capability, body in (
        (token, {"cursor": flipped}),
        (token, {"cursor": cursor, "channel": {"kind": "slack", "address": CHAN_B}}),
        (token, {"cursor": cursor, "operation": "thread", "thread_id": THREAD_A}),
        (token, {"cursor": cursor, "oldest": _rfc3339(YESTERDAY - timedelta(hours=1))}),
        (token, {"cursor": cursor, "latest": _rfc3339(TODAY + timedelta(hours=1))}),
        (other_turn, {"cursor": cursor}),
    ):
        _refused(stack.read(capability, **body), 422, "channel_read.cursor_invalid")
    assert stack.slack.requests == []

    # Authorization precedes cursor checks: an unbound channel is not_bound.
    _refused(
        stack.read(token, cursor=cursor, channel={"kind": "slack", "address": CHAN_UNBOUND}),
        403,
        "channel_read.not_bound",
    )
    assert stack.slack.requests == []
    # Liveness: the untouched cursor still continues.
    _ok(stack.read(token, cursor=cursor))


# --------------------------------------------------------------------------- #
# The eight page budget
# --------------------------------------------------------------------------- #


def test_eighth_page_succeeds_and_ninth_is_refused(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    bodies = [
        _yesterday(),
        _thread(),
        _message(PARENT_USER),
        _message(f"{THREAD_A}:{REPLY_A1}"),
        _yesterday(limit=1),
        _thread(limit=1),
        _yesterday(),
        _thread(),
    ]
    for body in bodies:
        _ok(stack.read(token, **body))
    before = len(stack.slack.requests)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")
    _refused(stack.read(token, **_message(PARENT_USER)), 429, "channel_read.page_budget_exhausted")
    assert len(stack.slack.requests) == before


def test_concurrent_reservations_never_exceed_eight(stack: Stack) -> None:
    from curie_api.channel_read.ledger import ChannelReadLedger

    agent = uuid.uuid4()
    turn = _event()

    async def race(client: aioredis.Redis) -> list[str]:
        ledger = ChannelReadLedger(client, _prefix())
        gen = await ledger.open(agent, turn, f"owner-{uuid.uuid4().hex}", 3600, resume=False)
        assert isinstance(gen, int)
        outcomes = await asyncio.gather(*(ledger.reserve(agent, turn, gen) for _ in range(20)))
        return list(outcomes)

    stack.agents.append(str(agent))
    outcomes = _valkey_run(race)
    assert outcomes.count("reserved") == 8
    assert outcomes.count("exhausted") == 12


def test_a_fresh_event_gets_a_fresh_budget(stack: Stack) -> None:
    deployed = stack.deploy()
    first = stack.mint(deployed, event_id=_event())
    _spend(stack, first["token"], 8)
    _refused(stack.read(first["token"], **_yesterday()), 429, "channel_read.page_budget_exhausted")
    second = stack.mint(deployed, event_id=_event())
    assert second["turn_key"] != first["turn_key"]
    _spend(stack, second["token"], 8)


def test_approval_resume_shares_the_budget(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    opened = stack.mint(deployed, event_id=event)
    _spend(stack, opened["token"], 5)

    resume_event = _insert_approval(deployed.agent_id, event)
    resumed = stack.mint(deployed, event_id=resume_event)
    assert resumed["turn_key"] == opened["turn_key"]
    _spend(stack, resumed["token"], 3)
    _refused(
        stack.read(resumed["token"], **_yesterday()), 429, "channel_read.page_budget_exhausted"
    )

    # A resume of a resume stays on the same logical turn.
    second_resume = stack.mint(deployed, event_id=_insert_approval(deployed.agent_id, resume_event))
    assert second_resume["turn_key"] == opened["turn_key"]
    _refused(
        stack.read(second_resume["token"], **_yesterday()),
        429,
        "channel_read.page_budget_exhausted",
    )

    # Another agent's approval and a missing row do not resolve.
    other = stack.deploy(channel={"kind": "slack", "address": CHAN_OTHER_AGENT})
    foreign = _insert_approval(other.agent_id, _event())
    _refused(stack.mint_response(deployed, event_id=foreign), 409, "channel_read.turn_unresolvable")
    missing = f"approval-{uuid.uuid4()}-resolved"
    _refused(stack.mint_response(deployed, event_id=missing), 409, "channel_read.turn_unresolvable")


def test_resume_after_ledger_expiry_is_turn_expired(stack: Stack) -> None:
    from curie_internal.channel_read_ledger import ledger_keys

    deployed = stack.deploy()
    # Liveness: a resume inside the 7 day ledger keeps the spent budget.
    kept = _event()
    _spend(stack, stack.mint(deployed, event_id=kept)["token"], 8)
    resumed = stack.mint(deployed, event_id=_insert_approval(deployed.agent_id, kept))
    _refused(
        stack.read(resumed["token"], **_yesterday()), 429, "channel_read.page_budget_exhausted"
    )

    # After the TTL every ledger key of that turn is gone.
    lapsed = _event()
    opened = stack.mint(deployed, event_id=lapsed)
    _spend(stack, opened["token"], 8)
    keys = ledger_keys(_prefix(), uuid.UUID(deployed.agent_id), opened["turn_key"])
    _valkey_run(lambda client: client.delete(*keys))
    response = stack.mint_response(deployed, event_id=_insert_approval(deployed.agent_id, lapsed))
    _refused(response, 409, "channel_read.turn_expired")
    assert "token" not in response.json()
    assert _valkey_run(lambda client: client.exists(*keys)) == 0, "no generation is written"


# --------------------------------------------------------------------------- #
# Generations and revocation
# --------------------------------------------------------------------------- #


def test_revoked_owner_is_refused(stack: Stack) -> None:
    deployed = stack.deploy()
    owner = f"owner-{uuid.uuid4().hex}"
    context = stack.mint(deployed, event_id=_event(), owner=owner)
    _ok(stack.read(context["token"], **_yesterday()))
    assert (
        _revoke_owner(deployed.agent_id, context["turn_key"], f"owner-{uuid.uuid4().hex}") is False
    )
    _ok(stack.read(context["token"], **_yesterday()))
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is True
    before = len(stack.slack.requests)
    _refused(stack.read(context["token"], **_yesterday()), 409, "channel_read.turn_inactive")
    assert len(stack.slack.requests) == before


def test_steer_bump_refuses_the_old_generation_and_keeps_the_budget(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    opened = stack.mint(deployed, event_id=event)
    _spend(stack, opened["token"], 3)
    steered = stack.mint(deployed, event_id=event, mode="steer")
    assert steered["generation"] == opened["generation"] + 1
    assert steered["turn_key"] == opened["turn_key"]
    _refused(stack.read(opened["token"], **_yesterday()), 409, "channel_read.turn_inactive")
    _spend(stack, steered["token"], 5)
    _refused(
        stack.read(steered["token"], **_yesterday()), 429, "channel_read.page_budget_exhausted"
    )


def test_terminal_then_steer_gets_no_capability(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    owner = f"owner-{uuid.uuid4().hex}"
    opened = stack.mint(deployed, event_id=event, owner=owner)
    assert _revoke_owner(deployed.agent_id, opened["turn_key"], owner) is True
    response = stack.mint_response(deployed, event_id=event, mode="steer")
    _refused(response, 409, "channel_read.turn_inactive")
    assert "token" not in response.json()
    # A steer on a turn that never opened gets nothing either.
    _refused(
        stack.mint_response(deployed, event_id=_event(), mode="steer"),
        409,
        "channel_read.turn_inactive",
    )


def test_steer_then_terminal_kills_the_steer_token(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    owner = f"owner-{uuid.uuid4().hex}"
    opened = stack.mint(deployed, event_id=event, owner=owner)
    steered = stack.mint(deployed, event_id=event, mode="steer")
    _ok(stack.read(steered["token"], **_yesterday()))
    assert _revoke_owner(deployed.agent_id, opened["turn_key"], owner) is True
    _refused(stack.read(steered["token"], **_yesterday()), 409, "channel_read.turn_inactive")


def test_resume_opener_survives_a_late_revoke_from_the_original(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    original = f"owner-{uuid.uuid4().hex}"
    opened = stack.mint(deployed, event_id=event, owner=original)
    resumed = stack.mint(
        deployed,
        event_id=_insert_approval(deployed.agent_id, event),
        owner=f"owner-{uuid.uuid4().hex}",
    )
    assert _revoke_owner(deployed.agent_id, opened["turn_key"], original) is False
    _ok(stack.read(resumed["token"], **_yesterday()))


def test_a_revoke_issued_while_the_api_is_down_is_refused_after_recovery(stack: Stack) -> None:
    deployed = stack.deploy()
    owner = f"owner-{uuid.uuid4().hex}"
    context = stack.mint(deployed, event_id=_event(), owner=owner)
    _ok(stack.read(context["token"], **_yesterday()))
    revoked: list[bool] = []
    stack.restart(
        lambda: revoked.append(_revoke_owner(deployed.agent_id, context["turn_key"], owner))
    )
    assert revoked == [True]
    assert context["expires_at"] > datetime.now(UTC).timestamp()
    before = len(stack.slack.requests)
    _refused(stack.read(context["token"], **_yesterday()), 409, "channel_read.turn_inactive")
    assert len(stack.slack.requests) == before


# --------------------------------------------------------------------------- #
# Grant and binding are rechecked on every read
# --------------------------------------------------------------------------- #


def test_stopped_deployment_revokes_on_next_read(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    _ok(stack.read(token, **_yesterday()))
    assert (
        stack.http.delete(f"/deployments/{deployed.deployment_id}", headers=stack.auth).status_code
        == 204
    )
    before = len(stack.slack.requests)
    _refused(stack.read(token, **_yesterday()), 409, "channel_read.grant_revoked")
    assert len(stack.slack.requests) == before


def test_redeploy_to_an_ungranted_version_is_grant_revoked(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    _ok(stack.read(token, **_yesterday()))
    ungranted = stack.version(deployed.agent_id, None)
    assert (
        stack.http.delete(f"/deployments/{deployed.deployment_id}", headers=stack.auth).status_code
        == 204
    )
    redeployed = Deployed(
        deployed.agent_id, ungranted, stack.deployment(deployed.agent_id, ungranted)
    )
    before = len(stack.slack.requests)
    _refused(stack.read(token, **_yesterday()), 409, "channel_read.grant_revoked")
    # Forging the new deployment into the claims does not help: the grant digest
    # in the token no longer matches the deployed bundle.
    forged = _resign(token, deployment=redeployed.deployment_id)
    _refused(stack.read(forged, **_yesterday()), 409, "channel_read.grant_revoked")
    assert len(stack.slack.requests) == before
    _refused(stack.mint_response(redeployed, event_id=_event()), 409, "channel_read.grant_absent")


def test_removed_binding_revokes_on_next_read(stack: Stack) -> None:
    deployed = stack.deploy(extra=({"kind": "slack", "address": CHAN_B},))
    token = stack.mint(deployed, event_id=_event())["token"]
    named = {"kind": "slack", "address": CHAN_B}
    _ok(stack.read(token, **_yesterday(channel=named)))
    removed = stack.http.delete(
        f"/agents/{deployed.agent_id}/channels",
        params={"kind": "slack", "address": CHAN_B},
        headers=stack.auth,
    )
    assert removed.status_code == 204, removed.text
    before = len(stack.slack.requests)
    _refused(stack.read(token, **_yesterday(channel=named)), 403, "channel_read.not_bound")
    assert len(stack.slack.requests) == before


# --------------------------------------------------------------------------- #
# The capability itself
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "forge",
    [
        "forged_signature",
        "expired",
        "wir_prefix",
        "sbx_prefix",
        "platform_key",
        "missing",
        "garbage",
    ],
)
def test_invalid_capabilities_never_reach_slack(
    stack: Stack, auth_headers: dict[str, str], forge: str
) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    headers: dict[str, str] = {CAPABILITY_HEADER: token}
    if forge == "forged_signature":
        prefix, payload, _ = token.split(".")
        fake = base64.urlsafe_b64encode(
            hmac.new(b"not-the-key", payload.encode(), hashlib.sha256).digest()
        )
        headers = {CAPABILITY_HEADER: f"{prefix}.{payload}.{fake.rstrip(b'=').decode()}"}
    elif forge == "expired":
        now = int(datetime.now(UTC).timestamp())
        headers = {CAPABILITY_HEADER: _resign(token, iat=now - 7200, exp=now - 1)}
    elif forge == "wir_prefix":
        headers = {CAPABILITY_HEADER: "wir." + token.split(".", 1)[1]}
    elif forge == "sbx_prefix":
        headers = {CAPABILITY_HEADER: "sbx." + token.split(".", 1)[1]}
    elif forge == "platform_key":
        headers = dict(auth_headers)
    elif forge == "missing":
        headers = {}
    elif forge == "garbage":
        headers = {CAPABILITY_HEADER: "chr.not-base64.sig"}
    response = stack.http.post(READ_URL, json=_yesterday(), headers=headers)
    _refused(response, 401, "channel_read.invalid_capability")
    assert stack.slack.requests == []
    # A sibling capability header does not authorize this route either.
    sibling = stack.http.post(READ_URL, json=_yesterday(), headers={"X-Curie-Issue-Read": token})
    _refused(sibling, 401, "channel_read.invalid_capability")
    assert stack.slack.requests == []


def test_the_capability_authorizes_no_other_route(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    for path, header in (
        ("/agents", "X-API-Key"),
        ("/work-items/issue-read", "X-Curie-Issue-Read"),
    ):
        response = stack.http.post(path, json={}, headers={header: token})
        assert response.status_code in {401, 403}, path


# --------------------------------------------------------------------------- #
# Provider failures
# --------------------------------------------------------------------------- #


def test_rate_limit_maps_and_releases_the_reservation(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    stack.slack.fail_next(
        "conversations.history",
        httpx.Response(
            429, headers={"Retry-After": "2"}, json={"ok": False, "error": "ratelimited"}
        ),
    )
    response = stack.read(token, **_yesterday())
    detail = _refused(response, 429, "channel_read.provider_rate_limited")
    assert detail["retry_after"] == 2
    assert response.headers["retry-after"] == "2"
    # Slack can also answer 200 with the ratelimited error (another method,
    # since history is now cooling down).
    stack.slack.fail_next(
        "conversations.replies",
        httpx.Response(
            200, headers={"Retry-After": "2"}, json={"ok": False, "error": "ratelimited"}
        ),
    )
    _refused(stack.read(token, **_thread()), 429, "channel_read.provider_rate_limited")
    # Neither attempt consumed a page once the cooldowns pass.
    _wait_out_cooldown(2)
    _spend(stack, token, 8)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")


def test_provider_error_releases_the_reservation(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    stack.slack.fail_next(
        "conversations.history", httpx.Response(200, json={"ok": False, "error": "fatal_error"})
    )
    _refused(stack.read(token, **_yesterday()), 502, "channel_read.provider_error")
    stack.slack.fail_next(
        "conversations.history", httpx.Response(500, text="<html>upstream</html>")
    )
    _refused(stack.read(token, **_yesterday()), 502, "channel_read.provider_error")
    stack.slack.fail_next("conversations.history", httpx.Response(200, json={"ok": True}))
    _refused(stack.read(token, **_yesterday()), 502, "channel_read.provider_error")
    _spend(stack, token, 8)


@pytest.mark.parametrize("stack", [{"SLACK_BOT_TOKEN": ""}], indirect=True)
def test_unconfigured_identity_is_refused_without_a_provider_call(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    _refused(stack.read(token, **_yesterday()), 503, "channel_read.provider_unconfigured")
    assert stack.slack.requests == []


def test_valkey_outage_is_503(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    healthy = stack.http.app.state.valkey
    dead = aioredis.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.5, retry=Retry(NoBackoff(), 0)
    )
    stack.http.app.state.valkey = dead
    try:
        _refused(stack.read(token, **_yesterday()), 503, "channel_read.unavailable")
        _refused(stack.mint_response(deployed, event_id=_event()), 503, "channel_read.unavailable")
    finally:
        stack.http.app.state.valkey = healthy
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


# --------------------------------------------------------------------------- #
# Hygiene: caching and logs
# --------------------------------------------------------------------------- #


def test_every_answer_is_no_store(stack: Stack) -> None:
    deployed = stack.deploy()
    context = stack.mint(deployed, event_id=_event())
    token = context["token"]
    responses = [
        stack.read(token, **_yesterday()),  # 200
        stack.read(token, **_yesterday(channel={"address": CHAN_A})),  # 400
        stack.read(None, **_yesterday()),  # 401
        stack.read(token, **_yesterday(channel={"kind": "slack", "address": CHAN_UNBOUND})),  # 403
        stack.read(token, **_message(_ts(YESTERDAY + timedelta(seconds=3)))),  # 404
        stack.read(token, **_yesterday(limit=0)),  # 422
        stack.read(token, **{**_yesterday(), "extra": 1}),  # 422 request_invalid
        stack.mint_response(deployed, event_id=_event(), headers={}),  # 401 mint
    ]
    # One page is spent above (the 404 releases its reservation), seven here.
    _spend(stack, token, 7)
    responses.append(stack.read(token, **_yesterday()))  # 429
    assert sorted({r.status_code for r in responses}) == [200, 400, 401, 403, 404, 422, 429]
    for response in responses:
        assert response.headers.get("cache-control") == "no-store", response.status_code


def test_token_and_bodies_stay_out_of_logs(stack: Stack, caplog: pytest.LogCaptureFixture) -> None:
    deployed = stack.deploy()
    with caplog.at_level(logging.DEBUG):
        context = stack.mint(deployed, event_id=_event())
        token = context["token"]
        page = _ok(stack.read(token, **_message(PARENT_USER)))
        assert page["messages"][0]["text"] == SECRET_TEXT
        _refused(
            stack.read(token, **_yesterday(channel={"kind": "slack", "address": CHAN_UNBOUND})),
            403,
            "channel_read.not_bound",
        )
    assert token not in caplog.text
    assert token.split(".")[2] not in caplog.text
    assert SECRET_TEXT not in caplog.text
    assert BOT_TOKEN not in caplog.text

    async def values(client: aioredis.Redis) -> list[bytes]:
        found = []
        async for key in client.scan_iter(match=f"{_prefix()}:channel-read:*"):
            if await client.type(key) in (b"string", "string"):
                found.append(await client.get(key) or b"")
        return found

    stored = b"".join(v if isinstance(v, bytes) else v.encode() for v in _valkey_run(values))
    assert SECRET_TEXT.encode() not in stored
    assert token.encode() not in stored


# --------------------------------------------------------------------------- #
# Review round 1 regressions
# --------------------------------------------------------------------------- #


def _wait_out_cooldown(seconds: int) -> None:
    """Let a Slack Retry-After cooldown lapse so no state leaks into other tests."""

    import time

    time.sleep(seconds + 0.5)


def test_history_excludes_thread_broadcast_replies(stack: Stack) -> None:
    # conversations.history returns a reply posted with "Also send to channel"
    # as subtype thread_broadcast, with thread_ts naming its parent and a root
    # copy of the parent (https://docs.slack.dev/reference/methods/conversations.history,
    # https://docs.slack.dev/reference/events/message/thread_broadcast).
    broadcast_ts = _ts(YESTERDAY + timedelta(hours=15, minutes=20))
    broadcast = {
        "type": "message",
        "subtype": "thread_broadcast",
        "user": "U0CAROL003",
        "text": "broadcast: we hold until Friday",
        "ts": broadcast_ts,
        "thread_ts": THREAD_A,
        "root": {**stack.slack.channels[CHAN_A][3]},
    }
    stack.slack.channels[CHAN_A].append(broadcast)
    stack.slack.threads[(CHAN_A, THREAD_A)].append({**broadcast})
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    page = _ok(stack.read(token, **_yesterday()))
    ids = [record["id"] for record in page["messages"]]
    texts = [record["text"] for record in page["messages"]]
    # Positive control: the parent the broadcast belongs to is retained.
    assert THREAD_A in ids
    assert broadcast_ts not in ids
    assert f"{THREAD_A}:{broadcast_ts}" not in ids
    assert broadcast["text"] not in texts
    assert all(record.get("thread_id") is None for record in page["messages"])
    # The reply stays reachable where it belongs: in its thread.
    thread = _ok(stack.read(token, **_thread()))
    assert f"{THREAD_A}:{broadcast_ts}" in [record["id"] for record in thread["messages"]]


def test_time_paginated_history_continues_without_a_provider_cursor(stack: Stack) -> None:
    stack.slack.time_paginated = True
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    first = _ok(stack.read(token, **_yesterday(limit=2)))
    first_call = _params(stack.slack.calls("conversations.history")[0])
    # Slack's inclusive end returns the midnight message, which the route drops.
    assert 1 <= len(first["messages"]) <= 2
    assert first["has_more"] is True, "Slack said has_more; the page must too"
    assert first.get("next_cursor"), "an opaque cursor is required to continue"

    second = _ok(stack.read(token, cursor=first["next_cursor"]))
    second_call = _params(stack.slack.calls("conversations.history")[1])
    assert Decimal(second_call["oldest"]) == Decimal(first_call["oldest"])
    assert Decimal(second_call["latest"]) <= Decimal(first_call["latest"]), "no window widening"
    first_ids = [record["id"] for record in first["messages"]]
    second_ids = [record["id"] for record in second["messages"]]
    assert not set(first_ids) & set(second_ids), "no duplicates across pages"
    assert sorted(first_ids + second_ids) == sorted([PARENT_USER, PARENT_BOT, THREAD_A])
    assert second["has_more"] is False


def test_failed_provider_attempts_hit_a_per_turn_attempt_cap(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    missing = _message(_ts(YESTERDAY + timedelta(hours=11, seconds=7)))
    codes: list[str] = []
    for _ in range(64):
        response = stack.read(token, **missing)
        code = response.json()["detail"]["code"]
        codes.append(code)
        if code == "channel_read.attempt_budget_exhausted":
            assert response.status_code == 429, response.text
            assert response.headers["cache-control"] == "no-store"
            break
        assert code == "channel_read.message_not_found", code
    assert codes[-1] == "channel_read.attempt_budget_exhausted", "64 failed Slack calls, no cap"
    assert codes[0] == "channel_read.message_not_found"
    before = len(stack.slack.requests)
    _refused(stack.read(token, **missing), 429, "channel_read.attempt_budget_exhausted")
    assert len(stack.slack.requests) == before, "an exhausted attempt budget calls Slack no more"


def test_a_few_failures_leave_all_eight_pages(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    missing = _message(_ts(YESTERDAY + timedelta(hours=11, seconds=7)))
    for _ in range(3):
        _refused(stack.read(token, **missing), 404, "channel_read.message_not_found")
    _spend(stack, token, 8)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")


def test_a_slack_429_cools_down_the_method_for_every_agent(stack: Stack) -> None:
    first = stack.deploy()
    second = stack.deploy(channel={"kind": "slack", "address": CHAN_OTHER_AGENT})
    first_token = stack.mint(first, event_id=_event())["token"]
    second_token = stack.mint(second, event_id=_event())["token"]
    stack.slack.fail_next(
        "conversations.history",
        httpx.Response(
            429, headers={"Retry-After": "3"}, json={"ok": False, "error": "ratelimited"}
        ),
    )
    try:
        _refused(stack.read(first_token, **_yesterday()), 429, "channel_read.provider_rate_limited")
        before = len(stack.slack.requests)
        # Same bot identity and method, another agent: refused with no Slack call.
        other = {"channel": {"kind": "slack", "address": CHAN_OTHER_AGENT}}
        cooled = stack.read(second_token, **_yesterday(**other))
        detail = _refused(cooled, 429, "channel_read.provider_rate_limited")
        assert 0 < detail["retry_after"] <= 3
        assert cooled.headers["retry-after"] == str(detail["retry_after"])
        _refused(stack.read(first_token, **_yesterday()), 429, "channel_read.provider_rate_limited")
        assert len(stack.slack.requests) == before, "a cooled down method reaches Slack"
        # Liveness: another method is not cooled down.
        _ok(stack.read(first_token, **_thread()))
    finally:
        _wait_out_cooldown(3)
    # After the cooldown the method is usable again.
    _ok(
        stack.read(
            second_token, **_yesterday(channel={"kind": "slack", "address": CHAN_OTHER_AGENT})
        )
    )


def test_thread_limit_one_continues_past_a_parent_only_page(stack: Stack) -> None:
    stack.slack.time_paginated = True
    stack.slack.replies_parent_in_limit = True
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    expected = {f"{THREAD_A}:{REPLY_A1}", f"{THREAD_A}:{REPLY_A2}"}
    seen: list[str] = []
    page = _ok(stack.read(token, **_thread(limit=1)))
    first_call = _params(stack.slack.calls("conversations.replies")[0])
    for _ in range(4):
        ids = [record["id"] for record in page["messages"]]
        assert THREAD_A not in ids, "the parent is never returned as a reply"
        assert all(record["thread_id"] == THREAD_A for record in page["messages"])
        seen.extend(ids)
        if not page["has_more"]:
            break
        assert page.get("next_cursor"), "has_more needs a cursor to continue"
        page = _ok(stack.read(token, cursor=page["next_cursor"]))
    assert len(seen) == len(set(seen)), "no duplicates across pages"
    assert set(seen) == expected, "the replies after a parent only page were lost"
    for call in stack.slack.calls("conversations.replies"):
        params = _params(call)
        assert params["ts"] == THREAD_A
        assert Decimal(params["oldest"]) >= Decimal(first_call["oldest"]), "no window widening"
        assert Decimal(params["latest"]) <= Decimal(first_call["latest"]), "no window widening"


def _parent_only(body: dict[str, Any]) -> dict[str, Any]:
    """conversations.replies answering with the thread parent alone.

    The parent is always the first element and counts toward `limit`
    (https://docs.slack.dev/reference/methods/conversations.replies), so a page
    can hold nothing else; `has_more` is true and, under time based paging,
    no `response_metadata.next_cursor` is offered.
    """

    return {"ok": True, "messages": body["messages"][:1], "has_more": True}


def _after_parent() -> dict[str, Any]:
    # A reply window that opens after the parent's ts and before both replies.
    return _thread(oldest=_rfc3339(_moment(THREAD_A) + timedelta(minutes=1)), limit=1)


def test_a_stuck_parent_only_replies_page_is_provider_incomplete(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    stack.slack.tamper["conversations.replies"] = _parent_only
    response = stack.read(token, **_after_parent())
    # Never false exhaustion: an empty, finished page would lose both replies.
    assert not (response.status_code == 200 and response.json()["has_more"] is False), (
        "a parent only page with has_more true was reported as the end of the thread"
    )
    detail = response.json()["detail"]
    assert detail["code"] == "channel_read.provider_incomplete", response.text
    assert response.headers["cache-control"] == "no-store"
    assert stack.slack.calls("conversations.replies"), "the provider was asked"
    del stack.slack.tamper["conversations.replies"]
    # The refusal charged no page: all eight remain.
    _spend(stack, token, 8)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")


def test_a_parent_only_replies_page_continues_when_progress_is_possible(stack: Stack) -> None:
    deployed = stack.deploy()
    token = stack.mint(deployed, event_id=_event())["token"]
    answered: list[int] = []

    def first_call_parent_only(body: dict[str, Any]) -> dict[str, Any]:
        answered.append(1)
        return _parent_only(body) if len(answered) == 1 else body

    stack.slack.tamper["conversations.replies"] = first_call_parent_only
    seen: list[str] = []
    page = _ok(stack.read(token, **_after_parent()))
    for _ in range(4):
        ids = [record["id"] for record in page["messages"]]
        assert THREAD_A not in ids, "the parent is never returned as a reply"
        seen.extend(ids)
        if not page["has_more"]:
            break
        assert page.get("next_cursor"), "has_more needs a cursor to continue"
        page = _ok(stack.read(token, cursor=page["next_cursor"]))
    assert len(seen) == len(set(seen)), "no duplicates across pages"
    assert set(seen) == {f"{THREAD_A}:{REPLY_A1}", f"{THREAD_A}:{REPLY_A2}"}, (
        "the replies after a parent only page were lost"
    )
