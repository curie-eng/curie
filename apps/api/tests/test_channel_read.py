"""The agent's bounded read of its own channel (ADR 0100, #2877).

Everything drives the real app: real routes, Postgres, Valkey, the object store,
and the internal route the worker mints the capability from. Only Slack is
replaced, at the HTTP seam, with response shapes from Slack's reference pages:

conversations.history https://docs.slack.dev/reference/methods/conversations.history
(newest first, `has_more`, `response_metadata.next_cursor`, `oldest`/`latest` with
the `inclusive` flag applying to both ends, `not_in_channel`, `channel_not_found`;
bot messages carry `bot_id` and `subtype: bot_message` instead of `user`; a thread
parent carries `thread_ts` and `reply_count`).
conversations.replies https://docs.slack.dev/reference/methods/conversations.replies
(the parent is the first element, every reply carries `thread_ts`, an unknown
thread in the named channel is `thread_not_found`).
auth.test https://docs.slack.dev/reference/methods/auth.test (`url` is the
workspace URL permalinks are built from).
Rate limits https://docs.slack.dev/apis/web-api/rate-limits (HTTP 429 with a
`Retry-After` header and `{"ok": false, "error": "ratelimited"}`).
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
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest
import redis.asyncio as aioredis
from channel_protocol import ChannelCapability
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
AUTH_TEST = {"ok": True, "url": WORKSPACE_URL, "team_id": "T0EXAMPLE1", "bot_id": "B0CURIEBOT"}

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
MISSING = _ts(YESTERDAY + timedelta(hours=11, seconds=7))
REPLY_IDS = {f"{THREAD_A}:{REPLY_A1}", f"{THREAD_A}:{REPLY_A2}"}
SECRET_TEXT = "the release decision was to hold 0.13 until Friday"


# -- Fake Slack, at the HTTP seam only ---------------------------------------- #
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


def _slack_error(error: str, status: int = 200, retry_after: int | None = None) -> httpx.Response:
    headers = {} if retry_after is None else {"Retry-After": str(retry_after)}
    return httpx.Response(status, headers=headers, json={"ok": False, "error": error})


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
        if queued := self.scripted.get(method):
            return queued.pop(0)
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return _slack_error("invalid_auth")
        params = _params(request)
        if method == "auth.test":
            return httpx.Response(200, json=AUTH_TEST)
        channel = params.get("channel", "")
        if channel not in self.channels:
            return _slack_error("channel_not_found")
        if channel not in self.members:
            return _slack_error("not_in_channel")
        if method == "conversations.history":
            ordered = sorted(self.channels[channel], key=lambda m: Decimal(m["ts"]), reverse=True)
            if self.time_paginated:
                params = {k: v for k, v in params.items() if k != "cursor"}
            body = self._page(ordered, params)
            if self.time_paginated:
                body.pop("response_metadata")
        elif method == "conversations.replies":
            thread_ts = params.get("ts", "")
            parent = next((m for m in self.channels[channel] if m["ts"] == thread_ts), None)
            if parent is None:
                return _slack_error("thread_not_found")
            replies = sorted(
                self.threads.get((channel, thread_ts), []), key=lambda m: Decimal(m["ts"])
            )
            if self.replies_parent_in_limit:
                limit = int(params.get("limit") or 1000)
                window = self._page(replies, {**params, "limit": "1000", "cursor": ""})["messages"]
                cursor = params.get("cursor")
                start = _decode_cursor(cursor) if cursor and not self.time_paginated else 0
                taken = window[start : start + max(limit - 1, 0)]
                more = start + len(taken) < len(window)
                body = {"ok": True, "messages": [parent, *taken], "has_more": more}
                if not self.time_paginated:
                    next_cursor = _encode_cursor(start + len(taken)) if more else ""
                    body["response_metadata"] = {"next_cursor": next_cursor}
            else:
                body = self._page(replies, params)
                # Slack always leads a replies page with the thread parent.
                body["messages"] = [parent, *body["messages"]]
        else:
            return _slack_error("unknown_method")
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
        more = start + limit < len(window)
        return {
            "ok": True,
            "messages": window[start : start + limit],
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


def _reply(ts: str, text: str, thread_ts: str, user: str = "U0BOB00002") -> dict[str, Any]:
    return {**_user(ts, text, user), "thread_ts": thread_ts, "parent_user_id": "U0ALICE001"}


def _seeded_slack() -> FakeSlack:
    slack = FakeSlack()
    parent_a = _user(THREAD_A, "should we ship 0.13 this week?")
    parent_a.update({"thread_ts": THREAD_A, "reply_count": 2, "latest_reply": REPLY_A2})
    parent_b = _user(THREAD_B, "channel B planning thread")
    parent_b.update({"thread_ts": THREAD_B, "reply_count": 1, "latest_reply": REPLY_B1})
    bot = {"type": "message", "subtype": "bot_message", "bot_id": "B0DEPLOYER"}
    slack.channels = {
        CHAN_A: [
            _user(BEFORE_WINDOW, "too early"),
            _user(PARENT_USER, SECRET_TEXT),
            {**bot, "text": "deploy finished", "ts": PARENT_BOT},
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
            _reply(REPLY_A1, "no, hold until Friday", THREAD_A),
            _reply(REPLY_A2, "agreed", THREAD_A, "U0CAROL003"),
        ],
        (CHAN_B, THREAD_B): [_reply(REPLY_B1, "reply in B", THREAD_B)],
    }
    # The bot is a member of every seeded channel except CHAN_NOT_MEMBER.
    slack.members = {CHAN_A, CHAN_B, CHAN_OTHER_AGENT, CHAN_UNBOUND}
    return slack


# -- Real stack ---------------------------------------------------------------- #
SKILL_MD = b"---\nname: reader-bot\ndescription: t\n---\nhi\n"


def _bundle_archive(grant: bool | None) -> bytes:
    return _grants_archive({} if grant is None else {"channelRead": grant})


def _grants_archive(grants: Mapping[str, Any]) -> bytes:
    """A bundle whose manifest carries exactly these platform Slack grant keys
    (``channelRead``, ``canvasList``, ``canvasRead``, ``canvasEdit``; ADR 0200)."""
    manifest: dict[str, Any] = {"name": "reader-bot", "version": "0.1.0", "description": "t"}
    manifest.update(grants)
    files = {
        "reader-bot/.claude-plugin/plugin.json": json.dumps(manifest).encode(),
        "reader-bot/skills/reader-bot/SKILL.md": SKILL_MD,
    }
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


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


def _event() -> str:
    return f"evt-{uuid.uuid4().hex}"


def _owner() -> str:
    return f"owner-{uuid.uuid4().hex}"


def _named(address: str, kind: str = "slack") -> dict[str, str]:
    return {"kind": kind, "address": address}


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

    def start(self) -> None:
        self.client = TestClient(create_app())
        self.client.__enter__()
        self._http = httpx.AsyncClient(transport=httpx.MockTransport(self.slack.handler))
        self.client.app.state.http_client = self._http

    def stop(self) -> None:
        """Models the API process going away; `start` brings it back."""
        assert self.client is not None and self._http is not None
        self.client.__exit__(None, None, None)
        asyncio.run(self._http.aclose())
        self.client = None

    @property
    def http(self) -> TestClient:
        assert self.client is not None
        return self.client

    # Setup through the real routes.
    def admin(self, method: str, path: str, status: int = 201, **kwargs: Any) -> Any:
        response = self.http.request(method, path, headers=self.auth, **kwargs)
        assert response.status_code == status, response.text
        return response.json() if response.content else None

    def deploy(
        self,
        *,
        grant: bool | None = True,
        channel: dict[str, str] | None = None,
        extra: tuple[dict[str, str], ...] = (),
    ) -> Deployed:
        name = f"reader-{uuid.uuid4().hex[:8]}"
        agent = self.admin(
            "POST", "/agents", json={"name": name, "channel": channel or _named(CHAN_A)}
        )
        self.agents.append(agent["id"])
        for binding in extra:
            self.admin("POST", f"/agents/{agent['id']}/channels", json=binding)
        return self.redeploy(agent["id"], grant)

    def redeploy(self, agent_id: str, grant: bool | None) -> Deployed:
        """A new version with the given grant, and a deployment of it."""
        label = {"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "operator"}
        version_id = str(self.admin("POST", f"/agents/{agent_id}/versions", json=label)["id"])
        bundle = {"file": ("reader-bot.tar.gz", _bundle_archive(grant))}
        self.admin("PUT", f"/agents/{agent_id}/versions/{version_id}/bundle", files=bundle)
        body = {"agent_id": agent_id, "version_id": version_id, "environment": "dev"}
        deployment_id = str(self.admin("POST", "/deployments", json=body)["id"])
        return Deployed(agent_id, version_id, deployment_id)

    def undeploy(self, deployed: Deployed) -> None:
        self.admin("DELETE", f"/deployments/{deployed.deployment_id}", status=204)

    # The two routes under test.
    def mint_response(
        self,
        deployed: Deployed,
        *,
        event_id: str | None = None,
        mode: str = "open",
        owner: str | None = None,
        omit_owner: bool = False,
        no_default: bool = False,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        body: dict[str, Any] = {
            "agent_id": deployed.agent_id,
            "deployment_id": deployed.deployment_id,
            "event_id": event_id or _event(),
            "mode": mode,
            "default_channel": None if no_default else _named(CHAN_A),
            "ttl_s": 3600,
        }
        if owner is None and mode == "open" and not omit_owner:
            owner = _owner()
        if owner is not None:
            body["owner"] = owner
        headers = WORKER_HEADERS if headers is None else headers
        return self.http.post(MINT_URL, json=body, headers=headers)

    def mint(self, deployed: Deployed, **kwargs: Any) -> dict[str, Any]:
        context = _ok(self.mint_response(deployed, **kwargs))
        assert context["token"].startswith("chr.")
        return context

    def open(self, **deploy: Any) -> tuple[Deployed, str]:
        """Deploy a granted agent and mint a fresh turn's capability."""
        deployed = self.deploy(**deploy)
        return deployed, str(self.mint(deployed)["token"])

    def read(self, token: str | None, **body: Any) -> httpx.Response:
        headers = {} if token is None else {CAPABILITY_HEADER: token}
        return self.http.post(READ_URL, json=body, headers=headers)

    @contextmanager
    def no_provider_calls(self) -> Iterator[None]:
        before = len(self.slack.requests)
        yield
        assert len(self.slack.requests) == before, "a refused read reached the provider"


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
            if keys := _ledger_keys_of(agent_id):
                _valkey_run(lambda client, keys=keys: client.delete(*keys))
        get_settings.cache_clear()


# -- Small helpers ------------------------------------------------------------- #
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


def _ids(page: dict[str, Any]) -> list[str]:
    return [record["id"] for record in page["messages"]]


def _yesterday(**extra: Any) -> dict[str, Any]:
    return {
        "operation": "history",
        "oldest": _rfc3339(YESTERDAY),
        "latest": _rfc3339(TODAY),
    } | extra


def _thread(thread_id: str = THREAD_A, **extra: Any) -> dict[str, Any]:
    return _yesterday(operation="thread", thread_id=thread_id, **extra)


def _message(message_id: str, **extra: Any) -> dict[str, Any]:
    return {"operation": "message", "message_id": message_id, **extra}


def _spend(stack: Stack, token: str, pages: int) -> None:
    for _ in range(pages):
        _ok(stack.read(token, **_yesterday()))


def _budget_left(stack: Stack, token: str, pages: int) -> None:
    """Exactly `pages` reads still fit the turn's budget; the next is refused."""
    _spend(stack, token, pages)
    _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")


def _resign(token: str, **changes: Any) -> str:
    """A validly signed `chr` capability with changed claims, as a key holder would forge."""
    from curie_internal import sandbox_token

    prefix, payload, _ = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    encoded = json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
    signed = f"{prefix}.{base64.urlsafe_b64encode(encoded).rstrip(b'=').decode()}"
    return f"{signed}.{sandbox_token.signature(get_settings().api_key, signed)}"


APPROVAL_SQL = (
    "INSERT INTO curie.approvals (id, agent_id, conversation_id, author, summary, reply_kind, "
    "reply_channel, reply_placeholder, dedupe_key, status) VALUES (:id, :agent, :conv, "
    f"'U0ALICE001', 'merge it', 'slack', '{CHAN_A}', 'p-1', :dedupe, 'approved')"
)


def _insert_approval(agent_id: str, dedupe_key: str) -> str:
    approval_id = uuid.uuid4()
    conv = f"th-{approval_id.hex[:8]}"
    params = {"id": approval_id, "agent": uuid.UUID(agent_id), "conv": conv, "dedupe": dedupe_key}

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        async with engine.begin() as conn:
            await conn.execute(text(APPROVAL_SQL), params)
        await engine.dispose()

    asyncio.run(run())
    return f"approval-{approval_id}-resolved"


def _revoke_owner(agent_id: str, turn_key: str, owner: str) -> bool:
    """The worker's terminal revoke: a direct owner checked delete in the shared Valkey."""
    from curie_internal.channel_read_ledger import revoke_owner

    agent = uuid.UUID(agent_id)
    return bool(_valkey_run(lambda client: revoke_owner(client, _prefix(), agent, turn_key, owner)))


# -- Mint ---------------------------------------------------------------------- #
@pytest.mark.parametrize("grant", [None, False])
def test_mint_refuses_a_bundle_without_the_grant(stack: Stack, grant: bool | None) -> None:
    deployed = stack.deploy(grant=grant)
    response = stack.mint_response(deployed)
    _refused(response, 409, "channel_read.grant_absent")
    assert "token" not in response.text
    assert _ledger_keys_of(deployed.agent_id) == []
    # Liveness: the same agent on a granted version mints.
    stack.mint(stack.redeploy(deployed.agent_id, True))


def test_mint_refuses_bad_credentials_owners_and_deployments_and_scopes_its_token(
    stack: Stack,
) -> None:
    mine = stack.deploy()
    for headers in ({}, stack.auth, {"X-Curie-Worker-Token": "not-the-token"}):
        response = stack.mint_response(mine, headers=headers)
        assert response.status_code == 401, response.text
        assert response.headers["cache-control"] == "no-store"
    assert _ledger_keys_of(mine.agent_id) == []
    # An open requires an owner and a steer forbids one.
    event = _event()
    assert stack.mint_response(mine, event_id=event, omit_owner=True).status_code == 422
    token = stack.mint(mine, event_id=event)["token"]
    steer = stack.mint_response(mine, event_id=event, mode="steer", owner=_owner())
    assert steer.status_code == 422, steer.text
    # The minted capability authorizes no other route.
    for path, header in (
        ("/agents", "X-API-Key"),
        ("/work-items/issue-read", "X-Curie-Issue-Read"),
    ):
        assert stack.http.post(path, json={}, headers={header: token}).status_code in {401, 403}
    # A deployment of another agent, or a stopped one, is inactive.
    other = stack.deploy(channel=_named(CHAN_OTHER_AGENT))
    foreign = Deployed(mine.agent_id, mine.version_id, other.deployment_id)
    _refused(stack.mint_response(foreign), 409, "channel_read.deployment_inactive")
    stack.undeploy(mine)
    _refused(stack.mint_response(mine), 409, "channel_read.deployment_inactive")
    assert stack.slack.requests == []


# -- Reads that succeed -------------------------------------------------------- #
def test_granted_bound_member_reads_yesterdays_history(stack: Stack) -> None:
    _, token = stack.open()
    page = _ok(stack.read(token, **_yesterday()))
    by_id = {record["id"]: record for record in page["messages"]}
    # [oldest, latest): the midnight message and the one before the window are excluded.
    assert set(by_id) == {PARENT_USER, PARENT_BOT, THREAD_A}
    assert page["has_more"] is False
    user = by_id[PARENT_USER]
    assert (user["text"], user["author"], user["truncated"]) == (SECRET_TEXT, "U0ALICE001", False)
    assert datetime.fromisoformat(user["timestamp"].replace("Z", "+00:00")) == _moment(PARENT_USER)
    assert user["timestamp"].endswith(("Z", "+00:00"))
    assert user["provenance"] == f"{WORKSPACE_URL}archives/{CHAN_A}/p{PARENT_USER.replace('.', '')}"
    assert by_id[PARENT_BOT]["author"] == "B0DEPLOYER", "a bot message is authored by its bot id"
    assert by_id[THREAD_A]["reply_count"] == 2
    allowed = set("id thread_id timestamp author text truncated provenance reply_count".split())
    assert all(set(r) <= allowed for r in page["messages"]), "no Slack field leaks into a record"

    (call,) = stack.slack.calls("conversations.history")
    params = _params(call)
    assert call.headers["authorization"] == f"Bearer {BOT_TOKEN}"
    assert (params["channel"], params["limit"]) == (CHAN_A, "50")
    assert Decimal(params["oldest"]) == Decimal(_ts(YESTERDAY))
    assert Decimal(params["latest"]) == Decimal(_ts(TODAY))
    assert params["inclusive"].lower() in {"true", "1"}


def test_thread_replies_exclude_the_parent_and_stay_in_the_window(stack: Stack) -> None:
    _, token = stack.open()
    page = _ok(stack.read(token, **_thread()))
    assert sorted(_ids(page)) == sorted(REPLY_IDS)
    assert all(record["thread_id"] == THREAD_A for record in page["messages"])
    reply = next(r for r in page["messages"] if r["id"] == f"{THREAD_A}:{REPLY_A1}")
    assert reply["provenance"] == (
        f"{WORKSPACE_URL}archives/{CHAN_A}/p{REPLY_A1.replace('.', '')}"
        f"?thread_ts={THREAD_A}&cid={CHAN_A}"
    )
    (call,) = stack.slack.calls("conversations.replies")
    assert (_params(call)["ts"], _params(call)["channel"]) == (THREAD_A, CHAN_A)
    # A window that closes before the replies returns none of them.
    early = _thread(latest=_rfc3339(_moment(REPLY_A1) - timedelta(minutes=1)))
    assert _ok(stack.read(token, **early))["messages"] == []


def test_message_reads_parents_and_replies_and_truncates(stack: Stack) -> None:
    long_ts = _ts(YESTERDAY + timedelta(hours=20))
    stack.slack.channels[CHAN_A].append(_user(long_ts, "x" * 3990 + "y" * 1010))
    _, token = stack.open()
    (hit,) = _ok(stack.read(token, **_message(PARENT_USER)))["messages"]
    assert (hit["id"], hit["text"], hit["truncated"]) == (PARENT_USER, SECRET_TEXT, False)
    # A well formed id that is not in the channel is an error, unlike an empty page.
    _refused(stack.read(token, **_message(MISSING)), 404, "channel_read.message_not_found")
    (record,) = _ok(stack.read(token, **_message(long_ts)))["messages"]
    assert record["truncated"] is True
    assert record["text"].startswith("x" * 3990) and record["text"].endswith("[truncated]")
    assert len(record["text"]) <= 4000 + len(" [truncated]")
    assert "y" * 1000 not in record["text"]

    # History omits replies, so only the thread form of the id can reach one.
    reply_id = f"{THREAD_A}:{REPLY_A2}"
    assert reply_id not in _ids(_ok(stack.read(token, **_yesterday())))
    stack.slack.requests.clear()
    page = _ok(stack.read(token, **_message(reply_id)))
    assert (_ids(page), page["messages"][0]["text"]) == ([reply_id], "agreed")
    replies = stack.slack.calls("conversations.replies")
    assert replies, "a reply id is looked up inside its thread"
    params = _params(replies[0])
    assert (params["channel"], params["ts"]) == (CHAN_A, THREAD_A)
    assert Decimal(params["oldest"]) == Decimal(params["latest"]) == Decimal(REPLY_A2)
    stray = f"{THREAD_A}:{_ts(YESTERDAY + timedelta(hours=15, minutes=7))}"
    _refused(stack.read(token, **_message(stray)), 404, "channel_read.message_not_found")
    # A parent id goes to history.
    stack.slack.requests.clear()
    _ok(stack.read(token, **_message(THREAD_A)))
    assert stack.slack.calls("conversations.history")
    assert not stack.slack.calls("conversations.replies")


# -- Which channel: binding, kind, membership ---------------------------------- #
def test_a_channel_outside_the_bindings_or_since_unbound_is_refused(stack: Stack) -> None:
    deployed, token = stack.open(extra=(_named(CHAN_B),))
    stack.deploy(channel=_named(CHAN_OTHER_AGENT))
    # The bound address under the wrong kind is not this binding either.
    for named in (_named(CHAN_UNBOUND), _named(CHAN_OTHER_AGENT), _named(CHAN_A, "email")):
        _refused(stack.read(token, **_yesterday(channel=named)), 403, "channel_read.not_bound")
    # A channel named without its kind, or a turn with no default, names no channel.
    no_default = stack.mint(deployed, no_default=True)["token"]
    kindless = _yesterday(channel={"address": CHAN_A})
    for capability, body in ((token, kindless), (no_default, _yesterday())):
        _refused(stack.read(capability, **body), 400, "channel_read.channel_required")
    assert stack.slack.requests == []
    # Liveness: a turn with no default names a bound pair; a second binding reads by name.
    _ok(stack.read(no_default, **_yesterday(channel=_named(CHAN_A))))
    assert _ids(_ok(stack.read(token, **_yesterday(channel=_named(CHAN_B))))) == [THREAD_B]
    assert _params(stack.slack.calls("conversations.history")[-1])["channel"] == CHAN_B
    # Removing that binding revokes it on the next read.
    stack.admin(
        "DELETE", f"/agents/{deployed.agent_id}/channels", status=204, params=_named(CHAN_B)
    )
    with stack.no_provider_calls():
        response = stack.read(token, **_yesterday(channel=_named(CHAN_B)))
        _refused(response, 403, "channel_read.not_bound")


@pytest.mark.parametrize("slack_error", ["not_in_channel", "channel_not_found"])
def test_bot_not_in_channel_becomes_not_member(stack: Stack, slack_error: str) -> None:
    if slack_error == "channel_not_found":
        del stack.slack.channels[CHAN_NOT_MEMBER]
    _, token = stack.open(extra=(_named(CHAN_NOT_MEMBER),))
    named = _named(CHAN_NOT_MEMBER)
    _refused(stack.read(token, **_yesterday(channel=named)), 403, "channel_read.not_member")
    assert len(stack.slack.reads()) == 1, "membership is the one refusal the provider decides"
    _budget_left(stack, token, 8)


def test_thread_from_another_channel_is_refused(stack: Stack) -> None:
    _, token = stack.open(extra=(_named(CHAN_B),))
    # Channel B's thread named under channel A: Slack looks it up in A only.
    response = stack.read(token, **_thread(THREAD_B))
    _refused(response, 404, "channel_read.thread_not_in_channel")
    (call,) = stack.slack.calls("conversations.replies")
    assert _params(call)["channel"] == CHAN_A, "the thread id never selects a channel"
    assert "reply in B" not in response.text

    # Defence in depth: a reply from another thread is never returned, whatever Slack sent.
    def foreign_reply(body: dict[str, Any]) -> dict[str, Any]:
        body["messages"].append({**_user(REPLY_B1, "reply in B"), "thread_ts": THREAD_B})
        return body

    stack.slack.tamper["conversations.replies"] = foreign_reply
    leaked = stack.read(token, **_thread())
    _refused(leaked, 502, "channel_read.provider_error")
    assert "reply in B" not in leaked.text and "hold until Friday" not in leaked.text


def test_non_slack_binding_is_capability_unsupported(stack: Stack) -> None:
    _, token = stack.open(extra=(_named(EMAIL, "email"),))
    response = stack.read(token, **_yesterday(channel=_named(EMAIL, "email")))
    _refused(response, 409, "channel_read.capability_unsupported")
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


def test_a_reader_without_history_read_is_unsupported() -> None:
    from curie_api.channel_read.readers import ChannelReader, reader_for

    @dataclass
    class Reader:
        capabilities: frozenset[ChannelCapability]

    readers: dict[str, ChannelReader] = {
        "blind": Reader(frozenset()),  # type: ignore[dict-item]
        "sighted": Reader(frozenset({ChannelCapability.HISTORY_READ})),  # type: ignore[dict-item]
    }
    assert reader_for(readers, "blind") is None
    assert reader_for(readers, "sighted") is readers["sighted"]
    assert reader_for(readers, "unregistered") is None


# -- Shape: window, limit, identifiers, body ----------------------------------- #

_HISTORY_FROM = {"operation": "history", "oldest": _rfc3339(YESTERDAY)}


@pytest.mark.parametrize(
    ("code", "bodies"),
    [
        (
            "window_invalid",
            [
                {"operation": "history", "latest": _rfc3339(TODAY)},
                {"operation": "history", "oldest": _rfc3339(TODAY + timedelta(days=2))},
                _yesterday(latest=_rfc3339(YESTERDAY)),
                _yesterday(oldest=_rfc3339(TODAY), latest=_rfc3339(YESTERDAY)),
                {"operation": "history", "oldest": YESTERDAY.replace(tzinfo=None).isoformat()},
                {"operation": "history", "oldest": "yesterday"},
                _message(PARENT_USER, oldest=_rfc3339(YESTERDAY)),
            ],
        ),
        ("window_too_wide", [_yesterday(oldest=_rfc3339(TODAY - timedelta(days=7, seconds=1)))]),
        (
            "limit_invalid",
            [*(_yesterday(limit=n) for n in (0, 101, -1)), _message(PARENT_USER, limit=1)],
        ),
        (
            "invalid_identifier",
            [
                *(_thread(bad) for bad in ("not-a-ts", "1512085950.21")),
                {"operation": "thread", "oldest": _rfc3339(YESTERDAY), "latest": _rfc3339(TODAY)},
                _yesterday(message_id=PARENT_USER),
                _yesterday(thread_id=THREAD_A),
                {"operation": "message"},
                *(_message(bad) for bad in ("12345", f"{THREAD_A}:junk", f"{THREAD_A}/{REPLY_A1}")),
            ],
        ),
        (
            "request_invalid",
            [
                _yesterday(operation="search"),
                _yesterday(query="release"),
                _yesterday(channel={"kind": "slack"}),
                _yesterday(limit="ten"),
            ],
        ),
        ("operation_required", [{"oldest": _rfc3339(YESTERDAY), "latest": _rfc3339(TODAY)}]),
    ],
)
def test_shape_refusals_name_their_reason(
    stack: Stack, code: str, bodies: list[dict[str, Any]]
) -> None:
    _, token = stack.open()
    for body in bodies:
        detail = _refused(stack.read(token, **body), 422, f"channel_read.{code}")
        assert "release" not in json.dumps(detail), "the message names the location, never input"
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


def test_shape_liveness_at_every_bound(stack: Stack) -> None:
    _, token = stack.open()
    week = _ok(stack.read(token, **_yesterday(oldest=_rfc3339(TODAY - timedelta(days=7)))))
    assert PARENT_USER in _ids(week)
    # A first page with no latest runs up to now.
    _ok(stack.read(token, **_HISTORY_FROM))
    # An empty page is success, not an error.
    quiet = TODAY - timedelta(days=5)
    hour = {"oldest": _rfc3339(quiet), "latest": _rfc3339(quiet + timedelta(hours=1))}
    empty = _ok(stack.read(token, **_yesterday(**hour)))
    assert (empty["messages"], empty["has_more"]) == ([], False)
    for limit in (1, 100):
        _ok(stack.read(token, **_yesterday(limit=limit)))
        assert _params(stack.slack.calls("conversations.history")[-1])["limit"] == str(limit)
    _ok(stack.read(token, **_thread(THREAD_A)))
    _ok(stack.read(token, **_message(f"{THREAD_A}:{REPLY_A1}")))


# -- Cursors ------------------------------------------------------------------- #
def _first_page_with_cursor(stack: Stack, token: str) -> dict[str, Any]:
    page = _ok(stack.read(token, **_HISTORY_FROM, limit=2))
    assert page["has_more"] is True and page["next_cursor"]
    return page


def test_cursor_continuation_keeps_the_window_and_refuses_tampering(stack: Stack) -> None:
    deployed, token = stack.open(extra=(_named(CHAN_B),))
    first = _first_page_with_cursor(stack, token)
    first_call = _params(stack.slack.calls("conversations.history")[0])
    assert "cursor" not in first_call
    second = _ok(stack.read(token, cursor=first["next_cursor"]))
    second_call = _params(stack.slack.calls("conversations.history")[1])
    assert second_call["cursor"] == _encode_cursor(2), "the provider cursor is passed through"
    assert Decimal(second_call["oldest"]) == Decimal(first_call["oldest"])
    assert Decimal(second_call["latest"]) == Decimal(first_call["latest"]), "latest fixed on page 1"
    assert second_call["limit"] == "2"
    assert set(_ids(first) + _ids(second)) == {PARENT_USER, PARENT_BOT, THREAD_A, AT_MIDNIGHT}
    assert len(_ids(first) + _ids(second)) == 4
    # The same continuation with matching bounds supplied is also accepted.
    cursor, latest = first["next_cursor"], _rfc3339(_moment(first_call["latest"]))
    _ok(stack.read(token, **_yesterday(latest=latest, cursor=cursor)))

    stack.slack.requests.clear()
    middle = len(cursor) // 2
    flipped = cursor[:middle] + ("A" if cursor[middle] != "A" else "B") + cursor[middle + 1 :]
    other_turn = stack.mint(deployed)["token"]
    for capability, body in (
        (token, {"cursor": flipped}),
        (token, {"cursor": cursor, "channel": _named(CHAN_B)}),
        (token, {"cursor": cursor, "operation": "thread", "thread_id": THREAD_A}),
        (token, {"cursor": cursor, "oldest": _rfc3339(YESTERDAY - timedelta(hours=1))}),
        (token, {"cursor": cursor, "latest": _rfc3339(TODAY + timedelta(hours=1))}),
        (other_turn, {"cursor": cursor}),
    ):
        _refused(stack.read(capability, **body), 422, "channel_read.cursor_invalid")
    # Authorization precedes cursor checks: an unbound channel is not_bound.
    unbound = stack.read(token, cursor=cursor, channel=_named(CHAN_UNBOUND))
    _refused(unbound, 403, "channel_read.not_bound")
    assert stack.slack.requests == []
    _ok(stack.read(token, cursor=cursor))  # the untouched cursor still continues


# -- The eight page budget ----------------------------------------------------- #
def test_eighth_page_succeeds_and_ninth_is_refused_and_a_fresh_event_starts_over(
    stack: Stack,
) -> None:
    deployed = stack.deploy()
    first = stack.mint(deployed)
    mixed = [_yesterday(), _thread(), _message(PARENT_USER), _message(f"{THREAD_A}:{REPLY_A1}")]
    for body in mixed + [_yesterday(limit=1), _thread(limit=1), _yesterday(), _thread()]:
        _ok(stack.read(first["token"], **body))
    with stack.no_provider_calls():
        for body in (_yesterday(), _message(PARENT_USER)):
            _refused(stack.read(first["token"], **body), 429, "channel_read.page_budget_exhausted")
    second = stack.mint(deployed)
    assert second["turn_key"] != first["turn_key"]
    _spend(stack, second["token"], 8)


def test_concurrent_reservations_never_exceed_eight(stack: Stack) -> None:
    from curie_api.channel_read.ledger import ChannelReadLedger

    agent, turn = uuid.uuid4(), _event()

    async def race(client: aioredis.Redis) -> list[str]:
        ledger = ChannelReadLedger(client, _prefix())
        gen = await ledger.open(agent, turn, _owner(), 3600, resume=False)
        assert isinstance(gen, int)
        return list(await asyncio.gather(*(ledger.reserve(agent, turn, gen) for _ in range(20))))

    stack.agents.append(str(agent))
    outcomes = _valkey_run(race)
    assert (outcomes.count("reserved"), outcomes.count("exhausted")) == (8, 12)


def test_approval_resume_shares_the_budget(stack: Stack) -> None:
    deployed = stack.deploy()
    event = _event()
    opened = stack.mint(deployed, event_id=event)
    _spend(stack, opened["token"], 5)
    resume_event = _insert_approval(deployed.agent_id, event)
    resumed = stack.mint(deployed, event_id=resume_event)
    assert resumed["turn_key"] == opened["turn_key"]
    _budget_left(stack, resumed["token"], 3)
    # A resume of a resume stays on the same logical turn.
    second_resume = stack.mint(deployed, event_id=_insert_approval(deployed.agent_id, resume_event))
    assert second_resume["turn_key"] == opened["turn_key"]
    _budget_left(stack, second_resume["token"], 0)
    # Another agent's approval and a missing row do not resolve.
    other = stack.deploy(channel=_named(CHAN_OTHER_AGENT))
    missing_row = f"approval-{uuid.uuid4()}-resolved"
    for unresolvable in (_insert_approval(other.agent_id, _event()), missing_row):
        response = stack.mint_response(deployed, event_id=unresolvable)
        _refused(response, 409, "channel_read.turn_unresolvable")


def test_resume_after_ledger_expiry_is_turn_expired(stack: Stack) -> None:
    from curie_internal.channel_read_ledger import ledger_keys

    deployed = stack.deploy()
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


# -- Generations and revocation ------------------------------------------------ #
def test_steer_and_terminal_revoke_generations(stack: Stack) -> None:
    deployed = stack.deploy()

    def opened() -> tuple[str, str, dict[str, Any]]:
        event, owner = _event(), _owner()
        return event, owner, stack.mint(deployed, event_id=event, owner=owner)

    # A steer bumps the generation, refuses the old one, and keeps the budget.
    event, _, context = opened()
    _spend(stack, context["token"], 3)
    steered = stack.mint(deployed, event_id=event, mode="steer")
    assert steered["generation"] == context["generation"] + 1
    assert steered["turn_key"] == context["turn_key"]
    _refused(stack.read(context["token"], **_yesterday()), 409, "channel_read.turn_inactive")
    _budget_left(stack, steered["token"], 5)
    # Terminal, then steer: no capability, nor for a steer on a turn that never opened.
    event, owner, context = opened()
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is True
    for steer_event in (event, _event()):
        steer = stack.mint_response(deployed, event_id=steer_event, mode="steer")
        _refused(steer, 409, "channel_read.turn_inactive")
        assert "token" not in steer.json()
    # Steer, then terminal: the steer token dies with the turn.
    event, owner, context = opened()
    steered = stack.mint(deployed, event_id=event, mode="steer")
    _ok(stack.read(steered["token"], **_yesterday()))
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is True
    _refused(stack.read(steered["token"], **_yesterday()), 409, "channel_read.turn_inactive")
    # A resume's opener survives a late revoke from the original owner.
    event, owner, context = opened()
    resumed = stack.mint(deployed, event_id=_insert_approval(deployed.agent_id, event))
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is False
    _ok(stack.read(resumed["token"], **_yesterday()))


def test_a_revoke_issued_while_the_api_is_down_is_refused_after_recovery(stack: Stack) -> None:
    deployed = stack.deploy()
    owner = _owner()
    context = stack.mint(deployed, owner=owner)
    _ok(stack.read(context["token"], **_yesterday()))
    # Liveness: only the owner's revoke counts.
    assert _revoke_owner(deployed.agent_id, context["turn_key"], _owner()) is False
    _ok(stack.read(context["token"], **_yesterday()))
    stack.stop()
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is True
    stack.start()
    assert context["expires_at"] > datetime.now(UTC).timestamp()
    with stack.no_provider_calls():
        _refused(stack.read(context["token"], **_yesterday()), 409, "channel_read.turn_inactive")


# -- Grant and binding are rechecked on every read ----------------------------- #
def test_stopped_or_ungranted_deployment_is_grant_revoked(stack: Stack) -> None:
    deployed, token = stack.open()
    _ok(stack.read(token, **_yesterday()))
    stack.undeploy(deployed)
    with stack.no_provider_calls():
        _refused(stack.read(token, **_yesterday()), 409, "channel_read.grant_revoked")
        redeployed = stack.redeploy(deployed.agent_id, None)
        _refused(stack.read(token, **_yesterday()), 409, "channel_read.grant_revoked")
        # Forging the new deployment into the claims does not help: the grant
        # digest in the token no longer matches the deployed bundle.
        forged = _resign(token, deployment=redeployed.deployment_id)
        _refused(stack.read(forged, **_yesterday()), 409, "channel_read.grant_revoked")
    _refused(stack.mint_response(redeployed), 409, "channel_read.grant_absent")


# -- The capability itself ----------------------------------------------------- #
@pytest.mark.parametrize(
    "forge",
    "forged_signature expired wir_prefix sbx_prefix platform_key missing garbage sibling".split(),
)
def test_invalid_capabilities_never_reach_slack(stack: Stack, forge: str) -> None:
    _, token = stack.open()
    prefix, payload, signature = token.split(".")
    now = int(datetime.now(UTC).timestamp())
    mac = hmac.new(b"not-the-key", payload.encode(), hashlib.sha256).digest()
    fake = base64.urlsafe_b64encode(mac).rstrip(b"=").decode()
    headers = {
        "forged_signature": {CAPABILITY_HEADER: f"{prefix}.{payload}.{fake}"},
        "expired": {CAPABILITY_HEADER: _resign(token, iat=now - 7200, exp=now - 1)},
        "wir_prefix": {CAPABILITY_HEADER: f"wir.{payload}.{signature}"},
        "sbx_prefix": {CAPABILITY_HEADER: f"sbx.{payload}.{signature}"},
        "platform_key": dict(stack.auth),
        "missing": {},
        "garbage": {CAPABILITY_HEADER: "chr.not-base64.sig"},
        # A sibling capability header does not authorize this route either.
        "sibling": {"X-Curie-Issue-Read": token},
    }[forge]
    response = stack.http.post(READ_URL, json=_yesterday(), headers=headers)
    _refused(response, 401, "channel_read.invalid_capability")
    assert stack.slack.requests == []


def test_a_capability_minted_before_the_grants_claim_is_invalid(stack: Stack) -> None:
    """ADR 0200 adds the bundle's grants to the strict claims, so a token signed
    with the right key in the old shape (no ``grants``) fails closed."""
    from curie_internal import sandbox_token

    _, token = stack.open()
    prefix, payload, _ = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    assert "grants" in claims, "the minted claims name the bundle's grants"
    del claims["grants"]
    encoded = json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
    signed = f"{prefix}.{base64.urlsafe_b64encode(encoded).rstrip(b'=').decode()}"
    old_shape = f"{signed}.{sandbox_token.signature(get_settings().api_key, signed)}"
    _refused(stack.read(old_shape, **_yesterday()), 401, "channel_read.invalid_capability")
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


def test_a_canvas_only_bundle_mints_but_never_reads_history(stack: Stack) -> None:
    """ADR 0200: any platform Slack grant mints the capability, but history needs
    ``channelRead`` itself, refused after the digest check and before any Slack call."""
    name = f"canvas-{uuid.uuid4().hex[:8]}"
    agent_id = stack.admin("POST", "/agents", json={"name": name, "channel": _named(CHAN_A)})["id"]
    stack.agents.append(agent_id)
    label = {"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "operator"}
    version_id = str(stack.admin("POST", f"/agents/{agent_id}/versions", json=label)["id"])
    bundle = {"file": ("reader-bot.tar.gz", _grants_archive({"canvasRead": True}))}
    stack.admin("PUT", f"/agents/{agent_id}/versions/{version_id}/bundle", files=bundle)
    body = {"agent_id": agent_id, "version_id": version_id, "environment": "dev"}
    deployment_id = str(stack.admin("POST", "/deployments", json=body)["id"])
    deployed = Deployed(agent_id, version_id, deployment_id)
    token = stack.mint(deployed)["token"]
    for read in (_yesterday(), _thread(), _message(PARENT_USER)):
        detail = _refused(stack.read(token, **read), 403, "channel_read.history_not_granted")
        assert SECRET_TEXT not in json.dumps(detail)
    assert stack.slack.requests == []
    # The digest check still comes first: a stopped deployment is grant_revoked.
    stack.undeploy(deployed)
    _refused(stack.read(token, **_yesterday()), 409, "channel_read.grant_revoked")
    assert stack.slack.requests == []


# -- Provider failures --------------------------------------------------------- #
def test_slack_rate_limit_cools_down_every_agent_and_charges_no_page(stack: Stack) -> None:
    _, first = stack.open()
    _, second = stack.open(channel=_named(CHAN_OTHER_AGENT))
    other = _yesterday(channel=_named(CHAN_OTHER_AGENT))
    stack.slack.fail_next("conversations.history", _slack_error("ratelimited", 429, 2))
    try:
        response = stack.read(first, **_yesterday())
        detail = _refused(response, 429, "channel_read.provider_rate_limited")
        assert (detail["retry_after"], response.headers["retry-after"]) == (2, "2")
        # Same bot identity and method, another agent: refused with no Slack call.
        with stack.no_provider_calls():
            cooled = stack.read(second, **other)
            detail = _refused(cooled, 429, "channel_read.provider_rate_limited")
            assert 0 < detail["retry_after"] <= 2
            assert cooled.headers["retry-after"] == str(detail["retry_after"])
            _refused(stack.read(first, **_yesterday()), 429, "channel_read.provider_rate_limited")
        # Liveness: another method is not cooled down.
        _ok(stack.read(first, **_thread()))
        # Slack can also answer 200 with the ratelimited error.
        stack.slack.fail_next("conversations.replies", _slack_error("ratelimited", 200, 2))
        _refused(stack.read(first, **_thread()), 429, "channel_read.provider_rate_limited")
    finally:
        time.sleep(2.5)  # let the cooldowns lapse so no state leaks into other tests
    _ok(stack.read(second, **other))
    # No rate limited attempt charged a page: one thread read is spent, seven remain.
    _budget_left(stack, first, 7)


def test_provider_errors_and_a_few_misses_release_the_reservation(stack: Stack) -> None:
    _, token = stack.open()
    for failure in (
        _slack_error("fatal_error"),
        httpx.Response(500, text="<html>upstream</html>"),
        httpx.Response(200, json={"ok": True}),
    ):
        stack.slack.fail_next("conversations.history", failure)
        _refused(stack.read(token, **_yesterday()), 502, "channel_read.provider_error")
    # A few misses stay under the attempt cap too.
    for _ in range(3):
        _refused(stack.read(token, **_message(MISSING)), 404, "channel_read.message_not_found")
    _budget_left(stack, token, 8)


def test_failed_provider_attempts_hit_a_per_turn_attempt_cap(stack: Stack) -> None:
    _, token = stack.open()
    codes: list[str] = []
    while len(codes) < 64 and "channel_read.attempt_budget_exhausted" not in codes:
        response = stack.read(token, **_message(MISSING))
        codes.append(response.json()["detail"]["code"])
    assert set(codes[:-1]) == {"channel_read.message_not_found"}, codes
    _refused(response, 429, "channel_read.attempt_budget_exhausted")  # 64 misses never capped
    with stack.no_provider_calls():
        response = stack.read(token, **_message(MISSING))
        _refused(response, 429, "channel_read.attempt_budget_exhausted")


@pytest.mark.parametrize("stack", [{"SLACK_BOT_TOKEN": ""}], indirect=True)
def test_unconfigured_identity_is_refused_without_a_provider_call(stack: Stack) -> None:
    _, token = stack.open()
    _refused(stack.read(token, **_yesterday()), 503, "channel_read.provider_unconfigured")
    assert stack.slack.requests == []


def test_valkey_outage_is_503(stack: Stack) -> None:
    deployed, token = stack.open()
    healthy = stack.http.app.state.valkey
    stack.http.app.state.valkey = aioredis.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.5, retry=Retry(NoBackoff(), 0)
    )
    try:
        _refused(stack.read(token, **_yesterday()), 503, "channel_read.unavailable")
        _refused(stack.mint_response(deployed), 503, "channel_read.unavailable")
    finally:
        stack.http.app.state.valkey = healthy
    assert stack.slack.requests == []
    _ok(stack.read(token, **_yesterday()))


# -- Hygiene: logs and stored state -------------------------------------------- #
def test_token_and_bodies_stay_out_of_logs(stack: Stack, caplog: pytest.LogCaptureFixture) -> None:
    deployed = stack.deploy()
    with caplog.at_level(logging.DEBUG):
        token = stack.mint(deployed)["token"]
        page = _ok(stack.read(token, **_message(PARENT_USER)))
        assert page["messages"][0]["text"] == SECRET_TEXT
        unbound = stack.read(token, **_yesterday(channel=_named(CHAN_UNBOUND)))
        _refused(unbound, 403, "channel_read.not_bound")
    for secret in (token, token.split(".")[2], SECRET_TEXT, BOT_TOKEN):
        assert secret not in caplog.text

    async def values(client: aioredis.Redis) -> list[bytes]:
        found = []
        async for key in client.scan_iter(match=f"{_prefix()}:channel-read:*"):
            if await client.type(key) in (b"string", "string"):
                found.append(await client.get(key) or b"")
        return found

    stored = b"".join(v if isinstance(v, bytes) else v.encode() for v in _valkey_run(values))
    assert SECRET_TEXT.encode() not in stored
    assert token.encode() not in stored


# -- Slack paging and thread shapes -------------------------------------------- #
def test_history_excludes_thread_broadcast_replies(stack: Stack) -> None:
    # conversations.history returns a reply posted with "Also send to channel"
    # as subtype thread_broadcast, with thread_ts naming its parent and a root
    # copy of the parent (https://docs.slack.dev/reference/methods/conversations.history,
    # https://docs.slack.dev/reference/events/message/thread_broadcast).
    broadcast_ts = _ts(YESTERDAY + timedelta(hours=15, minutes=20))
    broadcast = _reply(broadcast_ts, "broadcast: we hold until Friday", THREAD_A, "U0CAROL003")
    broadcast.update({"subtype": "thread_broadcast", "root": {**stack.slack.channels[CHAN_A][3]}})
    stack.slack.channels[CHAN_A].append(broadcast)
    stack.slack.threads[(CHAN_A, THREAD_A)].append({**broadcast})
    _, token = stack.open()
    page = _ok(stack.read(token, **_yesterday()))
    assert THREAD_A in _ids(page), "the parent the broadcast belongs to is retained"
    assert broadcast_ts not in _ids(page) and f"{THREAD_A}:{broadcast_ts}" not in _ids(page)
    assert broadcast["text"] not in [record["text"] for record in page["messages"]]
    assert all(record.get("thread_id") is None for record in page["messages"])
    # The reply stays reachable where it belongs: in its thread.
    assert f"{THREAD_A}:{broadcast_ts}" in _ids(_ok(stack.read(token, **_thread())))


def test_time_paginated_history_continues_without_a_provider_cursor(stack: Stack) -> None:
    stack.slack.time_paginated = True
    _, token = stack.open()
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
    assert not set(_ids(first)) & set(_ids(second)), "no duplicates across pages"
    assert sorted(_ids(first) + _ids(second)) == sorted([PARENT_USER, PARENT_BOT, THREAD_A])
    assert second["has_more"] is False


def _drain_replies(stack: Stack, token: str, page: dict[str, Any]) -> None:
    """Follow a thread's cursors; together the pages hold each reply once, never the parent."""
    seen: list[str] = []
    for _ in range(4):
        assert THREAD_A not in _ids(page), "the parent is never returned as a reply"
        assert all(record["thread_id"] == THREAD_A for record in page["messages"])
        seen.extend(_ids(page))
        if not page["has_more"]:
            break
        assert page.get("next_cursor"), "has_more needs a cursor to continue"
        page = _ok(stack.read(token, cursor=page["next_cursor"]))
    assert len(seen) == len(set(seen)), "no duplicates across pages"
    assert set(seen) == REPLY_IDS, "the replies after a parent only page were lost"


def test_thread_limit_one_continues_past_a_parent_only_page(stack: Stack) -> None:
    stack.slack.time_paginated = True
    stack.slack.replies_parent_in_limit = True
    _, token = stack.open()
    _drain_replies(stack, token, _ok(stack.read(token, **_thread(limit=1))))
    first_call = _params(stack.slack.calls("conversations.replies")[0])
    for params in map(_params, stack.slack.calls("conversations.replies")):
        assert params["ts"] == THREAD_A
        assert Decimal(params["oldest"]) >= Decimal(first_call["oldest"]), "no window widening"
        assert Decimal(params["latest"]) <= Decimal(first_call["latest"]), "no window widening"


def _parent_only(body: dict[str, Any]) -> dict[str, Any]:
    """conversations.replies with the parent alone: it is always first and counts toward
    `limit` (https://docs.slack.dev/reference/methods/conversations.replies), `has_more`
    is true, and time based paging offers no `response_metadata.next_cursor`."""
    return {"ok": True, "messages": body["messages"][:1], "has_more": True}


# A reply window that opens after the parent's ts and before both replies.
_AFTER_PARENT = _thread(oldest=_rfc3339(_moment(THREAD_A) + timedelta(minutes=1)), limit=1)


def test_a_parent_only_replies_page_is_incomplete_when_stuck_and_continues_otherwise(
    stack: Stack,
) -> None:
    deployed, token = stack.open()
    stack.slack.tamper["conversations.replies"] = _parent_only
    # Never false exhaustion: an empty, finished page would lose both replies.
    _refused(stack.read(token, **_AFTER_PARENT), 502, "channel_read.provider_incomplete")
    assert stack.slack.calls("conversations.replies"), "the provider was asked"
    del stack.slack.tamper["conversations.replies"]
    _budget_left(stack, token, 8)

    # Only the first answer is parent only: the read continues past it.
    token = stack.mint(deployed)["token"]
    answered: list[int] = []

    def first_call_parent_only(body: dict[str, Any]) -> dict[str, Any]:
        answered.append(1)
        return _parent_only(body) if len(answered) == 1 else body

    stack.slack.tamper["conversations.replies"] = first_call_parent_only
    _drain_replies(stack, token, _ok(stack.read(token, **_AFTER_PARENT)))
