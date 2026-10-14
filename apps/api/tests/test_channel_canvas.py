"""Canvas list, read and cell edit on an agent's bound channels (ADR 0200, #3819).

Everything drives the real app: real routes, Postgres, Valkey, the object store,
and the internal route the worker mints the capability from. Only Slack is
replaced, at the HTTP seam. The fake replays shapes recorded on 2026-10-05 from a
dev workspace with a bot token, anonymized: team, file, channel and user ids,
URLs and section ids are placeholders, and the workspace name is gone.

files.info https://docs.slack.dev/reference/methods/files.info (the `file`
object: `filetype: "quip"` and `mimetype: "application/vnd.slack-docs"` for a
canvas, `channels` and `groups` naming where it is shared, `url_private` on
files.slack.com; `file_not_found` or `file_deleted` for a file it cannot see).
files.list https://docs.slack.dev/reference/methods/files.list (`types=canvas`
filters to canvases, https://docs.slack.dev/surfaces/canvases/#finding-canvases-with-fileslist;
a channel canvas is titled "Untitled"; `created` is epoch seconds; `paging` is
`{count, total, page, pages}`).
conversations.info https://docs.slack.dev/reference/methods/conversations.info
(`channel.is_member`; `channel_not_found`; `missing_scope` with `needed`).
canvases.edit https://docs.slack.dev/reference/methods/canvases.edit (success is
`{"ok": true}`; an unknown section is `canvas_editing_failed` with a `detail`
of `Section <id> was not found`).
canvases.sections.lookup https://docs.slack.dev/reference/methods/canvases.sections.lookup
is not the read path: it returns ids only, no text.
The `url_private` download is the canvas as HTML under a `quip-canvas-content`
root, every table cell one `<p id="temp:C:..." class="line">`; a download Slack
does not authorize answers a sign-in page instead.
Rate limits https://docs.slack.dev/apis/web-api/rate-limits (HTTP 429 with
`Retry-After`, or `{"ok": false, "error": "ratelimited"}`).
"""

from __future__ import annotations

import asyncio
import copy
import html
import json
import logging
import re
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import redis.asyncio as aioredis
from curie_api.channel_read.provider_guard import ProviderGuard
from curie_api.config import get_settings
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_channel_read import (
    CAPABILITY_HEADER,
    CHAN_A,
    CHAN_B,
    CHAN_NOT_MEMBER,
    CHAN_UNBOUND,
    EMAIL,
    WORKER_TOKEN,
    Deployed,
    FakeSlack,
    Stack,
    _event,
    _grants_archive,
    _insert_approval,
    _ledger_keys_of,
    _named,
    _ok,
    _owner,
    _params,
    _refused,
    _revoke_owner,
    _seeded_slack,
    _slack_error,
    _valkey_run,
    _yesterday,
)

CANVAS_URL = "/channel-canvas"
CANVAS_BOT_TOKEN = "xoxb-channel-canvas-fixture"
SECOND_BOT_TOKEN = "xoxb-channel-canvas-second-identity"
SLACK_API_HOST = "slack.com"
FILES_HOST = "files.slack.com"
CANVAS_MIME = "application/vnd.slack-docs"
TEAM = "T0EXAMPLE1"
USER = "U0ALICE001"
CREATED = 1791219412

# Two more channels than the read tests seed: a private one shared through
# `groups`, and a member channel that sorts after CHAN_NOT_MEMBER.
CHAN_PRIVATE = "G0EXAMPLE6"
CHAN_LATE = "C0EXAMPLE7"

CANVAS = "F0EXAMPLE01"  # the fixture canvas, shared into CHAN_A
CANVAS_B = "F0EXAMPLE02"  # shared into CHAN_B
CANVAS_UNBOUND = "F0EXAMPLE03"  # shared into CHAN_UNBOUND
CANVAS_PRIVATE = "F0EXAMPLE04"  # shared into CHAN_PRIVATE through `groups`
CANVAS_TWO_SHARES = "F0EXAMPLE05"  # shared into CHAN_NOT_MEMBER and CHAN_LATE
CANVAS_NOT_MEMBER = "F0EXAMPLE06"  # shared into CHAN_NOT_MEMBER only
MISSING_CANVAS = "F0EXAMPLE99"

CANVAS_GRANTS: Mapping[str, Any] = {"canvasList": True, "canvasRead": True, "canvasEdit": True}
ALL_GRANTS: Mapping[str, Any] = {"channelRead": True, **CANVAS_GRANTS}

TWO_IDENTITIES = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": None,
        },
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
            "signing_secret_env": None,
        },
    ]
)


# -- Recorded shapes, anonymized ---------------------------------------------- #
def _sid(tag: str, n: int) -> str:
    """A section id shaped like Slack's `temp:C:` ids (same alphabet and length)."""
    return f"temp:C:{tag}{n:025x}"


INTRO = "This canvas is a fixture for the Curie canvas live test. Edits reset it."
SENTINEL = _sid("EXA", 8)
BASELINE = "sentinel-baseline"


def _fixture_html(tag: str = "EXA") -> str:
    """The recorded `url_private` download, verbatim in structure: an `h1`, one
    `p`, then two tables whose every cell is one `<p id=... class="line">`."""
    s = [_sid(tag, n) for n in range(16)]
    return (
        '<div class="quip-canvas-content">'
        f'<h1 id="{s[1]}">Curie canvas fixture</h1>'
        f'<p id="{s[2]}" class="line">{INTRO}</p>'
        "<table>"
        f'<tr><td><p id="{s[3]}" class="line">Item</p></td>'
        f'<td><p id="{s[4]}" class="line">Owner</p></td>'
        f'<td><p id="{s[5]}" class="line">Status</p></td></tr>'
        f'<tr><td><p id="{s[6]}" class="line">Weekly plan review</p></td>'
        f'<td><p id="{s[7]}" class="line">Platform</p></td>'
        f'<td><p id="{s[8]}" class="line">sentinel-baseline</p></td></tr>'
        f'<tr><td><p id="{s[9]}" class="line">Fixture second row</p></td>'
        f'<td><p id="{s[10]}" class="line">Platform</p></td>'
        f'<td><p id="{s[11]}" class="line">Not started</p></td></tr>'
        "</table>"
        "<table>"
        f'<tr><td><p id="{s[12]}" class="line">Risk</p></td>'
        f'<td><p id="{s[13]}" class="line">Mitigation</p></td></tr>'
        f'<tr><td><p id="{s[14]}" class="line">Fixture drift</p></td>'
        f'<td><p id="{s[15]}" class="line">Reset by edit</p></td></tr>'
        "</table>"
        "</div>"
    )


FIXTURE_HTML = _fixture_html()


def _cell(tag: str, n: int, value: str) -> dict[str, Any]:
    return {"section_id": _sid(tag, n), "text": value, "truncated": False}


def _expected_tables(tag: str = "EXA") -> list[dict[str, Any]]:
    return [
        {
            "header": [_cell(tag, 3, "Item"), _cell(tag, 4, "Owner"), _cell(tag, 5, "Status")],
            "rows": [
                [
                    _cell(tag, 6, "Weekly plan review"),
                    _cell(tag, 7, "Platform"),
                    _cell(tag, 8, "sentinel-baseline"),
                ],
                [
                    _cell(tag, 9, "Fixture second row"),
                    _cell(tag, 10, "Platform"),
                    _cell(tag, 11, "Not started"),
                ],
            ],
        },
        {
            "header": [_cell(tag, 12, "Risk"), _cell(tag, 13, "Mitigation")],
            "rows": [[_cell(tag, 14, "Fixture drift"), _cell(tag, 15, "Reset by edit")]],
        },
    ]


def _expected_paragraphs(tag: str = "EXA") -> list[dict[str, Any]]:
    return [_cell(tag, 1, "Curie canvas fixture"), _cell(tag, 2, INTRO)]


# What Slack serves at `url_private` when it does not accept the credential.
LOGIN_PAGE = (
    '<!DOCTYPE html><html lang="en-US"><head><meta charset="utf-8">'
    '<title>Slack</title></head><body><div id="signin"><h1>Sign in to Slack</h1>'
    '<form action="https://example.slack.com/" method="post"></form></div></body></html>'
)


def _share() -> list[dict[str, Any]]:
    return [
        {
            "ts": "1791219413.657369",
            "channel_name": "fixture-channel",
            "team_id": TEAM,
            "access": "write",
            "share_user_id": USER,
            "source": "CHANNEL_TAB",
            "is_silent_share": True,
            "reply_users": [],
            "reply_users_count": 0,
            "reply_count": 0,
        }
    ]


def _file(
    file_id: str,
    *,
    channels: tuple[str, ...] = (CHAN_A,),
    groups: tuple[str, ...] = (),
    title: str = "Untitled",
    filetype: str = "quip",
    mimetype: str = CANVAS_MIME,
    created: int = CREATED,
) -> dict[str, Any]:
    """The `file` object of files.info (and of a files.list item), as recorded:
    the fields the API reads plus a few it ignores."""
    shares: dict[str, Any] = {}
    if channels:
        shares["public"] = {channel: _share() for channel in channels}
    if groups:
        shares["private"] = {group: _share() for group in groups}
    return {
        "id": file_id,
        "created": created,
        "timestamp": created,
        "name": "-",
        "title": title,
        "mimetype": mimetype,
        "filetype": filetype,
        "pretty_type": "Canvas" if filetype == "quip" else filetype.upper(),
        "user": USER,
        "user_team": TEAM,
        "editable": True,
        "size": 871,
        "mode": "quip" if filetype == "quip" else "hosted",
        "is_external": False,
        "is_public": True,
        "url_private": f"https://{FILES_HOST}/files-pri/{TEAM}-{file_id}/canvas",
        "url_private_download": f"https://{FILES_HOST}/files-pri/{TEAM}-{file_id}/download/canvas",
        "permalink": f"https://example.slack.com/docs/{TEAM}/{file_id}",
        "shares": shares,
        "channels": list(channels),
        "groups": list(groups),
        "ims": [],
        "has_more_shares": False,
        "access": "owner",
        "file_access": "visible",
        "canvas_creator_id": USER,
        "comments_count": 0,
    }


def _bearer(request: httpx.Request) -> str:
    value = request.headers.get("authorization", "")
    return value.removeprefix("Bearer ")


@dataclass
class FakeCanvas:
    info: dict[str, Any]
    html: str


CANVAS_METHODS = frozenset({"files.info", "files.list", "conversations.info", "canvases.edit"})
DOWNLOAD = "url_private"  # the `scripted` key for the download


@dataclass
class FakeCanvasSlack(FakeSlack):
    """Slack's canvas surface on top of the read fake (history stays served)."""

    tokens: set[str] = field(default_factory=set)
    canvases: dict[str, FakeCanvas] = field(default_factory=dict)
    # Called when canvases.edit arrives, before anything is applied.
    on_edit: Callable[[httpx.Request], None] | None = None
    # Called when a canvas download arrives, before it is answered: the moment a
    # download is in flight and authority can change underneath it.
    on_download: Callable[[httpx.Request], None] | None = None
    # Answers after a replace was applied; each may raise to drop the connection.
    after_apply: list[Callable[[httpx.Request], httpx.Response]] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host != SLACK_API_HOST:
            return self._download(request)
        method = request.url.path.rsplit("/", 1)[-1]
        if method not in CANVAS_METHODS:
            return super().handler(request)
        self.requests.append(request)
        if queued := self.scripted.get(method):
            return queued.pop(0)
        if _bearer(request) not in self.tokens:
            return _slack_error("invalid_auth")
        if method == "canvases.edit":
            return self._edit(request)
        params = _params(request)
        if method == "files.info":
            canvas = self.canvases.get(params.get("file", ""))
            if canvas is None:
                return _slack_error("file_not_found")
            body: dict[str, Any] = {
                "ok": True,
                "file": copy.deepcopy(canvas.info),
                "comments": [],
                "response_metadata": {"next_cursor": ""},
            }
        elif method == "files.list":
            body = self._list(params)
        else:
            channel = params.get("channel", "")
            if channel not in self.channels:
                return _slack_error("channel_not_found")
            body = {"ok": True, "channel": self._channel(channel)}
        transform = self.tamper.get(method)
        return httpx.Response(200, json=transform(body) if transform else body)

    def _list(self, params: dict[str, str]) -> dict[str, Any]:
        channel = params.get("channel", "")
        wants_canvases = "canvas" in params.get("types", "").split(",")
        files = [
            copy.deepcopy(c.info)
            for c in self.canvases.values()
            if wants_canvases
            and c.info["filetype"] == "quip"
            and channel in c.info["channels"] + c.info["groups"]
        ]
        files.sort(key=lambda f: f["created"], reverse=True)
        count = int(params.get("count") or 100)
        return {
            "ok": True,
            "files": files,
            "paging": {"count": count, "total": len(files), "page": 1, "pages": 1},
        }

    def _channel(self, channel: str) -> dict[str, Any]:
        tabs = [
            {"id": f"Ct{fid[1:]}", "type": "canvas", "data": {"file_id": fid}, "label": ""}
            for fid, c in self.canvases.items()
            if channel in c.info["channels"]
        ][:1]
        return {
            "id": channel,
            "name": "fixture-channel",
            "is_channel": True,
            "is_group": False,
            "is_im": False,
            "is_mpim": False,
            "is_private": channel.startswith("G"),
            "is_archived": False,
            "is_general": False,
            "is_member": channel in self.members,
            "created": CREATED - 86400,
            "creator": USER,
            "properties": {"tabs": tabs},
        }

    def _download(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.host != FILES_HOST or request.url.scheme != "https":
            # Whatever answers on another host would gladly take the token.
            return httpx.Response(200, text=FIXTURE_HTML)
        if self.on_download is not None:
            self.on_download(request)
        if queued := self.scripted.get(DOWNLOAD):
            return queued.pop(0)
        url = str(request.url)
        canvas = next((c for c in self.canvases.values() if c.info["url_private"] == url), None)
        if canvas is None:
            return httpx.Response(404, text="Not Found")
        if _bearer(request) not in self.tokens:
            return httpx.Response(200, headers={"content-type": "text/html"}, text=LOGIN_PAGE)
        return httpx.Response(
            200, headers={"content-type": "text/html; charset=utf-8"}, text=canvas.html
        )

    def _edit(self, request: httpx.Request) -> httpx.Response:
        if self.on_edit is not None:
            self.on_edit(request)
        body = json.loads(request.content)
        canvas = self.canvases.get(body.get("canvas_id", ""))
        if canvas is None:
            return _slack_error("canvas_not_found")
        changes = body.get("changes", [])
        for change in changes:
            section = change.get("section_id", "")
            if f'id="{section}"' not in canvas.html:
                return httpx.Response(
                    200,
                    json={
                        "ok": False,
                        "error": "canvas_editing_failed",
                        "detail": f"Section {section} was not found",
                    },
                )
        for change in changes:
            markdown = change["document_content"]["markdown"]
            pattern = re.compile(rf'(<p id="{re.escape(change["section_id"])}"[^>]*>)(.*?)(</p>)')
            canvas.html = pattern.sub(
                lambda m, md=markdown: m.group(1) + html.escape(md, quote=False) + m.group(3),
                canvas.html,
                count=1,
            )
        if self.after_apply:
            return self.after_apply.pop(0)(request)
        return httpx.Response(200, json={"ok": True})

    def downloads(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.host != SLACK_API_HOST]


def _canvas_slack(token: str) -> FakeCanvasSlack:
    seeded = _seeded_slack()
    slack = FakeCanvasSlack(token=token)
    slack.tokens = {t for t in (token, SECOND_BOT_TOKEN) if t}
    slack.channels = {**seeded.channels, CHAN_PRIVATE: [], CHAN_LATE: []}
    slack.threads = seeded.threads
    slack.members = seeded.members | {CHAN_PRIVATE, CHAN_LATE}
    slack.canvases = {
        CANVAS: FakeCanvas(_file(CANVAS), FIXTURE_HTML),
        CANVAS_B: FakeCanvas(_file(CANVAS_B, channels=(CHAN_B,)), _fixture_html("EXB")),
        CANVAS_UNBOUND: FakeCanvas(
            _file(CANVAS_UNBOUND, channels=(CHAN_UNBOUND,)), _fixture_html("EXC")
        ),
        CANVAS_PRIVATE: FakeCanvas(
            _file(CANVAS_PRIVATE, channels=(), groups=(CHAN_PRIVATE,)), _fixture_html("EXD")
        ),
        CANVAS_TWO_SHARES: FakeCanvas(
            _file(CANVAS_TWO_SHARES, channels=(CHAN_NOT_MEMBER, CHAN_LATE)), _fixture_html("EXE")
        ),
        CANVAS_NOT_MEMBER: FakeCanvas(
            _file(CANVAS_NOT_MEMBER, channels=(CHAN_NOT_MEMBER,)), _fixture_html("EXF")
        ),
    }
    return slack


# -- Real stack ---------------------------------------------------------------- #
class CanvasStack(Stack):
    def __init__(self, auth_headers: dict[str, str], token: str) -> None:
        super().__init__(auth_headers)
        self.fake = _canvas_slack(token)
        self.slack = self.fake

    def deploy_with(
        self,
        grants: Mapping[str, Any] = CANVAS_GRANTS,
        *,
        channel: dict[str, str] | None = None,
        extra: tuple[dict[str, str], ...] = (),
    ) -> Deployed:
        name = f"canvas-{uuid.uuid4().hex[:8]}"
        agent = self.admin(
            "POST", "/agents", json={"name": name, "channel": channel or _named(CHAN_A)}
        )
        self.agents.append(agent["id"])
        for binding in extra:
            self.admin("POST", f"/agents/{agent['id']}/channels", json=binding)
        return self.redeploy_with(agent["id"], grants)

    def redeploy_with(self, agent_id: str, grants: Mapping[str, Any]) -> Deployed:
        label = {"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "operator"}
        version_id = str(self.admin("POST", f"/agents/{agent_id}/versions", json=label)["id"])
        bundle = {"file": ("reader-bot.tar.gz", _grants_archive(grants))}
        self.admin("PUT", f"/agents/{agent_id}/versions/{version_id}/bundle", files=bundle)
        body = {"agent_id": agent_id, "version_id": version_id, "environment": "dev"}
        deployment_id = str(self.admin("POST", "/deployments", json=body)["id"])
        return Deployed(agent_id, version_id, deployment_id)

    def open_with(
        self, grants: Mapping[str, Any] = CANVAS_GRANTS, **deploy: Any
    ) -> tuple[Deployed, str]:
        deployed = self.deploy_with(grants, **deploy)
        return deployed, str(self.mint(deployed)["token"])

    def canvas(self, token: str | None, **body: Any) -> httpx.Response:
        headers = {} if token is None else {CAPABILITY_HEADER: token}
        return self.http.post(CANVAS_URL, json=body, headers=headers)


@pytest.fixture
def stack(
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    auth_headers: dict[str, str],
    request: pytest.FixtureRequest,
) -> Iterator[CanvasStack]:
    env = {
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "SLACK_BOT_TOKEN": CANVAS_BOT_TOKEN,
        "RUNS_STREAM": f"test:curie:channel-canvas-runs:{uuid.uuid4().hex}",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "RESUME_RECONCILER_ENABLED": "false",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        **getattr(request, "param", {}),
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    built = CanvasStack(auth_headers, env["SLACK_BOT_TOKEN"])
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
def _list(**extra: Any) -> dict[str, Any]:
    return {"operation": "list", **extra}


def _read(canvas_id: str = CANVAS, **extra: Any) -> dict[str, Any]:
    return {"operation": "read", "canvas_id": canvas_id, **extra}


def _edit(
    value: str = "sentinel-3f9a1c2e",
    *,
    section_id: str = SENTINEL,
    canvas_id: str = CANVAS,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "operation": "edit",
        "canvas_id": canvas_id,
        "section_id": section_id,
        "text": value,
        **extra,
    }


def _edit_body(canvas_id: str, section_id: str, value: str) -> dict[str, Any]:
    """The one-change replace canvases.edit takes."""
    return {
        "canvas_id": canvas_id,
        "changes": [
            {
                "operation": "replace",
                "section_id": section_id,
                "document_content": {"type": "markdown", "markdown": value},
            }
        ],
    }


def _canvas_budget_left(
    stack: CanvasStack, token: str, pages: int, canvas_id: str = CANVAS
) -> None:
    """Exactly `pages` canvas reads still fit the turn's budget; the next is refused."""
    for _ in range(pages):
        _ok(stack.canvas(token, **_read(canvas_id)))
    with stack.no_provider_calls():
        response = stack.canvas(token, **_read(canvas_id))
        _refused(response, 429, "channel_read.page_budget_exhausted")


AUDIT_SQL = text(
    "SELECT agent_id, deployment_id, turn, kind, channel_address, canvas_id, section_id, "
    "before_text, after_text, status, error_code, created_at, completed_at "
    "FROM curie.channel_canvas_edits WHERE agent_id = :agent ORDER BY created_at"
)


def _audit(agent_id: str) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(AUDIT_SQL, {"agent": uuid.UUID(agent_id)})
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _audit_from_slack(agent_id: str) -> list[dict[str, Any]]:
    """The audit rows as committed when Slack receives the edit (the fake runs
    inside the API's event loop, so the query runs on its own thread and loop)."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_audit, agent_id).result()


def _cell_text_in(canvas_html: str, section_id: str) -> str:
    found = re.search(rf'<p id="{re.escape(section_id)}"[^>]*>(.*?)</p>', canvas_html)
    assert found is not None, "the section is still in the canvas"
    return html.unescape(found.group(1))


# -- AC1, AC2: read ------------------------------------------------------------ #
def test_ac1_read_returns_both_tables_by_position_and_the_paragraphs(stack: CanvasStack) -> None:
    _, token = stack.open_with()
    page = _ok(stack.canvas(token, **_read()))
    assert (page["operation"], page["canvas_id"], page["title"]) == ("read", CANVAS, "Untitled")
    assert page["tables"] == _expected_tables()
    assert page["paragraphs"] == _expected_paragraphs()
    # AC2: the sentinel status cell reads back exactly.
    header = [cell["text"] for cell in page["tables"][0]["header"]]
    sentinel = page["tables"][0]["rows"][0][header.index("Status")]
    assert (sentinel["text"], sentinel["section_id"]) == ("sentinel-baseline", SENTINEL)

    (info,) = stack.fake.calls("files.info")
    assert _params(info)["file"] == CANVAS
    assert info.headers["authorization"] == f"Bearer {CANVAS_BOT_TOKEN}"
    (member,) = stack.fake.calls("conversations.info")
    assert _params(member)["channel"] == CHAN_A
    (download,) = stack.fake.downloads()
    assert str(download.url) == stack.fake.canvases[CANVAS].info["url_private"]
    assert download.headers["authorization"] == f"Bearer {CANVAS_BOT_TOKEN}"
    assert stack.fake.calls("canvases.edit") == []
    # One page charged: seven more fit.
    _canvas_budget_left(stack, token, 7)


def test_read_by_kind_reaches_every_binding_of_that_kind(stack: CanvasStack) -> None:
    deployed = stack.deploy_with(extra=(_named(CHAN_B), _named(CHAN_PRIVATE)))
    no_default = str(stack.mint(deployed, no_default=True)["token"])
    # A canvas shared into a second binding, and one shared through `groups`.
    page = _ok(stack.canvas(no_default, **_read(CANVAS_B, kind="slack")))
    assert page["tables"] == _expected_tables("EXB")
    page = _ok(stack.canvas(no_default, **_read(CANVAS_PRIVATE, kind="slack")))
    assert page["tables"] == _expected_tables("EXD")
    assert _params(stack.fake.calls("conversations.info")[-1])["channel"] == CHAN_PRIVATE


# -- AC3: list ------------------------------------------------------------------ #
def test_ac3_list_returns_the_channel_canvas_for_the_default_and_a_named_channel(
    stack: CanvasStack,
) -> None:
    _, token = stack.open_with(extra=(_named(CHAN_B),))
    page = _ok(stack.canvas(token, **_list()))
    assert page["operation"] == "list"
    assert page["has_more"] is False
    (summary,) = page["canvases"]
    assert set(summary) == {"id", "title", "created"}
    assert (summary["id"], summary["title"]) == (CANVAS, "Untitled")
    created = datetime.fromisoformat(summary["created"].replace("Z", "+00:00"))
    assert created == datetime.fromtimestamp(CREATED, UTC)
    assert created.utcoffset() is not None, "created is RFC 3339 with an offset"
    (call,) = stack.fake.calls("files.list")
    assert (_params(call)["channel"], _params(call)["types"]) == (CHAN_A, "canvas")
    assert call.headers["authorization"] == f"Bearer {CANVAS_BOT_TOKEN}"

    named = _ok(stack.canvas(token, **_list(channel=_named(CHAN_B))))
    assert [c["id"] for c in named["canvases"]] == [CANVAS_B]
    call = stack.fake.calls("files.list")[-1]
    assert (_params(call)["channel"], _params(call)["types"]) == (CHAN_B, "canvas")
    assert stack.fake.downloads() == [], "a list reads no canvas content"


def test_list_keeps_only_canvases_and_reports_more_pages(stack: CanvasStack) -> None:
    def with_strays(body: dict[str, Any]) -> dict[str, Any]:
        pdf = _file("F0EXAMPLEPDF", filetype="pdf", mimetype="application/pdf")
        odd = _file("F0EXAMPLEODD", mimetype="application/octet-stream")
        body["files"] = [*body["files"], pdf, odd]
        body["paging"] = {"count": 100, "total": 103, "page": 1, "pages": 2}
        return body

    stack.fake.tamper["files.list"] = with_strays
    _, token = stack.open_with()
    page = _ok(stack.canvas(token, **_list()))
    assert [c["id"] for c in page["canvases"]] == [CANVAS]
    assert page["has_more"] is True


# -- AC4: bound -------------------------------------------------------------------- #
def test_ac4_a_canvas_shared_only_into_an_unbound_channel_is_refused(stack: CanvasStack) -> None:
    _, token = stack.open_with(channel=_named(CHAN_UNBOUND))
    response = stack.canvas(token, **_read(CANVAS))
    _refused(response, 403, "channel_read.canvas_not_bound")
    assert "sentinel-baseline" not in response.text
    # The metadata lookup is the only provider call: no membership, no download, no edit.
    assert [r.url.path.rsplit("/", 1)[-1] for r in stack.fake.requests] == ["files.info"]
    assert stack.fake.downloads() == []
    # The page was released: all eight remain for a canvas this agent may read.
    _canvas_budget_left(stack, token, 8, CANVAS_UNBOUND)
    # Liveness: the same canvas for an agent bound to its channel.
    _, bound = stack.open_with()
    _ok(stack.canvas(bound, **_read(CANVAS)))


def test_edit_of_an_unbound_canvas_is_refused_before_any_content_call(
    stack: CanvasStack,
) -> None:
    deployed, token = stack.open_with(extra=(_named(CHAN_UNBOUND),))
    _ok(stack.canvas(token, **_read(CANVAS_UNBOUND)))
    stack.admin(
        "DELETE",
        f"/agents/{deployed.agent_id}/channels",
        status=204,
        params=_named(CHAN_UNBOUND),
    )
    stack.fake.requests.clear()
    edit = _edit(canvas_id=CANVAS_UNBOUND, section_id=_sid("EXC", 8))
    _refused(stack.canvas(token, **edit), 403, "channel_read.canvas_not_bound")
    assert [r.url.path.rsplit("/", 1)[-1] for r in stack.fake.requests] == ["files.info"]
    assert _audit(deployed.agent_id) == []


def test_another_agents_canvas_is_not_bound(stack: CanvasStack) -> None:
    stack.deploy_with(channel=_named(CHAN_B))
    _, token = stack.open_with()
    _refused(stack.canvas(token, **_read(CANVAS_B)), 403, "channel_read.canvas_not_bound")
    assert stack.fake.downloads() == []


def test_a_canvas_shared_into_two_bindings_uses_the_one_the_bot_is_in(
    stack: CanvasStack,
) -> None:
    deployed, token = stack.open_with(channel=_named(CHAN_NOT_MEMBER), extra=(_named(CHAN_LATE),))
    page = _ok(stack.canvas(token, **_read(CANVAS_TWO_SHARES)))
    assert page["tables"] == _expected_tables("EXE")
    asked = {_params(r)["channel"] for r in stack.fake.calls("conversations.info")}
    assert CHAN_LATE in asked
    edit = _edit("Blocked on review (Dana)", canvas_id=CANVAS_TWO_SHARES, section_id=_sid("EXE", 8))
    _ok(stack.canvas(token, **edit))
    (row,) = _audit(deployed.agent_id)
    assert (row["channel_address"], row["status"]) == (CHAN_LATE, "applied")


# -- AC5: edit ------------------------------------------------------------------------ #
def test_ac5_edit_changes_exactly_the_sentinel_cell_and_is_audited(stack: CanvasStack) -> None:
    deployed = stack.deploy_with()
    event = _event()
    token = str(stack.mint(deployed, event_id=event)["token"])
    _ok(stack.canvas(token, **_read()))
    at_slack: list[dict[str, Any]] = []
    stack.fake.on_edit = lambda _request: at_slack.extend(_audit_from_slack(deployed.agent_id))

    result = _ok(stack.canvas(token, **_edit("sentinel-3f9a1c2e")))
    assert (result["operation"], result["canvas_id"], result["section_id"], result["edited"]) == (
        "edit",
        CANVAS,
        SENTINEL,
        True,
    )
    (call,) = stack.fake.calls("canvases.edit")
    assert call.method == "POST"
    assert call.headers["content-type"].startswith("application/json")
    assert call.headers["authorization"] == f"Bearer {CANVAS_BOT_TOKEN}"
    assert json.loads(call.content) == _edit_body(CANVAS, SENTINEL, "sentinel-3f9a1c2e")
    # Byte level: the re-download is the original except that one cell.
    changed = FIXTURE_HTML.replace(">sentinel-baseline<", ">sentinel-3f9a1c2e<")
    assert changed != FIXTURE_HTML
    assert stack.fake.canvases[CANVAS].html == changed
    # Through the route: same tables, same section ids, one cell's text changed.
    expected = _expected_tables()
    expected[0]["rows"][0][2]["text"] = "sentinel-3f9a1c2e"
    again = _ok(stack.canvas(token, **_read()))
    assert again["tables"] == expected
    assert again["paragraphs"] == _expected_paragraphs()

    # The row was committed as attempted before Slack saw the edit...
    seen = [(r["status"], r["before_text"], r["after_text"], r["completed_at"]) for r in at_slack]
    assert seen == [("attempted", "sentinel-baseline", "sentinel-3f9a1c2e", None)]
    # ...and settled applied after it.
    (row,) = _audit(deployed.agent_id)
    assert row["completed_at"] is not None
    assert {k: v for k, v in row.items() if k not in {"created_at", "completed_at"}} == {
        "agent_id": uuid.UUID(deployed.agent_id),
        "deployment_id": uuid.UUID(deployed.deployment_id),
        "turn": event,
        "kind": "slack",
        "channel_address": CHAN_A,
        "canvas_id": CANVAS,
        "section_id": SENTINEL,
        "before_text": "sentinel-baseline",
        "after_text": "sentinel-3f9a1c2e",
        "status": "applied",
        "error_code": None,
    }
    # Read, edit and read: three pages.
    _canvas_budget_left(stack, token, 5)


def test_the_before_text_comes_from_the_fresh_download(stack: CanvasStack) -> None:
    deployed, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    # A person edits the cell between the agent's read and its edit.
    fake = stack.fake.canvases[CANVAS]
    fake.html = fake.html.replace(">sentinel-baseline<", ">changed by a person<")
    _ok(stack.canvas(token, **_edit("Status: blocked (waiting on API)")))
    (row,) = _audit(deployed.agent_id)
    assert (row["before_text"], row["after_text"]) == (
        "changed by a person",
        "Status: blocked (waiting on API)",
    )


# -- Authority withdrawn while the download is in flight ---------------------------- #
def _in_thread(work: Callable[[], Any]) -> None:
    """Run blocking setup off the API's event loop (the fake answers on that loop)."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(work).result()


def _sql(statement: str, params: dict[str, Any]) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(run())


def _supersede_the_turn(deployed: Deployed, context: dict[str, Any], owner: str) -> None:
    assert _revoke_owner(deployed.agent_id, context["turn_key"], owner) is True


def _stop_the_deployment(deployed: Deployed, context: dict[str, Any], owner: str) -> None:
    _sql(
        "UPDATE curie.deployments SET status = 'stopped' WHERE id = :id",
        {"id": uuid.UUID(deployed.deployment_id)},
    )


def _delete_the_binding(deployed: Deployed, context: dict[str, Any], owner: str) -> None:
    _sql(
        "DELETE FROM curie.agent_channels WHERE agent_id = :agent",
        {"agent": uuid.UUID(deployed.agent_id)},
    )


@pytest.mark.parametrize(
    ("withdraw", "status", "code"),
    [
        (_supersede_the_turn, 409, "channel_read.turn_inactive"),
        (_stop_the_deployment, 409, "channel_read.grant_revoked"),
        (_delete_the_binding, 403, "channel_read.not_bound"),
    ],
    ids=["turn-superseded", "deployment-stopped", "binding-deleted"],
)
def test_an_edit_is_refused_when_authority_is_withdrawn_during_the_download(
    stack: CanvasStack,
    withdraw: Callable[[Deployed, dict[str, Any], str], None],
    status: int,
    code: str,
) -> None:
    deployed = stack.deploy_with()
    owner = _owner()
    context = stack.mint(deployed, owner=owner)
    token = str(context["token"])
    _ok(stack.canvas(token, **_read()))
    stack.fake.on_download = lambda _request: _in_thread(lambda: withdraw(deployed, context, owner))

    _refused(stack.canvas(token, **_edit()), status, code)
    assert stack.fake.calls("canvases.edit") == [], "the edit reached Slack after the withdrawal"
    assert stack.fake.canvases[CANVAS].html == FIXTURE_HTML, "the canvas cell changed"
    # The recheck happens before the audit insert, so no row exists at all.
    assert [r for r in _audit(deployed.agent_id) if r["status"] == "applied"] == []


@pytest.mark.parametrize(
    ("withdraw", "status", "code"),
    [
        (_supersede_the_turn, 409, "channel_read.turn_inactive"),
        (_stop_the_deployment, 409, "channel_read.grant_revoked"),
        (_delete_the_binding, 403, "channel_read.not_bound"),
    ],
    ids=["turn-superseded", "deployment-stopped", "binding-deleted"],
)
def test_an_edit_is_refused_when_authority_is_withdrawn_at_the_edit_cooldown_check(
    stack: CanvasStack,
    monkeypatch: pytest.MonkeyPatch,
    withdraw: Callable[[Deployed, dict[str, Any], str], None],
    status: int,
    code: str,
) -> None:
    deployed = stack.deploy_with()
    owner = _owner()
    context = stack.mint(deployed, owner=owner)
    token = str(context["token"])
    _ok(stack.canvas(token, **_read()))

    # Timing injection only: the withdrawal fires as the guard checks the
    # canvases.edit cooldown (after the audit row is committed, before the
    # Slack call). The guard and Valkey stay real; the real method still runs.
    real = ProviderGuard.cooldown_remaining

    async def withdraw_then_check(
        self: ProviderGuard, identity_key: str, method: str
    ) -> int | None:
        if method == "canvases.edit":
            _in_thread(lambda: withdraw(deployed, context, owner))
        return await real(self, identity_key, method)

    monkeypatch.setattr(ProviderGuard, "cooldown_remaining", withdraw_then_check)

    _refused(stack.canvas(token, **_edit()), status, code)
    assert stack.fake.calls("canvases.edit") == [], "the edit reached Slack after the withdrawal"
    assert stack.fake.canvases[CANVAS].html == FIXTURE_HTML, "the canvas cell changed"
    (row,) = _audit(deployed.agent_id)
    assert (row["status"], row["error_code"]) == ("failed", code)


def test_an_edit_still_applies_when_the_download_is_slow_and_authority_holds(
    stack: CanvasStack,
) -> None:
    deployed = stack.deploy_with()
    token = str(stack.mint(deployed)["token"])
    _ok(stack.canvas(token, **_read()))
    pending: list[httpx.Request] = []

    def slow(request: httpx.Request) -> None:
        pending.append(request)
        _in_thread(lambda: time.sleep(0.2))

    stack.fake.on_download = slow
    result = _ok(stack.canvas(token, **_edit()))
    assert (result["operation"], result["edited"]) == ("edit", True)
    assert len(pending) == 1, "the edit's own download was the delayed one"
    assert len(stack.fake.calls("canvases.edit")) == 1
    assert _cell_text_in(stack.fake.canvases[CANVAS].html, SENTINEL) == "sentinel-3f9a1c2e"
    (row,) = _audit(deployed.agent_id)
    assert row["status"] == "applied"


# -- Edit races and provider outcomes --------------------------------------------- #
def _split_cell(canvas_html: str) -> str:
    extra = _sid("EXA", 0x99)
    return canvas_html.replace(
        ">sentinel-baseline</p>", f'>sentinel-baseline</p><p id="{extra}" class="line">x</p>'
    )


def _drop_cell(canvas_html: str) -> str:
    return re.sub(rf'<p id="{re.escape(SENTINEL)}"[^>]*>.*?</p>', "", canvas_html)


@pytest.mark.parametrize("change", [_split_cell, _drop_cell], ids=["two-paragraphs", "removed"])
def test_a_section_that_stopped_being_one_cell_is_not_editable(
    stack: CanvasStack, change: Callable[[str], str]
) -> None:
    deployed, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    fake = stack.fake.canvases[CANVAS]
    fake.html = change(fake.html)
    _refused(stack.canvas(token, **_edit()), 409, "channel_read.section_not_editable")
    assert stack.fake.calls("canvases.edit") == []
    assert _audit(deployed.agent_id) == [], "no attempt was made, so nothing is audited"
    _canvas_budget_left(stack, token, 7)


@pytest.mark.parametrize(
    ("answer", "status", "code"),
    [
        (_slack_error("restricted_action"), 403, "channel_read.canvas_edit_refused"),
        (_slack_error("not_allowed"), 403, "channel_read.canvas_edit_refused"),
        (_slack_error("access_denied"), 403, "channel_read.canvas_edit_refused"),
        (_slack_error("canvas_disabled"), 403, "channel_read.canvas_edit_refused"),
        (
            httpx.Response(
                200,
                json={
                    "ok": False,
                    "error": "canvas_editing_failed",
                    "detail": f"Section {SENTINEL} was not found",
                },
            ),
            409,
            "channel_read.section_not_found",
        ),
        (
            httpx.Response(
                200,
                json={
                    "ok": False,
                    "error": "missing_scope",
                    "needed": "canvases:write",
                    "provided": "channels:history,channels:read,files:read",
                },
            ),
            503,
            "channel_read.provider_scope_missing",
        ),
        (_slack_error("ratelimited", 429, 1), 429, "channel_read.provider_rate_limited"),
    ],
    ids=[
        "restricted_action",
        "not_allowed",
        "access_denied",
        "canvas_disabled",
        "canvas_editing_failed",
        "missing_scope",
        "http-429",
    ],
)
def test_a_definite_provider_refusal_settles_the_audit_row_failed(
    stack: CanvasStack, answer: httpx.Response, status: int, code: str
) -> None:
    deployed, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    stack.fake.fail_next("canvases.edit", answer)
    try:
        _refused(stack.canvas(token, **_edit()), status, code)
        (row,) = _audit(deployed.agent_id)
        assert (row["status"], row["error_code"]) == ("failed", code)
        assert row["completed_at"] is not None
        assert (row["before_text"], row["after_text"]) == ("sentinel-baseline", "sentinel-3f9a1c2e")
        assert stack.fake.canvases[CANVAS].html == FIXTURE_HTML, "Slack applied nothing"
        _canvas_budget_left(stack, token, 7)
    finally:
        if status == 429:
            time.sleep(1.5)  # let the edit cooldown lapse


def _connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection reset after the request was sent", request=request)


def _read_timeout(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("no answer before the timeout", request=request)


def _http_503(request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, text="upstream unavailable")


def _not_json(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, headers={"content-type": "text/html"}, text="<html>gateway</html>")


@pytest.mark.parametrize(
    "lost",
    [_connect_error, _read_timeout, _http_503, _not_json],
    ids=["connect-error", "read-timeout", "http-503", "not-json"],
)
def test_an_edit_slack_may_have_applied_stays_attempted(
    stack: CanvasStack, lost: Callable[[httpx.Request], httpx.Response]
) -> None:
    deployed, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    stack.fake.after_apply.append(lost)
    response = stack.canvas(token, **_edit())
    _refused(response, 502, "channel_read.edit_outcome_unknown")
    (row,) = _audit(deployed.agent_id)
    assert (row["status"], row["error_code"], row["completed_at"]) == ("attempted", None, None)
    assert (row["before_text"], row["after_text"]) == ("sentinel-baseline", "sentinel-3f9a1c2e")
    # The row is honest: Slack did apply it, so it must not say failed.
    page = _ok(stack.canvas(token, **_read()))
    assert page["tables"][0]["rows"][0][2]["text"] == "sentinel-3f9a1c2e"
    # Read, unknown edit (released) and read: two pages.
    _canvas_budget_left(stack, token, 6)


# -- Sections read this turn -------------------------------------------------------- #
def test_an_edit_needs_a_read_of_that_canvas_in_the_same_logical_turn(
    stack: CanvasStack,
) -> None:
    deployed = stack.deploy_with(extra=(_named(CHAN_B),))
    event = _event()
    token = str(stack.mint(deployed, event_id=event)["token"])
    with stack.no_provider_calls():
        _refused(stack.canvas(token, **_edit()), 409, "channel_read.section_not_read")
    _ok(stack.canvas(token, **_read()))
    with stack.no_provider_calls():
        # A section of another canvas, or this canvas's section named under the other.
        other = _edit(canvas_id=CANVAS_B, section_id=_sid("EXB", 8))
        _refused(stack.canvas(token, **other), 409, "channel_read.section_not_read")
        crossed = _edit(canvas_id=CANVAS_B, section_id=SENTINEL)
        _refused(stack.canvas(token, **crossed), 409, "channel_read.section_not_read")
        # A paragraph's id is informational; only table cells are recorded.
        heading = _edit(section_id=_sid("EXA", 1))
        _refused(stack.canvas(token, **heading), 409, "channel_read.section_not_read")
        # A new logical turn starts empty.
        fresh = str(stack.mint(deployed)["token"])
        _refused(stack.canvas(fresh, **_edit()), 409, "channel_read.section_not_read")
    # The same logical turn after an approval resume keeps what was read.
    resumed = str(
        stack.mint(deployed, event_id=_insert_approval(deployed.agent_id, event))["token"]
    )
    _ok(stack.canvas(resumed, **_edit()))
    # And after a steer.
    steered = str(stack.mint(deployed, event_id=event, mode="steer")["token"])
    _ok(stack.canvas(steered, **_edit("Blocked on review (Dana)")))
    # Liveness for the other canvas once it is read.
    _ok(stack.canvas(steered, **_read(CANVAS_B)))
    _ok(stack.canvas(steered, **other))
    assert [r["turn"] for r in _audit(deployed.agent_id)] == [event, event, event]


# -- Edit text rule ------------------------------------------------------------------- #
# The marker lets the test prove a refusal never echoes what was sent.
MARK = "Zq7"
REJECTED_TEXTS = [
    "",
    "x" * 1001,
    f" {MARK} leading space",
    f"{MARK} trailing space ",
    f"{MARK}\nsecond line",
    f"{MARK}\rcarriage return",
    f"{MARK}\ttab",
    f"{MARK}\x00nul",
    f"{MARK}\x7fdelete",
    f"{MARK} | pipe",
    f"{MARK} *bold*",
    f"{MARK} _under_",
    f"{MARK} `code`",
    f"{MARK} ~strike~",
    f"{MARK} [link",
    f"{MARK} link]",
    f"{MARK} <tag",
    f"{MARK} quote>",
    f"{MARK} back\\slash",
    f"# {MARK}",
    f"#{MARK}",
    f"- {MARK}",
    f"-{MARK}",
    f"+ {MARK}",
    f"> {MARK}",
    f"1. {MARK}",
    f"12) {MARK}",
    f"Ready :smile: {MARK}",
    f":+1: {MARK}",
    f"A &amp; B {MARK}",
    f"A &#38; B {MARK}",
    f"&#x26; {MARK}",
    f"{MARK} &nbsp;",
]
ACCEPTED_TEXTS = [
    "sentinel-3f9a1c2e",
    "Blocked on review (Dana)",
    "50% done, ETA Friday!",
    "Q4: ship 0.13 & close #2881",
    "Status: blocked (waiting on API)",
    "10:30 standup",
]


def test_cell_text_that_markdown_would_reinterpret_is_refused_without_echo(
    stack: CanvasStack,
) -> None:
    _, token = stack.open_with()
    with stack.no_provider_calls():
        for value in REJECTED_TEXTS:
            response = stack.canvas(token, **_edit(value))
            detail = _refused(response, 422, "channel_read.cell_text_invalid")
            assert detail["message"], "the message names the rule"
            assert MARK not in response.text, "the refusal echoes the submitted text"
            assert "x" * 50 not in response.text


def test_plain_cell_text_is_edited_verbatim(stack: CanvasStack) -> None:
    _, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    for value in ACCEPTED_TEXTS:
        _ok(stack.canvas(token, **_edit(value)))
        sent = json.loads(stack.fake.calls("canvases.edit")[-1].content)
        assert sent == _edit_body(CANVAS, SENTINEL, value)
        assert _cell_text_in(stack.fake.canvases[CANVAS].html, SENTINEL) == value
    page = _ok(stack.canvas(token, **_read()))
    assert page["tables"][0]["rows"][0][2]["text"] == ACCEPTED_TEXTS[-1]


# -- Identifiers and body shape -------------------------------------------------------- #
@pytest.mark.parametrize(
    "body",
    [
        _read("F0EX"),
        _read("C0EXAMPLE1"),
        _read("../x"),
        _read("F" + "A" * 29),
        _read("f0example01"),
        _read(f"{CANVAS}/../{CANVAS_B}"),
        {"operation": "read"},
        _edit(canvas_id="F0EX"),
        _edit(section_id="temp:C:short"),
        _edit(section_id="temp:D:EXA0000000000000000000000008"),
        _edit(section_id=f"{SENTINEL}/../x"),
        _edit(section_id=f"temp:C:{'A' * 65}"),
        {"operation": "edit", "canvas_id": CANVAS, "text": "x"},
        _read(text="x"),
        _read(section_id=SENTINEL),
        _read(channel=_named(CHAN_A)),
        _edit(channel=_named(CHAN_A)),
        _list(canvas_id=CANVAS),
        _list(section_id=SENTINEL),
        _list(text="x"),
        _list(kind="slack"),
    ],
)
def test_malformed_or_misplaced_identifiers_never_reach_slack(
    stack: CanvasStack, body: dict[str, Any]
) -> None:
    _, token = stack.open_with()
    with stack.no_provider_calls():
        _refused(stack.canvas(token, **body), 422, "channel_read.invalid_identifier")
    assert stack.fake.requests == []
    _ok(stack.canvas(token, **_read(CANVAS)))


def test_a_misplaced_field_is_named(stack: CanvasStack) -> None:
    _, token = stack.open_with()
    for body, name in (
        (_read(text="x"), "text"),
        (_read(channel=_named(CHAN_A)), "channel"),
        (_list(canvas_id=CANVAS), "canvas_id"),
    ):
        detail = _refused(stack.canvas(token, **body), 422, "channel_read.invalid_identifier")
        assert name in detail["message"]


@pytest.mark.parametrize(
    "body",
    [
        {"operation": "delete", "canvas_id": CANVAS},
        {"canvas_id": CANVAS},
        _read(query="release"),
        _read(canvas_id=["F0EXAMPLE01"]),
        _edit("x" * 4097),
        _read("F" + "A" * 64),
    ],
)
def test_a_malformed_body_is_request_invalid(stack: CanvasStack, body: dict[str, Any]) -> None:
    _, token = stack.open_with()
    response = stack.canvas(token, **body)
    _refused(response, 422, "channel_read.request_invalid")
    assert "release" not in response.text
    assert stack.fake.requests == []


# -- Grants ---------------------------------------------------------------------------- #
_OPS = {
    "canvasList": ("list", _list()),
    "canvasRead": ("read", _read()),
    "canvasEdit": ("edit", _edit()),
}


@pytest.mark.parametrize("granted", ["canvasList", "canvasRead", "canvasEdit"])
def test_each_canvas_grant_enables_only_its_own_operation(stack: CanvasStack, granted: str) -> None:
    deployed, token = stack.open_with({granted: True})
    with stack.no_provider_calls():
        for grant, (op, body) in _OPS.items():
            if grant != granted:
                code = f"channel_read.canvas_{op}_not_granted"
                _refused(stack.canvas(token, **body), 403, code)
        _refused(stack.read(token, **_yesterday()), 403, "channel_read.history_not_granted")
    _, body = _OPS[granted]
    if granted == "canvasEdit":
        # Granted, but an edit still needs a read of the canvas in this turn,
        # and a read needs canvasRead.
        with stack.no_provider_calls():
            _refused(stack.canvas(token, **body), 409, "channel_read.section_not_read")
    else:
        _ok(stack.canvas(token, **body))
    assert _audit(deployed.agent_id) == []


def test_a_channel_read_only_bundle_reads_history_and_no_canvas(stack: CanvasStack) -> None:
    _, token = stack.open_with({"channelRead": True})
    with stack.no_provider_calls():
        for op, body in _OPS.values():
            _refused(stack.canvas(token, **body), 403, f"channel_read.canvas_{op}_not_granted")
    page = _ok(stack.read(token, **_yesterday()))
    assert page["messages"], "history reads are unchanged"
    assert stack.fake.calls("files.info") == stack.fake.calls("files.list") == []


def test_every_grant_lets_every_operation_through(stack: CanvasStack) -> None:
    deployed, token = stack.open_with(ALL_GRANTS)
    _ok(stack.read(token, **_yesterday()))
    _ok(stack.canvas(token, **_list()))
    _ok(stack.canvas(token, **_read()))
    _ok(stack.canvas(token, **_edit()))
    assert [r["status"] for r in _audit(deployed.agent_id)] == ["applied"]


@pytest.mark.parametrize("grants", [{}, {"canvasRead": False, "channelRead": False}])
def test_a_bundle_with_no_grant_gets_no_capability(
    stack: CanvasStack, grants: dict[str, Any]
) -> None:
    deployed = stack.deploy_with(grants)
    _refused(stack.mint_response(deployed), 409, "channel_read.grant_absent")
    # Liveness: one canvas grant on a new version of the same agent mints.
    stack.mint(stack.redeploy_with(deployed.agent_id, {"canvasList": True}))


@pytest.mark.parametrize("value", ["true", 1, None])
@pytest.mark.parametrize("grant", ["canvasList", "canvasRead", "canvasEdit"])
def test_only_a_literal_true_grants(stack: CanvasStack, grant: str, value: Any) -> None:
    name = f"canvas-{uuid.uuid4().hex[:8]}"
    agent = stack.admin("POST", "/agents", json={"name": name, "channel": _named(CHAN_A)})
    stack.agents.append(agent["id"])
    label = {"version_label": "v-odd", "created_by": "operator"}
    version_id = stack.admin("POST", f"/agents/{agent['id']}/versions", json=label)["id"]
    bundle = {"file": ("reader-bot.tar.gz", _grants_archive({grant: value}))}
    response = stack.http.put(
        f"/agents/{agent['id']}/versions/{version_id}/bundle", headers=stack.auth, files=bundle
    )
    assert response.status_code == 422, response.text
    assert grant in response.text


def test_refusals_come_in_the_contract_order(stack: CanvasStack) -> None:
    # 1. The capability: nothing else is looked at.
    response = stack.canvas(None, **_edit("| bad", canvas_id="../x"))
    _refused(response, 401, "channel_read.invalid_capability")
    response = stack.http.post(CANVAS_URL, json=_read(), headers=dict(stack.auth))
    _refused(response, 401, "channel_read.invalid_capability")
    # 2. Generation, then the deployment digest, before the grant.
    read_only = stack.deploy_with({"canvasRead": True}, channel=_named(CHAN_B))
    event = _event()
    stale = str(stack.mint(read_only, event_id=event)["token"])
    stack.mint(read_only, event_id=event, mode="steer")
    _refused(stack.canvas(stale, **_list()), 409, "channel_read.turn_inactive")
    revoked = str(stack.mint(read_only)["token"])
    stack.undeploy(read_only)
    _refused(stack.canvas(revoked, **_list()), 409, "channel_read.grant_revoked")
    # 3. The grant, before any shape check.
    _, read_token = stack.open_with({"canvasRead": True}, channel=_named(CHAN_LATE))
    unbound_list = _list(channel=_named(CHAN_UNBOUND))
    _refused(stack.canvas(read_token, **unbound_list), 403, "channel_read.canvas_list_not_granted")
    bad_edit = _edit("| bad", canvas_id="../x")
    _refused(stack.canvas(read_token, **bad_edit), 403, "channel_read.canvas_edit_not_granted")
    # 4. Shape: kind, binding, identifiers, text, then the sections read.
    deployed, token = stack.open_with(extra=(_named(EMAIL, "email"),))
    no_default = str(stack.mint(deployed, no_default=True)["token"])
    _refused(stack.canvas(no_default, **_read("../x")), 400, "channel_read.kind_required")
    _refused(stack.canvas(no_default, **_list()), 400, "channel_read.channel_required")
    unbound = stack.canvas(token, **_list(channel=_named(CHAN_UNBOUND)))
    _refused(unbound, 403, "channel_read.not_bound")
    kindless = stack.canvas(token, **_list(channel={"address": CHAN_A}))
    _refused(kindless, 400, "channel_read.channel_required")
    _refused(stack.canvas(token, **_read("../x", kind="discord")), 403, "channel_read.not_bound")
    bad_both = _edit("| bad", canvas_id="../x")
    _refused(stack.canvas(token, **bad_both), 422, "channel_read.invalid_identifier")
    _refused(stack.canvas(token, **_edit("| bad")), 422, "channel_read.cell_text_invalid")
    # 4 before 5: the sections read is checked before the kind's capability.
    email_edit = _edit(kind="email")
    _refused(stack.canvas(token, **email_edit), 409, "channel_read.section_not_read")
    # 5. A bound kind with no canvas reader.
    email_read = _read(kind="email")
    _refused(stack.canvas(token, **email_read), 409, "channel_read.capability_unsupported")
    email_list = _list(channel=_named(EMAIL, "email"))
    _refused(stack.canvas(token, **email_list), 409, "channel_read.capability_unsupported")
    assert stack.fake.requests == [], "every refusal above came before any provider call"
    # Liveness: the same turn with a no-default token and a kind.
    _ok(stack.canvas(no_default, **_read(kind="slack")))
    _ok(stack.canvas(no_default, **_list(channel=_named(CHAN_A))))


@pytest.mark.parametrize("stack", [{"SLACK_BOT_TOKEN": ""}], indirect=True)
def test_unconfigured_identity_is_refused_without_a_provider_call(stack: CanvasStack) -> None:
    _, token = stack.open_with(extra=(_named(EMAIL, "email"),))
    _refused(stack.canvas(token, **_read()), 503, "channel_read.provider_unconfigured")
    _refused(stack.canvas(token, **_list()), 503, "channel_read.provider_unconfigured")
    # The capability check comes first.
    response = stack.canvas(token, **_read(kind="email"))
    _refused(response, 409, "channel_read.capability_unsupported")
    assert stack.fake.requests == []


def test_valkey_outage_is_503(stack: CanvasStack) -> None:
    _, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))
    healthy = stack.http.app.state.valkey
    stack.http.app.state.valkey = aioredis.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.5, retry=Retry(NoBackoff(), 0)
    )
    before = len(stack.fake.requests)
    try:
        for body in (_read(), _list(), _edit()):
            _refused(stack.canvas(token, **body), 503, "channel_read.unavailable")
    finally:
        stack.http.app.state.valkey = healthy
    assert len(stack.fake.requests) == before
    _ok(stack.canvas(token, **_edit()))


# -- Provider refusals ------------------------------------------------------------------ #
@pytest.mark.parametrize(
    "answer",
    [
        _slack_error("file_not_found"),
        _slack_error("file_deleted"),
    ],
    ids=["file_not_found", "file_deleted"],
)
def test_a_canvas_slack_cannot_find_is_not_found(
    stack: CanvasStack, answer: httpx.Response
) -> None:
    _, token = stack.open_with()
    stack.fake.fail_next("files.info", answer)
    _refused(stack.canvas(token, **_read()), 404, "channel_read.canvas_not_found")
    assert stack.fake.downloads() == []
    _canvas_budget_left(stack, token, 8)


def test_a_files_info_answer_for_another_file_is_not_found(stack: CanvasStack) -> None:
    def other_file(body: dict[str, Any]) -> dict[str, Any]:
        body["file"]["id"] = CANVAS_B
        return body

    stack.fake.tamper["files.info"] = other_file
    _, token = stack.open_with()
    _refused(stack.canvas(token, **_read()), 404, "channel_read.canvas_not_found")
    assert stack.fake.downloads() == []


@pytest.mark.parametrize(
    ("filetype", "mimetype"),
    [("pdf", "application/pdf"), ("quip", "application/octet-stream"), ("docx", CANVAS_MIME)],
)
def test_a_file_that_is_not_a_canvas_is_refused_before_download(
    stack: CanvasStack, filetype: str, mimetype: str
) -> None:
    stack.fake.canvases[CANVAS].info.update(filetype=filetype, mimetype=mimetype)
    _, token = stack.open_with()
    _refused(stack.canvas(token, **_read()), 422, "channel_read.not_a_canvas")
    assert stack.fake.downloads() == []
    assert stack.fake.calls("conversations.info") == []


@pytest.mark.parametrize("how", ["is_member_false", "channel_not_found", "not_in_channel"])
def test_a_bound_channel_the_bot_is_not_in_is_not_member(stack: CanvasStack, how: str) -> None:
    _, token = stack.open_with(extra=(_named(CHAN_NOT_MEMBER),))
    if how != "is_member_false":
        stack.fake.fail_next("conversations.info", _slack_error(how))
    _refused(stack.canvas(token, **_read(CANVAS_NOT_MEMBER)), 403, "channel_read.not_member")
    assert stack.fake.downloads() == []
    list_body = _list(channel=_named(CHAN_NOT_MEMBER))
    if how != "is_member_false":
        stack.fake.fail_next("conversations.info", _slack_error(how))
    _refused(stack.canvas(token, **list_body), 403, "channel_read.not_member")
    assert stack.fake.calls("files.list") == []
    _canvas_budget_left(stack, token, 8)


def test_a_missing_scope_is_named(stack: CanvasStack) -> None:
    missing = {
        "ok": False,
        "error": "missing_scope",
        "needed": "groups:read",
        "provided": "channels:history,channels:read,files:read",
    }
    _, token = stack.open_with(extra=(_named(CHAN_PRIVATE),))
    for method, body in (
        ("conversations.info", _read(CANVAS_PRIVATE)),
        ("files.info", _read()),
        ("files.list", _list()),
    ):
        stack.fake.fail_next(method, httpx.Response(200, json=missing))
        _refused(stack.canvas(token, **body), 503, "channel_read.provider_scope_missing")
    assert stack.fake.downloads() == []
    _canvas_budget_left(stack, token, 8)


@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, headers={"content-type": "text/html"}, text=LOGIN_PAGE),
        httpx.Response(302, headers={"location": "https://example.slack.com/?redir=%2Ffiles-pri"}),
        httpx.Response(404, text="Not Found"),
        httpx.Response(500, text="<html>error</html>"),
    ],
    ids=["login-page", "redirect", "not-found", "server-error"],
)
def test_an_unreadable_download_is_refused(stack: CanvasStack, answer: httpx.Response) -> None:
    _, token = stack.open_with()
    stack.fake.fail_next(DOWNLOAD, answer)
    response = stack.canvas(token, **_read())
    _refused(response, 502, "channel_read.canvas_unreadable")
    assert "Sign in" not in response.text
    assert len(stack.fake.downloads()) == 1, "a redirect is never followed"
    _canvas_budget_left(stack, token, 8)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "http://files.slack.com/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "https://files.slack.com.evil.example/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "https://user@files.slack.com/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "https://files.slack.com:8443/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "ftp://files.slack.com/files-pri/T0EXAMPLE1-F0EXAMPLE01/canvas",
        "",
    ],
)
def test_the_token_is_only_ever_sent_to_files_slack_com(stack: CanvasStack, url: str) -> None:
    stack.fake.canvases[CANVAS].info["url_private"] = url
    _, token = stack.open_with()
    _refused(stack.canvas(token, **_read()), 502, "channel_read.canvas_unreadable")
    assert stack.fake.downloads() == [], "no request left for the url_private host"
    assert all(r.url.host == SLACK_API_HOST for r in stack.fake.requests)


def test_a_download_over_one_mebibyte_is_too_large(stack: CanvasStack) -> None:
    cap = 1024 * 1024
    head, tail = '<div class="quip-canvas-content"><p>', "</p></div>"
    fits = head + "x" * (cap - len(head) - len(tail)) + tail
    assert len(fits.encode()) == cap
    _, token = stack.open_with()
    stack.fake.canvases[CANVAS].html = fits
    page = _ok(stack.canvas(token, **_read()))
    (paragraph,) = page["paragraphs"]
    assert paragraph["truncated"] is True and paragraph["section_id"] is None
    stack.fake.canvases[CANVAS].html = fits.replace("<p>", "<p>x", 1)
    assert len(stack.fake.canvases[CANVAS].html.encode()) == cap + 1
    _refused(stack.canvas(token, **_read()), 409, "channel_read.canvas_too_large")
    stack.fake.canvases[CANVAS].html = FIXTURE_HTML
    _canvas_budget_left(stack, token, 7)


# -- Budgets and rate limits ------------------------------------------------------------- #
def test_canvas_operations_and_history_share_the_eight_pages(stack: CanvasStack) -> None:
    deployed, token = stack.open_with(ALL_GRANTS)
    successes = [
        lambda: stack.canvas(token, **_list()),
        lambda: stack.canvas(token, **_read()),
        lambda: stack.canvas(token, **_edit()),
        lambda: stack.read(token, **_yesterday()),
        lambda: stack.canvas(token, **_read()),
        lambda: stack.canvas(token, **_list()),
        lambda: stack.read(token, **_yesterday()),
        lambda: stack.canvas(token, **_edit("Blocked on review (Dana)")),
    ]
    for index, call in enumerate(successes):
        _ok(call())
        if index in {1, 5}:
            # Refusals in between give their page back.
            missing = stack.canvas(token, **_read(MISSING_CANVAS))
            _refused(missing, 404, "channel_read.canvas_not_found")
            _refused(stack.canvas(token, **_read(CANVAS_B)), 403, "channel_read.canvas_not_bound")
    with stack.no_provider_calls():
        for body in (_read(), _list(), _edit()):
            _refused(stack.canvas(token, **body), 429, "channel_read.page_budget_exhausted")
        _refused(stack.read(token, **_yesterday()), 429, "channel_read.page_budget_exhausted")
    assert [r["status"] for r in _audit(deployed.agent_id)] == ["applied", "applied"]
    # A fresh logical turn starts over.
    _canvas_budget_left(stack, str(stack.mint(deployed)["token"]), 8)


def test_failed_canvas_attempts_hit_the_per_turn_attempt_cap(stack: CanvasStack) -> None:
    _, token = stack.open_with()
    codes: list[str] = []
    while len(codes) < 64 and "channel_read.attempt_budget_exhausted" not in codes:
        codes.append(stack.canvas(token, **_read(MISSING_CANVAS)).json()["detail"]["code"])
    assert set(codes[:-1]) == {"channel_read.canvas_not_found"}, codes
    assert codes[-1] == "channel_read.attempt_budget_exhausted"
    assert len(codes) == 25, "24 attempts per turn, then the cap"
    with stack.no_provider_calls():
        response = stack.canvas(token, **_read())
        _refused(response, 429, "channel_read.attempt_budget_exhausted")


COOLDOWN_TOKEN = "xoxb-channel-canvas-cooldown"


@pytest.mark.parametrize(
    "stack",
    [
        {
            "SLACK_BOT_TOKEN": COOLDOWN_TOKEN,
            "CURIE_SLACK_IDENTITIES": TWO_IDENTITIES,
            "CURIE_SLACK_BOT_TOKEN__0": SECOND_BOT_TOKEN,
        }
    ],
    indirect=True,
)
def test_rate_limits_cool_down_one_method_for_one_identity(stack: CanvasStack) -> None:
    deployed, token = stack.open_with()
    _ok(stack.canvas(token, **_read()))  # one attempt

    # A 429 on conversations.info during a list...
    stack.fake.fail_next("conversations.info", _slack_error("ratelimited", 429, 7))
    response = stack.canvas(token, **_list())
    detail = _refused(response, 429, "channel_read.provider_rate_limited")
    assert (detail["retry_after"], response.headers["retry-after"]) == (7, "7")
    # ...blocks a later read's conversations.info, but not its files.info.
    stack.fake.requests.clear()
    response = stack.canvas(token, **_read())
    detail = _refused(response, 429, "channel_read.provider_rate_limited")
    assert 0 < detail["retry_after"] <= 7
    assert [r.url.path.rsplit("/", 1)[-1] for r in stack.fake.requests] == ["files.info"]
    assert stack.fake.downloads() == []

    # During an edit, files.info answers 429...
    stack.fake.fail_next("files.info", _slack_error("ratelimited", 429, 7))
    response = stack.canvas(token, **_edit())
    detail = _refused(response, 429, "channel_read.provider_rate_limited")
    assert (detail["retry_after"], response.headers["retry-after"]) == (7, "7")
    assert stack.fake.calls("canvases.edit") == []
    # ...so the next read is refused with no Slack call at all.
    with stack.no_provider_calls():
        response = stack.canvas(token, **_read())
        detail = _refused(response, 429, "channel_read.provider_rate_limited")
        assert response.headers["retry-after"] == str(detail["retry_after"])

    # Liveness: a binding served by another bot identity is not cooled down.
    second, second_token = stack.open_with(
        channel={"kind": "slack", "address": CHAN_B, "adapter": "second"}
    )
    stack.fake.requests.clear()
    page = _ok(stack.canvas(second_token, **_read(CANVAS_B)))
    assert page["tables"] == _expected_tables("EXB")
    used = {r.headers["authorization"] for r in stack.fake.requests}
    assert used == {f"Bearer {SECOND_BOT_TOKEN}"}
    assert second.agent_id != deployed.agent_id

    # Each refused operation charged at most one provider attempt: after the
    # cooldowns lapse, at least 24 - 1 - 4 attempts remain in the first turn.
    time.sleep(7.5)
    misses = 0
    while misses < 30:
        code = stack.canvas(token, **_read(MISSING_CANVAS)).json()["detail"]["code"]
        if code != "channel_read.canvas_not_found":
            assert code == "channel_read.attempt_budget_exhausted", code
            break
        misses += 1
    assert 19 <= misses <= 23, misses


# -- Hygiene ------------------------------------------------------------------------------ #
def test_tokens_and_canvas_text_stay_out_of_logs(
    stack: CanvasStack, caplog: pytest.LogCaptureFixture
) -> None:
    title = "Weekly plan Zq7 confidential title"
    stack.fake.canvases[CANVAS].info["title"] = title
    new_text = "Blocked on review (Dana) Zq7"
    deployed = stack.deploy_with()
    with caplog.at_level(logging.DEBUG):
        token = str(stack.mint(deployed)["token"])
        _ok(stack.canvas(token, **_list()))
        page = _ok(stack.canvas(token, **_read()))
        assert page["title"] == title
        _ok(stack.canvas(token, **_edit(new_text)))
        _refused(
            stack.canvas(token, **_edit("| Zq7 refused")), 422, "channel_read.cell_text_invalid"
        )
        stack.fake.after_apply.append(_connect_error)
        _refused(stack.canvas(token, **_edit("Zq7 lost")), 502, "channel_read.edit_outcome_unknown")
        _refused(stack.canvas(token, **_read(CANVAS_B)), 403, "channel_read.canvas_not_bound")
    secrets = [
        CANVAS_BOT_TOKEN,
        token,
        token.split(".")[2],
        title,
        "Zq7",
        INTRO,
        "Curie canvas fixture",
        "Weekly plan review",
        "Fixture second row",
        "sentinel-baseline",
        "Reset by edit",
        "Fixture drift",
    ]
    for secret in secrets:
        assert secret not in caplog.text, "a token or canvas text reached the logs"

    async def values(client: aioredis.Redis) -> list[bytes]:
        found = []
        async for key in client.scan_iter(
            match=f"{get_settings().worker_key_prefix}:channel-read:*"
        ):
            kind = await client.type(key)
            if kind in (b"string", "string"):
                found.append(await client.get(key) or b"")
            elif kind in (b"set", "set"):
                found.extend(await client.smembers(key))
        return found

    stored = b"".join(v if isinstance(v, bytes) else v.encode() for v in _valkey_run(values))
    for secret in ("sentinel-baseline", "Weekly plan review", title, token):
        assert secret.encode() not in stored, "canvas text or the capability reached Valkey"


# -- Pure: the HTML parse and the text rule ---------------------------------------------- #
def _rows(cells: list[Any]) -> list[tuple[str | None, str, bool]]:
    return [(c.section_id, c.text, c.truncated) for c in cells]


def _root(body: str) -> str:
    return f'<div class="quip-canvas-content">{body}</div>'


def test_parse_the_recorded_canvas() -> None:
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    document = parse_canvas_html(FIXTURE_HTML)
    expected = _expected_tables()
    assert len(document.tables) == 2
    for table, want in zip(document.tables, expected, strict=True):
        assert _rows(table.header) == [(c["section_id"], c["text"], False) for c in want["header"]]
        assert [_rows(row) for row in table.rows] == [
            [(c["section_id"], c["text"], False) for c in row] for row in want["rows"]
        ]
    assert _rows(document.paragraphs) == [
        (_sid("EXA", 1), "Curie canvas fixture", False),
        (_sid("EXA", 2), INTRO, False),
    ]


def test_parse_header_is_the_first_row_by_position() -> None:
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    a, b, c = _sid("EXA", 0xA1), _sid("EXA", 0xB2), _sid("EXA", 0xC3)
    document = parse_canvas_html(
        _root(
            f'<table><tr><td><p id="{a}" class="line">Fixture drift</p></td></tr></table>'
            f'<table><tr><th><p id="{b}" class="line">Only</p></th></tr></table>'
            f'<table><tr><td><p id="{c}" class="line">x</p></td></tr>'
            "<tr><td><p>1</p></td><td><p>2</p></td></tr><tr><td><p>3</p></td></tr></table>"
        )
    )
    first, second, third = document.tables
    assert (_rows(first.header), first.rows) == ([(a, "Fixture drift", False)], [])
    assert _rows(second.header) == [(b, "Only", False)]
    # Ragged rows come back as they are, without padding.
    assert [[cell.text for cell in row] for row in third.rows] == [["1", "2"], ["3"]]


def test_parse_cell_rules() -> None:
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    one, two, empty, inline, ent, long, edge, plain = (_sid("EXA", n) for n in range(0xD0, 0xD8))
    document = parse_canvas_html(
        _root(
            "<table><tr>"
            f'<td><p id="{one}" class="line">one</p><p id="{two}" class="line">two</p></td>'
            f'<td><p id="{empty}" class="line"></p></td>'
            f'<td><p id="{inline}" class="line"><b>Bold</b> and '
            '<a href="https://example.com/">link</a><br/>next <i>it</i> '
            "<span>sp</span> <code>cd</code></p></td>"
            f'<td><p id="{ent}" class="line">A &amp; B &lt;C&gt;</p></td>'
            f'<td><p id="{long}" class="line">{"y" * 4001}</p></td>'
            f'<td><p id="{edge}" class="line">{"z" * 4000}</p></td>'
            f'<td><p id="{plain}" class="line">plain id</p></td>'
            '<td><p id="temp:C:x" class="line">short id</p></td>'
            "<td><p>no id</p></td>"
            "</tr></table>"
        )
    )
    (table,) = document.tables
    cells = _rows(table.header)
    assert cells[0] == (None, "one\ntwo", False), "two paragraphs are not one editable cell"
    assert cells[1] == (empty, "", False), "an empty single paragraph cell is editable"
    assert cells[2] == (inline, "Bold and link next it sp cd", False)
    assert cells[3] == (ent, "A & B <C>", False)
    assert cells[4] == (None, "y" * 4000 + " [truncated]", True)
    assert cells[5] == (edge, "z" * 4000, False)
    assert cells[6] == (plain, "plain id", False)
    assert cells[7] == (None, "short id", False)
    assert cells[8] == (None, "no id", False)


def test_parse_nested_tables_flatten_into_one_uneditable_cell() -> None:
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    outer, inner = _sid("EXA", 0xE1), _sid("EXA", 0xE2)
    document = parse_canvas_html(
        _root(
            "<table><tr><td>"
            f'<p id="{outer}" class="line">outer</p>'
            f'<table><tr><td><p id="{inner}" class="line">inner</p></td></tr></table>'
            "</td></tr></table>"
        )
    )
    (table,) = document.tables
    ((section_id, value, _),) = _rows(table.header)
    assert section_id is None
    assert "outer" in value and "inner" in value


def test_parse_text_outside_tables_becomes_paragraphs_in_order() -> None:
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    h2, li = _sid("EXA", 0xF1), _sid("EXA", 0xF2)
    document = parse_canvas_html(
        _root(
            f'<h2 id="{h2}">Plan</h2><ul><li id="{li}">first</li><li>second</li></ul>'
            '<blockquote>quoted</blockquote><pre>code</pre><p id="not-a-section">p</p>'
            "<h6>small</h6>"
        )
    )
    assert document.tables == []
    assert _rows(document.paragraphs) == [
        (h2, "Plan", False),
        (li, "first", False),
        (None, "second", False),
        (None, "quoted", False),
        (None, "code", False),
        (None, "p", False),
        (None, "small", False),
    ]


@pytest.mark.parametrize(
    "page",
    [LOGIN_PAGE, "", "<div>no root</div>", "<table><tr><td><p>x</p></td></tr></table>"],
    ids=["login-page", "empty", "no-root", "bare-table"],
)
def test_parse_without_the_canvas_root_is_unreadable(page: str) -> None:
    from curie_api.channel_read.errors import ChannelReadRefused
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    with pytest.raises(ChannelReadRefused) as refused:
        parse_canvas_html(page)
    assert (refused.value.status, refused.value.code) == (502, "channel_read.canvas_unreadable")


@pytest.mark.parametrize(
    "value", REJECTED_TEXTS, ids=[f"rejected-{i}" for i in range(len(REJECTED_TEXTS))]
)
def test_cell_text_rule_rejects(value: str) -> None:
    from curie_api.channel_read.slack_canvas import cell_text_problem

    problem = cell_text_problem(value)
    assert isinstance(problem, str) and problem
    assert MARK not in problem


@pytest.mark.parametrize(
    "value",
    [*ACCEPTED_TEXTS, "x" * 1000, "a", "C# and F# builds", "3.5 GB free", "sign-off pending"],
)
def test_cell_text_rule_accepts(value: str) -> None:
    from curie_api.channel_read.slack_canvas import cell_text_problem

    assert cell_text_problem(value) is None


def test_identifier_rules() -> None:
    from curie_api.channel_read.slack_canvas import valid_canvas_id, valid_section_id

    for good in (CANVAS, "F" + "A" * 8, "F" + "9" * 24):
        assert valid_canvas_id(good), good
    for bad in ("F0EX", "F" + "A" * 7, "F" + "A" * 25, "C0EXAMPLE1", "f0example01", "../x", ""):
        assert not valid_canvas_id(bad), bad
    for good in (SENTINEL, "temp:C:" + "a" * 8, "temp:C:" + "Z9" * 32):
        assert valid_section_id(good), good
    for bad in ("temp:C:short", "temp:D:" + "a" * 10, "temp:C:" + "a" * 65, "temp:C:abc-12345", ""):
        assert not valid_section_id(bad), bad
