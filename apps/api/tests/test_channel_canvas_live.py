"""Canvas list, read and cell edit against a real Slack workspace (ADR 0200, #3819).

Opt in with CURIE_LIVE_CHANNEL_CANVAS=1. CI never sets it. Once opted in, the
setup is required, never skipped: SLACK_BOT_TOKEN (an app reinstalled with
`canvases:read` and `canvases:write`) and SLACK_TEST_CHANNEL (a channel the bot
is in). Postgres, Valkey and the object store are the compose services, as in
the rest of this suite. Nothing is faked: the API's own HTTP client talks to
slack.com.

The fixture is the test channel's own channel canvas, found through the
channel's tabs and created once if absent: a heading, a paragraph, a table
"Item | Owner | Status" and a table "Risk | Mitigation". It is persistent. The
test never deletes it (on a free plan a deleted channel canvas leaves a dead tab
that blocks a new one); it resets the sentinel cell by edit at the start and in
`finally`. It posts nothing and touches nothing outside SLACK_TEST_CHANNEL.

Slack methods used directly by the test for setup and reset:
https://docs.slack.dev/reference/methods/conversations.info
https://docs.slack.dev/reference/methods/conversations.canvases.create
https://docs.slack.dev/reference/methods/files.info
https://docs.slack.dev/reference/methods/canvases.edit

Failure messages name what was wrong, never a channel, file or section id, a
URL or a token.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_channel_read import (
    CAPABILITY_HEADER,
    MINT_URL,
    WORKER_HEADERS,
    WORKER_TOKEN,
    _grants_archive,
)
from apps.api.tests.test_channel_read_live import UNBOUND, _code, _page, _slack

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_LIVE_CHANNEL_CANVAS") != "1",
    reason="live Slack canvases run only with CURIE_LIVE_CHANNEL_CANVAS=1",
)

REQUIRED = ("SLACK_BOT_TOKEN", "SLACK_TEST_CHANNEL")
CANVAS_URL = "/channel-canvas"
BASELINE = "sentinel-baseline"
CANVAS_GRANTS: Mapping[str, Any] = {"canvasList": True, "canvasRead": True, "canvasEdit": True}
FIXTURE_HEADERS = (["Item", "Owner", "Status"], ["Risk", "Mitigation"])
FIXTURE_ROWS = (
    [
        ["Weekly plan review", "Platform", BASELINE],
        ["Fixture second row", "Platform", "Not started"],
    ],
    [["Fixture drift", "Reset by edit"]],
)
FIXTURE_MARKDOWN = (
    "# Curie canvas fixture\n\n"
    "This canvas is a fixture for the Curie canvas live test. Edits reset it.\n\n"
    "| Item | Owner | Status |\n"
    "| --- | --- | --- |\n"
    f"| Weekly plan review | Platform | {BASELINE} |\n"
    "| Fixture second row | Platform | Not started |\n\n"
    "| Risk | Mitigation |\n"
    "| --- | --- |\n"
    "| Fixture drift | Reset by edit |\n"
)
LIST_ATTEMPTS = 5
LIST_RETRY_S = 3.0


@dataclass(frozen=True)
class LiveCanvas:
    token: str
    channel: str
    canvas_id: str


def _slack_json(method: str, token: str, body: dict[str, Any]) -> dict[str, Any]:
    response = httpx.post(
        f"https://slack.com/api/{method}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        content=json.dumps(body),
        timeout=20.0,
    )
    if response.status_code != 200:
        pytest.fail(f"Slack {method} answered HTTP {response.status_code}")
    answer = response.json()
    if not isinstance(answer, dict):
        pytest.fail(f"Slack {method} answered a non object body")
    return answer


def _channel_canvas_id(token: str, channel: str) -> str | None:
    """The channel canvas, from the channel's tabs."""
    info = _slack("conversations.info", token, channel=channel)
    if not info.get("ok"):
        pytest.fail(f"conversations.info failed: {info.get('error', 'unknown')}")
    found = info.get("channel") or {}
    if found.get("is_member") is not True:
        pytest.fail("the bot is not a member of SLACK_TEST_CHANNEL; invite it first")
    tabs = (found.get("properties") or {}).get("tabs") or []
    for tab in tabs:
        if isinstance(tab, dict) and tab.get("type") == "canvas":
            file_id = (tab.get("data") or {}).get("file_id")
            if isinstance(file_id, str) and file_id:
                return file_id
    return None


def _download(token: str, canvas_id: str) -> Any:
    """The canvas as the API parses it, fetched directly with the bot token."""
    from curie_api.channel_read.slack_canvas import parse_canvas_html

    info = _slack("files.info", token, file=canvas_id)
    if not info.get("ok"):
        pytest.fail(f"files.info on the fixture canvas failed: {info.get('error', 'unknown')}")
    url = str((info.get("file") or {}).get("url_private") or "")
    if not url.startswith("https://files.slack.com/"):
        pytest.fail("the fixture canvas has no url_private on files.slack.com")
    response = httpx.get(
        url, headers={"Authorization": f"Bearer {token}"}, follow_redirects=False, timeout=20.0
    )
    if response.status_code != 200:
        pytest.fail(f"downloading the fixture canvas answered HTTP {response.status_code}")
    return parse_canvas_html(response.text)


def _sentinel_cell(document: Any) -> Any:
    if len(document.tables) != 2:
        pytest.fail("create the fixture canvas in the test channel: it lacks the two tables")
    first = document.tables[0]
    header = [cell.text for cell in first.header]
    if header != FIXTURE_HEADERS[0] or not first.rows:
        pytest.fail("create the fixture canvas in the test channel: its first table changed")
    return first.rows[0][header.index("Status")]


def _reset_sentinel(live: LiveCanvas) -> None:
    """Put the sentinel cell back with a direct canvases.edit (never a delete)."""
    cell = _sentinel_cell(_download(live.token, live.canvas_id))
    if cell.section_id is None:
        pytest.fail("the fixture sentinel cell is not a single paragraph cell; reset it by hand")
    change = {
        "operation": "replace",
        "section_id": cell.section_id,
        "document_content": {"type": "markdown", "markdown": BASELINE},
    }
    edited = _slack_json(
        "canvases.edit", live.token, {"canvas_id": live.canvas_id, "changes": [change]}
    )
    if not edited.get("ok"):
        pytest.fail(f"resetting the fixture sentinel failed: {edited.get('error', 'unknown')}")


@pytest.fixture(scope="module")
def live_canvas() -> LiveCanvas:
    missing = [name for name in REQUIRED if not os.environ.get(name, "").strip()]
    if missing:
        pytest.fail(
            "CURIE_LIVE_CHANNEL_CANVAS=1 needs " + ", ".join(missing) + "; this is a setup failure"
        )
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_TEST_CHANNEL"]
    identity = _slack("auth.test", token)
    if not identity.get("ok"):
        pytest.fail(f"SLACK_BOT_TOKEN is not usable: {identity.get('error', 'unknown')}")
    canvas_id = _channel_canvas_id(token, channel)
    if canvas_id is None:
        created = _slack_json(
            "conversations.canvases.create",
            token,
            {
                "channel_id": channel,
                "document_content": {"type": "markdown", "markdown": FIXTURE_MARKDOWN},
            },
        )
        if not created.get("ok") or not isinstance(created.get("canvas_id"), str):
            pytest.fail(
                "create the fixture canvas in the test channel: Slack refused "
                f"({created.get('error', 'unknown')})"
            )
        canvas_id = str(created["canvas_id"])
    live = LiveCanvas(token, channel, canvas_id)
    _sentinel_cell(_download(token, canvas_id))  # the fixture holds both tables
    return live


@dataclass
class LiveApi:
    client: TestClient
    auth: dict[str, str]

    def deploy(self, channel: str, grants: Mapping[str, Any]) -> tuple[str, str]:
        agent = self.client.post(
            "/agents",
            json={
                "name": f"live-canvas-{uuid.uuid4().hex[:8]}",
                "channel": {"kind": "slack", "address": channel},
            },
            headers=self.auth,
        )
        assert agent.status_code == 201, "agent create failed"
        agent_id = str(agent.json()["id"])
        version = self.client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": "live", "created_by": "operator"},
            headers=self.auth,
        )
        assert version.status_code == 201, "version create failed"
        version_id = version.json()["id"]
        upload = self.client.put(
            f"/agents/{agent_id}/versions/{version_id}/bundle",
            files={"file": ("reader-bot.tar.gz", _grants_archive(grants))},
            headers=self.auth,
        )
        assert upload.status_code == 201, "bundle upload failed"
        deployment = self.client.post(
            "/deployments",
            json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
            headers=self.auth,
        )
        assert deployment.status_code == 201, "deployment create failed"
        return agent_id, str(deployment.json()["id"])

    def mint(self, agent_id: str, deployment_id: str, channel: str) -> str:
        """A fresh logical turn's capability."""
        minted = self.client.post(
            MINT_URL,
            json={
                "agent_id": agent_id,
                "deployment_id": deployment_id,
                "event_id": f"live-{uuid.uuid4().hex}",
                "mode": "open",
                "owner": f"owner-{uuid.uuid4().hex}",
                "default_channel": {"kind": "slack", "address": channel},
                "ttl_s": 3600,
            },
            headers=WORKER_HEADERS,
        )
        assert minted.status_code == 200, f"mint answered {minted.status_code}"
        return str(minted.json()["token"])

    def canvas(self, token: str, **body: Any) -> httpx.Response:
        return self.client.post(CANVAS_URL, json=body, headers={CAPABILITY_HEADER: token})


@pytest.fixture
def live_api(
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    auth_headers: dict[str, str],
    live_canvas: LiveCanvas,
) -> Iterator[LiveApi]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", live_canvas.token)
    monkeypatch.setenv("RUNS_STREAM", f"test:curie:channel-canvas-live:{uuid.uuid4().hex}")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield LiveApi(client, auth_headers)
    get_settings.cache_clear()


def _texts(rows: list[list[dict[str, Any]]]) -> list[list[str]]:
    return [[cell["text"] for cell in row] for row in rows]


def _cells(page: dict[str, Any]) -> list[tuple[str | None, str]]:
    """Every table cell in document order, as (section id, text)."""
    return [
        (cell["section_id"], cell["text"])
        for table in page["tables"]
        for row in [table["header"], *table["rows"]]
        for cell in row
    ]


def _sentinel_of(page: dict[str, Any]) -> dict[str, Any]:
    header = [cell["text"] for cell in page["tables"][0]["header"]]
    return dict(page["tables"][0]["rows"][0][header.index("Status")])


def _assert_fixture_shape(page: dict[str, Any]) -> None:
    """AC1: both tables, header row by position, the fixture's rows."""
    assert len(page["tables"]) == 2, "the read did not return both tables"
    for table, header, rows in zip(page["tables"], FIXTURE_HEADERS, FIXTURE_ROWS, strict=True):
        header_ok = [cell["text"] for cell in table["header"]] == header
        assert header_ok, "a table's header row is not the fixture's"
        # The sentinel is compared on its own; every other cell exactly.
        got = _texts(table["rows"])
        want = [list(row) for row in rows]
        if header == FIXTURE_HEADERS[0] and got:
            got[0][2] = want[0][2]
        assert got == want, "a table's rows are not the fixture's"
    editable = all(cell[0] is not None for cell in _cells(page))
    assert editable, "a fixture cell came back without a section id"


def _audit_rows(agent_id: str) -> list[dict[str, Any]]:
    query = text(
        "SELECT status, before_text, after_text, completed_at FROM curie.channel_canvas_edits "
        "WHERE agent_id = :agent ORDER BY created_at"
    )

    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(query, {"agent": uuid.UUID(agent_id)})
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _read_edit_read(
    api: LiveApi, token: str, canvas_id: str, value: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read, edit the sentinel to ``value``, read again: three pages of one turn."""
    before = _page(api.canvas(token, operation="read", canvas_id=canvas_id))
    sentinel = _sentinel_of(before)
    edited = _page(
        api.canvas(
            token,
            operation="edit",
            canvas_id=canvas_id,
            section_id=sentinel["section_id"],
            text=value,
        )
    )
    assert edited.get("edited") is True, "the edit did not report success"
    after = _page(api.canvas(token, operation="read", canvas_id=canvas_id))
    return before, after


def test_live_fixture_canvas_is_listed_read_and_edited_in_one_cell(
    live_api: LiveApi, live_canvas: LiveCanvas
) -> None:
    _reset_sentinel(live_canvas)
    try:
        agent_id, deployment_id = live_api.deploy(live_canvas.channel, CANVAS_GRANTS)

        # AC3: the production list route returns the known fixture id. Slack's
        # file index can lag a fresh canvas, so the list is retried.
        token = live_api.mint(agent_id, deployment_id, live_canvas.channel)
        listed = False
        for attempt in range(LIST_ATTEMPTS):
            page = _page(live_api.canvas(token, operation="list"))
            listed = any(c.get("id") == live_canvas.canvas_id for c in page["canvases"])
            if listed:
                break
            if attempt + 1 < LIST_ATTEMPTS:
                time.sleep(LIST_RETRY_S)
        assert listed, "listing the test channel did not return the fixture canvas"

        # AC1, AC2 and AC5 in a fresh turn: read, edit the sentinel, read again.
        token = live_api.mint(agent_id, deployment_id, live_canvas.channel)
        sentinel = f"sentinel-{secrets.token_hex(4)}"
        before, after = _read_edit_read(live_api, token, live_canvas.canvas_id, sentinel)
        _assert_fixture_shape(before)
        baseline = _sentinel_of(before)["text"] == BASELINE
        assert baseline, "the sentinel cell did not read back exactly as the baseline"
        _assert_fixture_shape(after)
        changed = [
            index
            for index, (old, new) in enumerate(zip(_cells(before), _cells(after), strict=True))
            if old != new
        ]
        sentinel_index = _cells(before).index(
            (_sentinel_of(before)["section_id"], _sentinel_of(before)["text"])
        )
        assert changed == [sentinel_index], "the edit changed something besides the sentinel"
        same_ids = [c[0] for c in _cells(before)] == [c[0] for c in _cells(after)]
        assert same_ids, "a section id changed across the edit"
        exact = _sentinel_of(after)["text"] == sentinel
        assert exact, "the sentinel did not read back exactly as edited"
        audited = [
            row
            for row in _audit_rows(agent_id)
            if row["after_text"] == sentinel
            and row["before_text"] == BASELINE
            and row["status"] == "applied"
            and row["completed_at"] is not None
        ]
        assert len(audited) == 1, "the edit has no applied audit row with its before and after"

        # Text the rule must not refuse reads back verbatim, each in a fresh turn.
        for value in ("Q4: ship 0.13 & close #2881", "Status: blocked (waiting on API)"):
            token = live_api.mint(agent_id, deployment_id, live_canvas.channel)
            _, after = _read_edit_read(live_api, token, live_canvas.canvas_id, value)
            verbatim = _sentinel_of(after)["text"] == value
            assert verbatim, "plain cell text did not read back verbatim"
    finally:
        _reset_sentinel(live_canvas)


def test_live_canvas_for_an_unbound_agent_is_canvas_not_bound(
    live_api: LiveApi, live_canvas: LiveCanvas
) -> None:
    """AC4: an agent bound only to a channel the fixture is not shared into."""
    agent_id, deployment_id = live_api.deploy(UNBOUND, {"canvasRead": True})
    token = live_api.mint(agent_id, deployment_id, UNBOUND)
    response = live_api.canvas(token, operation="read", canvas_id=live_canvas.canvas_id)
    refused = (response.status_code, _code(response)) == (403, "channel_read.canvas_not_bound")
    assert refused, "a canvas shared only into an unbound channel was not canvas_not_bound"
    leaked = BASELINE in response.text or "Weekly plan review" in response.text
    assert not leaked, "the refusal carried canvas text"
