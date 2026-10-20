"""Learned skills on the runner (epic #4780, ADR 0237).

The runner reads an agent's active learned skills at boot from the dedicated
learned-skills API, materializes them as a generated local plugin named
``curie-learned``, lists their qualified names beside the bundle's own (ADR
0189), mounts six in-process tools on the ``learned_skills`` server, and adds
the learned-skills guidance to the system prompt. With the toggle off, or the
store unreachable, none of that is present and the session boots as before.

The API is written in the same change, so the fake below follows the plan's
interface contract, not a running server: ``SkillOut`` is
``{id, slug, description, body, revision, author, conversation, updated_at,
created_at, deleted}`` (a STATE caller gets it without ``conversation``/``turn``,
R15, which is what the runner always is), list is ``{"enabled", "skills"}``,
and every error body is ``{"detail": "..."}``.

Names this file pins beyond the build brief (the implementer builds to them):

* ``build_runner(..., learned_skills: MaterializedPlugin | None = None)`` and a
  ``_BootFetches.learned_skills`` field of the same name, filled by
  ``_load_boot_fetches`` through ``load_for_session``; ``_serve`` feeds the
  fetches to ``build_runner`` by field name, as for memory facts.
* Reads and writes send the token as ``X-API-Key``, like ``MemoryFactsStore``.
* Tool arguments: ``read {skill}``, ``create {slug, description, body}``,
  ``patch {skill, expected_revision, old_string?, new_string?, description?}``,
  ``delete {skill, expected_revision}``, ``revert {skill, expected_revision,
  to_revision?}``; ``skill`` is the id or the slug.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import logging
import os
import stat
import tempfile
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
import yaml
from aci_protocol import Event, ToolAccess
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.memory_facts import NO_PERSON, MemoryTurn

READ_TOKEN = "boot-read-token-SECRET-1111"
WRITE_TOKEN = "turn-write-token-SECRET-2222"
AGENT = "0b9f3c1e-agent"
BASE = f"/agents/{AGENT}/learned-skills"
STATE = f"/agents/{AGENT}/state"
BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
UNREACHABLE_LOG = "learned skills store unreachable; starting without learned skills"
JOB_REFUSAL = "Scheduled runs can read learned skills but cannot change them"
TOOL_SHORT_NAMES = ("list", "read", "create", "patch", "delete", "revert")


def _live(name: str) -> str:
    return f"mcp__learned_skills__{name}"


LIVE_NAMES = frozenset(_live(name) for name in TOOL_SHORT_NAMES)


# --------------------------------------------------------------------------- #
# The fake learned-skills API (shapes from the plan's interface contract)
# --------------------------------------------------------------------------- #


class FakeLearnedSkillsApi:
    """An in-memory ``/agents/{id}/learned-skills`` router over real HTTP."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.skills: dict[str, dict[str, Any]] = {}
        # Per skill id, the content after each revision (index 0 is revision 1).
        self.history: dict[str, list[dict[str, Any]]] = {}
        self.requests: list[dict[str, Any]] = []
        # (status, detail) every request answers with, when set.
        self.fail: tuple[int, str] | None = None
        # After a write commits, the response is lost: "drop" closes the
        # connection, "500" answers a server error, "garbage" answers 200 with
        # a body that is not JSON.
        self.lose_response: str | None = None

    def seed(self, slug: str, description: str, body: str, *, revisions: int = 1) -> dict[str, Any]:
        skill_id = str(uuid.uuid4())
        content = {"slug": slug, "description": description, "body": body, "deleted": False}
        self.history[skill_id] = [dict(content) for _ in range(revisions)]
        self.skills[skill_id] = {"id": skill_id, **content, "revision": revisions, "author": "U1"}
        return self.skills[skill_id]

    def writes(self) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["method"] in ("POST", "DELETE")]

    def _out(self, skill: dict[str, Any]) -> dict[str, Any]:
        # The STATE representation (R15): no conversation, no turn.
        return {
            "id": skill["id"],
            "slug": skill["slug"],
            "description": skill["description"],
            "body": skill["body"],
            "revision": skill["revision"],
            "author": skill["author"],
            "created_at": "2026-10-01T00:00:00+00:00",
            "updated_at": "2026-10-02T00:00:00+00:00",
            "deleted": skill["deleted"],
        }

    def _find(self, ref: str) -> dict[str, Any] | None:
        if ref in self.skills:
            return self.skills[ref]
        active = [s for s in self.skills.values() if s["slug"] == ref and not s["deleted"]]
        if active:
            return active[0]
        deleted = [s for s in self.skills.values() if s["slug"] == ref]
        return deleted[-1] if deleted else None

    def _advance(self, skill: dict[str, Any], content: dict[str, Any]) -> None:
        skill.update(content)
        skill["revision"] += 1
        skill["author"] = "U123"
        self.history[skill["id"]].append(
            {k: skill[k] for k in ("slug", "description", "body", "deleted")}
        )

    def app(self) -> web.Application:
        def error(status: int, detail: str) -> web.Response:
            return web.json_response({"detail": detail}, status=status)

        async def handle(request: web.Request) -> web.StreamResponse:
            response = await commit(request)
            if self.lose_response and request.method in ("POST", "DELETE"):
                if self.lose_response == "drop":
                    assert request.transport is not None
                    request.transport.close()
                    return web.Response(status=204)
                if self.lose_response == "500":
                    return error(500, "internal error")
                return web.Response(text="<html>not json</html>", content_type="text/html")
            return response

        async def commit(request: web.Request) -> web.StreamResponse:
            body = await request.json() if request.can_read_body else None
            self.requests.append(
                {
                    "method": request.method,
                    "path": request.path,
                    "query": dict(request.query),
                    "token": request.headers.get("X-API-Key"),
                    "body": body,
                }
            )
            if self.fail is not None:
                return error(*self.fail)
            if not request.path.startswith(BASE):
                return error(404, "no route")
            rest = request.path[len(BASE) :].strip("/")
            parts = rest.split("/") if rest else []
            if not parts and request.method == "GET":
                active = sorted(
                    (s for s in self.skills.values() if not s["deleted"]),
                    key=lambda s: s["slug"],
                )
                return web.json_response(
                    {
                        "enabled": self.enabled,
                        "skills": [self._out(s) for s in active] if self.enabled else [],
                    }
                )
            if not parts and request.method == "POST":
                assert isinstance(body, dict)
                active = (s for s in self.skills.values() if not s["deleted"])
                if any(s["slug"] == body["slug"] for s in active):
                    return error(409, f"a learned skill named {body['slug']} already exists")
                skill = self.seed(body["slug"], body["description"], body["body"])
                skill["author"] = "U123"
                return web.json_response(self._out(skill), status=201)
            skill = self._find(parts[0]) if parts else None
            if skill is None:
                return error(404, "learned skill not found")
            if len(parts) == 1 and request.method == "GET":
                return web.json_response(self._out(skill))
            expected = (
                int(request.query["expected_revision"])
                if request.method == "DELETE"
                else (body or {}).get("expected_revision")
            )
            if expected != skill["revision"]:
                return error(
                    409, f"stale revision: expected {expected}, head is {skill['revision']}"
                )
            if len(parts) == 1 and request.method == "DELETE":
                self._advance(skill, {"deleted": True})
                return web.json_response(self._out(skill))
            assert isinstance(body, dict)
            if parts[1:] == ["patch"]:
                change: dict[str, Any] = {}
                if body.get("old_string") is not None:
                    if skill["body"].count(body["old_string"]) != 1:
                        return error(422, "old_string must occur exactly once in the body")
                    change["body"] = skill["body"].replace(
                        body["old_string"], body.get("new_string") or "", 1
                    )
                if body.get("description") is not None:
                    change["description"] = body["description"]
                if not change:
                    return error(422, "a patch must change the body or the description")
                self._advance(skill, change)
                return web.json_response(self._out(skill))
            if parts[1:] == ["revert"]:
                target = body.get("to_revision") or skill["revision"] - 1
                if target < 1:
                    self._advance(skill, {"deleted": True})
                else:
                    self._advance(skill, dict(self.history[skill["id"]][target - 1]))
                return web.json_response(self._out(skill))
            return error(404, "no route")

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", handle)
        return app


def _run(fn: Callable[[TestServer], Any], api: FakeLearnedSkillsApi) -> Any:
    async def go() -> Any:
        async with TestServer(api.app()) as server:
            return await fn(server)

    return anyio.run(go)


def _client(server: TestServer, write: Callable[[], str | None] = lambda: WRITE_TOKEN) -> Any:
    from curie_runner.learned_skills import LearnedSkillsClient

    return LearnedSkillsClient(str(server.make_url(BASE)), READ_TOKEN, write)


@pytest.fixture(autouse=True)
def _writable_tree(tmp_path: Path) -> Iterator[None]:
    """Materialized trees are 0555/0444; make them removable after each test."""

    yield
    for dirpath, _dirs, files in os.walk(tmp_path):
        os.chmod(dirpath, 0o755)
        for name in files:
            os.chmod(os.path.join(dirpath, name), 0o644)


@pytest.fixture
def scratch_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``tempfile.mkdtemp`` at the test's own directory."""

    root = tmp_path / "tmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    return root


# --------------------------------------------------------------------------- #
# 1. URL derivation (R1)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("state_url", "expected"),
    [
        (
            "http://curie-api:8000/agents/abc/state",
            "http://curie-api:8000/agents/abc/learned-skills",
        ),
        (
            "http://curie-api:8000/agents/abc/state/",
            "http://curie-api:8000/agents/abc/learned-skills",
        ),
        (
            "https://api.example/agents/abc/state/bindings/slack/T01%2FC02",
            "https://api.example/agents/abc/learned-skills",
        ),
        (
            "https://api.example/base/agents/abc/state/bindings/github/org%2Frepo%23issue-7",
            "https://api.example/base/agents/abc/learned-skills",
        ),
    ],
    ids=["agent-wide", "trailing-slash", "binding-encoded", "path-prefix-binding"],
)
def test_learned_skills_url_derives_the_agent_prefix(state_url: str, expected: str) -> None:
    from curie_runner.learned_skills import learned_skills_url

    assert learned_skills_url(state_url) == expected


@pytest.mark.parametrize(
    "state_url",
    [
        "",
        "http://curie-api:8000/v1/memory",
        "http://curie-api:8000/agents/abc/statefoo",
        "http://curie-api:8000/agents//state",
        "http://curie-api:8000/agents/abc",
    ],
)
def test_learned_skills_url_is_none_without_an_agent_state_path(state_url: str) -> None:
    from curie_runner.learned_skills import learned_skills_url

    assert learned_skills_url(state_url) is None


# --------------------------------------------------------------------------- #
# 2. The client
# --------------------------------------------------------------------------- #


def test_load_active_returns_enabled_and_skills_with_the_read_token() -> None:
    from curie_runner.learned_skills import LearnedSkill

    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.", revisions=3)
    api.seed("bake-bread", "Bake bread.", "Knead.")

    async def go(server: TestServer) -> tuple[bool, list[Any]]:
        return await _client(server).load_active()

    enabled, skills = _run(go, api)

    assert enabled is True
    assert all(isinstance(s, LearnedSkill) for s in skills)
    assert [(s.slug, s.revision) for s in skills] == [("bake-bread", 1), ("deploy-app", 3)]
    assert skills[1].description == "Deploy the app."
    assert skills[1].body == "Run make deploy."
    assert skills[1].id == api._find("deploy-app")["id"]  # noqa: SLF001
    assert [(r["method"], r["path"], r["token"]) for r in api.requests] == [
        ("GET", BASE, READ_TOKEN)
    ]


def test_load_active_reports_disabled() -> None:
    api = FakeLearnedSkillsApi(enabled=False)
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    async def go(server: TestServer) -> tuple[bool, list[Any]]:
        return await _client(server).load_active()

    assert _run(go, api) == (False, [])


def test_reads_use_the_read_token_and_writes_use_the_turn_token() -> None:
    api = FakeLearnedSkillsApi()
    seeded = api.seed("deploy-app", "Deploy the app.", "Run make deploy.\nThen check.")

    async def go(server: TestServer) -> dict[str, Any]:
        client = _client(server)
        out: dict[str, Any] = {}
        out["list"] = await client.list()
        out["read"] = await client.read("deploy-app")
        out["created"] = await client.create("bake-bread", 'Bake "rye": dark', "Knead.")
        out["patched"] = await client.patch(
            "deploy-app", 1, old_string="make deploy", new_string="make release"
        )
        out["described"] = await client.patch(seeded["id"], 2, description="Ship it.")
        out["reverted"] = await client.revert("deploy-app", 3)
        out["reverted_to"] = await client.revert("deploy-app", 4, to_revision=1)
        out["deleted"] = await client.delete("bake-bread", 1)
        return out

    out = _run(go, api)

    assert [s.slug for s in out["list"]] == ["deploy-app"]
    assert out["read"].body == "Run make deploy.\nThen check."
    assert (out["created"].slug, out["created"].revision) == ("bake-bread", 1)
    assert out["created"].description == 'Bake "rye": dark'
    assert out["patched"].body == "Run make release.\nThen check."
    assert out["patched"].revision == 2
    assert out["described"].description == "Ship it."
    assert out["reverted"].revision == 4
    assert out["reverted"].description == "Deploy the app."
    assert out["reverted_to"].revision == 5
    assert out["reverted_to"].body == "Run make deploy.\nThen check."

    reads = [r for r in api.requests if r["method"] == "GET"]
    assert reads and all(r["token"] == READ_TOKEN for r in reads), reads
    writes = api.writes()
    assert len(writes) == 6
    assert all(r["token"] == WRITE_TOKEN for r in writes), writes

    create, patch, describe, revert, revert_to, delete = writes
    assert (create["method"], create["path"]) == ("POST", BASE)
    assert create["body"] == {
        "slug": "bake-bread",
        "description": 'Bake "rye": dark',
        "body": "Knead.",
    }
    assert patch["path"] == f"{BASE}/deploy-app/patch"
    assert patch["body"]["expected_revision"] == 1
    assert patch["body"]["old_string"] == "make deploy"
    assert patch["body"]["new_string"] == "make release"
    assert patch["body"].get("description") is None
    # Never a wholesale body field.
    assert "body" not in patch["body"]
    assert describe["path"] == f"{BASE}/{seeded['id']}/patch"
    assert describe["body"]["description"] == "Ship it."
    assert describe["body"].get("old_string") is None
    assert revert["path"] == f"{BASE}/deploy-app/revert"
    assert revert["body"]["expected_revision"] == 3
    assert revert["body"].get("to_revision") is None
    assert revert_to["body"] == {"expected_revision": 4, "to_revision": 1}
    assert (delete["method"], delete["path"]) == ("DELETE", f"{BASE}/bake-bread")
    assert delete["query"] == {"expected_revision": "1"}


@pytest.mark.parametrize(
    ("status", "detail", "call"),
    [
        (403, "scheduled runs can read learned skills but cannot change them", "create"),
        (409, "stale revision: expected 1, head is 2", "patch"),
        (422, "body must be at most 16384 bytes", "create"),
        (404, "learned skill not found", "read"),
    ],
)
def test_api_errors_raise_with_status_and_detail_and_never_the_token(
    status: int, detail: str, call: str
) -> None:
    from curie_runner.learned_skills import LearnedSkillsError

    api = FakeLearnedSkillsApi()
    api.fail = (status, detail)

    async def go(server: TestServer) -> None:
        client = _client(server)
        if call == "create":
            await client.create("x", "d", "b")
        elif call == "patch":
            await client.patch("x", 1, old_string="a", new_string="b")
        else:
            await client.read("x")

    with pytest.raises(LearnedSkillsError) as caught:
        _run(go, api)

    assert caught.value.status == status
    assert detail in str(caught.value)
    for rendered in (str(caught.value), repr(caught.value)):
        assert READ_TOKEN not in rendered
        assert WRITE_TOKEN not in rendered


def test_an_unreachable_store_raises_learned_skills_error() -> None:
    from curie_runner.learned_skills import LearnedSkillsClient, LearnedSkillsError

    with socket_closed_port() as port:
        client = LearnedSkillsClient(
            f"http://127.0.0.1:{port}{BASE}", READ_TOKEN, lambda: WRITE_TOKEN
        )
        with pytest.raises(LearnedSkillsError) as caught:
            anyio.run(client.list)
    assert READ_TOKEN not in str(caught.value)


class socket_closed_port:  # noqa: N801 - used as a context manager
    """A localhost port nothing listens on."""

    def __enter__(self) -> int:
        import socket

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port

    def __exit__(self, *_exc: object) -> None:
        return None


# --------------------------------------------------------------------------- #
# 3. Materialize (R12)
# --------------------------------------------------------------------------- #


def _skill(slug: str, description: str = "Deploy the app.", body: str = "Run it.", rev: int = 1):
    from curie_runner.learned_skills import LearnedSkill

    return LearnedSkill(
        id=str(uuid.uuid4()), slug=slug, description=description, body=body, revision=rev
    )


def _frontmatter(text: str) -> tuple[dict[str, Any], str]:
    assert text.startswith("---\n")
    head, _, rest = text[4:].partition("\n---\n")
    return yaml.safe_load(head), rest


def test_materialize_writes_a_curie_learned_plugin(tmp_path: Path) -> None:
    from curie_runner.learned_skills import LEARNED_PLUGIN_NAME, materialize

    root = tmp_path / "learned"
    root.mkdir()
    body = "1. Run `make deploy`.\n2. Watch the rollout.\n"
    plugin = materialize(
        [_skill("deploy-app", body=body, rev=3), _skill("bake-bread", "Bake bread.", "Knead.")],
        root=root,
    )

    assert LEARNED_PLUGIN_NAME == "curie-learned"
    assert Path(plugin.root) == root
    manifest = json.loads((root / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))
    assert manifest["name"] == "curie-learned"
    skill_md = (root / "skills" / "deploy-app" / "SKILL.md").read_text(encoding="utf-8")
    assert skill_md == f'---\nname: deploy-app\ndescription: "Deploy the app."\n---\n\n{body}'
    assert sorted(plugin.qualified_names) == [
        "curie-learned:bake-bread",
        "curie-learned:deploy-app",
    ]
    assert sorted(plugin.loaded) == ["bake-bread@1", "deploy-app@3"]
    # The same names the bundle reader would give this tree, so the SDK lists them.
    from curie_runner.plugin import _validate_skill_name, bundle_skill_names

    assert sorted(bundle_skill_names(str(root))) == sorted(plugin.qualified_names)
    for name in plugin.qualified_names:
        _validate_skill_name(name)


@pytest.mark.parametrize(
    "description",
    [
        'Say "hi": then leave',
        "key: value # not a comment",
        "--- not a fence",
        "a\nname: evil\nallowed-tools: Bash",
        "[list, like] {map: like} & *anchor !tag",
    ],
)
def test_description_is_quoted_so_frontmatter_holds_only_name_and_description(
    tmp_path: Path, description: str
) -> None:
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    materialize([_skill("tricky", description=description)], root=root)

    text = (root / "skills" / "tricky" / "SKILL.md").read_text(encoding="utf-8")
    front, rest = _frontmatter(text)
    assert front == {"name": "tricky", "description": description}
    assert f"description: {json.dumps(description)}\n" in text
    assert rest == "\nRun it."


def test_materialized_files_are_0444_and_dirs_0555(tmp_path: Path) -> None:
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    materialize([_skill("deploy-app"), _skill("bake-bread")], root=root)

    files = [p for p in root.rglob("*") if p.is_file()]
    dirs = [p for p in root.rglob("*") if p.is_dir()]
    assert len(files) == 3, files
    assert {root / ".claude-plugin", root / "skills", root / "skills" / "deploy-app"} <= set(dirs)
    for path in files:
        assert stat.S_IMODE(path.stat().st_mode) == 0o444, path
    for path in dirs:
        assert stat.S_IMODE(path.stat().st_mode) == 0o555, path


def test_materialize_defaults_to_a_fresh_tmp_dir(scratch_tmp: Path) -> None:
    from curie_runner.learned_skills import materialize

    plugin = materialize([_skill("deploy-app")])

    root = Path(plugin.root)
    assert root.parent == scratch_tmp
    assert root.name.startswith("curie-learned-skills-")
    assert (root / "skills" / "deploy-app" / "SKILL.md").is_file()


@pytest.mark.parametrize("slug", ["../escape", "a/b", "a,b", "", "Upper"])
def test_a_slug_outside_the_rule_never_writes_outside_the_root_or_lists(
    tmp_path: Path, slug: str
) -> None:
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    try:
        plugin = materialize([_skill(slug), _skill("good")], root=root)
    except ValueError:
        # Refusing the whole set is acceptable; LearnedSkillsError may subclass it.
        plugin = None
    except Exception as exc:  # noqa: BLE001
        from curie_runner.learned_skills import LearnedSkillsError

        assert isinstance(exc, LearnedSkillsError), exc
        plugin = None
    assert not (tmp_path / "escape").exists()
    if plugin is not None:
        assert plugin.qualified_names == ["curie-learned:good"]
        assert all(name.startswith("good@") for name in plugin.loaded)


# --------------------------------------------------------------------------- #
# 4. Gate shadowing on the learned root (R12)
# --------------------------------------------------------------------------- #


def test_a_materialized_learned_root_shadows_no_gate(tmp_path: Path) -> None:
    from curie_runner.approval import assert_gates_not_shadowed, build_approval_gate, shadowed_gates
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    # A body that talks about allowed-tools is still just body text.
    plugin = materialize(
        [_skill("deploy-app", body="allowed-tools: Bash\nRun Bash(make deploy).")], root=root
    )

    assert shadowed_gates(plugin.root, frozenset({"Bash", "Write"})) == ()
    gate = build_approval_gate(operator_tools=frozenset({"Bash"}), policy_routes={})
    assert gate is not None
    assert_gates_not_shadowed(str(plugin.root), gate)


def test_a_hand_written_allowed_tools_in_a_learned_root_would_be_flagged(tmp_path: Path) -> None:
    # Regression guard: the check really reads this tree, so a tampered or
    # hand-written learned SKILL.md that preauthorizes a gated tool is caught.
    from curie_runner.approval import shadowed_gates
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    materialize([_skill("deploy-app")], root=root)
    skill_md = root / "skills" / "deploy-app" / "SKILL.md"
    os.chmod(skill_md.parent, 0o755)
    os.chmod(skill_md, 0o644)
    skill_md.write_text(
        "---\nname: deploy-app\ndescription: x\nallowed-tools: Bash\n---\n\nRun it.\n",
        encoding="utf-8",
    )

    conflicts = shadowed_gates(str(root), frozenset({"Bash"}))
    assert len(conflicts) == 1


# --------------------------------------------------------------------------- #
# 5. load_for_session degrade paths
# --------------------------------------------------------------------------- #


def _bundle(root: Path, *, name: str = "acme-bot", extra: dict[str, Any] | None = None) -> str:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": name,
                "version": "0.1.0",
                "description": "t",
                "systemPrompt": "You are the acme bot.",
                **(extra or {}),
            }
        ),
        encoding="utf-8",
    )
    skill = root / "skills" / "greet"
    skill.mkdir(parents=True, exist_ok=True)
    (skill / "SKILL.md").write_text(
        "---\nname: greet\ndescription: Say hello.\n---\n\nSay hello.\n", encoding="utf-8"
    )
    return str(root)


def _session_env(state_url: str | None, token: str | None = READ_TOKEN) -> dict[str, str]:
    from curie_runner.memory import MEMORY_TOKEN_ENV
    from curie_runner.state import STATE_URL_ENV

    env: dict[str, str] = {}
    if state_url is not None:
        env[STATE_URL_ENV] = state_url
    if token is not None:
        env[MEMORY_TOKEN_ENV] = token
    return env


def _load(
    api: FakeLearnedSkillsApi,
    tmp_path: Path,
    *,
    bundle_name: str = "acme-bot",
    fake_model: bool = False,
    state_path: str = STATE,
    token: str | None = READ_TOKEN,
    url: bool = True,
) -> Any:
    from curie_runner.learned_skills import load_for_session

    bundle = _bundle(tmp_path / "bundle", name=bundle_name)

    async def go(server: TestServer) -> Any:
        env = _session_env(str(server.make_url(state_path)) if url else None, token)
        return await load_for_session(env, bundle_plugin_dir=bundle, fake_model=fake_model)

    return _run(go, api)


def test_load_for_session_materializes_the_active_skills(tmp_path: Path, scratch_tmp: Path) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.", revisions=2)

    plugin = _load(api, tmp_path)

    assert plugin is not None
    assert plugin.qualified_names == ["curie-learned:deploy-app"]
    assert plugin.loaded == ["deploy-app@2"]
    assert Path(plugin.root).parent == scratch_tmp
    assert [r["token"] for r in api.requests] == [READ_TOKEN]


def test_load_for_session_reaches_the_store_from_a_binding_state_url(
    tmp_path: Path, scratch_tmp: Path
) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    plugin = _load(api, tmp_path, state_path=f"{STATE}/bindings/slack/T01%2FC02")

    assert plugin is not None
    assert [r["path"] for r in api.requests] == [BASE]


def test_an_unreachable_store_starts_without_learned_skills(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from curie_runner.learned_skills import load_for_session

    caplog.set_level(logging.INFO, logger="curie_runner")
    bundle = _bundle(tmp_path / "bundle")
    with socket_closed_port() as port:
        env = _session_env(f"http://127.0.0.1:{port}{STATE}")
        result = anyio.run(
            lambda: load_for_session(env, bundle_plugin_dir=bundle, fake_model=False)
        )

    assert result is None
    messages = [r.getMessage() for r in caplog.records]
    assert any(UNREACHABLE_LOG in m for m in messages), messages
    assert not any(READ_TOKEN in m for m in messages)


def test_a_failing_store_starts_without_learned_skills(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    api = FakeLearnedSkillsApi()
    api.fail = (503, "unavailable")

    assert _load(api, tmp_path) is None
    assert any(UNREACHABLE_LOG in r.getMessage() for r in caplog.records)


def test_a_state_url_without_an_agent_prefix_starts_without_learned_skills(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    api = FakeLearnedSkillsApi()

    assert _load(api, tmp_path, state_path="/v1/memory") is None
    assert api.requests == []
    assert any(UNREACHABLE_LOG in r.getMessage() for r in caplog.records)


def test_toggle_off_loads_nothing(tmp_path: Path) -> None:
    api = FakeLearnedSkillsApi(enabled=False)
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    assert _load(api, tmp_path) is None


def test_a_bundle_named_curie_learned_loads_no_learned_skills(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    assert _load(api, tmp_path, bundle_name="curie-learned") is None
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("curie-learned" in r.getMessage() for r in errors), caplog.records


@pytest.mark.parametrize(
    "case", ["fake-model", "no-token", "no-url"], ids=["fake-model", "no-token", "no-url"]
)
def test_fake_model_or_missing_config_loads_nothing_without_network(
    tmp_path: Path, case: str
) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    result = _load(
        api,
        tmp_path,
        fake_model=case == "fake-model",
        token=None if case == "no-token" else READ_TOKEN,
        url=case != "no-url",
    )

    assert result is None
    assert api.requests == []


# --------------------------------------------------------------------------- #
# 6. The tools
# --------------------------------------------------------------------------- #


def _turn(event_type: str = "message", *, user: str = "U123", token: str | None = WRITE_TOKEN):
    turn = MemoryTurn()
    turn.begin(Event(type=event_type, text="go", user=user, ts="1", memory_token=token))
    return turn


def _is_error(result: mcp_types.CallToolResult) -> bool:
    return bool(getattr(result, "is_error", None) or getattr(result, "isError", False))


def _text(result: mcp_types.CallToolResult) -> str:
    return " ".join(getattr(block, "text", "") for block in result.content)


def _drive_tools(
    api: FakeLearnedSkillsApi,
    turn: MemoryTurn,
    script: list[tuple[str, dict[str, Any]]],
) -> tuple[set[str], list[mcp_types.CallToolResult]]:
    from claude_agent_sdk import create_sdk_mcp_server
    from curie_runner.harness.claude.learned_skill_tools import (
        LEARNED_SKILLS_SERVER_NAME,
        build_learned_skill_tools,
    )

    assert LEARNED_SKILLS_SERVER_NAME == "learned_skills"

    async def go(server: TestServer) -> tuple[set[str], list[mcp_types.CallToolResult]]:
        client = _client(server, lambda: turn.write_token)
        tools = build_learned_skill_tools(client=client, turn=turn)
        instance = create_sdk_mcp_server(name=LEARNED_SKILLS_SERVER_NAME, tools=tools)["instance"]
        listing = await instance.get_request_handler("tools/list").handler(
            None, mcp_types.PaginatedRequestParams()
        )
        call = instance.get_request_handler("tools/call")
        results = []
        for name, arguments in script:
            results.append(
                await call.handler(
                    None, mcp_types.CallToolRequestParams(name=name, arguments=arguments)
                )
            )
        return {tool.name for tool in listing.tools}, results

    return _run(go, api)


def test_the_six_tools_are_published() -> None:
    names, _ = _drive_tools(FakeLearnedSkillsApi(), _turn(), [])
    assert names == set(TOOL_SHORT_NAMES)


def test_list_and_read_show_the_skills() -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.", revisions=2)

    _names, (listed, read) = _drive_tools(
        api, _turn(), [("list", {}), ("read", {"skill": "deploy-app"})]
    )

    assert not _is_error(listed), _text(listed)
    assert "deploy-app" in _text(listed)
    assert "Deploy the app." in _text(listed)
    assert not _is_error(read), _text(read)
    assert "Run make deploy." in _text(read)
    assert "2" in _text(read)
    assert all(r["token"] == READ_TOKEN for r in api.requests)


def test_create_patch_revert_delete_drive_the_api_with_the_turn_token() -> None:
    api = FakeLearnedSkillsApi()

    _names, results = _drive_tools(
        api,
        _turn(),
        [
            (
                "create",
                {
                    "slug": "deploy-app",
                    "description": "Deploy the app.",
                    "body": "Run make deploy.",
                },
            ),
            (
                "patch",
                {
                    "skill": "deploy-app",
                    "expected_revision": 1,
                    "old_string": "make deploy",
                    "new_string": "make release",
                },
            ),
            ("revert", {"skill": "deploy-app", "expected_revision": 2}),
            ("delete", {"skill": "deploy-app", "expected_revision": 3}),
        ],
    )

    for result, revision in zip(results, (1, 2, 3, 4), strict=True):
        text = _text(result)
        assert not _is_error(result), text
        assert "deploy-app" in text
        assert str(revision) in text
        assert "undo" in text.lower()
    writes = api.writes()
    assert [(w["method"], w["path"]) for w in writes] == [
        ("POST", BASE),
        ("POST", f"{BASE}/deploy-app/patch"),
        ("POST", f"{BASE}/deploy-app/revert"),
        ("DELETE", f"{BASE}/deploy-app"),
    ]
    assert all(w["token"] == WRITE_TOKEN for w in writes), writes
    assert api._find("deploy-app")["deleted"] is True  # noqa: SLF001


def test_a_job_turn_reads_but_cannot_write() -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    turn = _turn("job", user="cron:nightly")
    assert turn.author == NO_PERSON

    _names, results = _drive_tools(
        api,
        turn,
        [
            ("list", {}),
            ("read", {"skill": "deploy-app"}),
            ("create", {"slug": "new-one", "description": "d", "body": "b"}),
            (
                "patch",
                {
                    "skill": "deploy-app",
                    "expected_revision": 1,
                    "old_string": "a",
                    "new_string": "b",
                },
            ),
            ("delete", {"skill": "deploy-app", "expected_revision": 1}),
            ("revert", {"skill": "deploy-app", "expected_revision": 1}),
        ],
    )

    listed, read, *writes = results
    assert not _is_error(listed) and "deploy-app" in _text(listed)
    assert not _is_error(read) and "Run make deploy." in _text(read)
    for result in writes:
        assert _is_error(result), _text(result)
        assert JOB_REFUSAL in _text(result)
    assert api.writes() == []


def test_a_turn_without_a_write_token_cannot_write() -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    _names, results = _drive_tools(
        api,
        _turn(token=None),
        [
            ("create", {"slug": "new-one", "description": "d", "body": "b"}),
            ("delete", {"skill": "deploy-app", "expected_revision": 1}),
            ("list", {}),
        ],
    )

    create, delete, listed = results
    assert _is_error(create) and _is_error(delete)
    assert not _is_error(listed)
    assert api.writes() == []


def test_an_api_refusal_is_a_tool_error_without_the_token() -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.", revisions=2)

    _names, (stale,) = _drive_tools(
        api,
        _turn(),
        [
            (
                "patch",
                {
                    "skill": "deploy-app",
                    "expected_revision": 1,
                    "old_string": "make",
                    "new_string": "just",
                },
            )
        ],
    )

    assert _is_error(stale)
    assert WRITE_TOKEN not in _text(stale) and READ_TOKEN not in _text(stale)
    assert api._find("deploy-app")["body"] == "Run make deploy."  # noqa: SLF001


_WRITE_CALLS: dict[str, dict[str, Any]] = {
    "create": {"slug": "bake-bread", "description": "Bake.", "body": "Knead."},
    "patch": {
        "skill": "deploy-app",
        "expected_revision": 1,
        "old_string": "make",
        "new_string": "just",
    },
    "delete": {"skill": "deploy-app", "expected_revision": 1},
    "revert": {"skill": "deploy-app", "expected_revision": 1, "to_revision": 1},
}


@pytest.mark.parametrize("tool", sorted(_WRITE_CALLS))
@pytest.mark.parametrize("mode", ["drop", "500", "garbage"])
def test_a_write_whose_response_is_lost_is_unconfirmed_not_unchanged(tool: str, mode: str) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    api.lose_response = mode

    _names, (result,) = _drive_tools(api, _turn(), [(tool, _WRITE_CALLS[tool])])

    assert len(api.writes()) == 1  # the write reached the store
    assert _is_error(result)
    text = _text(result)
    assert "could not confirm" in text.lower()
    assert "Nothing changed" not in text
    assert WRITE_TOKEN not in text and READ_TOKEN not in text


@pytest.mark.parametrize("tool", sorted(_WRITE_CALLS))
@pytest.mark.parametrize("status", [403, 409, 422])
def test_a_genuine_refusal_still_says_nothing_changed(tool: str, status: int) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    api.fail = (status, "refused by the store")

    _names, (result,) = _drive_tools(api, _turn(), [(tool, _WRITE_CALLS[tool])])

    assert _is_error(result)
    text = _text(result)
    assert "Nothing changed" in text
    assert "could not confirm" not in text.lower()


# --------------------------------------------------------------------------- #
# 7. Boot wiring through _load_boot_fetches and build_runner (R14)
# --------------------------------------------------------------------------- #

_PROBE = McpToolCapabilityProbe(complete=True, has_potential_write_tool=False, tool_count=0)


class _CapturedSession:
    def __init__(self, options: Any) -> None:
        self.options = options


def _boot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api: FakeLearnedSkillsApi,
    *,
    manifest_extra: dict[str, Any] | None = None,
) -> tuple[Any, Any, Any]:
    """Boot like ``_serve``: boot fetches feed ``build_runner`` by field name."""

    for key in (
        "CURIE_CHANNEL_MEMORY_REF",
        "CURIE_MEMORY_WRITES",
        "CURIE_MEMORY_REF",
        "CURIE_STATE_TOKEN",
        "CURIE_APPROVAL_REQUIRED_TOOLS",
    ):
        monkeypatch.delenv(key, raising=False)

    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        return _PROBE

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    captured: dict[str, Any] = {}

    async def go(server: TestServer) -> None:
        state_url = str(server.make_url(STATE))
        monkeypatch.setenv("CURIE_STATE_URL", state_url)
        monkeypatch.setenv("CURIE_MEMORY_TOKEN", READ_TOKEN)
        env = {
            "CURIE_PLUGIN_DIR": _bundle(tmp_path / "bundle", extra=manifest_extra),
            "CURIE_SESSION_ID": "s-learned",
            "CURIE_SANDBOX_ID": "b-learned",
            "CURIE_BUDGET": BUDGET,
            "CURIE_STATE_URL": state_url,
            "CURIE_MEMORY_TOKEN": READ_TOKEN,
        }
        config = RunnerConfig.from_env(env)
        fetches = await boot._load_boot_fetches(config, False, None)  # noqa: SLF001
        accepted = set(inspect.signature(build_runner).parameters)
        assert "learned_skills" in accepted
        kwargs = {
            f.name: getattr(fetches, f.name)
            for f in dataclasses.fields(fetches)
            if f.name in accepted
        }
        assert "learned_skills" in kwargs, "the boot fetch must feed build_runner by name"
        kwargs["mcp_capability"] = _PROBE
        runner = build_runner(config, fake_model=False, **kwargs)
        captured["runner"] = runner
        captured["options"] = runner._factory().options  # noqa: SLF001
        captured["learned"] = fetches.learned_skills

    _run(go, api)
    return captured["runner"], captured["options"], captured["learned"]


def _published(options: Any) -> set[str]:
    async def listed(instance: Any) -> list[str]:
        entry = instance.get_request_handler("tools/list")
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return [tool.name for tool in result.tools]

    names: set[str] = set()
    for server_name, config in options.mcp_servers.items():
        if config.get("type") != "sdk":
            continue
        for tool_name in anyio.run(listed, config["instance"]):
            names.add(f"mcp__{server_name}__{tool_name}")
    return names


def _plugin_paths(options: Any) -> list[str]:
    return [str(p.get("path")) for p in options.plugins]


def test_learned_skills_on_wires_plugin_skills_tools_and_guidance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scratch_tmp: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from curie_runner.learned_skills import LEARNED_SKILLS_GUIDANCE

    caplog.set_level(logging.INFO, logger="curie_runner")
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.", revisions=3)

    runner, options, learned = _boot(monkeypatch, tmp_path, api)

    assert learned is not None
    assert options.skills == ["acme-bot:greet", "curie-learned:deploy-app"]
    assert str(learned.root) in _plugin_paths(options)
    assert "learned_skills" in options.mcp_servers
    assert LIVE_NAMES <= _published(options)
    assert LEARNED_SKILLS_GUIDANCE in options.system_prompt
    # The tools need a turn to read the author and write token from, even with
    # memory writes off.
    assert runner._memory_turn is not None  # noqa: SLF001
    messages = [r.getMessage() for r in caplog.records]
    loaded = [m for m in messages if m.startswith("learned skills loaded session=s-learned")]
    assert loaded and "deploy-app@3" in loaded[0], messages


def test_learned_list_and_read_are_read_only_and_writes_are_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    runner, _options, _learned = _boot(monkeypatch, tmp_path, api)

    classifier = runner._classifier  # noqa: SLF001
    for name in (_live("list"), _live("read")):
        assert not classifier.is_side_effecting(name), name
    for name in (_live("create"), _live("patch"), _live("delete"), _live("revert")):
        assert classifier.is_side_effecting(name), name

    access = runner._tool_access  # noqa: SLF001
    access.begin(ToolAccess.READ_ONLY)
    try:
        assert access.refusal(_live("list")) is None
        assert access.refusal(_live("read")) is None
        for name in ("create", "patch", "delete", "revert"):
            assert access.refusal(_live(name)) is not None, name
    finally:
        access.begin(None)


def test_a_bundle_tool_policy_does_not_hide_or_block_the_learned_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    # Platform-owned tools are outside toolPolicy scope, like the memory tools.
    from claude_agent_sdk.types import PermissionResultAllow, ToolPermissionContext

    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    policy = {"toolPolicy": {"enforcement": "curie/mcp-tool-policy@1", "deny": ["*/*"]}}

    runner, options, _learned = _boot(monkeypatch, tmp_path, api, manifest_extra=policy)

    assert not (LIVE_NAMES & set(options.disallowed_tools or ()))
    assert runner._approval_gate is not None  # noqa: SLF001
    assert options.can_use_tool is not None

    async def decide(name: str) -> Any:
        return await options.can_use_tool(name, {}, ToolPermissionContext())

    for name in sorted(LIVE_NAMES):
        assert isinstance(anyio.run(decide, name), PermissionResultAllow), name


def test_tool_policy_still_denies_names_that_only_share_the_learned_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    from claude_agent_sdk.types import PermissionResultDeny, ToolPermissionContext

    api = FakeLearnedSkillsApi()
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    policy = {"toolPolicy": {"enforcement": "curie/mcp-tool-policy@1", "deny": ["*/*"]}}

    _runner, options, _learned = _boot(monkeypatch, tmp_path, api, manifest_extra=policy)
    assert options.can_use_tool is not None

    async def decide(name: str) -> Any:
        return await options.can_use_tool(name, {}, ToolPermissionContext())

    for name in ("mcp__learned_skills__create_extra", "mcp__learned_skillsx__create"):
        assert isinstance(anyio.run(decide, name), PermissionResultDeny), name


def test_learned_tools_are_not_exempt_from_tool_policy_when_not_mounted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    from claude_agent_sdk.types import PermissionResultAllow, ToolPermissionContext

    api = FakeLearnedSkillsApi(enabled=False)
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")
    policy = {"toolPolicy": {"enforcement": "curie/mcp-tool-policy@1", "deny": ["*/*"]}}

    _runner, options, learned = _boot(monkeypatch, tmp_path, api, manifest_extra=policy)
    assert learned is None
    assert options.can_use_tool is not None

    async def decide(name: str) -> Any:
        return await options.can_use_tool(name, {}, ToolPermissionContext())

    for name in sorted(LIVE_NAMES):
        assert not isinstance(anyio.run(decide, name), PermissionResultAllow), name


def test_learned_skills_off_wires_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    from curie_runner.learned_skills import LEARNED_SKILLS_GUIDANCE

    api = FakeLearnedSkillsApi(enabled=False)
    api.seed("deploy-app", "Deploy the app.", "Run make deploy.")

    runner, options, learned = _boot(monkeypatch, tmp_path, api)

    assert learned is None
    assert options.skills == ["acme-bot:greet"]
    assert not any("curie-learned-skills-" in p for p in _plugin_paths(options))
    assert "learned_skills" not in options.mcp_servers
    assert not (LIVE_NAMES & _published(options))
    assert LEARNED_SKILLS_GUIDANCE not in (options.system_prompt or "")
    assert list(scratch_tmp.iterdir()) == []


def test_fake_model_boot_wires_no_learned_skills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch_tmp: Path
) -> None:
    from curie_runner.learned_skills import materialize

    root = tmp_path / "learned"
    root.mkdir()
    learned = materialize([_skill("deploy-app")], root=root)
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": _bundle(tmp_path / "bundle"),
            "CURIE_SESSION_ID": "s-fake",
            "CURIE_SANDBOX_ID": "b-fake",
            "CURIE_BUDGET": BUDGET,
        }
    )

    async def go() -> Any:
        return await boot._load_boot_fetches(config, True, None)  # noqa: SLF001

    fetches = anyio.run(go)
    assert fetches.learned_skills is None
    # Even handed a plugin, the fake tier mounts no platform server.
    runner = build_runner(config, fake_model=True, learned_skills=learned)
    assert runner._memory_turn is None  # noqa: SLF001
