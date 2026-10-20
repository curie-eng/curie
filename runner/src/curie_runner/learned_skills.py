"""Learned skills: an agent's own Markdown skills, loaded per session (ADR 0237).

The runner reads the agent's active learned skills at boot from the dedicated
``/agents/<id>/learned-skills`` router, reached from the agent prefix of
``CURIE_STATE_URL`` with the boot read token. It materializes them as a
generated local plugin named ``curie-learned`` under ``/tmp`` (the root
filesystem is read-only), files 0444 and directories 0555, and lists their
qualified names beside the bundle's own (amending ADR 0189). The snapshot is
taken once per session, so a change made in a session takes effect in the next.

Writes go through six in-process tools (``harness/claude/learned_skill_tools.py``)
with the turn's own credential (``MemoryTurn.write_token``). This module stays
free of the harness SDK. With the toggle off, the store unreachable, or the fake
model, nothing loads: no plugin, no tools, no guidance.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
from plugin_format import resolve_manifest

from .memory import MEMORY_TOKEN_ENV
from .plugin import _validate_skill_name
from .state import STATE_URL_ENV
from .tool_names import LEARNED_SKILLS_SERVER_NAME as LEARNED_SKILLS_SERVER_NAME

logger = logging.getLogger(__name__)

LEARNED_PLUGIN_NAME = "curie-learned"
UNREACHABLE_LOG = "learned skills store unreachable; starting without learned skills"

# Adapted from the OpenClaw skill-workshop learning prompt (MIT) and Hermes
# skill_manage patch semantics (MIT). It rides in every prompt, so keep it short.
LEARNED_SKILLS_GUIDANCE = """\
Learned skills

You can keep reusable procedures as learned skills with the learned_skills tools. A multi-step procedure becomes a skill. Facts, preferences and one-line rules stay in memory. One turn may write both.
1. When someone says "remember how to do this" or otherwise asks you to keep a procedure, save it directly with create.
2. Offer rule: when a message explains how to do something in steps and does not ask you to keep it, your reply must end with one line of the form Save this as a skill: <slug>? (for example: Save this as a skill: gadget-return?). Add that line even when you also answer or carry out the procedure. Do not call create until the person replies yes; when they reply yes, create it.
3. When someone corrects a skill, read it first, then patch that skill with a targeted old_string/new_string edit against its current revision. Never create a duplicate.
4. When someone says "undo", call revert on the skill you most recently changed.
5. After any save or change, tell the channel the skill name and that replying "undo" reverts it.
6. When a request matches a learned skill, invoke it with the Skill tool (its name is curie-learned:<slug>) and follow it exactly.
A skill saved now takes effect in the next session."""  # noqa: E501

# ``\Z``, not ``$``: ``$`` also matches before a trailing newline.
_SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}\Z")
_AGENT_PREFIX = re.compile(r"^(?P<prefix>.*/agents/[^/]+)/state(?:/.*)?\Z")
_TIMEOUT = aiohttp.ClientTimeout(total=15)


class LearnedSkillsError(RuntimeError):
    """A learned-skills request failed. ``status`` is the HTTP status, if any.

    The message carries the API's ``detail`` only, never a token."""

    def __init__(self, detail: str, *, status: int | None = None) -> None:
        super().__init__(detail)
        self.status = status


@dataclass(frozen=True)
class LearnedSkill:
    id: str
    slug: str
    description: str
    body: str
    revision: int
    deleted: bool = False


@dataclass(frozen=True)
class MaterializedPlugin:
    """The generated ``curie-learned`` plugin: its root and what it lists."""

    root: str
    qualified_names: list[str] = field(default_factory=list)
    # ``slug@revision`` per loaded skill: the per-session snapshot record.
    loaded: list[str] = field(default_factory=list)


def learned_skills_url(state_url: str) -> str | None:
    """``<agent prefix>/learned-skills`` from a state URL, or None (R1)."""

    match = _AGENT_PREFIX.match(state_url or "")
    return f"{match.group('prefix')}/learned-skills" if match else None


def _parse(payload: object) -> LearnedSkill:
    if not isinstance(payload, Mapping):
        raise LearnedSkillsError("learned skill is not a JSON object")
    try:
        return LearnedSkill(
            id=str(payload["id"]),
            slug=str(payload["slug"]),
            description=str(payload["description"]),
            body=str(payload["body"]),
            revision=int(payload["revision"]),
            deleted=bool(payload.get("deleted", False)),
        )
    except (KeyError, TypeError, ValueError):
        raise LearnedSkillsError("learned skill is malformed") from None


async def _detail(resp: aiohttp.ClientResponse) -> str:
    text = await resp.text()
    try:
        payload = json.loads(text)
    except ValueError:
        return text[:200]
    detail = payload.get("detail") if isinstance(payload, Mapping) else None
    return detail if isinstance(detail, str) else text[:200]


class LearnedSkillsClient:
    """The agent's learned-skills router.

    Reads present ``read_token`` (the boot read token); writes present
    ``write_token()``, the turn's own credential, and are refused here without
    one. Both go as ``X-API-Key``, like ``MemoryFactsStore``.
    """

    def __init__(
        self, url: str, read_token: str | None, write_token: Callable[[], str | None]
    ) -> None:
        self._base = url.rstrip("/")
        self._read_token = read_token
        self._write_token = write_token

    def __repr__(self) -> str:
        return f"LearnedSkillsClient({self._base!r})"

    def _url(self, ref: str, action: str = "") -> str:
        return f"{self._base}/{quote(ref, safe='')}" + (f"/{action}" if action else "")

    async def _call(
        self,
        method: str,
        url: str,
        *,
        write: bool = False,
        json_body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> Any:
        token = self._write_token() if write else self._read_token
        if not token:
            raise LearnedSkillsError("no credential for this request", status=403)
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                # aiohttp resends idempotent methods (DELETE included) once after a dropped
                # connection and has no public switch for it. A resent write against an
                # expected revision turns a committed change into a false 409 refusal.
                session._retry_connection = False  # noqa: SLF001
                async with session.request(
                    method, url, json=json_body, params=params, headers={"X-API-Key": token}
                ) as resp:
                    if resp.status >= 400:
                        raise LearnedSkillsError(await _detail(resp), status=resp.status)
                    return await resp.json()
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            raise LearnedSkillsError(
                f"learned skills request failed error_class={type(exc).__name__}"
            ) from None

    async def load_active(self) -> tuple[bool, list[LearnedSkill]]:
        """``(enabled, active skills)``; ``(False, [])`` when the toggle is off."""

        payload = await self._call("GET", self._base)
        if not isinstance(payload, Mapping) or not isinstance(payload.get("skills"), list):
            raise LearnedSkillsError("learned skills list is malformed")
        if payload.get("enabled") is not True:
            return False, []
        return True, [_parse(item) for item in payload["skills"]]

    async def list(self) -> list[LearnedSkill]:
        return (await self.load_active())[1]

    async def read(self, ref: str) -> LearnedSkill:
        return _parse(await self._call("GET", self._url(ref)))

    async def create(self, slug: str, description: str, body: str) -> LearnedSkill:
        payload = {"slug": slug, "description": description, "body": body}
        return _parse(await self._call("POST", self._base, write=True, json_body=payload))

    async def patch(
        self,
        ref: str,
        expected_revision: int,
        *,
        old_string: str | None = None,
        new_string: str | None = None,
        description: str | None = None,
    ) -> LearnedSkill:
        payload: dict[str, Any] = {"expected_revision": expected_revision}
        for key, value in (
            ("old_string", old_string),
            ("new_string", new_string),
            ("description", description),
        ):
            if value is not None:
                payload[key] = value
        return _parse(
            await self._call("POST", self._url(ref, "patch"), write=True, json_body=payload)
        )

    async def revert(
        self, ref: str, expected_revision: int, *, to_revision: int | None = None
    ) -> LearnedSkill:
        payload: dict[str, Any] = {"expected_revision": expected_revision}
        if to_revision is not None:
            payload["to_revision"] = to_revision
        return _parse(
            await self._call("POST", self._url(ref, "revert"), write=True, json_body=payload)
        )

    async def delete(self, ref: str, expected_revision: int) -> LearnedSkill:
        params = {"expected_revision": str(expected_revision)}
        return _parse(await self._call("DELETE", self._url(ref), write=True, params=params))


def skill_markdown(skill: LearnedSkill) -> str:
    """The generated ``SKILL.md``: frontmatter holds only name and description.

    The description is JSON-quoted, which is a valid YAML double-quoted scalar,
    so no description can add a frontmatter key or close the block."""

    description = json.dumps(skill.description, ensure_ascii=False)
    return f"---\nname: {skill.slug}\ndescription: {description}\n---\n\n{skill.body}"


def materialize(skills: list[LearnedSkill], root: Path | str | None = None) -> MaterializedPlugin:
    """Write ``skills`` as the ``curie-learned`` plugin under ``root``.

    ``root`` defaults to a fresh ``curie-learned-skills-*`` dir from
    ``tempfile.mkdtemp``. A slug outside the API's rule, or one the SDK cannot
    carry, is skipped with a warning and never becomes a path.
    """

    base = (
        Path(root) if root is not None else Path(tempfile.mkdtemp(prefix="curie-learned-skills-"))
    )
    (base / ".claude-plugin").mkdir()
    (base / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": LEARNED_PLUGIN_NAME,
                "version": "1.0.0",
                "description": "Skills this agent learned (generated by the runner).",
            }
        ),
        encoding="utf-8",
    )
    (base / "skills").mkdir()
    names: list[str] = []
    loaded: list[str] = []
    for skill in sorted(skills, key=lambda s: s.slug):
        name = f"{LEARNED_PLUGIN_NAME}:{skill.slug}"
        try:
            if not _SLUG.match(skill.slug):
                raise ValueError("slug does not match the learned skill rule")
            _validate_skill_name(name)
        except ValueError as exc:
            logger.warning("skipping learned skill %r: %s", skill.slug, exc)
            continue
        skill_dir = base / "skills" / skill.slug
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text(skill_markdown(skill), encoding="utf-8")
        names.append(name)
        loaded.append(f"{skill.slug}@{skill.revision}")
    # Read-only snapshot (R12): deepest first, so each dir is written before it closes.
    for dirpath, _dirs, files in sorted(os.walk(base), key=lambda w: -len(w[0])):
        for name in files:
            os.chmod(os.path.join(dirpath, name), 0o444)
        os.chmod(dirpath, 0o555)
    return MaterializedPlugin(root=str(base), qualified_names=names, loaded=loaded)


def _bundle_name(plugin_dir: str | None) -> str | None:
    manifest = resolve_manifest(Path(plugin_dir)) if plugin_dir else None
    if manifest is None:
        return None
    try:
        name = json.loads(manifest.read_text(encoding="utf-8")).get("name")
    except (OSError, ValueError, AttributeError):
        return None
    return name if isinstance(name, str) else None


async def load_for_session(
    env: Mapping[str, str], *, bundle_plugin_dir: str | None, fake_model: bool
) -> MaterializedPlugin | None:
    """The session's learned-skills plugin, or None when nothing should load.

    The fake model and a boot without a state URL or read token load nothing
    and make no call. Any store failure degrades to None with one log line.
    """

    state_url = env.get(STATE_URL_ENV)
    token = env.get(MEMORY_TOKEN_ENV)
    if fake_model or not state_url or not token:
        return None
    url = learned_skills_url(state_url)
    if url is None:
        logger.warning("%s: the state URL has no agent prefix", UNREACHABLE_LOG)
        return None
    if _bundle_name(bundle_plugin_dir) == LEARNED_PLUGIN_NAME:
        logger.error(
            "bundle is named %s, which is reserved for learned skills; loading none",
            LEARNED_PLUGIN_NAME,
        )
        return None
    try:
        enabled, skills = await LearnedSkillsClient(url, token, lambda: None).load_active()
    except LearnedSkillsError as exc:
        logger.warning("%s status=%s", UNREACHABLE_LOG, exc.status)
        return None
    if not enabled:
        return None
    try:
        return materialize(skills)
    except OSError as exc:
        logger.warning(
            "learned skills not materialized error_class=%s; starting without learned skills",
            type(exc).__name__,
        )
        return None
