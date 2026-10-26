"""The learned-skills tools on their own in-process SDK MCP server (ADR 0247).

``list`` and ``read`` use the boot read token. ``create``, ``patch``,
``delete`` and ``revert`` use the turn's own credential and are refused here on
a turn with no person behind it (a scheduled job) or with no credential; the API
enforces the same from the credential's claims. No argument names an author:
the API stamps it from the credential.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server, tool
from claude_agent_sdk.types import McpSdkServerConfig

from ...approval import _approval_error
from ...learned_skills import (
    LEARNED_SKILLS_SERVER_NAME,
    LearnedSkill,
    LearnedSkillsClient,
    LearnedSkillsError,
)
from ...memory_facts import NO_PERSON, MemoryTurn

logger = logging.getLogger(__name__)

JOB_REFUSAL = "Scheduled runs can read learned skills but cannot change them. Nothing changed."
NO_CREDENTIAL_REFUSAL = "Learned skills cannot be changed from this turn. Nothing changed."

_SKILL = {"type": "string", "description": "The skill's slug or id."}
_REVISION = {"type": "integer", "description": "The skill's current revision, as read."}


def _schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required}


_CREATE_SCHEMA = _schema(
    {
        "slug": {"type": "string", "description": "Lowercase name: letters, digits, hyphens."},
        "description": {"type": "string", "description": "One line: when to use this skill."},
        "body": {"type": "string", "description": "The procedure, in Markdown, no frontmatter."},
    },
    ["slug", "description", "body"],
)
_PATCH_SCHEMA = _schema(
    {
        "skill": _SKILL,
        "expected_revision": _REVISION,
        "old_string": {"type": "string", "description": "Text occurring exactly once in the body."},
        "new_string": {"type": "string", "description": "Replacement for old_string."},
        "description": {"type": "string", "description": "A new one-line description."},
    },
    ["skill", "expected_revision"],
)
_DELETE_SCHEMA = _schema(
    {"skill": _SKILL, "expected_revision": _REVISION}, ["skill", "expected_revision"]
)
_REVERT_SCHEMA = _schema(
    {
        "skill": _SKILL,
        "expected_revision": _REVISION,
        "to_revision": {
            "type": "integer",
            "description": "Restore this revision; omit to undo the last change.",
        },
    },
    ["skill", "expected_revision"],
)


def _ok(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _render(skill: LearnedSkill) -> str:
    return f"{skill.slug} (revision {skill.revision}, id {skill.id}): {skill.description}"


def _int(args: dict[str, Any], key: str) -> int:
    value = args.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise LearnedSkillsError(f"{key} must be a revision number", status=422)
    return value


def _failure(exc: LearnedSkillsError) -> dict[str, Any]:
    # The detail is the API's own message (a limit, a conflict, not found);
    # the exception never carries a token.
    if exc.status == 409:
        return _approval_error(
            f"Refused: {exc}. Read the skill again and retry against its current revision. "
            "Nothing changed."
        )
    if exc.status is not None and 400 <= exc.status < 500:
        return _approval_error(f"Refused: {exc}. Nothing changed.")
    logger.warning(
        "learned skill tool failed error_class=%s status=%s", type(exc).__name__, exc.status
    )
    return _approval_error("The learned skills store could not be reached. Nothing changed.")


def build_learned_skill_tools(
    *, client: LearnedSkillsClient, turn: MemoryTurn
) -> list[SdkMcpTool[Any]]:
    """The six tools. Every failure is an ``is_error`` result the model reads."""

    async def guarded(
        done: str, subject: str, call: Callable[[], Awaitable[LearnedSkill]]
    ) -> dict[str, Any]:
        if turn.author == NO_PERSON:
            return _approval_error(JOB_REFUSAL)
        if not turn.write_token:
            return _approval_error(NO_CREDENTIAL_REFUSAL)
        try:
            skill = await call()
        except LearnedSkillsError as exc:
            if exc.status is None or exc.status >= 500:
                logger.warning(
                    "learned skill write unconfirmed error_class=%s status=%s",
                    type(exc).__name__,
                    exc.status,
                )
                return _approval_error(
                    f"Could not confirm whether the change to {subject} was saved. "
                    "Read it with mcp__learned_skills__read before retrying."
                )
            return _failure(exc)
        return _ok(
            f"{done} learned skill {skill.slug}, now revision {skill.revision}. Tell the channel "
            f'the skill name {skill.slug} and that replying "undo" reverts it. The change takes '
            "effect in the next session."
        )

    @tool("list", "List this agent's learned skills with their revisions.", _schema({}, []))
    async def list_skills(_args: dict[str, Any]) -> dict[str, Any]:
        try:
            skills = await client.list()
        except LearnedSkillsError as exc:
            return _failure(exc)
        return _ok("\n".join(_render(s) for s in skills) or "No learned skills yet.")

    @tool(
        "read",
        "Read one learned skill, with its body and revision.",
        _schema({"skill": _SKILL}, ["skill"]),
    )
    async def read(args: dict[str, Any]) -> dict[str, Any]:
        try:
            skill = await client.read(str(args.get("skill")))
        except LearnedSkillsError as exc:
            return _failure(exc)
        return _ok(f"{_render(skill)}\n\n{skill.body}")

    @tool("create", "Save a new learned skill: a reusable multi-step procedure.", _CREATE_SCHEMA)
    async def create(args: dict[str, Any]) -> dict[str, Any]:
        return await guarded(
            "Saved",
            str(args.get("slug")),
            lambda: client.create(
                str(args.get("slug")), str(args.get("description")), str(args.get("body"))
            ),
        )

    @tool(
        "patch",
        "Correct a learned skill with a targeted edit: old_string, which must occur exactly "
        "once in the body, becomes new_string; description optionally replaces the description.",
        _PATCH_SCHEMA,
    )
    async def patch(args: dict[str, Any]) -> dict[str, Any]:
        return await guarded(
            "Changed",
            str(args.get("skill")),
            lambda: client.patch(
                str(args.get("skill")),
                _int(args, "expected_revision"),
                old_string=args.get("old_string"),
                new_string=args.get("new_string"),
                description=args.get("description"),
            ),
        )

    @tool("delete", "Delete a learned skill.", _DELETE_SCHEMA)
    async def delete(args: dict[str, Any]) -> dict[str, Any]:
        return await guarded(
            "Deleted",
            str(args.get("skill")),
            lambda: client.delete(str(args.get("skill")), _int(args, "expected_revision")),
        )

    @tool(
        "revert",
        'Undo the last change to a learned skill (what "undo" means), or restore to_revision.',
        _REVERT_SCHEMA,
    )
    async def revert(args: dict[str, Any]) -> dict[str, Any]:
        return await guarded(
            "Reverted",
            str(args.get("skill")),
            lambda: client.revert(
                str(args.get("skill")),
                _int(args, "expected_revision"),
                to_revision=_int(args, "to_revision") if "to_revision" in args else None,
            ),
        )

    return [list_skills, read, create, patch, delete, revert]


def build_learned_skills_server(
    *, client: LearnedSkillsClient, turn: MemoryTurn
) -> McpSdkServerConfig:
    return create_sdk_mcp_server(
        name=LEARNED_SKILLS_SERVER_NAME,
        version="1.0.0",
        tools=build_learned_skill_tools(client=client, turn=turn),
    )
