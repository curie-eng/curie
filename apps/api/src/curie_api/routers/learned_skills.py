"""Learned skills (ADR 0237): an agent's own Markdown skills, with history.

The runner reads them at boot with its broad ``state`` credential and changes
them with the per-turn credential the worker mints (``skills: "write"``); the
operator reads and repairs them with the platform key. The bundle-facing app
token is refused on every route, and the raw ``learned_skills`` state
namespace is refused to every sandbox credential (``routers.state``), so this
router is the only door. ``{ref}`` is a skill id or its slug.

Who may change a skill: an agent turn with a person behind it (never a
scheduled run), while its turn is open and the agent's toggle is on; the
operator always may delete and revert (repair while off), and may create and
patch while the toggle is on. Every change appends a revision and moves the
head under the agent row lock, guarded by ``expected_revision`` (stale is 409).
"""

import re
import uuid
from dataclasses import dataclass
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from ..crud import learned_skills as store
from ..deps import SessionDep
from ..models import LearnedSkill, LearnedSkillRevision
from ..schemas.learned_skills import (
    LearnedSkillCreate,
    LearnedSkillList,
    LearnedSkillOut,
    LearnedSkillPatch,
    LearnedSkillRevert,
    LearnedSkillRevisionOut,
    LearnedSkillRevisions,
)
from .state import StateCaller, StatePrincipal, require_state_access, require_turn_open

router = APIRouter(prefix="/agents/{agent_id}/learned-skills", tags=["learned-skills"])
Principal = Annotated[StatePrincipal, Depends(require_state_access)]

SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}\Z")
MAX_DESCRIPTION_CHARS = 1024
MAX_BODY_BYTES = 16384
MAX_ACTIVE_SKILLS = 32
# The runner's ``memory_facts.NO_PERSON``: the sender of a turn no person started.
NO_PERSON = "<no person>"
OFF = "learned skills are off for this agent"
SCHEDULED = "scheduled runs can read learned skills but cannot change them"


@dataclass(frozen=True)
class _Stamp:
    """Provenance of a change, taken from the credential, never the body."""

    author: str
    source: str
    conversation: str | None = None
    turn: str | None = None


_OPERATOR = _Stamp("operator", "operator")


def _forbidden(detail: str) -> HTTPException:
    return HTTPException(status.HTTP_403_FORBIDDEN, detail)


def _unprocessable(detail: str) -> HTTPException:
    return HTTPException(422, detail)


def _refuse_app(principal: StatePrincipal) -> None:
    if principal.caller is StateCaller.APP:
        raise _forbidden("the bundle token cannot reach learned skills")


def _check_content(slug: str, description: str, body: str) -> None:
    if SLUG.fullmatch(slug) is None:
        raise _unprocessable("slug must match ^[a-z0-9][a-z0-9-]{0,63}$")
    if len(description) > MAX_DESCRIPTION_CHARS or description.splitlines() != [description]:
        raise _unprocessable(
            f"description must be one line of 1 to {MAX_DESCRIPTION_CHARS} characters"
        )
    if not body.strip() or len(body.encode()) > MAX_BODY_BYTES:
        raise _unprocessable(f"body must be 1 to {MAX_BODY_BYTES} UTF-8 bytes")
    if next((line for line in body.splitlines() if line.strip()), "").strip() == "---":
        # The runner generates every skill's frontmatter (allowed-tools,
        # hooks and the rest); a body never authors it.
        raise _unprocessable("body must not open with a YAML frontmatter block")


async def _readable(session: SessionDep, agent_id: uuid.UUID, principal: StatePrincipal) -> bool:
    """Whether the agent's learned skills are on; 404 for no agent."""

    _refuse_app(principal)
    enabled = await store.toggle(session, agent_id)
    if enabled is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return enabled


async def _writer(
    session: SessionDep,
    request: Request,
    agent_id: uuid.UUID,
    principal: StatePrincipal,
    *,
    repair: bool,
) -> _Stamp:
    """Authorize a change and take the agent row lock it runs under.

    ``repair`` (delete, revert) lets the operator act while the toggle is off."""

    _refuse_app(principal)
    enabled = await store.lock_toggle(session, agent_id)
    if enabled is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if principal.caller is StateCaller.PLATFORM:
        if not enabled and not repair:
            raise _forbidden(OFF)
        return _OPERATOR
    if not enabled:
        raise _forbidden(OFF)
    if principal.skills != "write":
        raise _forbidden("this credential cannot change learned skills")
    sender = principal.sender
    if not sender:
        raise _forbidden("this credential names no sender")
    if sender == NO_PERSON or sender.startswith("cron:"):
        raise _forbidden(SCHEDULED)
    if not principal.turn or not principal.conversation:
        raise _forbidden("this credential names no turn or conversation")
    await require_turn_open(principal, agent_id, request, f"learned-skills/{agent_id}")
    return _Stamp(sender, "agent", principal.conversation, principal.turn)


async def _found(
    session: SessionDep, agent_id: uuid.UUID, ref: str, *, include_deleted: bool
) -> LearnedSkill:
    skill = await store.find(session, agent_id, ref, include_deleted=include_deleted)
    if skill is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such learned skill")
    return skill


def _check_head(skill: LearnedSkill, expected: int, *, live: bool = True) -> None:
    if skill.revision != expected:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"learned skill is at revision {skill.revision}, not {expected}",
        )
    if live and skill.deleted_at is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "learned skill is deleted; revert it first")


async def _check_room(session: SessionDep, agent_id: uuid.UUID, slug: str) -> None:
    """A skill about to become active needs a free slot and a free slug."""

    if await store.active_count(session, agent_id) >= MAX_ACTIVE_SKILLS:
        raise _unprocessable(f"an agent holds at most {MAX_ACTIVE_SKILLS} active learned skills")
    if await store.find(session, agent_id, slug, include_deleted=False) is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, f"learned skill {slug!r} already exists")


def _shown(principal: StatePrincipal, name: str) -> bool:
    """Conversation and turn are for the operator only; a sandbox response
    leaves them unset, and the routes drop unset fields."""

    return principal.caller is StateCaller.PLATFORM or name not in ("conversation", "turn")


def _skill_out(
    principal: StatePrincipal, skill: LearnedSkill, head: LearnedSkillRevision
) -> LearnedSkillOut:
    return LearnedSkillOut(
        id=skill.id,
        slug=skill.slug,
        description=skill.description,
        body=skill.body,
        revision=skill.revision,
        author=head.author,
        created_at=skill.created_at,
        updated_at=skill.updated_at,
        deleted=skill.deleted_at is not None,
        **({"conversation": head.conversation} if _shown(principal, "conversation") else {}),
    )


async def _head_out(
    session: SessionDep, principal: StatePrincipal, skill: LearnedSkill
) -> LearnedSkillOut:
    head = await store.revision(session, skill.id, skill.revision)
    assert head is not None  # every head revision is recorded with its skill
    return _skill_out(principal, skill, head)


async def _change(
    session: SessionDep,
    principal: StatePrincipal,
    stamp: _Stamp,
    skill: LearnedSkill,
    action: str,
    description: str,
    body: str,
    *,
    deleted: bool,
    reverted_to: int | None = None,
) -> LearnedSkillOut:
    """Record ``action`` as ``skill``'s next revision and commit."""

    head = await store.record(
        session,
        skill,
        action=action,
        description=description,
        body=body,
        deleted=deleted,
        reverted_to=reverted_to,
        **vars(stamp),
    )
    await session.commit()
    return _skill_out(principal, skill, head)


@router.get("", response_model=LearnedSkillList, response_model_exclude_unset=True)
async def list_learned_skills(
    agent_id: uuid.UUID, session: SessionDep, principal: Principal
) -> LearnedSkillList:
    """Active skills by slug. A sandbox credential sees none while the toggle is off."""

    enabled = await _readable(session, agent_id, principal)
    if not enabled and principal.caller is not StateCaller.PLATFORM:
        return LearnedSkillList(enabled=False, skills=[])
    rows = await store.active_with_heads(session, agent_id)
    return LearnedSkillList(
        enabled=enabled, skills=[_skill_out(principal, skill, head) for skill, head in rows]
    )


async def _visible(
    session: SessionDep, agent_id: uuid.UUID, ref: str, principal: StatePrincipal
) -> LearnedSkill:
    """A skill a read may see: the operator's include deleted ones, a sandbox
    credential's only active ones and only while the toggle is on."""

    platform = principal.caller is StateCaller.PLATFORM
    if not await _readable(session, agent_id, principal) and not platform:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such learned skill")
    return await _found(session, agent_id, ref, include_deleted=platform)


@router.get("/{ref}", response_model=LearnedSkillOut, response_model_exclude_unset=True)
async def get_learned_skill(
    agent_id: uuid.UUID, ref: str, session: SessionDep, principal: Principal
) -> LearnedSkillOut:
    skill = await _visible(session, agent_id, ref, principal)
    return await _head_out(session, principal, skill)


@router.get(
    "/{ref}/revisions", response_model=LearnedSkillRevisions, response_model_exclude_unset=True
)
async def list_learned_skill_revisions(
    agent_id: uuid.UUID, ref: str, session: SessionDep, principal: Principal
) -> LearnedSkillRevisions:
    skill = await _visible(session, agent_id, ref, principal)
    fields = [name for name in LearnedSkillRevisionOut.model_fields if _shown(principal, name)]
    return LearnedSkillRevisions(
        revisions=[
            LearnedSkillRevisionOut.model_validate({name: getattr(row, name) for name in fields})
            for row in await store.revisions(session, skill.id)
        ]
    )


@router.post(
    "",
    response_model=LearnedSkillOut,
    response_model_exclude_unset=True,
    status_code=status.HTTP_201_CREATED,
)
async def create_learned_skill(
    agent_id: uuid.UUID,
    data: LearnedSkillCreate,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> LearnedSkillOut:
    stamp = await _writer(session, request, agent_id, principal, repair=False)
    _check_content(data.slug, data.description, data.body)
    await _check_room(session, agent_id, data.slug)
    skill = LearnedSkill(agent_id=agent_id, slug=data.slug, revision=0)
    return await _change(
        session, principal, stamp, skill, "create", data.description, data.body, deleted=False
    )


@router.post("/{ref}/patch", response_model=LearnedSkillOut, response_model_exclude_unset=True)
async def patch_learned_skill(
    agent_id: uuid.UUID,
    ref: str,
    data: LearnedSkillPatch,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> LearnedSkillOut:
    """A targeted edit: ``old_string`` must occur exactly once in the body."""

    stamp = await _writer(session, request, agent_id, principal, repair=False)
    platform = principal.caller is StateCaller.PLATFORM
    skill = await _found(session, agent_id, ref, include_deleted=platform)
    _check_head(skill, data.expected_revision)
    if (data.old_string is None) != (data.new_string is None):
        raise _unprocessable("old_string and new_string are given together")
    if data.old_string is None and data.description is None:
        raise _unprocessable("a patch changes old_string/new_string, description or both")
    body = skill.body
    if data.old_string is not None and data.new_string is not None:
        found = body.count(data.old_string)
        if found != 1:
            raise _unprocessable(f"old_string must occur exactly once in the body, found {found}")
        body = body.replace(data.old_string, data.new_string, 1)
    description = skill.description if data.description is None else data.description
    _check_content(skill.slug, description, body)
    return await _change(
        session, principal, stamp, skill, "patch", description, body, deleted=False
    )


@router.post("/{ref}/revert", response_model=LearnedSkillOut, response_model_exclude_unset=True)
async def revert_learned_skill(
    agent_id: uuid.UUID,
    ref: str,
    data: LearnedSkillRevert,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> LearnedSkillOut:
    """Restore a revision's content and state. Without ``to_revision`` this is
    undo: back to the revision before the head, or a delete when the head is
    the create. Undo of an undo is a redo. Any caller reaches a deleted skill."""

    stamp = await _writer(session, request, agent_id, principal, repair=True)
    skill = await _found(session, agent_id, ref, include_deleted=True)
    _check_head(skill, data.expected_revision, live=False)
    if data.to_revision is None and skill.revision == 1:
        return await _change(
            session, principal, stamp, skill, "delete", skill.description, skill.body, deleted=True
        )
    number = skill.revision - 1 if data.to_revision is None else data.to_revision
    target = await store.revision(session, skill.id, number)
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"learned skill has no revision {number}")
    if skill.deleted_at is not None and not target.deleted:
        await _check_room(session, agent_id, skill.slug)
    return await _change(
        session,
        principal,
        stamp,
        skill,
        "revert",
        target.description,
        target.body,
        deleted=target.deleted,
        reverted_to=number,
    )


@router.delete("/{ref}", response_model=LearnedSkillOut, response_model_exclude_unset=True)
async def delete_learned_skill(
    agent_id: uuid.UUID,
    ref: str,
    request: Request,
    session: SessionDep,
    principal: Principal,
    expected_revision: Annotated[int, Query()],
) -> LearnedSkillOut:
    stamp = await _writer(session, request, agent_id, principal, repair=True)
    platform = principal.caller is StateCaller.PLATFORM
    skill = await _found(session, agent_id, ref, include_deleted=platform)
    _check_head(skill, expected_revision)
    return await _change(
        session, principal, stamp, skill, "delete", skill.description, skill.body, deleted=True
    )
