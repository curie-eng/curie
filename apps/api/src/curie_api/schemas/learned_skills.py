"""Learned skills (ADR 0247): request and response bodies.

Content limits (slug, description, body) are judged by the router after the
caller is authorized, so a patch is held to them on its result, not its input.
Responses to a sandbox credential leave ``conversation`` and ``turn`` unset,
and the routes drop unset fields, so only the operator sees full provenance.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class LearnedSkillCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str
    description: str
    body: str


class LearnedSkillPatch(BaseModel):
    """A targeted edit: ``old_string`` must occur exactly once in the body and
    is replaced by ``new_string``; ``description`` replaces the description.
    There is deliberately no wholesale ``body`` field."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int
    old_string: str | None = None
    new_string: str | None = None
    description: str | None = None


class LearnedSkillRevert(BaseModel):
    """Restore revision ``to_revision``'s content and state; omitted is undo."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int
    to_revision: int | None = None


class LearnedSkillOut(BaseModel):
    id: uuid.UUID
    slug: str
    description: str
    body: str
    revision: int
    author: str
    conversation: str | None = None
    created_at: datetime
    updated_at: datetime
    deleted: bool


class LearnedSkillList(BaseModel):
    enabled: bool
    skills: list[LearnedSkillOut]


class LearnedSkillRevisionOut(BaseModel):
    revision: int
    action: Literal["create", "patch", "revert", "delete"]
    slug: str
    description: str
    body: str
    digest: str
    deleted: bool
    author: str
    conversation: str | None = None
    turn: str | None = None
    source: Literal["agent", "operator"]
    reverted_to: int | None
    created_at: datetime


class LearnedSkillRevisions(BaseModel):
    revisions: list[LearnedSkillRevisionOut]
