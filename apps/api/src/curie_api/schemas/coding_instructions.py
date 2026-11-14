"""Owner coding profile and tenant policy wire shapes (#5590)."""

from datetime import datetime
from typing import Literal

from curie_internal.requester_coding_instructions import MAX_REVISION
from pydantic import BaseModel, ConfigDict, Field


class CodingInstructionsWrite(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)

    text: str = Field(repr=False)
    expected_revision: int = Field(ge=0, le=MAX_REVISION)


class CodingInstructionsOut(BaseModel):
    revision: int
    sha256: str | None
    format_version: Literal[1]
    byte_length: int
    text: str | None = Field(repr=False)
    updated_at: datetime | None
    tenant_enabled: bool
    application: Literal["unavailable"]


class CodingInstructionsPolicyWrite(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)

    enabled: bool


class CodingInstructionsPolicyOut(BaseModel):
    enabled: bool
