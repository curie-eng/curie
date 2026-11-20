"""Owner enrollment wire shapes (#5377, ADR 0268 section 3).

The CLI mirrors these components by name (``cli/api-mirrors.json``). No shape
here carries a Secret namespace, name, key or the grant; the grant is a
write-only ``SecretStr`` in the request alone. The token rule is the shared
vector ``tests/vectors/personal-enrollment-token.json``: 1 to 4096 characters,
each printable ASCII without space (0x21 to 0x7E). Nothing about a prefix or
shape is inferred, and acceptance never implies the grant is valid.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

TOKEN_MAX_LENGTH = 4096
_TOKEN = re.compile(r"[\x21-\x7e]+")

EnrollmentIntent = Literal["enroll", "replace", "reconnect"]
EnrollmentStateName = Literal["absent", "active", "inactive"]


class PersonalEnrollmentWrite(BaseModel):
    """``PUT /personal-claude/enrollment``. ``expected_version`` is null only for enroll."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    intent: EnrollmentIntent
    expected_version: int | None = Field(default=None, ge=1)
    token: SecretStr = Field(min_length=1, max_length=TOKEN_MAX_LENGTH)

    @field_validator("token")
    @classmethod
    def _printable(cls, value: SecretStr) -> SecretStr:
        if not _TOKEN.fullmatch(value.get_secret_value()):
            raise ValueError("token must be printable ASCII without spaces")
        return value

    @model_validator(mode="after")
    def _version_follows_intent(self) -> PersonalEnrollmentWrite:
        if (self.intent == "enroll") != (self.expected_version is None):
            raise ValueError("expected_version is required except for enroll")
        return self


class PersonalEnrollmentTeardownOut(BaseModel):
    """Teardown still owed after a replace or disconnect."""

    pending_runtimes: int = Field(ge=0)
    pending_custody_cleanup: int = Field(ge=0)


class PersonalEnrollmentOut(BaseModel):
    """The caller's own enrollment; ``generation`` and ``version`` are null when absent."""

    state: EnrollmentStateName
    generation: int | None
    version: int | None
    teardown: PersonalEnrollmentTeardownOut
