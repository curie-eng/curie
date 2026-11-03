"""Wire shapes of the worker's UI evidence upload (plan 5.2).

The worker sends the normalized evidence index it built from the runner's
answer. Unknown keys are ignored, so an older API accepts a newer worker's
index. Display text is truncated rather than refused, because evidence is
optional and a long label must never cost the whole upload.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 100 * 1024 * 1024
MAX_INDEX_BYTES = 8 * 1024 * 1024
MAX_PATH_CHARS = 512
MAX_LABEL_CHARS = 200
MAX_REASON_CHARS = 1000
MAX_WARNINGS = 64

_OID = r"^[0-9a-f]{40}([0-9a-f]{24})?$"
_SHA256 = r"^[0-9a-f]{64}$"
_CODE = r"^[a-z][a-z0-9_:.-]{0,199}$"


def _truncated(limit: int) -> BeforeValidator:
    def cut(value: Any) -> Any:
        return value[:limit] if isinstance(value, str) else value

    return BeforeValidator(cut)


Oid = Annotated[str, Field(pattern=_OID)]
Code = Annotated[str, Field(pattern=_CODE)]


class UiEvidenceImageIn(BaseModel):
    """One manifest entry: where it was, what it shows, and whether the runner
    and worker bound and fetched it."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    ordinal: int = Field(ge=0, le=1_000_000)
    # Null only on a refused entry whose manifest value was missing or ill-typed.
    path: Annotated[str, _truncated(MAX_PATH_CHARS)] | None
    label: Annotated[str, _truncated(MAX_LABEL_CHARS)] | None
    state: Literal["before", "after"] | None
    size: int | None = Field(default=None, ge=0, le=MAX_IMAGE_BYTES)
    sha256: Annotated[str, Field(pattern=_SHA256)] | None = None
    refusal: Code | None = None
    bound: bool

    @model_validator(mode="after")
    def _described_unless_refused(self) -> UiEvidenceImageIn:
        if self.refusal is None and None in (self.path, self.label, self.state):
            raise ValueError("an unrefused entry names its path, label and state")
        return self


class UiEvidenceIndexIn(BaseModel):
    """The worker-to-API evidence index (``PUT .../ui-evidence``)."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    version: Literal[1]
    manifest_state: Literal["valid", "invalid"]
    manifest_refusal: Code | None = None
    status: Literal["complete", "partial", "skipped", "failed"] | None = None
    reason: Annotated[str, _truncated(MAX_REASON_CHARS)] | None = None
    base_sha: Oid
    manifest_base_sha: Oid | None = None
    manifest_head_sha: Oid | None = None
    candidate_dirty: bool | None = None
    manifest_candidate_tree: Oid | None = None
    candidate_tree: Oid | None = None
    head_tree: Oid | None = None
    warnings: list[Code] = Field(default_factory=list, max_length=MAX_WARNINGS)
    images: list[UiEvidenceImageIn] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ordinals(self) -> UiEvidenceIndexIn:
        ordinals = [image.ordinal for image in self.images]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("image ordinals must be unique")
        return self


class UiEvidenceIndexOut(BaseModel):
    status: str
    expected: list[int]


class UiEvidenceImageOut(BaseModel):
    ordinal: int
    stored: bool


class UiEvidenceSealOut(BaseModel):
    status: str
    stored: list[int]
    missing: list[int]
