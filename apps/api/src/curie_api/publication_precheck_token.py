"""The API issued publication read capability from ADR 0174."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from .sandbox_token import _b64url, _b64url_decode, _signature

_PREFIX = "ppc"


class PublicationPrecheckClaims(BaseModel):
    """Strict claims authenticated before any durable authority lookup."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    scope: Literal["publication.precheck"]
    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    runtime_epoch: int = Field(gt=0)
    conversation_id: str = Field(min_length=1, max_length=4096)
    lineage_id: uuid.UUID
    lineage_version: int = Field(gt=0)
    expected_head: str = Field(pattern=r"^[0-9a-f]{40}$")
    queued_event_id: str = Field(min_length=1, max_length=1024)
    observed_title_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: AwareDatetime
    iat: int = Field(ge=0)
    exp: int = Field(gt=0)


def metadata_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def mint(api_key: str, claims: PublicationPrecheckClaims) -> str:
    payload = json.dumps(
        claims.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    signed = f"{_PREFIX}.{_b64url(payload)}"
    return f"{signed}.{_signature(api_key, signed)}"


def verify_claims(token: str, api_key: str) -> PublicationPrecheckClaims | None:
    """Reject malformed or expired credentials without disclosing their content."""

    if len(token) > 16_384:
        return None
    try:
        prefix, payload, signature = token.split(".")
        if prefix != _PREFIX or not hmac.compare_digest(
            signature, _signature(api_key, f"{prefix}.{payload}")
        ):
            return None
        claims = PublicationPrecheckClaims.model_validate_json(_b64url_decode(payload))
    except (ValueError, TypeError, ValidationError):
        return None
    now = time.time()
    if claims.iat > now or claims.exp <= now or claims.exp <= claims.iat:
        return None
    if not claims.iat <= claims.observed_at.timestamp() < claims.exp:
        return None
    return claims
