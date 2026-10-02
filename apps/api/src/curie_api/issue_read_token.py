"""The API issued factory issue read capability from ADR 0187.

It follows the ADR 0174 publication precheck capability: an HMAC signed claim
set the sandbox presents to exactly one API route. It names one execution and
its WorkItem's repository and issue, so it can read nothing else.
"""

from __future__ import annotations

import hmac
import json
import time
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .sandbox_token import _b64url, _b64url_decode, _signature

_PREFIX = "wir"


class IssueReadClaims(BaseModel):
    """Strict claims authenticated before any durable authority lookup."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    scope: Literal["work_item.issue_read"]
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    repo_full_name: str = Field(min_length=3, max_length=512)
    github_repository_id: int = Field(gt=0)
    issue_number: int = Field(gt=0)
    iat: int = Field(ge=0)
    exp: int = Field(gt=0)


def mint(api_key: str, claims: IssueReadClaims) -> str:
    payload = json.dumps(
        claims.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    signed = f"{_PREFIX}.{_b64url(payload)}"
    return f"{signed}.{_signature(api_key, signed)}"


def verify_claims(token: str, api_key: str) -> IssueReadClaims | None:
    """Reject malformed or expired credentials without disclosing their content."""

    if len(token) > 4096:
        return None
    try:
        prefix, payload, signature = token.split(".")
        if prefix != _PREFIX or not hmac.compare_digest(
            signature, _signature(api_key, f"{prefix}.{payload}")
        ):
            return None
        claims = IssueReadClaims.model_validate_json(_b64url_decode(payload))
    except (ValueError, TypeError, ValidationError):
        return None
    now = time.time()
    if claims.iat > now or claims.exp <= now or claims.exp <= claims.iat:
        return None
    return claims
