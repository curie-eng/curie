"""One bounded directory capability; no other API credential accepts this prefix."""

from __future__ import annotations

import hmac
import json
import time
import uuid
from dataclasses import dataclass

from curie_internal.sandbox_token import b64url, b64url_decode, signature

from .directory_sync import DirectoryScope, DirectorySyncError


@dataclass(frozen=True)
class DirectoryClaims:
    scope: DirectoryScope
    namespace_ids: frozenset[uuid.UUID]
    config_version: int
    exp: int


def mint(
    api_key: str,
    *,
    scope: DirectoryScope,
    namespace_ids: frozenset[uuid.UUID],
    config_version: int,
    exp: int,
) -> str:
    payload = json.dumps(
        {
            "tenant_id": str(scope.tenant_id),
            "issuer": scope.issuer,
            "managed_by": scope.managed_by,
            "team_ids": sorted(map(str, scope.team_ids)),
            "namespace_ids": sorted(map(str, namespace_ids)),
            "config_version": config_version,
            "scope": "directory.write",
            "exp": exp,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    body = "dir." + b64url(payload.encode())
    return body + "." + signature(api_key, body)


def verify(token: str, api_key: str, *, now: int | None = None) -> DirectoryClaims | None:
    try:
        if not api_key or not isinstance(token, str) or len(token) > 32768:
            return None
        prefix, segment, sig = token.split(".")
        if prefix != "dir" or not hmac.compare_digest(sig, signature(api_key, f"dir.{segment}")):
            return None
        data = json.loads(b64url_decode(segment))
        if set(data) != {
            "tenant_id",
            "issuer",
            "managed_by",
            "team_ids",
            "namespace_ids",
            "config_version",
            "scope",
            "exp",
        }:
            return None
        exp, version = data["exp"], data["config_version"]
        if (
            data["scope"] != "directory.write"
            or type(exp) is not int
            or exp <= (int(time.time()) if now is None else now)
            or type(version) is not int
            or version < 1
        ):
            return None
        for field in ("team_ids", "namespace_ids"):
            if not isinstance(data[field], list) or len(data[field]) > 1000:
                return None
            if any(not isinstance(value, str) for value in data[field]):
                return None
            if len(set(data[field])) != len(data[field]):
                return None
        scope = DirectoryScope(
            uuid.UUID(data["tenant_id"]),
            data["issuer"],
            data["managed_by"],
            frozenset(uuid.UUID(value) for value in data["team_ids"]),
        )
        return DirectoryClaims(
            scope, frozenset(uuid.UUID(value) for value in data["namespace_ids"]), version, exp
        )
    except (ValueError, TypeError, KeyError, AttributeError, DirectorySyncError):
        return None
