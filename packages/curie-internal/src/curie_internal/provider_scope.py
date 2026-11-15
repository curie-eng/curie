"""The provider scope the runner and the worker both derive from a model env (#5426).

A scope names one provider, one host and one credential without holding the
credential: ``scope_key`` is a digest the runner puts on a ``RetryCondition``
and the worker registers in the provider gate ledger. Both sides call
``provider_scope`` on the env they see, so the derivation exists once. It
agrees with the runner's ``sdk_auth`` resolution and fails closed (returns
None) wherever the two sides could disagree or the provider is unknown.

``parse_openrouter_key`` reads the response of OpenRouter's non-generating key
read into an ``AllowanceReading``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Literal
from urllib.parse import urlsplit

PROVIDER_OPENROUTER: Final[str] = "openrouter"
PROVIDER_ANTHROPIC: Final[str] = "anthropic"
SUPPORTED_PROVIDERS: Final[frozenset[str]] = frozenset({PROVIDER_OPENROUTER, PROVIDER_ANTHROPIC})
# Providers with a read that reports allowance without generating anything.
PROBED_PROVIDERS: Final[frozenset[str]] = frozenset({PROVIDER_OPENROUTER})

OPENROUTER_HOST: Final[str] = "openrouter.ai"
ANTHROPIC_HOST: Final[str] = "api.anthropic.com"

# Boot env names. The BootEnv-declared ones are pinned against aci_protocol in
# the tests; this module stays standard library only.
CURIE_CREDENTIALS: Final[str] = "CURIE_CREDENTIALS"
CURIE_MODEL_ENV_KEY: Final[str] = "CURIE_MODEL_ENV_KEY"
ANTHROPIC_BASE_URL: Final[str] = "ANTHROPIC_BASE_URL"
CURIE_MODEL_BASE_URL: Final[str] = "CURIE_MODEL_BASE_URL"
CURIE_MODEL_AUTH_MODE: Final[str] = "CURIE_MODEL_AUTH_MODE"
ANTHROPIC_API_KEY: Final[str] = "ANTHROPIC_API_KEY"
CLAUDE_CODE_OAUTH_TOKEN: Final[str] = "CLAUDE_CODE_OAUTH_TOKEN"

_CURIE_PREFIX: Final[str] = "CURIE_"
_SCOPE_VERSION: Final[int] = 1


@dataclass(frozen=True)
class ProviderScope:
    """One provider, host and credential, named only by digest."""

    provider: str
    host: str
    scope_key: str


@dataclass(frozen=True)
class AllowanceReading:
    """What one non-generating allowance read showed."""

    outcome: Literal["authenticated", "rejected", "unreadable"]
    limit_remaining: Decimal | None
    # The key's credit limit. None when the key has no limit (a JSON null) or
    # the value is not a finite number: an unlimited key shows no allowance that
    # a probe could verify.
    limit: Decimal | None


def scope_key(provider: str, host: str, credential: str) -> str:
    """The ``sha256:<hex>`` digest of a versioned, credential-free scope document."""

    document = {
        "credential_sha256": hashlib.sha256(credential.encode("utf-8")).hexdigest(),
        "host": host,
        "provider": provider,
        "v": _SCOPE_VERSION,
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _declared_names(raw: str) -> tuple[str, ...] | None:
    """The credential env names ``CURIE_MODEL_ENV_KEY`` declares, None when refused."""

    if not raw.strip():
        return (CURIE_CREDENTIALS,)
    try:
        decoded: object = json.loads(raw)
    except ValueError:
        decoded = raw
    if isinstance(decoded, str):
        names: list[str] = [decoded]
    elif isinstance(decoded, list) and all(isinstance(item, str) for item in decoded):
        names = [item for item in decoded if isinstance(item, str)]
    else:
        return None
    keys = tuple(name.strip() for name in names if name.strip())
    if not keys:
        return None
    if any(key.startswith(_CURIE_PREFIX) and key != CURIE_CREDENTIALS for key in keys):
        return None
    return keys


def provider_scope(env: Mapping[str, str]) -> ProviderScope | None:
    """Derive the scope of the credential ``env`` authenticates with, or None.

    None means no scope can be named safely: personal mode, an explicit SDK
    credential, a refused or empty credential declaration, an unknown base URL
    host, or a bare ``sk-`` key.
    """

    if env.get(CURIE_MODEL_AUTH_MODE, "").strip().lower() == "personal":
        return None
    if env.get(ANTHROPIC_API_KEY) or env.get(CLAUDE_CODE_OAUTH_TOKEN):
        return None
    names = _declared_names(env.get(CURIE_MODEL_ENV_KEY, ""))
    if names is None:
        return None
    credential = next((env[name] for name in names if env.get(name)), "")
    if not credential:
        return None
    if credential.startswith("sk-or-"):
        provider, host = PROVIDER_OPENROUTER, OPENROUTER_HOST
    elif base_url := env.get(ANTHROPIC_BASE_URL) or env.get(CURIE_MODEL_BASE_URL):
        try:
            hostname = urlsplit(base_url.strip()).hostname
        except ValueError:
            return None
        if (hostname or "").lower() != OPENROUTER_HOST:
            return None
        provider, host = PROVIDER_OPENROUTER, OPENROUTER_HOST
    elif credential.startswith("sk-ant-") or not credential.startswith("sk-"):
        provider, host = PROVIDER_ANTHROPIC, ANTHROPIC_HOST
    else:
        return None
    return ProviderScope(
        provider=provider, host=host, scope_key=scope_key(provider, host, credential)
    )


def parse_openrouter_key(status: int | None, body: object) -> AllowanceReading:
    """Read the response of OpenRouter's GET /api/v1/key.

    Contract:
    https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key
    A 200 carries a ``data`` object with ``limit`` and ``limit_remaining``; a
    null ``limit`` is an unlimited key. ``data.limit_reset`` is a reset period
    string (for example "monthly"), never an absolute time, so it is not read.
    401 means the key was rejected; anything else, including a malformed 200 and
    a failed transport (``status`` None), is unreadable. The body is never kept.
    """

    if status == 401:
        return AllowanceReading(outcome="rejected", limit_remaining=None, limit=None)
    if status != 200 or not isinstance(body, dict):
        return AllowanceReading(outcome="unreadable", limit_remaining=None, limit=None)
    data = body.get("data")
    if not isinstance(data, dict):
        return AllowanceReading(outcome="unreadable", limit_remaining=None, limit=None)
    return AllowanceReading(
        outcome="authenticated",
        limit_remaining=_finite_number(data.get("limit_remaining")),
        limit=_finite_number(data.get("limit")),
    )


def _finite_number(value: object) -> Decimal | None:
    """A JSON int or float (never a bool) that is finite, as a Decimal, else None."""

    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
        return Decimal(str(value))
    return None
