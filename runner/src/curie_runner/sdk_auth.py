"""Map the ACI ``CURIE_CREDENTIALS`` reference onto the SDK's credential env.

The claude-agent-sdk authenticates from ``CLAUDE_CODE_OAUTH_TOKEN`` /
``ANTHROPIC_API_KEY`` in its own environment; it never reads the ACI
``CURIE_CREDENTIALS`` reference. When the platform supplies only
``CURIE_CREDENTIALS`` (the worker forwards it), the runner would otherwise drop
it and the real session would fail with a generic ``authentication_failed``. This
resolves it to the correct SDK variable before the session starts.

Rules (order matters -- an OAuth token and an API key share the ``sk-ant-``
prefix, so the more specific OAuth prefix is checked first):
- An explicit SDK credential already in the environment wins (the platform set it
  deliberately); this is then a no-op.
- ``sk-ant-oat...`` is a Claude Code OAuth token -> ``CLAUDE_CODE_OAUTH_TOKEN``.
- ``sk-ant-...`` (any other) is an Anthropic API key -> ``ANTHROPIC_API_KEY``.
- ``sk-or-...`` (OpenRouter) is routed through the base-URL-override seam:
  base URL -> OpenRouter's Anthropic endpoint, key -> ``ANTHROPIC_API_KEY``
  (the ``x-api-key`` header OpenRouter reads), and ``ANTHROPIC_AUTH_TOKEN``
  left blank. A bare ``sk-...`` OpenAI-style key is still rejected loudly on the
  direct-Anthropic path (no base URL set).
- Anything else is treated as a Claude Code OAuth token ->
  ``CLAUDE_CODE_OAUTH_TOKEN``.

Provider-native Anthropic-compatible endpoints (Zhipu/GLM, Moonshot/Kimi,
DeepSeek, ...) share OpenAI's ``sk-`` key shape or use a non-``sk-`` key, so a
key prefix cannot select them unambiguously. They are instead selected by base
URL: the config author points ``CURIE_MODEL_BASE_URL`` (or the raw SDK var
``ANTHROPIC_BASE_URL``) at the provider's ``/anthropic`` endpoint and supplies
the provider key in ``CURIE_CREDENTIALS``. In that base-URL-override mode the
provider key is forwarded into ``ANTHROPIC_API_KEY`` (the ``x-api-key`` header
these endpoints read). Canonical base URLs live in ``PROVIDER_BASE_URLS``. A
Claude Code OAuth token (``sk-ant-oat``) is never forwarded to a third-party
endpoint -- override mode keeps it blanked (hermetic).

The endpoint's wire protocol is **declared, not inferred** (#514).
``CURIE_MODEL_API_BACKEND`` names it as an ``ApiBackend`` member and defaults to
``messages``, which is the only wire format the runner can dial: the session runs
on claude-agent-sdk, which speaks the Anthropic Messages format. The
OpenAI-shaped backends (``chat_completions``, ``responses``) are therefore
declarable but rejected up front, in ``resolve_sdk_env``, before any credential or
base-URL branching -- a deterministic gate, so an OpenAI-shaped endpoint fails
loudly with an actionable message instead of being silently mis-dialed. Reaching
one needs a translating proxy in front of the runner.

Which env var carries the credential is likewise declarable (#514).
``CURIE_MODEL_ENV_KEY`` holds either a bare env-var name or a JSON array of
them; the keys are walked in order and the first that is both set and non-empty
wins, so a config author is not forced onto the single hardcoded
``CURIE_CREDENTIALS`` name. A key that is present but EMPTY is skipped rather
than winning -- an empty value is "not supplied", never a credential. Unset or
empty, the list defaults to ``(CURIE_CREDENTIALS,)``, which is exactly today's
behavior. What it may TARGET is fenced: the boot env also holds the platform's
scoped state tokens (ADR-0033) and the agent's connector secrets, so an
``CURIE_``-prefixed target (other than ``CURIE_CREDENTIALS`` itself) is
refused, keeping the declaration from becoming a way to forward the platform's
own secrets to a third-party endpoint.

The credential value (and its length) is never logged or echoed.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping
from enum import StrEnum
from urllib.parse import urlsplit

from aci_protocol import BootEnv

# A declared boot key: the worker renders the agent's credential reference under
# this name, so it is read from the one declaration rather than retyped (#488).
CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")
OAUTH_TOKEN_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
API_KEY_ENV = "ANTHROPIC_API_KEY"
# A declared boot key: the worker renders the agent's model base URL under this
# name, so it is read from the one declaration rather than retyped here (#488).
BASE_URL_ENV = BootEnv.env_key("base_url")
AUTH_TOKEN_ENV = "ANTHROPIC_AUTH_TOKEN"
# CURIE_-namespaced alias for the base-URL override seam, mapped onto the raw
# SDK var (BASE_URL_ENV). It lets a config author stay in the CURIE_* config
# namespace (like CURIE_MODEL / CURIE_CREDENTIALS) instead of setting the
# SDK's own ANTHROPIC_BASE_URL directly. The raw var wins when both are set.
MODEL_BASE_URL_ENV = "CURIE_MODEL_BASE_URL"
# Declares the endpoint's wire protocol (see module docstring). A declared boot
# key: the worker renders it, so it is read from the one declaration rather than
# retyped (#488). CURIE_-namespaced like its MODEL_BASE_URL sibling, so it is
# also fenced by the reserved-boot-env prefix rule in plugin_format.reserved_env.
API_BACKEND_ENV = BootEnv.env_key("api_backend")
# Declares which env var(s) carry the credential: a bare name or a JSON array.
# Also a declared boot key, read from the declaration for the same reason.
MODEL_ENV_KEY_ENV = BootEnv.env_key("model_env_key")
# The platform's own boot-env namespace. No var in it is a model credential
# (CURIE_CREDENTIALS excepted), so it is off limits as an env_key target --
# see parse_env_keys.
_CURIE_ENV_PREFIX = "CURIE_"
OPENROUTER_BASE_URL = "https://openrouter.ai/api"

# A Claude Code OAuth token shares the sk-ant- prefix with an API key; this more
# specific prefix distinguishes it and marks it as non-forwardable to any
# third-party endpoint.
OAUTH_TOKEN_PREFIX = "sk-ant-oat"

# Canonical provider-native Anthropic-compatible base URLs (the SDK appends
# /v1/messages). These keep the Anthropic wire format -- and therefore provider
# automatic prefix caching -- rather than the OpenAI chat-completions shape.
# Selected by base URL (see module docstring), not by key prefix. OpenRouter is
# the one prefix-routed provider (sk-or-) and is included here for reference.
ZHIPU_BASE_URL = "https://api.z.ai/api/anthropic"
MOONSHOT_BASE_URL = "https://api.moonshot.ai/anthropic"
DEEPSEEK_BASE_URL = "https://api.deepseek.com/anthropic"
PROVIDER_BASE_URLS: dict[str, str] = {
    "zhipu": ZHIPU_BASE_URL,
    "moonshot": MOONSHOT_BASE_URL,
    "deepseek": DEEPSEEK_BASE_URL,
    "openrouter": OPENROUTER_BASE_URL,
}

_SDK_CREDENTIAL_ENV = (OAUTH_TOKEN_ENV, API_KEY_ENV)

# Non-empty no-op placeholder API key used in base-URL override mode. It is
# deliberately not sk-... shaped so it can never be mistaken for a real
# credential. The bundled Claude CLI treats an EMPTY ANTHROPIC_API_KEY as "not
# logged in" and refuses to call the endpoint, so the placeholder must be
# non-empty to pass the CLI's auth gate before it dials the overridden base URL.
NO_OP_API_KEY = "not-needed"


class UnsupportedCredentialError(RuntimeError):
    """``CURIE_CREDENTIALS`` is a recognizably non-Anthropic credential."""


class UnsupportedApiBackendError(RuntimeError):
    """``CURIE_MODEL_API_BACKEND`` names an unknown or undialable wire format."""


class InvalidEnvKeyError(ValueError):
    """``CURIE_MODEL_ENV_KEY`` is not a name or a JSON array of names."""


class ApiBackend(StrEnum):
    """The wire protocol an endpoint speaks, as declared by config.

    The member values are the wire contract: they are what a config author writes
    into ``CURIE_MODEL_API_BACKEND``.
    """

    MESSAGES = "messages"
    CHAT_COMPLETIONS = "chat_completions"
    RESPONSES = "responses"

    @property
    def speaks_anthropic_wire(self) -> bool:
        """Whether claude-agent-sdk can dial this backend directly.

        The runner's session is a claude-agent-sdk session, which speaks only the
        Anthropic Messages wire format. The OpenAI-shaped backends have a
        different request/response body, so they are not dialable without a
        translating proxy in front of the runner.
        """
        return self is ApiBackend.MESSAGES


DEFAULT_API_BACKEND = ApiBackend.MESSAGES

# The credential env var(s) consulted when CURIE_MODEL_ENV_KEY is not declared.
# This is today's behavior expressed as the default of the new list.
DEFAULT_CREDENTIAL_ENV_KEYS: tuple[str, ...] = (CREDENTIALS_ENV,)


def resolve_api_backend(env: MutableMapping[str, str]) -> ApiBackend:
    """Resolve the declared wire protocol, defaulting to Messages.

    Unset or empty means "not declared", not "declared as empty", so both fall
    back to ``DEFAULT_API_BACKEND`` and preserve the pre-#514 assumption that
    every endpoint speaks Messages. The value is hand-written config, so casing
    and stray whitespace are tolerated; anything else unrecognized raises rather
    than silently defaulting, since a near-miss spelling would otherwise mis-dial.
    """
    raw = env.get(API_BACKEND_ENV, "").strip()
    if not raw:
        return DEFAULT_API_BACKEND
    try:
        return ApiBackend(raw.lower())
    except ValueError:
        supported = ", ".join(member.value for member in ApiBackend)
        raise UnsupportedApiBackendError(
            f"{API_BACKEND_ENV}={raw!r} is not a supported model API backend. "
            f"Supported values: {supported}."
        ) from None


def parse_env_keys(raw: str) -> tuple[str, ...]:
    """Parse ``CURIE_MODEL_ENV_KEY`` into an ordered tuple of env var names.

    Accepts a JSON array of names or a single bare name. A bare name like
    ``MY_CRED`` is not valid JSON, so a decode failure falls back to the
    bare-string form rather than raising. Blank entries are dropped: hand-written
    config picks up stray empties and they carry no meaning. Valid JSON of the
    wrong shape (a non-string array member, a mapping, a bare number) and a list
    that reduces to nothing are config errors, raised loudly.

    A ``CURIE_``-prefixed target is refused (``CURIE_CREDENTIALS``, the
    canonical credential name, excepted). The runner boot env is shared: it also
    carries the platform's own scoped state tokens (ADR-0033) and the agent's
    connector secrets. Naming one of those as a credential source under a
    base-URL override would forward it to a third-party endpoint as the
    ``x-api-key`` header, so the fence exists to keep the declaration from being a
    secret-exfiltration primitive. The platform's own boot vars are never a model
    credential, so naming one is either a mistake or an attack, and the runner
    refuses either way. One bad name poisons the whole declaration rather than
    being dropped silently: a silently-dropped name would let a typo'd (or
    deliberate) exfil attempt look like it worked. The error may name the
    offending env var, never its value.
    """
    try:
        decoded: object = json.loads(raw)
    except ValueError:
        decoded = raw  # not JSON at all: the bare-name form
    if isinstance(decoded, str):
        names = [decoded]
    elif isinstance(decoded, list):
        if not all(isinstance(item, str) for item in decoded):
            raise InvalidEnvKeyError(
                f"{MODEL_ENV_KEY_ENV} must be an env var name or a JSON array of names."
            )
        names = [item for item in decoded if isinstance(item, str)]
    else:
        raise InvalidEnvKeyError(
            f"{MODEL_ENV_KEY_ENV} must be an env var name or a JSON array of names."
        )
    keys = tuple(name.strip() for name in names if name.strip())
    if not keys:
        raise InvalidEnvKeyError(f"{MODEL_ENV_KEY_ENV} declares no usable env var name.")
    for key in keys:
        if key.startswith(_CURIE_ENV_PREFIX) and key != CREDENTIALS_ENV:
            raise InvalidEnvKeyError(
                f"{MODEL_ENV_KEY_ENV} may not name {key}: the platform's own "
                f"{_CURIE_ENV_PREFIX}* boot vars are never a model credential. "
                f"Name a provider-native env var, or {CREDENTIALS_ENV}."
            )
    return keys


def resolve_credential_env_keys(env: MutableMapping[str, str]) -> tuple[str, ...]:
    """Resolve the ordered credential env var names to consult.

    Unset or empty falls back to ``DEFAULT_CREDENTIAL_ENV_KEYS``, keeping the
    undeclared path byte-identical to today's single ``CURIE_CREDENTIALS`` read.
    """
    raw = env.get(MODEL_ENV_KEY_ENV, "").strip()
    if not raw:
        return DEFAULT_CREDENTIAL_ENV_KEYS
    return parse_env_keys(raw)


def resolve_credential(env: MutableMapping[str, str]) -> str:
    """Read the credential from the first declared key that actually carries one.

    Walks the keys in declared order and returns the first value that is both set
    AND non-empty, or ``""`` when none is. A key that is present but empty is
    SKIPPED rather than winning: an empty credential is "not supplied", and
    letting it beat a real one further down the list is a silent auth failure. The
    value is returned, never logged or echoed.
    """
    for key in resolve_credential_env_keys(env):
        value = env.get(key)
        if value:
            return value
    return ""


def resolve_base_url_override(env: MutableMapping[str, str]) -> dict[str, str] | None:
    """Build the generic SDK base URL override env when configured.

    This is the provider agnostic base URL override path: local Ollama and
    OpenRouter/#24 today. It targets any Anthropic-compatible endpoint without
    a real Anthropic credential.

    The placeholder API key is NON-EMPTY on purpose (empirically verified
    2026-07-07): the bundled Claude CLI treats an empty ANTHROPIC_API_KEY as "not
    logged in" and refuses to call the endpoint before it ever reaches the base
    URL, so it must carry a non-credential placeholder to pass the auth gate.
    Paired with the overridden base URL, that placeholder cannot authenticate
    against real Anthropic.

    Both CLAUDE_CODE_OAUTH_TOKEN and ANTHROPIC_AUTH_TOKEN are blanked to "" so an
    inherited OAuth token or Bearer token cannot take precedence over the
    placeholder + base URL, nor leak to the overridden (local/third-party)
    endpoint, keeping override mode hermetic. The base URL is not a secret, but
    this module keeps the same no echo discipline it uses for credentials.
    """
    base_url = env.get(BASE_URL_ENV) or env.get(MODEL_BASE_URL_ENV)
    if not base_url:
        return None
    return {
        BASE_URL_ENV: base_url,
        API_KEY_ENV: NO_OP_API_KEY,
        OAUTH_TOKEN_ENV: "",
        AUTH_TOKEN_ENV: "",
    }


def _is_forwardable_provider_credential(credential: str) -> bool:
    """Whether a credential may be forwarded to a base-URL-overridden endpoint.

    In base-URL-override mode the operator has deliberately pointed the runner at
    a provider-native Anthropic-compatible endpoint (Zhipu/Moonshot/DeepSeek,
    etc.) and supplied that provider's key; it is sent as the ``x-api-key``
    header. A Claude Code OAuth token (``sk-ant-oat``) is the one thing never
    forwarded to a third-party endpoint, so it stays blanked (hermetic).
    """
    if not credential:
        return False
    return not credential.startswith(OAUTH_TOKEN_PREFIX)


def resolve_model_credential(env: MutableMapping[str, str]) -> None:
    """Populate the SDK credential env from the declared credential key when needed.

    The credential is read via ``resolve_credential`` (``CURIE_CREDENTIALS`` by
    default, or the keys ``CURIE_MODEL_ENV_KEY`` declares). Mutates ``env`` in
    place. No-op when an SDK credential is already present or no declared key
    carries a value. Raises ``UnsupportedCredentialError`` for a recognizably
    non-Anthropic key.
    """
    if any(env.get(var) for var in _SDK_CREDENTIAL_ENV):
        return  # an explicit SDK credential wins
    credential = resolve_credential(env)
    if not credential:
        return
    if credential.startswith("sk-ant-oat"):
        # Claude Code OAuth tokens share the sk-ant- prefix with API keys, so
        # this more specific check must come first.
        env[OAUTH_TOKEN_ENV] = credential
    elif credential.startswith("sk-ant-"):
        env[API_KEY_ENV] = credential
    elif credential.startswith("sk-or-"):
        # OpenRouter: reuse the shared base-URL-override seam to target OpenRouter's
        # native Anthropic Messages endpoint (the SDK appends /v1/messages). The real
        # key goes in ANTHROPIC_API_KEY (sent as the x-api-key header, which is what
        # OpenRouter's Anthropic endpoint reads), overriding the NO_OP_API_KEY
        # placeholder the override just set; ANTHROPIC_AUTH_TOKEN stays blank. Staying
        # on the Anthropic wire format keeps prompt caching intact (the OpenAI
        # chat-completions path silently breaks it at ~10x cost).
        env[BASE_URL_ENV] = OPENROUTER_BASE_URL
        override = resolve_base_url_override(env)
        assert override is not None  # base URL was just set
        env.update(override)
        env[API_KEY_ENV] = credential  # real key -> x-api-key (what OpenRouter reads)
    elif credential.startswith("sk-"):
        # OpenAI-style ("sk-...") and similar. Fail loudly rather than
        # forwarding a key the Anthropic SDK cannot use.
        raise UnsupportedCredentialError(
            "CURIE_CREDENTIALS is not a supported model credential. Provide an "
            "Anthropic API key (sk-ant-...) or a Claude Code OAuth token."
        )
    else:
        env[OAUTH_TOKEN_ENV] = credential


def model_endpoint_host(env: Mapping[str, str], override: Mapping[str, str] | None = None) -> str:
    """The host of the model endpoint this boot resolved, as boot evidence.

    Host only (``openrouter.ai``), or ``default`` when no base URL is set and
    the SDK dials its own Anthropic endpoint. Never the credential, the path or
    the query. ``override`` is ``resolve_sdk_env``'s answer, which wins over
    ``env`` the way it does for the spawned SDK.
    """

    raw = (
        (override or {}).get(BASE_URL_ENV)
        or env.get(BASE_URL_ENV)
        or env.get(MODEL_BASE_URL_ENV)
        or ""
    ).strip()
    if not raw:
        return "default"
    try:
        host = urlsplit(raw).hostname
    except ValueError:
        host = None
    return host or "unparseable"


def resolve_sdk_env(env: MutableMapping[str, str]) -> dict[str, str] | None:
    """Decide the SDK auth env from the boot env.

    The declared wire protocol is checked FIRST, before any credential or base-URL
    branching, so rejecting an undialable backend is a deterministic gate on the
    declaration itself rather than a side effect of whichever path config happens
    to select.

    An ``sk-or-`` OpenRouter credential is routed by ``resolve_model_credential``
    (which sets the OpenRouter base URL and the real key in ``ANTHROPIC_API_KEY``,
    the ``x-api-key`` header OpenRouter reads) even when ``ANTHROPIC_BASE_URL`` is
    already set, so its key is never dropped. Otherwise a generic base-URL override
    wins when configured, and a plain Anthropic credential falls through to
    ``resolve_model_credential``.
    Returns the override dict to pass as ``ClaudeAgentOptions.env`` (base-URL
    override mode), or ``None`` when resolution mutated ``env`` in place.
    """
    backend = resolve_api_backend(env)
    if not backend.speaks_anthropic_wire:
        raise UnsupportedApiBackendError(
            f"{API_BACKEND_ENV}={backend.value!r} is not dialable by this runner. The "
            "runner speaks the Anthropic Messages wire format via claude-agent-sdk; "
            f"{ApiBackend.CHAT_COMPLETIONS.value!r} and {ApiBackend.RESPONSES.value!r} "
            "are OpenAI-shaped and need a translating proxy in front of the runner. "
            f"Set {API_BACKEND_ENV}={ApiBackend.MESSAGES.value!r} (the default) and "
            "point the base URL at an Anthropic Messages endpoint."
        )
    credential = resolve_credential(env)
    if not credential.startswith("sk-or-"):
        override = resolve_base_url_override(env)
        if override is not None:
            # Provider-native Anthropic-compatible endpoints (Zhipu/GLM,
            # Moonshot/Kimi, DeepSeek, ...) are selected by base URL and
            # authenticate with their own key sent as x-api-key. Forward a
            # supplied provider credential into ANTHROPIC_API_KEY, overriding the
            # NO_OP placeholder. A missing credential (e.g. local Ollama) keeps
            # the placeholder; an Anthropic OAuth token stays blanked (hermetic).
            if _is_forwardable_provider_credential(credential):
                override[API_KEY_ENV] = credential
            return override
    resolve_model_credential(env)
    return None
