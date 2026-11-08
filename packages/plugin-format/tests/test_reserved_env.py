"""Unit pin for the shared reserved boot-env name policy (#457).

`RESERVED_BOOT_ENV` + `is_reserved_boot_env_name` are the single source of truth
that every write seam (API validator, bundle validator, worker injection loop,
Helm guard) consults so a connector secret cannot shadow a runner-owned model
credential. #445 fenced only the `CURIE_` prefix; the four non-prefixed
credential keys the runner's `sdk_auth` owns slipped through, which is exactly
the redirect this predicate closes.
"""

from __future__ import annotations

import pytest
from plugin_format import RESERVED_BOOT_ENV, is_reserved_boot_env_name

# The four credential keys the runner's sdk_auth owns that are NOT CURIE_-
# prefixed -- the gap #457 exists to close.
_CREDENTIAL_KEYS = [
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_AUTH_TOKEN",
]


@pytest.mark.parametrize("name", _CREDENTIAL_KEYS)
def test_credential_keys_are_reserved(name: str) -> None:
    assert is_reserved_boot_env_name(name) is True


@pytest.mark.parametrize("name", _CREDENTIAL_KEYS)
def test_credential_keys_are_members_of_the_set(name: str) -> None:
    assert name in RESERVED_BOOT_ENV


@pytest.mark.parametrize(
    "name",
    [
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "NODE_EXTRA_CA_CERTS",
        "ANTHROPIC_CUSTOM_HEADERS",
        "ALL_PROXY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "all_proxy",
        "ssl_cert_file",
        "ssl_cert_dir",
        "NODE_TLS_REJECT_UNAUTHORIZED",
        "NODE_OPTIONS",
    ],
)
def test_redirect_capture_keys_are_reserved(name: str) -> None:
    # #487: generic env that redirects/captures the model session (proxy, extra
    # CA, custom headers) is reserved even though the worker/runner don't "own" it
    # and it carries no CURIE_ prefix -- it reaches the same capture end state.
    assert is_reserved_boot_env_name(name) is True
    assert name in RESERVED_BOOT_ENV


def test_model_base_url_alias_is_reserved() -> None:
    # sdk_auth's CURIE_-prefixed alias of the base-URL seam.
    assert is_reserved_boot_env_name("CURIE_MODEL_BASE_URL") is True


def test_curie_prefix_is_the_catch_all() -> None:
    # An arbitrary CURIE_* name nobody enumerated is still fenced by the
    # prefix rule (the forward-safe half of the predicate).
    assert is_reserved_boot_env_name("CURIE_FOO") is True


def test_legitimate_connector_name_is_not_reserved() -> None:
    # Negative control: a real connector token name must still be allowed.
    assert is_reserved_boot_env_name("GITHUB_PERSONAL_ACCESS_TOKEN") is False
