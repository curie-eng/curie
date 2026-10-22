"""Pairing and construction of both Bitbucket code hosts (#4401, AC 2; decisions 2 to 5).

Each adapter is built the way production builds it: a `ForgesConfig` through
`build_registry`, whose code host factory refuses a bad entry at build time.
Every refusal is beside the valid neighbor that must still build.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    MARKED_COMMENT_OPERATIONS,
    Operation,
    Support,
    validate_pairing,
)
from curie_api.forges.config import ForgesConfig
from curie_api.forges.errors import InvalidPairing
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.ports import CodeHost
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import (
    CredentialExpiry,
    CredentialHeader,
    CredentialScope,
)
from forge_fakes.bitbucket_cloud import FACTORY_TOKEN, BitbucketCloudFake
from forge_fakes.bitbucket_lifecycle import memory_jira_tracker

TOKEN = "fixture-bitbucket-config-token"
EXPIRES = datetime(2027, 1, 31, tzinfo=UTC)
DC_API_URL = "https://bitbucket-dc.example.invalid/bitbucket"

# Decision 3: every mandatory operation supported; diagnostics, rerun and the
# write check unsupported on both kinds.
EXPECTED_CAPABILITIES = {
    **dict.fromkeys(CODE_HOST_OPERATIONS, Support.SUPPORTED),
    Operation.CI_DIAGNOSTICS: Support.UNSUPPORTED,
    Operation.RERUN_FAILED: Support.UNSUPPORTED,
    Operation.USER_CAN_WRITE: Support.UNSUPPORTED,
    Operation.REQUIRED_CHECKS: Support.UNSUPPORTED,
}

KINDS = [
    pytest.param(types.BITBUCKET_CLOUD, id="bitbucket_cloud"),
    pytest.param(types.BITBUCKET_DC, id="bitbucket_dc"),
]


def _entry(kind: str, **credential: Any) -> dict[str, Any]:
    if kind == types.BITBUCKET_CLOUD:
        host, api_url = "bitbucket.org", "https://api.bitbucket.org/2.0"
    else:
        host, api_url = "bitbucket-dc.example.invalid", DC_API_URL
    return {
        "name": kind.replace("_", "-"),
        "kind": kind,
        "host": host,
        "api_url": api_url,
        "credential": {"type": "static_token", "token": TOKEN, **credential},
    }


def _registry(*entries: dict[str, Any]) -> ForgeRegistry:
    return build_registry(ForgesConfig.model_validate({"code_hosts": list(entries)}))


def _adapter(entry: dict[str, Any]) -> CodeHost:
    return _registry(entry).code_host(entry["name"]).adapter(httpx.AsyncClient())


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.parametrize("kind", KINDS)
def test_bitbucket_config_jira_pairs_and_github_refuses(kind: str) -> None:
    adapter = _adapter(_entry(kind))
    assert (adapter.kind, adapter.marked_comments.kind) == (kind, kind)

    validate_pairing(memory_jira_tracker(), adapter)

    github = InMemoryTracker(kind=types.GITHUB)
    with pytest.raises(InvalidPairing) as refused:
        validate_pairing(github, adapter)
    assert refused.value.reason == "a github tracker pairs only with a github code host"


@pytest.mark.parametrize("kind", KINDS)
def test_bitbucket_config_capabilities_are_exact(kind: str) -> None:
    adapter = _adapter(_entry(kind))

    assert dict(adapter.capabilities) == EXPECTED_CAPABILITIES
    assert dict(adapter.marked_comments.capabilities) == dict.fromkeys(
        MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
    )


@pytest.mark.parametrize("kind", KINDS)
def test_bitbucket_config_username_is_refused(kind: str) -> None:
    # The neighbor without a username builds.
    _registry(_entry(kind))

    with pytest.raises(ValueError) as refused:
        _registry(_entry(kind, username="x-token-auth"))
    assert f"{kind} uses an access token; remove username" in str(refused.value)
    assert TOKEN not in str(refused.value)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        pytest.param(
            "host",
            "bitbucket.example",
            "bitbucket_cloud host must be bitbucket.org, not bitbucket.example",
            id="other-host",
        ),
        pytest.param(
            "api_url",
            "https://api.bitbucket.example/2.0",
            "bitbucket_cloud api_url must be https://api.bitbucket.org/2.0",
            id="other-api",
        ),
        pytest.param(
            "api_url",
            "http://localhost:8080",
            "bitbucket_cloud api_url must be https://api.bitbucket.org/2.0",
            id="loopback-by-name",
        ),
    ],
)
def test_bitbucket_config_cloud_host_must_be_bitbucket_org(
    field: str, value: str, message: str
) -> None:
    with pytest.raises(ValueError) as refused:
        _registry({**_entry(types.BITBUCKET_CLOUD), field: value})
    assert message in str(refused.value)


@pytest.mark.parametrize(
    "api_url",
    [
        pytest.param("https://api.bitbucket.org/2.0", id="bitbucket-org"),
        pytest.param("http://127.0.0.1:8080", id="test-url"),
    ],
)
def test_bitbucket_config_cloud_accepts_its_api_and_a_local_test_url(api_url: str) -> None:
    entry = {**_entry(types.BITBUCKET_CLOUD), "api_url": api_url}
    assert _adapter(entry).host == "bitbucket.org"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("credential", "expiry", "expires_at"),
    [
        pytest.param(
            {"expires_at": EXPIRES.isoformat()}, CredentialExpiry.KNOWN, EXPIRES, id="known"
        ),
        pytest.param({}, CredentialExpiry.UNKNOWN, None, id="unknown"),
        pytest.param({"never_expires": True}, CredentialExpiry.NONE, None, id="none"),
    ],
)
@pytest.mark.parametrize("kind", KINDS)
async def test_bitbucket_config_credentials(
    kind: str,
    credential: dict[str, Any],
    expiry: CredentialExpiry,
    expires_at: datetime | None,
) -> None:
    entry = _entry(kind, **credential)
    connection = _registry(entry).code_host(entry["name"])

    for scope in CredentialScope:
        minted = await connection.credentials.get(scope)
        assert minted.scope is scope and minted.secret == TOKEN
        assert (minted.expiry, minted.expires_at) == (expiry, expires_at)
        if kind == types.BITBUCKET_CLOUD:
            # Decision 4.1: git takes basic auth as x-token-auth on bitbucket.org.
            assert (minted.header, minted.username, minted.origin) == (
                CredentialHeader.AUTHORIZATION_BASIC,
                "x-token-auth",
                "https://bitbucket.org",
            )
        else:
            # Decision 4.2 and 5.2: Bearer for git too, under the server's
            # /scm, so <origin>/<KEY>/<slug>.git is its clone URL.
            assert (minted.header, minted.username, minted.origin) == (
                CredentialHeader.AUTHORIZATION_BEARER,
                None,
                f"{DC_API_URL}/scm",
            )
        assert TOKEN not in repr(minted)
    assert connection.credentials.expiry().expiry is expiry
    assert TOKEN not in repr(connection)
    assert TOKEN not in repr(connection.adapter(httpx.AsyncClient()))


@pytest.mark.anyio
async def test_bitbucket_config_cloud_credential_through_the_port() -> None:
    """The port's credential resolves the repository first, then hands git
    the basic credential for bitbucket.org (decisions 4.1 and 6.1)."""

    fake = BitbucketCloudFake()
    entry = {
        **_entry(types.BITBUCKET_CLOUD),
        "credential": {"type": "static_token", "token": FACTORY_TOKEN},
    }
    adapter = _registry(entry).code_host(entry["name"]).adapter(fake.client())

    repository = await adapter.resolve_repository(fake.repo_uuid)
    assert repository == fake.repository()
    assert (repository.path, repository.default_branch) == (fake.full_name, "main")
    credential = await adapter.credential(repository, CredentialScope.PUSH)

    assert credential.git_header().startswith("Authorization: Basic ")
    assert (credential.username, credential.origin) == ("x-token-auth", "https://bitbucket.org")
    # Resolved by UUID under the literal `{}` workspace (decision 6.1).
    assert any(seen.path == f"/2.0/repositories/{{}}/{fake.repo_uuid}" for seen in fake.requests)
