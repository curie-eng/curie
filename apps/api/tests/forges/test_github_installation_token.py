"""The GitHub code host mints only for the installation a WorkItem recorded
(#4358 plan D14 items 2 and 2a, review r2 finding 2).

`installation_token` is the token source of every per-WorkItem GitHub tracker:
it rediscovers the App's installation on the repository and mints only when it
is the recorded one, with the provider timeout capped by the caller.
`installation_for` names the installation the App has on a repository now. A
refusal is `Unavailable("installation")` and carries no id or token. The App's
HTTP calls go to a fake GitHub through the real `GitHubCredentials`.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.errors import Unavailable
from curie_api.forges.registry import build_registry
from curie_api.forges.types import RepositoryRef

REPO = "acme-corp/acme-bot"
REPOSITORY = RepositoryRef(types.GITHUB, "github.com", "4401", REPO)
_REAL_CLIENT = httpx.Client


def _minted(installation: object) -> str:
    """The fake installation token the fake App mints for one installation."""

    return "ghs_" + f"minted_{installation}"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


class _GitHubApp:
    """The App endpoints `GitHubCredentials` calls, and the clients it opened."""

    def __init__(self, *, installation: int | None) -> None:
        self.installation = installation
        self.requests: list[httpx.Request] = []
        self.client_kwargs: list[dict[str, Any]] = []

    def client(self, *args: Any, **kwargs: Any) -> httpx.Client:
        self.client_kwargs.append(kwargs)
        return _REAL_CLIENT(transport=httpx.MockTransport(self.handle))

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == f"/repos/{REPO}/installation":
            if self.installation is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"id": self.installation})
        minted = re.fullmatch(r"/app/installations/(\d+)/access_tokens", request.url.path)
        if minted and request.method == "POST":
            return httpx.Response(
                201,
                json={"token": _minted(minted[1]), "expires_at": "2999-01-01T00:00:00Z"},
            )
        return httpx.Response(404, json={"message": "Not Found"})

    def mints(self) -> list[str]:
        return [r.url.path for r in self.requests if r.url.path.endswith("/access_tokens")]


def _connection(private_key: str, monkeypatch: pytest.MonkeyPatch, app: _GitHubApp) -> Any:
    monkeypatch.setattr("curie_api.github_app.httpx.Client", app.client)
    config = ForgesConfig.model_validate(
        {
            "code_hosts": [
                {
                    "name": "corp-github",
                    "kind": "github",
                    "host": "github.com",
                    "api_url": "https://api.github.com",
                    # A fresh App id per test: resolvers and their token caches
                    # are shared per credential configuration.
                    "credential": {
                        "type": "github_app",
                        "app_id": str(uuid.uuid4().int % 10**9),
                        "private_key": private_key,
                    },
                }
            ]
        }
    )
    return build_registry(config).code_host("corp-github")


def _assert_clean(message: str) -> None:
    assert "ghs_" not in message
    assert not re.search(r"\d", message), message


@pytest.mark.anyio
async def test_installation_token_mints_for_the_recorded_installation(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _GitHubApp(installation=4242)
    connection = _connection(private_key, monkeypatch, app)

    token = await connection.installation_token(REPOSITORY, 4242, timeout_s=5.0)

    assert token == _minted(4242)
    assert app.mints() == ["/app/installations/4242/access_tokens"]
    # The entry's provider timeout (15 s by default) is capped by the caller's.
    assert app.client_kwargs and {kw["timeout"] for kw in app.client_kwargs} == {5.0}


@pytest.mark.anyio
async def test_installation_token_without_a_cap_keeps_the_provider_timeout(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _GitHubApp(installation=4242)
    connection = _connection(private_key, monkeypatch, app)

    assert await connection.installation_token(REPOSITORY, 4242) == _minted(4242)
    assert {kw["timeout"] for kw in app.client_kwargs} == {15.0}


@pytest.mark.anyio
async def test_a_mismatched_installation_is_refused(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The repository was recorded under installation 1; the App now has 2.
    app = _GitHubApp(installation=2)
    connection = _connection(private_key, monkeypatch, app)

    with pytest.raises(Unavailable) as refused:
        await connection.installation_token(REPOSITORY, 1, timeout_s=5.0)

    assert str(refused.value) == "installation"
    _assert_clean(str(refused.value))
    assert refused.value.__cause__ is None
    assert app.mints() == []


@pytest.mark.anyio
async def test_installation_for_names_the_apps_installation_now(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _GitHubApp(installation=4242)
    connection = _connection(private_key, monkeypatch, app)

    assert await connection.installation_for(REPOSITORY) == 4242


@pytest.mark.anyio
async def test_installation_for_a_repository_without_the_app_is_unavailable(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _GitHubApp(installation=None)
    connection = _connection(private_key, monkeypatch, app)

    with pytest.raises(Unavailable) as refused:
        await connection.installation_for(REPOSITORY)

    assert str(refused.value) == "installation"
    assert app.mints() == []
