"""One code host operation carries one static token, git credential included (#4630).

A code host adapter is one operation. Its HTTP checks and the git credential
`CodeHost.credential` returns must carry the same token, even when the operator
replaces the Secret key between them: otherwise git could clone with a token
the adapter never checked the repository with. The next operation (a newly
built adapter) reads the replaced file.

Each code host is driven through production wiring: ``load_forges_json`` reads a
``CURIE_FORGES`` document naming a projected-volume key path, ``build_registry``
builds the connection, and the adapter talks to the stateful fake for its forge.
The token each request sent is read from the fake's request log; the git
credential is what `credential` returns.

Placeholder hosts, accounts and tokens only.
"""

from __future__ import annotations

import base64
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.loader import load_forges_json
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import CredentialScope, RepositoryRef
from forge_fakes import bitbucket_cloud as cloud_fake
from forge_fakes import bitbucket_dc as dc_fake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes.projected_volume import ProjectedVolume


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _basic(username: str, token: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{token}".encode()).decode()


@dataclass
class Host:
    """One code host: its fake, its entry, and what it sees and git sends."""

    name: str
    fake: Any
    inner: Callable[[], httpx.AsyncBaseTransport]
    entry: Callable[[str], dict[str, Any]]
    register: Callable[[str], None]
    # The token-bearing header of each request the fake received, in order.
    seen: Callable[[], list[str | None]]
    # That header for a token.
    sent: Callable[[str], str]
    # The git ``http.extraHeader`` value for a token.
    git: Callable[[str], str]
    project_id: str


def _gitlab() -> Host:
    fake = gitlab_fake.GitLabAPI()
    return Host(
        name=gitlab_fake.CODE_HOST_NAME,
        fake=fake,
        inner=lambda: httpx.MockTransport(fake.handle),
        entry=lambda token_file: {
            "name": gitlab_fake.CODE_HOST_NAME,
            "kind": "gitlab",
            "host": gitlab_fake.HOST,
            "api_url": fake.base_url,
            "credential": {"type": "static_token", "token_file": token_file},
        },
        register=lambda token: fake.add_user(
            gitlab_fake.BOT, token=token, scopes=("api", "write_repository")
        ),
        seen=lambda: [request.headers.get("private-token") for request in fake.requests],
        sent=lambda token: token,
        git=lambda token: f"Authorization: {_basic('oauth2', token)}",
        project_id=str(gitlab_fake.PROJECT_ID),
    )


def _cloud() -> Host:
    fake = cloud_fake.BitbucketCloudFake()
    return Host(
        name="bitbucket-cloud",
        fake=fake,
        inner=lambda: httpx.ASGITransport(app=fake.app),
        entry=lambda token_file: {
            "name": "bitbucket-cloud",
            "kind": "bitbucket_cloud",
            "host": cloud_fake.HOST,
            "api_url": cloud_fake.API_URL,
            "credential": {"type": "static_token", "token_file": token_file},
        },
        register=lambda token: fake.add_user(cloud_fake.FACTORY, token),
        seen=lambda: [request.headers.get("authorization") for request in fake.requests],
        sent=lambda token: f"Bearer {token}",
        git=lambda token: f"Authorization: {_basic('x-token-auth', token)}",
        project_id=cloud_fake.REPO_UUID,
    )


def _dc() -> Host:
    fake = dc_fake.BitbucketDcFake(base_url=dc_fake.API_URL)
    return Host(
        name=dc_fake.CODE_HOST_NAME,
        fake=fake,
        inner=fake.transport,
        entry=lambda token_file: {
            "name": dc_fake.CODE_HOST_NAME,
            "kind": "bitbucket_dc",
            "host": dc_fake.HOST,
            "api_url": dc_fake.API_URL,
            "credential": {"type": "static_token", "token_file": token_file},
        },
        register=lambda token: fake.add_user(dc_fake.FACTORY, token=token, service=True),
        seen=lambda: [request.headers.get("authorization") for request in fake.requests],
        sent=lambda token: f"Bearer {token}",
        git=lambda token: f"Authorization: Bearer {token}",
        project_id=str(dc_fake.REPO_ID),
    )


HOSTS: dict[str, Callable[[], Host]] = {
    "gitlab": _gitlab,
    "bitbucket-cloud": _cloud,
    "bitbucket-dc": _dc,
}


@dataclass
class Rig:
    host: Host
    volume: ProjectedVolume
    registry: ForgeRegistry
    old: str
    new: str

    def adapter(self, client: httpx.AsyncClient) -> Any:
        return self.registry.code_host(self.host.name).adapter(client)

    def mark(self) -> int:
        return len(self.host.seen())

    def sent_since(self, mark: int) -> list[str | None]:
        return self.host.seen()[mark:]


@pytest.fixture(params=sorted(HOSTS))
def rig(request: pytest.FixtureRequest, tmp_path: Path) -> Rig:
    host = HOSTS[request.param]()
    sentinel = secrets.token_hex(8)
    old, new = f"old-{sentinel}", f"new-{sentinel}"
    for token in (old, new):
        host.register(token)
    volume = ProjectedVolume(tmp_path / "entry", f"{old}\n")
    document = {"code_hosts": [host.entry(str(volume.path))], "trackers": []}
    registry = build_registry(load_forges_json(json.dumps(document)))
    return Rig(host, volume, registry, old, new)


async def _repository(rig: Rig, adapter: Any) -> RepositoryRef:
    repository: RepositoryRef = await adapter.resolve_repository(rig.host.project_id)
    return repository


@pytest.mark.anyio
async def test_a_replacement_mid_operation_does_not_change_its_git_credential(rig: Rig) -> None:
    async with httpx.AsyncClient(transport=rig.host.inner()) as client:
        adapter = rig.adapter(client)
        mark = rig.mark()
        repository = await _repository(rig, adapter)

        # The operator replaces the key between the adapter's HTTP check and
        # its git credential.
        rig.volume.publish(f"{rig.new}\n")
        credential = await adapter.credential(repository, CredentialScope.CLONE)

        sent = rig.sent_since(mark)
        assert sent, "the operation sent no request"
        # The credential's own repository checks and the git credential both
        # carry the token the operation started with.
        assert set(sent) == {rig.host.sent(rig.old)}
        assert credential.git_header() == rig.host.git(rig.old)


@pytest.mark.anyio
async def test_the_next_operation_reads_the_replaced_token_for_git(rig: Rig) -> None:
    async with httpx.AsyncClient(transport=rig.host.inner()) as client:
        first = rig.adapter(client)
        repository = await _repository(rig, first)
        before = await first.credential(repository, CredentialScope.CLONE)
        assert before.git_header() == rig.host.git(rig.old)

        rig.volume.publish(f"{rig.new}\n")
        mark = rig.mark()
        after = await rig.adapter(client).credential(repository, CredentialScope.CLONE)

        sent = rig.sent_since(mark)
        assert sent and set(sent) == {rig.host.sent(rig.new)}
        assert after.git_header() == rig.host.git(rig.new)
