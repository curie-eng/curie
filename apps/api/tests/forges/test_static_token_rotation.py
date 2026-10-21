"""A replaced static token file takes effect at the next operation (#4630, AC 2).

An operator rotates a static forge token by replacing the Kubernetes Secret
key. The kubelet then rewrites the projected volume atomically: it writes a new
timestamped directory, points a temporary ``..data`` symlink at it and renames
that over ``..data``, while the key path the pod reads stays a symlink through
``..data``
(https://kubernetes.io/docs/concepts/configuration/secret/#mounted-secrets-are-updated-automatically).
``forge_fakes.projected_volume.ProjectedVolume`` lays files out and replaces
them the same way.

Each consumer of a static token is driven through production wiring: the
config is loaded by ``load_forges_json`` from a ``CURIE_FORGES`` document naming
the volume's key path, the registry is built by ``build_registry``, and every
HTTP read goes through the real adapter into the stateful fake for its forge
over an in-process transport. What the forge received is read from the fake's
request log, never from adapter internals. The git consumer is the code host's
credential provider, whose ``git_header`` is what git sends.

1. A replacement is sent on the next operation (a newly built adapter).
2. A missing, unreadable, empty, blank or malformed file fails closed with
   `CredentialUnreadable` and the previous value is never sent; replacing it
   with a valid file recovers (liveness beside every refusal).
3. A second, independent entry keeps working while the first is broken.
4. One operation (one adapter, one transport) sends one token even when the
   file is replaced between its requests.

Placeholder hosts, accounts and tokens only.
"""

from __future__ import annotations

import base64
import json
import secrets
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.config import (
    BindingConfig,
    RepositoryBindingConfig,
    StaticTokenCredentialConfig,
)
from curie_api.forges.credential_providers import StaticTokenProvider, TokenSnapshot
from curie_api.forges.errors import CredentialUnreadable, Unavailable
from curie_api.forges.loader import load_forges_json
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import CredentialExpiry, CredentialHeader, CredentialScope
from forge_fakes import bitbucket_cloud as cloud_fake
from forge_fakes import bitbucket_dc as dc_fake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes import jira as jira_fake
from forge_fakes.projected_volume import BROKEN, ProjectedVolume
from pydantic import ValidationError

CONTROL_HOST = "bbdc-control.example.invalid"
CONTROL_API = f"https://{CONTROL_HOST}/bitbucket"
CONTROL_NAME = "control"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def sentinel() -> str:
    return f"snt{secrets.token_hex(8)}"


def _needles(*tokens: str) -> list[str]:
    """Each token bare and in every header form it could take."""

    found: list[str] = []
    for token in tokens:
        found += [token, f"Bearer {token}", base64.b64encode(token.encode()).decode()]
        for username in ("oauth2", "x-token-auth", jira_fake.BOT_EMAIL):
            found.append(base64.b64encode(f"{username}:{token}".encode()).decode())
    return found


def _leaks(error: BaseException, *tokens: str) -> list[str]:
    texts = {
        "str": str(error),
        "repr": repr(error),
        "traceback": "".join(traceback.format_exception(error)),
        "problem": str(getattr(error, "problem", "")),
        "args": repr(error.args),
    }
    needles = _needles(*tokens)
    return [where for where, text in texts.items() if any(n in text for n in needles)]


# The forges ---------------------------------------------------------------------------


def _basic(username: str, token: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{token}".encode()).decode()


@dataclass
class Forge:
    """One consumer under test: its fake, entry, and how to drive and observe it."""

    entry: Callable[[str], dict[str, Any]]
    section: str
    fake: Any
    inner: Callable[[], httpx.AsyncBaseTransport]
    register: Callable[[str], None]
    seen: Callable[[], list[str | None]]
    expected: Callable[[str], str]
    build: Callable[[ForgeRegistry, httpx.AsyncClient], Any]
    read: Callable[[Any], Awaitable[Any]]


def _gitlab(consumer: str) -> Forge:
    fake = gitlab_fake.GitLabAPI()
    iid = fake.add_issue("Widget parser drops the last field")
    project = str(gitlab_fake.PROJECT_ID)
    tracker_name = "gitlab-issues"
    binding = BindingConfig(
        name=gitlab_fake.BINDING_NAME,
        tracker=tracker_name,
        tracker_scope_id=project,
        repos=(
            RepositoryBindingConfig(
                alias="widgets",
                code_host=gitlab_fake.CODE_HOST_NAME,
                project_id=project,
                path=gitlab_fake.PROJECT_PATH,
            ),
        ),
        default_repo="widgets",
    )

    def entry(token_file: str) -> dict[str, Any]:
        base = {
            "kind": "gitlab",
            "host": gitlab_fake.HOST,
            "api_url": fake.base_url,
            "credential": {"type": "static_token", "token_file": token_file},
        }
        if consumer == "tracker":
            return {
                **base,
                "name": tracker_name,
                "label": gitlab_fake.LABEL,
                "mention": gitlab_fake.BOT.login,
            }
        return {**base, "name": gitlab_fake.CODE_HOST_NAME}

    def build(registry: ForgeRegistry, client: httpx.AsyncClient) -> Any:
        if consumer == "tracker":
            return registry.binding_tracker(tracker_name).for_binding(client, binding)
        return registry.code_host(gitlab_fake.CODE_HOST_NAME).adapter(client)

    async def read(adapter: Any) -> Any:
        if consumer == "tracker":
            return await adapter.ticket_labels(fake.issue_ref(iid))
        return await adapter.resolve_repository(project)

    return Forge(
        entry=entry,
        section="trackers" if consumer == "tracker" else "code_hosts",
        fake=fake,
        inner=lambda: httpx.MockTransport(fake.handle),
        register=lambda token: fake.add_user(
            gitlab_fake.BOT, token=token, scopes=("api", "write_repository")
        ),
        seen=lambda: [request.headers.get("private-token") for request in fake.requests],
        # GitLab REST authenticates a personal, project or group access token in
        # the PRIVATE-TOKEN header
        # (https://docs.gitlab.com/api/rest/authentication/#personalprojectgroup-access-tokens).
        expected=lambda token: token,
        build=build,
        read=read,
    )


def _cloud() -> Forge:
    fake = cloud_fake.BitbucketCloudFake()

    def entry(token_file: str) -> dict[str, Any]:
        return {
            "name": "bitbucket-cloud",
            "kind": "bitbucket_cloud",
            "host": cloud_fake.HOST,
            "api_url": cloud_fake.API_URL,
            "credential": {"type": "static_token", "token_file": token_file},
        }

    return Forge(
        entry=entry,
        section="code_hosts",
        fake=fake,
        inner=lambda: httpx.ASGITransport(app=fake.app),
        register=lambda token: fake.add_user(cloud_fake.FACTORY, token),
        seen=lambda: [seen.headers.get("authorization") for seen in fake.requests],
        # A Bitbucket Cloud access token is sent as a Bearer token on API calls
        # (https://developer.atlassian.com/cloud/bitbucket/rest/intro/#access-tokens).
        expected=lambda token: f"Bearer {token}",
        build=lambda registry, client: registry.code_host("bitbucket-cloud").adapter(client),
        read=lambda adapter: adapter.resolve_repository(cloud_fake.REPO_UUID),
    )


def _dc(
    *, name: str = dc_fake.CODE_HOST_NAME, host: str = dc_fake.HOST, api: str = dc_fake.API_URL
) -> Forge:
    fake = dc_fake.BitbucketDcFake(base_url=api)

    def entry(token_file: str) -> dict[str, Any]:
        return {
            "name": name,
            "kind": "bitbucket_dc",
            "host": host,
            "api_url": api,
            "credential": {"type": "static_token", "token_file": token_file},
        }

    return Forge(
        entry=entry,
        section="code_hosts",
        fake=fake,
        inner=fake.transport,
        register=lambda token: fake.add_user(dc_fake.FACTORY, token=token, service=True),
        seen=lambda: [request.headers.get("authorization") for request in fake.requests],
        # A Bitbucket Data Center HTTP access token is sent as a Bearer token
        # (https://confluence.atlassian.com/bitbucketserver/http-access-tokens-939515499.html).
        expected=lambda token: f"Bearer {token}",
        build=lambda registry, client: registry.code_host(name).adapter(client),
        read=lambda adapter: adapter.resolve_repository(str(dc_fake.REPO_ID)),
    )


def _jira() -> Forge:
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Widget parser drops the last field", "Two fields, not three.")
    binding = BindingConfig.model_validate(
        {
            "name": jira_fake.BINDING_NAME,
            "tracker": jira_fake.TRACKER_NAME,
            "tracker_scope_id": jira_fake.CLOUD_ID,
            "repos": [
                {
                    "alias": "bot",
                    "code_host": jira_fake.CODE_HOST_NAME,
                    "project_id": jira_fake.GITHUB_REPO_ID,
                    "path": jira_fake.GITHUB_REPO,
                }
            ],
            "default_repo": "bot",
            "start_authority": {"type": "allowlist", "accounts": ["5b10a0000000000000000a11"]},
        }
    )

    def entry(token_file: str) -> dict[str, Any]:
        return {
            "name": jira_fake.TRACKER_NAME,
            "kind": "jira_cloud",
            "host": jira_fake.SITE_HOST,
            "api_url": jira_fake.SITE,
            "credential": {
                "type": "static_token",
                "token_file": token_file,
                "username": jira_fake.BOT_EMAIL,
            },
            "label": jira_fake.LABEL,
            "mention": jira_fake.BOT_ACCOUNT_ID,
        }

    return Forge(
        entry=entry,
        section="trackers",
        fake=fake,
        inner=lambda: httpx.ASGITransport(app=fake.app),
        register=lambda token: fake.accept_credential(jira_fake.BOT_EMAIL, token, fake.bot),
        seen=lambda: [request.authorization for request in fake.requests],
        # Jira Cloud takes the account email and API token as HTTP basic auth
        # (https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis/).
        expected=lambda token: _basic(jira_fake.BOT_EMAIL, token),
        build=lambda registry, client: registry.binding_tracker(jira_fake.TRACKER_NAME).for_binding(
            client, binding
        ),
        read=lambda tracker: tracker.ticket_labels(fake.ref(issue)),
    )


HTTP_FORGES: dict[str, Callable[[], Forge]] = {
    "gitlab-tracker": lambda: _gitlab("tracker"),
    "gitlab-code-host": lambda: _gitlab("code_host"),
    "bitbucket-cloud": _cloud,
    "bitbucket-dc": _dc,
    "jira-cloud": _jira,
}


@dataclass(frozen=True)
class Git:
    forge: str
    name: str
    # The git ``http.extraHeader`` value for a token.
    header: Callable[[str], str]


GIT_FORGES: dict[str, Git] = {
    # GitLab authenticates git over HTTPS with any token type as the username
    # oauth2 and the token as the password
    # (https://docs.gitlab.com/user/profile/personal_access_tokens/#clone-repository-using-personal-access-token).
    "gitlab": Git(
        "gitlab-code-host",
        gitlab_fake.CODE_HOST_NAME,
        lambda token: f"Authorization: {_basic('oauth2', token)}",
    ),
    # Bitbucket Cloud access tokens authenticate git as x-token-auth
    # (https://support.atlassian.com/bitbucket-cloud/docs/using-access-tokens/).
    "bitbucket-cloud": Git(
        "bitbucket-cloud",
        "bitbucket-cloud",
        lambda token: f"Authorization: {_basic('x-token-auth', token)}",
    ),
    # Bitbucket Data Center HTTP access tokens authenticate git as a Bearer token
    # (https://confluence.atlassian.com/bitbucketserver/http-access-tokens-939515499.html).
    "bitbucket-dc": Git(
        "bitbucket-dc",
        dc_fake.CODE_HOST_NAME,
        lambda token: f"Authorization: Bearer {token}",
    ),
}


# The rig ------------------------------------------------------------------------------


@dataclass
class Rig:
    """The forge under test and an independent control entry, both loaded from
    one ``CURIE_FORGES`` document naming projected-volume key paths."""

    forge: Forge
    volume: ProjectedVolume
    control: Forge
    control_volume: ProjectedVolume
    registry: ForgeRegistry
    old: str
    new: str
    control_token: str
    marks: list[int] = field(default_factory=list)

    def client(self, *, after_first: Callable[[], None] | None = None) -> httpx.AsyncClient:
        inner = self.forge.inner()
        if after_first is not None:
            inner = SwapAfterFirst(inner, after_first)
        return httpx.AsyncClient(transport=inner)

    async def operate(self) -> Any:
        """One operation: a freshly built adapter and one read."""

        async with self.client() as client:
            return await self.forge.read(self.forge.build(self.registry, client))

    def mark(self) -> int:
        return len(self.forge.seen())

    def sent_since(self, mark: int) -> list[str | None]:
        return self.forge.seen()[mark:]

    async def control_works(self) -> None:
        mark = len(self.control.seen())
        async with httpx.AsyncClient(transport=self.control.inner()) as client:
            await self.control.read(self.control.build(self.registry, client))
        sent = self.control.seen()[mark:]
        assert sent, "the control entry sent no request"
        assert set(sent) == {self.control.expected(self.control_token)}


class SwapAfterFirst(httpx.AsyncBaseTransport):
    """Forwards to ``inner``; runs ``action`` once, right after the first
    request of the operation was answered."""

    def __init__(self, inner: httpx.AsyncBaseTransport, action: Callable[[], None]) -> None:
        self._inner = inner
        self._action: Callable[[], None] | None = action

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        action, self._action = self._action, None
        if action is not None:
            action()
        return response


def _rig(tmp_path: Path, sentinel: str, forge: Forge) -> Rig:
    old, new, control_token = f"old-{sentinel}", f"new-{sentinel}", f"ctl-{sentinel}"
    control = _dc(name=CONTROL_NAME, host=CONTROL_HOST, api=CONTROL_API)
    for token in (old, new):
        forge.register(token)
    control.register(control_token)
    volume = ProjectedVolume(tmp_path / "entry", f"{old}\n")
    control_volume = ProjectedVolume(tmp_path / "control", f"{control_token}\n")
    document: dict[str, list[dict[str, Any]]] = {"code_hosts": [], "trackers": []}
    document[forge.section].append(forge.entry(str(volume.path)))
    document["code_hosts"].append(control.entry(str(control_volume.path)))
    registry = build_registry(load_forges_json(json.dumps(document)))
    return Rig(forge, volume, control, control_volume, registry, old, new, control_token)


@pytest.fixture(params=sorted(HTTP_FORGES))
def http_rig(request: pytest.FixtureRequest, tmp_path: Path, sentinel: str) -> Rig:
    return _rig(tmp_path, sentinel, HTTP_FORGES[request.param]())


@pytest.fixture(params=sorted(GIT_FORGES))
def git_case(request: pytest.FixtureRequest, tmp_path: Path, sentinel: str) -> tuple[Rig, Git]:
    git = GIT_FORGES[request.param]
    return _rig(tmp_path, sentinel, HTTP_FORGES[git.forge]()), git


async def _git_header(rig: Rig, git: Git) -> str:
    credential = await rig.registry.code_host(git.name).credentials.get(CredentialScope.CLONE)
    header: str = credential.git_header()
    return header


# 1. Replacement is used on the next operation ----------------------------------------


@pytest.mark.anyio
async def test_a_replaced_file_is_sent_on_the_next_http_operation(http_rig: Rig) -> None:
    rig = http_rig
    await rig.operate()
    assert set(rig.sent_since(0)) == {rig.forge.expected(rig.old)}

    rig.volume.publish(f"{rig.new}\n")
    mark = rig.mark()
    await rig.operate()

    sent = rig.sent_since(mark)
    assert sent, "the operation sent no request"
    assert set(sent) == {rig.forge.expected(rig.new)}


@pytest.mark.anyio
async def test_a_replaced_file_is_the_next_git_credential(git_case: tuple[Rig, Git]) -> None:
    rig, git = git_case
    assert await _git_header(rig, git) == git.header(rig.old)

    rig.volume.publish(f"  {rig.new}\r\n")

    assert await _git_header(rig, git) == git.header(rig.new)


@pytest.mark.anyio
async def test_a_replacement_back_to_the_original_value_is_sent_again(http_rig: Rig) -> None:
    rig = http_rig
    rig.volume.publish(rig.new)
    mark = rig.mark()
    await rig.operate()
    assert set(rig.sent_since(mark)) == {rig.forge.expected(rig.new)}
    rig.volume.publish(rig.old)
    mark = rig.mark()

    await rig.operate()

    assert set(rig.sent_since(mark)) == {rig.forge.expected(rig.old)}


# 2. Broken files fail closed, and a valid replacement recovers -----------------------


@pytest.mark.anyio
@pytest.mark.parametrize("how", sorted(BROKEN))
async def test_a_broken_file_fails_closed_over_http_and_a_valid_one_recovers(
    http_rig: Rig, sentinel: str, how: str
) -> None:
    rig = http_rig
    await rig.operate()
    rig.volume.break_with(how, rig.new)
    mark = rig.mark()

    with pytest.raises(CredentialUnreadable) as refused:
        await rig.operate()

    assert isinstance(refused.value, Unavailable)
    assert isinstance(refused.value.problem, str) and refused.value.problem
    assert _leaks(refused.value, rig.old, rig.new, sentinel) == []
    assert rig.forge.expected(rig.old) not in rig.sent_since(mark)

    # Liveness: the next valid replacement is accepted and sent.
    rig.volume.publish(f"{rig.new}\n")
    mark = rig.mark()
    await rig.operate()
    sent = rig.sent_since(mark)
    assert sent and set(sent) == {rig.forge.expected(rig.new)}


@pytest.mark.anyio
@pytest.mark.parametrize("how", sorted(BROKEN))
async def test_a_broken_file_fails_closed_for_git_and_a_valid_one_recovers(
    git_case: tuple[Rig, Git], sentinel: str, how: str
) -> None:
    rig, git = git_case
    assert await _git_header(rig, git) == git.header(rig.old)
    rig.volume.break_with(how, rig.new)

    with pytest.raises(CredentialUnreadable) as refused:
        await _git_header(rig, git)

    assert _leaks(refused.value, rig.old, rig.new, sentinel) == []

    # Liveness: a valid replacement is the next credential, never the old one.
    rig.volume.publish(f"{rig.new}\n")
    assert await _git_header(rig, git) == git.header(rig.new)


# 3. An independent entry keeps working ------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("how", ["missing", "empty", "nul"])
async def test_an_independent_entry_keeps_working_while_another_is_broken(
    http_rig: Rig, how: str
) -> None:
    rig = http_rig
    await rig.control_works()
    rig.volume.break_with(how, rig.new)

    with pytest.raises(CredentialUnreadable):
        await rig.operate()
    await rig.control_works()

    rig.volume.publish(rig.new)
    await rig.operate()
    await rig.control_works()


# 4. One operation, one token ------------------------------------------------------------


@pytest.mark.anyio
async def test_one_operation_sends_one_token_when_the_file_is_replaced_mid_operation(
    http_rig: Rig,
) -> None:
    rig = http_rig
    mark = rig.mark()
    async with rig.client(after_first=lambda: rig.volume.publish(rig.new)) as client:
        adapter = rig.forge.build(rig.registry, client)
        await rig.forge.read(adapter)
        await rig.forge.read(adapter)

    within = rig.sent_since(mark)
    assert len(within) >= 2, within
    assert set(within) == {rig.forge.expected(rig.old)}

    mark = rig.mark()
    await rig.operate()
    assert set(rig.sent_since(mark)) == {rig.forge.expected(rig.new)}


# The configuration and provider interface ---------------------------------------------


def _document(volume: ProjectedVolume) -> str:
    forge = _dc()
    return json.dumps({"code_hosts": [forge.entry(str(volume.path))]})


def test_the_loader_keeps_the_token_file_reference_not_its_value(
    tmp_path: Path, sentinel: str
) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"tok-{sentinel}\n")

    config = load_forges_json(_document(volume))

    credential = config.code_hosts[0].credential
    assert isinstance(credential, StaticTokenCredentialConfig)
    assert credential.token_file == str(volume.path)
    assert credential.token is None
    assert sentinel not in repr(config)
    assert sentinel not in config.model_dump_json()
    assert credential.current_token() == f"tok-{sentinel}"


@pytest.mark.parametrize("how", sorted(BROKEN))
def test_the_loader_still_refuses_a_broken_file_at_load(
    tmp_path: Path, sentinel: str, how: str
) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"tok-{sentinel}")
    volume.break_with(how, f"tok-{sentinel}")

    with pytest.raises(ValueError) as refused:
        load_forges_json(_document(volume))

    message = str(refused.value)
    assert repr(dc_fake.CODE_HOST_NAME) in message, message
    assert str(volume.path) in message, message
    assert _leaks(refused.value, f"tok-{sentinel}", sentinel) == []


def test_current_token_rereads_the_file_through_a_symlink_swap(
    tmp_path: Path, sentinel: str
) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"one-{sentinel}\n")
    credential = load_forges_json(_document(volume)).code_hosts[0].credential
    assert isinstance(credential, StaticTokenCredentialConfig)
    assert credential.current_token() == f"one-{sentinel}"

    volume.publish(f" two-{sentinel}\t\r\n")

    assert credential.current_token() == f"two-{sentinel}"


@pytest.mark.parametrize("how", sorted(BROKEN))
def test_current_token_refuses_a_broken_file_without_the_value(
    tmp_path: Path, sentinel: str, how: str
) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"one-{sentinel}")
    credential = load_forges_json(_document(volume)).code_hosts[0].credential
    assert isinstance(credential, StaticTokenCredentialConfig)
    volume.break_with(how, f"two-{sentinel}")

    with pytest.raises(CredentialUnreadable) as refused:
        credential.current_token()

    assert isinstance(refused.value, Unavailable)
    assert _leaks(refused.value, f"one-{sentinel}", f"two-{sentinel}", sentinel) == []

    # Liveness: a valid file is read again, the earlier value never comes back.
    volume.publish(f"three-{sentinel}\n")
    assert credential.current_token() == f"three-{sentinel}"


def test_a_static_token_credential_names_exactly_one_source(tmp_path: Path, sentinel: str) -> None:
    path = tmp_path / "token"
    path.write_text(f"tok-{sentinel}")

    with pytest.raises(ValidationError) as both:
        StaticTokenCredentialConfig.model_validate(
            {"token": f"tok-{sentinel}", "token_file": str(path)}
        )
    assert sentinel not in str(both.value)
    with pytest.raises(ValidationError):
        StaticTokenCredentialConfig.model_validate({"type": "static_token"})

    inline = StaticTokenCredentialConfig.model_validate({"token": f" tok-{sentinel}\n"})
    assert inline.current_token() == f"tok-{sentinel}"
    by_file = StaticTokenCredentialConfig.model_validate({"token_file": str(path)})
    assert by_file.current_token() == f"tok-{sentinel}"


def test_a_token_snapshot_reads_once_and_holds_the_value(tmp_path: Path, sentinel: str) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"one-{sentinel}")
    credential = load_forges_json(_document(volume)).code_hosts[0].credential

    snapshot = TokenSnapshot(credential)
    assert snapshot.value() == f"one-{sentinel}"
    volume.publish(f"two-{sentinel}")
    assert snapshot.value() == f"one-{sentinel}"
    volume.break_with("empty", "")
    assert snapshot.value() == f"one-{sentinel}"

    # A fresh snapshot reads the file now; a broken file refuses at first value().
    later = TokenSnapshot(credential)
    with pytest.raises(CredentialUnreadable):
        later.value()
    volume.publish(f"three-{sentinel}")
    assert TokenSnapshot(credential).value() == f"three-{sentinel}"


@pytest.mark.anyio
async def test_a_static_token_provider_rereads_on_every_get(tmp_path: Path, sentinel: str) -> None:
    volume = ProjectedVolume(tmp_path / "entry", f"one-{sentinel}")
    credential = load_forges_json(_document(volume)).code_hosts[0].credential
    assert isinstance(credential, StaticTokenCredentialConfig)

    provider = StaticTokenProvider(
        credential,
        origin=f"{dc_fake.API_URL}/scm",
        header=CredentialHeader.AUTHORIZATION_BEARER,
    )
    assert sentinel not in repr(provider)
    assert provider.expiry().expiry is CredentialExpiry.UNKNOWN

    first = await provider.get(CredentialScope.CLONE)
    assert first.secret == f"one-{sentinel}"
    volume.publish(f"two-{sentinel}\n")
    assert (await provider.get(CredentialScope.PUSH)).secret == f"two-{sentinel}"
    volume.break_with("missing", "")
    with pytest.raises(CredentialUnreadable):
        await provider.get(CredentialScope.CLONE)
