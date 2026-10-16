"""``GET /forges/credentials/health`` reports whether each forge accepts its credential (#4630).

The report is read-only and platform-key authenticated. For every configured
code host and tracker it reads the current credential and makes one existing
read through the real adapter: a static-token code host resolves the first
repository a binding names on it, a binding-scoped tracker verifies its
credential with a read. Authentication is ``accepted`` on success,
``rejected`` (``unauthorized``) on a 401, and otherwise ``unknown`` with a
reason: never accepted without a successful authenticated read.

The app is the real FastAPI app over a disposable Postgres, booted with a
``CURIE_FORGES`` document whose static tokens live in Kubernetes-style projected
volumes (`forge_fakes.projected_volume`). Each forge host is answered by its
stateful fake: every request any API client sends to a fake's host is routed
into it in process, so no seam inside the API is replaced and the test does not
depend on how the probe builds its clients. Placeholder hosts, accounts and
tokens only.
"""

from __future__ import annotations

import base64
import json
import logging
import secrets
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes import bitbucket_cloud as cloud_fake
from forge_fakes import bitbucket_dc as dc_fake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes import jira as jira_fake
from forge_fakes.github import fake_installation_tokens
from forge_fakes.github_comments import _rows
from forge_fakes.projected_volume import ProjectedVolume

pytestmark = pytest.mark.usefixtures("clean_db")

PATH = "/forges/credentials/health"
CODE_HOST = "code_host"
TRACKER = "tracker"
GITLAB = gitlab_fake.CODE_HOST_NAME
GITLAB_TRACKER = gitlab_fake.TRACKER_NAME
# The GitLab code host and tracker share one config name, so the volumes and
# tokens are kept per slot.
GL_HOST_SLOT = "gitlab-code-host"
GL_TRACKER_SLOT = "gitlab-tracker"
DC = dc_fake.CODE_HOST_NAME
CLOUD = "bitbucket-cloud"
GITHUB = "github"
JIRA = jira_fake.TRACKER_NAME
ENTRY_FIELDS = {
    "section",
    "name",
    "kind",
    "credential_type",
    "authentication",
    "reason",
    "observed_at",
}


# The forges -----------------------------------------------------------------------------


class _Routed:
    """Every request to a fake's host goes into that fake; others pass through."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = httpx.AsyncHTTPTransport.handle_async_request
        routes = self.routes

        async def handle(transport: Any, request: httpx.Request) -> httpx.Response:
            build = routes.get(request.url.host)
            if build is None:
                return await original(transport, request)
            routed: httpx.AsyncBaseTransport = build()
            return await routed.handle_async_request(request)

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)


@dataclass
class Install:
    client: TestClient
    gitlab: gitlab_fake.GitLabAPI
    dc: dc_fake.BitbucketDcFake
    jira: jira_fake.JiraFake
    cloud: cloud_fake.BitbucketCloudFake
    volumes: dict[str, ProjectedVolume]
    tokens: dict[str, str]
    spare: dict[str, str]
    private_key: str

    def report(self, headers: dict[str, str] | None = None) -> httpx.Response:
        if headers is None:
            headers = {"X-API-Key": get_settings().api_key}
        return self.client.get(PATH, headers=headers)

    def entries(self) -> dict[tuple[str, str], dict[str, Any]]:
        response = self.report()
        assert response.status_code == 200, response.text
        body = response.json()
        _parse_time(body["observed_at"])
        entries = {(entry["section"], entry["name"]): entry for entry in body["entries"]}
        assert len(entries) == len(body["entries"]), body
        return entries

    def all_requests(self) -> list[tuple[str, str]]:
        """(method, host-tagged path) of every request each fake received."""

        return (
            [(r.method, f"gitlab {r.url.path}") for r in self.gitlab.requests]
            + [(r.method, f"dc {r.path}") for r in self.dc.requests]
            + [(r.method, f"jira {r.path}") for r in self.jira.requests]
            + [(s.method, f"cloud {s.path}") for s in self.cloud.requests]
        )

    def needles(self) -> list[str]:
        """Every credential value, bare and in each header form it could take."""

        found = [self.private_key]
        for token in [*self.tokens.values(), *self.spare.values()]:
            found += [token, f"Bearer {token}", base64.b64encode(token.encode()).decode()]
            for username in ("oauth2", "x-token-auth", jira_fake.BOT_EMAIL):
                found.append(base64.b64encode(f"{username}:{token}".encode()).decode())
        return found


def _parse_time(value: Any) -> datetime:
    assert isinstance(value, str), value
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None, value
    return parsed


def _credential(volume: ProjectedVolume, **extra: Any) -> dict[str, Any]:
    return {"type": "static_token", "token_file": str(volume.path), **extra}


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Install]:
    sentinel = secrets.token_hex(8)
    gitlab = gitlab_fake.GitLabAPI()
    dc = dc_fake.BitbucketDcFake()
    jira = jira_fake.JiraFake()
    cloud = cloud_fake.BitbucketCloudFake()
    # One current and one spare valid token per entry, each registered on its
    # forge, so a test can tell a replacement was read.
    names = (GL_HOST_SLOT, GL_TRACKER_SLOT, DC, JIRA, CLOUD)
    tokens = {name: f"cur-{name}-{sentinel}" for name in names}
    spare = {name: f"next-{name}-{sentinel}" for name in names}
    for name in (GL_HOST_SLOT, GL_TRACKER_SLOT):
        for token in (tokens[name], spare[name]):
            gitlab.add_user(gitlab_fake.BOT, token=token, scopes=("api", "write_repository"))
    for token in (tokens[DC], spare[DC]):
        dc.add_user(dc_fake.FACTORY, token=token, service=True)
    for token in (tokens[JIRA], spare[JIRA]):
        jira.accept_credential(jira_fake.BOT_EMAIL, token, jira.bot)
    for token in (tokens[CLOUD], spare[CLOUD]):
        cloud.add_user(cloud_fake.FACTORY, token)
    volumes = {name: ProjectedVolume(tmp_path / name, f"{tokens[name]}\n") for name in names}
    private_key = f"example-private-key-{sentinel}"
    key_file = tmp_path / "github-app-key"
    key_file.write_text(private_key)

    document = {
        "code_hosts": [
            {
                "name": GITLAB,
                "kind": "gitlab",
                "host": gitlab_fake.HOST,
                "api_url": gitlab.base_url,
                "credential": _credential(volumes[GL_HOST_SLOT], never_expires=True),
            },
            {
                "name": DC,
                "kind": "bitbucket_dc",
                "host": dc_fake.HOST,
                "api_url": dc_fake.API_URL,
                "credential": _credential(volumes[DC]),
            },
            # Bound by no binding: nothing to probe.
            {
                "name": CLOUD,
                "kind": "bitbucket_cloud",
                "host": cloud_fake.HOST,
                "api_url": cloud_fake.API_URL,
                "credential": _credential(volumes[CLOUD]),
            },
            {
                "name": GITHUB,
                "kind": "github",
                "host": "github.com",
                "api_url": "https://api.github.com",
                "credential": {
                    "type": "github_app",
                    "app_id": "51",
                    "private_key_file": str(key_file),
                },
            },
        ],
        "trackers": [
            {
                "name": GITLAB_TRACKER,
                "kind": "gitlab",
                "host": gitlab_fake.HOST,
                "api_url": gitlab.base_url,
                "credential": _credential(volumes[GL_TRACKER_SLOT]),
                "label": gitlab_fake.LABEL,
                "mention": gitlab_fake.BOT.login,
            },
            {
                "name": JIRA,
                "kind": "jira_cloud",
                "host": jira_fake.SITE_HOST,
                "api_url": jira_fake.SITE,
                "credential": _credential(volumes[JIRA], username=jira_fake.BOT_EMAIL),
                "label": jira_fake.LABEL,
                "mention": jira_fake.BOT_ACCOUNT_ID,
            },
        ],
        "bindings": [
            gitlab_fake.forges_document()["bindings"][0],
            {
                "name": jira_fake.BINDING_NAME,
                "tracker": JIRA,
                "tracker_scope_id": jira_fake.CLOUD_ID,
                "repos": [
                    {
                        "alias": "demo",
                        "code_host": DC,
                        "project_id": str(dc_fake.REPO_ID),
                        "path": dc_fake.REPO_PATH,
                    }
                ],
                "default_repo": "demo",
                "start_authority": {
                    "type": "allowlist",
                    "accounts": ["5b10a0000000000000000a11"],
                },
            },
        ],
    }
    env = {
        "FACTORY_ENABLED": "true",
        "FACTORY_INTAKE": "poll",
        "CURIE_FORGES": json.dumps(document),
        "GITHUB_APP_ID": "51",
        "GITHUB_APP_PRIVATE_KEY": "example-private-key",
        "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_TOKEN": "",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "RUNS_STREAM": f"test:curie:credential-health:{uuid.uuid4().hex}",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(name, raising=False)
    fake_installation_tokens(monkeypatch)
    _Routed(
        {
            gitlab_fake.HOST: lambda: httpx.MockTransport(gitlab.handle),
            dc_fake.HOST: dc.transport,
            jira_fake.SITE_HOST: lambda: httpx.ASGITransport(app=jira.app),
            "api.bitbucket.org": lambda: httpx.ASGITransport(app=cloud.app),
        }
    ).install(monkeypatch)
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            yield Install(client, gitlab, dc, jira, cloud, volumes, tokens, spare, private_key)
    finally:
        get_settings.cache_clear()


def _assert_read_only(install: Install) -> None:
    """The probes only read: every request was a GET, no fake counted a write,
    and no intake cursor was saved."""

    requests = install.all_requests()
    assert requests, "no probe reached a forge"
    assert {method for method, _ in requests} == {"GET"}, requests
    assert (install.gitlab.writes, install.dc.writes, install.jira.writes) == (0, 0, 0)
    assert install.cloud.writes == 0
    assert _rows("SELECT * FROM curie.factory_poll_cursors", {}) == []


def _gitlab_sent(install: Install, since: int) -> list[str | None]:
    return [r.headers.get("private-token") for r in install.gitlab.requests[since:]]


# Authentication of the route -----------------------------------------------------------


def test_the_report_requires_the_platform_key(install: Install) -> None:
    assert install.report(headers={}).status_code == 401
    assert install.report(headers={"X-API-Key": "not-the-platform-key"}).status_code == 401
    assert install.all_requests() == []


# Accepted, and the entries that are never probed ----------------------------------------


def test_every_probed_static_entry_is_accepted_and_the_rest_are_unknown(install: Install) -> None:
    entries = install.entries()

    assert set(entries) == {
        (CODE_HOST, GITLAB),
        (CODE_HOST, DC),
        (CODE_HOST, CLOUD),
        (CODE_HOST, GITHUB),
        (TRACKER, GITLAB_TRACKER),
        (TRACKER, JIRA),
    }
    for entry in entries.values():
        assert set(entry) >= ENTRY_FIELDS, entry
        _parse_time(entry["observed_at"])
    for key, kind in (
        ((CODE_HOST, GITLAB), "gitlab"),
        ((CODE_HOST, DC), "bitbucket_dc"),
        ((TRACKER, GITLAB_TRACKER), "gitlab"),
        ((TRACKER, JIRA), "jira_cloud"),
    ):
        assert entries[key]["kind"] == kind
        assert entries[key]["credential_type"] == "static_token"
        assert entries[key]["authentication"] == "accepted", entries[key]
        assert entries[key]["reason"] is None
    # A static entry no binding uses has no read to make.
    assert entries[(CODE_HOST, CLOUD)]["authentication"] == "unknown"
    assert entries[(CODE_HOST, CLOUD)]["reason"] == "no_probe"
    assert install.cloud.requests == []
    # A re-minting credential is not probed.
    assert entries[(CODE_HOST, GITHUB)]["credential_type"] == "github_app"
    assert entries[(CODE_HOST, GITHUB)]["authentication"] == "unknown"
    assert entries[(CODE_HOST, GITHUB)]["reason"] == "not_probed"
    # Each probe authenticated with the entry's own current token.
    assert set(_gitlab_sent(install, 0)) == {
        install.tokens[GL_HOST_SLOT],
        install.tokens[GL_TRACKER_SLOT],
    }
    _assert_read_only(install)


# Rejected, and recovery after a valid replacement ----------------------------------------


def test_a_rejected_replacement_is_reported_and_a_restored_one_recovers(install: Install) -> None:
    assert install.entries()[(CODE_HOST, GITLAB)]["authentication"] == "accepted"
    assert install.entries()[(TRACKER, JIRA)]["authentication"] == "accepted"
    # Nonempty, well formed, and unknown to the forge: GitLab and Jira answer 401.
    install.volumes[GL_HOST_SLOT].publish("revoked-gitlab-token-value\n")
    install.volumes[JIRA].publish("revoked-jira-token-value\n")

    rejected = install.entries()

    for key in ((CODE_HOST, GITLAB), (TRACKER, JIRA)):
        assert rejected[key]["authentication"] == "rejected", rejected[key]
        assert rejected[key]["reason"] == "unauthorized"
    # Independent entries on the same forges keep their own credentials.
    assert rejected[(TRACKER, GITLAB_TRACKER)]["authentication"] == "accepted"
    assert rejected[(CODE_HOST, DC)]["authentication"] == "accepted"

    install.volumes[GL_HOST_SLOT].publish(f"{install.spare[GL_HOST_SLOT]}\n")
    install.volumes[JIRA].publish(f"{install.spare[JIRA]}\n")
    mark = len(install.gitlab.requests)

    recovered = install.entries()

    for key in ((CODE_HOST, GITLAB), (TRACKER, JIRA)):
        assert recovered[key]["authentication"] == "accepted", recovered[key]
        assert recovered[key]["reason"] is None
    assert install.spare[GL_HOST_SLOT] in _gitlab_sent(install, mark)
    assert install.tokens[GL_HOST_SLOT] not in _gitlab_sent(install, mark)
    assert jira_fake.basic(jira_fake.BOT_EMAIL, install.spare[JIRA]) in [
        r.authorization for r in install.jira.requests
    ]
    _assert_read_only(install)


# Unknown: never accepted without an authenticated read ----------------------------------


def test_a_transport_failure_is_unknown_never_accepted(install: Install) -> None:
    install.gitlab.transport_errors(1000)

    entries = install.entries()

    for key in ((CODE_HOST, GITLAB), (TRACKER, GITLAB_TRACKER)):
        assert entries[key]["authentication"] == "unknown", entries[key]
        assert entries[key]["reason"] == "unavailable"
    assert entries[(CODE_HOST, DC)]["authentication"] == "accepted"


def test_a_missing_repository_is_unknown_not_found(install: Install) -> None:
    # The bound project id no longer names a project on the forge.
    install.gitlab.project_id = 9999
    install.gitlab.project_path = "acme/platform/elsewhere"

    entries = install.entries()

    assert entries[(CODE_HOST, GITLAB)]["authentication"] == "unknown"
    assert entries[(CODE_HOST, GITLAB)]["reason"] == "not_found"


@pytest.mark.parametrize("how", ["missing", "empty", "nul", "directory"])
def test_an_unreadable_file_is_unknown_and_its_old_value_is_not_sent(
    install: Install, how: str
) -> None:
    assert install.entries()[(CODE_HOST, GITLAB)]["authentication"] == "accepted"
    install.volumes[GL_HOST_SLOT].break_with(how, install.spare[GL_HOST_SLOT])
    mark = len(install.gitlab.requests)

    entries = install.entries()

    assert entries[(CODE_HOST, GITLAB)]["authentication"] == "unknown"
    assert entries[(CODE_HOST, GITLAB)]["reason"] == "credential_unreadable"
    assert install.tokens[GL_HOST_SLOT] not in _gitlab_sent(install, mark)
    assert entries[(TRACKER, GITLAB_TRACKER)]["authentication"] == "accepted"

    # Liveness: a valid replacement is accepted on the next report.
    install.volumes[GL_HOST_SLOT].publish(install.spare[GL_HOST_SLOT])
    assert install.entries()[(CODE_HOST, GITLAB)]["authentication"] == "accepted"


# No credential bytes anywhere -------------------------------------------------------------


def test_no_credential_bytes_reach_the_report_or_the_logs(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    bodies = [install.report().text]
    install.volumes[GL_HOST_SLOT].publish(f"rejected-{install.tokens[GL_HOST_SLOT]}")
    install.volumes[JIRA].publish(f"rejected-{install.tokens[JIRA]}")
    install.volumes[DC].break_with("line-break", install.spare[DC])
    install.gitlab.echo_token_in_errors = True
    install.dc.echo_token_in_errors = True
    bodies.append(install.report().text)
    entries = {
        (entry["section"], entry["name"]): entry for entry in json.loads(bodies[-1])["entries"]
    }
    assert entries[(CODE_HOST, GITLAB)]["authentication"] == "rejected"
    assert entries[(CODE_HOST, DC)]["reason"] == "credential_unreadable"

    needles = install.needles()
    for body in bodies:
        assert not [needle for needle in needles if needle in body], body
    assert not [needle for needle in needles if needle in caplog.text]
    _assert_read_only(install)
