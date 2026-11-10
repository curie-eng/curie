"""The live GitLab CE 19.4.1 fixture suite (#4399 decision 25, AC 5 to 7).

Runs only when ``CURIE_GITLAB_FIXTURE`` names the ``state.json`` of a fixture
brought up with ``curie dev forge-fixture gitlab up``; otherwise every test here
is skipped. When the variable is set, a fixture that does not answer fails the
suite instead of skipping it.

The adapters under test are the production ones: a `ForgesConfig` built through
`build_registry` with the fixture CA as the install's code host bundle, then
``binding_tracker(...).for_binding`` and ``code_host(...).adapter`` over the
connection's own client. The factory's identity is the seeded service account
token. People act through the root token with a ``Sudo: <username>`` header.
Each test makes its own issues, branches and merge requests under unique names,
so the suite runs again on the same fixture.
"""

from __future__ import annotations

import json
import os
import ssl
import subprocess
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import Actor, Credential, RepositoryRef, TrackerIssueRef
from forge_fakes.gitlab import code_host_entry, tracker_entry

STATE_ENV = "CURIE_GITLAB_FIXTURE"
TRACKER_NAME = "gitlab"
CODE_HOST_NAME = "gitlab"
BINDING_NAME = "widgets"
TIMEOUT_S = 30.0


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def state() -> dict[str, Any]:
    raw = os.environ.get(STATE_ENV)
    if not raw:
        pytest.skip(f"{STATE_ENV} is unset; bring the fixture up to run this suite")
    try:
        loaded: dict[str, Any] = json.loads(Path(raw).read_text())
        token = Path(loaded["root_token_file"]).read_text().strip()
        api_url, ca_path = loaded["local_api_url"], loaded["ca_path"]
    except (OSError, ValueError, KeyError) as error:
        pytest.fail(f"{STATE_ENV} does not name a readable fixture state.json: {error}")
    try:
        answer = httpx.get(
            f"{api_url}/version",
            headers={"PRIVATE-TOKEN": token},
            verify=ssl.create_default_context(cafile=ca_path),
            timeout=15.0,
        )
        version = answer.json().get("version") if answer.status_code == 200 else None
    except (httpx.HTTPError, ValueError) as error:
        pytest.fail(f"the fixture at {api_url} does not answer: {type(error).__name__}")
    if version is None:
        pytest.fail(f"the fixture at {api_url} answered /version with {answer.status_code}")
    return loaded


@pytest.fixture(autouse=True)
def _fixture_required(state: dict[str, Any]) -> None:
    """Every test here needs the fixture: skipped without it, failed if it is down."""


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class GitLabApi:
    """Synchronous REST v4 calls as one token, optionally acting as another user."""

    def __init__(self, state: Mapping[str, Any], token_file: str) -> None:
        self.state = state
        self.root = state["local_api_url"].rstrip("/")
        self._token = Path(token_file).read_text().strip()
        self._verify = ssl.create_default_context(cafile=state["ca_path"])

    def __repr__(self) -> str:
        return f"GitLabApi(root={self.root!r})"

    def request(
        self,
        method: str,
        path: str,
        *,
        sudo: str | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
    ) -> httpx.Response:
        headers = {"PRIVATE-TOKEN": self._token}
        if sudo is not None:
            headers["Sudo"] = sudo
        return httpx.request(
            method,
            f"{self.root}{path}",
            headers=headers,
            params=params,
            json=json,
            verify=self._verify,
            timeout=TIMEOUT_S,
        )

    def call(self, method: str, path: str, *, sudo: str | None = None, **kwargs: Any) -> Any:
        """The decoded answer; fails the test on a non-2xx status."""

        response = self.request(method, path, sudo=sudo, **kwargs)
        if not 200 <= response.status_code < 300:
            pytest.fail(f"{method} {path} as {sudo or 'token user'}: {response.status_code}")
        return response.json() if response.content else None


@dataclass
class Fixture:
    """The live fixture: its state, the root API, and the factory's adapters."""

    state: dict[str, Any]
    root: GitLabApi
    bot: GitLabApi
    registry: ForgeRegistry
    work: Path

    # Identities ------------------------------------------------------------

    @property
    def host(self) -> str:
        return urlsplit(self.state["local_api_url"]).netloc

    @property
    def project_id(self) -> int:
        return int(self.state["projects"]["widgets"]["id"])

    @property
    def project(self) -> str:
        return f"/projects/{self.project_id}"

    def actor(self, who: str) -> Actor:
        user = self.state["users"][who]
        return Actor(str(user["id"]), user["username"])

    def username(self, who: str) -> str:
        return str(self.state["users"][who]["username"])

    def repository(self, name: str = "widgets") -> RepositoryRef:
        project = self.state["projects"][name]
        return RepositoryRef(
            types.GITLAB,
            self.host,
            str(project["id"]),
            project["path"],
            default_branch=project["default_branch"],
        )

    def issue_ref(self, iid: int) -> TrackerIssueRef:
        path = self.state["projects"]["widgets"]["path"]
        return TrackerIssueRef(
            types.GITLAB, self.host, str(self.project_id), str(iid), f"{path}#{iid}"
        )

    # Adapters ----------------------------------------------------------------

    def client(self) -> httpx.AsyncClient:
        """The tracker connection's own client: the fixture CA, 15 second timeout."""

        return self.registry.binding_tracker(TRACKER_NAME).client()

    def tracker(self, client: httpx.AsyncClient, *, label: str) -> Tracker:
        return self.registry.binding_tracker(TRACKER_NAME).for_binding(
            client, self.registry.binding(BINDING_NAME), label=label
        )

    def code_host(self, client: httpx.AsyncClient) -> CodeHost:
        return self.registry.code_host(CODE_HOST_NAME).adapter(client, repository=self.repository())

    # People's actions ----------------------------------------------------------

    def open_issue(self, title: str, *, who: str = "developer") -> int:
        created = self.root.call(
            "POST",
            f"{self.project}/issues",
            sudo=self.username(who),
            json={"title": title, "description": "Fixture issue."},
        )
        return int(created["iid"])

    def edit_issue(self, iid: int, who: str, change: Mapping[str, Any]) -> None:
        if who == "service_account":
            self.bot.call("PUT", f"{self.project}/issues/{iid}", json=dict(change))
        else:
            self.root.call(
                "PUT", f"{self.project}/issues/{iid}", sudo=self.username(who), json=dict(change)
            )

    def label(self, iid: int, label: str, who: str, *, add: bool = True) -> None:
        self.edit_issue(iid, who, {"add_labels" if add else "remove_labels": label})

    def close_issue(self, iid: int, who: str = "developer") -> None:
        self.edit_issue(iid, who, {"state_event": "close"})

    def issue_note(self, iid: int, body: str, who: str = "developer") -> int:
        created = self.root.call(
            "POST",
            f"{self.project}/issues/{iid}/notes",
            sudo=self.username(who),
            json={"body": body},
        )
        return int(created["id"])

    def label_events(self, iid: int) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = self.root.call(
            "GET", f"{self.project}/issues/{iid}/resource_label_events", params={"per_page": 100}
        )
        return found

    def state_events(self, iid: int) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = self.root.call(
            "GET", f"{self.project}/issues/{iid}/resource_state_events", params={"per_page": 100}
        )
        return found

    def issue(self, iid: int) -> dict[str, Any]:
        found: dict[str, Any] = self.root.call("GET", f"{self.project}/issues/{iid}")
        return found

    # Git ---------------------------------------------------------------------

    def git(self, credential: Credential, *args: str, cwd: Path | None = None) -> str:
        """git with the credential's header and the fixture CA, both through the
        environment only: never argv, never a config file."""

        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(
            {
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_SSL_CAINFO": self.state["ca_path"],
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": f"http.{credential.origin}/.extraHeader",
                "GIT_CONFIG_VALUE_0": credential.git_header(),
                "GIT_AUTHOR_NAME": "placeholder-user-1",
                "GIT_AUTHOR_EMAIL": "placeholder-user-1@example.invalid",
                "GIT_COMMITTER_NAME": "placeholder-user-1",
                "GIT_COMMITTER_EMAIL": "placeholder-user-1@example.invalid",
            }
        )
        argv = ["git", *args]
        assert all(credential.secret not in part for part in argv)
        done = subprocess.run(
            argv, cwd=cwd or self.work, env=env, capture_output=True, text=True, check=False
        )
        if done.returncode != 0:
            # The token travels only in the environment, never in argv or output.
            pytest.fail(f"git {args[0]} failed: {done.stderr.replace(credential.secret, '***')}")
        return done.stdout

    def wait(self, what: str, probe: Any, *, timeout_s: float = 300.0, every_s: float = 3.0) -> Any:
        """Poll ``probe()`` until it returns a truthy value; fail after ``timeout_s``."""

        deadline = time.monotonic() + timeout_s
        while True:
            found = probe()
            if found:
                return found
            if time.monotonic() >= deadline:
                pytest.fail(f"timed out after {timeout_s:.0f} s waiting for {what}")
            time.sleep(every_s)


def forges_document(state: Mapping[str, Any], token: str, *, mention: str) -> dict[str, Any]:
    api_url = state["local_api_url"]
    host = urlsplit(api_url).netloc
    expires = f"{state['tokens']['service_account']['expires_at']}T00:00:00Z"
    widgets = state["projects"]["widgets"]
    return {
        "code_hosts": [
            code_host_entry(
                name=CODE_HOST_NAME, api_url=api_url, host=host, token=token, expires_at=expires
            )
        ],
        "trackers": [
            tracker_entry(
                name=TRACKER_NAME,
                api_url=api_url,
                host=host,
                token=token,
                mention=mention,
                expires_at=expires,
            )
        ],
        "bindings": [
            {
                "name": BINDING_NAME,
                "tracker": TRACKER_NAME,
                "tracker_scope_id": str(widgets["id"]),
                "repos": [
                    {
                        "alias": BINDING_NAME,
                        "code_host": CODE_HOST_NAME,
                        "project_id": str(widgets["id"]),
                        "path": widgets["path"],
                    }
                ],
            }
        ],
    }


@pytest.fixture
def fixture(state: dict[str, Any], tmp_path: Path) -> Iterator[Fixture]:
    bot_token_file = state["tokens"]["service_account"]["file"]
    token = Path(bot_token_file).read_text().strip()
    document = forges_document(state, token, mention=state["users"]["service_account"]["username"])
    registry = build_registry(ForgesConfig.model_validate(document), ca_bundle=state["ca_path"])
    yield Fixture(
        state=state,
        root=GitLabApi(state, state["root_token_file"]),
        bot=GitLabApi(state, bot_token_file),
        registry=registry,
        work=tmp_path,
    )


def encode(value: str) -> str:
    return quote(value, safe="")
