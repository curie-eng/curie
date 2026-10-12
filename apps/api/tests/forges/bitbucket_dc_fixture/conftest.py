"""The live Bitbucket Data Center fixture suite (#4401 AC 7 and AC 10).

Runs only when ``CURIE_BITBUCKET_DC_FIXTURE`` names the ``state.json`` of a
fixture brought up with ``curie dev forge-fixture bitbucket-dc up``; otherwise
every test here is skipped. When the variable is set, a fixture that does not
answer fails the suite instead of skipping it.

Test-side actions (build statuses, comments as ``reviewer`` or ``outsider``,
merges) go through the fixture's own helpers,
``tools/forge-fixtures/bitbucket-dc/helpers.py``.
"""

from __future__ import annotations

import importlib.util
import os
import ssl
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx
import pytest
from curie_api.code_host_trust import verify_for_bundle
from curie_api.forges import types
from curie_api.forges.config import CodeHostConfig
from curie_api.forges.types import Actor, RepositoryRef
from forge_fakes.bitbucket_dc import code_host_entry, forges_document

STATE_ENV = "CURIE_BITBUCKET_DC_FIXTURE"
REPO_ROOT = Path(__file__).resolve().parents[5]
HELPERS = REPO_ROOT / "tools" / "forge-fixtures" / "bitbucket-dc" / "helpers.py"
CODE_HOST_NAME = "bitbucket-dc"


def _load_helpers() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bitbucket_dc_fixture_helpers", HELPERS)
    if spec is None or spec.loader is None:
        pytest.fail(f"cannot load the fixture helpers at {HELPERS}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def helpers() -> ModuleType:
    if not os.environ.get(STATE_ENV):
        pytest.skip(f"{STATE_ENV} is unset; bring the fixture up to run this suite")
    return _load_helpers()


@pytest.fixture(scope="session")
def state(helpers: ModuleType) -> dict[str, Any]:
    path = Path(os.environ[STATE_ENV])
    try:
        loaded: dict[str, Any] = helpers.load_state(path)
    except (OSError, ValueError) as error:
        pytest.fail(f"{STATE_ENV} does not name a readable fixture state.json: {error}")
    context = ssl.create_default_context(cafile=loaded["ca_file"])
    try:
        status = httpx.get(f"{loaded['base_url']}/status", verify=context, timeout=15.0)
        running = status.status_code == 200 and status.json().get("state") == "RUNNING"
    except (httpx.HTTPError, ValueError) as error:
        pytest.fail(f"the fixture at {loaded['base_url']} does not answer: {error}")
    if not running:
        pytest.fail(f"the fixture at {loaded['base_url']} is not RUNNING")
    return loaded


@pytest.fixture(autouse=True)
def _fixture_required(state: dict[str, Any]) -> None:
    """Every test here needs the fixture: skipped without it, failed if it is down."""


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def token(state: dict[str, Any]) -> str:
    return Path(state["token_file"]).read_text().strip()


def code_host_document(state: dict[str, Any], token: str) -> dict[str, Any]:
    return code_host_entry(
        name=CODE_HOST_NAME, api_url=state["base_url"], host=state["host"], token=token
    )


@pytest.fixture
def code_host_config(state: dict[str, Any], token: str) -> CodeHostConfig:
    return CodeHostConfig.model_validate(code_host_document(state, token))


@pytest.fixture
def adapter(state: dict[str, Any], code_host_config: CodeHostConfig) -> Iterator[Any]:
    """The DC adapter built by its connection, trusting the fixture CA."""

    from curie_api.forges.bitbucket_dc.connection import BitbucketDcConnection

    connection = BitbucketDcConnection(code_host_config, ca_bundle=state["ca_file"])
    client = httpx.AsyncClient(verify=verify_for_bundle(state["ca_file"]), timeout=15.0)
    yield connection.adapter(client)


@pytest.fixture
def repository(state: dict[str, Any]) -> RepositoryRef:
    return RepositoryRef(
        types.BITBUCKET_DC,
        state["host"],
        str(state["repo_id"]),
        state["repo_path"],
        default_branch=state["default_branch"],
    )


class BitbucketDcLiveBackend:
    """The live fixture as the shared Bitbucket lifecycle drives it
    (`forge_fakes.bitbucket_lifecycle.Backend`), plus what the adapter suite
    needs: git pushes with the factory token and raw reads for assertions."""

    kind = types.BITBUCKET_DC
    code_host_name = CODE_HOST_NAME

    def __init__(self, state: dict[str, Any], token: str, helpers: ModuleType, work: Path) -> None:
        self.state = state
        self.token = token
        self.helpers = helpers
        self._work = work
        self._clone: Path | None = None
        self.files: dict[str, str] = {}
        self.opened: list[int] = []

    # Backend ---------------------------------------------------------------

    def forges_document(self) -> dict[str, Any]:
        return forges_document(
            code_host=code_host_document(self.state, self.token),
            project_id=str(self.state["repo_id"]),
            path=self.state["repo_path"],
            reviewers=(str(self.state["users"]["reviewer"]["user_id"]),),
        )

    def adapter_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(verify=verify_for_bundle(self.state["ca_file"]), timeout=15.0)

    def repository(self) -> RepositoryRef:
        return RepositoryRef(
            types.BITBUCKET_DC,
            self.state["host"],
            str(self.state["repo_id"]),
            self.state["repo_path"],
            default_branch=self.state["default_branch"],
        )

    def push(self, branch: str, message: str) -> str:
        clone = self._ensure_clone()
        self._git("fetch", "origin", cwd=clone)
        remote = self._git("ls-remote", "--heads", "origin", branch, cwd=clone).strip()
        start = f"origin/{branch}" if remote else f"origin/{self.state['default_branch']}"
        self._git("checkout", "-B", branch, start, cwd=clone)
        name = f"{branch.replace('/', '-')}.txt"
        path = clone / name
        path.write_text(path.read_text() + f"{message}\n" if path.exists() else f"{message}\n")
        self.files[branch] = name
        self._git("add", name, cwd=clone)
        self._git(
            "-c",
            "user.name=placeholder-user-1",
            "-c",
            "user.email=placeholder-user-1@example.invalid",
            "commit",
            "-m",
            message,
            cwd=clone,
        )
        self._git("push", "origin", f"HEAD:refs/heads/{branch}", cwd=clone)
        return self._git("rev-parse", "HEAD", cwd=clone).strip()

    def post_status(self, sha: str, key: str, state: str) -> None:
        self.helpers.post_build_status(self.state, sha, key, state)

    def actor(self, who: str) -> Actor:
        user = self.state["users"][who]
        return Actor(str(user["user_id"]), user["slug"])

    def comment_as(
        self, who: str, pr: str, body: str, *, inline: bool, parent: str | None = None
    ) -> str:
        anchor = None
        if inline and parent is None:
            anchor = self.anchor(int(pr))
        return str(
            self.helpers.comment(
                self.state,
                who,
                int(pr),
                body,
                anchor=anchor,
                parent=None if parent is None else int(parent),
            )
        )

    def thread_replies(self, pr: str, root: str) -> list[tuple[str, str]]:
        found = self.comment(int(pr), int(root))
        replies: list[tuple[str, str]] = []

        def walk(comment: dict[str, Any]) -> None:
            for reply in comment.get("comments", []):
                replies.append((str(reply["author"]["id"]), reply["text"]))
                walk(reply)

        walk(found)
        return replies

    # Suite helpers ---------------------------------------------------------

    def anchor(self, pr: int) -> dict[str, Any]:
        """An inline anchor on the file the pull request's branch changed."""

        head = self.helpers.pull_request(self.state, pr)["fromRef"]["displayId"]
        return {
            "path": self.files[head],
            "line": 1,
            "lineType": "ADDED",
            "fileType": "TO",
            "diffType": "EFFECTIVE",
        }

    def rest(self, path: str, *, markup: bool = False) -> Any:
        """A GET under ``rest/api/latest`` as the factory token."""

        url = f"{self.state['base_url']}/rest/api/latest/{path.lstrip('/')}"
        context = ssl.create_default_context(cafile=self.state["ca_file"])
        response = httpx.get(
            url,
            params={"markup": "true"} if markup else None,
            headers={"Authorization": f"Bearer {self.token}"},
            verify=context,
            timeout=15.0,
        )
        response.raise_for_status()
        return response.json()

    def comment(self, pr: int, comment_id: int, *, markup: bool = False) -> dict[str, Any]:
        key, slug = self.state["project_key"], self.state["repo_slug"]
        found: dict[str, Any] = self.rest(
            f"projects/{key}/repos/{slug}/pull-requests/{pr}/comments/{comment_id}",
            markup=markup,
        )
        return found

    def branch(self, prefix: str) -> str:
        return f"curie-test/{prefix}-{uuid.uuid4().hex[:8]}"

    def clone_url(self) -> str:
        return (
            f"{self.state['base_url']}/scm/{self.state['project_key']}/"
            f"{self.state['repo_slug']}.git"
        )

    def decline_opened(self) -> None:
        for pr in self.opened:
            if self.helpers.pull_request(self.state, pr)["state"] == "OPEN":
                self.helpers.decline(self.state, pr)

    def _ensure_clone(self) -> Path:
        if self._clone is None:
            target = self._work / "clone"
            self._git("clone", self.clone_url(), str(target), cwd=self._work)
            self._clone = target
        return self._clone

    def _git(self, *args: str, cwd: Path) -> str:
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(self.helpers.git_env(self.state, self.token))
        env["GIT_TERMINAL_PROMPT"] = "0"
        done = subprocess.run(
            ["git", *args], cwd=cwd, env=env, capture_output=True, text=True, check=False
        )
        if done.returncode != 0:
            # The token travels only in the environment, never in argv or output.
            pytest.fail(f"git {args[0]} failed: {done.stderr.replace(self.token, '***')}")
        return done.stdout


@pytest.fixture
def live(
    state: dict[str, Any], token: str, helpers: ModuleType, tmp_path: Path
) -> Iterator[BitbucketDcLiveBackend]:
    backend = BitbucketDcLiveBackend(state, token, helpers, tmp_path)
    yield backend
    backend.decline_opened()
