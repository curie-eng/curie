"""UI evidence from runner PNG files to one comment on the pull request (AC1).

A black-box path through every changed component: the real runner app serves
a real git checkout, the real worker `RunnerClient` snapshots it and the real
`capture_ui_evidence` fetches the screenshots, the real worker `ApprovalClient`
creates the publication against the real API over ASGI and the real
`relay_ui_evidence` uploads the capture, the publication is approved through
the public API, and the real `UiEvidenceReconciler` publishes through the real
GitHub code host adapter. Only GitHub is faked (`GitHubRepositoryFake`, which
replays recorded GitHub shapes), and only the push Job's half (the head commit,
branch and pull request landing on GitHub) is stood in for, because that Job
is unchanged by this work.

Everything async runs on the TestClient's portal loop, so the API app, the
ASGI transport and the runner's aiohttp server share one event loop.

``test_live_github_round_trip`` runs the same path against real GitHub. It is
skipped unless the caller names a throwaway repository in
``CURIE_UI_EVIDENCE_LIVE_GITHUB_REPO`` and supplies a test App through the
API's own ``GITHUB_APP_ID`` and ``GITHUB_APP_PRIVATE_KEY`` settings.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_api.config import get_settings
from curie_api.forges.github.code_host import PATH_ID_PREFIX
from curie_api.forges.github.comments import GitHubMarkedComments, static_token
from curie_api.forges.hosts import code_host_for
from curie_api.forges.types import PullRequestRef, ReplyTarget
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_api.ui_evidence_comment import ui_evidence_marker
from curie_api.ui_evidence_reconciler import UiEvidenceReconciler
from curie_runner import RunTracer, SideEffectClassifier, workspace_snapshot
from curie_runner import create_app as create_runner_app
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_worker.approvals import ApprovalClient, PublicationCreateRequest
from curie_worker.runner_client import RunnerClient, RunnerWorkspaceSnapshot
from curie_worker.ui_evidence import capture_ui_evidence, relay_ui_evidence
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (
    APP_ID,
    PRIVATE_KEY,
    REPO,
    GitHubRepositoryFake,
    fake_installation_tokens,
)
from test_publications import (  # noqa: F401 - publication_stack is a fixture
    WORKER_HEADERS,
    WORKER_TOKEN,
    _claim_publication_lease,
    _execute,
    _mark_outcome_history_ready,
    _patch_lineage,
    _resolve,
    _rows,
    _workspace_identity,
    publication_stack,
)
from test_ui_evidence_reconciler import png_bytes

LIVE_REPO_ENV = "CURIE_UI_EVIDENCE_LIVE_GITHUB_REPO"
GITHUB = "https://github.com"
RUNNER_TOKEN = "runner-token-value"
TITLE = "Publish the agent change"
BODY = "Carries the agent's change and its screenshots stay out of it."
CHANNEL = "C0EXAMPLE1"
API_BASE = "http://api.example.com"
IDENTITY = {
    "GIT_AUTHOR_NAME": "Acme Bot",
    "GIT_AUTHOR_EMAIL": "acme-bot@example.com",
    "GIT_COMMITTER_NAME": "Acme Bot",
    "GIT_COMMITTER_EMAIL": "acme-bot@example.com",
    "GIT_AUTHOR_DATE": "2026-11-03T12:00:00+00:00",
    "GIT_COMMITTER_DATE": "2026-11-03T12:00:00+00:00",
}
_BASE_FILES = {
    ".gitignore": b".curie/ui-evidence/\n",
    "README.md": b"Acme bot\n",
    "src/app.py": b"TITLE = 'Home'\n",
}


# --- the checkout -----------------------------------------------------------------


def _git(repo: Path, *args: str, env: dict[str, str] | None = None) -> str:
    merged = {**os.environ, **IDENTITY, "GIT_CONFIG_NOSYSTEM": "1", **(env or {})}
    merged.pop("GIT_DIR", None)
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", *args],
        cwd=repo,
        env=merged,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write(repo: Path, path: str, data: bytes) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


class Checkout:
    """A real agent checkout: base P, an uncommitted change, two screenshots in
    the gitignored evidence directory and the manifest naming them."""

    def __init__(self, root: Path, repo_path: str, *, clone_header: str | None = None) -> None:
        """A fresh repository whose P commits ``_BASE_FILES``; with
        ``clone_header`` (an ``Authorization`` value), a shallow clone of the
        repository's default branch instead, so P is its current tip and the
        evidence directory is ignored through ``.git/info/exclude``."""

        self.root = root / "workspace"
        self.repo_path = repo_path
        origin = f"{GITHUB}/{repo_path}.git"
        if clone_header is None:
            self.root.mkdir(parents=True)
            _git(self.root, "init", "--quiet")
            _git(self.root, "remote", "add", "origin", origin)
            for path, data in _BASE_FILES.items():
                _write(self.root, path, data)
            _git(self.root, "add", "-A")
            _git(self.root, "commit", "--quiet", "-m", "Prepared base")
        else:
            # The header is passed per command, so no credential is stored.
            _git(
                root,
                "-c",
                f"http.extraHeader=Authorization: {clone_header}",
                "clone",
                "--quiet",
                "--depth",
                "1",
                origin,
                str(self.root),
            )
            _write(self.root, ".git/info/exclude", b".curie/ui-evidence/\n")
        self.base_sha = _git(self.root, "rev-parse", "HEAD")
        self.base_tree = _git(self.root, "rev-parse", "HEAD^{tree}")
        # The agent's change, left uncommitted as a turn leaves it.
        self.changed = {
            "README.md": b"Acme bot\n\nNow with a settings page.\n",
            "src/settings.py": b"TITLE = 'Settings'\n",
        }
        for path, data in self.changed.items():
            _write(self.root, path, data)
        self.before = png_bytes("end-to-end before")
        self.after = png_bytes("end-to-end after")
        _write(self.root, ".curie/ui-evidence/before/home.png", self.before)
        _write(self.root, ".curie/ui-evidence/after/home.png", self.after)
        self.candidate_tree = self._candidate_tree(root)
        manifest = {
            "version": 1,
            "status": "complete",
            "base_sha": self.base_sha,
            "candidate_dirty": True,
            "candidate_tree": self.candidate_tree,
            "images": [
                {"path": "before/home.png", "label": "Home page", "state": "before"},
                {"path": "after/home.png", "label": "Home page", "state": "after"},
            ],
        }
        _write(self.root, ".curie/ui-evidence/manifest.json", json.dumps(manifest).encode())

    def _candidate_tree(self, root: Path) -> str:
        """C, computed independently of runner code: P's index plus every
        working tree change that is not ignored, written as a tree."""

        index = {"GIT_INDEX_FILE": str(root / "candidate.index")}
        _git(self.root, "read-tree", "HEAD", env=index)
        _git(self.root, "add", "-A", env=index)
        return _git(self.root, "write-tree", env=index)

    def head_commit(self, publication_id: str) -> tuple[str, str]:
        """The commit the push Job makes: tree C on parent P with the revision
        trailer. Returns its sha and message."""

        message = f"{TITLE}\n\nCurie-Revision: {publication_id}\n"
        sha = _git(
            self.root, "commit-tree", self.candidate_tree, "-p", self.base_sha, "-m", message
        )
        return sha, message


# --- the runner, worker and API -------------------------------------------------------


def _session_runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="ui-evidence-end-to-end",
    )


async def _capture(checkout: Checkout) -> tuple[RunnerWorkspaceSnapshot, Any]:
    """The kernel's two runner reads, in its order: the publication snapshot,
    then the evidence capture against the same base."""

    runner = _session_runner()
    await runner.start()
    app = create_runner_app(
        runner,
        token=RUNNER_TOKEN,
        snapshotter=workspace_snapshot.workspace_snapshotter(
            checkout.root,
            trusted_origin=GITHUB,
            repository_path=checkout.repo_path,
            proposal=lambda: (TITLE, BODY),
        ),
        ui_evidence_workspace=checkout.root,
    )
    server = TestServer(app)
    await server.start_server()
    client = RunnerClient(total_timeout_s=30.0)
    base_url = f"http://127.0.0.1:{server.port}"
    try:
        snapshot = await client.snapshot(base_url, token=RUNNER_TOKEN, base_sha=checkout.base_sha)
        capture = await capture_ui_evidence(
            client,
            base_url,
            RUNNER_TOKEN,
            base_sha=snapshot.base_sha,
            remaining_s=600.0,
        )
        return snapshot, capture
    finally:
        await client.close()
        await server.close()


def _payload(deployment_id: str, repo: str, base_sha: str) -> dict[str, Any]:
    conversation = f"evidence-{uuid.uuid4().hex[:8]}"
    return {
        "deployment_id": deployment_id,
        "conversation_id": conversation,
        "repo_full_name": repo,
        "author": "U0REQUEST1",
        "reply_kind": "slack",
        "reply_channel": CHANNEL,
        "base_sha": base_sha,
    }


def _deployment(client: TestClient, auth_headers: dict[str, str], repo: str) -> str:
    suffix = uuid.uuid4().hex[:8]
    agent = client.post(
        "/agents",
        json={
            "name": f"evidence-{suffix}",
            "channel": {"kind": "slack", "address": CHANNEL},
            "repo_full_name": repo,
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    version = client.post(
        f"/agents/{agent.json()['id']}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        json={
            "agent_id": agent.json()["id"],
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=auth_headers,
    )
    assert deployment.status_code == 201, deployment.text
    return str(deployment.json()["id"])


def _select_workspace(client: TestClient, payload: dict[str, Any]) -> None:
    selected = client.post(
        f"/v1/internal/workspaces/{payload['deployment_id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text


async def _create_and_relay(
    client: TestClient,
    payload: dict[str, Any],
    snapshot: RunnerWorkspaceSnapshot,
    capture: Any,
) -> tuple[str, str]:
    """The kernel's approval half: create the publication, then relay evidence.
    Returns the publication and approval ids."""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=client.app), base_url=API_BASE
    ) as api:
        creator = ApprovalClient(
            api_base_url=API_BASE,
            api_key=get_settings().api_key,
            client=api,
            read_timeout_s=30.0,
            worker_token=WORKER_TOKEN,
        )
        created = await creator.create_publication(
            PublicationCreateRequest(
                deployment_id=uuid.UUID(payload["deployment_id"]),
                conversation_id=payload["conversation_id"],
                repo_full_name=payload["repo_full_name"],
                author=payload["author"],
                summary=TITLE,
                reply_kind=payload["reply_kind"],
                reply_channel=payload["reply_channel"],
                reply_placeholder="1700000000.000001",
                reply_endpoint=None,
                reply_adapter=None,
                dedupe_key=f"publish-{uuid.uuid4().hex}",
                base_sha=snapshot.base_sha,
                patch=snapshot.patch,
                changed_paths=snapshot.changed_paths,
                expires_in_seconds=600,
                title=snapshot.publication_title,
                body=snapshot.publication_body,
            )
        )
        await relay_ui_evidence(creator, created.id, capture)
        return created.id, created.approval_id


async def _reconcile(http: httpx.AsyncClient) -> None:
    engine = create_async_engine(get_settings().database_url)
    try:
        await UiEvidenceReconciler(
            async_sessionmaker(engine, expire_on_commit=False),
            get_settings(),
            http,
            interval_seconds=10,
        ).run_once()
    finally:
        await engine.dispose()


def _publication_facts(publication_id: str) -> dict[str, Any]:
    return _rows(
        "SELECT p.status, p.result_url, p.lineage_id, p.base_sha, l.branch, l.pr_number, "
        "l.pr_url, l.head_sha, l.status AS lineage_status, l.version AS lineage_version "
        "FROM curie.publications p JOIN curie.thread_publication_lineages l "
        "ON l.id = p.lineage_id WHERE p.id = :id",
        {"id": uuid.UUID(publication_id)},
    )[0]


def _code_pr(publication_id: str) -> dict[str, Any]:
    facts = _publication_facts(publication_id)
    return {
        key: facts[key]
        for key in ("status", "result_url", "pr_number", "pr_url", "head_sha", "lineage_status")
    }


def _record_lineage(
    client: TestClient,
    publication_id: str,
    *,
    head_sha: str,
    pr_number: int,
    pr_url: str,
    state: str = "open",
) -> None:
    facts = _publication_facts(publication_id)
    owner = "evidence-push-job"
    lease = (_claim_publication_lease(publication_id, owner), owner)
    recorded = _patch_lineage(
        client,
        publication_id,
        int(facts["lineage_version"]),
        facts["head_sha"],
        head_sha,
        state,
        pr_number,
        pr_url,
        lease,
    )
    assert recorded.status_code == 200, recorded.text


def _assert_no_evidence_in_the_code_change(snapshot: RunnerWorkspaceSnapshot) -> None:
    assert b".curie/ui-evidence" not in snapshot.patch
    assert not [path for path in snapshot.changed_paths if path.startswith(".curie/")]
    assert set(snapshot.changed_paths) == {"README.md", "src/settings.py"}


def _image_contents(publication_id: str) -> list[Any]:
    return [
        row["content"]
        for row in _rows(
            "SELECT content FROM curie.publication_ui_evidence_images "
            "WHERE publication_id = :id ORDER BY ordinal",
            {"id": uuid.UUID(publication_id)},
        )
    ]


# --- hermetic ---------------------------------------------------------------------------


@pytest.fixture
def evidence_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UI_EVIDENCE_RECONCILE_INTERVAL_S", "0")
    # The App the fake authors comments as, so comment ownership holds.
    monkeypatch.setenv("GITHUB_APP_ID", APP_ID)
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", PRIVATE_KEY)
    fake_installation_tokens(monkeypatch)


def test_png_fixtures_reach_one_bound_comment_on_the_published_pull_request(
    evidence_api: None,
    publication_stack: tuple[TestClient, str],  # noqa: F811
    clean_db: None,
    auth_headers: dict[str, str],
    tmp_path: Path,
) -> None:
    client, _ = publication_stack
    checkout = Checkout(tmp_path, REPO)
    fake = GitHubRepositoryFake()
    fake.seed_commit(
        checkout.base_sha, parents=[], message="Prepared base", tree=checkout.base_tree
    )
    fake.branches[fake.default_branch] = checkout.base_sha
    # Every GitHub call the API makes goes to the fake, never the network.
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handle))
    original = client.app.state.http_client
    client.app.state.http_client = http
    try:
        _drive_hermetic(client, auth_headers, checkout, fake, http)
    finally:
        client.app.state.http_client = original
        client.portal.call(http.aclose)


def _drive_hermetic(
    client: TestClient,
    auth_headers: dict[str, str],
    checkout: Checkout,
    fake: GitHubRepositoryFake,
    http: httpx.AsyncClient,
) -> None:
    snapshot, capture = client.portal.call(_capture, checkout)
    _assert_no_evidence_in_the_code_change(snapshot)
    assert capture is not None

    payload = _payload(_deployment(client, auth_headers, REPO), REPO, checkout.base_sha)
    _select_workspace(client, payload)
    publication_id, approval_id = client.portal.call(
        _create_and_relay, client, payload, snapshot, capture
    )
    assert _resolve(client, auth_headers, approval_id).status_code == 200

    # The unchanged push Job's half: H (tree C on P) lands, with its branch and
    # pull request, and its result is recorded through the real route.
    head_sha, message = checkout.head_commit(publication_id)
    fake.seed_commit(
        head_sha, parents=[checkout.base_sha], message=message, tree=checkout.candidate_tree
    )
    facts = _publication_facts(publication_id)
    lineage_id = uuid.UUID(str(facts["lineage_id"]))
    fake.branches[facts["branch"]] = head_sha
    number = fake.seed_pull(facts["branch"], fake.default_branch, title=TITLE, body=BODY)
    pr_url = f"{GITHUB}/{REPO}/pull/{number}"
    pull = json.loads(json.dumps(fake.pulls[number]))
    _record_lineage(client, publication_id, head_sha=head_sha, pr_number=number, pr_url=pr_url)
    _mark_outcome_history_ready(publication_id)
    code_pr = _code_pr(publication_id)
    assert code_pr["status"] == "succeeded" and code_pr["result_url"] == pr_url

    client.portal.call(_reconcile, http)

    marker = f"<!-- {ui_evidence_marker(lineage_id)} -->"
    assert marker == f"<!-- curie-ui-evidence:{lineage_id} -->"
    comments = [c for c in fake.comments if c["_number"] == number and marker in c["body"]]
    assert len(comments) == 1, [c["body"] for c in fake.comments]
    body = comments[0]["body"]
    assert f"**Pull request head:** `{head_sha}` (revision 1)" in body
    assert f"**Base:** `{checkout.base_sha}`" in body
    assert "**Outcome:** complete" in body
    assert "#### Before" in body and "#### After" in body

    evidence = fake.evidence_ref(str(lineage_id))
    assert evidence is not None
    assert fake.commits[evidence]["parents"] == []
    before_url = f"{GITHUB}/{REPO}/raw/{evidence}/before/0000.png"
    after_url = f"{GITHUB}/{REPO}/raw/{evidence}/after/0001.png"
    assert f"]({before_url})" in body and f"]({after_url})" in body
    assert fake.raw(evidence, "before/0000.png") == checkout.before
    assert fake.raw(evidence, "after/0001.png") == checkout.after
    served = fake.handle(httpx.Request("GET", after_url))
    assert served.status_code == 200 and served.content == checkout.after

    # The code pull request is exactly what the publication recorded.
    assert _code_pr(publication_id) == code_pr
    assert fake.pulls[number] == pull
    assert fake.branches[facts["branch"]] == head_sha
    assert _image_contents(publication_id) == [None, None]


# --- live --------------------------------------------------------------------------------


@pytest.fixture
def live_repo() -> str:
    repo = os.environ.get(LIVE_REPO_ENV, "").strip()
    if not repo:
        pytest.skip(f"set {LIVE_REPO_ENV} to a throwaway repository to run against GitHub")
    for name in ("GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY"):
        if not os.environ.get(name, "").strip():
            pytest.skip(f"the live run needs the test App's {name}")
    return repo


@pytest.fixture
def live_stack(
    live_repo: str, _disposable_db: Any, clean_db: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, str]]:
    # ``live_repo`` comes first so an unconfigured run skips before any stack.
    repo = live_repo
    monkeypatch.setenv("UI_EVIDENCE_RECONCILE_INTERVAL_S", "0")
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", json.dumps([repo]))
    monkeypatch.setenv("RUNS_STREAM", f"test:curie:evidence-live:{uuid.uuid4().hex}")
    get_settings.cache_clear()
    _RESOLVERS.clear()
    with TestClient(create_app()) as test_client:
        yield test_client, repo
    _RESOLVERS.clear()
    get_settings.cache_clear()


class _LiveGitHub:
    """The push Job's half and the observations, through GitHub's REST API with
    the test App's installation token. Every ref and pull request it creates is
    recorded at once, so cleanup finds it whatever fails afterwards."""

    def __init__(self, http: httpx.AsyncClient, repo: str, token: str) -> None:
        self._http = http
        self.repo = repo
        self.token = token
        self.refs: list[str] = []
        self.pulls: list[int] = []

    async def call(
        self, method: str, path: str, *, body: Any = None, accept: str | None = None
    ) -> httpx.Response:
        return await self._http.request(
            method,
            f"https://api.github.com/repos/{self.repo}{path}",
            json=body,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": accept or "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30.0,
        )

    async def commit_over(
        self, base_tree: str, files: dict[str, bytes], *, parent: str, message: str
    ) -> str:
        """``files`` laid over ``base_tree`` as one commit on ``parent``, the way
        the push Job's commit lands; returns the commit."""

        entries = []
        for path, content in sorted(files.items()):
            blob = await self.call(
                "POST",
                "/git/blobs",
                body={"content": base64.b64encode(content).decode(), "encoding": "base64"},
            )
            assert blob.status_code == 201, blob.text
            entries.append(
                {"path": path, "mode": "100644", "type": "blob", "sha": blob.json()["sha"]}
            )
        tree = await self.call("POST", "/git/trees", body={"base_tree": base_tree, "tree": entries})
        assert tree.status_code == 201, tree.text
        stamp = {
            "name": IDENTITY["GIT_AUTHOR_NAME"],
            "email": IDENTITY["GIT_AUTHOR_EMAIL"],
            "date": IDENTITY["GIT_AUTHOR_DATE"],
        }
        commit = await self.call(
            "POST",
            "/git/commits",
            body={
                "message": message,
                "tree": tree.json()["sha"],
                "parents": [parent],
                "author": stamp,
                "committer": stamp,
            },
        )
        assert commit.status_code == 201, commit.text
        return str(commit.json()["sha"])

    async def branch(self, name: str, sha: str) -> None:
        ref = {"ref": f"refs/heads/{name}", "sha": sha}
        created = await self.call("POST", "/git/refs", body=ref)
        assert created.status_code == 201, created.text
        self.refs.append(f"heads/{name}")

    async def cleanup(self, extra_refs: list[str]) -> list[str]:
        """Close every pull request and delete every ref; returns what failed."""

        failures: list[str] = []
        for number in self.pulls:
            closed = await self.call("PATCH", f"/pulls/{number}", body={"state": "closed"})
            if closed.status_code != 200:
                failures.append(f"close pull {number}: {closed.status_code}")
        for ref in [*extra_refs, *self.refs]:
            deleted = await self.call("DELETE", f"/git/refs/{ref}")
            if deleted.status_code not in {204, 404, 422}:
                failures.append(f"delete {ref}: {deleted.status_code}")
        return failures


def test_live_github_round_trip(
    live_stack: tuple[TestClient, str], auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """The hermetic path against a real throwaway repository and test App.

    P is the default branch's current tip (a shallow clone). The pull request
    targets the default branch, the base the commit proof records, as
    production does; that branch is only read and named as a base, never
    pushed to or changed. H lands on its own publication branch through the
    Git Data API, and the result is recorded through the same lineage PATCH
    route the hermetic test uses.
    """

    client, repo = live_stack
    settings = get_settings()
    http: httpx.AsyncClient = client.app.state.http_client
    host = code_host_for(settings, http)
    live: _LiveGitHub | None = None
    lineage_id: uuid.UUID | None = None
    reconciled_closed = False
    gone: httpx.Response | None = None
    failures: list[str] = []
    try:
        header = client.portal.call(host._token, repo)  # noqa: SLF001 - its own credential
        _, _, encoded = header.partition(" ")
        live = _LiveGitHub(http, repo, base64.b64decode(encoded).decode().split(":", 1)[1])
        repository = client.portal.call(host.resolve_repository, f"{PATH_ID_PREFIX}{repo}")
        checkout = Checkout(tmp_path, repo, clone_header=header)
        base_sha = checkout.base_sha
        tip = client.portal.call(
            live.call, "GET", f"/git/ref/heads/{repository.default_branch}"
        )
        assert tip.json()["object"]["sha"] == base_sha

        snapshot, capture = client.portal.call(_capture, checkout)
        _assert_no_evidence_in_the_code_change(snapshot)
        payload = _payload(_deployment(client, auth_headers, repo), repo, base_sha)
        _select_workspace(client, payload)
        publication_id, approval_id = client.portal.call(
            _create_and_relay, client, payload, snapshot, capture
        )
        facts = _publication_facts(publication_id)
        lineage_id = uuid.UUID(str(facts["lineage_id"]))
        assert _resolve(client, auth_headers, approval_id).status_code == 200

        _, message = checkout.head_commit(publication_id)
        head_sha = client.portal.call(
            lambda: live.commit_over(
                checkout.base_tree, checkout.changed, parent=base_sha, message=message
            )
        )
        pushed = client.portal.call(live.call, "GET", f"/git/commits/{head_sha}")
        assert pushed.json()["tree"]["sha"] == checkout.candidate_tree
        client.portal.call(live.branch, facts["branch"], head_sha)
        pull = client.portal.call(
            lambda: host.open_pull_request(
                repository,
                head_ref=facts["branch"],
                base_ref=repository.default_branch,
                title=TITLE,
                body=BODY,
            )
        )
        live.pulls.append(int(pull.ref.number))
        _record_lineage(
            client,
            publication_id,
            head_sha=head_sha,
            pr_number=int(pull.ref.number),
            pr_url=pull.url,
        )
        _mark_outcome_history_ready(publication_id)
        code_pr = _code_pr(publication_id)

        client.portal.call(_reconcile, http)

        comments = GitHubMarkedComments(
            http,
            api="https://api.github.com",
            host="github.com",
            repo_full_name=repo,
            repository_id=int(repository.project_id),
            token=static_token(live.token),
            app_id=settings.github_app_id,
        )
        target = ReplyTarget(
            "pull_request", pull_request=PullRequestRef(repository, pull.ref.number)
        )
        found = client.portal.call(comments.find_marked, target, ui_evidence_marker(lineage_id))
        assert found is not None
        assert f"`{head_sha}` (revision 1)" in found.body
        assert f"`{base_sha}`" in found.body
        evidence = client.portal.call(live.call, "GET", f"/git/ref/curie/ui-evidence/{lineage_id}")
        assert evidence.status_code == 200, evidence.text
        commit = evidence.json()["object"]["sha"]
        for path, expected in (
            ("before/0000.png", checkout.before),
            ("after/0001.png", checkout.after),
        ):
            assert f"{GITHUB}/{repo}/raw/{commit}/{path}" in found.body
            served = client.portal.call(
                lambda path=path: live.call(
                    "GET", f"/contents/{path}?ref={commit}", accept="application/vnd.github.raw"
                )
            )
            assert served.status_code == 200 and served.content == expected
        assert _code_pr(publication_id) == code_pr

        # The pull request ends; the lineage reconciler's record of it lets the
        # evidence reconciler delete its ref on its own.
        closed = client.portal.call(
            lambda: live.call("PATCH", f"/pulls/{pull.ref.number}", body={"state": "closed"})
        )
        assert closed.status_code == 200, closed.text
        _execute(
            "UPDATE curie.thread_publication_lineages SET status = 'closed' WHERE id = :id",
            {"id": lineage_id},
        )
        client.portal.call(_reconcile, http)
        reconciled_closed = True
        gone = client.portal.call(live.call, "GET", f"/git/ref/curie/ui-evidence/{lineage_id}")
    finally:
        if live is not None:
            evidence_refs = [f"curie/ui-evidence/{lineage_id}"] if lineage_id else []
            failures = client.portal.call(live.cleanup, evidence_refs)
    assert not failures, failures
    if reconciled_closed:
        assert gone is not None and gone.status_code == 404
