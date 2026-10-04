"""The worker branches from, and publishes against, a factory ticket's base (#3095, ADR 0186).

Clone: the credential response carries the WorkItem's recorded base branch and
commit; a fresh workspace clones that branch and pins that commit.

Publication: the job receives BASE_REF and opens its pull request against it;
deterministic-head recovery uses the same base and never asks GitHub for the
repository default branch.
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import threading
import uuid
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_publication_k8s import (
    LINEAGE_BRANCH,
    REVISION_HEAD,
    WRITE_CREDENTIAL,
    _embedded_github_script,
    _job_env,
    _payload,
    _settings,
)
from test_workspace import (
    CLEAN_URL,
    DEPLOYMENT_ID,
    GIT_CREDENTIAL,
    WORKER_AUTH,
    _FakeCommands,
    _limits,
    _StreamingObjectStore,
)

REPO = "acme-corp/acme-bot"
BASE_COMMIT = "e" * 40
REVISION = "f" * 40
THREAD = "1700000000.000100"


# --- Clone ----------------------------------------------------------------------------


class _BaseCredentialClient:
    def __init__(self, workspace: Any, **fields: Any) -> None:
        self._workspace = workspace
        self._fields = fields

    def redeem(self, deployment_id: uuid.UUID, conversation_id: str) -> Any:
        return self._workspace.WorkspaceCredential(
            repo_full_name=REPO,
            clone_url=CLEAN_URL,
            authorization_header=GIT_CREDENTIAL,
            github_html_base="https://github.com",
            **self._fields,
        )


def _preparer(tmp_path: Path, **fields: Any) -> tuple[Any, _FakeCommands]:
    workspace = importlib.import_module("curie_worker.workspace")
    commands = _FakeCommands()
    preparer = workspace.WorkspacePreparer(
        credentials=_BaseCredentialClient(workspace, **fields),
        commands=commands,
        objects=_StreamingObjectStore(),
        scratch_root=tmp_path / "clone-scratch",
        limits=_limits(workspace),
    )
    return preparer, commands


def _call(commands: _FakeCommands, verb: str) -> list[str]:
    return next(call["argv"] for call in commands.calls if verb in call.get("argv", []))


def test_credential_redemption_parses_the_recorded_base() -> None:
    workspace = importlib.import_module("curie_worker.workspace")

    def transport(**_request: Any) -> Any:
        return SimpleNamespace(
            status=200,
            headers={"Cache-Control": "no-store"},
            body=json.dumps(
                {
                    "repo_full_name": REPO,
                    "clone_url": CLEAN_URL,
                    "authorization_header": GIT_CREDENTIAL,
                    "revision": None,
                    "base_branch": "next",
                    "base_commit": BASE_COMMIT,
                }
            ).encode(),
        )

    client = workspace.WorkspaceCredentialClient(
        api_url="https://api.example.com",
        github_api_url="https://api.github.com",
        worker_token=WORKER_AUTH,
        transport=transport,
    )
    redeemed = client.redeem(DEPLOYMENT_ID, THREAD)

    assert redeemed.base_branch == "next"
    assert redeemed.base_commit == BASE_COMMIT


def test_credential_without_a_base_defaults_to_none() -> None:
    workspace = importlib.import_module("curie_worker.workspace")
    credential = workspace.WorkspaceCredential(
        repo_full_name=REPO,
        clone_url=CLEAN_URL,
        authorization_header=GIT_CREDENTIAL,
        github_html_base="https://github.com",
    )

    assert credential.base_branch is None
    assert credential.base_commit is None


def test_fresh_prepare_clones_the_recorded_base_and_pins_its_commit(tmp_path: Path) -> None:
    preparer, commands = _preparer(tmp_path, base_branch="next", base_commit=BASE_COMMIT)

    prepared = preparer.prepare(deployment_id=DEPLOYMENT_ID, thread_key=THREAD, generation="g1")

    clone = _call(commands, "clone")
    assert clone[clone.index("--branch") + 1] == "next"
    assert _call(commands, "fetch")[-1] == BASE_COMMIT
    assert _call(commands, "checkout") == ["git", "checkout", "--detach", BASE_COMMIT]
    assert prepared.base_sha == BASE_COMMIT


def test_the_recorded_base_commit_beats_the_credential_revision(tmp_path: Path) -> None:
    preparer, commands = _preparer(
        tmp_path, revision=REVISION, base_branch="next", base_commit=BASE_COMMIT
    )

    preparer.prepare(deployment_id=DEPLOYMENT_ID, thread_key=THREAD, generation="g2")

    assert _call(commands, "fetch")[-1] == BASE_COMMIT
    assert _call(commands, "checkout")[-1] == BASE_COMMIT


def test_an_explicit_lineage_base_beats_the_recorded_base(tmp_path: Path) -> None:
    preparer, commands = _preparer(tmp_path, base_branch="next", base_commit=BASE_COMMIT)
    expected_base = "a" * 40

    preparer.prepare_lineage_base(
        deployment_id=DEPLOYMENT_ID,
        thread_key=THREAD,
        generation="g3",
        expected_base=expected_base,
    )

    assert _call(commands, "fetch")[-1] == expected_base
    assert _call(commands, "checkout")[-1] == expected_base


def test_an_explicit_lineage_branch_beats_the_recorded_base_branch(tmp_path: Path) -> None:
    preparer, commands = _preparer(tmp_path, base_branch="next", base_commit=BASE_COMMIT)

    preparer.prepare_lineage(
        deployment_id=DEPLOYMENT_ID,
        thread_key=THREAD,
        generation="g4",
        branch=LINEAGE_BRANCH,
        expected_head="a" * 40,
    )

    clone = _call(commands, "clone")
    assert clone[clone.index("--branch") + 1] == LINEAGE_BRANCH
    assert BASE_COMMIT not in " ".join(
        " ".join(call.get("argv", [])) for call in commands.calls
    )


def test_no_recorded_base_keeps_the_default_branch_clone(tmp_path: Path) -> None:
    preparer, commands = _preparer(tmp_path)

    preparer.prepare(deployment_id=DEPLOYMENT_ID, thread_key=THREAD, generation="g5")

    assert "--branch" not in _call(commands, "clone")


# --- Publication job ---------------------------------------------------------------------


def _publication_k8s() -> Any:
    return importlib.import_module("curie_worker.publication_k8s")


def _resources(base_ref: str | None, **overrides: Any) -> Any:
    module = _publication_k8s()
    payload = replace(_payload(module), base_ref=base_ref, **overrides)
    return module.build_publication_resources(
        payload, credential=WRITE_CREDENTIAL, settings=_settings(module)
    )


def test_the_job_env_carries_the_recorded_base() -> None:
    assert _job_env(_resources("next"))["BASE_REF"] == "next"
    assert _job_env(_resources(None)).get("BASE_REF", "") == ""


def test_the_loop_payload_carries_the_work_base_ref() -> None:
    loop = importlib.import_module("curie_worker.publication_loop")
    from test_publication_loop import _work

    work = replace(_work(loop), base_ref="next")

    payload = loop.PublicationReconciler._payload(work, clean_clone_url=CLEAN_URL)

    assert payload.base_ref == "next"
    assert loop.PublicationWork.__dataclass_fields__["base_ref"].default is None


class _RoutedGitHub(BaseHTTPRequestHandler):
    default_branch = "main"
    pull_base = "next"
    posts: list[dict[str, Any]] = []
    gets: list[str] = []

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _pull(self, base: str) -> dict[str, Any]:
        return {
            "number": 123,
            "node_id": "PR_example_123",
            "html_url": f"https://github.com/{REPO}/pull/123",
            "state": "open",
            "merged": False,
            "title": "Update repository",
            "body": "Approved platform publication.",
            "updated_at": "2026-10-01T00:00:00Z",
            "head": {"ref": LINEAGE_BRANCH, "sha": REVISION_HEAD, "repo": {"full_name": REPO}},
            "base": {"ref": base, "repo": {"full_name": REPO}},
        }

    def do_GET(self) -> None:
        type(self).gets.append(self.path)
        path = self.path.split("?", 1)[0]
        if path == f"/repos/{REPO}":
            self._send(200, {"id": 9001, "full_name": REPO, "default_branch": self.default_branch})
        elif path == f"/repos/{REPO}/pulls":
            self._send(200, [])
        elif path == f"/repos/{REPO}/pulls/123":
            self._send(200, self._pull(type(self).pull_base))
        else:
            self._send(404, {"message": "missing fixture"})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        type(self).posts.append(body)
        self._send(201, self._pull(body["base"]))

    def log_message(self, _format: str, *args: object) -> None:
        return


def _run_script(
    tmp_path: Path, resources: Any, *, phase: str, expected_head: str
) -> subprocess.CompletedProcess[str]:
    credential = tmp_path / "credential"
    credential.write_text(WRITE_CREDENTIAL)
    _RoutedGitHub.posts = []
    _RoutedGitHub.gets = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RoutedGitHub)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        return subprocess.run(
            ["python3", "-c", _embedded_github_script(resources)],
            env={
                **os.environ,
                **_job_env(resources),
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "CURIE_GITHUB_PHASE": phase,
                "CURIE_EXPECTED_HEAD": expected_head,
                "CURIE_CREDENTIAL_PATH": str(credential),
                "CURIE_PR_FACTS_PATH": str(tmp_path / "pr-facts.json"),
            },
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_the_job_opens_its_pull_request_against_the_recorded_base(tmp_path: Path) -> None:
    completed = _run_script(
        tmp_path, _resources("next"), phase="post-push", expected_head=REVISION_HEAD
    )

    assert completed.returncode == 0, completed.stderr
    assert [post["base"] for post in _RoutedGitHub.posts] == ["next"]
    assert "CURIE_PR_NUMBER=123" in completed.stdout


def test_the_job_without_a_base_still_targets_the_default_branch(tmp_path: Path) -> None:
    completed = _run_script(
        tmp_path, _resources(None), phase="post-push", expected_head=REVISION_HEAD
    )

    assert completed.returncode == 0, completed.stderr
    assert [post["base"] for post in _RoutedGitHub.posts] == ["main"]


def test_a_stored_pull_on_the_recorded_base_validates_before_push(tmp_path: Path) -> None:
    resources = _resources(
        "next", pr_number=123, pr_url=f"https://github.com/{REPO}/pull/123"
    )
    _RoutedGitHub.pull_base = "next"

    completed = _run_script(tmp_path, resources, phase="pre-push", expected_head=REVISION_HEAD)

    assert completed.returncode == 0, completed.stderr
    facts = json.loads((tmp_path / "pr-facts.json").read_text())
    assert facts["base"] == "next"


# --- Deterministic-head recovery ------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_recovery_posts_with_the_given_base_and_never_reads_the_repository(
    anyio_backend: str,
) -> None:
    from curie_worker.publication_clients import GitHubPublicationLookup

    requests: list[httpx.Request] = []
    posted: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path != f"/repos/{REPO}", "repository default branch was read"
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        assert request.url.path == f"/repos/{REPO}/pulls"
        if request.method == "POST":
            body = json.loads(request.content)
            posted.append(body)
            return httpx.Response(
                201,
                json={
                    "number": 123,
                    "html_url": f"https://github.com/{REPO}/pull/123",
                    "state": "open",
                    "merged_at": None,
                    "title": body["title"],
                    "body": body["body"],
                    "head": {
                        "ref": LINEAGE_BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": body["base"], "repo": {"full_name": REPO}},
                },
            )
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await GitHubPublicationLookup(client).recover_pr_by_head(
            REPO,
            LINEAGE_BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer rotated-installation-token",
            base="next",
        )

    assert recovered is not None
    assert recovered.number == 123
    assert [body["base"] for body in posted] == ["next"]
    assert all(request.url.path != f"/repos/{REPO}" for request in requests)


# --- Real git: the recorded commit, not the advanced branch tip -------------------------------
#
# SubprocessCommands scrubs the environment and allows only https, so this port
# delegates to it after pointing the server-derived GitHub URL at a local bare
# repository. Everything else (clone, fetch, checkout, set-url, archive) is real git.


def _git(*args: str, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    )
    return completed.stdout.strip()


class _LocalOriginCommands:
    def __init__(self, workspace: Any, origin: Path) -> None:
        self._real = workspace.SubprocessCommands()
        self._origin = origin
        self.calls: list[list[str]] = []

    def require(self, executable: str) -> None:
        self._real.require(executable)

    def run(
        self,
        argv: Any,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> Any:
        args = [str(part) for part in argv]
        self.calls.append(args)
        if "clone" in args or "fetch" in args:
            args = [str(self._origin) if part == CLEAN_URL else part for part in args]
            env = {**(env or {}), "GIT_ALLOW_PROTOCOL": "file"}
        return self._real.run(args, cwd=cwd, env=env, timeout_seconds=timeout_seconds)


def _origin(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """A bare origin with main, next (advanced after admission) and a lineage branch."""

    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    _git("init", "--bare", "--initial-branch=main", str(origin), cwd=tmp_path)
    _git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=origin)
    _git("init", "--initial-branch=main", str(work), cwd=tmp_path)
    for key, value in (("user.name", "t"), ("user.email", "t@example.com")):
        _git("config", key, value, cwd=work)

    def commit(text_value: str) -> str:
        (work / "VERSION").write_text(text_value + "\n")
        _git("add", "VERSION", cwd=work)
        _git("commit", "-m", text_value, cwd=work)
        return _git("rev-parse", "HEAD", cwd=work)

    shas = {"main": commit("main")}
    _git("checkout", "-b", "next", cwd=work)
    shas["recorded"] = commit("next-at-admission")
    shas["advanced"] = commit("next-advanced")
    _git("checkout", "-b", LINEAGE_BRANCH, shas["recorded"], cwd=work)
    shas["lineage"] = commit("lineage-head")
    _git("remote", "add", "origin", str(origin), cwd=work)
    _git("push", "origin", "main", "next", LINEAGE_BRANCH, cwd=work)
    return origin, shas


def _real_preparer(tmp_path: Path, origin: Path, **fields: Any) -> tuple[Any, Any]:
    workspace = importlib.import_module("curie_worker.workspace")
    objects = _StreamingObjectStore()
    preparer = workspace.WorkspacePreparer(
        credentials=_BaseCredentialClient(workspace, **fields),
        commands=_LocalOriginCommands(workspace, origin),
        objects=objects,
        scratch_root=tmp_path / "clone-scratch",
        limits=_limits(workspace),
    )
    return preparer, objects


def _version_in(objects: Any, key: str) -> str:
    from test_workspace import _archive_members

    members = _archive_members(objects.objects[key])
    (version,) = [body for name, body in members.items() if name.endswith("VERSION")]
    return version.decode().strip()


def test_real_clone_pins_the_recorded_commit_after_the_branch_advanced(tmp_path: Path) -> None:
    origin, shas = _origin(tmp_path)
    preparer, objects = _real_preparer(
        tmp_path, origin, base_branch="next", base_commit=shas["recorded"]
    )

    prepared = preparer.prepare(deployment_id=DEPLOYMENT_ID, thread_key=THREAD, generation="r1")

    assert prepared.base_sha == shas["recorded"]
    assert prepared.materialized_head == shas["recorded"]
    assert _version_in(objects, prepared.object_key) == "next-at-admission"


def test_real_lineage_head_takes_precedence_over_the_recorded_base(tmp_path: Path) -> None:
    origin, shas = _origin(tmp_path)
    preparer, objects = _real_preparer(
        tmp_path, origin, base_branch="next", base_commit=shas["recorded"]
    )

    prepared = preparer.prepare_lineage(
        deployment_id=DEPLOYMENT_ID,
        thread_key=THREAD,
        generation="r2",
        branch=LINEAGE_BRANCH,
        expected_head=shas["lineage"],
    )

    assert prepared.materialized_head == shas["lineage"]
    assert _version_in(objects, prepared.object_key) == "lineage-head"
