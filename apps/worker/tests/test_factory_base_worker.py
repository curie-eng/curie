"""The worker branches from a factory ticket's base (#3095, ADR 0186).

Clone: the credential response carries the WorkItem's recorded base branch and
commit; a fresh workspace clones that branch and pins that commit.

Publication against that base is the API's: the publication Job only pushes,
and the API opens or finds the pull request against the WorkItem's recorded
base (ADR 0197; apps/api/tests/test_publication_code_host_routes.py).
"""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_publication_k8s import LINEAGE_BRANCH
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
            origin="https://github.com",
            header_form="authorization_basic",
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
                    "origin": "https://github.com",
                    "header_form": "authorization_basic",
                    "revision": None,
                    "base_branch": "next",
                    "base_commit": BASE_COMMIT,
                }
            ).encode(),
        )

    client = workspace.WorkspaceCredentialClient(
        api_url="https://api.example.com",
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
        origin="https://github.com",
        header_form="authorization_basic",
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
