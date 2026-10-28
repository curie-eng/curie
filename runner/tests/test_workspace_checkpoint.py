"""POST /v1/checkpoint: a trusted-base capture of the managed checkout (#5138).

Every test boots the actual fake-model runner from ``build_runner`` on a
synthetic git checkout and drives the real authenticated route served by
``create_app``. Nothing in the capture implementation is imported or patched.
The one coordination seam is a ``git`` wrapper script placed first on PATH: the
capture runs git by argv name in a worker thread of this same process, and the
runner's subprocess env (``shell_and_hook_env``) keeps PATH, so the wrapper sees
every git call and can act on a chosen subcommand before exec'ing real git.

Covered:

1. Restore: tracked, staged, untracked, binary, deleted, executable and
   local-commit changes against the requested base are captured, the original
   checkout is destroyed, and the patch applied to an independently retained
   copy of the base reproduces every file and mode; the manifest agrees.
2. Authority: missing, wrong and other-claim bearers, an unauthenticated
   malformed body, a tokenless app and a workspace-less boot are refused.
3. Refusals: malformed bodies, an active turn, unavailable or non-commit bases
   (including a shallow clone) and unsafe entries (symlink and gitlink changes,
   nested repositories, special files), each with a live positive control.
4. Bounds: the patch and manifest caps at their exact byte boundaries.
5. A real subprocess writer racing the capture yields no checkpoint.
6. Deadlines during git and during hashing return a distinct timeout, leave no
   scratch directory or child process behind and keep the git state intact.
7. The workspace index, refs, HEAD and object store are untouched by a
   success, a refusal and a timeout.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import shlex
import shutil
import stat
import subprocess
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.__main__ import build_runner
from curie_runner.config import RunnerConfig
from curie_runner.server import bind_status_attestation
from curie_runner.session import SessionRunner

PATH = "/v1/checkpoint"
TOKEN = "checkpoint-token-claim-a"
OTHER_TOKEN = "checkpoint-token-claim-b"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
MAX_PATCH = 900_000
MAX_MANIFEST = 1_048_576
SUCCESS_KEYS = {
    "base_sha",
    "patch_base64",
    "patch_size_bytes",
    "patch_sha256",
    "changed_paths",
    "manifest_base64",
    "manifest_size_bytes",
    "manifest_sha256",
}
SCRATCH_PREFIX = "curie-checkpoint-"
_EVENT_FRAME = {"kind": "event", "type": "message", "text": "hi", "user": "U", "ts": "1"}

# Resolved before any test puts a wrapper first on PATH, so fixture setup never
# goes through a wrapper.
REAL_GIT = shutil.which("git") or "git"
_GIT_CONFIG = (
    "-c",
    "user.name=t",
    "-c",
    "user.email=t@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "-c",
    "core.hooksPath=/dev/null",
)


# --------------------------------------------------------------------------- #
# Synthetic repositories
# --------------------------------------------------------------------------- #


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        [REAL_GIT, *_GIT_CONFIG, *args], cwd=repo, check=True, capture_output=True
    )
    return result.stdout.decode("utf-8").strip()


def _init(repo: Path) -> Path:
    repo.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    return repo


def _commit_all(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _simple_repo(repo: Path) -> tuple[Path, str]:
    """A tiny checkout at its base commit with one tracked file edited."""

    _init(repo)
    (repo / "README.md").write_text("base\n")
    (repo / "tracked.txt").write_text("tracked\n")
    base = _commit_all(repo, "base")
    (repo / "README.md").write_text("edited\n")
    return repo, base


def _mode(st_mode: int) -> str:
    return "100755" if st_mode & 0o100 else "100644"


def _tree(root: Path, *, skip: frozenset[str] = frozenset()) -> dict[str, tuple[str, int, str]]:
    """sha256, size and git mode of every regular file outside ``.git``."""

    files: dict[str, tuple[str, int, str]] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        if Path(dirpath) == root:
            dirnames[:] = [name for name in dirnames if name != ".git"]
        for name in filenames:
            path = Path(dirpath) / name
            rel = path.relative_to(root).as_posix()
            if rel in skip:
                continue
            info = path.lstat()
            assert stat.S_ISREG(info.st_mode), rel
            data = path.read_bytes()
            files[rel] = (hashlib.sha256(data).hexdigest(), len(data), _mode(info.st_mode))
    return files


def _git_state(repo: Path) -> dict[str, object]:
    """Every byte under ``.git`` plus the resolved HEAD and ref listing."""

    git_dir = repo / ".git"
    contents = {
        path.relative_to(git_dir).as_posix(): (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "dir"
        )
        for path in sorted(git_dir.rglob("*"))
    }
    return {
        "contents": contents,
        "index": (git_dir / "index").read_bytes(),
        "HEAD": (git_dir / "HEAD").read_bytes(),
        "packed-refs": (
            (git_dir / "packed-refs").read_bytes() if (git_dir / "packed-refs").exists() else None
        ),
        "objects": sorted(
            path.relative_to(git_dir).as_posix() for path in (git_dir / "objects").rglob("*")
        ),
        "rev-parse": _git(repo, "rev-parse", "HEAD"),
        "for-each-ref": _git(repo, "for-each-ref"),
    }


def _scratch_dirs() -> set[str]:
    return {path.name for path in Path("/tmp").glob(f"{SCRATCH_PREFIX}*")}


async def _assert_new_scratch_gone(before: set[str]) -> None:
    """No scratch directory created by this request survives it.

    Another worker may be capturing concurrently, so an entry that appears is
    only a failure if it does not disappear within a short grace period.
    """

    new = _scratch_dirs() - before
    with anyio.fail_after(10):
        while any((Path("/tmp") / name).exists() for name in new):
            await anyio.sleep(0.05)


# --------------------------------------------------------------------------- #
# Runner and HTTP harness
# --------------------------------------------------------------------------- #


def _boot(tmp_path: Path, name: str, workspace: Path | None) -> SessionRunner:
    """The actual fake-model boot path with a managed workspace (or none)."""

    plugin_dir = tmp_path / f"bundle-{name}"
    manifest_dir = plugin_dir / ".claude-plugin"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "plugin.json").write_text(json.dumps({"name": "acme-bot"}), encoding="utf-8")
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": f"session-acme-{name}",
            "CURIE_SANDBOX_ID": f"sandbox-acme-{name}",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    return build_runner(config, fake_model=True, workspace_path=workspace)


@contextlib.asynccontextmanager
async def _serve(runner: SessionRunner, token: str | None = TOKEN) -> AsyncIterator[TestClient]:
    await runner.start()
    async with TestClient(TestServer(create_app(runner, token=token))) as client:
        yield client


def _request(base_sha: str, budget: float = 30) -> dict[str, object]:
    return {"base_sha": base_sha, "budget_seconds": budget}


async def _post(
    client: TestClient,
    *,
    json_body: object = None,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    headers = AUTH if headers is None else headers
    if data is not None:
        response = await client.post(
            PATH, data=data, headers={**headers, "Content-Type": "application/json"}
        )
    else:
        response = await client.post(PATH, json=json_body, headers=headers)
    text = await response.text()
    try:
        body = json.loads(text)
    except ValueError:
        body = {"_raw": text}
    return response.status, body


async def _capture(client: TestClient, base_sha: str, budget: float = 30) -> dict[str, Any]:
    status, body = await _post(client, json_body=_request(base_sha, budget))
    assert status == 200, body
    assert set(body) == SUCCESS_KEYS
    assert body["base_sha"] == base_sha
    assert body["changed_paths"] == sorted(body["changed_paths"])
    return body


async def _refused(
    client: TestClient,
    status: int,
    code: str,
    *,
    json_body: object = None,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    got, body = await _post(client, json_body=json_body, data=data, headers=headers)
    assert (got, body.get("code")) == (status, code), body
    assert isinstance(body.get("error"), str)
    return body


async def _unauthorized(client: TestClient, **kwargs: Any) -> None:
    status, body = await _post(client, **kwargs)
    assert status == 401, body
    assert "code" not in body


def _decode(body: dict[str, Any]) -> tuple[bytes, bytes, dict[str, Any]]:
    """Decode patch and manifest, checking the size and digest fields."""

    patch = base64.b64decode(body["patch_base64"], validate=True)
    manifest_bytes = base64.b64decode(body["manifest_base64"], validate=True)
    assert body["patch_size_bytes"] == len(patch)
    assert body["patch_sha256"] == hashlib.sha256(patch).hexdigest()
    assert body["manifest_size_bytes"] == len(manifest_bytes)
    assert body["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
    manifest = json.loads(manifest_bytes)
    canonical = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert manifest_bytes == canonical
    assert set(manifest) == {"base_sha", "entries", "version"}
    assert manifest["version"] == 1
    assert manifest["base_sha"] == body["base_sha"]
    paths = [entry["path"] for entry in manifest["entries"]]
    assert paths == sorted(paths) == body["changed_paths"]
    return patch, manifest_bytes, manifest


class _HeldSession:
    """One model turn whose SDK wait is released only by the test."""

    def __init__(self) -> None:
        self.entered = anyio.Event()
        self.release = anyio.Event()

    async def connect(self) -> None: ...

    async def query(self, _text: str) -> None: ...

    async def receive_turn(self):  # noqa: ANN201 - async generator shape
        self.entered.set()
        await self.release.wait()
        if False:  # pragma: no cover - retain the async-generator shape
            yield None

    async def interrupt(self) -> None:
        self.release.set()

    async def close(self) -> None:
        self.release.set()


def _turn_runner(workspace: Path) -> tuple[SessionRunner, _HeldSession]:
    """A runner whose turn the test holds open, bound by the production binder."""

    session = _HeldSession()
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    bind_status_attestation(
        runner, session_id="session-acme-turn", sandbox_id="sandbox-acme-turn", cwd=str(workspace)
    )
    return runner, session


# --------------------------------------------------------------------------- #
# PATH wrapper for git
# --------------------------------------------------------------------------- #


def _git_wrapper(bin_dir: Path, *, subcommand: str, action: str) -> str:
    """Write a ``git`` that runs ``action`` on ``subcommand``, then execs real git.

    Returns the PATH value that puts the wrapper first.
    """

    bin_dir.mkdir()
    script = bin_dir / "git"
    script.write_text(
        "#!/bin/sh\n"
        'sub=""\n'
        "skip=0\n"
        'for arg in "$@"; do\n'
        '  if [ "$skip" = 1 ]; then skip=0; continue; fi\n'
        '  case "$arg" in\n'
        "    -c|-C|--git-dir|--work-tree|--namespace) skip=1 ;;\n"
        "    -*) ;;\n"
        '    *) sub="$arg"; break ;;\n'
        "  esac\n"
        "done\n"
        f'if [ "$sub" = {shlex.quote(subcommand)} ]; then\n'
        f"{action}\n"
        "fi\n"
        f'exec {shlex.quote(REAL_GIT)} "$@"\n'
    )
    script.chmod(0o755)
    return f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"


def _hang_action(record: Path) -> str:
    """Start a background child, record pids and scratch env, then wait."""

    q = shlex.quote
    return (
        "  sleep 30 &\n"
        f"  echo \"$!\" > {q(str(record / 'child.pid'))}\n"
        f"  printf '%s\\n%s\\n' \"$GIT_OBJECT_DIRECTORY\" \"$GIT_INDEX_FILE\" "
        f"> {q(str(record / 'scratch'))}\n"
        f"  echo \"$$\" > {q(str(record / 'self.pid'))}\n"
        "  wait\n"
    )


def _pid_gone(pid: int, *, zombie_ok: bool) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    if not zombie_ok:
        return False
    # The background child is reparented away from the runner when its parent
    # dies, so reaping it is the new parent's job; a zombie has already exited.
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except OSError:
        return True
    return state == "Z"


async def _assert_hang_cleaned_up(record: Path) -> None:
    """The hung wrapper and its child are gone and its scratch dir removed."""

    self_pid = int((record / "self.pid").read_text())
    child_pid = int((record / "child.pid").read_text())
    try:
        with anyio.fail_after(3):
            while not (
                _pid_gone(self_pid, zombie_ok=False) and _pid_gone(child_pid, zombie_ok=True)
            ):
                await anyio.sleep(0.05)
    finally:
        # Never leave a 30 s sleeper behind when the assertion above fails.
        if not _pid_gone(child_pid, zombie_ok=True):
            with contextlib.suppress(OSError):
                os.kill(child_pid, 9)
    scratch_roots = set()
    for line in (record / "scratch").read_text().splitlines():
        for parent in (Path(line), *Path(line).parents):
            if parent.name.startswith(SCRATCH_PREFIX):
                scratch_roots.add(parent)
    assert scratch_roots, "git diff ran without the scratch object directory"
    for root in scratch_roots:
        assert not root.exists(), root


# --------------------------------------------------------------------------- #
# AC1: capture and independent restore
# --------------------------------------------------------------------------- #


def test_trusted_base_http_restore(tmp_path: Path) -> None:
    repo = _init(tmp_path / "workspace")
    (repo / "README.md").write_text("base readme\n")
    (repo / "keep.txt").write_text("unchanged\n")
    (repo / "modify.txt").write_text("before local commit\n")
    (repo / "staged.txt").write_text("staged base\n")
    (repo / "delete_me.txt").write_text("going away\n")
    (repo / "script.sh").write_text("#!/bin/sh\necho hi\n")
    (repo / "image.bin").write_bytes(b"\x00\x01base\xff\x00")
    (repo / ".gitignore").write_text("ignored.log\n")
    (repo / "script.sh").chmod(0o644)
    base = _commit_all(repo, "base")

    # Independent retained copy of the base, made before any local change.
    copy = tmp_path / "retained"
    subprocess.run(
        [REAL_GIT, "clone", "-q", "--no-hardlinks", str(repo), str(copy)],
        check=True,
        capture_output=True,
    )
    _git(copy, "checkout", "-q", "--detach", base)
    assert _git(copy, "rev-parse", "HEAD") == base

    # A local commit after the base that adds and modifies files.
    (repo / "added_in_commit.txt").write_text("added by a local commit\n")
    (repo / "modify.txt").write_text("after local commit\n")
    local = _commit_all(repo, "local work")
    assert local != base

    # Working tree and index changes on top of the local commit.
    (repo / "staged.txt").write_text("staged edit\n")
    _git(repo, "add", "staged.txt")
    (repo / "README.md").write_text("unstaged readme edit\n")
    (repo / "image.bin").write_bytes(b"\x00\x02changed\xfe" * 16)
    (repo / "delete_me.txt").unlink()
    (repo / "script.sh").chmod(0o755)
    (repo / "dir" / "nested").mkdir(parents=True)
    (repo / "dir" / "nested" / "new.txt").write_text("untracked text\n")
    (repo / "new.bin").write_bytes(b"\x00untracked\x00binary\xff")
    (repo / "ignored.log").write_text("never captured\n")

    expected = _tree(repo, skip=frozenset({"ignored.log"}))
    base_files = _tree(copy)
    deleted = set(base_files) - set(expected)
    changed = {path for path in expected if base_files.get(path) != expected[path]} | deleted
    assert deleted == {"delete_me.txt"}
    assert {"script.sh", "image.bin", "new.bin", "added_in_commit.txt", "modify.txt"} <= changed
    assert "keep.txt" not in changed

    runner = _boot(tmp_path, "restore", repo)

    async def go() -> dict[str, Any]:
        async with _serve(runner) as client:
            return await _capture(client, base)

    body = anyio.run(go)
    patch, _, manifest = _decode(body)
    assert body["changed_paths"] == sorted(changed)
    assert "ignored.log" not in body["changed_paths"]
    assert b"ignored.log" not in patch
    assert b"never captured" not in patch

    shutil.rmtree(repo)
    assert not repo.exists()

    patch_file = tmp_path / "checkpoint.patch"
    patch_file.write_bytes(patch)
    _git(copy, "apply", "--binary", str(patch_file))

    restored = _tree(copy)
    assert restored == expected
    assert not (copy / "delete_me.txt").exists()
    assert not (copy / "ignored.log").exists()

    for entry in manifest["entries"]:
        path = entry["path"]
        if path in deleted:
            assert entry == {"deleted": True, "path": path}
            continue
        sha256, size, mode = restored[path]
        assert entry == {"mode": mode, "path": path, "sha256": sha256, "size": size}
    assert {entry["path"] for entry in manifest["entries"]} == changed


# --------------------------------------------------------------------------- #
# AC2: authority and refusals
# --------------------------------------------------------------------------- #


def test_checkpoint_authority(tmp_path: Path) -> None:
    repo_a, base_a = _simple_repo(tmp_path / "workspace-a")
    repo_b, base_b = _simple_repo(tmp_path / "workspace-b")
    repo_c, base_c = _simple_repo(tmp_path / "workspace-c")
    runner_a = _boot(tmp_path, "a", repo_a)
    runner_b = _boot(tmp_path, "b", repo_b)
    tokenless_runner = _boot(tmp_path, "c", repo_c)
    no_workspace_runner = _boot(tmp_path, "none", None)

    async def go() -> None:
        async with contextlib.AsyncExitStack() as stack:
            app_a = await stack.enter_async_context(_serve(runner_a, TOKEN))
            app_b = await stack.enter_async_context(_serve(runner_b, OTHER_TOKEN))
            tokenless = await stack.enter_async_context(_serve(tokenless_runner, None))
            no_workspace = await stack.enter_async_context(_serve(no_workspace_runner, TOKEN))

            # Live positive controls: each claim's own bearer is accepted.
            await _capture(app_a, base_a)
            status, _ = await _post(
                app_b,
                json_body=_request(base_b),
                headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
            )
            assert status == 200

            await _unauthorized(app_a, json_body=_request(base_a), headers={})
            await _unauthorized(
                app_a,
                json_body=_request(base_a),
                headers={"Authorization": "Bearer wrong-token-PLACEHOLDER"},
            )
            await _unauthorized(app_a, json_body=_request(base_a), headers={"Authorization": TOKEN})
            # Another claim's real bearer does not open this claim's runner.
            await _unauthorized(
                app_a,
                json_body=_request(base_a),
                headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
            )
            # Authentication precedes body parsing.
            await _unauthorized(app_a, data=b"\xff not json", headers={})
            await _unauthorized(app_a, json_body=["not", "an", "object"], headers={})

            # A tokenless app has no authority to hand out a checkpoint at all.
            await _refused(tokenless, 409, "no_serving_token", json_body=_request(base_c))
            await _refused(
                tokenless, 409, "no_serving_token", json_body=_request(base_c), headers={}
            )
            await _refused(tokenless, 409, "no_serving_token", data=b"garbage", headers={})

            # A boot without a managed workspace has nothing to capture.
            await _refused(no_workspace, 409, "no_managed_workspace", json_body=_request(base_a))
            await _refused(no_workspace, 409, "no_managed_workspace", data=b"garbage")
            await _unauthorized(no_workspace, json_body=_request(base_a), headers={})

            # The positive control still holds after every refusal.
            await _capture(app_a, base_a)

    anyio.run(go)


def test_checkpoint_refusals(tmp_path: Path) -> None:
    # Body validation, base availability and an active turn on a plain repo.
    repo, base = _simple_repo(tmp_path / "workspace")
    tree_sha = _git(repo, "rev-parse", f"{base}^{{tree}}")
    blob_sha = _git(repo, "rev-parse", f"{base}:README.md")
    assert base.upper() != base

    # A two-commit repository and a depth-1 clone that lacks the parent commit.
    origin = _init(tmp_path / "origin")
    (origin / "a.txt").write_text("one\n")
    parent = _commit_all(origin, "one")
    (origin / "a.txt").write_text("two\n")
    head = _commit_all(origin, "two")
    shallow = tmp_path / "shallow"
    subprocess.run(
        [REAL_GIT, "clone", "-q", "--depth", "1", f"file://{origin}", str(shallow)],
        check=True,
        capture_output=True,
    )
    assert (shallow / ".git" / "shallow").exists()
    (shallow / "a.txt").write_text("three\n")

    # Unsafe entries: a tracked symlink and a gitlink in the base commit.
    unsafe = _init(tmp_path / "unsafe")
    (unsafe / "README.md").write_text("base\n")
    (unsafe / "target.txt").write_text("target\n")
    (unsafe / "other.txt").write_text("other\n")
    (unsafe / "pipe.txt").write_text("pipe\n")
    os.symlink("target.txt", unsafe / "link")
    first = _commit_all(unsafe, "files")
    _git(unsafe, "update-index", "--add", "--cacheinfo", f"160000,{first},sub")
    _git(unsafe, "commit", "-q", "-m", "gitlink")
    unsafe_base = _git(unsafe, "rev-parse", "HEAD")
    # An uninitialized submodule checkout is an empty directory.
    (unsafe / "sub").mkdir()
    (unsafe / "README.md").write_text("edited\n")

    # Names containing quote characters, in a separate small repository.
    quoted = _init(tmp_path / "quoted")
    (quoted / '"q.txt').write_text("q base\n")
    quoted_base = _commit_all(quoted, "quoted base")
    (quoted / '"q.txt').write_text("q edited\n")
    (quoted / '"note.txt"').write_text("note\n")
    quoted_runner = _boot(tmp_path, "quoted", quoted)

    runner = _boot(tmp_path, "refusals", repo)
    shallow_runner = _boot(tmp_path, "shallow", shallow)
    unsafe_runner = _boot(tmp_path, "unsafe", unsafe)
    turn_runner, held = _turn_runner(repo)

    async def go() -> None:
        async with contextlib.AsyncExitStack() as stack:
            client = await stack.enter_async_context(_serve(runner))
            shallow_client = await stack.enter_async_context(_serve(shallow_runner))
            unsafe_client = await stack.enter_async_context(_serve(unsafe_runner))
            turn_client = await stack.enter_async_context(_serve(turn_runner))
            quoted_client = await stack.enter_async_context(_serve(quoted_runner))

            # Quote characters in path names survive into paths and digests.
            quoted_body = await _capture(quoted_client, quoted_base)
            assert quoted_body["changed_paths"] == ['"note.txt"', '"q.txt']
            _, _, quoted_manifest = _decode(quoted_body)
            for entry in quoted_manifest["entries"]:
                digest = hashlib.sha256((quoted / entry["path"]).read_bytes()).hexdigest()
                assert entry["sha256"] == digest, entry

            # Malformed bodies.
            await _capture(client, base)
            await _capture(client, base, budget=60)
            await _capture(client, base, budget=30.5)
            await _refused(client, 400, "invalid_request", data=b"{not json")
            await _refused(client, 400, "invalid_request", data=b"")
            malformed: list[object] = [
                [base, 30],
                "a string",
                None,
                {"base_sha": base},
                {"budget_seconds": 30},
                {**_request(base), "extra": 1},
                _request(base[:39]),
                _request(base + "0"),
                _request(base.upper()),
                _request("g" * 40),
                {"base_sha": 123, "budget_seconds": 30},
                {"base_sha": base, "budget_seconds": 0},
                {"base_sha": base, "budget_seconds": -1},
                {"base_sha": base, "budget_seconds": 61},
                {"base_sha": base, "budget_seconds": 60.5},
                {"base_sha": base, "budget_seconds": True},
                {"base_sha": base, "budget_seconds": "5"},
                {"base_sha": base, "budget_seconds": None},
            ]
            for body in malformed:
                await _refused(client, 400, "invalid_request", json_body=body)
            # A budget too large for a float is still a clean 400.
            huge = ('{"base_sha": "%s", "budget_seconds": 1%s}' % (base, "0" * 400)).encode()
            await _refused(client, 400, "invalid_request", data=huge)
            await _capture(client, base)

            # An active turn refuses a well-formed request; a malformed one is
            # still a clean 400. The same runner answers once the turn ends.
            turn = await turn_client.post("/v1/event", json=_EVENT_FRAME, headers=AUTH)
            await held.entered.wait()
            await _refused(turn_client, 409, "turn_active", json_body=_request(base))
            await _refused(
                turn_client, 400, "invalid_request", json_body={"base_sha": base}
            )
            held.release.set()
            await turn.text()
            with anyio.fail_after(5):
                while True:
                    status = await turn_client.get("/v1/status", headers=AUTH)
                    if (await status.json())["turn_active"] is False:
                        break
                    await anyio.sleep(0.01)
            await _capture(turn_client, base)

            # Unavailable or non-commit bases; nothing is fetched or substituted.
            await _refused(client, 422, "base_unavailable", json_body=_request("ab" * 20))
            await _refused(client, 422, "base_unavailable", json_body=_request(tree_sha))
            await _refused(client, 422, "base_unavailable", json_body=_request(blob_sha))
            await _capture(client, base)
            await _refused(shallow_client, 422, "base_unavailable", json_body=_request(parent))
            shallow_body = await _capture(shallow_client, head)
            assert shallow_body["changed_paths"] == ["a.txt"]

            # Unsafe entries, each followed by a live positive control.
            positive = await _capture(unsafe_client, unsafe_base)
            assert positive["changed_paths"] == ["README.md"]

            os.symlink("target.txt", unsafe / "newlink")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            (unsafe / "newlink").unlink()
            await _capture(unsafe_client, unsafe_base)

            (unsafe / "link").unlink()
            os.symlink("other.txt", unsafe / "link")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            (unsafe / "link").unlink()
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            os.symlink("target.txt", unsafe / "link")
            await _capture(unsafe_client, unsafe_base)

            nested = unsafe / "nested"
            nested.mkdir()
            _git(nested, "init", "-q", "-b", "main")
            (nested / "inner.txt").write_text("inner\n")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            shutil.rmtree(nested)
            await _capture(unsafe_client, unsafe_base)

            (unsafe / "pipe.txt").unlink()
            os.mkfifo(unsafe / "pipe.txt")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            (unsafe / "pipe.txt").unlink()
            (unsafe / "pipe.txt").write_text("pipe\n")
            await _capture(unsafe_client, unsafe_base)

            _git(unsafe, "update-index", "--cacheinfo", f"160000,{unsafe_base},sub")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            _git(unsafe, "update-index", "--cacheinfo", f"160000,{first},sub")
            final = await _capture(unsafe_client, unsafe_base)
            assert final["changed_paths"] == ["README.md"]

            # An unchanged gitlink replaced by a file or a symlink is a type
            # change the capture must refuse, not hide behind the index entry.
            (unsafe / "sub").rmdir()
            (unsafe / "sub").write_text("now a file\n")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            (unsafe / "sub").unlink()
            os.symlink("target.txt", unsafe / "sub")
            await _refused(unsafe_client, 422, "unsafe_entry", json_body=_request(unsafe_base))
            (unsafe / "sub").unlink()
            (unsafe / "sub").mkdir()
            restored = await _capture(unsafe_client, unsafe_base)
            assert restored["changed_paths"] == ["README.md"]

    anyio.run(go)


def test_checkpoint_bounds(tmp_path: Path) -> None:
    file_count = 10_000
    repo = _init(tmp_path / "workspace")
    names = [f"f{index:05d}" for index in range(file_count)]
    for name in names:
        (repo / name).write_text("x\n")
    base = _commit_all(repo, "many files")
    runner = _boot(tmp_path, "bounds", repo)

    big = repo / "big.txt"

    def write_big(size: int) -> None:
        big.write_bytes(b"a" * (size - 1) + b"\n")

    pads: list[Path] = []

    def set_manifest_state(executable: int, pad_length: int) -> None:
        for index, name in enumerate(names):
            (repo / name).chmod(0o755 if index < executable else 0o644)
        while pads:
            pads.pop().unlink()
        pad = repo / ("pad" + "a" * pad_length)
        pad.write_bytes(b"")
        pads.append(pad)

    async def go() -> None:
        async with _serve(runner) as client:
            # Patch: a single one-line new file, linear in its byte length.
            write_big(100)
            measured = (await _capture(client, base))["patch_size_bytes"]
            exact = 100 + MAX_PATCH - measured
            write_big(exact)
            at_cap = await _capture(client, base)
            assert at_cap["patch_size_bytes"] == MAX_PATCH
            assert at_cap["changed_paths"] == ["big.txt"]
            write_big(exact + 1)
            await _refused(client, 422, "patch_too_large", json_body=_request(base))
            big.unlink()

            # Manifest: mode-only changes on fixed-width names plus one empty
            # untracked file whose name length is the free variable. Each
            # mode-only entry grows the manifest more than the patch.
            set_manifest_state(0, 1)
            floor = (await _capture(client, base))["manifest_size_bytes"]
            set_manifest_state(1000, 1)
            sample = (await _capture(client, base))["manifest_size_bytes"]
            assert (sample - floor) % 1000 == 0
            per_entry = (sample - floor) // 1000
            executable, remainder = divmod(MAX_MANIFEST - floor, per_entry)
            assert executable <= file_count

            set_manifest_state(executable, 1 + remainder)
            at_cap = await _capture(client, base)
            assert at_cap["manifest_size_bytes"] == MAX_MANIFEST
            assert at_cap["patch_size_bytes"] < MAX_PATCH
            assert len(at_cap["changed_paths"]) == executable + 1
            _decode(at_cap)

            set_manifest_state(executable, 2 + remainder)
            await _refused(client, 422, "manifest_too_large", json_body=_request(base))

    anyio.run(go)


# --------------------------------------------------------------------------- #
# AC3: concurrent writer and deadlines
# --------------------------------------------------------------------------- #


def test_checkpoint_writer_race(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo, base = _simple_repo(tmp_path / "workspace")
    (repo / "untracked.txt").write_text("new\n")
    marker = tmp_path / "raced"
    tracked = repo / "tracked.txt"
    q = shlex.quote
    action = (
        f"  if [ ! -e {q(str(marker))} ]; then\n"
        f"    : > {q(str(marker))}\n"
        f"    /bin/sh -c 'printf raced >> \"$1\"' writer {q(str(tracked))}\n"
        "  fi\n"
    )
    wrapped_path = _git_wrapper(tmp_path / "bin", subcommand="write-tree", action=action)
    runner = _boot(tmp_path, "race", repo)

    async def go() -> None:
        async with _serve(runner) as client:
            # Live control before the wrapper is in place.
            await _capture(client, base)

            monkeypatch.setenv("PATH", wrapped_path)
            await _refused(client, 409, "workspace_changed", json_body=_request(base))
            assert marker.exists()
            assert tracked.read_text() == "tracked\nraced"

            # Same wrapper, writer already spent: the inputs are stable again.
            stable = await _capture(client, base)
            assert "tracked.txt" in stable["changed_paths"]

    anyio.run(go)


def test_checkpoint_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo, base = _simple_repo(tmp_path / "workspace")
    (repo / "untracked.txt").write_text("new\n")
    record = tmp_path / "record"
    record.mkdir()
    original_path = os.environ.get("PATH", "")
    wrapped_path = _git_wrapper(tmp_path / "bin", subcommand="diff", action=_hang_action(record))
    runner = _boot(tmp_path, "deadline", repo)

    async def go() -> None:
        async with _serve(runner) as client:
            await _capture(client, base)

            # A git call outlives the budget.
            state = _git_state(repo)
            scratch_before = _scratch_dirs()
            monkeypatch.setenv("PATH", wrapped_path)
            started = time.monotonic()
            body = await _refused(
                client, 504, "deadline_exceeded", json_body=_request(base, 1.5)
            )
            elapsed = time.monotonic() - started
            monkeypatch.setenv("PATH", original_path)
            assert body["stage"] == "git"
            assert elapsed < 10
            await _assert_hang_cleaned_up(record)
            await _assert_new_scratch_gone(scratch_before)
            assert _git_state(repo) == state
            await _capture(client, base)

            # Hashing outlives the budget: a sparse 32 GiB untracked file.
            huge = repo / "huge.bin"
            with huge.open("wb") as handle:
                handle.truncate(32 * 1024**3)
            try:
                state = _git_state(repo)
                scratch_before = _scratch_dirs()
                started = time.monotonic()
                body = await _refused(
                    client, 504, "deadline_exceeded", json_body=_request(base, 2)
                )
                elapsed = time.monotonic() - started
                assert body["stage"] == "hash"
                assert elapsed < 10
                await _assert_new_scratch_gone(scratch_before)
                assert _git_state(repo) == state
            finally:
                huge.unlink()

            stable = await _capture(client, base)
            assert stable["changed_paths"] == ["README.md", "untracked.txt"]

    anyio.run(go)


def test_checkpoint_preserves_git_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init(tmp_path / "workspace")
    (repo / "README.md").write_text("base\n")
    (repo / "tracked.txt").write_text("tracked\n")
    base = _commit_all(repo, "base")
    (repo / "local.txt").write_text("local commit\n")
    _commit_all(repo, "local")
    _git(repo, "branch", "side", base)
    _git(repo, "tag", "v-base", base)
    _git(repo, "pack-refs", "--all")
    _git(repo, "branch", "loose", base)
    (repo / "tracked.txt").write_text("staged\n")
    _git(repo, "add", "tracked.txt")
    (repo / "README.md").write_text("unstaged\n")
    (repo / "untracked.bin").write_bytes(b"\x00new blob\xff")
    assert (repo / ".git" / "packed-refs").exists()

    record = tmp_path / "record"
    record.mkdir()
    original_path = os.environ.get("PATH", "")
    wrapped_path = _git_wrapper(tmp_path / "bin", subcommand="diff", action=_hang_action(record))
    runner = _boot(tmp_path, "state", repo)

    # Hooks installed after every setup commit, so only the capture could run
    # one. Each would leave a marker outside the repo and move a ref.
    hook_marker = tmp_path / "hook-ran"
    hook = repo / ".git" / "hooks" / "post-index-change"
    hook.parent.mkdir(exist_ok=True)
    hook.write_text(
        "#!/bin/sh\n"
        f": > {shlex.quote(str(hook_marker))}\n"
        f"{shlex.quote(REAL_GIT)} update-ref refs/heads/tripwire HEAD\n"
    )
    hook.chmod(0o755)

    # A split-index repository: the shared index file must not be rewritten.
    split = tmp_path / "split-workspace"
    _simple_repo(split)
    split_base = _git(split, "rev-parse", "HEAD")
    _git(split, "config", "core.splitIndex", "true")
    _git(split, "update-index", "--split-index")
    split_runner = _boot(tmp_path, "split", split)

    async def go() -> None:
        async with _serve(runner) as client:
            before = _git_state(repo)

            success = await _capture(client, base)
            assert "untracked.bin" in success["changed_paths"]
            assert _git_state(repo) == before
            assert not hook_marker.exists()
            assert not (repo / ".git" / "refs" / "heads" / "tripwire").exists()
            assert "tripwire" not in _git(repo, "for-each-ref")

            os.symlink("tracked.txt", repo / "newlink")
            await _refused(client, 422, "unsafe_entry", json_body=_request(base))
            assert _git_state(repo) == before
            (repo / "newlink").unlink()

            monkeypatch.setenv("PATH", wrapped_path)
            body = await _refused(
                client, 504, "deadline_exceeded", json_body=_request(base, 1.5)
            )
            monkeypatch.setenv("PATH", original_path)
            assert body["stage"] == "git"
            await _assert_hang_cleaned_up(record)
            assert _git_state(repo) == before

            again = await _capture(client, base)
            assert again["patch_sha256"] == success["patch_sha256"]
            assert again["manifest_sha256"] == success["manifest_sha256"]
            assert _git_state(repo) == before
            assert not hook_marker.exists()
            assert "tripwire" not in _git(repo, "for-each-ref")

        async with _serve(split_runner) as split_client:
            split_before = _git_state(split)
            shared = [name for name in split_before["contents"] if name.startswith("sharedindex.")]
            assert shared, "fixture lost its shared index"

            captured = await _capture(split_client, split_base)
            assert captured["changed_paths"] == ["README.md"]
            # Same files with the same bytes: no sharedindex.* added or removed.
            assert _git_state(split) == split_before

    anyio.run(go)
    hook.unlink()
