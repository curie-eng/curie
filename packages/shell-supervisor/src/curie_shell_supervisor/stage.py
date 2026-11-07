"""Stage one claim into a fresh directory, and remove every trace of it after.

The worker sends one uncompressed tar: ``claim.json`` (the claim's identity and
its filtered boot env), the extracted plugin bundle, and optionally the
workspace checkout and eager attachments. The worker already fetched and
extracted the bundle with its bounded extractor; this side only unpacks the
transfer archive it authenticated, bounded by size and member count, through
the standard library's ``data`` filter (no links out, no devices, no setuid).

Layout under ``<state_root>/claims/<random>/``::

    stage/bundle       root owned, read only   -> CURIE_PLUGIN_DIR
    stage/workspace    child owned             <- /workspace symlink
    stage/attachments  read only (eager) or child owned (lazy) <- /attachments
    home/              child owned, private    -> HOME
    tmp/               child owned, private    -> TMPDIR
"""

from __future__ import annotations

import contextlib
import json
import os
import secrets
import shutil
import stat
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .config import BASE_CHILD_ENV, SupervisorConfig
from .protocol import (
    ARCHIVE_ROOTS,
    ATTACHMENT_MODES,
    ATTACHMENTS_DIR,
    BUNDLE_DIR,
    MANIFEST_NAME,
    MANIFEST_VERSION,
    RUNNER_TOKEN_ENV,
    WORKSPACE_DIR,
)

PLUGIN_DIR_ENV = "CURIE_PLUGIN_DIR"
SANDBOX_ID_ENV = "CURIE_SANDBOX_ID"
RUNNER_PORT_ENV = "CURIE_RUNNER_PORT"
# Names the supervisor owns in the child environment; a claim env cannot set
# them. The loader and interpreter overrides would otherwise let one claim's
# env change what code the child runs.
_OWNED_ENV = frozenset(
    {
        "HOME",
        "TMPDIR",
        "PATH",
        PLUGIN_DIR_ENV,
        SANDBOX_ID_ENV,
        RUNNER_PORT_ENV,
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONSTARTUP",
    }
)


class StageError(ValueError):
    """The staging archive or its manifest is refused."""


@dataclass(frozen=True)
class Manifest:
    claim: str
    generation: int
    env: Mapping[str, str]
    executor_secret_names: frozenset[str] | None
    withheld_env: frozenset[str]
    attachments: str
    workspace: bool

    @property
    def runner_token(self) -> str:
        return self.env[RUNNER_TOKEN_ENV]


@dataclass(frozen=True)
class StagedClaim:
    root: Path
    bundle: Path
    workspace: Path | None
    attachments: Path | None
    home: Path
    tmp: Path


def _parse_manifest(raw: bytes, claim: str, generation: int) -> Manifest:
    try:
        value = json.loads(raw)
    except ValueError:
        raise StageError("claim manifest is not JSON") from None
    if not isinstance(value, dict) or value.get("v") != MANIFEST_VERSION:
        raise StageError("unsupported claim manifest version")
    env = value.get("env")
    if (
        value.get("claim") != claim
        or value.get("generation") != generation
        or not isinstance(env, dict)
        or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items())
    ):
        raise StageError("claim manifest does not match its claim path")
    if any(name.startswith("CURIE_SUPERVISOR_") or "=" in name or not name for name in env):
        raise StageError("claim env names a supervisor-owned variable")
    if not env.get(RUNNER_TOKEN_ENV):
        raise StageError("claim env carries no runner bearer")
    secrets_raw = value.get("executor_secret_names")
    if secrets_raw is not None and not (
        isinstance(secrets_raw, list) and all(isinstance(name, str) for name in secrets_raw)
    ):
        raise StageError("executor_secret_names must be a list of names")
    withheld = value.get("withheld_env", [])
    if not isinstance(withheld, list) or not all(isinstance(name, str) for name in withheld):
        raise StageError("withheld_env must be a list of names")
    attachments = value.get("attachments", "none")
    if attachments not in ATTACHMENT_MODES or not isinstance(value.get("workspace", False), bool):
        raise StageError("invalid attachment or workspace mode")
    return Manifest(
        claim=claim,
        generation=generation,
        env=dict(env),
        executor_secret_names=None if secrets_raw is None else frozenset(secrets_raw),
        withheld_env=frozenset(withheld),
        attachments=attachments,
        workspace=bool(value.get("workspace", False)),
    )


def unpack(
    archive: Path, dest: Path, *, claim: str, generation: int, config: SupervisorConfig
) -> Manifest:
    """Extract ``archive`` into the empty ``dest``; return its manifest.

    The manifest holds the claim's credentials, so it is read and deleted
    before anything can run with access to ``dest``.
    """

    try:
        with tarfile.open(archive, mode="r:") as tar:
            members = tar.getmembers()
            if len(members) > config.max_stage_members:
                raise StageError("staging archive has too many members")
            total = 0
            for member in members:
                top = member.name.split("/", 1)[0]
                if top not in ARCHIVE_ROOTS:
                    raise StageError("staging archive holds an unexpected entry")
                total += member.size
            if total > config.max_stage_bytes:
                raise StageError("staging archive exceeds the staged size limit")
            tar.extractall(dest, filter="data")
    except tarfile.TarError as exc:
        raise StageError(f"staging archive is unreadable ({type(exc).__name__})") from None
    manifest_path = dest / MANIFEST_NAME
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise StageError("staging archive has no claim manifest")
    raw = manifest_path.read_bytes()
    manifest_path.unlink()
    return _parse_manifest(raw, claim, generation)


def _own(path: Path, uid: int | None, gid: int | None, *, dir_mode: int, file_mode: int) -> None:
    """Give ``path``'s tree to the child, owner-only, never following a link."""

    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            target = Path(root) / name
            if uid is not None:
                os.chown(target, uid, gid if gid is not None else -1, follow_symlinks=False)
            if not target.is_symlink():
                mode = (
                    dir_mode if target.is_dir() else (os.lstat(target).st_mode & 0o100) | file_mode
                )
                os.chmod(target, mode)
    if uid is not None:
        os.chown(path, uid, gid if gid is not None else -1)
    os.chmod(path, dir_mode)


def _read_only(path: Path) -> None:
    """World readable, writable by nobody but root (the supervisor)."""

    for root, dirs, files in os.walk(path):
        for name in dirs:
            os.chmod(Path(root) / name, 0o555)
        for name in files:
            target = Path(root) / name
            if not target.is_symlink():
                os.chmod(target, (os.lstat(target).st_mode & 0o111) | 0o444)
    os.chmod(path, 0o555)


def prepare(
    archive: Path, *, claim: str, generation: int, config: SupervisorConfig
) -> tuple[StagedClaim, Manifest]:
    """Create a fresh claim directory and stage ``archive`` into it."""

    claims = config.state_root / "claims"
    claims.mkdir(parents=True, exist_ok=True, mode=0o711)
    root = claims / secrets.token_hex(16)
    root.mkdir(mode=0o711)
    try:
        stage = root / "stage"
        stage.mkdir(mode=0o711)
        manifest = unpack(archive, stage, claim=claim, generation=generation, config=config)
        uid, gid = config.child_uid, config.child_gid
        bundle = stage / BUNDLE_DIR
        bundle.mkdir(exist_ok=True)
        _read_only(bundle)
        workspace: Path | None = None
        if manifest.workspace:
            workspace = stage / WORKSPACE_DIR
            if not workspace.is_dir():
                raise StageError("manifest names a workspace the archive lacks")
            _own(workspace, uid, gid, dir_mode=0o700, file_mode=0o600)
        attachments: Path | None = None
        if manifest.attachments == "eager":
            attachments = stage / ATTACHMENTS_DIR
            if not attachments.is_dir():
                raise StageError("manifest names attachments the archive lacks")
            _read_only(attachments)
        elif manifest.attachments == "lazy":
            attachments = stage / ATTACHMENTS_DIR
            if attachments.exists():
                raise StageError("a lazy attachment cache starts empty")
            attachments.mkdir()
            _own(attachments, uid, gid, dir_mode=0o700, file_mode=0o600)
        home = root / "home"
        tmp = root / "tmp"
        for private in (home, tmp):
            private.mkdir()
            _own(private, uid, gid, dir_mode=0o700, file_mode=0o600)
    except BaseException:
        wipe(root)
        raise
    return StagedClaim(root, bundle, workspace, attachments, home, tmp), manifest


def child_environment(
    manifest: Manifest,
    staged: StagedClaim,
    *,
    config: SupervisorConfig,
    environ: Mapping[str, str],
) -> dict[str, str]:
    """The child's whole environment, built name by name, never inherited."""

    env = {name: environ[name] for name in BASE_CHILD_ENV if name in environ}
    env.update({name: environ[name] for name in config.child_env_public if name in environ})
    secret_names = set(config.child_env_secret)
    if manifest.executor_secret_names is not None:
        secret_names &= manifest.executor_secret_names
    secret_names -= manifest.withheld_env
    env.update({name: environ[name] for name in secret_names if name in environ})
    env.update({k: v for k, v in manifest.env.items() if k not in _OWNED_ENV})
    env.update(
        {
            "HOME": str(staged.home),
            "TMPDIR": str(staged.tmp),
            PLUGIN_DIR_ENV: str(staged.bundle),
            SANDBOX_ID_ENV: manifest.claim,
            RUNNER_PORT_ENV: str(config.child_port),
        }
    )
    env.setdefault("PATH", os.defpath)
    return env


def link(path: Path, target: Path | None) -> None:
    """Point the fixed runner path at this claim's tree, or remove it."""

    if path.is_symlink():
        path.unlink()
    elif path.exists():
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
        else:
            raise StageError(f"{path} exists and is not the supervisor's link")
    if target is None:
        return
    pending = path.with_name(f".{path.name}.{secrets.token_hex(4)}")
    os.symlink(target, pending)
    os.replace(pending, path)


def _writable(path: str) -> None:
    """Let root list and empty a real directory; never touch a symlink's target."""

    with contextlib.suppress(OSError):
        if stat.S_ISDIR(os.lstat(path).st_mode):
            os.chmod(path, stat.S_IRWXU)


def wipe(path: Path) -> bool:
    """Remove ``path`` entirely, read-only trees included; True when gone."""

    if path.is_symlink() or (path.exists() and not path.is_dir()):
        path.unlink()
        return True
    if not path.exists():
        return True
    for root, dirs, _files in os.walk(path):
        for name in dirs:
            _writable(os.path.join(root, name))
    _writable(str(path))

    def retry(function: object, failed: str, _exc: BaseException) -> None:
        parent = Path(failed).parent
        # Only modes inside the tree being removed change: never its parent,
        # which may be a shared scratch root such as /tmp.
        if parent == path or path in parent.parents:
            _writable(str(parent))
        with contextlib.suppress(FileNotFoundError):
            if os.path.isdir(failed) and not os.path.islink(failed):
                shutil.rmtree(failed, ignore_errors=True)
            else:
                os.unlink(failed)

    shutil.rmtree(path, onexc=retry)
    return not path.exists() and not path.is_symlink()


def scrub(paths: tuple[Path, ...], uid: int | None) -> bool:
    """Remove what the child left in shared writable paths; True when clean.

    With a distinct child uid, every entry that uid owns is the child's. A
    supervisor sharing the child's uid only scrubs paths configured for it,
    and then removes everything in them.
    """

    clean = True
    for base in paths:
        if not base.is_dir():
            continue
        for entry in base.iterdir():
            try:
                owner = entry.lstat().st_uid
            except FileNotFoundError:
                continue
            if uid is not None and owner != uid:
                continue
            if not wipe(entry):
                clean = False
    return clean
