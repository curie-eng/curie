"""Supervisor configuration, read once from the shell's own environment.

Every value comes from the deployment (the shell task definition). The control
credential is removed from ``os.environ`` as it is read, and the runner child
never inherits the supervisor environment: its environment is built name by
name in ``stage.child_environment``.
"""

from __future__ import annotations

import os
import shlex
import stat
import uuid
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path

from .protocol import DEFAULT_PORT

MIN_CONTROL_TOKEN_LENGTH = 32
# Inherited by the child whatever the deployment lists: the image's runtime
# setup, never a credential.
BASE_CHILD_ENV = (
    "PATH",
    "LANG",
    "LC_ALL",
    "TZ",
    "SSL_CERT_FILE",
    "PYTHONUNBUFFERED",
    "CURIE_PROC_DUMPABLE_PRELOAD",
)
# Only a root supervisor may scrub what the child left outside its claim
# directory, because only then is everything the child uid owns the child's.
ROOT_SCRUB_PATHS = ("/tmp", "/var/tmp", "/dev/shm", "/home/runner")
RUNNER_UID = 1000


class ConfigError(ValueError):
    """The shell environment cannot start a supervisor."""


def check_private(path: Path, *, child_uid: int | None, child_gid: int | None) -> None:
    """Refuse a credential file the runner child's uid or gid could read.

    The TLS key and a control credential file are the supervisor's alone. A
    deployment that mounts them world readable, or owned by the runner's uid
    or group, would hand them to every claim, so the shell does not start.
    """

    if child_uid is None:
        return
    info = path.stat()
    readable = (
        info.st_uid == child_uid
        or info.st_mode & stat.S_IROTH
        or (info.st_mode & stat.S_IRGRP and info.st_gid == child_gid)
    )
    if readable:
        raise ConfigError(f"{path} is readable by the runner child; make it private to root")


def _names(raw: str | None) -> tuple[str, ...]:
    return tuple(name.strip() for name in (raw or "").split(",") if name.strip())


def _positive(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number of seconds") from None
    if not 0 < value < 86_400:
        raise ConfigError(f"{name} must be between 0 and one day")
    return value


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer") from None
    if value <= 0:
        raise ConfigError(f"{name} must be positive")
    return value


@dataclass(frozen=True)
class SupervisorConfig:
    slot: str
    incarnation: str
    tls_cert_file: Path
    tls_key_file: Path
    control_token: str = field(repr=False)
    state_root: Path
    runner_command: tuple[str, ...]
    listen_host: str = "0.0.0.0"  # noqa: S104 - the shell's one ingress port
    port: int = DEFAULT_PORT
    child_port: int = 8080
    child_uid: int | None = None
    child_gid: int | None = None
    workspace_path: Path = Path("/workspace")
    attachments_path: Path = Path("/attachments")
    # Supervisor env names the child receives. Secret names are withheld from
    # an executor claim unless it names them; public names always pass.
    child_env_secret: tuple[str, ...] = ()
    child_env_public: tuple[str, ...] = ()
    scrub_paths: tuple[Path, ...] = ()
    max_stage_bytes: int = 2 * 1024**3
    max_stage_members: int = 200_000
    settle_timeout_s: float = 15.0
    stop_grace_s: float = 10.0
    ready_poll_s: float = 0.2

    @classmethod
    def from_env(cls, env: MutableMapping[str, str]) -> SupervisorConfig:
        """Build from ``CURIE_SUPERVISOR_*``; pops the control credential."""

        slot = env.get("CURIE_SUPERVISOR_SLOT", "").strip()
        if not slot:
            raise ConfigError("CURIE_SUPERVISOR_SLOT names this shell's slot")
        token = env.pop("CURIE_SUPERVISOR_CONTROL_TOKEN", "")
        token_file = env.get("CURIE_SUPERVISOR_CONTROL_TOKEN_FILE")
        if not token and token_file:
            token = Path(token_file).read_text(encoding="utf-8").strip()
        if len(token) < MIN_CONTROL_TOKEN_LENGTH:
            raise ConfigError(
                "CURIE_SUPERVISOR_CONTROL_TOKEN (or _FILE) must hold a control "
                f"credential of at least {MIN_CONTROL_TOKEN_LENGTH} characters"
            )
        cert = env.get("CURIE_SUPERVISOR_TLS_CERT_FILE")
        key = env.get("CURIE_SUPERVISOR_TLS_KEY_FILE")
        if not cert or not key:
            raise ConfigError(
                "CURIE_SUPERVISOR_TLS_CERT_FILE and CURIE_SUPERVISOR_TLS_KEY_FILE are "
                "required: the shell serves HTTPS only"
            )
        root = os.geteuid() == 0
        uid_raw = env.get("CURIE_SUPERVISOR_CHILD_UID")
        gid_raw = env.get("CURIE_SUPERVISOR_CHILD_GID")
        child_uid = int(uid_raw) if uid_raw else (RUNNER_UID if root else None)
        child_gid = int(gid_raw) if gid_raw else (child_uid if child_uid is not None else None)
        if child_uid is not None and not root:
            raise ConfigError("only a root supervisor can run the child as another uid")
        if child_uid == 0:
            raise ConfigError("the runner child never runs as root")
        scrub_raw = env.get("CURIE_SUPERVISOR_SCRUB_PATHS")
        scrub = (
            tuple(Path(path) for path in _names(scrub_raw))
            if scrub_raw is not None
            else (tuple(Path(path) for path in ROOT_SCRUB_PATHS) if child_uid is not None else ())
        )
        command = env.get("CURIE_SUPERVISOR_RUNNER_COMMAND", "python -m curie_runner")
        argv = tuple(shlex.split(command))
        if not argv:
            raise ConfigError("CURIE_SUPERVISOR_RUNNER_COMMAND is empty")
        public = _names(env.get("CURIE_SUPERVISOR_CHILD_ENV_PUBLIC"))
        secret = _names(env.get("CURIE_SUPERVISOR_CHILD_ENV"))
        if any(name.startswith("CURIE_SUPERVISOR_") for name in (*public, *secret)):
            raise ConfigError("supervisor settings are never forwarded to the child")
        return cls(
            slot=slot,
            incarnation=env.get("CURIE_SUPERVISOR_INCARNATION") or uuid.uuid4().hex,
            tls_cert_file=Path(cert),
            tls_key_file=Path(key),
            control_token=token,
            state_root=Path(env.get("CURIE_SUPERVISOR_STATE_ROOT", "/var/lib/curie-shell")),
            runner_command=argv,
            listen_host=env.get("CURIE_SUPERVISOR_LISTEN_HOST", "0.0.0.0"),  # noqa: S104
            port=_int(env, "CURIE_SUPERVISOR_PORT", DEFAULT_PORT),
            child_port=_int(env, "CURIE_SUPERVISOR_CHILD_PORT", 8080),
            child_uid=child_uid,
            child_gid=child_gid,
            workspace_path=Path(env.get("CURIE_SUPERVISOR_WORKSPACE_PATH", "/workspace")),
            attachments_path=Path(env.get("CURIE_SUPERVISOR_ATTACHMENTS_PATH", "/attachments")),
            child_env_secret=secret,
            child_env_public=public,
            scrub_paths=scrub,
            max_stage_bytes=_int(env, "CURIE_SUPERVISOR_MAX_STAGE_BYTES", 2 * 1024**3),
            max_stage_members=_int(env, "CURIE_SUPERVISOR_MAX_STAGE_MEMBERS", 200_000),
            settle_timeout_s=_positive(env, "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS", 15.0),
            stop_grace_s=_positive(env, "CURIE_SUPERVISOR_STOP_GRACE_SECONDS", 10.0),
        )
