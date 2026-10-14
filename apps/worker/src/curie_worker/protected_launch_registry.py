"""Private single-owner SQLite bookkeeping. @spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-8.

These records issue no authority and authenticate no provider observation. The
future registry-owning server must independently verify every positive operation
and actual termination/UID cleanup before recording its observations here.
Filesystem UID/mode checks are metadata defenses, never qualification or HA.
"""

from __future__ import annotations

import fcntl
import functools
import os
import re
import sqlite3
import stat
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Literal, NoReturn, cast, dataclass_transform
from uuid import UUID, uuid4

State = Literal["preparing", "idle", "waiting", "running", "retiring", "retired", "failed"]
OperationKind = Literal["start", "admit", "deny", "interrupt", "timeout", "retire"]
OperationStatus = Literal["pending", "applied", "rejected", "uncertain"]
_STATES = frozenset({"preparing", "idle", "waiting", "running", "retiring", "retired", "failed"})
_KINDS = frozenset({"start", "admit", "deny", "interrupt", "timeout", "retire"})
_STATUSES = frozenset({"pending", "applied", "rejected", "uncertain"})
_MAX_REVISION = 2**63 - 1


class RegistryError(RuntimeError):
    """Fixed, detached local refusal. @spec PROTECTED-HOOK-LANE-8."""

    def __init__(self) -> None:
        super().__init__("Protected launch registry refused operation")


class RegistryInvalid(RegistryError, TypeError):
    """Noncontract data or unsafe filesystem metadata."""


class RegistryConflict(RegistryError):
    """Changed idempotency tuple, lifecycle or stale revision."""


class RegistryUnauthorized(RegistryError):
    """Missing owned row; never reveals another principal's metadata."""


class RegistryUnavailable(RegistryError):
    """Storage unavailable, closed registry or another writer owns the lock."""


class RegistryRetired(RegistryError):
    """Positive reuse refused after retirement or boot loss."""


def _raise_detached(error: RegistryError) -> NoReturn:
    """Detach even ambient caller context after Python attaches it at raise time."""
    try:
        raise error
    finally:
        error.__cause__ = None
        error.__context__ = None


def _safe[**P, T](function: Callable[P, T]) -> Callable[P, T]:
    """Close error graphs without exposing SQLite, paths or supplied values."""

    @functools.wraps(function)
    def invoke(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return function(*args, **kwargs)
        except RegistryError as error:
            _raise_detached(type(error)())
        except sqlite3.Error:
            _raise_detached(RegistryUnavailable())
        except (OSError, ValueError, TypeError, OverflowError):
            _raise_detached(RegistryInvalid())

    return invoke


@dataclass_transform(frozen_default=True)
def _closed_record[T](cls: type[T]) -> type[T]:
    """Closed immutable metadata, including detached constructor refusals."""
    result = dataclass(frozen=True, slots=True, repr=False)(cls)
    result.__init__ = _safe(result.__init__)  # type: ignore[method-assign]
    return result


def _require(condition: bool) -> None:
    if not condition:
        raise RegistryInvalid()


def _pattern(value: object, pattern: str) -> None:
    _require(type(value) is str and re.fullmatch(pattern, value, flags=re.ASCII) is not None)


def _scalar(value: object, kind: str) -> None:
    """Existing closed UUID/digest/ref/generation grammars, no extensible blobs."""
    if kind == "uuid":
        _require(type(value) is str and str(UUID(value)) == value)
    elif kind == "generation":
        _pattern(value, r"[1-9][0-9]{0,18}")
        _require(int(str(value)) <= _MAX_REVISION)
    elif kind == "digest":
        _pattern(value, r"[0-9a-f]{64}")
    elif kind == "ref":
        _pattern(value, r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}")
    elif kind == "name":
        _pattern(value, r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
    elif kind == "version":
        _pattern(value, r"[!-~]{1,256}")
    elif kind == "container":
        _pattern(value, r"(?:containerd|cri-o|docker)://[0-9a-f]{64}")
    elif kind == "epoch":
        _pattern(value, r"[A-Za-z0-9_-]{32,256}")
    else:
        raise RegistryInvalid()


@_closed_record
class LaunchOwnership:
    """Bounded launch identities only; caller construction supplies no proof."""

    runtime_id: str
    runtime_generation: str
    manifest_digest: str
    qualification_id: str
    qualification_generation: str
    measurement_ref: str
    execution_config_digest: str
    event_id: str
    committed_stream_id: str
    payload_digest: str
    execution_session_key: str
    projection_digest: str
    guard_control_id: str
    guard_revision: str
    guard_config_digest: str

    def __post_init__(self) -> None:
        for value in (self.runtime_id, self.qualification_id, self.guard_control_id):
            _scalar(value, "uuid")
        for value in (self.runtime_generation, self.qualification_generation, self.guard_revision):
            _scalar(value, "generation")
        for value in (
            self.manifest_digest,
            self.execution_config_digest,
            self.payload_digest,
            self.projection_digest,
            self.guard_config_digest,
        ):
            _scalar(value, "digest")
        for value in (self.measurement_ref, self.event_id):
            _scalar(value, "ref")
        _pattern(self.committed_stream_id, r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})")
        _require(self.committed_stream_id != "0-0")
        _require(all(int(n) <= 2**64 - 1 for n in self.committed_stream_id.split("-")))
        _pattern(
            self.execution_session_key,
            re.escape(f"protected:{self.runtime_id}:{self.runtime_generation}:") + r"[0-9a-f]{64}",
        )


@_closed_record
class OwnedObjects:
    """Planned names, then exact observed UIDs; none are authorization facts."""

    namespace: str | None = None
    pod_name: str | None = None
    service_name: str | None = None
    secret_name: str | None = None
    pod_uid: str | None = None
    service_uid: str | None = None
    secret_uid: str | None = None
    pod_resource_version: str | None = None
    service_resource_version: str | None = None
    secret_resource_version: str | None = None
    container_id: str | None = None
    node_uid: str | None = None

    def __post_init__(self) -> None:
        for value in (self.namespace, self.pod_name, self.service_name, self.secret_name):
            if value is not None:
                _scalar(value, "name")
        for value in (self.pod_uid, self.service_uid, self.secret_uid, self.node_uid):
            if value is not None:
                _scalar(value, "uuid")
        for value in (
            self.pod_resource_version,
            self.service_resource_version,
            self.secret_resource_version,
        ):
            if value is not None:
                _scalar(value, "version")
        if self.container_id is not None:
            _scalar(self.container_id, "container")


@_closed_record
class AssignmentMetadata:
    """One immutable private assignment's nonsecret hashes/references."""

    control_capability_sha256: str
    runner_token_sha256: str
    history_binding_sha256: str
    history_signing_credential_id: str
    history_signing_credential_generation: str
    history_expires_at_ms: str
    model_credential_id: str
    model_credential_generation: str

    def __post_init__(self) -> None:
        for value in (
            self.control_capability_sha256,
            self.runner_token_sha256,
            self.history_binding_sha256,
        ):
            _scalar(value, "digest")
        for value in (self.history_signing_credential_id, self.model_credential_id):
            _scalar(value, "ref")
        for value in (self.history_signing_credential_generation, self.model_credential_generation):
            _scalar(value, "generation")
        _pattern(self.history_expires_at_ms, r"0|[1-9][0-9]{0,15}")
        _require(int(self.history_expires_at_ms) <= 9007199254740991)
        _require(self.history_signing_credential_id != self.model_credential_id)


@_closed_record
class TerminationObservation:
    """Durable factual metadata received from the trusted server, never a fence."""

    pod_uid: str
    container_id: str
    resource_version: str
    exit_code: int
    finished_at: str

    def __post_init__(self) -> None:
        _scalar(self.pod_uid, "uuid")
        _scalar(self.container_id, "container")
        _scalar(self.resource_version, "version")
        _require(type(self.exit_code) is int and 0 <= self.exit_code <= 2**31 - 1)
        _pattern(
            self.finished_at,
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})",
        )
        _require(datetime.fromisoformat(self.finished_at).tzinfo is not None)


@_closed_record
class CleanupReceipt:
    """Non-authoritative completion metadata; no caller success boolean."""

    pod_uid: str
    service_uid: str
    secret_uid: str

    def __post_init__(self) -> None:
        for value in (self.pod_uid, self.service_uid, self.secret_uid):
            _scalar(value, "uuid")


@_closed_record
class LaunchSnapshot:
    """Immutable observational row; cannot be used as a positive capability."""

    launch_id: str
    worker_principal_id: str
    request_id: str
    request_digest: str
    creation_budget_ms: int
    ownership: LaunchOwnership
    revision: str
    state: State
    boot_id: str
    objects: OwnedObjects
    assignment: AssignmentMetadata | None
    owned_epoch: str | None
    termination: TerminationObservation | None
    retirement_outcome: Literal["cleanup-recorded"] | None


@_closed_record
class OperationRecord:
    """Closed journal entry; an uncertain entry never becomes pending again."""

    operation_id: str
    worker_principal_id: str
    launch_id: str
    kind: OperationKind
    request_digest: str
    status: OperationStatus
    boot_id: str
    epoch: str | None


@_closed_record
class OperationReservation:
    """New/existing is bookkeeping only, never outbound authorization."""

    operation: OperationRecord
    disposition: Literal["new", "existing"]
    revision: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS launches (
 launch_id TEXT PRIMARY KEY, worker_principal_id TEXT NOT NULL, request_id TEXT NOT NULL,
 request_digest TEXT NOT NULL, creation_budget_ms INTEGER NOT NULL
 CHECK(creation_budget_ms BETWEEN 1 AND 120000),
 runtime_id TEXT NOT NULL, runtime_generation TEXT NOT NULL,
 manifest_digest TEXT NOT NULL, qualification_id TEXT NOT NULL,
 qualification_generation TEXT NOT NULL, measurement_ref TEXT NOT NULL,
 execution_config_digest TEXT NOT NULL, event_id TEXT NOT NULL, committed_stream_id TEXT NOT NULL,
 payload_digest TEXT NOT NULL, execution_session_key TEXT NOT NULL, projection_digest TEXT NOT NULL,
 guard_control_id TEXT NOT NULL, guard_revision TEXT NOT NULL, guard_config_digest TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0), state TEXT NOT NULL
 CHECK(state IN ('preparing','idle','waiting','running','retiring','retired','failed')),
 boot_id TEXT NOT NULL, owned_epoch TEXT, retirement_outcome TEXT
 CHECK(retirement_outcome IS NULL OR retirement_outcome='cleanup-recorded'),
 UNIQUE(worker_principal_id,request_id)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS owned_execution ON launches
(runtime_id,runtime_generation,execution_session_key) WHERE state!='retired';
CREATE TABLE IF NOT EXISTS objects (
 launch_id TEXT PRIMARY KEY REFERENCES launches(launch_id), namespace TEXT,
 pod_name TEXT, service_name TEXT, secret_name TEXT,
 pod_uid TEXT, service_uid TEXT, secret_uid TEXT,
 pod_resource_version TEXT, service_resource_version TEXT, secret_resource_version TEXT,
 container_id TEXT, node_uid TEXT
) STRICT;
CREATE TABLE IF NOT EXISTS assignments (
 launch_id TEXT PRIMARY KEY REFERENCES launches(launch_id), control_capability_sha256 TEXT NOT NULL,
 runner_token_sha256 TEXT NOT NULL, history_binding_sha256 TEXT NOT NULL,
 history_signing_credential_id TEXT NOT NULL, history_signing_credential_generation TEXT NOT NULL,
 history_expires_at_ms TEXT NOT NULL, model_credential_id TEXT NOT NULL,
 model_credential_generation TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS terminations (
 launch_id TEXT PRIMARY KEY REFERENCES launches(launch_id), pod_uid TEXT NOT NULL,
 container_id TEXT NOT NULL, resource_version TEXT NOT NULL, exit_code INTEGER NOT NULL,
 finished_at TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS operations (
 operation_id TEXT NOT NULL, worker_principal_id TEXT NOT NULL,
 launch_id TEXT NOT NULL REFERENCES launches(launch_id), kind TEXT NOT NULL
 CHECK(kind IN ('start','admit','deny','interrupt','timeout','retire')),
 request_digest TEXT NOT NULL, status TEXT NOT NULL
 CHECK(status IN ('pending','applied','rejected','uncertain')), boot_id TEXT NOT NULL, epoch TEXT,
 PRIMARY KEY(worker_principal_id,operation_id)
) STRICT;
"""


def _open_root(root: str) -> int:
    """Walk every component without following symlinks; retain own descriptor."""
    _require(type(root) is str and root.startswith("/") and root != "/")
    components = root.split("/")[1:]
    _require(all(part not in {"", ".", ".."} for part in components))
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open("/", flags)
    try:
        for part in components:
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        info = os.fstat(fd)
        _require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700)
        return fd
    except (OSError, RegistryError):
        os.close(fd)
        raise


def _open_file(root_fd: int, name: str) -> int:
    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=root_fd)
    try:
        info = os.fstat(fd)
        _require(
            stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1
            and info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == 0o600
        )
        return fd
    except RegistryError:
        os.close(fd)
        raise


class ProtectedLaunchRegistry:
    """Actual single-host lock + SQLite, no failover or portable issuer claim.

    The explicit private root must be provisioner-owned and protected from same
    principal replacement. Metadata defenses do not independently establish that
    provisioning trust. All current connection/lock descriptors belong to this
    instance. Reopen always retires prior launches, even for a repeated boot UUID.

    Conservative initial limit: even a failed launch with no planned effects
    retains its execution identity. This module accepts no invented absence or
    cleanup proof to release it. Only the trusted server's exact termination and
    UID cleanup observations can record retirement and release that identity.
    """

    __slots__ = ("_root", "_root_fd", "_lock_fd", "_db_fd", "_db", "_mutex", "_boot_id")

    _root: str
    _root_fd: int
    _lock_fd: int
    _db_fd: int
    _db: sqlite3.Connection | None
    _mutex: threading.RLock
    _boot_id: str

    @_safe
    def __init__(self) -> None:
        raise RegistryInvalid()

    @classmethod
    @_safe
    def open(cls, root: str, *, boot_id: str) -> ProtectedLaunchRegistry:
        _scalar(boot_id, "uuid")
        instance = object.__new__(cls)
        instance._root = root
        instance._root_fd = instance._lock_fd = instance._db_fd = -1
        instance._db = None
        instance._mutex = threading.RLock()
        instance._boot_id = boot_id
        try:
            instance._root_fd = _open_root(root)
            instance._lock_fd = _open_file(instance._root_fd, "registry.lock")
            try:
                fcntl.flock(instance._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RegistryUnavailable() from None
            instance._db_fd = _open_file(instance._root_fd, "registry.sqlite")
            # SQLite manages its sidecars; reject preexisting symlinks/hardlinks
            # before SQLite can follow them. Same-principal storage is trusted.
            for name in ("registry.sqlite-wal", "registry.sqlite-shm", "registry.sqlite-journal"):
                fd = _open_file(instance._root_fd, name)
                os.close(fd)
            instance._check_root()
            instance._db = sqlite3.connect(
                root + "/registry.sqlite", isolation_level=None, timeout=0, check_same_thread=False
            )
            instance._db.row_factory = sqlite3.Row
            instance._db.execute("PRAGMA foreign_keys=ON")
            instance._db.execute("PRAGMA journal_mode=WAL")
            instance._db.execute("PRAGMA synchronous=FULL")
            instance._db.executescript(_SCHEMA)
            instance._check_root()
            with instance._transaction() as db:
                db.execute(
                    "UPDATE launches SET state='retiring',revision=revision+1 "
                    "WHERE state!='retired'"
                )
                db.execute("UPDATE operations SET status='uncertain' WHERE status='pending'")
            return instance
        except (RegistryError, sqlite3.Error, OSError, ValueError, TypeError, OverflowError):
            instance.close()
            raise

    def __repr__(self) -> str:
        return "<ProtectedLaunchRegistry>"

    def __enter__(self) -> ProtectedLaunchRegistry:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @_safe
    def close(self) -> None:
        """Release only this instance's live descriptors; repeated close is inert."""
        with self._mutex:
            try:
                if self._db is not None:
                    db, self._db = self._db, None
                    db.close()
            finally:
                for field in ("_db_fd", "_lock_fd", "_root_fd"):
                    fd = getattr(self, field)
                    if fd >= 0:
                        setattr(self, field, -1)
                        os.close(fd)

    def _check_root(self) -> None:
        _require(self._root_fd >= 0)
        actual = os.stat(self._root, follow_symlinks=False)
        pinned = os.fstat(self._root_fd)
        _require(
            (actual.st_dev, actual.st_ino) == (pinned.st_dev, pinned.st_ino)
            and stat.S_ISDIR(actual.st_mode)
            and actual.st_uid == os.geteuid()
            and stat.S_IMODE(actual.st_mode) == 0o700
        )
        if self._db_fd >= 0:
            actual_db = os.stat("registry.sqlite", dir_fd=self._root_fd, follow_symlinks=False)
            pinned_db = os.fstat(self._db_fd)
            _require(
                (actual_db.st_dev, actual_db.st_ino) == (pinned_db.st_dev, pinned_db.st_ino)
                and stat.S_ISREG(actual_db.st_mode)
                and actual_db.st_nlink == 1
                and actual_db.st_uid == os.geteuid()
                and stat.S_IMODE(actual_db.st_mode) == 0o600
            )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            if self._db is None:
                raise RegistryUnavailable()
            self._check_root()
            db = self._db
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except (RegistryError, sqlite3.Error, OSError, ValueError, TypeError, OverflowError):
                db.execute("ROLLBACK")
                raise

    def _owned(self, db: sqlite3.Connection, owner: str, launch_id: str) -> sqlite3.Row:
        _scalar(owner, "ref")
        _scalar(launch_id, "uuid")
        row = db.execute(
            "SELECT * FROM launches WHERE launch_id=? AND worker_principal_id=? LIMIT 1",
            (launch_id, owner),
        ).fetchone()
        if row is None:
            raise RegistryUnauthorized()
        return cast(sqlite3.Row, row)

    def _cas(
        self, db: sqlite3.Connection, owner: str, launch_id: str, expected_revision: str
    ) -> sqlite3.Row:
        _scalar(expected_revision, "generation")
        row = self._owned(db, owner, launch_id)
        if row["revision"] != int(expected_revision) or row["revision"] == _MAX_REVISION:
            raise RegistryConflict()
        return row

    @staticmethod
    def _positive(row: sqlite3.Row, boot_id: str) -> None:
        if row["boot_id"] != boot_id or row["state"] in {"retiring", "retired", "failed"}:
            raise RegistryRetired()

    @staticmethod
    def _snapshot(db: sqlite3.Connection, row: sqlite3.Row) -> LaunchSnapshot:
        launch_id = row["launch_id"]
        obj = db.execute("SELECT * FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)).fetchone()
        assignment = db.execute(
            "SELECT * FROM assignments WHERE launch_id=? LIMIT 1", (launch_id,)
        ).fetchone()
        termination = db.execute(
            "SELECT * FROM terminations WHERE launch_id=? LIMIT 1", (launch_id,)
        ).fetchone()
        return LaunchSnapshot(
            launch_id=launch_id,
            worker_principal_id=row["worker_principal_id"],
            request_id=row["request_id"],
            request_digest=row["request_digest"],
            creation_budget_ms=row["creation_budget_ms"],
            ownership=LaunchOwnership(
                **{field.name: row[field.name] for field in fields(LaunchOwnership)}
            ),
            revision=str(row["revision"]),
            state=row["state"],
            boot_id=row["boot_id"],
            objects=OwnedObjects(**{field.name: obj[field.name] for field in fields(OwnedObjects)}),
            assignment=None
            if assignment is None
            else AssignmentMetadata(
                **{field.name: assignment[field.name] for field in fields(AssignmentMetadata)}
            ),
            owned_epoch=row["owned_epoch"],
            termination=None
            if termination is None
            else TerminationObservation(
                **{field.name: termination[field.name] for field in fields(TerminationObservation)}
            ),
            retirement_outcome=row["retirement_outcome"],
        )

    @staticmethod
    def _operation(row: sqlite3.Row) -> OperationRecord:
        return OperationRecord(**{field.name: row[field.name] for field in fields(OperationRecord)})

    def _bump(self, db: sqlite3.Connection, owner: str, launch_id: str) -> LaunchSnapshot:
        db.execute(
            "UPDATE launches SET revision=revision+1 WHERE launch_id=? AND worker_principal_id=?",
            (launch_id, owner),
        )
        return self._snapshot(db, self._owned(db, owner, launch_id))

    @_safe
    def create(
        self,
        *,
        worker_principal_id: str,
        request_id: str,
        request_digest: str,
        ownership: LaunchOwnership,
        creation_budget_ms: int,
    ) -> LaunchSnapshot:
        """Persist preparing before effects with the caller's bounded creation budget.

        The trusted server independently selects the minimum profile/delivery
        budget. This bookkeeping API neither computes nor resets a deadline;
        restart keeps the original budget for cleanup and never resumes creation.
        """
        _scalar(worker_principal_id, "ref")
        _scalar(request_id, "uuid")
        _scalar(request_digest, "digest")
        _require(type(creation_budget_ms) is int and 1 <= creation_budget_ms <= 120000)
        _require(type(ownership) is LaunchOwnership)
        ownership.__post_init__()
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM launches WHERE worker_principal_id=? AND request_id=? LIMIT 1",
                (worker_principal_id, request_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["creation_budget_ms"] != creation_budget_ms
                    or existing["request_digest"] != request_digest
                    or any(
                        existing[field.name] != getattr(ownership, field.name)
                        for field in fields(LaunchOwnership)
                    )
                ):
                    raise RegistryConflict()
                self._positive(existing, self._boot_id)
                return self._snapshot(db, existing)
            launch_id = str(uuid4())
            try:
                db.execute(
                    "INSERT INTO launches(launch_id,worker_principal_id,request_id,request_digest,"
                    "creation_budget_ms,"
                    "runtime_id,runtime_generation,manifest_digest,qualification_id,"
                    "qualification_generation,measurement_ref,execution_config_digest,event_id,"
                    "committed_stream_id,payload_digest,execution_session_key,projection_digest,"
                    "guard_control_id,guard_revision,guard_config_digest,revision,state,boot_id) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,'preparing',?)",
                    (
                        launch_id,
                        worker_principal_id,
                        request_id,
                        request_digest,
                        creation_budget_ms,
                        ownership.runtime_id,
                        ownership.runtime_generation,
                        ownership.manifest_digest,
                        ownership.qualification_id,
                        ownership.qualification_generation,
                        ownership.measurement_ref,
                        ownership.execution_config_digest,
                        ownership.event_id,
                        ownership.committed_stream_id,
                        ownership.payload_digest,
                        ownership.execution_session_key,
                        ownership.projection_digest,
                        ownership.guard_control_id,
                        ownership.guard_revision,
                        ownership.guard_config_digest,
                        self._boot_id,
                    ),
                )
            except sqlite3.IntegrityError:
                raise RegistryConflict() from None
            db.execute("INSERT INTO objects(launch_id) VALUES(?)", (launch_id,))
            return self._snapshot(db, self._owned(db, worker_principal_id, launch_id))

    @_safe
    def get(self, worker_principal_id: str, launch_id: str) -> LaunchSnapshot:
        """Retained owner metadata remains readable after positive proof loss."""
        with self._transaction() as db:
            return self._snapshot(db, self._owned(db, worker_principal_id, launch_id))

    @_safe
    def find_request(self, worker_principal_id: str, request_id: str) -> LaunchSnapshot | None:
        """Exact owner-scoped retry metadata, including retained retired tombstones.

        The issuer can reject old-boot or conflicting requests before fresh
        positive broker proof is available. This lookup grants no capability.
        """
        _scalar(worker_principal_id, "ref")
        _scalar(request_id, "uuid")
        with self._transaction() as db:
            row = db.execute(
                "SELECT * FROM launches WHERE worker_principal_id=? AND request_id=? LIMIT 1",
                (worker_principal_id, request_id),
            ).fetchone()
            return None if row is None else self._snapshot(db, row)

    @_safe
    def enumerate_retiring(
        self, *, limit: int, after_launch_id: str | None = None
    ) -> tuple[LaunchSnapshot, ...]:
        """Bounded issuer-only restart cleanup metadata, never a worker HTTP surface.

        Keyset pagination prevents unresolved rows at the front from hiding later
        owned launches. The server retains the cursor for a bounded sweep and
        restarts it for its next pass. These snapshots do not prove termination.
        """
        _require(type(limit) is int and 1 <= limit <= 64)
        if after_launch_id is not None:
            _scalar(after_launch_id, "uuid")
        with self._transaction() as db:
            rows = db.execute(
                "SELECT * FROM launches WHERE state='retiring' AND launch_id>? "
                "ORDER BY launch_id LIMIT ?",
                (after_launch_id or "", limit),
            ).fetchall()
            return tuple(self._snapshot(db, row) for row in rows)

    @_safe
    def transition(
        self, worker_principal_id: str, launch_id: str, expected_revision: str, state: State
    ) -> LaunchSnapshot:
        """Constrained lifecycle CAS; termination/cleanup has separate recording APIs."""
        _require(type(state) is str and state in _STATES)
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if state == "retired":
                raise RegistryConflict()
            if row["state"] in {"retiring", "retired"}:
                raise RegistryRetired()
            if state != "retiring":
                self._positive(row, self._boot_id)
            permitted = {
                "preparing": {"idle", "retiring", "failed"},
                "idle": {"retiring", "failed"},
                "waiting": {"retiring", "failed"},
                "running": {"retiring", "failed"},
                "failed": {"retiring"},
            }
            if state not in permitted.get(row["state"], set()):
                raise RegistryConflict()
            if state == "failed":
                planned = db.execute(
                    "SELECT pod_name FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
                ).fetchone()
                journal = db.execute(
                    "SELECT operation_id FROM operations WHERE launch_id=? LIMIT 1", (launch_id,)
                ).fetchone()
                if planned["pod_name"] is not None or journal is not None:
                    state = "retiring"
            db.execute("UPDATE launches SET state=? WHERE launch_id=?", (state, launch_id))
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def plan_objects(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        *,
        namespace: str,
        pod_name: str,
        service_name: str,
        secret_name: str,
    ) -> LaunchSnapshot:
        """Durable names precede effects; cannot remap an existing plan."""
        for value in (namespace, pod_name, service_name, secret_name):
            _scalar(value, "name")
        _require(len({pod_name, service_name, secret_name}) == 3)
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            self._positive(row, self._boot_id)
            if row["state"] != "preparing":
                raise RegistryConflict()
            obj = db.execute(
                "SELECT namespace FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if obj["namespace"] is not None:
                raise RegistryConflict()
            db.execute(
                "UPDATE objects SET namespace=?,pod_name=?,service_name=?,secret_name=? "
                "WHERE launch_id=?",
                (namespace, pod_name, service_name, secret_name, launch_id),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_object_uid(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        kind: Literal["pod", "service", "secret"],
        uid: str,
        resource_version: str,
    ) -> LaunchSnapshot:
        """Record exact API UID after effect; may recover old owned creation intent."""
        _require(type(kind) is str and kind in {"pod", "service", "secret"})
        _scalar(uid, "uuid")
        _scalar(resource_version, "version")
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if row["state"] not in {"preparing", "retiring"}:
                raise RegistryConflict()
            obj = db.execute(
                "SELECT * FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if obj[kind + "_name"] is None or obj[kind + "_uid"] is not None:
                raise RegistryConflict()
            # SQL keys are constants, never assembled from caller strings.
            statements = {
                "pod": "UPDATE objects SET pod_uid=?,pod_resource_version=? WHERE launch_id=?",
                "service": "UPDATE objects SET service_uid=?,service_resource_version=? "
                "WHERE launch_id=?",
                "secret": "UPDATE objects SET secret_uid=?,secret_resource_version=? "
                "WHERE launch_id=?",
            }
            db.execute(statements[kind], (uid, resource_version, launch_id))
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_container_identity(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        container_id: str,
        node_uid: str,
    ) -> LaunchSnapshot:
        """Retain exact runner identity for future authenticated termination cleanup."""
        _scalar(container_id, "container")
        _scalar(node_uid, "uuid")
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if row["state"] not in {"preparing", "retiring"}:
                raise RegistryConflict()
            obj = db.execute(
                "SELECT pod_uid,container_id FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if obj["pod_uid"] is None or obj["container_id"] is not None:
                raise RegistryConflict()
            db.execute(
                "UPDATE objects SET container_id=?,node_uid=? WHERE launch_id=?",
                (container_id, node_uid, launch_id),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_assignment(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        assignment: AssignmentMetadata,
    ) -> LaunchSnapshot:
        """Hash/reference metadata only; actual immutable Secret assignment is server-owned."""
        _require(type(assignment) is AssignmentMetadata)
        assignment.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            self._positive(row, self._boot_id)
            if row["state"] != "preparing":
                raise RegistryConflict()
            existing = db.execute(
                "SELECT launch_id FROM assignments WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if existing is not None:
                raise RegistryConflict()
            db.execute(
                "INSERT INTO assignments VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    launch_id,
                    assignment.control_capability_sha256,
                    assignment.runner_token_sha256,
                    assignment.history_binding_sha256,
                    assignment.history_signing_credential_id,
                    assignment.history_signing_credential_generation,
                    assignment.history_expires_at_ms,
                    assignment.model_credential_id,
                    assignment.model_credential_generation,
                ),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def get_operation(
        self, worker_principal_id: str, launch_id: str, operation_id: str
    ) -> OperationRecord:
        """Bounded owner-only journal lookup, including old boot/retired metadata."""
        _scalar(operation_id, "uuid")
        with self._transaction() as db:
            self._owned(db, worker_principal_id, launch_id)
            row = db.execute(
                "SELECT * FROM operations WHERE worker_principal_id=? AND launch_id=? "
                "AND operation_id=? LIMIT 1",
                (worker_principal_id, launch_id, operation_id),
            ).fetchone()
            if row is None:
                raise RegistryUnauthorized()
            return self._operation(row)

    @_safe
    def begin_operation(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        operation_id: str,
        kind: OperationKind,
        request_digest: str,
        *,
        epoch: str | None = None,
    ) -> OperationReservation:
        """Persist pending before effect. Existing IDs never reserve another effect.

        An existing old-boot positive entry is returned as observational replay
        bookkeeping only. It cannot create a new reservation or change its status.
        """
        _scalar(operation_id, "uuid")
        _scalar(request_digest, "digest")
        _scalar(expected_revision, "generation")
        _require(type(kind) is str and kind in _KINDS)
        if epoch is not None:
            _scalar(epoch, "epoch")
            if kind == "start":
                raise RegistryConflict()
        with self._transaction() as db:
            row = self._owned(db, worker_principal_id, launch_id)
            existing = db.execute(
                "SELECT * FROM operations WHERE worker_principal_id=? AND operation_id=? LIMIT 1",
                (worker_principal_id, operation_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["launch_id"] != launch_id
                    or existing["kind"] != kind
                    or existing["request_digest"] != request_digest
                    or (kind != "start" and existing["epoch"] != epoch)
                ):
                    raise RegistryConflict()
                return OperationReservation(
                    self._operation(existing), "existing", str(row["revision"])
                )
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if kind in {"start", "admit"}:
                self._positive(row, self._boot_id)
                if row["state"] != ("idle" if kind == "start" else "waiting"):
                    raise RegistryConflict()
                pending = db.execute(
                    "SELECT operation_id FROM operations WHERE launch_id=? "
                    "AND kind IN ('start','admit') AND status='pending' LIMIT 1",
                    (launch_id,),
                ).fetchone()
                if pending is not None:
                    raise RegistryConflict()
            if kind == "start":
                if epoch is not None or row["owned_epoch"] is not None:
                    raise RegistryConflict()
            elif kind != "retire":
                if epoch is None or epoch != row["owned_epoch"]:
                    raise RegistryConflict()
            elif epoch is not None:
                raise RegistryInvalid()
            if row["state"] == "retired" and kind != "retire":
                raise RegistryRetired()
            db.execute(
                "INSERT INTO operations VALUES(?,?,?,?,?,'pending',?,?)",
                (
                    operation_id,
                    worker_principal_id,
                    launch_id,
                    kind,
                    request_digest,
                    self._boot_id,
                    epoch,
                ),
            )
            if kind == "retire" and row["state"] != "retired":
                db.execute("UPDATE launches SET state='retiring' WHERE launch_id=?", (launch_id,))
            snapshot = self._bump(db, worker_principal_id, launch_id)
            operation = db.execute(
                "SELECT * FROM operations WHERE worker_principal_id=? AND operation_id=? LIMIT 1",
                (worker_principal_id, operation_id),
            ).fetchone()
            return OperationReservation(self._operation(operation), "new", snapshot.revision)

    @_safe
    def finish_operation(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        operation_id: str,
        outcome: Literal["applied", "rejected", "uncertain"],
        *,
        epoch: str | None = None,
        state: Literal["waiting", "running"] | None = None,
    ) -> LaunchSnapshot:
        """CAS outcome metadata; uncertain acceptance irrevocably starts retirement."""
        _scalar(operation_id, "uuid")
        _require(type(outcome) is str and outcome in _STATUSES - {"pending"})
        if epoch is not None:
            _scalar(epoch, "epoch")
        _require(state is None or (type(state) is str and state in {"waiting", "running"}))
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            operation = db.execute(
                "SELECT * FROM operations WHERE worker_principal_id=? AND launch_id=? "
                "AND operation_id=? LIMIT 1",
                (worker_principal_id, launch_id, operation_id),
            ).fetchone()
            if operation is None:
                raise RegistryUnauthorized()
            if operation["status"] != "pending" or operation["boot_id"] != self._boot_id:
                raise RegistryConflict()
            if operation["kind"] in {"start", "admit"} and outcome == "applied":
                self._positive(row, self._boot_id)
                if operation["kind"] == "start":
                    if epoch is None or state is None or row["state"] != "idle":
                        raise RegistryConflict()
                    db.execute(
                        "UPDATE launches SET owned_epoch=?,state=? WHERE launch_id=?",
                        (epoch, state, launch_id),
                    )
                else:
                    if (
                        epoch != row["owned_epoch"]
                        or state != "running"
                        or row["state"] != "waiting"
                    ):
                        raise RegistryConflict()
                    db.execute(
                        "UPDATE launches SET state='running' WHERE launch_id=?", (launch_id,)
                    )
            elif state is not None or (epoch is not None and epoch != operation["epoch"]):
                raise RegistryConflict()
            if outcome == "uncertain":
                if row["state"] == "retired":
                    raise RegistryRetired()
                db.execute("UPDATE launches SET state='retiring' WHERE launch_id=?", (launch_id,))
            db.execute(
                "UPDATE operations SET status=?,epoch=COALESCE(?,epoch) "
                "WHERE worker_principal_id=? "
                "AND operation_id=?",
                (outcome, epoch, worker_principal_id, operation_id),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_termination_observation(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        observation: TerminationObservation,
    ) -> LaunchSnapshot:
        """Persist server-supplied exact runner facts before finalizer removal.

        This storage API does not authenticate the observation. The actual trusted
        server reader and qualification remain mandatory before calling it.
        """
        _require(type(observation) is TerminationObservation)
        observation.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            obj = db.execute(
                "SELECT pod_uid,container_id FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if (
                row["state"] != "retiring"
                or observation.pod_uid != obj["pod_uid"]
                or observation.container_id != obj["container_id"]
            ):
                raise RegistryConflict()
            existing = db.execute(
                "SELECT launch_id FROM terminations WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if existing is not None:
                raise RegistryConflict()
            db.execute(
                "INSERT INTO terminations VALUES(?,?,?,?,?,?)",
                (
                    launch_id,
                    observation.pod_uid,
                    observation.container_id,
                    observation.resource_version,
                    observation.exit_code,
                    observation.finished_at,
                ),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_cleanup_receipt(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        receipt: CleanupReceipt,
    ) -> LaunchSnapshot:
        """Record non-authoritative completion after the trusted server fences/cleans.

        Only exact known UIDs plus prior durable runner termination metadata fit.
        No bool, provider NotFound or object absence is accepted as evidence here.
        """
        _require(type(receipt) is CleanupReceipt)
        receipt.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            obj = db.execute(
                "SELECT pod_uid,service_uid,secret_uid FROM objects WHERE launch_id=? LIMIT 1",
                (launch_id,),
            ).fetchone()
            terminated = db.execute(
                "SELECT pod_uid FROM terminations WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if (
                row["state"] != "retiring"
                or terminated is None
                or any(
                    obj[field.name] != getattr(receipt, field.name)
                    for field in fields(CleanupReceipt)
                )
                or terminated["pod_uid"] != receipt.pod_uid
            ):
                raise RegistryConflict()
            db.execute(
                "UPDATE launches SET state='retired',retirement_outcome='cleanup-recorded' "
                "WHERE launch_id=?",
                (launch_id,),
            )
            return self._bump(db, worker_principal_id, launch_id)
