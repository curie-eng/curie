"""Primitive-probe ownership before any permit. @spec PROTECTED-COMMISSIONING-8.

Before a permit exists, the provisioner's primitive probes follow the same
ownership rules as campaign launches, in a separate registry that grants no
campaign or production authority. This is that registry.

The rules are the campaign registry's: a probe's ID and its deterministic
Pod, Service and Secret names are durable before any create; a UID is recorded
only from this boot's own create response, so a same-name object found by
name is never adopted; and a probe retires only over the recorded termination
of its original runner container, or the recorded observation that the runner
never started, before the finalizer is removed and absence is confirmed. The
termination and never-started facts are the campaign registry's own types,
not new ones.

What it is not: the rows name no permit, launch request, reservation,
worker principal, assignment, Qualification or Readiness. Nothing here can be
read as a campaign launch or a production launch. The registry lives in its
own root: a root that holds the production or campaign registry refuses, so a
probe row is never beside a launch row. The first open pins one scope digest
(the installation's control profile), so a registry never mixes probes of two
installations. A primitive completion is a digest the runtime records; it
authenticates nothing by itself.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import fields
from typing import Literal, cast

from .commissioning_registry import UnstartedObservation, object_names
from .protected_launch_registry import (
    CleanupReceipt,
    OwnedObjects,
    RegistryConflict,
    RegistryError,
    RegistryInvalid,
    RegistryRetired,
    RegistryUnauthorized,
    RegistryUnavailable,
    TerminationObservation,
    _closed_record,
    _open_file,
    _open_root,
    _require,
    _safe,
    _scalar,
)

__all__ = ["MAX_PROBES", "Kind", "ProbeRegistry", "ProbeSnapshot", "ProbeState", "probe_names"]

ProbeState = Literal["preparing", "live", "retiring", "uncertain", "retired"]
Kind = Literal["pod", "service", "secret"]
_KINDS = frozenset({"pod", "service", "secret"})
_MAX_REVISION = 2**63 - 1
MAX_PROBES = 64
_FILE = "probe.sqlite"
_LOCK = "probe.lock"
# The production and campaign registries' files. A probe root holds neither.
_LAUNCH_FILES = ("registry.sqlite", "commissioning.sqlite")


def probe_names(probe_id: str) -> tuple[str, str, str]:
    """Deterministic Pod, Service and Secret names of one probe.

    The campaign derivation, because the measured guard admits only those
    names; the guard is not revised for probes.
    """
    return object_names(probe_id)


@_closed_record
class ProbeSnapshot:
    """Immutable observational row; it is not a capability of any kind."""

    probe_id: str
    scope_digest: str
    create_operation_id: str
    revision: str
    state: ProbeState
    boot_id: str
    objects: OwnedObjects
    termination: TerminationObservation | None
    unstarted: UnstartedObservation | None
    completion_digest: str | None
    retirement_outcome: Literal["cleanup-recorded"] | None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS scope (
 id INTEGER PRIMARY KEY CHECK(id=1), scope_digest TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS probes (
 probe_id TEXT PRIMARY KEY, create_operation_id TEXT NOT NULL UNIQUE,
 revision INTEGER NOT NULL CHECK(revision>0), state TEXT NOT NULL
 CHECK(state IN ('preparing','live','retiring','uncertain','retired')),
 boot_id TEXT NOT NULL, completion_digest TEXT, retirement_outcome TEXT
 CHECK(retirement_outcome IS NULL OR retirement_outcome='cleanup-recorded')
) STRICT;
CREATE TABLE IF NOT EXISTS objects (
 probe_id TEXT PRIMARY KEY REFERENCES probes(probe_id), namespace TEXT NOT NULL,
 pod_name TEXT NOT NULL UNIQUE, service_name TEXT NOT NULL UNIQUE,
 secret_name TEXT NOT NULL UNIQUE,
 pod_uid TEXT, service_uid TEXT, secret_uid TEXT,
 pod_resource_version TEXT, service_resource_version TEXT, secret_resource_version TEXT,
 container_id TEXT, node_uid TEXT
) STRICT;
CREATE TABLE IF NOT EXISTS terminations (
 probe_id TEXT PRIMARY KEY REFERENCES probes(probe_id), pod_uid TEXT NOT NULL,
 container_id TEXT NOT NULL, resource_version TEXT NOT NULL, exit_code INTEGER NOT NULL,
 finished_at TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS unstarted (
 probe_id TEXT PRIMARY KEY REFERENCES probes(probe_id), pod_uid TEXT NOT NULL,
 resource_version TEXT NOT NULL,
 evidence TEXT NOT NULL CHECK(evidence IN ('unscheduled','terminal'))
) STRICT;
"""

# SQL keys are constants, never assembled from caller strings.
_SET_UID = {
    "pod": "UPDATE objects SET pod_uid=?,pod_resource_version=? WHERE probe_id=?",
    "service": "UPDATE objects SET service_uid=?,service_resource_version=? WHERE probe_id=?",
    "secret": "UPDATE objects SET secret_uid=?,secret_resource_version=? WHERE probe_id=?",
}


class ProbeRegistry:
    """Single-host lock plus SQLite for one installation's primitive probes.

    The explicit private root must be owned by the control principal and hold
    no launch registry. Reopen moves every ``preparing`` or ``live`` probe to
    ``retiring``: a restarted control process never resumes a creation and
    never exercises a probe it did not create on this boot.
    """

    __slots__ = ("_root", "_root_fd", "_lock_fd", "_db_fd", "_db", "_mutex", "_boot_id", "_scope")

    _root: str
    _root_fd: int
    _lock_fd: int
    _db_fd: int
    _db: sqlite3.Connection | None
    _mutex: threading.RLock
    _boot_id: str
    _scope: str

    @_safe
    def __init__(self) -> None:
        raise RegistryInvalid()

    @classmethod
    @_safe
    def open(cls, root: str, *, boot_id: str, scope_digest: str) -> ProbeRegistry:
        _scalar(boot_id, "uuid")
        _scalar(scope_digest, "digest")
        instance = object.__new__(cls)
        instance._root, instance._boot_id, instance._scope = root, boot_id, scope_digest
        instance._root_fd = instance._lock_fd = instance._db_fd = -1
        instance._db = None
        instance._mutex = threading.RLock()
        try:
            instance._root_fd = _open_root(root)
            for name in _LAUNCH_FILES:
                # Never beside a launch registry: no row here can sit with launch rows.
                _require(not _exists(instance._root_fd, name))
            instance._lock_fd = _open_file(instance._root_fd, _LOCK)
            try:
                fcntl.flock(instance._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RegistryUnavailable() from None
            instance._db_fd = _open_file(instance._root_fd, _FILE)
            for suffix in ("-wal", "-shm", "-journal"):
                os.close(_open_file(instance._root_fd, _FILE + suffix))
            instance._check_root()
            instance._db = sqlite3.connect(
                root + "/" + _FILE, isolation_level=None, timeout=0, check_same_thread=False
            )
            instance._db.row_factory = sqlite3.Row
            instance._db.execute("PRAGMA foreign_keys=ON")
            instance._db.execute("PRAGMA journal_mode=WAL")
            instance._db.execute("PRAGMA synchronous=FULL")
            instance._db.executescript(_SCHEMA)
            with instance._transaction() as db:
                pinned = db.execute("SELECT scope_digest FROM scope WHERE id=1").fetchone()
                if pinned is None:
                    db.execute("INSERT INTO scope VALUES(1,?)", (scope_digest,))
                elif pinned["scope_digest"] != scope_digest:
                    raise RegistryConflict()
                db.execute(
                    "UPDATE probes SET state='retiring',revision=revision+1 "
                    "WHERE state IN ('preparing','live')"
                )
            return instance
        except (RegistryError, sqlite3.Error, OSError, ValueError, TypeError, OverflowError):
            instance.close()
            raise

    def __repr__(self) -> str:
        return "<ProbeRegistry>"

    def __enter__(self) -> ProbeRegistry:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def scope_digest(self) -> str:
        return self._scope

    @_safe
    def close(self) -> None:
        """Release only this instance's live descriptors; repeated close is inert."""
        with self._mutex:
            try:
                if self._db is not None:
                    db, self._db = self._db, None
                    db.close()
            finally:
                for name in ("_db_fd", "_lock_fd", "_root_fd"):
                    fd = getattr(self, name)
                    if fd >= 0:
                        setattr(self, name, -1)
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
        for name in _LAUNCH_FILES:
            _require(not _exists(self._root_fd, name))
        if self._db_fd >= 0:
            actual_db = os.stat(_FILE, dir_fd=self._root_fd, follow_symlinks=False)
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

    def _row(self, db: sqlite3.Connection, probe_id: str) -> sqlite3.Row:
        _scalar(probe_id, "uuid")
        row = db.execute("SELECT * FROM probes WHERE probe_id=? LIMIT 1", (probe_id,)).fetchone()
        if row is None:
            raise RegistryUnauthorized()
        return cast(sqlite3.Row, row)

    def _cas(self, db: sqlite3.Connection, probe_id: str, expected_revision: str) -> sqlite3.Row:
        _scalar(expected_revision, "generation")
        row = self._row(db, probe_id)
        if row["revision"] != int(expected_revision) or row["revision"] == _MAX_REVISION:
            raise RegistryConflict()
        return row

    def _current(self, row: sqlite3.Row, state: str) -> None:
        if row["boot_id"] != self._boot_id or row["state"] != state:
            raise RegistryConflict()

    def _snapshot(self, db: sqlite3.Connection, row: sqlite3.Row) -> ProbeSnapshot:
        probe_id = row["probe_id"]
        obj = db.execute("SELECT * FROM objects WHERE probe_id=? LIMIT 1", (probe_id,)).fetchone()
        termination = db.execute(
            "SELECT * FROM terminations WHERE probe_id=? LIMIT 1", (probe_id,)
        ).fetchone()
        unstarted = db.execute(
            "SELECT * FROM unstarted WHERE probe_id=? LIMIT 1", (probe_id,)
        ).fetchone()
        return ProbeSnapshot(
            probe_id=probe_id,
            scope_digest=self._scope,
            create_operation_id=row["create_operation_id"],
            revision=str(row["revision"]),
            state=row["state"],
            boot_id=row["boot_id"],
            objects=OwnedObjects(**{field.name: obj[field.name] for field in fields(OwnedObjects)}),
            termination=None
            if termination is None
            else TerminationObservation(
                **{field.name: termination[field.name] for field in fields(TerminationObservation)}
            ),
            unstarted=None
            if unstarted is None
            else UnstartedObservation(
                **{field.name: unstarted[field.name] for field in fields(UnstartedObservation)}
            ),
            completion_digest=row["completion_digest"],
            retirement_outcome=row["retirement_outcome"],
        )

    def _bump(self, db: sqlite3.Connection, probe_id: str) -> ProbeSnapshot:
        db.execute("UPDATE probes SET revision=revision+1 WHERE probe_id=?", (probe_id,))
        return self._snapshot(db, self._row(db, probe_id))

    @staticmethod
    def _objects(db: sqlite3.Connection, probe_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM objects WHERE probe_id=? LIMIT 1", (probe_id,)).fetchone()
        return cast(sqlite3.Row, row)

    @staticmethod
    def _has(db: sqlite3.Connection, table: str, probe_id: str) -> bool:
        query = {
            "terminations": "SELECT 1 FROM terminations WHERE probe_id=? LIMIT 1",
            "unstarted": "SELECT 1 FROM unstarted WHERE probe_id=? LIMIT 1",
        }[table]
        return db.execute(query, (probe_id,)).fetchone() is not None

    @_safe
    def create(self, probe_id: str, *, namespace: str, create_operation_id: str) -> ProbeSnapshot:
        """Persist ``preparing`` and the deterministic names before any effect.

        An identical replay on the same boot returns the row; any changed member
        conflicts, and a row from an earlier boot never resumes creation.
        """
        _scalar(namespace, "name")
        _scalar(create_operation_id, "uuid")
        pod, service, secret = probe_names(probe_id)
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM probes WHERE probe_id=? LIMIT 1", (probe_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["create_operation_id"] != create_operation_id
                    or self._objects(db, probe_id)["namespace"] != namespace
                ):
                    raise RegistryConflict()
                if existing["boot_id"] != self._boot_id or existing["state"] != "preparing":
                    raise RegistryRetired()
                return self._snapshot(db, existing)
            count = db.execute("SELECT count(*) FROM probes").fetchone()[0]
            if count >= MAX_PROBES:
                raise RegistryConflict()
            try:
                db.execute(
                    "INSERT INTO probes(probe_id,create_operation_id,revision,state,boot_id) "
                    "VALUES(?,?,1,'preparing',?)",
                    (probe_id, create_operation_id, self._boot_id),
                )
                db.execute(
                    "INSERT INTO objects(probe_id,namespace,pod_name,service_name,secret_name) "
                    "VALUES(?,?,?,?,?)",
                    (probe_id, namespace, pod, service, secret),
                )
            except sqlite3.IntegrityError:
                raise RegistryConflict() from None
            return self._snapshot(db, self._row(db, probe_id))

    @_safe
    def get(self, probe_id: str) -> ProbeSnapshot:
        with self._transaction() as db:
            return self._snapshot(db, self._row(db, probe_id))

    @_safe
    def probes(self) -> tuple[ProbeSnapshot, ...]:
        """Every row, in probe ID order; at most ``MAX_PROBES``."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT * FROM probes ORDER BY probe_id LIMIT ?", (MAX_PROBES,)
            ).fetchall()
            return tuple(self._snapshot(db, row) for row in rows)

    @_safe
    def record_object_uid(
        self, probe_id: str, expected_revision: str, kind: Kind, uid: str, resource_version: str
    ) -> ProbeSnapshot:
        """Record the UID of this boot's own create response, never a name lookup."""
        _require(type(kind) is str and kind in _KINDS)
        _scalar(uid, "uuid")
        _scalar(resource_version, "version")
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            self._current(row, "preparing")
            if self._objects(db, probe_id)[kind + "_uid"] is not None:
                raise RegistryConflict()
            db.execute(_SET_UID[kind], (uid, resource_version, probe_id))
            return self._bump(db, probe_id)

    @_safe
    def transition(
        self, probe_id: str, expected_revision: str, state: Literal["live", "retiring"]
    ) -> ProbeSnapshot:
        """``preparing`` to ``live`` with every UID known, and live states to ``retiring``."""
        _require(type(state) is str and state in {"live", "retiring"})
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            if row["state"] in {"retiring", "uncertain", "retired"}:
                raise RegistryRetired()
            if state == "live":
                self._current(row, "preparing")
                obj = self._objects(db, probe_id)
                if any(obj[kind + "_uid"] is None for kind in sorted(_KINDS)):
                    raise RegistryConflict()
            db.execute("UPDATE probes SET state=? WHERE probe_id=?", (state, probe_id))
            return self._bump(db, probe_id)

    @_safe
    def mark_uncertain(self, probe_id: str, expected_revision: str) -> ProbeSnapshot:
        """Record an ambiguous create. Nothing moves it on; it blocks completion."""
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            if row["state"] not in {"preparing", "live", "retiring"}:
                raise RegistryConflict()
            db.execute("UPDATE probes SET state='uncertain' WHERE probe_id=?", (probe_id,))
            return self._bump(db, probe_id)

    @_safe
    def record_container_identity(
        self, probe_id: str, expected_revision: str, container_id: str, node_uid: str
    ) -> ProbeSnapshot:
        """Retain the exact runner identity that termination must later prove."""
        _scalar(container_id, "container")
        _scalar(node_uid, "uuid")
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            if row["state"] not in {"live", "retiring"}:
                raise RegistryConflict()
            obj = self._objects(db, probe_id)
            if (
                obj["pod_uid"] is None
                or obj["container_id"] is not None
                or self._has(db, "unstarted", probe_id)
            ):
                raise RegistryConflict()
            db.execute(
                "UPDATE objects SET container_id=?,node_uid=? WHERE probe_id=?",
                (container_id, node_uid, probe_id),
            )
            return self._bump(db, probe_id)

    @_safe
    def record_completion(
        self, probe_id: str, expected_revision: str, completion_digest: str
    ) -> ProbeSnapshot:
        """The digest of one primitive's result against this boot's live probe, once."""
        _scalar(completion_digest, "digest")
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            self._current(row, "live")
            if (
                row["completion_digest"] is not None
                or self._objects(db, probe_id)["container_id"] is None
            ):
                raise RegistryConflict()
            db.execute(
                "UPDATE probes SET completion_digest=? WHERE probe_id=?",
                (completion_digest, probe_id),
            )
            return self._bump(db, probe_id)

    @_safe
    def record_termination(
        self, probe_id: str, expected_revision: str, observation: TerminationObservation
    ) -> ProbeSnapshot:
        """Persist the original runner's termination before finalizer removal."""
        _require(type(observation) is TerminationObservation)
        observation.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            obj = self._objects(db, probe_id)
            if (
                row["state"] != "retiring"
                or observation.pod_uid != obj["pod_uid"]
                or observation.container_id != obj["container_id"]
                or self._has(db, "terminations", probe_id)
                or self._has(db, "unstarted", probe_id)
            ):
                raise RegistryConflict()
            db.execute(
                "INSERT INTO terminations VALUES(?,?,?,?,?,?)",
                (probe_id, *(getattr(observation, f.name) for f in fields(TerminationObservation))),
            )
            return self._bump(db, probe_id)

    @_safe
    def record_unstarted(
        self, probe_id: str, expected_revision: str, observation: UnstartedObservation
    ) -> ProbeSnapshot:
        """Persist that the runner never started, before finalizer removal.

        Only for a retiring row with every UID known and neither a recorded
        container nor a termination: a recorded container is proved by its
        own termination.
        """
        _require(type(observation) is UnstartedObservation)
        observation.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            obj = self._objects(db, probe_id)
            if (
                row["state"] != "retiring"
                or obj["service_uid"] is None
                or obj["secret_uid"] is None
                or obj["pod_uid"] != observation.pod_uid
                or obj["container_id"] is not None
                or self._has(db, "terminations", probe_id)
                or self._has(db, "unstarted", probe_id)
            ):
                raise RegistryConflict()
            db.execute(
                "INSERT INTO unstarted VALUES(?,?,?,?)",
                (probe_id, *(getattr(observation, f.name) for f in fields(UnstartedObservation))),
            )
            return self._bump(db, probe_id)

    @_safe
    def record_cleanup_receipt(
        self, probe_id: str, expected_revision: str, receipt: CleanupReceipt
    ) -> ProbeSnapshot:
        """Retire only over exact known UIDs and a recorded termination or never-started fact."""
        _require(type(receipt) is CleanupReceipt)
        receipt.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, probe_id, expected_revision)
            obj = self._objects(db, probe_id)
            proof = db.execute(
                "SELECT pod_uid FROM terminations WHERE probe_id=? "
                "UNION ALL SELECT pod_uid FROM unstarted WHERE probe_id=? LIMIT 1",
                (probe_id, probe_id),
            ).fetchone()
            if (
                row["state"] != "retiring"
                or proof is None
                or proof["pod_uid"] != receipt.pod_uid
                or any(obj[f.name] != getattr(receipt, f.name) for f in fields(CleanupReceipt))
            ):
                raise RegistryConflict()
            db.execute(
                "UPDATE probes SET state='retired',retirement_outcome='cleanup-recorded' "
                "WHERE probe_id=?",
                (probe_id,),
            )
            return self._bump(db, probe_id)


def _exists(root_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True
