"""Private commissioning launch bookkeeping. @spec PROTECTED-COMMISSIONING-8.

The shared launch mechanics record Kubernetes effects here when they run under
the commissioning authorizer. The rows issue no authority, carry no production
Qualification, Readiness or receipt field, and authenticate no observation.
The authoritative campaign ownership record is the broker's launch ownership
record; this store keeps the mediator-local object UIDs, container identity and
termination facts that cleanup needs after a restart.

It differs from the production registry where the specification requires:
the launch ID comes from the campaign launch request and the owned names derive
from it, both durable before any create; one launch reservation backs exactly
one launch; and a UID is recorded only from the create response. A same-name
object found by name is never adopted. An ambiguous create stays unresolved
until a guard-owned witness names the UID it admitted for that create.
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
from urllib.parse import quote
from uuid import UUID

from .protected_launch_registry import (
    AssignmentMetadata,
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

State = Literal["preparing", "idle", "retiring", "uncertain", "retired"]
UnstartedEvidence = Literal["unscheduled", "terminal"]
Kind = Literal["pod", "service", "secret"]
_KINDS = frozenset({"pod", "service", "secret"})
_MAX_REVISION = 2**63 - 1
_FILE = "commissioning.sqlite"


def object_names(launch_id: str) -> tuple[str, str, str]:
    """Deterministic owned Pod, Service and Secret names of one campaign launch.

    The derivation is the production mediator's (``pl-p-``, ``pl-s-``,
    ``pl-c-`` plus the launch UUID hex), because the measured guard admits only
    those names; the guard is not revised for commissioning. A production
    launch needs a Qualification, which exists only after the campaign closes,
    and ``create`` refuses any launch the production registry in the same root
    already knows. @spec PROTECTED-COMMISSIONING-8.
    """
    _scalar(launch_id, "uuid")
    suffix = UUID(launch_id).hex
    return "pl-p-" + suffix, "pl-s-" + suffix, "pl-c-" + suffix


@_closed_record
class CampaignLaunchBinding:
    """Digests and IDs that bind one row to its campaign launch request.

    It carries workload binding W's create operation ID and the guard identity
    that admits the creates, so an ambiguous create resolves only against a
    witness of that exact operation from that guard. Construction supplies no
    proof; the commissioning authorizer verifies the request, consumes the
    launch reservation and records broker ownership.
    """

    permit_digest: str
    launch_request_digest: str
    projection_digest: str
    launch_reservation_id: str
    create_operation_id: str
    guard_control_id: str
    guard_revision: str
    guard_config_digest: str

    def __post_init__(self) -> None:
        for value in (
            self.permit_digest,
            self.launch_request_digest,
            self.projection_digest,
            self.guard_config_digest,
        ):
            _scalar(value, "digest")
        for value in (self.launch_reservation_id, self.create_operation_id, self.guard_control_id):
            _scalar(value, "uuid")
        _scalar(self.guard_revision, "generation")


@_closed_record
class GuardCreateWitness:
    """The guard's statement of the UID it admitted for one recorded create.

    Construction supplies no proof: the caller obtains it from the guard-owned
    identity and observes the object at the exact recorded name with this UID.
    @spec PROTECTED-COMMISSIONING-8.
    """

    kind: Kind
    uid: str
    create_operation_id: str
    guard_control_id: str
    guard_revision: str
    guard_config_digest: str

    def __post_init__(self) -> None:
        _require(type(self.kind) is str and self.kind in _KINDS)
        for value in (self.uid, self.create_operation_id, self.guard_control_id):
            _scalar(value, "uuid")
        _scalar(self.guard_revision, "generation")
        _scalar(self.guard_config_digest, "digest")


@_closed_record
class UnstartedObservation:
    """The mediator's observation that the owned Pod's runner never started.

    Observed on the exact deleted Pod once no container can start any more:
    ``unscheduled`` when it was never bound to a node (a terminating Pod cannot
    be bound), ``terminal`` when the kubelet reported the deleted Pod in a
    terminal phase. In both cases no container status carried an identity.
    Construction supplies no proof. @spec PROTECTED-COMMISSIONING-8.
    """

    pod_uid: str
    resource_version: str
    evidence: UnstartedEvidence

    def __post_init__(self) -> None:
        _scalar(self.pod_uid, "uuid")
        _scalar(self.resource_version, "version")
        _require(type(self.evidence) is str and self.evidence in {"unscheduled", "terminal"})


@_closed_record
class CommissioningLaunchSnapshot:
    """Immutable observational row; cannot be used as a positive capability."""

    launch_id: str
    worker_principal_id: str
    binding: CampaignLaunchBinding
    creation_budget_ms: int
    revision: str
    state: State
    boot_id: str
    objects: OwnedObjects
    assignment: AssignmentMetadata | None
    owned_epoch: None
    termination: TerminationObservation | None
    unstarted: UnstartedObservation | None
    retirement_outcome: Literal["cleanup-recorded"] | None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS campaign (
 id INTEGER PRIMARY KEY CHECK(id=1), permit_digest TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS launches (
 launch_id TEXT PRIMARY KEY, worker_principal_id TEXT NOT NULL,
 permit_digest TEXT NOT NULL, launch_request_digest TEXT NOT NULL,
 projection_digest TEXT NOT NULL, launch_reservation_id TEXT NOT NULL UNIQUE,
 create_operation_id TEXT NOT NULL UNIQUE, guard_control_id TEXT NOT NULL,
 guard_revision TEXT NOT NULL, guard_config_digest TEXT NOT NULL,
 creation_budget_ms INTEGER NOT NULL CHECK(creation_budget_ms BETWEEN 1 AND 120000),
 revision INTEGER NOT NULL CHECK(revision>0), state TEXT NOT NULL
 CHECK(state IN ('preparing','idle','retiring','uncertain','retired')),
 boot_id TEXT NOT NULL, retirement_outcome TEXT
 CHECK(retirement_outcome IS NULL OR retirement_outcome='cleanup-recorded')
) STRICT;
CREATE TABLE IF NOT EXISTS objects (
 launch_id TEXT PRIMARY KEY REFERENCES launches(launch_id), namespace TEXT NOT NULL,
 pod_name TEXT NOT NULL UNIQUE, service_name TEXT NOT NULL UNIQUE,
 secret_name TEXT NOT NULL UNIQUE,
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
CREATE TABLE IF NOT EXISTS unstarted (
 launch_id TEXT PRIMARY KEY REFERENCES launches(launch_id), pod_uid TEXT NOT NULL,
 resource_version TEXT NOT NULL,
 evidence TEXT NOT NULL CHECK(evidence IN ('unscheduled','terminal'))
) STRICT;
"""

# SQL keys are constants, never assembled from caller strings.
_SET_UID = {
    "pod": "UPDATE objects SET pod_uid=?,pod_resource_version=? WHERE launch_id=?",
    "service": "UPDATE objects SET service_uid=?,service_resource_version=? WHERE launch_id=?",
    "secret": "UPDATE objects SET secret_uid=?,secret_resource_version=? WHERE launch_id=?",
}


class CommissioningLaunchRegistry:
    """Single-host lock plus SQLite for one campaign permit, beside production rows.

    The explicit private root must be owned by the mediator principal. Its files
    are distinct from the production registry's, and the first open pins the
    permit digest: a registry never holds two campaigns. Reopen moves every live
    launch to ``retiring``; a restarted mediator never resumes a creation.
    """

    __slots__ = ("_root", "_root_fd", "_lock_fd", "_db_fd", "_db", "_mutex", "_boot_id", "_permit")

    _root: str
    _root_fd: int
    _lock_fd: int
    _db_fd: int
    _db: sqlite3.Connection | None
    _mutex: threading.RLock
    _boot_id: str
    _permit: str

    @_safe
    def __init__(self) -> None:
        raise RegistryInvalid()

    @classmethod
    @_safe
    def open(cls, root: str, *, boot_id: str, permit_digest: str) -> CommissioningLaunchRegistry:
        _scalar(boot_id, "uuid")
        _scalar(permit_digest, "digest")
        instance = object.__new__(cls)
        instance._root, instance._boot_id, instance._permit = root, boot_id, permit_digest
        instance._root_fd = instance._lock_fd = instance._db_fd = -1
        instance._db = None
        instance._mutex = threading.RLock()
        try:
            instance._root_fd = _open_root(root)
            instance._lock_fd = _open_file(instance._root_fd, "commissioning.lock")
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
                pinned = db.execute("SELECT permit_digest FROM campaign WHERE id=1").fetchone()
                if pinned is None:
                    db.execute("INSERT INTO campaign VALUES(1,?)", (permit_digest,))
                elif pinned["permit_digest"] != permit_digest:
                    raise RegistryConflict()
                db.execute(
                    "UPDATE launches SET state='retiring',revision=revision+1 "
                    "WHERE state IN ('preparing','idle')"
                )
            return instance
        except (RegistryError, sqlite3.Error, OSError, ValueError, TypeError, OverflowError):
            instance.close()
            raise

    def __repr__(self) -> str:
        return "<CommissioningLaunchRegistry>"

    def __enter__(self) -> CommissioningLaunchRegistry:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def permit_digest(self) -> str:
        return self._permit

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

    def _current_preparing(self, row: sqlite3.Row) -> None:
        if row["boot_id"] != self._boot_id or row["state"] != "preparing":
            raise RegistryConflict()

    @staticmethod
    def _snapshot(db: sqlite3.Connection, row: sqlite3.Row) -> CommissioningLaunchSnapshot:
        launch_id = row["launch_id"]
        obj = db.execute("SELECT * FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)).fetchone()
        assignment = db.execute(
            "SELECT * FROM assignments WHERE launch_id=? LIMIT 1", (launch_id,)
        ).fetchone()
        termination = db.execute(
            "SELECT * FROM terminations WHERE launch_id=? LIMIT 1", (launch_id,)
        ).fetchone()
        unstarted = db.execute(
            "SELECT * FROM unstarted WHERE launch_id=? LIMIT 1", (launch_id,)
        ).fetchone()
        return CommissioningLaunchSnapshot(
            launch_id=launch_id,
            worker_principal_id=row["worker_principal_id"],
            binding=CampaignLaunchBinding(
                **{field.name: row[field.name] for field in fields(CampaignLaunchBinding)}
            ),
            creation_budget_ms=row["creation_budget_ms"],
            revision=str(row["revision"]),
            state=row["state"],
            boot_id=row["boot_id"],
            objects=OwnedObjects(**{field.name: obj[field.name] for field in fields(OwnedObjects)}),
            assignment=None
            if assignment is None
            else AssignmentMetadata(
                **{field.name: assignment[field.name] for field in fields(AssignmentMetadata)}
            ),
            owned_epoch=None,
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
            retirement_outcome=row["retirement_outcome"],
        )

    def _bump(
        self, db: sqlite3.Connection, owner: str, launch_id: str
    ) -> CommissioningLaunchSnapshot:
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
        launch_id: str,
        binding: CampaignLaunchBinding,
        namespace: str,
        creation_budget_ms: int,
    ) -> CommissioningLaunchSnapshot:
        """Persist ``preparing`` and the deterministic names before any effect.

        An identical replay on the same boot returns the row; any changed member
        conflicts, and a row from an earlier boot never resumes creation.
        """
        _scalar(worker_principal_id, "ref")
        _require(type(binding) is CampaignLaunchBinding)
        binding.__post_init__()
        _scalar(namespace, "name")
        _require(type(creation_budget_ms) is int and 1 <= creation_budget_ms <= 120000)
        pod, service, secret = object_names(launch_id)
        if binding.permit_digest != self._permit:
            raise RegistryConflict()
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM launches WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if existing is not None:
                obj = db.execute(
                    "SELECT namespace FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
                ).fetchone()
                if (
                    existing["worker_principal_id"] != worker_principal_id
                    or existing["creation_budget_ms"] != creation_budget_ms
                    or obj["namespace"] != namespace
                    or any(
                        existing[field.name] != getattr(binding, field.name)
                        for field in fields(CampaignLaunchBinding)
                    )
                ):
                    raise RegistryConflict()
                if existing["boot_id"] != self._boot_id or existing["state"] in {
                    "retiring",
                    "retired",
                }:
                    raise RegistryRetired()
                return self._snapshot(db, existing)
            if self._production_knows(launch_id, (pod, service, secret)):
                raise RegistryConflict()
            try:
                db.execute(
                    "INSERT INTO launches(launch_id,worker_principal_id,"
                    + ",".join(f.name for f in fields(CampaignLaunchBinding))
                    + ",creation_budget_ms,revision,state,boot_id) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,1,'preparing',?)",
                    (
                        launch_id,
                        worker_principal_id,
                        *(getattr(binding, f.name) for f in fields(CampaignLaunchBinding)),
                        creation_budget_ms,
                        self._boot_id,
                    ),
                )
                db.execute(
                    "INSERT INTO objects(launch_id,namespace,pod_name,service_name,secret_name) "
                    "VALUES(?,?,?,?,?)",
                    (launch_id, namespace, pod, service, secret),
                )
            except sqlite3.IntegrityError:
                raise RegistryConflict() from None
            return self._snapshot(db, self._owned(db, worker_principal_id, launch_id))

    def _production_knows(self, launch_id: str, names: tuple[str, str, str]) -> bool:
        """Whether the production registry in this root holds this launch or a name.

        An absent production registry knows nothing; an unreadable one refuses.
        """
        try:
            fd = os.open(
                "registry.sqlite", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._root_fd
            )
        except FileNotFoundError:
            return False
        try:
            info = os.fstat(fd)
            _require(
                stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.geteuid()
            )
        finally:
            os.close(fd)
        db = sqlite3.connect(
            "file:" + quote(self._root + "/registry.sqlite") + "?mode=ro", uri=True, timeout=0
        )
        try:
            known = db.execute(
                "SELECT 1 FROM launches WHERE launch_id=? UNION ALL SELECT 1 FROM objects "
                "WHERE pod_name IN (?,?,?) OR service_name IN (?,?,?) OR secret_name IN (?,?,?) "
                "LIMIT 1",
                (launch_id, *names, *names, *names),
            ).fetchone()
            return known is not None
        finally:
            db.close()

    @_safe
    def get(self, worker_principal_id: str, launch_id: str) -> CommissioningLaunchSnapshot:
        """Retained owner metadata remains readable after positive proof loss."""
        with self._transaction() as db:
            return self._snapshot(db, self._owned(db, worker_principal_id, launch_id))

    @_safe
    def find(self, worker_principal_id: str, launch_id: str) -> CommissioningLaunchSnapshot | None:
        """Owner-scoped lookup that grants no capability."""
        try:
            return self.get(worker_principal_id, launch_id)
        except RegistryUnauthorized:
            return None

    @_safe
    def enumerate_retiring(
        self, *, limit: int, after_launch_id: str | None = None
    ) -> tuple[CommissioningLaunchSnapshot, ...]:
        """Bounded keyset sweep of retiring rows for restart cleanup."""
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
    ) -> CommissioningLaunchSnapshot:
        """``preparing`` to ``idle``, and any live state to ``retiring``.

        Retirement has its own recording APIs and never follows a transition.
        """
        _require(type(state) is str and state in {"idle", "retiring", "retired"})
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if state == "retired":
                raise RegistryConflict()
            if row["state"] in {"retiring", "uncertain", "retired"}:
                raise RegistryRetired()
            if state == "idle":
                self._current_preparing(row)
            db.execute("UPDATE launches SET state=? WHERE launch_id=?", (state, launch_id))
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def mark_uncertain(
        self, worker_principal_id: str, launch_id: str, expected_revision: str
    ) -> CommissioningLaunchSnapshot:
        """Record an ambiguous create, distinct from an ordinary retirement.

        Only a guard-witnessed UID moves it on, to ``retiring``; restart keeps it.
        It blocks acceptance. @spec PROTECTED-COMMISSIONING-7/8.
        """
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if row["state"] not in {"preparing", "idle", "retiring"}:
                raise RegistryConflict()
            db.execute("UPDATE launches SET state='uncertain' WHERE launch_id=?", (launch_id,))
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_object_uid(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        kind: Kind,
        uid: str,
        resource_version: str,
    ) -> CommissioningLaunchSnapshot:
        """Record the UID of this boot's own create response, never a name lookup.

        A UID observed by name while retiring is a same-name object, not an
        owned one, and is refused here. @spec PROTECTED-COMMISSIONING-8.
        """
        _require(type(kind) is str and kind in _KINDS)
        _scalar(uid, "uuid")
        _scalar(resource_version, "version")
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            self._current_preparing(row)
            self._set_uid(db, launch_id, kind, uid, resource_version)
            return self._bump(db, worker_principal_id, launch_id)

    @staticmethod
    def _set_uid(
        db: sqlite3.Connection, launch_id: str, kind: str, uid: str, resource_version: str
    ) -> None:
        obj = db.execute("SELECT * FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)).fetchone()
        if obj[kind + "_uid"] is not None:
            raise RegistryConflict()
        db.execute(_SET_UID[kind], (uid, resource_version, launch_id))

    @_safe
    def record_witnessed_uid(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        witness: GuardCreateWitness,
        resource_version: str,
    ) -> CommissioningLaunchSnapshot:
        """Resolve one ambiguous create from the guard's witness of its exact operation.

        Only while ``retiring`` or ``uncertain``, only for an unknown UID, and
        only for the recorded create operation ID and guard identity. Only a
        Pod witness moves an ``uncertain`` launch to ``retiring``; a Service or
        Secret witness is recorded without changing the state. The caller must also observe
        the object at the exact recorded name with the witnessed UID. This
        storage API authenticates neither. @spec PROTECTED-COMMISSIONING-7/8.
        """
        _require(type(witness) is GuardCreateWitness)
        witness.__post_init__()
        _scalar(resource_version, "version")
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if row["state"] not in {"retiring", "uncertain"} or any(
                row[name] != getattr(witness, name)
                for name in (
                    "create_operation_id",
                    "guard_control_id",
                    "guard_revision",
                    "guard_config_digest",
                )
            ):
                raise RegistryConflict()
            self._set_uid(db, launch_id, witness.kind, witness.uid, resource_version)
            if witness.kind == "pod":
                # Workload binding W is the Pod: only its witness clears uncertainty.
                db.execute("UPDATE launches SET state='retiring' WHERE launch_id=?", (launch_id,))
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_container_identity(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        container_id: str,
        node_uid: str,
    ) -> CommissioningLaunchSnapshot:
        """Retain exact runner identity for authenticated termination cleanup."""
        _scalar(container_id, "container")
        _scalar(node_uid, "uuid")
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            if row["state"] not in {"preparing", "retiring"}:
                raise RegistryConflict()
            obj = db.execute(
                "SELECT pod_uid,container_id FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if (
                obj["pod_uid"] is None
                or obj["container_id"] is not None
                or self._unstarted(db, launch_id)
            ):
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
    ) -> CommissioningLaunchSnapshot:
        """Hash and reference metadata only; the Secret itself is mediator owned."""
        _require(type(assignment) is AssignmentMetadata)
        assignment.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            self._current_preparing(row)
            existing = db.execute(
                "SELECT launch_id FROM assignments WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if existing is not None:
                raise RegistryConflict()
            db.execute(
                "INSERT INTO assignments VALUES(?,?,?,?,?,?,?,?,?)",
                (launch_id, *(getattr(assignment, f.name) for f in fields(AssignmentMetadata))),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_termination_observation(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        observation: TerminationObservation,
    ) -> CommissioningLaunchSnapshot:
        """Persist the mediator's exact runner facts before finalizer removal."""
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
            if existing is not None or self._unstarted(db, launch_id):
                raise RegistryConflict()
            db.execute(
                "INSERT INTO terminations VALUES(?,?,?,?,?,?)",
                (
                    launch_id,
                    *(getattr(observation, f.name) for f in fields(TerminationObservation)),
                ),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @staticmethod
    def _unstarted(db: sqlite3.Connection, launch_id: str) -> bool:
        found = db.execute("SELECT 1 FROM unstarted WHERE launch_id=? LIMIT 1", (launch_id,))
        return found.fetchone() is not None

    @_safe
    def record_unstarted(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        observation: UnstartedObservation,
    ) -> CommissioningLaunchSnapshot:
        """Persist that the owned Pod's runner never started, before finalizer removal.

        Only for a retiring row with every UID known and neither a recorded
        container nor a termination: a recorded container is proved by its
        own termination. @spec PROTECTED-COMMISSIONING-8.
        """
        _require(type(observation) is UnstartedObservation)
        observation.__post_init__()
        with self._transaction() as db:
            row = self._cas(db, worker_principal_id, launch_id, expected_revision)
            obj = db.execute(
                "SELECT * FROM objects WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            terminated = db.execute(
                "SELECT 1 FROM terminations WHERE launch_id=? LIMIT 1", (launch_id,)
            ).fetchone()
            if (
                row["state"] != "retiring"
                or obj["service_uid"] is None
                or obj["secret_uid"] is None
                or obj["pod_uid"] != observation.pod_uid
                or obj["container_id"] is not None
                or terminated is not None
                or self._unstarted(db, launch_id)
            ):
                raise RegistryConflict()
            db.execute(
                "INSERT INTO unstarted VALUES(?,?,?,?)",
                (launch_id, *(getattr(observation, f.name) for f in fields(UnstartedObservation))),
            )
            return self._bump(db, worker_principal_id, launch_id)

    @_safe
    def record_cleanup_receipt(
        self,
        worker_principal_id: str,
        launch_id: str,
        expected_revision: str,
        receipt: CleanupReceipt,
    ) -> CommissioningLaunchSnapshot:
        """Retire only over exact known UIDs and the recorded runner termination.

        A runner that never started has its recorded never-started observation
        instead; nothing else stands in for a termination.
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
                "SELECT pod_uid FROM terminations WHERE launch_id=? "
                "UNION ALL SELECT pod_uid FROM unstarted WHERE launch_id=? LIMIT 1",
                (launch_id, launch_id),
            ).fetchone()
            if (
                row["state"] != "retiring"
                or terminated is None
                or any(obj[f.name] != getattr(receipt, f.name) for f in fields(CleanupReceipt))
                or terminated["pod_uid"] != receipt.pod_uid
            ):
                raise RegistryConflict()
            db.execute(
                "UPDATE launches SET state='retired',retirement_outcome='cleanup-recorded' "
                "WHERE launch_id=?",
                (launch_id,),
            )
            return self._bump(db, worker_principal_id, launch_id)
