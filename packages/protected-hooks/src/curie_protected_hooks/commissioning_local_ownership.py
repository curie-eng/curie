"""Provisioner ownership ledger and explicit-context argv, @spec PROTECTED-COMMISSIONING-5.

ADR 0236 makes the provisioner register ownership of every resource before it
is created. The ledger records each resource as ``intended`` durably before a
create ticket exists, binds the observed identity once, and never adopts a
same-name object. Cleanup and the independent absence check act only on exact
recorded identities. The argv builders pin this installation's private
kubeconfig and profile and refuse caller-supplied cluster selection and global
prune operations. Nothing here runs a command or calls a cluster.
"""

from __future__ import annotations

import copy
import json
import os
import re
import secrets
import stat
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, Self

__all__ = [
    "KINDS",
    "ClusterCommands",
    "CreateTicket",
    "Entry",
    "OwnershipLedger",
    "OwnershipRefused",
    "fresh_owner",
    "refuse_cluster_selection",
    "valid_owner",
]

KINDS = frozenset(
    {
        "minikube-profile",
        "docker-network",
        "docker-container",
        "docker-volume",
        "image-tag",
        "node-path",
        "kubernetes",
    }
)
_STATES = frozenset({"intended", "created", "uncertain", "removed"})
_OWNER = re.compile(r"curie-commission-[0-9a-f]{12}")
_LEDGER = "ownership.json"
_MAX_KEY = 512
# Operator arguments: every spelling that selects another cluster, credential,
# profile or Docker daemon. The argv builders below use allowlists instead.
_SELECTION = frozenset(
    {
        "--context",
        "--kubeconfig",
        "--cluster",
        "--user",
        "--server",
        "-s",
        "--token",
        "--as",
        "--as-group",
        "--as-uid",
        "--username",
        "--password",
        "--certificate-authority",
        "--client-certificate",
        "--client-key",
        "--tls-server-name",
        "--insecure-skip-tls-verify",
        "-p",
        "--profile",
        "-H",
        "--host",
    }
)
# Subcommands and flags each tool may receive; anything else refuses. The first
# argument must be the subcommand, so no global flag precedes it.
_KUBECTL_COMMANDS = frozenset(
    {
        "get",
        "version",
        "apply",
        "create",
        "delete",
        "patch",
        "wait",
        "rollout",
        "describe",
        "logs",
        "label",
        "port-forward",
    }
)
_KUBECTL_FLAGS = frozenset(
    {
        "-n",
        "--namespace",
        "-o",
        "--output",
        "-f",
        "--filename",
        "-l",
        "--selector",
        "--field-selector",
        "--timeout",
        "--request-timeout",
        "--for",
        "--ignore-not-found",
        "--grace-period",
        "--server-side",
        "--field-manager",
        "--force-conflicts",
        "--type",
        "-p",
        "--patch",
        "--subresource",
        "--overwrite",
        "--duration",
        "--container",
        "--previous",
    }
)
_MINIKUBE_COMMANDS = frozenset({"start", "status", "stop", "delete", "ip", "image", "addons"})
_MINIKUBE_FLAGS = frozenset(
    {
        "--driver",
        "--container-runtime",
        "--kubernetes-version",
        "--cni",
        "--network",
        "--keep-context",
        "--memory",
        "--cpus",
        "--interactive",
        "--wait",
        "--wait-timeout",
        "--nodes",
        "-o",
        "--output",
    }
)
_DOCKER_COMMANDS = frozenset(
    {
        "network",
        "container",
        "volume",
        "image",
        "inspect",
        "run",
        "rm",
        "ps",
        "tag",
        "push",
        "pull",
        "save",
        "load",
        "build",
    }
)
_DOCKER_FLAGS = frozenset(
    {
        "--label",
        "--filter",
        "--format",
        "-f",
        "--force",
        "--type",
        "--name",
        "--network",
        "-d",
        "--detach",
        "--publish",
        "--volume",
        "-v",
        "--rm",
        "--restart",
        "-t",
        "--tag",
        "--driver",
        "-q",
        "--quiet",
        "-o",
        "--output",
        "-i",
        "--input",
        "--platform",
        "--read-only",
    }
)
_DOCKER_FORBIDDEN_WORDS = frozenset({"prune", "system", "context"})
# Ambient variables that select a cluster, profile or daemon; cleared, then pinned.
_AMBIENT_PREFIXES = ("DOCKER_", "MINIKUBE_", "KUBECTL_", "KUBE")


class OwnershipRefused(RuntimeError):
    """Redacted ownership refusal, @spec PROTECTED-COMMISSIONING-5."""

    def __init__(self) -> None:
        """Never echo a kind, key or identity, @spec PROTECTED-COMMISSIONING-5."""
        super().__init__("Commissioning ownership refused")


def _refuse() -> NoReturn:
    raise OwnershipRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def _text(value: Any) -> bool:
    return type(value) is str and 0 < len(value) <= _MAX_KEY


def valid_owner(owner: Any) -> bool:
    """@spec PROTECTED-COMMISSIONING-5."""
    return type(owner) is str and _OWNER.fullmatch(owner) is not None


def fresh_owner() -> str:
    """A fresh installation owner, also its profile and network name.

    @spec PROTECTED-COMMISSIONING-5.
    """
    return "curie-commission-" + secrets.token_hex(6)


@dataclass(frozen=True, slots=True)
class Entry:
    """One recorded resource, @spec PROTECTED-COMMISSIONING-5."""

    kind: str
    key: str
    state: str
    identity: dict[str, str]


@dataclass(frozen=True, slots=True)
class CreateTicket:
    """Proof that a resource was registered before its create, @spec PROTECTED-COMMISSIONING-5."""

    kind: str
    key: str
    _ledger: str


def _identity(value: Any) -> dict[str, str]:
    _require(type(value) is dict and bool(value))
    _require(all(_text(key) and _text(item) for key, item in value.items()))
    return dict(value)


def _entry(value: Any) -> dict[str, Any]:
    _require(type(value) is dict and value.keys() == {"kind", "key", "state", "identity"})
    _require(value["kind"] in KINDS and _text(value["key"]) and value["state"] in _STATES)
    identity = value["identity"]
    _require(type(identity) is dict)
    if value["state"] == "created":
        _identity(identity)
    elif value["state"] == "removed":
        # Removed from created keeps its identity; removed from uncertain has none.
        _require(identity == {} or bool(_identity(identity)))
    else:
        _require(identity == {})
    entry: dict[str, Any] = value
    return entry


def _find(entries: list[dict[str, Any]], kind: str, key: str) -> dict[str, Any]:
    entry = next((e for e in entries if e["kind"] == kind and e["key"] == key), None)
    _require(entry is not None)
    assert entry is not None
    return entry


def _private_directory(root: Path) -> None:
    """A real directory, not a symlink, owned by this user with mode 0700."""
    info = os.lstat(root)
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid())
    _require(stat.S_IMODE(info.st_mode) == 0o700)


def _read_private(path: Path) -> str:
    """Read a regular 0600 file owned by this user without following a symlink."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid())
        _require(stat.S_IMODE(info.st_mode) == 0o600)
        return stream.read()


class OwnershipLedger:
    """Register-before-create ledger of one installation, @spec PROTECTED-COMMISSIONING-5."""

    def __init__(self, root: Path, owner: str, entries: list[dict[str, Any]]) -> None:
        """Use ``create`` or ``open``, @spec PROTECTED-COMMISSIONING-5."""
        self.root, self.owner, self._entries = root, owner, entries
        # Tickets are valid only for this open ledger, never after a restart.
        self._token = secrets.token_hex(16)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "OwnershipLedger()"

    @classmethod
    def create(cls, root: Path, *, owner: str) -> Self:
        """A new private ledger directory; an existing one refuses.

        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(valid_owner(owner))
        try:
            root.mkdir(mode=0o700)
        except FileExistsError:
            _refuse()
        root.chmod(0o700)
        _private_directory(root)
        ledger = cls(root, owner, [])
        ledger._save(ledger._entries)
        return ledger

    @classmethod
    def open(cls, root: Path, *, owner: str) -> Self:
        """Reopen this installation's ledger after a restart.

        Tickets do not survive it. @spec PROTECTED-COMMISSIONING-5.
        """
        try:
            _require(valid_owner(owner))
            _private_directory(root)
            value = json.loads(_read_private(root / _LEDGER))
            _require(type(value) is dict and value.keys() == {"owner", "entries"})
            _require(value["owner"] == owner and type(value["entries"]) is list)
            entries = [_entry(entry) for entry in value["entries"]]
            keys = {(entry["kind"], entry["key"]) for entry in entries}
            _require(len(keys) == len(entries))
            return cls(root, owner, entries)
        except OwnershipRefused:
            raise
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            _refuse()

    def _save(self, entries: list[dict[str, Any]]) -> None:
        """Durably replace the ledger file; the caller swaps memory only after."""
        path = self.root / _LEDGER
        temporary = self.root / (_LEDGER + ".tmp")
        raw = json.dumps({"owner": self.owner, "entries": entries}, indent=2) + "\n"
        # A leftover or planted temporary is removed, never written through.
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _update(self, kind: str, key: str, states: tuple[str, ...], **changes: Any) -> None:
        """Change one entry in a copy, save it, then swap memory."""
        entries = copy.deepcopy(self._entries)
        entry = _find(entries, kind, key)
        _require(entry["state"] in states)
        entry.update(changes)
        self._save(entries)
        self._entries = entries

    def intend(self, kind: str, key: str) -> CreateTicket:
        """Record ownership durably, then return the create ticket.

        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(kind in KINDS and _text(key))
        _require(all(e["kind"] != kind or e["key"] != key for e in self._entries))
        entries = [
            *copy.deepcopy(self._entries),
            {"kind": kind, "key": key, "state": "intended", "identity": {}},
        ]
        self._save(entries)
        self._entries = entries
        return CreateTicket(kind, key, self._token)

    def bind(self, ticket: CreateTicket, **identity: str) -> None:
        """Bind the observed identity of a create made with this ticket.

        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(type(ticket) is CreateTicket and ticket._ledger == self._token)
        self._update(
            ticket.kind, ticket.key, ("intended",), state="created", identity=_identity(identity)
        )

    def uncertain(self, kind: str, key: str) -> None:
        """An ambiguous or interrupted create, @spec PROTECTED-COMMISSIONING-5."""
        self._update(kind, key, ("intended",), state="uncertain")

    def resolve(self, kind: str, key: str, **identity: str) -> None:
        """Bind an uncertain create found by its exact recorded name and owner.

        The observed object must carry this installation's owner label. A
        created entry is never rebound, so a same-name object is never adopted.
        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(identity.get("owner") == self.owner)
        self._update(kind, key, ("uncertain",), state="created", identity=_identity(identity))

    def removed(self, kind: str, key: str) -> None:
        """Record independently confirmed absence; terminal, @spec PROTECTED-COMMISSIONING-5."""
        # An intended entry may have been created; it must first become uncertain.
        self._update(kind, key, ("created", "uncertain"), state="removed")

    def identity(self, kind: str, key: str) -> dict[str, str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        return dict(_find(self._entries, kind, key)["identity"])

    def entries(self) -> tuple[Entry, ...]:
        """@spec PROTECTED-COMMISSIONING-5."""
        return tuple(
            Entry(e["kind"], e["key"], e["state"], dict(e["identity"])) for e in self._entries
        )

    def pending_reverse(self) -> Iterator[Entry]:
        """Owned resources not yet removed, newest first, @spec PROTECTED-COMMISSIONING-5."""
        for entry in reversed(self.entries()):
            if entry.state != "removed":
                yield entry

    def all_removed(self) -> bool:
        """@spec PROTECTED-COMMISSIONING-5."""
        return all(e["state"] == "removed" for e in self._entries)


def _flag(argument: str) -> str | None:
    """The flag name an argument sets, including glued short forms, or None."""
    if not argument.startswith("-") or argument == "-":
        return None
    if argument.startswith("--"):
        return argument.split("=", 1)[0]
    return argument[:2]


def refuse_cluster_selection(argv: Sequence[str]) -> None:
    """Refuse operator arguments that select a cluster, credential, profile or daemon.

    The operator command creates its own profile and private kubeconfig; it
    never acts on a caller's context. @spec PROTECTED-COMMISSIONING-5.
    """
    _require(all(type(argument) is str for argument in argv))
    _require(not any(_flag(argument) in _SELECTION for argument in argv))


def _allowed(args: tuple[str, ...], commands: frozenset[str], flags: frozenset[str]) -> None:
    _require(all(type(a) is str for a in args))
    _require(bool(args) and args[0] in commands)
    for argument in args:
        flag = _flag(argument)
        _require(flag is None or flag in flags)


class ClusterCommands:
    """Argv bound to this installation's profile and private kubeconfig.

    Building argv runs nothing. Each tool takes only listed subcommands and
    flags. @spec PROTECTED-COMMISSIONING-5.
    """

    def __init__(self, profile: str, *, kubeconfig: Path) -> None:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(valid_owner(profile) and isinstance(kubeconfig, Path))
        self.profile, self.kubeconfig = profile, kubeconfig

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "ClusterCommands()"

    def environment(self, ambient: Mapping[str, str]) -> dict[str, str]:
        """A copy of ``ambient`` without cluster, profile or daemon selection.

        The operator's own kubeconfig is never read or changed. @spec PROTECTED-COMMISSIONING-5.
        """
        result = {k: v for k, v in ambient.items() if not k.startswith(_AMBIENT_PREFIXES)}
        result["KUBECONFIG"] = str(self.kubeconfig)
        result["MINIKUBE_PROFILE"] = self.profile
        return result

    def kubectl_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _allowed(args, _KUBECTL_COMMANDS, _KUBECTL_FLAGS)
        return ["kubectl", "--context", self.profile, "--kubeconfig", str(self.kubeconfig), *args]

    def minikube_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _allowed(args, _MINIKUBE_COMMANDS, _MINIKUBE_FLAGS)
        return ["minikube", "-p", self.profile, *args]

    def docker_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _allowed(args, _DOCKER_COMMANDS, _DOCKER_FLAGS)
        _require(not _DOCKER_FORBIDDEN_WORDS.intersection(args))
        return ["docker", *args]
