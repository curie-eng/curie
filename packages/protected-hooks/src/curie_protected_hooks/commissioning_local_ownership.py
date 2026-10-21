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

import json
import os
import re
import secrets
from collections.abc import Iterator, Sequence
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
# Flags that would point a command at another cluster, user or profile.
_KUBECTL_SELECTION = frozenset({"--context", "--kubeconfig", "--cluster", "--user"})
_PROFILE_SELECTION = frozenset({"-p", "--profile"})


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
        ledger = cls(root, owner, [])
        ledger._save()
        return ledger

    @classmethod
    def open(cls, root: Path) -> Self:
        """Reopen after a restart; tickets do not survive it, @spec PROTECTED-COMMISSIONING-5."""
        try:
            value = json.loads((root / _LEDGER).read_text(encoding="utf-8"))
            _require(type(value) is dict and value.keys() == {"owner", "entries"})
            _require(valid_owner(value["owner"]) and type(value["entries"]) is list)
            entries = [_entry(entry) for entry in value["entries"]]
            keys = {(entry["kind"], entry["key"]) for entry in entries}
            _require(len(keys) == len(entries))
            return cls(root, value["owner"], entries)
        except OwnershipRefused:
            raise
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            _refuse()

    def _save(self) -> None:
        path = self.root / _LEDGER
        temporary = self.root / (_LEDGER + ".tmp")
        raw = json.dumps({"owner": self.owner, "entries": self._entries}, indent=2) + "\n"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

    def _find(self, kind: str, key: str) -> dict[str, Any]:
        entry = next((e for e in self._entries if e["kind"] == kind and e["key"] == key), None)
        _require(entry is not None)
        assert entry is not None
        return entry

    def intend(self, kind: str, key: str) -> CreateTicket:
        """Record ownership durably, then return the create ticket.

        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(kind in KINDS and _text(key))
        _require(all(e["kind"] != kind or e["key"] != key for e in self._entries))
        self._entries.append({"kind": kind, "key": key, "state": "intended", "identity": {}})
        self._save()
        return CreateTicket(kind, key, self._token)

    def bind(self, ticket: CreateTicket, **identity: str) -> None:
        """Bind the observed identity of a create made with this ticket.

        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(type(ticket) is CreateTicket and ticket._ledger == self._token)
        entry = self._find(ticket.kind, ticket.key)
        _require(entry["state"] == "intended")
        entry["state"], entry["identity"] = "created", _identity(identity)
        self._save()

    def uncertain(self, kind: str, key: str) -> None:
        """An ambiguous or interrupted create, @spec PROTECTED-COMMISSIONING-5."""
        entry = self._find(kind, key)
        _require(entry["state"] == "intended")
        entry["state"] = "uncertain"
        self._save()

    def resolve(self, kind: str, key: str, **identity: str) -> None:
        """Bind an uncertain create found by its exact recorded name and owner.

        A created entry is never rebound, so a same-name object is never
        adopted. @spec PROTECTED-COMMISSIONING-5.
        """
        entry = self._find(kind, key)
        _require(entry["state"] == "uncertain")
        entry["state"], entry["identity"] = "created", _identity(identity)
        self._save()

    def removed(self, kind: str, key: str) -> None:
        """Record independently confirmed absence; terminal, @spec PROTECTED-COMMISSIONING-5."""
        entry = self._find(kind, key)
        # An intended entry may have been created; it must first become uncertain.
        _require(entry["state"] in ("created", "uncertain"))
        entry["state"] = "removed"
        self._save()

    def identity(self, kind: str, key: str) -> dict[str, str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        return dict(self._find(kind, key)["identity"])

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


def _flag(argument: str) -> str:
    return argument.split("=", 1)[0]


def refuse_cluster_selection(argv: Sequence[str]) -> None:
    """Refuse operator arguments that select a cluster, kubeconfig or profile.

    The operator command creates its own profile and private kubeconfig; it
    never acts on a caller's context. @spec PROTECTED-COMMISSIONING-5.
    """
    _require(all(type(argument) is str for argument in argv))
    selection = _KUBECTL_SELECTION | _PROFILE_SELECTION
    _require(not any(_flag(argument) in selection for argument in argv))


class ClusterCommands:
    """Argv bound to this installation's profile and private kubeconfig.

    Building argv runs nothing. @spec PROTECTED-COMMISSIONING-5.
    """

    def __init__(self, profile: str, *, kubeconfig: Path) -> None:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(valid_owner(profile) and isinstance(kubeconfig, Path))
        self.profile, self.kubeconfig = profile, kubeconfig

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "ClusterCommands()"

    def environment_overrides(self) -> dict[str, str]:
        """The operator's own kubeconfig is never read or changed.

        @spec PROTECTED-COMMISSIONING-5.
        """
        return {"KUBECONFIG": str(self.kubeconfig)}

    def kubectl_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(all(type(a) is str for a in args))
        _require(not any(_flag(a) in _KUBECTL_SELECTION for a in args))
        _require(not (args and args[0] == "config"))
        return ["kubectl", "--context", self.profile, "--kubeconfig", str(self.kubeconfig), *args]

    def minikube_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(all(type(a) is str for a in args))
        _require(not any(_flag(a) in _PROFILE_SELECTION or a == "--all" for a in args))
        return ["minikube", "-p", self.profile, *args]

    def docker_argv(self, *args: str) -> list[str]:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(all(type(a) is str for a in args))
        _require("prune" not in args and not (args and args[0] == "system"))
        return ["docker", *args]
