"""The credential source seam (ADR 0239 "Static sources, OAuth and revocation").

Admission decides WHETHER a call may have a credential; a source decides WHAT
it is. Each admitted binding is classified into one kind and the registered
source for that kind releases it, inside the admission transaction, from the
row the transaction just read. A source does local work only (decrypt and
parse), never I/O: the transaction holds the application and binding share
locks until it commits, and nothing slow may sit under them.

Kinds:

1. ``static``: a platform-custody binding whose ciphertext is the v1
   envelope the CLI import seals, released field by field.
2. ``connector``: connector custody ("Custody" 6, Draft ADR 0158's scoped
   clarification). The resolver holds no material and answers identity-only
   admission; the connector then reads its own binding-indexed store.
3. ``refresh``: a platform binding with a durable refresh intent. Its source
   is a ``RefreshCapableSource`` (``refresh.py``), which may answer
   ``RefreshNeeded`` so the service runs the fenced refresh protocol outside
   this transaction.

The frozen v1 envelope::

    {"version": 1, "slots": {"<slot>": {"<field>": {"text": "<str>"} | {"bytes": "<b64>"}}}}

Field names match ``[A-Za-z_][A-Za-z0-9_]{0,63}`` and each slot has at least
one field. A malformed envelope or another version is ``source_unavailable``;
an absent slot is ``slot_unbound``. Nothing raised here carries plaintext, and
``repr`` of a field or release shows no value.
"""

from __future__ import annotations

import binascii
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .admission import Admitted
from .keys import KeyRing, KeyUnavailable
from .wire import ResolveRefusal

KIND_STATIC = "static"
KIND_REFRESH = "refresh"
KIND_CONNECTOR = "connector"

ENVELOPE_VERSION = 1
_FIELD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class SourceRefusal(ResolveRefusal):
    """A source's refusal; ``code`` is a wire refusal code."""


@dataclass(frozen=True)
class Field:
    """One credential field: exactly one of ``text`` or ``data`` is set."""

    text: str | None = field(default=None, repr=False)
    data: bytes | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (self.text is None) == (self.data is None):
            raise ValueError("a credential field holds exactly one of text or data")


@dataclass(frozen=True)
class Release:
    """What a source releases. ``fields`` None is identity-only admission."""

    fields: Mapping[str, Field] | None = field(repr=False)


@dataclass(frozen=True)
class RefreshNeeded:
    """The material must be refreshed first. Only a refresh-capable source answers it."""


class CredentialSource(ABC):
    kind: ClassVar[str]

    @abstractmethod
    def release(self, admitted: Admitted, slot: str, keys: KeyRing) -> Release | RefreshNeeded:
        """Release ``slot`` of ``admitted`` inside the admission transaction.

        Local work only. Raise ``SourceRefusal`` with a wire code to refuse.
        """


def classify(admitted: Admitted) -> str:
    if admitted.custody == "connector":
        return KIND_CONNECTOR
    if admitted.refresh is not None:
        return KIND_REFRESH
    return KIND_STATIC


class SourceRegistry:
    """At most one source per kind; a missing kind is ``source_unavailable``."""

    def __init__(self) -> None:
        self._sources: dict[str, CredentialSource] = {}

    def register(self, source: CredentialSource) -> None:
        if not isinstance(source, CredentialSource):
            raise ValueError("a source must be a CredentialSource")
        kind = getattr(source, "kind", None)
        if not isinstance(kind, str) or not kind:
            raise ValueError("a source must declare its kind")
        if kind in self._sources:
            raise ValueError(f"a {kind} source is already registered")
        self._sources[kind] = source

    def get(self, kind: str) -> CredentialSource:
        source = self._sources.get(kind)
        if source is None:
            raise SourceRefusal("source_unavailable")
        return source

    def kinds(self) -> frozenset[str]:
        return frozenset(self._sources)


def _strict_b64(text: str) -> bytes:
    try:
        return binascii.a2b_base64(text.encode("ascii"), strict_mode=True)
    except (UnicodeEncodeError, binascii.Error):
        raise SourceRefusal("source_unavailable") from None


def _field(value: Any) -> Field:
    if not isinstance(value, dict) or len(value) != 1:
        raise SourceRefusal("source_unavailable")
    ((form, content),) = value.items()
    if not isinstance(content, str):
        raise SourceRefusal("source_unavailable")
    if form == "text":
        return Field(text=content)
    if form == "bytes":
        return Field(data=_strict_b64(content))
    raise SourceRefusal("source_unavailable")


def parse_envelope(plaintext: bytes, slot: str) -> Mapping[str, Field]:
    """The fields of ``slot`` in a v1 envelope."""

    try:
        document = json.loads(plaintext.decode("utf-8"))
    except ValueError:
        raise SourceRefusal("source_unavailable") from None
    if not isinstance(document, dict) or set(document) != {"version", "slots"}:
        raise SourceRefusal("source_unavailable")
    version = document["version"]
    slots = document["slots"]
    if type(version) is not int or version != ENVELOPE_VERSION or not isinstance(slots, dict):
        raise SourceRefusal("source_unavailable")
    if slot not in slots:
        raise SourceRefusal("slot_unbound")
    fields = slots[slot]
    if not isinstance(fields, dict) or not fields:
        raise SourceRefusal("source_unavailable")
    parsed: dict[str, Field] = {}
    for name, value in fields.items():
        if _FIELD_NAME.fullmatch(name) is None:
            raise SourceRefusal("source_unavailable")
        parsed[name] = _field(value)
    return parsed


def open_material(admitted: Admitted, keys: KeyRing) -> bytes:
    """The binding's plaintext; ``key_unavailable`` when no held key opens it."""

    if admitted.recipient_id is None or admitted.ciphertext is None:
        raise SourceRefusal("source_unavailable")
    try:
        return keys.open(admitted.recipient_id, admitted.ciphertext)
    except KeyUnavailable:
        raise SourceRefusal("key_unavailable") from None


class StaticSource(CredentialSource):
    kind = KIND_STATIC

    def release(self, admitted: Admitted, slot: str, keys: KeyRing) -> Release | RefreshNeeded:
        return Release(fields=parse_envelope(open_material(admitted, keys), slot))


class ConnectorIdentitySource(CredentialSource):
    kind = KIND_CONNECTOR

    def release(self, admitted: Admitted, slot: str, keys: KeyRing) -> Release | RefreshNeeded:
        return Release(fields=None)


def default_registry() -> SourceRegistry:
    registry = SourceRegistry()
    registry.register(StaticSource())
    registry.register(ConnectorIdentitySource())
    return registry
