"""Resolve inbound channel attachment REFS into sandbox-readable objects.

The dispatcher records file *references* on the wire and nothing else:
``aci_protocol.Attachment`` carries ``id``/``name`` and deliberately no url and
no bytes, because a carried channel URL would invite a sandbox-side fetch that
ADR-0032's default-deny egress forbids.  This module is the other half.  The
worker holds the channel credential and can reach the channel (the bot token
for Slack, the adapter's egress secret for a channel-port binding, ADR-0153),
so it downloads each referenced file, parks the bytes in the private object
store under its OWN prefix, and mints a short-lived one-object read capability
that the sandbox's attachment init container redeems.  The credential (the bot
token, or an adapter's secret) never leaves the worker, which
is what keeps ADR-0075's boundary intact while the Agent Proxy is unbuilt.

Three properties are load-bearing and each has a reason it is not merely style:

* **The size cap is enforced WHILE STREAMING** (ADR-0059 decision 3), in the
  shape of ``curie_api.routers.bundles._read_bounded_upload``: the chunk loop
  refuses the moment the running total crosses the cap, so memory never holds
  more than one chunk past it.  "Read it all, then measure" would refuse the
  same files while buffering an arbitrary body first.

* **A resolve is ALL-OR-NOTHING.**  One failing entry refuses the whole set and
  removes the objects its neighbours already wrote.  A partial set is
  indistinguishable to an agent from a complete one -- it would read two of
  three spreadsheets and answer with total confidence about the whole upload --
  and an object under a key no ledger names is exactly what the retention sweep
  cannot reach.

* **Retention is a SIBLING ledger, not a tenant of the workspace one.**  The
  workspace ownership ledger is keyed by ``sha256(thread_key)`` under
  ``_ownership/``, decodes to a typed ``PreparedWorkspace``, and carries the
  ROUTE lease, refreshed on every affinity touch.  Attachment bytes answer a
  different question ("may a retry of this turn still fetch them?") on a
  different clock, so they get their own prefix and their own expiry field,
  swept from the SAME ``reap_orphans`` tick rather than folded into a
  thread-ownership authority.

The object-store port is the worker's existing ``WorkspaceObjectPort``: a
sandbox is handed a presigned URL for exactly one object, never an object-store
credential.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from aci_protocol import Attachment, BootEnv, ReplyHandle
from aci_protocol.turn import DEFAULT_IDENTITY, slack_speaking_identity

from .ledger_client import ThreadAttachmentRef
from .reply_sink import ADAPTER_SECRET_HEADER, SLACK_KIND
from .workspace import WorkspaceObjectPort

# The attachment lane's own object namespace. Never the workspace archives':
# the two object classes have different owners, different TTLs and different
# reapers, and a shared prefix would put one lane's sweep in reach of the
# other's bytes.
ATTACHMENT_OBJECT_PREFIX = "attachments"

# The sibling retention ledger, beside ``_ownership`` and deliberately not in
# it. The workspace enumeration decodes every record under its own prefix into
# a typed ``PreparedWorkspace``, so an attachment record sharing that prefix
# would turn the first resolve in a workspace-enabled deployment into a decode
# failure on every reap tick.
ATTACHMENT_LEDGER_PREFIX = "_attachments"

# The claim-env key carrying the minted capability set. Like
# ``CURIE_WORKSPACE_REF`` this is a substrate-local delivery input consumed by
# an init container, not a field the sandbox runner reads: the k8s substrate
# strips it off the runner container and re-emits it named at the attachment
# init container only.
ATTACHMENTS_REF_ENV = "CURIE_ATTACHMENTS_REF"

#: The claim-env key carrying the attachment manifest to the runner (ADR 0205
#: decision 8): each delivered file's on-disk name and whether it arrived on the
#: current message, plus the earlier files that are unavailable or omitted.
#: Unlike ``CURIE_ATTACHMENTS_REF`` it is a runner input (``BootEnv``'s
#: ``attachments_manifest``) and carries no URL, no id and no credential.
ATTACHMENTS_MANIFEST_ENV = BootEnv.env_key("attachments_manifest")

#: Where a resolved attachment lands inside the sandbox, on EVERY substrate.
#: Kubernetes reaches it through an init container and a mounted emptyDir;
#: Docker bind-mounts it directly. The runner probes this same path
#: (``curie_runner.__main__.ATTACHMENTS_DIR``) and the chart mounts its volume
#: there, so the three must agree -- a substrate that materialized files
#: somewhere else would leave the agent probing an empty directory and reporting
#: no attachment for a file that did arrive, which is the silent loss #2567
#: exists to close.
ATTACHMENTS_MOUNT_PATH = "/attachments"

# How much longer than one capability a parked copy must outlive the boot to be
# re-minted rather than fetched again: the init container redeems within the
# capability's life, and the margin absorbs claim latency and clock skew.
_REUSE_MARGIN_SECONDS = 60

# Where Slack's file metadata is looked up. The id in ``Attachment`` is the
# channel's own file id, so resolving it is the port's job, not this module's.
_SLACK_API_URL = "https://slack.com/api"

# A files.info envelope is metadata; nothing legitimate is large. Bounded so a
# hostile or broken response cannot be read into memory without limit.
_MAX_INFO_BYTES = 64 * 1024


class AttachmentResolutionError(RuntimeError):
    """An inbound attachment could not be made available to a sandbox."""

    def __init__(self, stage: str, detail: str) -> None:
        self.stage = stage
        super().__init__(f"attachment {stage} failed: {detail}")


class AttachmentTooLargeError(AttachmentResolutionError):
    """A download crossed the per-file cap and was refused mid-stream."""

    def __init__(self, name: str, max_file_bytes: int) -> None:
        self.name = name
        self.max_file_bytes = max_file_bytes
        super().__init__(
            "size-cap",
            f"{name!r} exceeds the {max_file_bytes} byte per-file cap",
        )


class AttachmentFetchError(AttachmentResolutionError):
    """The channel would not hand over one referenced file's bytes."""

    def __init__(self, name: str, detail: str) -> None:
        self.name = name
        super().__init__("fetch", f"{name!r} could not be downloaded: {detail}")


class SlackFileError(RuntimeError):
    """Slack refused a file lookup or download at the application level.

    Slack answers ``200`` with ``{"ok": false, "error": "..."}`` for an
    application-level failure, so translating that envelope belongs to the port
    rather than to the resolver above it. ``status`` is the HTTP status when the
    refusal was one, so an earlier file's unavailability can be named.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


@dataclass(frozen=True)
class AttachmentLimits:
    """Resource envelope for one turn's attachment resolution.

    Every bound is validated at construction, exactly as ``WorkspaceLimits``
    does: a zero cap that silently means "unlimited" is how a bounded ingestion
    path stops being bounded.
    """

    max_file_bytes: int = 32 * 1024 * 1024
    read_chunk_bytes: int = 1024 * 1024
    #: How long the minted one-object capability stays redeemable.
    reference_ttl_seconds: int = 300
    #: How long the parked bytes are retained for a redelivery of the turn.
    retention_ttl_seconds: int = 3600
    max_files: int = 10
    #: How many files one boot materializes for a whole thread (ADR 0205
    #: decision 7). Never below ``max_files``: a single message's files are
    #: always delivered whole, so a thread budget smaller than one message
    #: could only ever be exceeded.
    thread_max_files: int = 20
    #: The total recorded bytes one boot materializes for a thread. The chart
    #: checks it against the attachments volume, not this class.
    thread_max_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        numeric = {
            "max_file_bytes": self.max_file_bytes,
            "read_chunk_bytes": self.read_chunk_bytes,
            "reference_ttl_seconds": self.reference_ttl_seconds,
            "retention_ttl_seconds": self.retention_ttl_seconds,
            "max_files": self.max_files,
            "thread_max_files": self.thread_max_files,
            "thread_max_bytes": self.thread_max_bytes,
        }
        for name, value in numeric.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.thread_max_files < self.max_files:
            raise ValueError("thread_max_files must be at least max_files")


@dataclass(frozen=True)
class AttachmentRef:
    """One opaque claim-scoped read capability, shaped like ``WorkspaceRef``.

    ``sha256`` is of the bytes that actually landed in the store, never of a
    size the channel reported: the init container verifies this digest before it
    materializes the file, and a digest computed from anything else would make
    that verification a no-op.
    """

    name: str
    url: str
    sha256: str
    size_bytes: int
    expires_at_epoch: int
    mime_type: str | None = None

    @property
    def expires_in_seconds(self) -> int:
        return max(0, self.expires_at_epoch - int(time.time()))


def encode_attachment_refs(refs: Sequence[AttachmentRef]) -> str:
    """Pack a whole resolved set into one opaque claim-env value.

    One env var for the set rather than one per file: the init container reads a
    single value whose length does not encode how many files a person attached,
    and a per-file scheme would have to invent an index namespace on the claim.
    """

    payload = [
        {
            "n": ref.name,
            "u": ref.url,
            "s": ref.sha256,
            "b": ref.size_bytes,
            "e": ref.expires_at_epoch,
            "m": ref.mime_type,
        }
        for ref in refs
    ]
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_attachment_refs(value: str) -> tuple[AttachmentRef, ...]:
    """Unpack a claim-env value, refusing a malformed or unusable entry.

    Validated on the same terms as ``WorkspaceRef.decode``: an HTTP(S) url and a
    real hex digest, so an init container never fetches an arbitrary scheme or
    verifies against a digest that cannot match anything.
    """

    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        entries = json.loads(raw)
        if not isinstance(entries, list):
            raise TypeError("attachment reference payload is not a list")
        refs = tuple(
            AttachmentRef(
                name=str(entry["n"]),
                url=str(entry["u"]),
                sha256=str(entry["s"]),
                size_bytes=int(entry["b"]),
                expires_at_epoch=int(entry["e"]),
                mime_type=None if entry.get("m") is None else str(entry["m"]),
            )
            for entry in entries
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AttachmentResolutionError("reference", "invalid attachment reference") from exc
    for ref in refs:
        if not ref.url.startswith(("http://", "https://")):
            raise AttachmentResolutionError("reference", "attachment URL is not HTTP(S)")
        if len(ref.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in ref.sha256.lower()
        ):
            raise AttachmentResolutionError("reference", "attachment digest is invalid")
    return refs


def unique_attachment_leaf(leaf: str, taken: Container[str]) -> str:
    """The name this attachment gets on disk once ``taken`` is already there.

    Two people attaching ``report.pdf`` to one message is ordinary, and until
    this existed the second one landed on top of the first: the substrate wrote
    both to ``<mount>/report.pdf`` and the agent saw one file where two arrived,
    with nothing anywhere saying one had been destroyed. That is the same silent
    loss #2567 was filed to close, one layer down, so it is disambiguated rather
    than refused -- refusing would turn a routine upload into a dead turn.

    The scheme is a numeric suffix BEFORE the extension (``report.pdf``,
    ``report-2.pdf``, ``report-3.pdf``). The first file keeps the name the
    person sent, which is what the agent will be asked about by name; the
    extension survives, so anything selecting by suffix still works; and the
    collision is visible in the listing the runner announces rather than hidden.
    A per-attachment subdirectory was the alternative and was rejected: it
    changes the path shape of the overwhelmingly common single-file turn to pay
    for the rare one.

    The counter skips a name that is itself already taken, so an upload set of
    (``report.pdf``, ``report-2.pdf``, ``report.pdf``) lands as those two plus
    ``report-3.pdf`` rather than colliding on the rename.

    ``charts/curie/templates/agent-sandbox.yaml`` reimplements this inline --
    the init container is a rendered program and cannot import the worker -- and
    ``charts/curie/ci/attachment-init-behavior-assertions.sh`` executes that copy
    against the same case this one is tested on, so the two tiers cannot drift
    into naming the same set differently.
    """

    if leaf not in taken:
        return leaf
    stem, extension = os.path.splitext(leaf)
    bump = 2
    while f"{stem}-{bump}{extension}" in taken:
        bump += 1
    return f"{stem}-{bump}{extension}"


def clean_attachment_leaf(name: str) -> str:
    """The on-disk leaf for a channel-supplied file name (ADR 0205 decision 4).

    The attachments init container's own cleaning, ported: backslashes become
    slashes, the name is stripped, and only its basename survives, so a name
    can never climb out of the mount. One rule is added: a leading ``.`` becomes
    ``_``. A dot name would be hidden from the runner's discovery and could
    collide with the init container's own ``.curie-*`` status files, and a
    hidden attachment is exactly the silent loss #2567 exists to close.

    A name that cleans to nothing usable (empty, ``.`` or ``..``) is refused
    rather than invented, with stage ``name``.
    """

    leaf = name.replace("\\", "/").strip().rsplit("/", 1)[-1].strip()
    if leaf in {"", ".", ".."}:
        raise AttachmentResolutionError("name", f"attachment name {name!r} has no usable leaf")
    if leaf.startswith("."):
        leaf = "_" + leaf[1:]
    return _fit_leaf(leaf)


# Filesystems cap a name at 255 BYTES. Cleaned names are cut to leave room for
# the ``-N`` suffix disambiguation may add, so init never meets one too long.
_MAX_LEAF_BYTES = 255
_SUFFIX_ROOM_BYTES = 12


def _fit_leaf(leaf: str, limit: int = _MAX_LEAF_BYTES - _SUFFIX_ROOM_BYTES) -> str:
    """Shorten ``leaf`` to ``limit`` UTF-8 bytes, keeping a short extension."""

    if len(leaf.encode("utf-8")) <= limit:
        return leaf
    stem, extension = os.path.splitext(leaf)
    if len(extension.encode("utf-8")) > 16:
        stem, extension = leaf, ""
    room = limit - len(extension.encode("utf-8"))
    cut = stem.encode("utf-8")[:room].decode("utf-8", errors="ignore")
    return cut + extension


class _FoldedNames:
    """``in`` that ignores case: a docker host on macOS or Windows folds case,
    so ``Report.pdf`` and ``report.pdf`` would land on one file there."""

    def __init__(self, names: Iterable[str]) -> None:
        self._folded = {name.casefold() for name in names}

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name.casefold() in self._folded

    def add(self, name: str) -> None:
        self._folded.add(name.casefold())


def assign_disk_names(names: Sequence[str], *, taken: Iterable[str]) -> tuple[str, ...]:
    """Fix each name's on-disk leaf against every name already held.

    ``taken`` is every disk name the thread's ledger already records, including
    files this boot will not deliver, so a name is never reused and a later
    omission never shifts another file's name. Names assigned earlier in the
    same call are taken too.
    """

    held = _FoldedNames(taken)
    assigned: list[str] = []
    for name in names:
        leaf = unique_attachment_leaf(clean_attachment_leaf(name), held)
        held.add(leaf)
        assigned.append(leaf)
    return tuple(assigned)


@dataclass(frozen=True)
class AgentRoute:
    """One of the agent's bindings as it is NOW (ADR 0205 decision 5).

    A re-fetch resolves its route from these and never from anything recorded
    with the file, so a rotated endpoint or a removed binding takes effect on
    the next boot. Structural: any object with these three attributes serves.
    """

    kind: str
    adapter: str | None
    endpoint: str | None


@dataclass(frozen=True)
class PreparedAttachments:
    """One turn's resolved set: what was written, what was minted, until when."""

    refs: tuple[AttachmentRef, ...]
    object_keys: tuple[str, ...]
    retention_expires_at_epoch: int

    #: Which durable owner record in the retention ledger this exact resolve
    #: wrote. Two workers resolving the same thread outside the route lock can
    #: produce byte-identical field values, and each still has to be able to
    #: discard ITS OWN record without touching the other's, so identity comes
    #: from a token minted per construction rather than from the fields. It is
    #: excluded from ``init`` so the three-argument constructor is unchanged,
    #: and from ``compare``/``repr`` so equality, hashing and logging are what
    #: they were before it existed.
    _owner_token: str = field(
        init=False, compare=False, repr=False, default_factory=lambda: uuid.uuid4().hex
    )

    def claim_env(self) -> dict[str, str]:
        """The claim-env contribution, EMPTY for a turn carrying no files.

        Empty rather than an empty-valued key: an init container reads a present
        key as "there is work here", so the overwhelming majority of turns must
        contribute nothing at all and keep today's boot env byte-identical.
        """

        if not self.refs:
            return {}
        return {ATTACHMENTS_REF_ENV: encode_attachment_refs(self.refs)}


#: Why an earlier file could not be delivered on this boot (ADR 0205 decision
#: 6). Named to the agent in the manifest; never a reason to fail the boot.
UNAVAILABLE_REASONS = frozenset(
    {
        "no_route",
        "no_credential",
        "not_found",
        "forbidden",
        "rate_limited",
        "timeout",
        "digest_changed",
        "expired",
        "deadline",
        "fetch_failed",
    }
)


@dataclass(frozen=True)
class ThreadSetEntry:
    """One file a boot materializes, under the name it was recorded with."""

    disk_name: str
    object_key: str
    sha256: str
    size_bytes: int
    mime_type: str | None
    current: bool


@dataclass(frozen=True)
class UnavailableAttachment:
    """An earlier file this boot could not deliver, and why."""

    name: str
    reason: str


@dataclass(frozen=True)
class PreparedThreadSet:
    """One boot's whole thread set (ADR 0205 decision 3).

    ``entries`` are in arrival order with the current message's files last.
    ``object_keys`` are only the objects this prepare WROTE; a re-minted parked
    copy is named by this prepare's owner record but belongs to an earlier one,
    so a discard never deletes it. ``append_refs`` are the current message's
    references, recorded only once the set is installed.
    """

    entries: tuple[ThreadSetEntry, ...] = ()
    unavailable: tuple[UnavailableAttachment, ...] = ()
    omitted: tuple[str, ...] = ()
    append_refs: tuple[ThreadAttachmentRef, ...] = ()
    ledger_unavailable: bool = False
    object_keys: tuple[str, ...] = ()
    #: The minted capabilities, aligned with ``entries``.
    refs: tuple[AttachmentRef, ...] = field(default=(), repr=False)
    #: Whether this prepare wrote an owner record, under ``_owner_token``.
    owner_recorded: bool = field(default=False, repr=False)
    _owner_token: str = field(
        init=False, compare=False, repr=False, default_factory=lambda: uuid.uuid4().hex
    )

    def claim_env(self) -> dict[str, str]:
        """The claim-env contribution: refs for the init container, manifest
        for the runner, or nothing at all for a thread with nothing to say.

        Each ref entry carries the exact disk name in ``n`` and ``c`` (1 for a
        current file, 0 for an earlier one) so the init container writes that
        name and never renames. The manifest carries names and reasons only,
        never a URL, an id or a credential.
        """

        if not (self.entries or self.unavailable or self.omitted or self.ledger_unavailable):
            return {}
        env: dict[str, str] = {}
        if self.entries:
            payload = [
                {
                    "n": entry.disk_name,
                    "u": ref.url,
                    "s": ref.sha256,
                    "b": ref.size_bytes,
                    "e": ref.expires_at_epoch,
                    "m": ref.mime_type,
                    "c": 1 if entry.current else 0,
                }
                for entry, ref in zip(self.entries, self.refs, strict=True)
            ]
            raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
            env[ATTACHMENTS_REF_ENV] = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        env[ATTACHMENTS_MANIFEST_ENV] = json.dumps(
            {
                "v": 1,
                "files": [
                    {"name": entry.disk_name, "current": entry.current}
                    for entry in self.entries
                ],
                "unavailable": [
                    {"name": item.name, "reason": item.reason} for item in self.unavailable
                ],
                "omitted": list(self.omitted),
                "ledger_unavailable": self.ledger_unavailable,
            },
            separators=(",", ":"),
        )
        return env


class _EarlierUnavailable(Exception):
    """Internal: one earlier file is unavailable for ``reason``."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _unavailable_reason(exc: BaseException) -> str:
    """Name why an earlier file's fetch failed, from the failure itself.

    Walks the cause chain because ``_park`` wraps the port's own error in an
    ``AttachmentFetchError``. A status-carrying refusal is named by its status;
    a timeout anywhere in the chain is a timeout; anything else is
    ``fetch_failed``.
    """

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return "timeout"
        if isinstance(current, urllib.error.URLError) and isinstance(
            current.reason, TimeoutError
        ):
            return "timeout"
        status = getattr(current, "status", None)
        if isinstance(current, (SlackFileError, ChannelPortFileError)) and status is not None:
            if status == 404 or status == 410:
                return "not_found"
            if status in (401, 403):
                return "forbidden"
            if status == 429:
                return "rate_limited"
            return "fetch_failed"
        current = current.__cause__ or current.__context__
    return "fetch_failed"


@dataclass(frozen=True)
class _AttachmentSet:
    """The durable retention record for ONE resolved set, not one thread.

    A thread can have several of these at once -- two workers resolving the same
    turn, or a replaced set still inside its retention window -- and each names
    only the objects its own resolve parked. That is what keeps an installed set
    owned, and therefore sweepable, after a later resolve supersedes it.
    """

    thread_key: str
    refs: tuple[AttachmentRef, ...]
    object_keys: tuple[str, ...]
    expires_at_epoch: int
    #: The agent whose bytes these are, or None on a record written before
    #: ADR 0205. Optional fields, never a version bump: a stable-line worker
    #: shares the bucket during a rolling upgrade and its reap tick refuses any
    #: version but 1.
    agent_id: str | None = None
    #: object key -> sha256 of the parked bytes, the parked-cache index a boot
    #: re-mints an earlier file from. Empty on a pre-ADR-0205 record.
    shas: Mapping[str, str] = field(default_factory=dict, hash=False)

    def encode(self) -> bytes:
        payload: dict[str, Any] = {
            "version": 1,
            "thread_key": self.thread_key,
            "expires_at_epoch": self.expires_at_epoch,
            "object_keys": list(self.object_keys),
            "refs": encode_attachment_refs(self.refs),
        }
        if self.agent_id is not None:
            payload["agent"] = self.agent_id
        if self.shas:
            payload["shas"] = dict(self.shas)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> _AttachmentSet:
        """Decode one owner record, taking the thread it names at face value.

        There is deliberately no expected-thread argument. Every discovery path
        is one scan of the whole ledger prefix, so the decoder meets other
        threads' records constantly; refusing them here would turn any
        neighbour's record into a failure of this thread's discard or reap.
        Callers filter on the decoded ``thread_key`` instead.
        """

        try:
            raw = json.loads(payload)
            if raw.get("version") != 1:
                raise ValueError("unsupported attachment record version")
            thread_key = str(raw["thread_key"])
            object_keys = tuple(str(key) for key in raw["object_keys"])
            agent = raw.get("agent")
            raw_shas = raw.get("shas") or {}
            if not isinstance(raw_shas, Mapping):
                raise TypeError("attachment record shas is not a mapping")
            record = cls(
                thread_key=thread_key,
                refs=decode_attachment_refs(str(raw["refs"])),
                object_keys=object_keys,
                expires_at_epoch=int(raw["expires_at_epoch"]),
                agent_id=None if agent is None else str(agent),
                shas={str(key): str(value) for key, value in raw_shas.items()},
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AttachmentResolutionError(
                "retention-ledger", "private attachment record is invalid"
            ) from exc
        if not thread_key or not object_keys:
            raise AttachmentResolutionError(
                "retention-ledger", "private attachment record is incomplete"
            )
        if any(not key or key.startswith("_") for key in object_keys):
            raise AttachmentResolutionError(
                "retention-ledger", "private attachment record names an invalid object key"
            )
        return record


@dataclass(frozen=True)
class AttachmentReapCandidate:
    """The exact expired owners observed, before their slow object cleanup.

    Opaque to the kernel, which only carries it from ``begin`` to ``finish``
    across its lock renewal and reads no field of it. The snapshots are what
    ``finish`` is allowed to delete and nothing else: an owner that appeared
    after this was taken was never observed, so it is not this reap's to touch.
    """

    owners: tuple[tuple[str, _AttachmentSet], ...]
    deleted_object_keys: tuple[str, ...]


class AttachmentFilePort(Protocol):
    """The channel's file download, chunk by chunk.

    A generator rather than a bytes-returning call: the size cap is enforced
    against what has actually been pulled, which is only possible if the caller
    controls the pull.
    """

    def fetch(self, file_id: str) -> Iterator[bytes]: ...


@dataclass(frozen=True)
class SlackFileResponse:
    """One transport answer: status, headers, and an unread chunk stream."""

    status: int
    headers: Mapping[str, str]
    chunks: Iterator[bytes]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        return None


def _slack_transport(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    chunk_bytes: int,
) -> SlackFileResponse:
    """One bearer-authenticated request whose body is left unread.

    Redirects are refused for the same reason the workspace credential client
    refuses them: a redirect would carry the ``Authorization`` header to a host
    Slack named rather than one this worker chose.
    """

    request = urllib.request.Request(url, headers=dict(headers), method=method)
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        response = opener.open(request, timeout=30)
    except urllib.error.HTTPError as exc:
        body = exc.read()
        return SlackFileResponse(
            status=int(exc.code),
            headers=dict(exc.headers.items()) if exc.headers is not None else {},
            chunks=iter([body]),
        )

    def _chunks() -> Iterator[bytes]:
        with response:
            while chunk := response.read(chunk_bytes):
                yield chunk

    return SlackFileResponse(
        status=int(response.status),
        headers=dict(response.headers.items()),
        chunks=_chunks(),
    )


class ChannelPortFileError(RuntimeError):
    """The adapter behind a channel-port binding would not serve a file."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


class ChannelPortFileClient:
    """Fetch a channel-port turn's files from the adapter that sent them (ADR-0153).

    Every adapter behind the channel port serves one shape, ``GET
    {endpoint}/attachments/{id}`` behind its adapter secret, so one client
    serves them all. The endpoint is the binding's reply endpoint, and the
    secret and its header are the ones the reply sink already sends there
    (``CURIE_ADAPTER_CREDENTIALS``), so this adds no credential and no new
    destination. Slack is the one kind whose files are not behind an adapter.
    Redirects are refused by the shared transport: following one would send the
    secret to a host the adapter named.

    ``id`` is opaque and the adapter's own. It is percent-encoded so a ``?``,
    ``#`` or space cannot end the path early, and ``/`` is kept so an id the
    adapter minted with path segments reaches it as minted.
    """

    def __init__(
        self,
        *,
        credentials: Mapping[str, str],
        transport: Callable[..., SlackFileResponse] = _slack_transport,
        read_chunk_bytes: int = 1024 * 1024,
    ) -> None:
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        self._credentials = dict(credentials)
        self._transport = transport
        self._read_chunk_bytes = read_chunk_bytes

    def bind(self, handle: ReplyHandle) -> AttachmentFilePort:
        """The file port for one binding, refused before any request when the
        worker has nowhere to ask or nothing to authenticate with."""

        if not (handle.endpoint or "").rstrip("/"):
            raise AttachmentResolutionError(
                "wiring",
                f"the {handle.kind} binding declares no endpoint to fetch its attachments from",
            )
        return self.bind_route(handle.adapter, handle.endpoint)

    def bind_route(self, adapter: str | None, endpoint: str | None) -> AttachmentFilePort:
        """The file port for one of the agent's CURRENT bindings (ADR 0205).

        ``bind`` for a route read from the bindings rather than from a reply
        handle: an earlier file is re-fetched from wherever its adapter lives
        now, never from an endpoint recorded with the file. Same two refusals
        as ``bind``, both before any request.
        """

        base = (endpoint or "").rstrip("/")
        if not base:
            raise AttachmentResolutionError(
                "wiring", "the binding declares no endpoint to fetch its attachments from"
            )
        secret = self._credentials.get(adapter or "")
        if not secret:
            raise AttachmentResolutionError(
                "credential",
                f"no adapter credential is configured on this worker for adapter "
                f"{adapter!r}, so its attachments cannot be fetched",
            )
        return _BoundChannelPortFiles(
            endpoint=base,
            secret=secret,
            transport=self._transport,
            read_chunk_bytes=self._read_chunk_bytes,
        )


@dataclass(frozen=True)
class _BoundChannelPortFiles:
    """One binding's ``AttachmentFilePort``."""

    endpoint: str
    secret: str = field(repr=False)
    transport: Callable[..., SlackFileResponse]
    read_chunk_bytes: int

    def fetch(self, file_id: str) -> Iterator[bytes]:
        response = self.transport(
            method="GET",
            url=f"{self.endpoint}/attachments/{urllib.parse.quote(file_id, safe='/')}",
            headers={ADAPTER_SECRET_HEADER: self.secret},
            chunk_bytes=self.read_chunk_bytes,
        )
        if response.status != 200:
            raise ChannelPortFileError(
                f"adapter attachment download failed: HTTP {response.status}",
                status=response.status,
            )
        return response.chunks


class SlackFileClient:
    """Download one Slack file's bytes with the bot token, and only here.

    The download reads ``url_private_download`` rather than ``url_private``:
    Slack answers a PDF's ``url_private`` with a 302 to ``slack-files.com``,
    and the transport below deliberately refuses to follow it (see
    ``_NoRedirect``), so the bearer token never reaches a host Slack chose.
    ``url_private_download`` is Slack's documented direct-download endpoint on
    the same file object and does not redirect
    (https://docs.slack.dev/reference/objects/file-object). It is NOT public,
    though: the download must carry ``Authorization: Bearer <token>``, or
    Slack answers with an HTML sign-in page rather than the bytes.

    The token authenticates exactly these two requests and reaches nothing
    downstream -- what the sandbox receives is a presigned one-object URL from
    the private object store.
    """

    def __init__(
        self,
        *,
        token: str,
        transport: Callable[..., SlackFileResponse] = _slack_transport,
        api_url: str = _SLACK_API_URL,
        read_chunk_bytes: int = 1024 * 1024,
    ) -> None:
        if not token:
            raise ValueError("attachment resolution requires a Slack bot token")
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        self._token = token
        self._transport = transport
        self._api_url = api_url.rstrip("/")
        self._read_chunk_bytes = read_chunk_bytes

    def fetch(self, file_id: str) -> Iterator[bytes]:
        download_url = self._download_url(file_id)
        response = self._transport(
            method="GET",
            url=download_url,
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            raise SlackFileError(
                f"slack file download failed: HTTP {response.status}", status=response.status
            )
        content_type = self._content_type(response.headers)
        if content_type.startswith("text/html"):
            # Slack serves its sign-in page, with a 200, when the bearer token
            # is missing or lacks files:read. Storing that page as the
            # "attachment" would hand the agent a login screen to read.
            raise SlackFileError(
                "slack file download returned an HTML page rather than file bytes; "
                "the bot token is likely missing the files:read scope"
            )
        return response.chunks

    def _download_url(self, file_id: str) -> str:
        response = self._transport(
            method="GET",
            url=f"{self._api_url}/files.info?file={file_id}",
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            raise SlackFileError(
                f"slack files.info failed: HTTP {response.status}", status=response.status
            )
        body = self._bounded_body(response.chunks)
        try:
            payload = json.loads(body)
            if not payload.get("ok"):
                raise SlackFileError(f"slack files.info failed: {payload.get('error')}")
            file_obj = payload["file"]
            if not isinstance(file_obj, Mapping):
                raise TypeError("file object is not a mapping")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SlackFileError("slack files.info returned an unusable envelope") from exc
        # The envelope parsed fine and was ok, but the field this module reads
        # instead of url_private (see the class docstring) may still be
        # absent. Named explicitly rather than folded into the except above,
        # since a missing field is not the same failure as a malformed
        # envelope and an operator needs to be able to tell them apart.
        if "url_private_download" not in file_obj:
            raise SlackFileError(
                "slack files.info's file object has no url_private_download field"
            )
        download_url = str(file_obj["url_private_download"])
        if not download_url.startswith("https://"):
            raise SlackFileError("slack files.info returned a non-HTTPS url_private_download")
        return download_url

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    @staticmethod
    def _content_type(headers: Mapping[str, str]) -> str:
        for key, value in headers.items():
            if str(key).lower() == "content-type":
                return str(value).lower()
        return ""

    @staticmethod
    def _bounded_body(chunks: Iterator[bytes]) -> bytes:
        body = bytearray()
        for chunk in chunks:
            body.extend(chunk)
            if len(body) > _MAX_INFO_BYTES:
                raise SlackFileError("slack files.info response is oversized")
        return bytes(body)


class AttachmentCoordinator:
    """Resolve, park and mint one turn's attachments, and own their retention.

    Constructed with worker-local ports so this lane stays independent of the
    concrete channel and object-store packages, exactly as the workspace lane
    is: ``files`` is the channel download, ``objects`` is the private store,
    ``clock`` is the wall clock the two independent expiry windows are measured
    against. ``identity_files`` holds each named Slack identity's own download
    (ADR-0168 decision 5); ``files`` is ``default``'s, or None on a worker that
    holds no bot token for it. ``channel_files`` fetches a channel-port
    binding's files from its adapter (ADR-0153), or is None on a worker that
    holds no adapter credential.
    """

    def __init__(
        self,
        *,
        files: AttachmentFilePort | None,
        objects: WorkspaceObjectPort,
        limits: AttachmentLimits | None = None,
        clock: Callable[[], float] = time.time,
        identity_files: Mapping[str, AttachmentFilePort] | None = None,
        channel_files: ChannelPortFileClient | None = None,
    ) -> None:
        # None when this worker holds no bot token for `default` (ADR-0168
        # decision 5): `_files_for` refuses that identity the same way it
        # refuses an unconfigured named one, rather than a caller having to
        # stand in a port whose every method raises.
        self.files = files
        self.objects = objects
        self.limits = limits or AttachmentLimits()
        self._clock = clock
        self._lock = threading.Lock()
        self._identity_files: dict[str, AttachmentFilePort] = dict(identity_files or {})
        self.channel_files = channel_files

    # -- resolve ------------------------------------------------------------

    def resolve(
        self,
        *,
        thread_key: str,
        agent_id: str | None,
        attachments: Sequence[Attachment],
        generation: str | None = None,
        identity: str = DEFAULT_IDENTITY,
        handle: ReplyHandle | None = None,
    ) -> PreparedAttachments:
        """Download, park and mint the whole set, or refuse it and leave nothing.

        The refusal is loud and total by design (see the module docstring): a
        partial set reads to an agent exactly like a complete one.

        ``handle`` is the turn's server-minted reply handle. A non-Slack kind
        selects its adapter's transport; Slack, or no handle, selects the bot
        token for ``identity``.
        """

        refs = tuple(attachments)
        if not refs:
            # The common case, and it must stay exactly as cheap as it is: no
            # channel round trip, no object written, no ledger, no claim env.
            return PreparedAttachments((), (), int(self._clock()))
        if len(refs) > self.limits.max_files:
            # Checked BEFORE the first fetch: the refs array arrives from
            # outside, so an unbounded loop over it is an unbounded cost.
            raise AttachmentResolutionError(
                "set-size",
                f"{len(refs)} attachments exceed the {self.limits.max_files}-file cap",
            )
        if not agent_id:
            raise AttachmentResolutionError(
                "wiring", "attachment resolution requires a bound agent"
            )
        files = self._files_for(identity, handle)

        mint = generation or uuid.uuid4().hex
        written: list[str] = []
        minted: list[AttachmentRef] = []
        try:
            for index, attachment in enumerate(refs):
                key = self._object_key(agent_id=agent_id, generation=mint, index=index)
                # Recorded before the upload starts, not after it returns: the
                # port promises no rollback, so a generator that raises
                # mid-upload leaves the chunks it already yielded under this key
                # and only a caller that remembers the key can remove them.
                written.append(key)
                digest, size = self._park(attachment.id, attachment.name, key, files)
                minted.append(
                    AttachmentRef(
                        name=attachment.name,
                        url=self.objects.presign_get(
                            key, expires_seconds=self.limits.reference_ttl_seconds
                        ),
                        sha256=digest,
                        size_bytes=size,
                        expires_at_epoch=int(self._clock())
                        + self.limits.reference_ttl_seconds,
                        mime_type=attachment.mime_type,
                    )
                )
        except Exception:
            self._discard(written)
            raise

        prepared = PreparedAttachments(
            refs=tuple(minted),
            object_keys=tuple(written),
            retention_expires_at_epoch=int(self._clock()) + self.limits.retention_ttl_seconds,
        )
        self._record(thread_key, prepared)
        return prepared

    # -- the thread set (ADR 0205) -------------------------------------------

    def prepare_thread_set(
        self,
        *,
        thread_key: str,
        agent_id: str | None,
        ledger_refs: Sequence[ThreadAttachmentRef],
        current: Sequence[Attachment] = (),
        event_id: str | None = None,
        identity: str = DEFAULT_IDENTITY,
        handle: ReplyHandle | None = None,
        routes: Sequence[Any] = (),
        deadline_epoch: float | None = None,
        ledger_unavailable: bool = False,
    ) -> PreparedThreadSet:
        """Build one boot's whole thread set (ADR 0205 decisions 3 to 7).

        ``ledger_refs`` is the thread's ledger in arrival order and ``current``
        the message this boot is for. The current message's files are fetched
        FIRST and all or nothing, exactly as ``resolve`` does: any failure
        raises and leaves no object and no owner record. Earlier files are then
        taken newest-first within the thread budget, each re-minted from a live
        parked copy of the same digest or fetched again through ``routes`` (the
        agent's bindings as they are now). An earlier file that cannot be had
        is named unavailable and never fails the set. Every capability is
        presigned only after every fetch has finished, so a slow fetch cannot
        age one out before the sandbox redeems it.

        A thread with no ledger reference and no current file costs nothing:
        no fetch, no store call, no claim env beyond the ledger-unavailable
        manifest the caller asked for.
        """

        held = tuple(ledger_refs)
        attached = tuple(current)
        if not held and not attached:
            return PreparedThreadSet(ledger_unavailable=ledger_unavailable)
        if len(attached) > self.limits.max_files:
            raise AttachmentResolutionError(
                "set-size",
                f"{len(attached)} attachments exceed the {self.limits.max_files}-file cap",
            )
        if not agent_id:
            raise AttachmentResolutionError(
                "wiring", "attachment resolution requires a bound agent"
            )

        # A ledger row recorded by THIS event for a file this message carries
        # is this message's file (a redelivered turn finds its own refs already
        # appended): it keeps the name already recorded and is delivered once,
        # as current. The same file id recorded by another event is an earlier
        # file, and this message's copy gets a new name (#4141).
        current_ids = {attachment.id for attachment in attached}
        recorded: dict[str, ThreadAttachmentRef] = {}
        for ref in held:
            if ref.event_id == event_id and ref.file_id in current_ids:
                recorded.setdefault(ref.file_id, ref)
        earlier = [
            ref
            for ref in held
            if not (ref.event_id == event_id and ref.file_id in current_ids)
        ]
        current_names = self._current_disk_names(attached, recorded, held)

        mint = uuid.uuid4().hex
        written: list[str] = []
        parked_current: list[tuple[str, int]] = []
        if attached:
            files = self._files_for(identity, handle)
            try:
                for attachment in attached:
                    key = self._object_key(
                        agent_id=agent_id, generation=mint, index=len(written)
                    )
                    written.append(key)
                    parked_current.append(
                        self._park(attachment.id, attachment.name, key, files)
                    )
            except Exception:
                self._discard(written)
                raise

        current_keys = tuple(written)
        kept, omitted_ids = self._within_budget(
            earlier,
            current_bytes=sum(size for _digest, size in parked_current),
            current_files=len(attached),
        )

        delivered: dict[int, ThreadSetEntry] = {}
        unavailable: dict[int, str] = {}
        reused: list[str] = []
        cache = self._parked_cache(thread_key, agent_id) if kept else {}
        for position, ref in kept:
            if ref.size_bytes is not None and ref.size_bytes > self.limits.max_file_bytes:
                # Recorded under a larger per-file cap than this worker's: it
                # would be refused mid-stream anyway, so it is not sent.
                unavailable[position] = "fetch_failed"
                continue
            hit = cache.get(ref.sha256)
            if hit is not None:
                delivered[position] = ThreadSetEntry(
                    disk_name=ref.disk_name,
                    object_key=hit,
                    sha256=ref.sha256,
                    size_bytes=ref.size_bytes if ref.size_bytes is not None else 0,
                    mime_type=ref.mime_type,
                    current=False,
                )
                reused.append(hit)
                continue
            key = self._object_key(agent_id=agent_id, generation=mint, index=len(written))
            try:
                entry = self._refetch_earlier(ref, key, routes, deadline_epoch)
            except _EarlierUnavailable as gone:
                unavailable[position] = gone.reason
                continue
            written.append(key)
            delivered[position] = entry

        entries: list[ThreadSetEntry] = [
            delivered[position] for position in range(len(earlier)) if position in delivered
        ]
        append_refs: list[ThreadAttachmentRef] = []
        for ordinal, (attachment, disk_name, key, (digest, size)) in enumerate(
            zip(attached, current_names, current_keys, parked_current, strict=True)
        ):
            entries.append(
                ThreadSetEntry(
                    disk_name=disk_name,
                    object_key=key,
                    sha256=digest,
                    size_bytes=size,
                    mime_type=attachment.mime_type,
                    current=True,
                )
            )
            append_refs.append(
                ThreadAttachmentRef(
                    file_id=attachment.id,
                    ordinal=ordinal,
                    name=attachment.name,
                    disk_name=disk_name,
                    mime_type=attachment.mime_type,
                    size_bytes=size,
                    sha256=digest,
                    route_kind=handle.kind if handle is not None else SLACK_KIND,
                    route_adapter=handle.adapter if handle is not None else None,
                    route_identity=identity,
                )
            )

        prepared = PreparedThreadSet(
            entries=tuple(entries),
            unavailable=tuple(
                UnavailableAttachment(name=earlier[position].disk_name, reason=reason)
                for position, reason in sorted(unavailable.items())
            ),
            omitted=tuple(earlier[position].disk_name for position in sorted(omitted_ids)),
            append_refs=tuple(append_refs),
            ledger_unavailable=ledger_unavailable,
            object_keys=tuple(written),
        )
        if not entries:
            return prepared
        try:
            # Signed LAST, on the clock as it stands after every fetch.
            signed_at = int(self._clock())
            refs = tuple(
                AttachmentRef(
                    name=entry.disk_name,
                    url=self.objects.presign_get(
                        entry.object_key, expires_seconds=self.limits.reference_ttl_seconds
                    ),
                    sha256=entry.sha256,
                    size_bytes=entry.size_bytes,
                    expires_at_epoch=signed_at + self.limits.reference_ttl_seconds,
                    mime_type=entry.mime_type,
                )
                for entry in entries
            )
            prepared = replace(prepared, refs=refs, owner_recorded=True)
            owned = tuple(dict.fromkeys([*written, *reused]))
            with self._lock:
                record = _AttachmentSet(
                    thread_key=thread_key,
                    refs=refs,
                    object_keys=owned,
                    expires_at_epoch=int(self._clock()) + self.limits.retention_ttl_seconds,
                    agent_id=agent_id,
                    shas={entry.object_key: entry.sha256 for entry in entries},
                )
                self.objects.put_stream(
                    self._owner_key(thread_key, prepared._owner_token), (record.encode(),)
                )
        except Exception:
            self._discard(written)
            raise
        return prepared

    def _current_disk_names(
        self,
        attached: Sequence[Attachment],
        recorded: Mapping[str, ThreadAttachmentRef],
        held: Sequence[ThreadAttachmentRef],
    ) -> tuple[str, ...]:
        """Each current file's disk name: the recorded one on a redelivery,
        otherwise fixed now against every name the ledger holds."""

        taken = {ref.disk_name for ref in held}
        names: list[str] = []
        reused_ids: set[str] = set()
        for attachment in attached:
            prior = recorded.get(attachment.id)
            if prior is not None and attachment.id not in reused_ids:
                reused_ids.add(attachment.id)
                names.append(prior.disk_name)
                continue
            (leaf,) = assign_disk_names([attachment.name], taken=(*taken, *names))
            taken.add(leaf)
            names.append(leaf)
        return tuple(names)

    def _within_budget(
        self,
        earlier: Sequence[ThreadAttachmentRef],
        *,
        current_bytes: int,
        current_files: int,
    ) -> tuple[list[tuple[int, ThreadAttachmentRef]], set[int]]:
        """Admit earlier files newest-first after the current ones.

        The current message is always kept and counted first, even alone over
        budget. The first earlier file that does not fit ends admission, so an
        older small file never displaces a newer one. A ref with no recorded
        size is counted as the per-file cap, the most it could be.
        """

        files = current_files
        total = current_bytes
        kept: list[tuple[int, ThreadAttachmentRef]] = []
        omitted: set[int] = set()
        full = False
        for position in range(len(earlier) - 1, -1, -1):
            ref = earlier[position]
            size = ref.size_bytes if ref.size_bytes is not None else self.limits.max_file_bytes
            if (
                full
                or files + 1 > self.limits.thread_max_files
                or total + size > self.limits.thread_max_bytes
            ):
                full = True
                omitted.add(position)
                continue
            files += 1
            total += size
            kept.append((position, ref))
        return kept, omitted

    def _refetch_earlier(
        self,
        ref: ThreadAttachmentRef,
        key: str,
        routes: Sequence[Any],
        deadline_epoch: float | None,
    ) -> ThreadSetEntry:
        """Fetch one earlier file again, or raise ``_EarlierUnavailable``.

        Never raises anything else: an earlier file is best effort (ADR 0205
        decision 6), and whatever it wrote is removed before it is named
        unavailable.
        """

        if deadline_epoch is not None and self._clock() >= deadline_epoch:
            raise _EarlierUnavailable("deadline")
        try:
            files = self._route_files(ref, routes)
        except _EarlierUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 -- an earlier file never fails the set
            raise _EarlierUnavailable("fetch_failed") from exc
        try:
            digest, size = self._park(ref.file_id, ref.disk_name, key, files)
        except Exception as exc:  # noqa: BLE001 -- named, never raised
            self._discard([key])
            raise _EarlierUnavailable(_unavailable_reason(exc)) from exc
        if digest != ref.sha256:
            # The bytes at the channel are not the bytes the agent was given.
            self._discard([key])
            raise _EarlierUnavailable("digest_changed")
        return ThreadSetEntry(
            disk_name=ref.disk_name,
            object_key=key,
            sha256=digest,
            size_bytes=size,
            mime_type=ref.mime_type,
            current=False,
        )

    def _route_files(
        self, ref: ThreadAttachmentRef, routes: Sequence[Any]
    ) -> AttachmentFilePort:
        """The download for an earlier file, from the agent's CURRENT bindings.

        Never from anything recorded with the file: Slack needs a Slack binding
        that speaks as the recorded identity and this worker's token for it;
        any other kind needs a binding of the same kind and adapter, fetched
        from that binding's endpoint as it is now.
        """

        if ref.route_kind == SLACK_KIND:
            if not any(
                route.kind == SLACK_KIND
                and slack_speaking_identity(route.kind, route.adapter, route.endpoint)
                == ref.route_identity
                for route in routes
            ):
                raise _EarlierUnavailable("no_route")
            files = (
                self.files
                if ref.route_identity == DEFAULT_IDENTITY
                else self._identity_files.get(ref.route_identity)
            )
            if files is None:
                raise _EarlierUnavailable("no_credential")
            return files
        matches = [
            route
            for route in routes
            if route.kind == ref.route_kind
            and route.adapter == ref.route_adapter
            and (route.endpoint or "").rstrip("/")
        ]
        if not matches:
            raise _EarlierUnavailable("no_route")
        if self.channel_files is None:
            raise _EarlierUnavailable("no_credential")
        try:
            return self.channel_files.bind_route(matches[0].adapter, matches[0].endpoint)
        except AttachmentResolutionError as exc:
            raise _EarlierUnavailable(
                "no_credential" if exc.stage == "credential" else "no_route"
            ) from exc

    def _parked_cache(self, thread_key: str, agent_id: str) -> dict[str, str]:
        """sha256 -> a parked object key this boot may re-mint (ADR 0205 decision 5).

        Only this thread's own owner records are read (its digest subprefix,
        not the whole ledger), only ones written for this agent, and only
        objects whose owner outlives the capability by a margin: a copy that
        would lapse before the sandbox redeems it is fetched again instead. A
        candidate key is confirmed present before it is offered. Any store
        failure here only means "no cache": the file is fetched again.
        """

        floor = int(self._clock()) + self.limits.reference_ttl_seconds + _REUSE_MARGIN_SECONDS
        digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()
        agent_prefix = f"{ATTACHMENT_OBJECT_PREFIX}/{agent_id}/"
        best: dict[str, tuple[int, str]] = {}
        try:
            with self._lock:
                keys = tuple(self.objects.list_keys(f"{ATTACHMENT_LEDGER_PREFIX}/{digest}"))
                for owner_key in keys:
                    try:
                        record = self._load_key(owner_key)
                    except Exception:  # noqa: BLE001 -- a bad record is not a cache hit
                        continue
                    if (
                        record.thread_key != thread_key
                        or record.agent_id != agent_id
                        or record.expires_at_epoch <= floor
                    ):
                        continue
                    for object_key, sha in record.shas.items():
                        if object_key not in record.object_keys or not object_key.startswith(
                            agent_prefix
                        ):
                            continue
                        held = best.get(sha)
                        if held is None or record.expires_at_epoch > held[0]:
                            best[sha] = (record.expires_at_epoch, object_key)
            present: dict[str, set[str]] = {}
            cache: dict[str, str] = {}
            for sha, (_expiry, object_key) in best.items():
                parent = object_key.rsplit("/", 1)[0]
                if parent not in present:
                    present[parent] = set(self.objects.list_keys(parent))
                if object_key in present[parent]:
                    cache[sha] = object_key
        except Exception:  # noqa: BLE001 -- no cache, fetch again
            return {}
        return cache

    def _files_for(self, identity: str, handle: ReplyHandle | None = None) -> AttachmentFilePort:
        """The download for this turn's binding, refused before any fetch when absent."""

        if handle is not None and handle.kind != SLACK_KIND:
            if self.channel_files is None:
                # Never fall through to Slack: the id is the adapter's, and
                # Slack would answer for a different file or none.
                raise AttachmentResolutionError(
                    "credential",
                    f"no adapter credential is configured on this worker, so the "
                    f"{handle.kind} binding's attachments cannot be fetched",
                )
            return self.channel_files.bind(handle)
        files = self.files if identity == DEFAULT_IDENTITY else self._identity_files.get(identity)
        if files is None:
            raise AttachmentResolutionError(
                "credential",
                f"no Slack bot token is configured on this worker for identity {identity!r}",
            )
        return files

    def _park(
        self, file_id: str, name: str, key: str, files: AttachmentFilePort
    ) -> tuple[str, int]:
        """Stream one file into the store under its cap, returning digest+size."""

        digest = hashlib.sha256()
        counted = [0]
        try:
            self.objects.put_stream(
                key,
                self._bounded(
                    files.fetch(file_id),
                    name=name,
                    digest=digest,
                    counted=counted,
                ),
            )
        except AttachmentResolutionError:
            raise
        except Exception as exc:
            # The observable a turn gets is a refusal, never a silently empty
            # set: a swallowed error would boot a sandbox with no files and the
            # agent would answer "I can't see any attachment" about a message
            # that visibly carries one.
            raise AttachmentFetchError(name, str(exc)) from exc
        return digest.hexdigest(), counted[0]

    def _bounded(
        self,
        source: Iterable[bytes],
        *,
        name: str,
        digest: Any,
        counted: list[int],
    ) -> Iterator[bytes]:
        """``_read_bounded_upload``'s shape: refuse at the crossing chunk.

        The running total is checked before the chunk is handed on, so memory
        never holds more than one chunk past the cap and the reader is never
        asked for the chunk after the crossing one.
        """

        total = 0
        for chunk in source:
            total += len(chunk)
            if total > self.limits.max_file_bytes:
                raise AttachmentTooLargeError(name, self.limits.max_file_bytes)
            digest.update(chunk)
            counted[0] = total
            yield chunk

    def _discard(self, keys: Iterable[str]) -> None:
        """Remove this refused resolve's own objects, best effort.

        Best effort because the refusal must stand: a delete that fails leaves
        an orphan to account for, while swallowing the original failure would
        hide the reason the turn was refused in the first place.
        """

        for key in keys:
            try:
                self.objects.delete(key)
            except Exception:  # noqa: BLE001 -- the refusal above is the real news
                continue

    def discard_prepared(
        self, *, thread_key: str, prepared: PreparedAttachments | PreparedThreadSet
    ) -> None:
        """Discard an abandoned preclaim set without disturbing a newer resolve.

        The caller knows the exact bytes it prepared but not whether another
        worker resolved a later turn for the same thread while the route was
        being decided.  Its OWN owner record is always safe to remove, because
        the token identifies exactly the record this resolve wrote; its objects
        are not, because a concurrent resolve on the same generation parks under
        the same keys and names them in a record of its own.

        The ledger is therefore read BEFORE any object is deleted -- the old
        order deleted the bytes first and only then asked who owned them, which
        is how a discard could destroy the objects a live turn was about to
        install -- and this resolve's own record is deleted LAST. That ordering
        is what keeps the cleanup obligation durable: the caller logs a failed
        discard and does not retry it, so a record dropped ahead of a failing
        scan would leave bytes that no later expiry sweep could ever find. While
        the record survives, the worst outcome is bytes that outlive the turn
        and are swept on schedule instead of immediately.
        """

        if isinstance(prepared, PreparedThreadSet):
            # A thread set may own no NEW object yet still have written an
            # owner record for parked copies it re-minted; that record is
            # this prepare's to remove. The re-minted keys are not in
            # ``object_keys``, so they are never deleted here.
            if not prepared.owner_recorded:
                return
        elif not prepared.object_keys:
            # An empty set owns no bytes, so it wrote no owner record and has
            # nothing to protect against. Returning here keeps the common
            # file-free turn at exactly zero store calls.
            return

        owner_key = self._owner_key(thread_key, prepared._owner_token)
        with self._lock:
            protected: set[str] = set()
            for key, record in self._scan_owners():
                # This resolve's own record is skipped rather than deleted up
                # front: it must not protect the very bytes being discarded,
                # and it must still be there if anything below fails.
                if key == owner_key or record.thread_key != thread_key:
                    continue
                protected.update(record.object_keys)
            removed = True
            for key in dict.fromkeys(prepared.object_keys):
                if key in protected:
                    continue
                try:
                    self.objects.delete(key)
                except Exception:  # noqa: BLE001 -- the record below is the recovery
                    # Not fatal and not swallowed either: the bytes are still
                    # there, so the record that names them has to stay too.
                    removed = False
            if removed:
                # Idempotent: the store treats a delete of an absent key as
                # done, so a retried discard is not an error.
                self.objects.delete(owner_key)

    @staticmethod
    def _object_key(*, agent_id: str, generation: str, index: int) -> str:
        """Agent-scoped, worker-minted structure -- never the channel's filename.

        The file ``name`` is text supplied by whoever uploaded it, so letting it
        into the key would hand an uploader a say in where bytes land in the
        private bucket. The name survives on the ref, which is what a person
        needs to see.
        """

        return f"{ATTACHMENT_OBJECT_PREFIX}/{agent_id}/{generation}/{index:03d}.bin"

    # -- retention ----------------------------------------------------------

    def current(self, thread_key: str) -> _AttachmentSet | None:
        """The retained set for this thread, or None once every owner has lapsed.

        Several owners can name one thread at once -- two workers resolving the
        same turn, or a replaced set still inside its retention window -- so
        "the" set is the unexpired owner with the greatest expiry, with the
        owner key breaking a tie so the answer is stable rather than dependent
        on listing order.

        A read path never mutates an expiry it observed: reaping is a separate
        enumerate -> distributed-lock -> exact re-read operation, so a fresh
        resolve landing beside this read cannot be deleted by it.
        """

        now = int(self._clock())
        with self._lock:
            live = (
                (key, record)
                for key, record in self._scan_owners()
                if record.thread_key == thread_key and record.expires_at_epoch > now
            )
            best = max(live, key=lambda owned: (owned[1].expires_at_epoch, owned[0]), default=None)
            return None if best is None else best[1]

    def enumerate_expired(self) -> list[str]:
        """Snapshot expired thread ids without mutating their durable ledgers.

        Deduplicated: a thread with three expired owners is still one thread to
        reap, and the sweep below removes all of them in one pass.
        """

        now = int(self._clock())
        with self._lock:
            return sorted(
                {
                    record.thread_key
                    for _key, record in self._scan_owners()
                    if record.expires_at_epoch <= now
                }
            )

    def begin_expired_reap(self, thread_key: str) -> AttachmentReapCandidate | None:
        """Re-read this thread's owners and delete only what no live one names.

        The enumeration above is advisory, so this re-reads the ledger: a set
        recorded by another worker after the snapshot is a LIVE owner here and
        protects every object key it names, including a key an expired owner
        names too. Deleting per expired set instead would take bytes out from
        under a turn that is still entitled to them.
        """

        now = int(self._clock())
        with self._lock:
            expired: list[tuple[str, _AttachmentSet]] = []
            protected: set[str] = set()
            for key, record in self._scan_owners():
                if record.thread_key != thread_key:
                    continue
                if record.expires_at_epoch <= now:
                    expired.append((key, record))
                else:
                    protected.update(record.object_keys)
            if not expired:
                return None
            deleted: list[str] = []
            for key in dict.fromkeys(
                object_key for _owner, record in expired for object_key in record.object_keys
            ):
                if key in protected:
                    continue
                self.objects.delete(key)
                deleted.append(key)
            return AttachmentReapCandidate(
                owners=tuple(expired), deleted_object_keys=tuple(deleted)
            )

    def finish_expired_reap(self, candidate: AttachmentReapCandidate) -> bool:
        """Delete only the UNCHANGED owners, after the caller fences its lock.

        The object deletes in ``begin`` can outlive the original lease, so the
        exact-record comparison is the second fence behind the route lock. It is
        per owner and by key, not by thread: a fresh resolve between the two
        halves mints its own record and leaves every observed one untouched, so
        the sweep completes instead of abandoning a genuinely expired set. An
        observed owner that is gone or different was discarded or rewritten
        under this reap, and the answer is False rather than a delete of
        whatever occupies the thread now.
        """

        with self._lock:
            intact = True
            for key, record in candidate.owners:
                try:
                    observed = self._load_key(key)
                except Exception as exc:
                    if self._is_missing_object(exc):
                        intact = False
                        continue
                    raise
                if observed != record:
                    intact = False
                    continue
                self.objects.delete(key)
            return intact

    def _record(self, thread_key: str, prepared: PreparedAttachments) -> None:
        """Write this resolve's own immutable owner record, exactly once.

        One key per resolve rather than one per thread. A single mutable
        ``_attachments/<digest>.json`` made the last writer the only owner, so
        the other worker's installed objects were named by nothing -- beyond the
        reach of every sweep and every discard. The payload is unchanged v1.
        """

        with self._lock:
            record = _AttachmentSet(
                thread_key=thread_key,
                refs=prepared.refs,
                object_keys=prepared.object_keys,
                expires_at_epoch=prepared.retention_expires_at_epoch,
            )
            self.objects.put_stream(
                self._owner_key(thread_key, prepared._owner_token), (record.encode(),)
            )

    @staticmethod
    def _owner_key(thread_key: str, owner_token: str) -> str:
        """The immutable key this resolve's record lives at, derived not stored.

        The thread digest keeps a thread's owners together for a human reading
        the bucket; the token is what makes the key unique per resolve.
        """

        digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()
        return f"{ATTACHMENT_LEDGER_PREFIX}/{digest}/{owner_token}.json"

    def _scan_owners(self) -> Iterator[tuple[str, _AttachmentSet]]:
        """THE owner discovery path: one listing of the whole ledger prefix.

        Never a listing of one thread's subprefix. ``list_keys`` matches on
        ``<prefix>/``, so scoping the scan to ``<prefix>/<digest>`` would walk
        straight past the pre-upgrade ``<prefix>/<digest>.json`` record that is
        already in the bucket of every deployment that ran the mutable ledger,
        and strand its objects forever. One scan and one decoder means an old
        record and a new one are the same thing to every caller, with no
        migration step and no second code path to keep honest.

        A key can be deleted by another worker's discard or reap between the
        listing and the read, which is ordinary rather than exceptional, so a
        missing object is skipped. Nothing else is: a record that is present but
        undecodable is a real fault and still raises.
        """

        for key in tuple(self.objects.list_keys(ATTACHMENT_LEDGER_PREFIX)):
            try:
                record = self._load_key(key)
            except Exception as exc:
                if self._is_missing_object(exc):
                    continue
                raise
            yield key, record

    def _load_key(self, key: str) -> _AttachmentSet:
        payload = b"".join(self.objects.get_stream(key))
        if len(payload) > 256 * 1024:
            raise AttachmentResolutionError(
                "retention-ledger", "private attachment record is oversized"
            )
        return _AttachmentSet.decode(payload)

    @staticmethod
    def _is_missing_object(exc: Exception) -> bool:
        if isinstance(exc, (KeyError, FileNotFoundError)):
            return True
        response = getattr(exc, "response", None)
        if not isinstance(response, Mapping):
            return False
        error = response.get("Error")
        code = error.get("Code") if isinstance(error, Mapping) else None
        return str(code) in {"404", "NoSuchKey", "NotFound"}
