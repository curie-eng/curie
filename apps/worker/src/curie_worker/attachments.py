"""Resolve inbound channel attachment REFS into sandbox-readable objects.

The dispatcher records file *references* on the wire and nothing else:
``aci_protocol.Attachment`` carries ``id``/``name`` and deliberately no url and
no bytes, because a carried channel URL would invite a sandbox-side fetch that
ADR-0032's default-deny egress forbids.  This module is the other half.  The
worker holds the bot token and can reach the channel, so it downloads each
referenced file, parks the bytes in the private object store under its OWN
prefix, and mints a short-lived one-object read capability that the sandbox's
attachment init container redeems.  The bot token never leaves the worker, which
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
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from aci_protocol import Attachment

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

# Where the channel's file metadata is looked up. Only Slack today; the id in
# ``Attachment`` is the channel's own file id, so resolving it is the port's
# job, not this module's.
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
    rather than to the resolver above it.
    """


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

    def __post_init__(self) -> None:
        numeric = {
            "max_file_bytes": self.max_file_bytes,
            "read_chunk_bytes": self.read_chunk_bytes,
            "reference_ttl_seconds": self.reference_ttl_seconds,
            "retention_ttl_seconds": self.retention_ttl_seconds,
            "max_files": self.max_files,
        }
        for name, value in numeric.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")


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


@dataclass(frozen=True)
class PreparedAttachments:
    """One turn's resolved set: what was written, what was minted, until when."""

    refs: tuple[AttachmentRef, ...]
    object_keys: tuple[str, ...]
    retention_expires_at_epoch: int

    def claim_env(self) -> dict[str, str]:
        """The claim-env contribution, EMPTY for a turn carrying no files.

        Empty rather than an empty-valued key: an init container reads a present
        key as "there is work here", so the overwhelming majority of turns must
        contribute nothing at all and keep today's boot env byte-identical.
        """

        if not self.refs:
            return {}
        return {ATTACHMENTS_REF_ENV: encode_attachment_refs(self.refs)}


@dataclass(frozen=True)
class _AttachmentSet:
    """The durable retention record for one thread's latest resolved set."""

    thread_key: str
    refs: tuple[AttachmentRef, ...]
    object_keys: tuple[str, ...]
    expires_at_epoch: int

    def encode(self) -> bytes:
        return json.dumps(
            {
                "version": 1,
                "thread_key": self.thread_key,
                "expires_at_epoch": self.expires_at_epoch,
                "object_keys": list(self.object_keys),
                "refs": encode_attachment_refs(self.refs),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def decode(
        cls, payload: bytes, *, expected_thread_key: str | None = None
    ) -> _AttachmentSet:
        try:
            raw = json.loads(payload)
            if raw.get("version") != 1:
                raise ValueError("unsupported attachment record version")
            thread_key = str(raw["thread_key"])
            if expected_thread_key is not None and thread_key != expected_thread_key:
                raise ValueError("attachment record names a different thread")
            object_keys = tuple(str(key) for key in raw["object_keys"])
            record = cls(
                thread_key=thread_key,
                refs=decode_attachment_refs(str(raw["refs"])),
                object_keys=object_keys,
                expires_at_epoch=int(raw["expires_at_epoch"]),
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
    """Exact expired ledger observed before its potentially slow object cleanup."""

    record: _AttachmentSet
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


class SlackFileClient:
    """Download one Slack file's bytes with the bot token, and only here.

    ``url_private`` is NOT public: the download must carry
    ``Authorization: Bearer <token>``, and Slack answers an unauthenticated
    request with an HTML sign-in page rather than the bytes
    (https://docs.slack.dev/reference/objects/file-object).  The token
    authenticates exactly these two requests and reaches nothing downstream --
    what the sandbox receives is a presigned one-object URL from the private
    object store.
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
        url_private = self._url_private(file_id)
        response = self._transport(
            method="GET",
            url=url_private,
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            raise SlackFileError(
                f"slack file download failed: HTTP {response.status}"
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

    def _url_private(self, file_id: str) -> str:
        response = self._transport(
            method="GET",
            url=f"{self._api_url}/files.info?file={file_id}",
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            raise SlackFileError(f"slack files.info failed: HTTP {response.status}")
        body = self._bounded_body(response.chunks)
        try:
            payload = json.loads(body)
            if not payload.get("ok"):
                raise SlackFileError(f"slack files.info failed: {payload.get('error')}")
            url_private = str(payload["file"]["url_private"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SlackFileError("slack files.info returned an unusable envelope") from exc
        if not url_private.startswith("https://"):
            raise SlackFileError("slack files.info returned a non-HTTPS url_private")
        return url_private

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
    against.
    """

    def __init__(
        self,
        *,
        files: AttachmentFilePort,
        objects: WorkspaceObjectPort,
        limits: AttachmentLimits | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.files = files
        self.objects = objects
        self.limits = limits or AttachmentLimits()
        self._clock = clock
        self._lock = threading.Lock()

    # -- resolve ------------------------------------------------------------

    def resolve(
        self,
        *,
        thread_key: str,
        agent_id: str | None,
        attachments: Sequence[Attachment],
        generation: str | None = None,
    ) -> PreparedAttachments:
        """Download, park and mint the whole set, or refuse it and leave nothing.

        The refusal is loud and total by design (see the module docstring): a
        partial set reads to an agent exactly like a complete one.
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
                digest, size = self._park(attachment, key)
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

    def _park(self, attachment: Attachment, key: str) -> tuple[str, int]:
        """Stream one file into the store under its cap, returning digest+size."""

        digest = hashlib.sha256()
        counted = [0]
        try:
            self.objects.put_stream(
                key,
                self._bounded(
                    self.files.fetch(attachment.id),
                    name=attachment.name,
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
            raise AttachmentFetchError(attachment.name, str(exc)) from exc
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
        """The retained set for this thread, or None once it has lapsed.

        A read path never mutates an expiry it observed: reaping is a separate
        enumerate -> distributed-lock -> exact re-read operation, so a fresh
        resolve landing beside this read cannot be deleted by it.
        """

        with self._lock:
            record = self._load(thread_key)
            if record is None or record.expires_at_epoch <= int(self._clock()):
                return None
            return record

    def enumerate_expired(self) -> list[str]:
        """Snapshot expired thread ids without mutating their durable ledgers."""

        expired: list[str] = []
        now = int(self._clock())
        with self._lock:
            for ledger_key in tuple(self.objects.list_keys(ATTACHMENT_LEDGER_PREFIX)):
                record = self._load_key(ledger_key)
                if record.expires_at_epoch <= now:
                    expired.append(record.thread_key)
        return sorted(expired)

    def begin_expired_reap(self, thread_key: str) -> AttachmentReapCandidate | None:
        """Re-read one expiry and delete only the objects that snapshot names.

        The enumeration above is advisory, so this re-reads the exact ledger: a
        set recorded by another worker after the snapshot must not be deleted
        out from under a live turn.
        """

        with self._lock:
            record = self._load(thread_key)
            if record is None or record.expires_at_epoch > int(self._clock()):
                return None
            deleted: list[str] = []
            for key in dict.fromkeys(record.object_keys):
                self.objects.delete(key)
                deleted.append(key)
            return AttachmentReapCandidate(record=record, deleted_object_keys=tuple(deleted))

    def finish_expired_reap(self, candidate: AttachmentReapCandidate) -> bool:
        """Delete only the UNCHANGED ledger, after the caller fences its lock.

        The object deletes in ``begin`` can outlive the original lease, so the
        exact-record comparison is the second fence behind the route lock: a
        fresh resolve landing between the two halves survives.
        """

        with self._lock:
            current = self._load(candidate.record.thread_key)
            if current != candidate.record:
                return False
            self.objects.delete(self._ledger_key(candidate.record.thread_key))
            return True

    def _record(self, thread_key: str, prepared: PreparedAttachments) -> None:
        with self._lock:
            record = _AttachmentSet(
                thread_key=thread_key,
                refs=prepared.refs,
                object_keys=prepared.object_keys,
                expires_at_epoch=prepared.retention_expires_at_epoch,
            )
            self.objects.put_stream(self._ledger_key(thread_key), (record.encode(),))

    @staticmethod
    def _ledger_key(thread_key: str) -> str:
        digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()
        return f"{ATTACHMENT_LEDGER_PREFIX}/{digest}.json"

    def _load(self, thread_key: str) -> _AttachmentSet | None:
        key = self._ledger_key(thread_key)
        try:
            return self._load_key(key, expected_thread_key=thread_key)
        except Exception as exc:
            if self._is_missing_object(exc):
                return None
            raise

    def _load_key(
        self, key: str, *, expected_thread_key: str | None = None
    ) -> _AttachmentSet:
        payload = b"".join(self.objects.get_stream(key))
        if len(payload) > 256 * 1024:
            raise AttachmentResolutionError(
                "retention-ledger", "private attachment record is oversized"
            )
        return _AttachmentSet.decode(payload, expected_thread_key=expected_thread_key)

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
