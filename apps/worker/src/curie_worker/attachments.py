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
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from aci_protocol import Attachment
from aci_protocol.turn import DEFAULT_IDENTITY

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

#: Where a resolved attachment lands inside the sandbox, on EVERY substrate.
#: Kubernetes reaches it through an init container and a mounted emptyDir;
#: Docker bind-mounts it directly. The runner probes this same path
#: (``curie_runner.__main__.ATTACHMENTS_DIR``) and the chart mounts its volume
#: there, so the three must agree -- a substrate that materialized files
#: somewhere else would leave the agent probing an empty directory and reporting
#: no attachment for a file that did arrive, which is the silent loss #2567
#: exists to close.
ATTACHMENTS_MOUNT_PATH = "/attachments"

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

    def _download_url(self, file_id: str) -> str:
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
    holds no bot token for it.
    """

    def __init__(
        self,
        *,
        files: AttachmentFilePort | None,
        objects: WorkspaceObjectPort,
        limits: AttachmentLimits | None = None,
        clock: Callable[[], float] = time.time,
        identity_files: Mapping[str, AttachmentFilePort] | None = None,
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

    # -- resolve ------------------------------------------------------------

    def resolve(
        self,
        *,
        thread_key: str,
        agent_id: str | None,
        attachments: Sequence[Attachment],
        generation: str | None = None,
        identity: str = DEFAULT_IDENTITY,
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
        files = self._files_for(identity)

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
                digest, size = self._park(attachment, key, files)
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

    def _files_for(self, identity: str) -> AttachmentFilePort:
        """The download for ``identity``, refused before any fetch when absent."""

        files = self.files if identity == DEFAULT_IDENTITY else self._identity_files.get(identity)
        if files is None:
            raise AttachmentResolutionError(
                "credential",
                f"no Slack bot token is configured on this worker for identity {identity!r}",
            )
        return files

    def _park(
        self, attachment: Attachment, key: str, files: AttachmentFilePort
    ) -> tuple[str, int]:
        """Stream one file into the store under its cap, returning digest+size."""

        digest = hashlib.sha256()
        counted = [0]
        try:
            self.objects.put_stream(
                key,
                self._bounded(
                    files.fetch(attachment.id),
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

    def discard_prepared(self, *, thread_key: str, prepared: PreparedAttachments) -> None:
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

        if not prepared.object_keys:
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
