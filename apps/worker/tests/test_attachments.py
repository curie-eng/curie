"""Resolve inbound attachment REFS into sandbox-readable objects (#2567, S3).

The dispatcher put file *references* on the wire and nothing else: ``Attachment``
carries ``id``/``name`` and deliberately no url and no bytes (see its docstring).
This seam is the other half -- the worker, which holds the bot token and can
reach Slack, turns each ref into private object-store bytes plus a short-lived
one-object read capability the sandbox's attachment init container can redeem.
The token never leaves the worker, which is what keeps ADR-0075's boundary
intact while the Agent Proxy is unbuilt.

**What is mocked and why.** Slack only. ``FakeSlackFiles`` stands in for the
external service the download hits; the object store is a conforming
implementation of the worker's own ``WorkspaceObjectPort``, so the assertions
below are about keys that really got written and bytes that really moved. See
``attachment_fixtures.py`` for both.

**Failing closed is the point** (ADR-0059 decision 3). The size cap is enforced
while streaming, in the shape of ``_read_bounded_upload``
(``apps/api/src/curie_api/routers/bundles.py``): the chunk loop refuses the
moment the running total crosses the cap, so memory never holds more than one
chunk past it. ``test_the_oversize_download_stops_pulling_chunks_at_the_cap`` is
the only way a test can observe that boundedness -- it counts what the reader
was asked for -- and a resolver that buffers the whole body and then measures it
fails there while still passing the plain refusal case.
"""

from __future__ import annotations

import hashlib
import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Attachment

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from attachment_fixtures import (  # noqa: E402
    AGENT_ID,
    THREAD_KEY,
    FakeSlackFiles,
    MovableClock,
    RetainingObjectStore,
    chunked,
    limits,
)


@pytest.fixture
def attachments() -> Any:
    """Load the new worker-local seam per test so every red contract collects.

    Same arrangement as ``test_workspace.py``'s ``workspace`` fixture: the module
    under test does not exist yet, so each node fails on its own missing seam
    rather than on one shared collection error.
    """

    return importlib.import_module("curie_worker.attachments")


def _ref(file_id: str = "F1", name: str = "report.csv", **overrides: Any) -> Attachment:
    fields: dict[str, Any] = {
        "id": file_id,
        "name": name,
        "mime_type": "text/csv",
        "size_bytes": None,
    }
    fields.update(overrides)
    return Attachment(**fields)


def _coordinator(
    module: Any,
    files: Any,
    *,
    objects: RetainingObjectStore | None = None,
    bounds: Any | None = None,
    clock: Any | None = None,
) -> tuple[Any, RetainingObjectStore]:
    store = objects or RetainingObjectStore()
    coordinator = module.AttachmentCoordinator(
        files=files,
        objects=store,
        limits=bounds or limits(module),
        clock=clock or MovableClock(),
    )
    return coordinator, store


def _resolve(coordinator: Any, refs: list[Attachment], *, generation: str = "gen-1") -> Any:
    return coordinator.resolve(
        thread_key=THREAD_KEY,
        agent_id=AGENT_ID,
        attachments=refs,
        generation=generation,
    )


# --- The common case: no attachments at all ------------------------------------


def test_a_turn_with_no_attachments_stores_nothing_and_never_calls_slack(
    attachments: Any,
) -> None:
    """The regression guard for every turn that exists today.

    ``QueuedTurn.attachments`` defaults to the empty list, so this is what the
    overwhelming majority of turns look like and it must stay exactly as cheap
    as it is now: no Slack round trip, no object written, and -- the part the
    claim path cares about -- an EMPTY claim-env contribution, so the sandbox
    boot env is byte-identical to the one it gets today.
    """

    files = FakeSlackFiles()
    coordinator, objects = _coordinator(attachments, files)

    prepared = _resolve(coordinator, [])

    assert files.requested == []
    assert objects.objects == {}
    assert objects.signed == []
    assert prepared.refs == ()
    assert prepared.object_keys == ()
    assert prepared.claim_env() == {}


# --- One attachment ------------------------------------------------------------


def test_one_attachment_is_stored_under_the_attachment_prefix_with_a_digest_of_its_bytes(
    attachments: Any,
) -> None:
    """The digest is of the ACTUAL bytes, and the key is agent-scoped.

    ``sha256`` is what the init container verifies before it materializes the
    file, so a digest computed from anything other than the bytes that landed in
    the store (Slack's reported ``size``, say) would make that verification a
    no-op. The key lives under the attachment lane's OWN prefix -- never under
    the workspace archives' -- because the two object classes have different
    owners, different TTLs and different reapers.
    """

    payload = b"col-a,col-b\n1,2\n"
    files = FakeSlackFiles({"F1": chunked(payload, 8)})
    coordinator, objects = _coordinator(attachments, files)

    prepared = _resolve(coordinator, [_ref("F1", "report.csv")])

    assert files.requested == ["F1"]
    assert len(prepared.object_keys) == 1
    key = prepared.object_keys[0]
    assert key.startswith(f"{attachments.ATTACHMENT_OBJECT_PREFIX}/")
    assert AGENT_ID in key, "the attachment object key must be agent-scoped"
    assert objects.objects[key] == payload

    (ref,) = prepared.refs
    assert ref.sha256 == hashlib.sha256(payload).hexdigest()
    assert ref.name == "report.csv"
    assert ref.size_bytes == len(payload)


def test_the_minted_reference_is_a_short_lived_signed_one_object_capability(
    attachments: Any,
) -> None:
    """Shaped like ``WorkspaceRef``: url + sha256 + expires_at_epoch.

    The sandbox is given a capability for exactly one object and nothing else --
    no object-store credential, no bucket listing -- and it expires on the
    REFERENCE ttl rather than the retention window, so a leaked env var is
    useless minutes later even though the bytes are kept longer for a retry.
    """

    payload = b"hello"
    clock = MovableClock()
    files = FakeSlackFiles({"F1": chunked(payload, 4)})
    coordinator, objects = _coordinator(
        attachments,
        files,
        bounds=limits(attachments, reference_ttl_seconds=120),
        clock=clock,
    )

    prepared = _resolve(coordinator, [_ref("F1")])
    (ref,) = prepared.refs

    assert objects.signed == [(prepared.object_keys[0], 120)]
    assert ref.url == f"https://objects.example.com/{prepared.object_keys[0]}?one-object=yes"
    assert ref.expires_at_epoch == int(clock.now) + 120
    assert ref.expires_in_seconds > 0

    # Round-trips through the opaque claim-env encoding, digest intact.
    encoded = prepared.claim_env()[attachments.ATTACHMENTS_REF_ENV]
    decoded = attachments.decode_attachment_refs(encoded)
    assert [(item.name, item.sha256, item.url) for item in decoded] == [
        (ref.name, ref.sha256, ref.url)
    ]


def test_a_hostile_filename_never_reaches_the_object_key(attachments: Any) -> None:
    """The name is channel-supplied text; the key is worker-minted structure.

    Slack's file ``name`` comes from whoever uploaded it, so letting it into the
    key would hand an uploader a say in where bytes land in the private bucket.
    The name survives on the ref -- that is what a person needs to see -- and the
    key is built from the agent, the generation and the index instead.
    """

    files = FakeSlackFiles({"F1": [b"x"]})
    coordinator, objects = _coordinator(attachments, files)

    prepared = _resolve(coordinator, [_ref("F1", "../../etc/passwd")])
    key = prepared.object_keys[0]

    assert ".." not in key
    assert "etc/passwd" not in key
    assert list(objects.list_keys(attachments.ATTACHMENT_OBJECT_PREFIX)) == [key]
    assert prepared.refs[0].name == "../../etc/passwd"


# --- The size cap, both directions ---------------------------------------------


def test_a_file_just_under_the_cap_is_stored_whole(attachments: Any) -> None:
    payload = b"u" * 64
    files = FakeSlackFiles({"F1": chunked(payload, 16)})
    coordinator, objects = _coordinator(
        attachments, files, bounds=limits(attachments, max_file_bytes=64, read_chunk_bytes=16)
    )

    prepared = _resolve(coordinator, [_ref("F1")])

    assert objects.objects[prepared.object_keys[0]] == payload
    assert prepared.refs[0].sha256 == hashlib.sha256(payload).hexdigest()


def test_a_file_over_the_cap_is_refused_loudly_and_leaves_no_partial_object(
    attachments: Any,
) -> None:
    """Fail closed: the refusal names the file, and the store is left clean.

    ``RetainingObjectStore`` keeps every chunk it was handed, so a resolver that
    streams straight into ``put_stream`` and lets the cap raise through it leaves
    a truncated object under a key a signed capability could still be minted for.
    Deleting it -- or capping before the upload starts -- is the only way past
    this assertion.
    """

    payload = b"o" * 96
    files = FakeSlackFiles({"F1": chunked(payload, 16)})
    coordinator, objects = _coordinator(
        attachments, files, bounds=limits(attachments, max_file_bytes=64, read_chunk_bytes=16)
    )

    with pytest.raises(attachments.AttachmentTooLargeError) as refusal:
        _resolve(coordinator, [_ref("F1", "huge.bin")])

    assert isinstance(refusal.value, attachments.AttachmentResolutionError)
    message = str(refusal.value)
    assert "huge.bin" in message, f"the refusal must name the file: {message!r}"
    assert "64" in message, f"the refusal must name the cap it enforced: {message!r}"
    assert objects.objects == {}, f"a partial object survived the refusal: {list(objects.objects)}"
    assert objects.signed == [], "no capability may be minted for a refused attachment"


def test_the_oversize_download_stops_pulling_chunks_at_the_cap(attachments: Any) -> None:
    """Memory boundedness, in the only form a test can observe it.

    Four chunks of 16 bytes against a 32-byte cap: the running total crosses at
    chunk three, so exactly three may be pulled and the fourth must never be
    requested. That is the ``_read_bounded_upload`` shape -- reject the moment
    the accumulated size crosses the bound -- rather than "read it all, then
    measure", which would pull all four and still raise, passing the refusal
    case above while holding the whole body in memory.
    """

    payload = b"z" * 64
    chunks = chunked(payload, 16)
    assert len(chunks) == 4
    files = FakeSlackFiles({"F1": chunks})
    coordinator, _objects = _coordinator(
        attachments, files, bounds=limits(attachments, max_file_bytes=32, read_chunk_bytes=16)
    )

    with pytest.raises(attachments.AttachmentTooLargeError):
        _resolve(coordinator, [_ref("F1")])

    delivered = files.delivered["F1"]
    assert len(delivered) == 3, (
        "the reader must be consumed in chunks and refused at the crossing chunk, "
        f"but {len(delivered)} of {len(chunks)} chunks were pulled"
    )


# --- A Slack fetch that fails ---------------------------------------------------


@pytest.mark.parametrize(
    ("failure", "label"),
    [
        # Slack answers 200 with ``{"ok": false, "error": "..."}`` for an
        # application-level failure, and the two below are the ones this seam can
        # actually provoke: a ``missing_scope`` when the workspace has not
        # reinstalled with ``files:read``, and ``file_not_found`` when the upload
        # was deleted between the event and the turn. Modelled as the port's own
        # error rather than as an HTTP status, because translating Slack's
        # envelope is the port's job, not the resolver's.
        # https://docs.slack.dev/reference/methods/files.info
        (RuntimeError("slack files.info failed: missing_scope"), "auth"),
        (RuntimeError("slack files.info failed: file_not_found"), "missing"),
        (TimeoutError("slack file download timed out"), "transport"),
    ],
)
def test_a_slack_fetch_failure_refuses_the_turn_rather_than_pretending_files_arrived(
    attachments: Any, failure: BaseException, label: str
) -> None:
    """The observable a turn gets is a refusal, never a silently empty set.

    The failure mode this closes is the quiet one: a resolver that swallowed the
    error and returned an empty ``PreparedAttachments`` would boot a sandbox with
    no files, and the agent would answer "I can't see any attachment" about a
    message that visibly carries one. So the fetch failure propagates as an
    ``AttachmentResolutionError`` naming the file, nothing is stored, and no
    capability is minted -- the caller decides what the person is told.
    """

    files = FakeSlackFiles()
    files.failures["F1"] = failure
    coordinator, objects = _coordinator(attachments, files)

    with pytest.raises(attachments.AttachmentFetchError) as refusal:
        _resolve(coordinator, [_ref("F1", "notes.pdf")])

    assert isinstance(refusal.value, attachments.AttachmentResolutionError)
    assert refusal.value.__cause__ is failure, "the underlying Slack error must be preserved"
    assert "notes.pdf" in str(refusal.value)
    assert objects.objects == {}
    assert objects.signed == []


# --- Several attachments -------------------------------------------------------


def test_every_attachment_is_stored_under_one_generation_in_wire_order(
    attachments: Any,
) -> None:
    payloads = {
        "F1": b"first" * 3,
        "F2": b"second" * 2,
        "F3": b"third",
    }
    files = FakeSlackFiles({fid: chunked(body, 8) for fid, body in payloads.items()})
    coordinator, objects = _coordinator(attachments, files)

    prepared = _resolve(
        coordinator,
        [_ref("F1", "a.txt"), _ref("F2", "b.txt"), _ref("F3", "c.txt")],
        generation="gen-multi",
    )

    assert files.requested == ["F1", "F2", "F3"]
    assert [ref.name for ref in prepared.refs] == ["a.txt", "b.txt", "c.txt"]
    assert len(set(prepared.object_keys)) == 3, "each attachment needs its own key"
    assert all("gen-multi" in key for key in prepared.object_keys)
    for ref, key, body in zip(prepared.refs, prepared.object_keys, payloads.values(), strict=True):
        assert objects.objects[key] == body
        assert ref.sha256 == hashlib.sha256(body).hexdigest()
    assert len(prepared.claim_env()) == 1, "one env var carries the whole set"


def test_one_failing_attachment_refuses_the_whole_set_and_leaves_no_objects_behind(
    attachments: Any,
) -> None:
    """INTENDED SEMANTICS: all-or-nothing, stated here deliberately.

    The alternative -- keep the files that resolved, drop the one that did not --
    is the more forgiving behavior and the wrong one. The agent cannot tell a
    partial set from a complete one: it would read two of three spreadsheets and
    answer with total confidence about the whole upload, and nothing downstream
    could detect that the third was missing. A refusal is legible; a confident
    answer from an incomplete set is not.

    So a single failing entry refuses the resolve, and the objects that DID land
    for its neighbours are removed -- an orphan object under a key no ledger
    names is exactly what the retention sweep cannot reach.
    """

    files = FakeSlackFiles({"F1": [b"ok-bytes"], "F3": [b"also-ok"]})
    files.failures["F2"] = RuntimeError("slack files.info failed: file_not_found")
    coordinator, objects = _coordinator(attachments, files)

    with pytest.raises(attachments.AttachmentFetchError) as refusal:
        _resolve(coordinator, [_ref("F1", "a.txt"), _ref("F2", "b.txt"), _ref("F3", "c.txt")])

    assert "b.txt" in str(refusal.value)
    assert objects.objects == {}, (
        "the neighbours' objects outlived the refusal and no ledger names them: "
        f"{list(objects.objects)}"
    )
    # A capability minted for an earlier entry is not itself a leak -- nothing
    # returns it -- but it must not point at anything that still exists, or the
    # refused set is half-live in the bucket.
    assert all(key not in objects.objects for key, _ttl in objects.signed), (
        "a signed capability outlived the object it points at"
    )
    # F3 is never fetched: the refusal stops the loop rather than paying for
    # every remaining download before failing anyway.
    assert files.requested == ["F1", "F2"]


def test_more_attachments_than_the_cap_allows_are_refused_before_any_download(
    attachments: Any,
) -> None:
    """``max_files`` bounds the whole set, and is checked before the first fetch.

    The refs array arrives from outside, so an unbounded loop over it is an
    unbounded cost -- the same reason ``derive_attachments`` bounds its own read
    of Slack's ``files``.
    """

    files = FakeSlackFiles({f"F{n}": [b"x"] for n in range(5)})
    coordinator, objects = _coordinator(
        attachments, files, bounds=limits(attachments, max_files=3)
    )

    with pytest.raises(attachments.AttachmentResolutionError):
        _resolve(coordinator, [_ref(f"F{n}", f"{n}.txt") for n in range(5)])

    assert files.requested == [], "the set is refused before anything is downloaded"
    assert objects.objects == {}


# --- The bot token stays on the worker -----------------------------------------


def test_the_slack_file_client_sends_the_bot_token_and_never_leaks_it_onward(
    attachments: Any,
) -> None:
    """The token authenticates the fetch and appears nowhere the sandbox can see.

    Slack's file object exposes ``url_private``, which is NOT public: the
    download must carry ``Authorization: Bearer <token>``, and Slack answers an
    unauthenticated request with an HTML sign-in page rather than the bytes.
    https://docs.slack.dev/reference/objects/file-object

    The half that matters for ADR-0075 is the second one: whatever the client did
    with the token, none of it reaches the object keys or the claim env, because
    the sandbox only ever gets a presigned one-object URL.
    """

    token = "xoxb-not-a-real-bot-token"
    payload = b"private-bytes"
    calls: list[dict[str, Any]] = []

    def transport(**request: Any) -> Any:
        calls.append(request)
        if "files.info" in request["url"]:
            body = (
                b'{"ok":true,"file":{"id":"F1","name":"report.csv",'
                b'"url_private":"https://files.slack.com/files-pri/T1-F1/report.csv"}}'
            )
            return attachments.SlackFileResponse(
                status=200,
                headers={"Content-Type": "application/json"},
                chunks=iter([body]),
            )
        return attachments.SlackFileResponse(
            status=200,
            headers={"Content-Type": "text/csv"},
            chunks=iter([payload]),
        )

    client = attachments.SlackFileClient(token=token, transport=transport)
    coordinator, objects = _coordinator(attachments, client)

    prepared = _resolve(coordinator, [_ref("F1", "report.csv")])

    download = calls[-1]
    assert download["url"].startswith("https://files.slack.com/files-pri/")
    assert download["headers"]["Authorization"] == f"Bearer {token}"
    assert objects.objects[prepared.object_keys[0]] == payload
    assert all(token not in key for key in prepared.object_keys)
    assert all(token not in value for value in prepared.claim_env().values())
    assert all(token not in ref.url for ref in prepared.refs)


# --- Bounds and keys -----------------------------------------------------------


def test_a_non_positive_limit_is_refused_rather_than_meaning_unlimited(
    attachments: Any,
) -> None:
    """Mirrors ``WorkspaceLimits.__post_init__``.

    A zero cap that silently means "unlimited" is how a bounded ingestion path
    stops being bounded, so every bound is validated at construction.
    """

    bounds = (
        "max_file_bytes",
        "read_chunk_bytes",
        "reference_ttl_seconds",
        "retention_ttl_seconds",
        "max_files",
    )
    for bound in bounds:
        with pytest.raises(ValueError):
            limits(attachments, **{bound: 0})


def test_a_second_resolve_of_the_same_turn_never_reuses_an_object_key(
    attachments: Any,
) -> None:
    """A retry writes new objects rather than overwriting live ones.

    Reusing the key would replace bytes an earlier claim's still-valid
    capability points at -- the same reason the workspace lane mints a
    generation per claim rather than keying on the thread alone. With no
    explicit ``generation`` the coordinator mints its own.
    """

    files = FakeSlackFiles({"F1": [b"one"]})
    coordinator, objects = _coordinator(attachments, files)
    first = coordinator.resolve(
        thread_key=THREAD_KEY, agent_id=AGENT_ID, attachments=[_ref("F1")]
    )
    files.payloads["F1"] = [b"two"]
    second = coordinator.resolve(
        thread_key=THREAD_KEY, agent_id=AGENT_ID, attachments=[_ref("F1")]
    )

    assert first.object_keys != second.object_keys
    assert set(objects.objects) >= set(first.object_keys) | set(second.object_keys)
