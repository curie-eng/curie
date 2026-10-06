"""Every boot rebuilds the thread's files: the worker lane half (ADR 0205, #4079).

ADR 0205 moves a file's reach from one sandbox to the thread. The kernel reads
the thread's ledger and hands it to the attachment lane, which turns it into one
boot's set: the current message's files (all or nothing, as ADR 0153 decides)
plus every earlier file the budget admits, each re-minted from the parked cache
or fetched again through the agent's CURRENT bindings, and each earlier file
that cannot be had named as unavailable instead of failing the boot.

Contract pinned here (the implementer follows these names; the kernel side is
pinned in ``tests/kernel/test_thread_attachment_rebuild.py``):

``curie_worker.attachments``
    ``ATTACHMENTS_MANIFEST_ENV = "CURIE_ATTACHMENTS_MANIFEST"``.
    ``clean_attachment_leaf(name: str) -> str``: the init container's cleaning
        (``\\`` -> ``/``, strip, basename) with a leading ``.`` mapped to ``_``;
        an empty, ``.`` or ``..`` result raises ``AttachmentResolutionError``
        with ``stage == "name"``.
    ``assign_disk_names(names: Sequence[str], *, taken: Iterable[str])
        -> tuple[str, ...]``: cleans each name and disambiguates it with
        ``unique_attachment_leaf`` against ``taken`` plus the names already
        assigned earlier in the same call.
    ``AgentRoute(kind: str, adapter: str | None, endpoint: str | None)``:
        one of the agent's current bindings (``BindingResolver.
        routes_for_agent``). Any object with those three attributes serves.
    ``AttachmentLimits`` gains ``thread_max_files: int = 20`` and
        ``thread_max_bytes: int = 256 MiB``; both must be positive and
        ``thread_max_files >= max_files`` (``ValueError``). No relation between
        ``thread_max_bytes`` and ``max_file_bytes`` is enforced here (the chart
        checks the budget against the attachments volume instead).
    ``ChannelPortFileClient.bind_route(adapter, endpoint) -> AttachmentFilePort``:
        ``bind`` for a route read from the bindings rather than a reply handle,
        with the same two refusals (no endpoint: stage ``wiring``; no secret for
        the adapter: stage ``credential``).
    ``AttachmentCoordinator.prepare_thread_set(*, thread_key, agent_id,
        ledger_refs, current=(), identity=DEFAULT_IDENTITY, handle=None,
        routes=(), deadline_epoch=None, ledger_unavailable=False)
        -> PreparedThreadSet``.
        ``ledger_refs`` are ``curie_worker.ledger_client.ThreadAttachmentRef``
        in arrival order. ``current`` is the message's ``Attachment`` list.
        ``deadline_epoch`` is measured on the coordinator's ``clock``. The
        current message's files are fetched first and any failure raises (and
        leaves no object or owner record behind). Earlier files are then taken
        newest-first within ``thread_max_files`` / ``thread_max_bytes``
        (current files always kept and counted first; recorded ``size_bytes``),
        re-minted from a live parked copy of the same sha256 for this thread
        when one outlives the capability, or fetched again. Capabilities are
        presigned only after every fetch has finished.
    ``PreparedThreadSet``:
        ``entries: tuple[ThreadSetEntry, ...]`` in arrival order, current last;
        ``unavailable: tuple[UnavailableAttachment, ...]`` (``name`` is the
        disk name, ``reason`` one of ``no_route``, ``no_credential``,
        ``not_found``, ``rate_limited``, ``timeout``, ``digest_changed``,
        ``deadline``, ``fetch_failed``); ``omitted: tuple[str, ...]`` (disk
        names, arrival order); ``append_refs: tuple[ThreadAttachmentRef, ...]`` (the current
        message's refs to append after install); ``ledger_unavailable: bool``;
        ``object_keys`` (keys this prepare wrote); ``claim_env()``.
    ``ThreadSetEntry``: ``disk_name, object_key, sha256, size_bytes,
        mime_type, current: bool``.
    ``claim_env()``: ``CURIE_ATTACHMENTS_REF`` carries one entry per
        ``entries`` item with ``"n"`` the exact disk name and ``"c"`` 1 for a
        current file and 0 for an earlier one; ``CURIE_ATTACHMENTS_MANIFEST``
        is ``{"v":1,"files":[{"name","current"}],"unavailable":[{"name",
        "reason"}],"omitted":[name],"ledger_unavailable":bool}``. Nothing at all
        when there is no entry, nothing unavailable or omitted, and the ledger
        read did not fail; the manifest alone when there is no entry but
        something to say.
    ``AttachmentCoordinator.discard_prepared(thread_key=, prepared=)`` takes a
        ``PreparedThreadSet`` too: it removes the objects and owner record this
        prepare wrote and never a reused key another live owner names.
    Retention owner records stay ``"version": 1`` and gain the optional
        ``"agent"`` (agent id) and ``"shas"`` (object key -> sha256) fields;
        ``_AttachmentSet`` exposes them as ``agent_id`` (None when absent) and
        ``shas`` (empty mapping when absent). Reusing a parked key writes a NEW
        owner record for it with a fresh retention expiry.

Slack and the channel adapters are the only fakes; the object store is the
conforming ``RetainingObjectStore`` from ``attachment_fixtures.py``.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Attachment, ReplyHandle

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from attachment_fixtures import (  # noqa: E402
    AGENT_ID,
    THREAD_KEY,
    FakeSlackFiles,
    MovableClock,
    RetainingObjectStore,
    limits,
)

REF_ENV = "CURIE_ATTACHMENTS_REF"
MANIFEST_ENV = "CURIE_ATTACHMENTS_MANIFEST"
ADAPTER = "agentmail"
ADAPTER_SECRET = "adapter-secret-value"
ADAPTER_ENDPOINT = "http://mail-adapter.example:8080/curie"
EMAIL_HANDLE = ReplyHandle(
    kind="email",
    channel="agent@example.test",
    placeholder=None,
    endpoint=ADAPTER_ENDPOINT,
    adapter=ADAPTER,
)


@pytest.fixture
def attachments() -> Any:
    return importlib.import_module("curie_worker.attachments")


@pytest.fixture
def ledger() -> Any:
    return importlib.import_module("curie_worker.ledger_client")


# --- helpers -------------------------------------------------------------------


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _earlier(
    ledger: Any,
    file_id: str,
    payload: bytes,
    *,
    disk_name: str | None = None,
    ordinal: int = 0,
    kind: str = "slack",
    adapter: str | None = None,
    identity: str = "default",
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> Any:
    name = disk_name or f"{file_id.lower()}.bin"
    return ledger.ThreadAttachmentRef(
        file_id=file_id,
        ordinal=ordinal,
        name=name,
        disk_name=name,
        mime_type="application/octet-stream",
        size_bytes=len(payload) if size_bytes is None else size_bytes,
        sha256=sha256 or _sha(payload),
        route_kind=kind,
        route_adapter=adapter,
        route_identity=identity,
    )


def _slack_route(attachments: Any, adapter: str | None = None) -> Any:
    return attachments.AgentRoute(kind="slack", adapter=adapter, endpoint=None)


def _email_route(attachments: Any, endpoint: str = ADAPTER_ENDPOINT) -> Any:
    return attachments.AgentRoute(kind="email", adapter=ADAPTER, endpoint=endpoint)


def _coordinator(
    attachments: Any,
    *,
    files: Any = None,
    store: RetainingObjectStore | None = None,
    clock: MovableClock | None = None,
    identity_files: dict[str, Any] | None = None,
    channel_files: Any = None,
    **limit_overrides: Any,
) -> tuple[Any, RetainingObjectStore, MovableClock]:
    store = store if store is not None else RetainingObjectStore()
    clock = clock if clock is not None else MovableClock()
    bounds = {"max_file_bytes": 1024, **limit_overrides}
    coordinator = attachments.AttachmentCoordinator(
        files=files,
        identity_files=identity_files or {},
        channel_files=channel_files,
        objects=store,
        limits=limits(attachments, **bounds),
        clock=clock,
    )
    return coordinator, store, clock


def _prepare(
    coordinator: Any,
    *,
    ledger_refs: Sequence[Any] = (),
    current: Sequence[Attachment] = (),
    routes: Sequence[Any] = (),
    handle: ReplyHandle | None = None,
    identity: str = "default",
    deadline_epoch: float | None = None,
    ledger_unavailable: bool = False,
) -> Any:
    return coordinator.prepare_thread_set(
        thread_key=THREAD_KEY,
        agent_id=AGENT_ID,
        ledger_refs=list(ledger_refs),
        current=list(current),
        identity=identity,
        handle=handle,
        routes=list(routes),
        deadline_epoch=deadline_epoch,
        ledger_unavailable=ledger_unavailable,
    )


def _ref_entries(env: dict[str, str]) -> list[dict[str, Any]]:
    value = env[REF_ENV]
    return json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))


def _manifest(env: dict[str, str]) -> dict[str, Any]:
    return json.loads(env[MANIFEST_ENV])


def _unavailable(prepared: Any) -> dict[str, str]:
    return {item.name: item.reason for item in prepared.unavailable}


def _owners(store: RetainingObjectStore) -> dict[str, dict[str, Any]]:
    return {
        key: json.loads(value)
        for key, value in store.objects.items()
        if key.startswith("_attachments/")
    }


def _attachment_objects(store: RetainingObjectStore) -> set[str]:
    return {key for key in store.objects if key.startswith("attachments/")}


class _Transport:
    """A channel adapter's HTTP answer per URL: ``(status, body)`` or an exception."""

    def __init__(self, attachments: Any, answers: dict[str, Any]) -> None:
        self._attachments = attachments
        self.answers = answers
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        chunk_bytes: int,
    ) -> Any:
        self.calls.append({"method": method, "url": url, "headers": dict(headers)})
        answer = self.answers[url]
        if isinstance(answer, BaseException):
            raise answer
        status, body = answer
        return self._attachments.SlackFileResponse(
            status=status, headers={}, chunks=iter([body])
        )


def _channel_files(
    attachments: Any, transport: _Transport, *, credentials: dict[str, str] | None = None
) -> Any:
    return attachments.ChannelPortFileClient(
        credentials={ADAPTER: ADAPTER_SECRET} if credentials is None else credentials,
        transport=transport,
    )


def _stable_line_decode(attachments: Any, payload: bytes) -> None:
    """The v1 owner-record decoder as it ships before ADR 0205, frozen here.

    A worker still on the stable line shares the bucket during a rolling
    upgrade and its reap tick decodes every record under ``_attachments/``;
    a record it cannot decode fails that tick. So a record written by the new
    lane must still pass exactly these checks.
    """

    raw = json.loads(payload)
    assert raw.get("version") == 1
    thread_key = str(raw["thread_key"])
    object_keys = tuple(str(key) for key in raw["object_keys"])
    attachments.decode_attachment_refs(str(raw["refs"]))
    int(raw["expires_at_epoch"])
    assert thread_key and object_keys
    assert not any(not key or key.startswith("_") for key in object_keys)


# --- naming: fixed at record time, never recomputed ------------------------------


@pytest.mark.parametrize(
    ("raw", "clean"),
    [
        ("report.pdf", "report.pdf"),
        ("  report.pdf  ", "report.pdf"),
        ("dir/sub/report.pdf", "report.pdf"),
        ("C:\\Users\\me\\report.pdf", "report.pdf"),
        ("../../etc/passwd", "passwd"),
        (".bashrc", "_bashrc"),
        (".curie-attachments-status.json", "_curie-attachments-status.json"),
        ("dir/.hidden", "_hidden"),
        ("...", "_.."),
    ],
)
def test_clean_attachment_leaf_ports_the_init_rules_and_unhides_dot_names(
    attachments: Any, raw: str, clean: str
) -> None:
    """A leading dot would hide the file from the runner's discovery and could
    collide with the init container's own ``.curie-*`` status files."""

    assert attachments.clean_attachment_leaf(raw) == clean


@pytest.mark.parametrize("raw", ["", "   ", ".", "..", "dir/", "a/..", "/", "\\"])
def test_clean_attachment_leaf_refuses_a_name_that_cleans_to_nothing(
    attachments: Any, raw: str
) -> None:
    with pytest.raises(attachments.AttachmentResolutionError) as refusal:
        attachments.clean_attachment_leaf(raw)
    assert refusal.value.stage == "name"


def test_assign_disk_names_never_reuses_a_taken_name_or_one_given_earlier(
    attachments: Any,
) -> None:
    assigned = attachments.assign_disk_names(
        ["report.pdf", "dir/report.pdf", ".env", "report-2.pdf"],
        taken={"report.pdf"},
    )

    assert assigned == ("report-2.pdf", "report-3.pdf", "_env", "report-2-2.pdf")


def test_current_files_are_named_against_every_ledger_name_and_each_other(
    attachments: Any, ledger: Any
) -> None:
    """Decision 4: names are disambiguated against EVERY ledger name, including
    a file this boot cannot deliver, so no name is ever reused."""

    files = FakeSlackFiles(
        {"C1": [b"one"], "C2": [b"two"], "C3": [b"three"], "C4": [b"four"]}
    )
    coordinator, _store, _clock = _coordinator(attachments, files=files)
    held = [
        _earlier(ledger, "E1", b"old-1", disk_name="report.pdf"),
        _earlier(ledger, "E2", b"old-2", disk_name="report-2.pdf", ordinal=1),
    ]

    prepared = _prepare(
        coordinator,
        ledger_refs=held,
        routes=(),  # no route: both earlier files are unavailable, names still held
        current=[
            Attachment(id="C1", name="report.pdf"),
            Attachment(id="C2", name="report.pdf"),
            Attachment(id="C3", name=".env"),
            Attachment(id="C4", name="a\\b\\notes.txt"),
        ],
    )

    current = [entry.disk_name for entry in prepared.entries if entry.current]
    assert current == ["report-3.pdf", "report-4.pdf", "_env", "notes.txt"]
    assert [ref.disk_name for ref in prepared.append_refs] == current
    assert _unavailable(prepared) == {"report.pdf": "no_route", "report-2.pdf": "no_route"}


def test_a_name_held_by_an_omitted_earlier_file_is_never_reused(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"C1": [b"new"]})
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, max_files=1, thread_max_files=1
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[_earlier(ledger, "E1", b"old", disk_name="data.csv")],
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="data.csv")],
    )

    assert prepared.omitted == ("data.csv",)
    assert [entry.disk_name for entry in prepared.entries] == ["data-2.csv"]


def test_a_redelivered_current_file_keeps_its_recorded_name_and_appears_once(
    attachments: Any, ledger: Any
) -> None:
    """A retried turn finds its own files already in the ledger. They are still
    this message's files, under the name already recorded, exactly once."""

    files = FakeSlackFiles({"C1": [b"same"]})
    coordinator, _store, _clock = _coordinator(attachments, files=files)
    first = _prepare(coordinator, current=[Attachment(id="C1", name="report.pdf")])

    again = _prepare(
        coordinator,
        ledger_refs=first.append_refs,
        current=[Attachment(id="C1", name="report.pdf")],
    )

    assert [(entry.disk_name, entry.current) for entry in again.entries] == [
        ("report.pdf", True)
    ]
    assert [ref.disk_name for ref in again.append_refs] == ["report.pdf"]
    assert [entry["n"] for entry in _ref_entries(again.claim_env())] == ["report.pdf"]


# --- what is recorded for the current message --------------------------------------


def test_append_refs_record_the_current_files_route_digest_and_order(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"C1": [b"%PDF-1.7"], "C2": [b"a,b\n"]})
    coordinator, _store, _clock = _coordinator(attachments, files=files)

    prepared = _prepare(
        coordinator,
        current=[
            Attachment(id="C1", name="report.pdf", mime_type="application/pdf"),
            Attachment(id="C2", name="table.csv"),
        ],
        handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1"),
    )

    assert prepared.append_refs == (
        ledger.ThreadAttachmentRef(
            file_id="C1",
            ordinal=0,
            name="report.pdf",
            disk_name="report.pdf",
            mime_type="application/pdf",
            size_bytes=len(b"%PDF-1.7"),
            sha256=_sha(b"%PDF-1.7"),
            route_kind="slack",
            route_adapter=None,
            route_identity="default",
        ),
        ledger.ThreadAttachmentRef(
            file_id="C2",
            ordinal=1,
            name="table.csv",
            disk_name="table.csv",
            mime_type=None,
            size_bytes=len(b"a,b\n"),
            sha256=_sha(b"a,b\n"),
            route_kind="slack",
            route_adapter=None,
            route_identity="default",
        ),
    )


def test_a_channel_port_files_ref_records_its_kind_and_adapter_never_its_endpoint(
    attachments: Any, ledger: Any
) -> None:
    transport = _Transport(
        attachments, {f"{ADAPTER_ENDPOINT}/attachments/M1": (200, b"mail-bytes")}
    )
    coordinator, _store, _clock = _coordinator(
        attachments, channel_files=_channel_files(attachments, transport)
    )

    prepared = _prepare(
        coordinator, current=[Attachment(id="M1", name="invoice.pdf")], handle=EMAIL_HANDLE
    )

    (ref,) = prepared.append_refs
    assert (ref.route_kind, ref.route_adapter, ref.route_identity) == (
        "email",
        ADAPTER,
        "default",
    )
    assert ADAPTER_ENDPOINT not in json.dumps(ref.to_wire())


# --- retention: still version 1, readable both ways -------------------------------


def test_a_new_owner_record_stays_version_1_and_the_stable_line_still_decodes_it(
    attachments: Any,
) -> None:
    files = FakeSlackFiles({"C1": [b"bytes"]})
    coordinator, store, _clock = _coordinator(attachments, files=files)

    prepared = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])

    owners = _owners(store)
    assert len(owners) == 1
    (key, raw) = next(iter(owners.items()))
    digest = hashlib.sha256(THREAD_KEY.encode()).hexdigest()
    assert key.startswith(f"_attachments/{digest}/")
    assert raw["version"] == 1
    assert raw["agent"] == AGENT_ID
    (entry,) = prepared.entries
    assert raw["shas"] == {entry.object_key: _sha(b"bytes")}
    _stable_line_decode(attachments, store.objects[key])


def test_an_old_owner_record_without_the_new_fields_still_decodes(attachments: Any) -> None:
    clock = MovableClock()
    ref = attachments.AttachmentRef(
        name="a.txt",
        url="https://objects.example.com/attachments/x/g/000.bin",
        sha256=_sha(b"old"),
        size_bytes=3,
        expires_at_epoch=int(clock()) + 300,
    )
    payload = json.dumps(
        {
            "version": 1,
            "thread_key": THREAD_KEY,
            "expires_at_epoch": int(clock()) + 3600,
            "object_keys": [f"attachments/{AGENT_ID}/g/000.bin"],
            "refs": attachments.encode_attachment_refs([ref]),
        }
    ).encode()

    record = attachments._AttachmentSet.decode(payload)  # noqa: SLF001

    assert record.agent_id is None
    assert dict(record.shas) == {}
    assert record.object_keys == (f"attachments/{AGENT_ID}/g/000.bin",)


def test_a_new_record_round_trips_through_the_new_decoder(attachments: Any) -> None:
    files = FakeSlackFiles({"C1": [b"bytes"]})
    coordinator, store, _clock = _coordinator(attachments, files=files)
    prepared = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])

    (key,) = _owners(store)
    record = attachments._AttachmentSet.decode(store.objects[key])  # noqa: SLF001

    assert record.agent_id == AGENT_ID
    assert dict(record.shas) == {prepared.entries[0].object_key: _sha(b"bytes")}
    assert record.thread_key == THREAD_KEY


# --- the parked cache -----------------------------------------------------------


def test_a_parked_earlier_file_is_reminted_without_a_fetch_and_gets_a_new_owner(
    attachments: Any,
) -> None:
    files = FakeSlackFiles({"C1": [b"parked"]})
    coordinator, store, clock = _coordinator(attachments, files=files)
    first = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])
    owners_before = set(_owners(store))
    clock.advance(600)

    later = _prepare(coordinator, ledger_refs=first.append_refs, routes=())

    assert files.requested == ["C1"], "a parked file must not be fetched again"
    (entry,) = later.entries
    assert entry.current is False
    assert entry.object_key == first.entries[0].object_key
    new_owners = {key: raw for key, raw in _owners(store).items() if key not in owners_before}
    assert len(new_owners) == 1, "reusing a key writes a new owner record for it"
    (raw,) = new_owners.values()
    assert raw["object_keys"] == [entry.object_key]
    assert raw["expires_at_epoch"] == int(clock()) + 3600
    assert raw["version"] == 1


def test_reaping_the_original_owner_keeps_a_reused_key(attachments: Any) -> None:
    files = FakeSlackFiles({"C1": [b"parked"]})
    coordinator, store, clock = _coordinator(attachments, files=files)
    first = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])
    key = first.entries[0].object_key
    clock.advance(2000)
    _prepare(coordinator, ledger_refs=first.append_refs)
    clock.advance(2000)  # the first owner has lapsed, the reuse owner has not

    assert coordinator.enumerate_expired() == [THREAD_KEY]
    candidate = coordinator.begin_expired_reap(THREAD_KEY)
    assert candidate is not None
    coordinator.finish_expired_reap(candidate)

    assert key in store.objects, "a key a live owner names is never reaped"
    assert key not in store.deleted


def test_a_parked_copy_that_would_lapse_before_redemption_is_fetched_again(
    attachments: Any,
) -> None:
    files = FakeSlackFiles({"C1": [b"parked"]})
    coordinator, _store, clock = _coordinator(attachments, files=files)
    first = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])
    clock.advance(3600 - 10)  # ten seconds of retention left, capability is 300

    later = _prepare(
        coordinator, ledger_refs=first.append_refs, routes=[_slack_route(attachments)]
    )

    assert files.requested == ["C1", "C1"]
    (entry,) = later.entries
    assert entry.object_key != first.entries[0].object_key


def test_discarding_a_thread_set_removes_only_what_it_wrote(attachments: Any) -> None:
    files = FakeSlackFiles({"C1": [b"parked"], "C2": [b"fresh"]})
    coordinator, store, clock = _coordinator(attachments, files=files)
    first = _prepare(coordinator, current=[Attachment(id="C1", name="a.txt")])
    reused_key = first.entries[0].object_key
    owners_before = set(_owners(store))
    clock.advance(60)

    later = _prepare(
        coordinator,
        ledger_refs=first.append_refs,
        current=[Attachment(id="C2", name="b.txt")],
    )
    fresh_key = next(entry.object_key for entry in later.entries if entry.current)

    coordinator.discard_prepared(thread_key=THREAD_KEY, prepared=later)

    assert reused_key in store.objects, "the earlier turn's owner still names it"
    assert fresh_key not in store.objects
    assert set(_owners(store)) == owners_before


# --- re-fetch through the agent's current bindings --------------------------------


def test_a_lapsed_slack_file_is_fetched_with_its_recorded_identitys_current_token(
    attachments: Any, ledger: Any
) -> None:
    default = FakeSlackFiles()
    ops = FakeSlackFiles({"B1": [b"ops-bytes"]})
    coordinator, store, _clock = _coordinator(
        attachments, files=default, identity_files={"ops": ops}
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[_earlier(ledger, "B1", b"ops-bytes", adapter="ops", identity="ops")],
        routes=[_slack_route(attachments), _slack_route(attachments, "ops")],
    )

    assert ops.requested == ["B1"]
    assert default.requested == []
    (entry,) = prepared.entries
    assert store.objects[entry.object_key] == b"ops-bytes"
    assert prepared.unavailable == ()


def test_a_lapsed_channel_port_file_is_fetched_from_the_routes_current_endpoint(
    attachments: Any, ledger: Any
) -> None:
    moved = "http://moved-adapter.example:9090/curie"
    transport = _Transport(attachments, {f"{moved}/attachments/M1": (200, b"mail")})
    coordinator, _store, _clock = _coordinator(
        attachments, channel_files=_channel_files(attachments, transport)
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[_earlier(ledger, "M1", b"mail", kind="email", adapter=ADAPTER)],
        routes=[_email_route(attachments, endpoint=f"{moved}/")],
    )

    assert [call["url"] for call in transport.calls] == [f"{moved}/attachments/M1"]
    assert transport.calls[0]["headers"]["X-Curie-Adapter-Secret"] == ADAPTER_SECRET
    assert [entry.disk_name for entry in prepared.entries] == ["m1.bin"]


def test_bind_route_refuses_before_any_request(attachments: Any) -> None:
    transport = _Transport(attachments, {})
    client = _channel_files(attachments, transport)

    with pytest.raises(attachments.AttachmentResolutionError) as no_endpoint:
        client.bind_route(ADAPTER, None)
    with pytest.raises(attachments.AttachmentResolutionError) as no_secret:
        client.bind_route("other-adapter", ADAPTER_ENDPOINT)

    assert no_endpoint.value.stage == "wiring"
    assert no_secret.value.stage == "credential"
    assert transport.calls == []


def _case_slack_identity_route_gone(attachments: Any, ledger: Any) -> dict[str, Any]:
    return {
        "ref": _earlier(ledger, "BAD", b"x", adapter="ops", identity="ops"),
        "identity_files": {"ops": FakeSlackFiles({"BAD": [b"x"]})},
        "reason": "no_route",
    }


def _case_slack_identity_token_removed(attachments: Any, ledger: Any) -> dict[str, Any]:
    return {
        "ref": _earlier(ledger, "BAD", b"x", adapter="ops", identity="ops"),
        "routes": [_slack_route(attachments, "ops")],
        "reason": "no_credential",
    }


def _case_channel_port_route_gone(attachments: Any, ledger: Any) -> dict[str, Any]:
    transport = _Transport(attachments, {})
    return {
        "ref": _earlier(ledger, "BAD", b"x", kind="email", adapter=ADAPTER),
        "channel_files": _channel_files(attachments, transport),
        "reason": "no_route",
    }


def _case_channel_port_secret_removed(attachments: Any, ledger: Any) -> dict[str, Any]:
    transport = _Transport(attachments, {})
    return {
        "ref": _earlier(ledger, "BAD", b"x", kind="email", adapter=ADAPTER),
        "routes": [_email_route(attachments)],
        "channel_files": _channel_files(attachments, transport, credentials={"other": "s"}),
        "reason": "no_credential",
    }


def _adapter_case(attachments: Any, ledger: Any, answer: Any, reason: str) -> dict[str, Any]:
    transport = _Transport(attachments, {f"{ADAPTER_ENDPOINT}/attachments/BAD": answer})
    return {
        "ref": _earlier(ledger, "BAD", b"x", kind="email", adapter=ADAPTER),
        "routes": [_email_route(attachments)],
        "channel_files": _channel_files(attachments, transport),
        "reason": reason,
    }


def _case_adapter_404(attachments: Any, ledger: Any) -> dict[str, Any]:
    return _adapter_case(attachments, ledger, (404, b"gone"), "not_found")


def _case_adapter_429(attachments: Any, ledger: Any) -> dict[str, Any]:
    return _adapter_case(attachments, ledger, (429, b"slow down"), "rate_limited")


def _case_adapter_timeout(attachments: Any, ledger: Any) -> dict[str, Any]:
    return _adapter_case(attachments, ledger, TimeoutError("read timed out"), "timeout")


def _case_slack_timeout(attachments: Any, ledger: Any) -> dict[str, Any]:
    return {
        "ref": _earlier(ledger, "BAD", b"x"),
        "slack_failures": {"BAD": TimeoutError("read timed out")},
        "reason": "timeout",
    }


def _case_slack_refused(attachments: Any, ledger: Any) -> dict[str, Any]:
    return {
        "ref": _earlier(ledger, "BAD", b"x"),
        "slack_failures": {"BAD": attachments.SlackFileError("file_not_found")},
        "reason": "fetch_failed",
    }


def _case_digest_changed(attachments: Any, ledger: Any) -> dict[str, Any]:
    return {
        "ref": _earlier(ledger, "BAD", b"x", sha256=_sha(b"what was parked")),
        "slack_payloads": {"BAD": [b"x"]},
        "reason": "digest_changed",
    }


@pytest.mark.parametrize(
    "case",
    [
        _case_slack_identity_route_gone,
        _case_slack_identity_token_removed,
        _case_channel_port_route_gone,
        _case_channel_port_secret_removed,
        _case_adapter_404,
        _case_adapter_429,
        _case_adapter_timeout,
        _case_slack_timeout,
        _case_slack_refused,
        _case_digest_changed,
    ],
    ids=lambda case: case.__name__.removeprefix("_case_"),
)
def test_an_earlier_file_that_cannot_be_had_is_unavailable_and_never_fails_the_boot(
    attachments: Any, ledger: Any, case: Any
) -> None:
    """Decision 6: an earlier file is best effort, named, and leaves no bytes."""

    spec = case(attachments, ledger)
    default = FakeSlackFiles(
        {"OLDER": [b"older-ok"], "NEWER": [b"newer-ok"], **spec.get("slack_payloads", {})}
    )
    default.failures.update(spec.get("slack_failures", {}))
    coordinator, store, _clock = _coordinator(
        attachments,
        files=default,
        identity_files=spec.get("identity_files"),
        channel_files=spec.get("channel_files"),
    )
    failing = spec["ref"]
    held = [
        _earlier(ledger, "OLDER", b"older-ok", disk_name="older.txt"),
        failing,
        _earlier(ledger, "NEWER", b"newer-ok", disk_name="newer.txt", ordinal=1),
    ]

    prepared = _prepare(
        coordinator,
        ledger_refs=held,
        routes=[_slack_route(attachments), *spec.get("routes", [])],
    )

    assert _unavailable(prepared) == {failing.disk_name: spec["reason"]}
    assert [entry.disk_name for entry in prepared.entries] == ["older.txt", "newer.txt"]
    assert _attachment_objects(store) == {entry.object_key for entry in prepared.entries}, (
        "an unavailable file leaves no parked bytes behind"
    )
    manifest = _manifest(prepared.claim_env())
    assert manifest["unavailable"] == [{"name": failing.disk_name, "reason": spec["reason"]}]


def test_a_spent_deadline_makes_every_earlier_file_unavailable_without_a_fetch(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"E1": [b"one"], "E2": [b"two"]})
    coordinator, _store, clock = _coordinator(attachments, files=files)

    prepared = _prepare(
        coordinator,
        ledger_refs=[
            _earlier(ledger, "E1", b"one", disk_name="one.txt"),
            _earlier(ledger, "E2", b"two", disk_name="two.txt", ordinal=1),
        ],
        routes=[_slack_route(attachments)],
        deadline_epoch=clock() - 1,
    )

    assert files.requested == []
    assert prepared.entries == ()
    assert _unavailable(prepared) == {"one.txt": "deadline", "two.txt": "deadline"}


class _SlowSlackFiles(FakeSlackFiles):
    """Each fetch takes ``seconds`` of the coordinator's clock."""

    def __init__(
        self, clock: MovableClock, seconds: float, payloads: dict[str, list[bytes]]
    ) -> None:
        super().__init__(payloads)
        self._clock = clock
        self._seconds = seconds

    def fetch(self, file_id: str) -> Iterator[bytes]:
        self._clock.advance(self._seconds)
        yield from super().fetch(file_id)


def test_earlier_files_are_fetched_newest_first_until_the_deadline(
    attachments: Any, ledger: Any
) -> None:
    clock = MovableClock()
    files = _SlowSlackFiles(clock, 50, {"OLD": [b"old"], "NEW": [b"new"]})
    coordinator, _store, _clock = _coordinator(attachments, files=files, clock=clock)

    prepared = _prepare(
        coordinator,
        ledger_refs=[
            _earlier(ledger, "OLD", b"old", disk_name="old.txt"),
            _earlier(ledger, "NEW", b"new", disk_name="new.txt", ordinal=1),
        ],
        routes=[_slack_route(attachments)],
        deadline_epoch=clock() + 30,
    )

    assert files.requested[0] == "NEW"
    assert "OLD" not in files.requested
    assert _unavailable(prepared)["old.txt"] == "deadline"


# --- the carrying message stays all or nothing -------------------------------------


def test_a_current_file_failure_refuses_the_set_and_leaves_nothing(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"C1": [b"ok"], "C2": [b"never"], "E1": [b"earlier"]})
    files.failures["C2"] = attachments.SlackFileError("file_not_found")
    coordinator, store, _clock = _coordinator(attachments, files=files)

    with pytest.raises(attachments.AttachmentResolutionError):
        _prepare(
            coordinator,
            ledger_refs=[_earlier(ledger, "E1", b"earlier", disk_name="earlier.txt")],
            routes=[_slack_route(attachments)],
            current=[Attachment(id="C1", name="a.txt"), Attachment(id="C2", name="b.txt")],
        )

    assert "E1" not in files.requested, "the current message is fetched first"
    assert _attachment_objects(store) == set()
    assert _owners(store) == {}


# --- the per-thread budget -----------------------------------------------------------


def test_the_file_budget_keeps_current_then_the_newest_earlier_files(
    attachments: Any, ledger: Any
) -> None:
    payloads = {f"E{index}": [f"e{index}".encode()] for index in range(1, 6)}
    files = FakeSlackFiles({**payloads, "C1": [b"current"]})
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, max_files=2, thread_max_files=3
    )
    held = [
        _earlier(ledger, f"E{index}", f"e{index}".encode(), disk_name=f"e{index}.txt")
        for index in range(1, 6)
    ]

    prepared = _prepare(
        coordinator,
        ledger_refs=held,
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="c.txt")],
    )

    assert [(entry.disk_name, entry.current) for entry in prepared.entries] == [
        ("e4.txt", False),
        ("e5.txt", False),
        ("c.txt", True),
    ]
    assert sorted(prepared.omitted) == ["e1.txt", "e2.txt", "e3.txt"]
    assert files.requested == ["C1", "E5", "E4"], "current first, then newest first"


def test_the_byte_budget_counts_recorded_sizes_newest_first(
    attachments: Any, ledger: Any
) -> None:
    sizes = {"E1": 10, "E2": 40, "E3": 30}
    payloads = {file_id: [b"x" * size] for file_id, size in sizes.items()}
    files = FakeSlackFiles({**payloads, "C1": [b"c" * 30]})
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, thread_max_bytes=100
    )
    held = [
        _earlier(ledger, file_id, b"x" * size, disk_name=f"{file_id.lower()}.bin")
        for file_id, size in sizes.items()
    ]

    prepared = _prepare(
        coordinator,
        ledger_refs=held,
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="c.bin")],
    )

    assert [entry.disk_name for entry in prepared.entries] == ["e2.bin", "e3.bin", "c.bin"]
    assert prepared.omitted == ("e1.bin",)
    assert "E1" not in files.requested, "an omitted file is never fetched"


def test_a_current_message_over_the_budget_alone_is_still_delivered_whole(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"C1": [b"c" * 120], "E1": [b"e"]})
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, thread_max_bytes=100
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[_earlier(ledger, "E1", b"e", disk_name="e.bin")],
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="c.bin")],
    )

    assert [entry.disk_name for entry in prepared.entries] == ["c.bin"]
    assert prepared.omitted == ("e.bin",)


# --- capabilities are signed last ---------------------------------------------------


class _OrderedStore(RetainingObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[str, str]] = []

    def put_stream(self, key: str, chunks: Any) -> None:
        super().put_stream(key, chunks)
        self.events.append(("put", key))

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        self.events.append(("sign", key))
        return super().presign_get(key, expires_seconds=expires_seconds)


def test_capabilities_are_presigned_only_after_every_fetch_finishes(
    attachments: Any, ledger: Any
) -> None:
    """A slow fetch must not age out a capability minted before it."""

    clock = MovableClock()
    store = _OrderedStore()
    files = _SlowSlackFiles(
        clock, 100, {"C1": [b"current"], "E1": [b"one"], "E2": [b"two"]}
    )
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, store=store, clock=clock
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[
            _earlier(ledger, "E1", b"one", disk_name="one.txt"),
            _earlier(ledger, "E2", b"two", disk_name="two.txt", ordinal=1),
        ],
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="c.txt")],
    )

    puts = [
        index
        for index, (kind, key) in enumerate(store.events)
        if kind == "put" and key.startswith("attachments/")
    ]
    signs = [index for index, (kind, _key) in enumerate(store.events) if kind == "sign"]
    assert len(signs) == 3
    assert min(signs) > max(puts), store.events
    entries = _ref_entries(prepared.claim_env())
    assert {entry["e"] for entry in entries} == {int(clock()) + 300}


# --- the claim env ------------------------------------------------------------------


def test_a_thread_with_no_files_contributes_nothing_and_touches_nothing(
    attachments: Any,
) -> None:
    files = FakeSlackFiles()
    coordinator, store, _clock = _coordinator(attachments, files=files)

    prepared = _prepare(coordinator)

    assert prepared.claim_env() == {}
    assert store.objects == {}
    assert files.requested == []


def test_a_failed_ledger_read_on_a_text_turn_contributes_the_manifest_alone(
    attachments: Any,
) -> None:
    coordinator, store, _clock = _coordinator(attachments, files=FakeSlackFiles())

    prepared = _prepare(coordinator, ledger_unavailable=True)

    env = prepared.claim_env()
    assert set(env) == {MANIFEST_ENV}
    assert _manifest(env) == {
        "v": 1,
        "files": [],
        "unavailable": [],
        "omitted": [],
        "ledger_unavailable": True,
    }
    assert store.objects == {}


def test_the_claim_env_names_every_file_exactly_and_marks_the_current_ones(
    attachments: Any, ledger: Any
) -> None:
    files = FakeSlackFiles({"E1": [b"one"], "C1": [b"current"]})
    coordinator, _store, _clock = _coordinator(
        attachments, files=files, max_files=2, thread_max_files=2
    )

    prepared = _prepare(
        coordinator,
        ledger_refs=[
            _earlier(ledger, "E0", b"zero", disk_name="zero.txt"),
            _earlier(ledger, "GONE", b"g", disk_name="gone.txt", kind="email", adapter="x"),
            _earlier(ledger, "E1", b"one", disk_name="one.txt", ordinal=1),
        ],
        routes=[_slack_route(attachments)],
        current=[Attachment(id="C1", name="one.txt")],
    )

    env = prepared.claim_env()
    assert [(entry["n"], entry["c"]) for entry in _ref_entries(env)] == [
        ("one.txt", 0),
        ("one-2.txt", 1),
    ]
    assert _manifest(env) == {
        "v": 1,
        "files": [
            {"name": "one.txt", "current": False},
            {"name": "one-2.txt", "current": True},
        ],
        "unavailable": [],
        "omitted": ["zero.txt", "gone.txt"],
        "ledger_unavailable": False,
    }
    assert "https://" not in env[MANIFEST_ENV], "the manifest carries no URL"


def test_only_unavailable_earlier_files_contribute_the_manifest_alone(
    attachments: Any, ledger: Any
) -> None:
    coordinator, _store, _clock = _coordinator(attachments, files=FakeSlackFiles())

    prepared = _prepare(
        coordinator,
        ledger_refs=[_earlier(ledger, "E1", b"one", disk_name="one.txt")],
        routes=(),
    )

    env = prepared.claim_env()
    assert set(env) == {MANIFEST_ENV}
    assert _manifest(env)["unavailable"] == [{"name": "one.txt", "reason": "no_route"}]


# --- the limits ----------------------------------------------------------------------


def test_the_thread_budget_defaults(attachments: Any) -> None:
    bounds = attachments.AttachmentLimits()

    assert bounds.thread_max_files == 20
    assert bounds.thread_max_bytes == 256 * 1024 * 1024


@pytest.mark.parametrize(
    "overrides",
    [
        {"thread_max_files": 0},
        {"thread_max_bytes": 0},
        {"max_files": 10, "thread_max_files": 9},
    ],
)
def test_a_thread_budget_below_one_message_is_refused(
    attachments: Any, overrides: dict[str, int]
) -> None:
    with pytest.raises(ValueError):
        attachments.AttachmentLimits(**overrides)
