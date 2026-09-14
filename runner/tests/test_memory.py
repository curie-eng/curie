"""The memory port: resolution, record shape, preamble, and the state-API store.

The StateApiMemoryStore is exercised against a tiny in-memory fake of the #248
log-shaped state endpoints (GET the key, POST .../append), so load/append and the
provenance round-trip are verified end-to-end over real HTTP without the API.
"""

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.memory import (
    ConsolidationResult,
    MemoryError,
    MemoryRecord,
    NullMemoryStore,
    Provenance,
    StateApiMemoryStore,
    consolidate_memory,
    consolidate_records,
    format_memory_preamble,
    merge_provenance,
    resolve_memory,
)


def _fake_state_app() -> tuple[web.Application, list]:
    """A minimal fake of the state API's log key at /agents/A/state/memory/log."""
    log: list = []
    app = web.Application()

    async def get_log(request: web.Request) -> web.Response:
        if not log:
            return web.json_response({"detail": "not found"}, status=404)
        return web.json_response(
            {"namespace": "memory", "key": "log", "value": list(log), "version": len(log)}
        )

    async def append_log(request: web.Request) -> web.Response:
        body = await request.json()
        log.append(body["item"])
        return web.json_response(
            {"namespace": "memory", "key": "log", "value": list(log), "version": len(log)}
        )

    app.router.add_get("/agents/A/state/memory/log", get_log)
    app.router.add_post("/agents/A/state/memory/log/append", append_log)
    return app, log


def test_record_and_provenance_round_trip() -> None:
    rec = MemoryRecord(
        content="prefer ruff over flake8",
        provenance=Provenance(
            learned_from_session_id="sess-1",
            source_trace_ids=("trace-a", "trace-b"),
            recorded_at="2026-07-13T00:00:00+00:00",
        ),
    )
    restored = MemoryRecord.from_dict(rec.to_dict())
    assert restored == rec


def test_resolve_absent_ref_is_null_store() -> None:
    store = resolve_memory(None, {})
    assert isinstance(store, NullMemoryStore)
    assert anyio.run(store.load) == []
    # Append on the null store is a silent no-op.
    anyio.run(store.append, MemoryRecord(content="x"))


def test_resolve_http_ref_is_state_store() -> None:
    store = resolve_memory("http://api:8000/agents/A/state/memory", {})
    assert isinstance(store, StateApiMemoryStore)


def test_resolve_unsupported_scheme_raises() -> None:
    with pytest.raises(MemoryError):
        resolve_memory("s3://bucket/mem", {})


def test_preamble_empty_is_none() -> None:
    assert format_memory_preamble([]) is None


def test_operator_provenance_survives_round_trip_and_merge() -> None:
    record = MemoryRecord.from_dict(
        {
            "content": "ask before French",
            "provenance": {
                "learned_from_session_id": None,
                "source_trace_ids": [],
                "recorded_at": "2026-08-27T00:00:00+00:00",
                "source": "operator",
            },
        }
    )
    assert record.provenance.source == "operator"
    assert record.to_dict()["provenance"]["source"] == "operator"
    merged = merge_provenance(
        record.provenance,
        Provenance(learned_from_session_id="sess-1", source_trace_ids=("t1",)),
    )
    assert merged.source == "operator"
    assert merged.source_trace_ids == ("t1",)


def test_preamble_includes_content_and_traces() -> None:
    records = [
        MemoryRecord(
            content="deploy is a git push",
            provenance=Provenance(source_trace_ids=("t1",)),
        ),
        MemoryRecord(content="no-trace lesson"),
    ]
    preamble = format_memory_preamble(records)
    assert preamble is not None
    assert preamble.startswith("# Agent memory (learned from prior sessions)")
    assert "provided by the operator" not in preamble
    assert "deploy is a git push" in preamble
    assert "t1" in preamble
    assert "no-trace lesson" in preamble


def test_preamble_operator_only_uses_operator_heading() -> None:
    records = [
        MemoryRecord(
            content="ask before French",
            provenance=Provenance(source="operator"),
        )
    ]
    preamble = format_memory_preamble(records)
    assert preamble is not None
    assert preamble.startswith("# Agent memory (provided by the operator)")
    assert "ask before French" in preamble
    assert "learned from prior sessions" not in preamble


def test_preamble_mixed_operator_then_learned() -> None:
    records = [
        MemoryRecord(content="learned first"),
        MemoryRecord(
            content="operator first",
            provenance=Provenance(source="operator"),
        ),
        MemoryRecord(content="learned second"),
        MemoryRecord(
            content="operator second",
            provenance=Provenance(source="operator"),
        ),
    ]
    preamble = format_memory_preamble(records)
    assert preamble is not None
    operator_heading = "# Agent memory (provided by the operator)"
    learned_heading = "# Agent memory (learned from prior sessions)"
    assert operator_heading in preamble
    assert learned_heading in preamble
    operator_at = preamble.index(operator_heading)
    learned_at = preamble.index(learned_heading)
    assert operator_at < learned_at
    operator_section = preamble[operator_at:learned_at]
    learned_section = preamble[learned_at:]
    assert "operator first" in operator_section
    assert "operator second" in operator_section
    assert operator_section.index("operator first") < operator_section.index("operator second")
    assert "learned first" not in operator_section
    assert "learned second" not in operator_section
    assert "learned first" in learned_section
    assert "learned second" in learned_section
    assert learned_section.index("learned first") < learned_section.index("learned second")
    assert "operator first" not in learned_section
    assert "operator second" not in learned_section


def test_state_store_load_empty_is_empty() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/memory"))
            store = StateApiMemoryStore(url, token=None)
            assert await store.load() == []

    anyio.run(go)


def test_state_store_append_then_load_round_trip() -> None:
    app, log = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/memory"))
            store = StateApiMemoryStore(url, token="k")
            rec = MemoryRecord(
                content="prod push reuses the dev bundle",
                provenance=Provenance(
                    learned_from_session_id="sess-9",
                    source_trace_ids=("trace-x",),
                    recorded_at="2026-07-13T00:00:00+00:00",
                ),
            )
            await store.append(rec)
            loaded = await store.load()
            assert loaded == [rec]
            # The provenance survived the persist/load round-trip.
            assert loaded[0].provenance.source_trace_ids == ("trace-x",)
            assert len(log) == 1

    anyio.run(go)


def test_session_runner_remember_appends_with_provenance() -> None:
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.fake import FakeModelSession
    from curie_runner.session import SessionRunner

    class _Recording:
        def __init__(self) -> None:
            self.records: list[MemoryRecord] = []

        async def load(self) -> list[MemoryRecord]:
            return list(self.records)

        async def append(self, record: MemoryRecord) -> None:
            self.records.append(record)

    store = _Recording()
    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        session_id="sess-42",
        memory_store=store,
    )

    anyio.run(lambda: runner.remember("lesson", source_trace_ids=("trace-7",)))
    assert len(store.records) == 1
    prov = store.records[0].provenance
    assert store.records[0].content == "lesson"
    assert prov.learned_from_session_id == "sess-42"
    assert prov.source_trace_ids == ("trace-7",)
    assert prov.recorded_at  # a timestamp was stamped


# --- Consolidation pipeline (#265) ---------------------------------------


class _ReplacingStore:
    """A memory store that supports load/append/replace, for consolidation tests."""

    def __init__(self, records: list[MemoryRecord] | None = None) -> None:
        self.records: list[MemoryRecord] = list(records or [])
        self.replaced: list[list[MemoryRecord]] = []

    async def load(self) -> list[MemoryRecord]:
        return list(self.records)

    async def append(self, record: MemoryRecord) -> None:
        self.records.append(record)

    async def replace(self, records) -> None:
        self.records = list(records)
        self.replaced.append(list(records))


def test_merge_provenance_unions_traces_and_keeps_earliest() -> None:
    a = Provenance(
        learned_from_session_id="sess-1",
        source_trace_ids=("t1", "t2"),
        recorded_at="2026-07-13T02:00:00+00:00",
    )
    b = Provenance(
        learned_from_session_id="sess-2",
        source_trace_ids=("t2", "t3"),
        recorded_at="2026-07-13T01:00:00+00:00",
    )
    merged = merge_provenance(a, b)
    assert merged.source_trace_ids == ("t1", "t2", "t3")
    assert merged.learned_from_session_id == "sess-1"
    # Earliest timestamp wins -- points at when the lesson was first learned.
    assert merged.recorded_at == "2026-07-13T01:00:00+00:00"


def test_consolidate_records_dedups_and_preserves_provenance() -> None:
    records = [
        MemoryRecord(
            content="deploy is a git push",
            provenance=Provenance(
                source_trace_ids=("t1",), recorded_at="2026-07-13T00:00:00+00:00"
            ),
        ),
        MemoryRecord(content="unique lesson", provenance=Provenance(source_trace_ids=("t9",))),
        # Near-duplicate: differing case + whitespace collapses to the same key.
        MemoryRecord(
            content="Deploy  is a   git push",
            provenance=Provenance(
                source_trace_ids=("t2",), recorded_at="2026-07-13T05:00:00+00:00"
            ),
        ),
    ]
    out = consolidate_records(records)
    assert len(out) == 2
    first = out[0]
    # First-seen surface form is kept.
    assert first.content == "deploy is a git push"
    # No provenance lost -- both traces survive the merge.
    assert set(first.provenance.source_trace_ids) == {"t1", "t2"}
    assert out[1].content == "unique lesson"


def test_consolidate_records_noop_when_no_duplicates() -> None:
    records = [MemoryRecord(content="a"), MemoryRecord(content="b")]
    assert consolidate_records(records) == records


def test_consolidate_memory_writes_back_when_reduced() -> None:
    store = _ReplacingStore(
        [
            MemoryRecord(content="x", provenance=Provenance(source_trace_ids=("t1",))),
            MemoryRecord(content="x", provenance=Provenance(source_trace_ids=("t2",))),
        ]
    )

    result: ConsolidationResult = anyio.run(consolidate_memory, store)
    assert result.before == 2
    assert result.after == 1
    assert result.removed == 1
    assert result.written is True
    assert len(store.replaced) == 1
    assert set(store.records[0].provenance.source_trace_ids) == {"t1", "t2"}


def test_consolidate_memory_noop_when_nothing_to_remove() -> None:
    store = _ReplacingStore([MemoryRecord(content="a"), MemoryRecord(content="b")])
    result = anyio.run(consolidate_memory, store)
    assert result.written is False
    assert store.replaced == []


def test_consolidate_memory_noop_on_store_without_replace() -> None:
    # NullMemoryStore implements replace as a no-op; a store lacking it entirely
    # must also not be written. Use a load-only store.
    class _ReadOnly:
        async def load(self) -> list[MemoryRecord]:
            return [MemoryRecord(content="a"), MemoryRecord(content="a")]

        async def append(self, record: MemoryRecord) -> None:  # noqa: ARG002
            return None

    result = anyio.run(consolidate_memory, _ReadOnly())
    # Consolidation still computes the compacted set, just cannot persist it.
    assert result.after == 1
    assert result.written is False


def test_null_store_replace_is_noop() -> None:
    store = NullMemoryStore()
    anyio.run(store.replace, [MemoryRecord(content="x")])
    assert anyio.run(store.load) == []


def test_state_store_replace_puts_log() -> None:
    put_bodies: list = []
    app = web.Application()

    async def put_log(request: web.Request) -> web.Response:
        body = await request.json()
        put_bodies.append(body)
        return web.json_response(
            {"namespace": "memory", "key": "log", "value": body["value"], "version": 2}
        )

    app.router.add_put("/agents/A/state/memory/log", put_log)

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/memory"))
            store = StateApiMemoryStore(url, token="k")
            await store.replace(
                [MemoryRecord(content="compacted", provenance=Provenance(source_trace_ids=("t1",)))]
            )

    anyio.run(go)
    assert len(put_bodies) == 1
    assert put_bodies[0]["value"][0]["content"] == "compacted"


def test_session_runner_consolidate_memory() -> None:
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.fake import FakeModelSession
    from curie_runner.session import SessionRunner

    store = _ReplacingStore(
        [MemoryRecord(content="dup"), MemoryRecord(content="dup"), MemoryRecord(content="keep")]
    )
    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        session_id="sess-1",
        memory_store=store,
    )
    result = anyio.run(runner.consolidate_memory)
    assert result.before == 3
    assert result.after == 2
    assert result.written is True


def test_state_store_load_rejects_non_array() -> None:
    app = web.Application()

    async def get_log(_request: web.Request) -> web.Response:
        return web.json_response(
            {"namespace": "memory", "key": "log", "value": {"not": "a list"}, "version": 1}
        )

    app.router.add_get("/agents/A/state/memory/log", get_log)

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/memory"))
            store = StateApiMemoryStore(url, token=None)
            with pytest.raises(MemoryError):
                await store.load()

    anyio.run(go)
