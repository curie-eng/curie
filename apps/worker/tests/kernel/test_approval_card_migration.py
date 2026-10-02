"""The one-shot boot rekey of pre-#1723 approval-card refs (#1751).

#1723 moved the card pointer from a THREAD-keyed Valkey entry to an
APPROVAL-ID-keyed one with no compatibility path, so any approval already
pending when the workers rolled became invisible to the settle path. A resolve
CLICK still heals its card; an EXPIRY has no click, so the card kept live
Approve/Reject buttons for the rest of its 14 day TTL.

These tests drive ``ApprovalCardStore.migrate_legacy_thread_keyed_refs``
against a REAL Valkey through the ordinary kernel harness, because the whole
behaviour is Valkey semantics: an NX write, a compare-and-delete, and a carried
TTL. The negative cases matter at least as much as the positive one -- a
migration that ate a CURRENT-layout entry would strand exactly the cards this
exists to rescue.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from curie_worker.approval_cards import DEFAULT_CARD_TTL_S, ApprovalCardStore
from redis.asyncio import Redis as AsyncRedis


def _legacy_payload(approval_id: str, **overrides: object) -> str:
    """A pre-#1723 thread-keyed payload: the current fields PLUS ``approval_id``.

    That extra field is the discriminator the migration keys off -- the current
    layout carries the approval id in the KEY, so a payload that also states it
    inline can only have come from the thread-keyed generation (#1199).
    """

    payload: dict[str, object] = {
        "channel": "C1",
        "ts": "1723.0001",
        "summary": "Refund order 42",
        "endpoint": "https://slack.example/hooks",
        "requested_by": "U1",
        "kind": "slack",
        "adapter": "slack-secondary",
        "approval_id": approval_id,
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_legacy_thread_keyed_ref_is_rekeyed_onto_its_approval_id(make_harness) -> None:
    """The whole point: after the pass, the ordinary single-keyed read finds it.

    Every field of the destination survives the move (the card is REBUILT from
    this ref, so a dropped ``kind`` or ``adapter`` would post the settled edit
    at the wrong place), the stale discriminator is gone from the stored value,
    the old key is gone, and the remaining TTL is carried rather than reset --
    an approval three days from its SLA must not have its memory resurrected
    for another fortnight.
    """

    async def go() -> None:
        async with make_harness() as h:
            legacy_key = f"{h.config.key_prefix}:approval-card:th-rolled"
            await h.async_redis.set(
                legacy_key, _legacy_payload("appr-legacy-1"), ex=3600
            )

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert (result.scanned, result.migrated, result.skipped) == (1, 1, 0)

            entry = await h.card_store.read("appr-legacy-1")
            assert entry is not None, "the settle path must now find the ref"
            ref, _raw = entry
            assert ref.channel == "C1"
            assert ref.ts == "1723.0001"
            assert ref.summary == "Refund order 42"
            assert ref.endpoint == "https://slack.example/hooks"
            assert ref.requested_by == "U1"
            assert ref.kind == "slack"
            assert ref.adapter == "slack-secondary"

            assert not await h.async_redis.exists(legacy_key)

            new_key = h.config.approval_card_key("appr-legacy-1")
            stored = json.loads(await h.async_redis.get(new_key))
            assert "approval_id" not in stored, (
                "the stale discriminator must not travel to the new key, or the "
                "next pass would treat the migrated entry as legacy again"
            )

            # Close to the hour it was seeded with, nowhere near the 14 day
            # ceiling a fresh remember() would have minted.
            ttl_ms = await h.async_redis.pttl(new_key)
            assert 3_000_000 < ttl_ms <= 3_600_000

    asyncio.run(go())


def test_current_layout_entry_is_left_byte_identical(make_harness) -> None:
    """The critical negative: a LIVE entry the running worker depends on.

    A current-layout payload carries no ``approval_id`` field, so the
    discriminator must decline it outright -- no rewrite, no TTL reset, no
    delete. Byte identity plus an untouched expiry is the assertion, because a
    "harmless" re-serialise would still reset the TTL.
    """

    async def go() -> None:
        async with make_harness() as h:
            await h.card_store.remember(
                "appr-live",
                channel="C2",
                ts="9000.1",
                summary="Ship it",
                endpoint=None,
                requested_by="U7",
                kind="slack",
                adapter=None,
            )
            key = h.config.approval_card_key("appr-live")
            before = await h.async_redis.get(key)
            ttl_before = await h.async_redis.pttl(key)

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert (result.scanned, result.migrated) == (1, 0)

            assert await h.async_redis.get(key) == before
            ttl_after = await h.async_redis.pttl(key)
            # Only the elapsed test time may separate them; a reset would jump.
            assert ttl_after <= ttl_before

    asyncio.run(go())


def test_pre_1199_legacy_entry_is_left_untouched(make_harness) -> None:
    """A thread-keyed entry from BEFORE #1199 never recorded its approval id.

    There is nothing to rekey it onto -- it was never pairable -- so it stays
    where it is and lapses with its TTL. Both shapes are covered: the field
    absent, and the field present but empty (which is not an identity either).
    """

    async def go() -> None:
        async with make_harness() as h:
            absent = f"{h.config.key_prefix}:approval-card:th-pre-1199"
            empty = f"{h.config.key_prefix}:approval-card:th-empty-id"
            no_id = json.dumps(
                {"channel": "C3", "ts": "1.1", "summary": "Old card"}
            )
            blank_id = _legacy_payload("", channel="C4")
            await h.async_redis.set(absent, no_id, ex=3600)
            await h.async_redis.set(empty, blank_id, ex=3600)

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert (result.scanned, result.migrated, result.skipped) == (2, 0, 2)

            assert await h.async_redis.get(absent) == no_id
            assert await h.async_redis.get(empty) == blank_id

    asyncio.run(go())


def test_an_existing_entry_for_the_same_approval_wins_over_the_legacy_ref(
    make_harness,
) -> None:
    """NX: the upgraded worker's own entry is authoritative.

    Both generations can name the same approval -- a new-version replica may
    already have re-posted and remembered its card. The legacy pointer is the
    stale one, so it must not overwrite; and it is dead either way, so the old
    key is still removed rather than left to be re-examined every boot.
    """

    async def go() -> None:
        async with make_harness() as h:
            await h.card_store.remember(
                "appr-both",
                channel="C_NEW",
                ts="new-ts",
                summary="The current card",
                endpoint=None,
            )
            legacy_key = f"{h.config.key_prefix}:approval-card:th-both"
            await h.async_redis.set(
                legacy_key,
                _legacy_payload("appr-both", channel="C_OLD", summary="The stale card"),
                ex=3600,
            )

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            # The current entry is scanned and declined; the legacy one is
            # scanned, loses the NX race, and is therefore not counted migrated.
            assert result.scanned == 2
            assert result.migrated == 0

            entry = await h.card_store.read("appr-both")
            assert entry is not None
            ref, _raw = entry
            assert (ref.channel, ref.summary) == ("C_NEW", "The current card")
            assert not await h.async_redis.exists(legacy_key)

    asyncio.run(go())


def test_a_corrupt_entry_does_not_stop_the_pass(make_harness) -> None:
    """One unparseable key must not abort the rest of the boot pass.

    Anything could be sitting under this prefix -- a half-written value, a
    shape from a future layout. The pass counts it as skipped, leaves it alone,
    and keeps going.
    """

    async def go() -> None:
        async with make_harness() as h:
            garbage = f"{h.config.key_prefix}:approval-card:th-garbage"
            not_a_dict = f"{h.config.key_prefix}:approval-card:th-list"
            legacy_key = f"{h.config.key_prefix}:approval-card:th-good"
            await h.async_redis.set(garbage, "{not json at all", ex=3600)
            await h.async_redis.set(not_a_dict, json.dumps(["nope"]), ex=3600)
            await h.async_redis.set(
                legacy_key, _legacy_payload("appr-after-garbage"), ex=3600
            )

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert result.scanned == 3
            assert result.migrated == 1
            assert result.skipped == 2

            assert await h.card_store.read("appr-after-garbage") is not None
            assert await h.async_redis.get(garbage) == "{not json at all"
            assert await h.async_redis.exists(not_a_dict)

    asyncio.run(go())


def test_the_migration_is_idempotent(make_harness) -> None:
    """A second boot moves nothing: the migrated entry is now current-layout.

    This is what makes an unconditional call at every worker start safe -- once
    the old key space is drained the pass is a no-op that only pays for one
    SCAN.
    """

    async def go() -> None:
        async with make_harness() as h:
            legacy_key = f"{h.config.key_prefix}:approval-card:th-twice"
            await h.async_redis.set(
                legacy_key, _legacy_payload("appr-twice"), ex=3600
            )

            first = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert first.migrated == 1
            new_key = h.config.approval_card_key("appr-twice")
            after_first = await h.async_redis.get(new_key)

            second = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert (second.scanned, second.migrated, second.skipped) == (1, 0, 1)
            assert await h.async_redis.get(new_key) == after_first

    asyncio.run(go())


def test_a_legacy_ref_with_no_expiry_still_migrates_with_the_ceiling(
    make_harness,
) -> None:
    """``PTTL`` -1 is "exists, no expiry", not "gone": it must still move.

    The two negative PTTL replies read alike and mean opposite things. -1 is a
    live key that simply never had a deadline set, so the standard 14 day
    ceiling applies -- the same one ``remember`` uses -- and the entry lands on
    its approval id like any other. Only -2 aborts (below).
    """

    async def go() -> None:
        async with make_harness() as h:
            legacy_key = f"{h.config.key_prefix}:approval-card:th-no-expiry"
            # No ``ex``/``px``: the key is persistent, so PTTL answers -1.
            await h.async_redis.set(legacy_key, _legacy_payload("appr-no-expiry"))
            assert await h.async_redis.pttl(legacy_key) == -1

            result = await h.card_store.migrate_legacy_thread_keyed_refs()
            assert (result.scanned, result.migrated, result.skipped) == (1, 1, 0)

            assert await h.card_store.read("appr-no-expiry") is not None
            assert not await h.async_redis.exists(legacy_key)

            new_key = h.config.approval_card_key("appr-no-expiry")
            ttl_ms = await h.async_redis.pttl(new_key)
            # The ceiling, not "no expiry": a rekeyed entry must never be the one
            # key under this prefix that outlives every sweep.
            assert 13 * 24 * 60 * 60 * 1000 < ttl_ms <= DEFAULT_CARD_TTL_S * 1000

    asyncio.run(go())


class _DeletesSourceOnPttl:
    """The REAL Valkey client, with the GET-then-PTTL window forced open once.

    Everything is delegated to the client the harness built -- the SCAN, the
    GET, the NX write, the compare-and-delete all run against real Valkey -- and
    the ONE interposition is that asking for the victim's PTTL deletes it first,
    so the store observes the -2 ("no such key") reply deterministically instead
    of waiting on a race no test can schedule.
    """

    def __init__(self, redis: AsyncRedis, victim: str) -> None:
        self._redis = redis
        self._victim = victim

    def __getattr__(self, name: str) -> Any:
        return getattr(self._redis, name)

    async def pttl(self, key: str | bytes) -> int:
        name = key.decode() if isinstance(key, bytes) else key
        if name == self._victim:
            await self._redis.delete(name)
        return int(await self._redis.pttl(key))


def test_a_source_that_vanishes_before_the_pttl_is_not_resurrected(
    make_harness,
) -> None:
    """``PTTL`` -2 must abort the entry, not mint a fresh fortnight.

    If the legacy key expires or is consumed between the GET and the PTTL, its
    payload in hand is already stale and the pointer it described has
    legitimately gone away. Treating that -2 as "no expiry recorded" would write
    the target with a brand new 14 day life -- resurrecting a card ref the
    system had finished with. Nothing is written, and nothing is deleted either.
    """

    async def go() -> None:
        async with make_harness() as h:
            legacy_key = f"{h.config.key_prefix}:approval-card:th-vanishes"
            await h.async_redis.set(
                legacy_key, _legacy_payload("appr-vanished"), ex=3600
            )
            racing = ApprovalCardStore(
                _DeletesSourceOnPttl(h.async_redis, legacy_key),  # type: ignore[arg-type]
                h.config,
            )

            result = await racing.migrate_legacy_thread_keyed_refs()
            assert (result.scanned, result.migrated, result.skipped) == (1, 0, 1)

            assert not await h.async_redis.exists(
                h.config.approval_card_key("appr-vanished")
            ), "a ref that had already gone must not come back with a fresh TTL"
            assert await h.card_store.read("appr-vanished") is None

    asyncio.run(go())


def test_a_self_keyed_legacy_ref_survives_a_bytes_client(make_harness) -> None:
    """The destructive miss: an un-normalised key compared against a ``str``.

    A store built on a ``decode_responses=False`` client gets ``bytes`` back from
    the SCAN, while the target it derives is always a ``str``. For a legacy entry
    whose thread key happens to EQUAL its approval id the two describe the same
    key, so the "already where it belongs" guard must fire. Compared
    un-normalised it never does: the NX write loses against the key's own value
    and the compare-and-delete then removes a live pointer -- the exact stranding
    this migration exists to end.
    """

    async def go() -> None:
        async with make_harness() as h:
            # Thread key == approval id: the one shape where source and target
            # are the same key.
            self_keyed = h.config.approval_card_key("appr-self-keyed")
            payload = _legacy_payload("appr-self-keyed")
            await h.async_redis.set(self_keyed, payload, ex=3600)

            raw_redis: AsyncRedis = AsyncRedis(
                host=h.config.valkey_host,
                port=h.config.valkey_port,
                password=h.config.valkey_password or None,
                db=h.config.valkey_db,
                decode_responses=False,
            )
            try:
                bytes_store = ApprovalCardStore(raw_redis, h.config)
                result = await bytes_store.migrate_legacy_thread_keyed_refs()
                assert (result.scanned, result.migrated, result.skipped) == (1, 0, 1)
            finally:
                await raw_redis.aclose()

            assert await h.async_redis.get(self_keyed) == payload, (
                "the pass must leave the entry exactly where it already was, "
                "not delete the only pointer to a live card"
            )

    asyncio.run(go())
