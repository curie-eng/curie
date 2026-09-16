"""Remember where an approval's Slack card was posted so it can be settled.

When the kernel pauses a run for approval (#246, ADR-0010) it posts a Block Kit
card with live Approve/Reject buttons. On resolution the dispatcher edits that
card in place from the click's interaction payload. An EXPIRY has no click
(#419): the #412 sweeper -- or a resolve attempt that arrives past the SLA --
flips the record to ``expired`` and enqueues a platform-authored resume turn,
but nothing ever touched the card, so its buttons keep looking live.

This tiny Valkey store bridges what the click payload would otherwise carry. The
kernel remembers the card destination and summary under the approval id, reads
that ref without consuming it, then removes the exact value only after the card
edit succeeds.

There is intentionally no dual read at runtime for entries keyed by THREAD
before this layout was introduced: the settle path stays single-keyed on the
approval id. Instead ``migrate_legacy_thread_keyed_refs`` runs once at worker
boot and REKEYS those entries onto their approval id, so the ordinary settle
path finds them (#1751). Without it, an approval already pending when the
workers rolled kept its live Approve/Reject buttons forever if it later EXPIRED
-- an expiry carries no click, so the click path could never heal it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any

from redis.asyncio import Redis

from .config import WorkerConfig

logger = logging.getLogger(__name__)

# Remove a ref only when it is still the exact value read before delivery. This
# prevents an older settlement pass from deleting a replacement ref written for
# the same approval while the card edit was in flight.
_CONSUME_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

# The card outlives the turn that posted it by however long the approval SLA runs
# (hours to days), so its memory must too. A fixed, generous ceiling avoids a new
# boot-env knob: an entry that outlives this is only ever cleaned up by TTL, and a
# card whose memory lapsed simply is not auto-disabled (the resolve-click path
# still heals it on the next interaction).
DEFAULT_CARD_TTL_S = 14 * 24 * 60 * 60

# One SCAN page. The boot pass walks a single narrow prefix once, so this only
# bounds how much of the keyspace one round trip inspects -- big enough that a
# few thousand pending approvals do not cost thousands of round trips, small
# enough that the cursor never blocks a Valkey serving the kernel's locks.
_MIGRATION_SCAN_COUNT = 200

# The one negative ``PTTL`` reply that still describes a LIVE key: "the key
# exists but has no expiry set". Every other negative reply -- -2, "no such
# key", and anything a future server adds -- means the key is not there, which
# is a completely different instruction to the migration than "no deadline".
_PTTL_NO_EXPIRY = -1


@dataclass(frozen=True)
class ApprovalCardRef:
    """Where a posted approval card lives, enough to REBUILD it in place later.

    ``requested_by`` lets the settled rebuild show the same "Requested by" line
    as the live card after the sandbox is gone.

    ``kind``/``adapter`` joined ``endpoint`` for ADR-0096 phase 2: the card's
    destination is selected from the channel it POSTS TO, not from the turn that
    requested it (a policy-routed card belongs to a channel the requesting turn
    may not even share a transport with), so the settle path must re-use the
    whole destination rather than rebuild two thirds of it from the resume turn.
    ``kind`` is the discriminator for a pre-upgrade entry: ``""`` means the
    destination was never recorded, and the kernel falls back to the resume
    turn's kind and route exactly as it did before. A non-empty ``kind`` means
    the triple is authoritative, and ``adapter=None`` there means the worker's
    default transport for that kind rather than "unknown".
    """

    channel: str
    ts: str
    summary: str
    endpoint: str | None = None
    requested_by: str = ""
    kind: str = ""
    adapter: str | None = None


@dataclass(frozen=True)
class LegacyCardMigration:
    """What one boot pass of the legacy rekey actually did.

    Returned rather than only logged so the behaviour is directly assertable:
    "the pass ran" and "the pass moved something" are different facts, and a
    second run proving ``migrated == 0`` is how idempotence is pinned.
    """

    scanned: int = 0
    migrated: int = 0
    skipped: int = 0


def _load(raw: str | bytes) -> dict[str, Any] | None:
    """The stored value decoded as a payload mapping, or None when it is not one.

    The ONE decode of a stored entry, and the one place that decides what
    "unparseable" means. Every question the module asks of an entry -- is it
    legacy, is it a usable ref -- is then asked of this single parsed dict, so
    two callers can no longer decode the same bytes twice, nor drift on which
    exceptions count as corrupt (they once did: one caught ``ValueError`` alone
    while the other also caught ``KeyError``/``TypeError``).
    """

    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _parse_ref(data: dict[str, Any]) -> ApprovalCardRef | None:
    """A parsed payload as a ref in the CURRENT shape, or None when it is not one.

    Shared by ``read`` and the legacy migration so the two can never drift on
    what a stored entry means. Unknown fields (notably the ``approval_id`` a
    pre-rekey entry carried) are dropped by construction: the ref is rebuilt
    field by field rather than splatted, so re-serialising the result is how the
    stale discriminator gets stripped.

    The decode already happened in ``_load``; what is caught here is strictly
    SHAPE drift -- a missing or unusable field in an otherwise valid payload.
    """

    try:
        return ApprovalCardRef(
            channel=str(data["channel"]),
            ts=str(data["ts"]),
            summary=str(data["summary"]),
            endpoint=data.get("endpoint"),
            requested_by=str(data.get("requested_by") or ""),
            kind=str(data.get("kind") or ""),
            adapter=data.get("adapter"),
        )
    except (ValueError, KeyError, TypeError):
        # A shape-drifted entry must not break the resume; treat it as no
        # remembered card (the click path can still heal the card).
        return None


def _legacy_approval_id(data: dict[str, Any]) -> str | None:
    """The approval id a PRE-rekey payload carried inside it, if any.

    This field is the whole discriminator between the two layouts (#1751). The
    thread-keyed layout stored an ``approval_id`` alongside the destination
    (added by #1199); the current approval-id-keyed layout does not, because the
    KEY carries that identity. So a payload with a non-empty ``approval_id`` is
    a legacy entry and a payload without one is either a current entry or a
    pre-#1199 legacy entry that never recorded which approval it belonged to --
    and therefore was never pairable in the first place. Both of those are left
    strictly alone; eating a live entry here would strand the very cards this
    exists to rescue.
    """

    approval_id = data.get("approval_id")
    if not isinstance(approval_id, str) or not approval_id:
        return None
    return approval_id


class ApprovalCardStore:
    """Valkey memory of posted approval cards, keyed by approval id."""

    def __init__(
        self, redis: Redis, config: WorkerConfig, *, ttl_s: int = DEFAULT_CARD_TTL_S
    ) -> None:
        self._redis = redis
        self._config = config
        self._ttl_s = ttl_s

    async def remember(
        self,
        approval_id: str,
        *,
        channel: str,
        ts: str,
        summary: str,
        endpoint: str | None,
        requested_by: str = "",
        kind: str = "",
        adapter: str | None = None,
    ) -> None:
        # Reject the empty id before it collapses every such write onto the same
        # bare approval card key.
        if not approval_id:
            raise ValueError("approval_id is required to remember an approval card")
        ref = ApprovalCardRef(
            channel=channel,
            ts=ts,
            summary=summary,
            endpoint=endpoint,
            requested_by=requested_by,
            kind=kind,
            adapter=adapter,
        )
        await self._write(approval_id, ref)

    async def _write(
        self, key: str, ref: ApprovalCardRef, *, nx: bool = False
    ) -> None:
        payload = json.dumps(asdict(ref))
        await self._redis.set(
            self._config.approval_card_key(key), payload, ex=self._ttl_s, nx=nx
        )

    async def restore(self, key: str, ref: ApprovalCardRef) -> None:
        """Restore a popped ref only when no newer ref replaced it."""

        await self._write(key, ref, nx=True)

    async def pop(self, key: str) -> ApprovalCardRef | None:
        """Atomically return and delete a remembered ref."""

        raw = await self._redis.getdel(self._config.approval_card_key(key))
        if not raw:
            return None
        try:
            data = json.loads(raw)
            return ApprovalCardRef(
                channel=str(data["channel"]),
                ts=str(data["ts"]),
                summary=str(data["summary"]),
                endpoint=data.get("endpoint"),
                requested_by=str(data.get("requested_by") or ""),
                kind=str(data.get("kind") or ""),
                adapter=data.get("adapter"),
            )
        except (ValueError, KeyError, TypeError):
            return None

    async def remember_notice_ref(self, approval_id: str, ref: str) -> None:
        """Remember the ref a pending notice minted for a ref-less row (#2721).

        The row is persisted before any delivery, so a placeholderless turn's
        record carries no ref; this is the only memory of the message the resume
        should edit. Same TTL as the card, for the same SLA reason.
        """

        if not approval_id:
            raise ValueError("approval_id is required to remember a notice ref")
        await self._redis.set(
            self._config.approval_notice_ref_key(approval_id), ref, ex=self._ttl_s
        )

    async def read_notice_ref(self, approval_id: str) -> str | None:
        """The remembered notice ref for an approval, if any (#2721)."""

        raw = await self._redis.get(self._config.approval_notice_ref_key(approval_id))
        if not raw:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    async def read(
        self, approval_id: str
    ) -> tuple[ApprovalCardRef, str | bytes] | None:
        """Return the remembered card and its exact stored value without mutation."""

        raw = await self._redis.get(self._config.approval_card_key(approval_id))
        if not raw:
            return None
        # A corrupt entry (undecodable) and a shape-drifted one (decodable but
        # not a usable ref) are the same answer here: no remembered card, never
        # a raised exception that would break the resume.
        data = _load(raw)
        if data is None:
            return None
        ref = _parse_ref(data)
        if ref is None:
            return None
        return ref, raw

    async def consume(self, approval_id: str, expected_raw: str | bytes) -> bool:
        """Delete the ref only when its exact stored value still matches."""

        return await self._consume_key(
            self._config.approval_card_key(approval_id), expected_raw
        )

    async def _consume_key(self, key: str, expected_raw: str | bytes) -> bool:
        """Compare-and-delete one KEY, whatever layout put the value there.

        The primitive under both ``consume`` -- which derives the key from an
        approval id -- and the legacy migration, which must act on the pre-rekey
        raw key it scanned and so cannot go through the public verb. Keeping one
        caller of ``eval`` means the script's argument order (one KEY, the
        expected value as ``ARGV[1]``) is written down in exactly one place.
        """

        removed = await self._redis.eval(_CONSUME_LUA, 1, key, expected_raw)
        return bool(removed)

    async def migrate_legacy_thread_keyed_refs(self) -> LegacyCardMigration:
        """Rekey pre-#1723 THREAD-keyed refs onto their approval id (#1751).

        A ONE-SHOT, best-effort, idempotent boot pass, not a runtime dual read
        and not a maintenance loop. The distinction matters: the comment on
        ``WorkerConfig.completions_pending_key`` forbids the REPEATING sweeper
        from scanning a production keyspace, and that prohibition stands. This
        runs once per process start, over one narrow prefix, with a bounded
        ``COUNT``, and becomes a pure no-op the moment no legacy entry remains
        -- every surviving key under the prefix is then a current-layout entry
        the discriminator declines to touch.

        Why rekey rather than dual-read: #1723 moved the pointer from the thread
        to the approval id with no compatibility path, so any approval already
        pending when the workers rolled is invisible to
        ``Kernel._finalize_settled_card``. A resolve CLICK still heals its card
        from the interaction payload; an EXPIRY has no click, so the card keeps
        live Approve/Reject buttons until its 14 day TTL lapses. Moving the
        entry lets the ordinary single-keyed settle path find it, which leaves
        exactly one read path in the hot loop.

        Every step is defensive about a still-running OLD-version replica and
        about a NEW-version replica that has already posted its own card for the
        same approval:

        * the write is ``NX``, so an entry the upgraded worker wrote for that
          approval always wins over the stale legacy pointer;
        * the delete is the same compare-and-delete Lua ``consume`` uses,
          against the exact bytes read in this pass, so an old replica that
          rewrote the thread key mid-pass is not clobbered;
        * the remaining TTL is carried over, so an approval three days from its
          SLA does not have its card memory resurrected for another fortnight.

        Per-entry failures are logged and swallowed: a single unparseable or
        racing key must not abort the rest of the pass, and the pass must never
        fail worker boot.

        The residual a one-shot pass cannot close: during a ROLLING upgrade an
        old-version replica is still serving, and it can pause a new approval --
        writing a fresh thread-keyed entry -- AFTER this replica's pass has
        already scanned. Nothing revisits that entry, so if it later expires its
        card keeps live buttons, exactly the #1751 symptom. Closing it would
        take the runtime dual read #1751 rules out, so it is stated rather than
        fixed: the window shuts when the roll finishes and every replica is on
        the new layout, and anything the pass missed lapses on its existing TTL.

        TRANSITIONAL -- MARKED FOR REMOVAL. This exists only to carry the #1723
        key-layout change across a roll, yet it is roughly half of a module whose
        real job is the hot settle path. It is safe to delete once no supported
        upgrade path starts from a worker older than that change: every legacy
        entry has lapsed on its 14 day TTL by then, so the pass can only ever be
        a no-op. Deleting it should take ``_legacy_approval_id``, the
        ``written_targets`` bookkeeping, and the migration constants
        (``_MIGRATION_SCAN_COUNT``, ``_PTTL_NO_EXPIRY``) with it.
        """

        scanned = migrated = skipped = 0
        # Targets THIS pass wrote. The pass writes rekeyed entries back into the
        # very keyspace it is scanning, and Redis makes no guarantee about keys
        # created during an in-flight SCAN -- a fresh target may or may not come
        # back on a later batch. Left unguarded, the returned counts, the GET
        # count, and the INFO summary an operator reads all depend on that coin
        # flip. Skipping our own output makes them identical either way. The set
        # is bounded by the number of entries actually MIGRATED (pending approval
        # cards only), so it does not reintroduce the unbounded-memory problem
        # the streaming ``scan_iter`` exists to avoid.
        written_targets: set[str] = set()
        # The prefix segment is shared by both layouts -- only the SUFFIX moved
        # (thread key -> approval id) -- so one match pattern sees every entry of
        # either generation, and the payload (not the key shape) decides which.
        pattern = f"{self._config.key_prefix}:approval-card:*"
        async for key in self._redis.scan_iter(match=pattern, count=_MIGRATION_SCAN_COUNT):
            # ``scan_iter`` yields ``str`` or ``bytes`` depending on how the
            # client was built (``decode_responses``), while every key this pass
            # DERIVES -- the target, the written-targets guard -- is always a
            # ``str``. Normalise ONCE, here, and use only the normalised form
            # below: an un-normalised ``bytes`` key compares unequal to its own
            # ``str`` target under ``decode_responses=False``, and for a legacy
            # entry whose thread key happens to equal its approval id that turns
            # the "already where it belongs" guard into a delete of a live
            # pointer. The GET/PTTL/EVAL all accept the ``str`` form, so there is
            # no reason to keep the raw one alive past this line.
            scanned_key = key.decode() if isinstance(key, bytes) else key
            if scanned_key in written_targets:
                # This pass's own output, not an entry it was asked to consider:
                # counted as neither ``scanned`` nor ``skipped``.
                continue
            scanned += 1
            try:
                if await self._migrate_one(scanned_key, written_targets):
                    migrated += 1
                else:
                    skipped += 1
            except Exception as exc:  # noqa: BLE001 - one bad key must not end the pass
                skipped += 1
                logger.warning(
                    "legacy approval card migration failed for key %s: %s",
                    scanned_key,
                    exc,
                )
        counts = LegacyCardMigration(scanned=scanned, migrated=migrated, skipped=skipped)
        logger.info(
            "legacy approval card migration: scanned=%d migrated=%d skipped=%d",
            counts.scanned,
            counts.migrated,
            counts.skipped,
        )
        return counts

    async def _migrate_one(self, key: str, written_targets: set[str]) -> bool:
        """Rekey one entry, returning whether this pass moved it.

        False covers every "leave it alone" case as well as a lost NX race, so
        the caller's ``migrated`` count only ever grows on a real relocation.

        ``key`` is the caller's ALREADY-NORMALISED ``str``, never the raw value
        ``scan_iter`` yielded, so the ``target == key`` comparison below is a
        like-for-like one whatever ``decode_responses`` the client was built
        with.

        ``written_targets`` collects every key THIS pass wrote, so the caller can
        skip its own output if the in-flight SCAN hands it back.
        """

        raw = await self._redis.get(key)
        if not raw:
            # Expired or consumed between the SCAN and the GET. Nothing to move.
            return False
        # Decoded ONCE here; both questions below -- which layout is this, and is
        # its destination usable -- are asked of this single parsed payload.
        data = _load(raw)
        if data is None:
            # A corrupt value. Left COMPLETELY untouched, like every other entry
            # this pass cannot positively identify as legacy.
            return False
        approval_id = _legacy_approval_id(data)
        if approval_id is None:
            # A current-layout entry, or a pre-#1199 legacy entry that never
            # recorded its approval. Both are left COMPLETELY untouched -- the
            # first is a live pointer the running worker depends on.
            return False
        target = self._config.approval_card_key(approval_id)
        if target == key:
            # Already where it belongs (a legacy entry whose "thread key" was the
            # approval id, or a re-run over a key we cannot improve). Rewriting it
            # to drop the stale field would buy nothing and risks the TTL.
            return False
        ref = _parse_ref(data)
        if ref is None:
            # It named an approval but its destination is unusable, so there is
            # nothing to settle. Leave it to its TTL rather than moving a value
            # the settle path would reject anyway.
            return False
        # Carry the REMAINING life over rather than minting a fresh TTL: this
        # memory exists to outlive the approval's SLA, not to be renewed by an
        # upgrade. The two non-positive PTTL replies mean OPPOSITE things and
        # must not be collapsed: -1 is KEY EXISTS, NO EXPIRY, which deserves the
        # standard ceiling; -2 is NO SUCH KEY -- the source expired or was
        # consumed between the GET above and this call -- and writing the target
        # then would resurrect a ref that had legitimately gone away, with a
        # fresh 14 days of life on an already-stale payload. Abort the entry
        # instead: no write, and no compare-and-delete either, since there is
        # nothing left to delete.
        pttl = await self._redis.pttl(key)
        if not isinstance(pttl, int):
            return False
        if pttl < 0 and pttl != _PTTL_NO_EXPIRY:
            return False
        # The residual this does NOT close: the source can still vanish just
        # AFTER a positive PTTL, and the NX write then re-creates an entry for
        # an approval whose pointer had gone. That outcome is bounded and
        # self-healing -- the settle path consumes it, or its carried-over TTL
        # (never a renewed one) disposes of it -- which is categorically
        # different from minting a fresh fortnight for a dead ref.
        ttl_ms = pttl if pttl > 0 else self._ttl_s * 1000
        # Re-serialised from the CURRENT ref shape, so the stale ``approval_id``
        # discriminator does not travel to the new key and make the migrated
        # entry look legacy to the next pass.
        payload = json.dumps(asdict(ref))
        written = await self._redis.set(target, payload, px=ttl_ms, nx=True)
        if written:
            # Only a WON NX write is ours to skip. On a lost race the target was
            # written by the running worker, not by this pass, so it is a genuine
            # pre-existing entry the scan is entitled to visit -- and being
            # current-layout it is correctly counted as a skip.
            written_targets.add(target)
        # Delete the old key EITHER WAY. If the NX write lost, an entry the
        # upgraded worker already wrote for this approval supersedes the legacy
        # pointer, so the legacy key is dead in both branches. The compare is
        # what protects a still-running old replica that rewrote it mid-pass.
        await self._consume_key(key, raw)
        return bool(written)
