"""The kernel issues each turn's channel read capability (ADR 0100, #2877).

The kernel is the only issuer. At turn open it mints a ``chr`` capability for
one logical turn through the API's internal context route, under an owner id
unique to the attempt and against the deployment the sandbox actually runs,
records the live logical turn in Valkey so a steer on any worker can renew it,
refuses the turn when the runner does not advertise ``channel_read: true``,
and attaches the capability to the runner ``Event``. A canvas grant (ADR 0200)
counts as a grant here: the same capability serves the canvas tools.

The grant is read from the stored bundle's manifest, cached by bundle ref,
so an ungranted turn makes no mint call and a granted turn on a runner that
cannot enforce it is refused whatever the mint or live record outcome. A
failed mint otherwise runs the turn without the capability (the read tools
then refuse); a failed mint whose grant could not be read either is refused
retryably. A steer renews the opener's logical turn and never revokes.

The active ledger entry is a lease (``LEASE_TTL_S``) that a heartbeat child
of the attempt renews every ``_LEASE_RENEW_S`` and that the attempt's end
stops, so a worker that dies or cannot reach Valkey lets it lapse within one
lease. Every attempt end also tombstones its owner (a late committed open is
then refused), revokes what it opened, and revokes by owner what it may have
opened without seeing the answer, directly in the Valkey the API reads. A
revoke that fails is retained and retried with bounded backoff until the
token's expiry. The token is never logged.
"""

from __future__ import annotations

import asyncio
import json
import math
import tempfile
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from aci_protocol import (
    CHANNEL_READ_STATUS_FIELD,
    ChannelReadCapability,
    Event,
    QueuedTurn,
)
from curie_internal.channel_read_ledger import LEASE_TTL_S
from plugin_format import PluginManifest, resolve_manifest
from pydantic import ValidationError

from ..binding import SANDBOX_TOKEN_TTL_SECONDS, ChannelReadGrantAbsent, ChannelReadMint
from ..bundle_store import BundleReader, extract_bundle
from ..markers import LiveChannelReadTurn
from ..runner_client import RunnerError
from ..sandbox.types import SandboxHandle

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, failures
from .log import logger

# The longest a capability may live: the API's cap, and the worker's sandbox
# token lifetime.
_MAX_TTL_S = SANDBOX_TOKEN_TTL_SECONDS

# The bound on one revoke or record delete, so a hung Valkey cannot hold a
# single try. Read at call time.
_CLOSE_TIMEOUT_S = 6.0

# Backoff between retries of a retained revoke: doubling, capped.
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 30.0

# How long shutdown waits for revocations still in flight.
_CLOSE_SHUTDOWN_GRACE_S = 2.0

# The grant caches are cleared when they grow past this many entries.
_CACHE_MAX = 4096

# How often the heartbeat renews the attempt's leases: a third of the lease,
# so two missed renewals still leave it alive. Read at call time.
_LEASE_RENEW_S = LEASE_TTL_S / 3

# The bound on one bundle grant lookup (download and extract). It runs on turn
# open under the per-thread lock, beside a mint bounded at 3 s and a status
# read bounded at 2 s, so 5 s keeps a cold lookup in the same control plane
# range while leaving room for a normal in-cluster object store read (well
# under a second). A timeout is an unknown grant and is not cached. Read at
# call time.
_BUNDLE_GRANT_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class TurnChannelReadGrant:
    """What the kernel needs to mint a turn's channel read capability.

    Built in the routing block from the resolved deployment. ``default`` is the
    turn's inbound binding ``(kind, address)``, or None for a targetless hook
    turn, which must name its channel on every read. Absent for an eval
    isolated turn and a turn with no resolved deployment. ``deployment_id`` is
    the resolved one; a retained sandbox mints against the one it booted."""

    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    default: tuple[str, str] | None
    thread_key: str
    # The resolved deployment's bundle, where the grant is read from.
    bundle_ref: str | None = None


@dataclass(frozen=True)
class _ChannelReadMint:
    """This attempt's grant, read where the turn actually opens."""

    qevent: QueuedTurn
    grant: TurnChannelReadGrant


def _new_owner() -> str:
    return uuid.uuid4().hex


@dataclass
class _AttemptChannelRead:
    """The logical turns one attempt opened, and the owner it opened them as.

    ``owner`` is unique per attempt, so an approval resume (a new attempt) is
    never revoked by the original attempt's late settlement. A work item
    continuation inside the attempt reuses it. ``opened`` holds
    ``(agent_id, turn_key, thread_key)``, appended the moment a mint returns.
    ``attempted`` names every agent an open mint was sent for, recorded before
    the request, so a mint whose answer was lost is still revoked by owner.
    ``deadline`` is the latest wall clock expiry any of those could carry."""

    owner: str = field(default_factory=_new_owner)
    opened: list[tuple[uuid.UUID, str, str]] = field(default_factory=list)
    attempted: set[uuid.UUID] = field(default_factory=set)
    deadline: float = 0.0
    # The lease renewal task, started at the first mint and cancelled by the
    # attempt's ``finally`` (``stop_heartbeat``).
    heartbeat: asyncio.Task[None] | None = None

    def pending(self) -> bool:
        return bool(self.opened or self.attempted)

    async def stop_heartbeat(self) -> None:
        """Cancel and join the lease renewal, so nothing renews after the end."""

        task, self.heartbeat = self.heartbeat, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@dataclass(eq=False)
class _RetainedRevoke:
    """A revoke that failed and is being retried until its deadline."""

    label: str
    wake: asyncio.Event = field(default_factory=asyncio.Event)


# Every revoke currently waiting to retry, so shutdown can attempt them now.
_RETAINED: set[_RetainedRevoke] = set()


def _ttl_s(self: Kernel, remaining_s: float | None) -> int:
    """The capability lifetime: the stream deadline of the call it rides."""

    return max(1, min(math.ceil(self._runner.turn_deadline_s(remaining_s)), _MAX_TTL_S))


def _capability(self: Kernel, token: str) -> ChannelReadCapability:
    base = self._config.runner_facing_api_base_url.rstrip("/")
    return ChannelReadCapability(url=f"{base}/channel-read", token=token)


def _remember(cache: set[uuid.UUID], deployment_id: uuid.UUID) -> None:
    if len(cache) >= _CACHE_MAX:
        cache.clear()
    cache.add(deployment_id)


async def _try_once(op: Callable[[], Awaitable[object]], label: str) -> bool:
    """One bounded try; True when it ran to completion. Never raises."""

    try:
        async with asyncio.timeout(_CLOSE_TIMEOUT_S):
            await op()
    except TimeoutError:
        logger.warning("channel read %s timed out", label)
        return False
    except Exception as exc:  # noqa: BLE001 -- retried by the caller
        logger.warning("channel read %s failed: %s", label, type(exc).__name__)
        return False
    return True


async def _revoke_until_settled(
    op: Callable[[], Awaitable[object]], label: str, deadline: float
) -> None:
    """Run ``op`` until it completes or ``deadline`` (wall clock) passes.

    A failure is retained in ``_RETAINED`` and retried with doubling backoff;
    shutdown wakes it for one more try. Past the deadline the capability has
    expired on its own, so there is nothing left to revoke."""

    if await _try_once(op, label):
        return
    retained = _RetainedRevoke(label=label)
    _RETAINED.add(retained)
    try:
        delay = _RETRY_INITIAL_S
        while (left := deadline - clock.time.time()) > 0:
            try:
                await asyncio.wait_for(retained.wake.wait(), timeout=min(delay, left))
            except TimeoutError:
                pass
            retained.wake.clear()
            if await _try_once(op, label):
                return
            delay = min(delay * 2, _RETRY_MAX_S)
        logger.warning("channel read %s left to expire after retries", label)
    finally:
        _RETAINED.discard(retained)


async def _revoke_now(self: Kernel, agent_id: uuid.UUID, turn_key: str, owner: str) -> None:
    """One owner checked revoke, bounded; never raises. Settlement retries it."""

    await _try_once(
        partial(self._markers.revoke_channel_read_owner, agent_id, turn_key, owner), "revoke"
    )


async def _booted_bundle_ref(self: Kernel, handle: SandboxHandle) -> str | None:
    """The bundle this sandbox booted with: its route, else its claim spec."""

    if handle.bundle_ref:
        return handle.bundle_ref
    reader = getattr(self._substrate, "claim_bundle_ref", None)
    if reader is None:
        return None
    try:
        booted = await asyncio.to_thread(reader, handle.claim_name)
    except Exception as exc:  # noqa: BLE001 -- unknown means no capability
        logger.warning("claim bundle ref unreadable: %s", type(exc).__name__)
        return None
    return booted if isinstance(booted, str) and booted else None


async def _pinned_deployment(
    self: Kernel, handle: SandboxHandle, grant: TurnChannelReadGrant
) -> tuple[uuid.UUID, str | None] | None:
    """The deployment and bundle the sandbox this turn opens on was booted for.

    A retained sandbox keeps the bundle it first booted with, so a redeploy
    between turns must not mint against the new deployment. The pin names the
    claim it was written for. Without a pin for this claim, the claim's own
    bundle ref decides: equal to the resolved row's, the resolved deployment
    becomes the pin; different or unknown, the turn gets no capability."""

    pin = await self._markers.read_channel_read_pin(grant.thread_key)
    if pin is not None and pin.claim_name == handle.claim_name and pin.agent_id == grant.agent_id:
        return pin.deployment_id, pin.bundle_ref
    booted = await _booted_bundle_ref(self, handle)
    if booted is None or booted != grant.bundle_ref:
        return None
    await self._markers.pin_channel_read_deployment(
        grant.thread_key, handle.claim_name, grant.agent_id, grant.deployment_id, booted
    )
    return grant.deployment_id, booted


class _ManifestUnreadable(Exception):
    """The bundle has no parseable manifest: its grant is unknown, not absent."""


def _read_grant(reader: BundleReader, bundle_ref: str, limits: tuple[int, float, int]) -> bool:
    """Whether a stored bundle's manifest grants any platform Slack capability.

    That is channel read or a canvas operation, each a literal true.

    Only a manifest that parses establishes an answer; a missing or malformed
    one raises ``_ManifestUnreadable`` so it is never taken as no grant."""

    data = reader.get(bundle_ref)
    max_bytes, max_ratio, max_members = limits
    with tempfile.TemporaryDirectory(prefix="curie-channel-read-") as tmp:
        root = extract_bundle(
            data,
            Path(tmp),
            max_uncompressed_bytes=max_bytes,
            max_compression_ratio=max_ratio,
            max_members=max_members,
        )
        manifest_path = resolve_manifest(root)
        if manifest_path is None:
            raise _ManifestUnreadable("the bundle has no manifest")
        try:
            manifest = PluginManifest.model_validate(
                json.loads(manifest_path.read_text(encoding="utf-8"))
            )
        except (ValidationError, ValueError) as exc:
            raise _ManifestUnreadable("the bundle manifest does not parse") from exc
    return bool(manifest.platform_slack_grants())


async def _bundle_grant(self: Kernel, bundle_ref: str | None) -> bool | None:
    """The bundle's channel read grant: True, False, or None when unreadable."""

    if bundle_ref is None:
        return False
    cached = self._bundle_grants.get(bundle_ref)
    if cached is not None:
        return cached
    if self._bundles is None:
        return None
    limits = (
        self._config.bundle_max_uncompressed_bytes,
        self._config.bundle_max_compression_ratio,
        self._config.bundle_max_members,
    )
    try:
        # Off the event loop, and bounded: an abandoned lookup's late answer
        # is discarded with this coroutine, so it is never cached.
        granted = await asyncio.wait_for(
            asyncio.to_thread(_read_grant, self._bundles, bundle_ref, limits),
            timeout=_BUNDLE_GRANT_TIMEOUT_S,
        )
    except Exception as exc:  # noqa: BLE001 -- unknown, decided by the mint
        logger.warning("channel read grant unreadable from the bundle: %s", type(exc).__name__)
        return None
    if len(self._bundle_grants) >= _CACHE_MAX:
        self._bundle_grants.clear()
    self._bundle_grants[bundle_ref] = granted
    return granted


async def _heartbeat(self: Kernel, record: _AttemptChannelRead) -> None:
    """Renew every lease the attempt holds until the attempt cancels this."""

    while True:
        await asyncio.sleep(_LEASE_RENEW_S)
        for agent_id, turn_key, _thread_key in list(dict.fromkeys(record.opened)):
            await _try_once(
                partial(self._markers.refresh_channel_read_owner, agent_id, turn_key, record.owner),
                "lease renewal",
            )


async def _with_channel_read(
    self: Kernel,
    event: Event,
    handle: SandboxHandle,
    remaining_s: float | None,
    *,
    grant: TurnChannelReadGrant | None = None,
    qevent: QueuedTurn | None = None,
) -> Event:
    """``event`` with this turn's channel read capability (ADR 0100).

    Call it just before the runner call that opens the turn, with the
    ``remaining_s`` that call gets. ``grant`` and ``qevent`` default to this
    attempt's carry.

    The grant is the bundle the sandbox booted with (its pin, or the claim's
    bundle ref matching the resolved row). An ungranted bundle, or a sandbox
    whose bundle cannot be matched, sends no capability and makes no mint
    call. A granted turn is refused with ``ChannelReadUnenforced`` when the
    runner does not advertise literal ``true``, whatever the mint or live
    record outcome. A failed mint whose grant could not be read either raises
    the retryable ``ChannelReadUnavailable``."""

    # ``_to_event`` never sets ``channel_read``, so an event left as is
    # carries null.
    unminted = event
    if grant is None or qevent is None:
        carry = constants._CHANNEL_READ_MINT.get()
        if carry is None:
            return unminted
        grant, qevent = carry.grant, carry.qevent
    record = constants._CHANNEL_READ_TURNS.get()
    context = getattr(self._binding, "channel_read_context", None)
    if record is None or context is None:
        return unminted
    if remaining_s is not None and remaining_s <= 0:
        return unminted
    try:
        pinned = await _pinned_deployment(self, handle, grant)
    except Exception as exc:  # noqa: BLE001 -- decided below without a mint
        logger.warning("channel read pin unavailable: %s", type(exc).__name__)
        # The pin store failed, so nothing is minted. The bundle the sandbox
        # BOOTED (its route, else its claim) still decides enforcement, never
        # the newly resolved one: a retained sandbox keeps its first bundle.
        # A granted boot needs an enforcing runner and then runs with no
        # capability; an unknown boot is refused retryably.
        booted = await _booted_bundle_ref(self, handle)
        if booted is None:
            raise failures.ChannelReadUnavailable() from None
        granted = await _bundle_grant(self, booted)
        if granted is False:
            return unminted
        if granted is None and self._bundles is not None:
            raise failures.ChannelReadUnavailable() from None
        if granted is True:
            await _require_enforcement(self, handle, remaining_s)
        return unminted
    if pinned is None:
        return unminted
    deployment_id, bundle_ref = pinned
    granted = await _bundle_grant(self, bundle_ref)
    if granted is False:
        return unminted
    if granted is None and deployment_id in self._channel_read_absent:
        return unminted
    ttl_s = _ttl_s(self, remaining_s)
    # Before the request: an answer lost to a timeout or cancellation is
    # still revoked by owner, and its owner tombstoned, at the attempt's end.
    record.attempted.add(grant.agent_id)
    requested_deadline = clock.time.time() + ttl_s
    minted: ChannelReadMint | None = None
    try:
        minted = await context(
            agent_id=grant.agent_id,
            deployment_id=deployment_id,
            event_id=qevent.event_id,
            mode="open",
            owner=record.owner,
            default=grant.default,
            ttl_s=ttl_s,
        )
    except ChannelReadGrantAbsent:
        self._channel_read_granted.discard(deployment_id)
        _remember(self._channel_read_absent, deployment_id)
        return unminted
    except Exception as exc:  # noqa: BLE001 -- a failed mint fails closed
        logger.warning("channel read mint failed for %s: %s", qevent.event_id, type(exc).__name__)
        minted = None
    if minted is None:
        record.deadline = max(record.deadline, requested_deadline)
        if granted is None and deployment_id not in self._channel_read_granted:
            if self._bundles is not None:
                # The bundle could not be read and the mint failed, so nothing
                # says whether this turn must be enforced: refuse, retryably.
                raise failures.ChannelReadUnavailable()
            # No bundle reader is wired, so the grant is never known locally:
            # the turn runs without a capability and its tools refuse.
            return unminted
        await _require_enforcement(self, handle, remaining_s)
        return unminted
    record.deadline = max(record.deadline, float(minted.expires_at))
    _remember(self._channel_read_granted, deployment_id)
    record.opened.append((grant.agent_id, minted.turn_key, grant.thread_key))
    if record.heartbeat is None:
        record.heartbeat = asyncio.ensure_future(_heartbeat(self, record))
    live = LiveChannelReadTurn(
        agent_id=grant.agent_id,
        deployment_id=deployment_id,
        event_id=qevent.event_id,
        owner=record.owner,
        default=grant.default,
    )
    try:
        await self._markers.record_channel_read_turn(grant.thread_key, live, ttl_s)
    except Exception as exc:  # noqa: BLE001 -- revoke, then run without it
        logger.warning(
            "channel read live record failed for %s: %s; revoking",
            qevent.event_id,
            type(exc).__name__,
        )
        await _revoke_now(self, grant.agent_id, minted.turn_key, record.owner)
        await _require_enforcement(self, handle, remaining_s)
        return unminted
    # The attempt's settlement revokes the capability and deletes the live
    # record on a refusal; ``opened`` already names them.
    await _require_enforcement(self, handle, remaining_s)
    return event.model_copy(update={"channel_read": _capability(self, minted.token)})


async def _require_enforcement(
    self: Kernel, handle: SandboxHandle, remaining_s: float | None
) -> None:
    """Refuse a granted turn whose runner does not advertise literal ``true``."""

    if not await self._require_channel_read(handle, remaining_s):
        raise failures.ChannelReadUnenforced()


async def _steer_channel_read(
    self: Kernel,
    event: Event,
    *,
    renew: bool,
    thread_key: str,
    remaining_s: float | None,
) -> Event:
    """``event`` with a renewed capability for the live logical turn, or null.

    ``renew`` is true only when the retained runner reports a live turn and
    advertises channel read. The renewal uses the live record's deployment,
    event id and default channel, so a steer never changes the turn's scope,
    and sends no owner. Any failure, or no live record, sends null, which
    clears the runner's credential. Nothing is written or revoked here."""

    unminted = event
    context = getattr(self._binding, "channel_read_context", None)
    if not renew or context is None:
        return unminted
    if remaining_s is not None and remaining_s <= 0:
        return unminted
    try:
        live = await self._markers.read_channel_read_turn(thread_key)
    except Exception as exc:  # noqa: BLE001 -- the steer clears the credential
        logger.warning("channel read live record unreadable: %s", type(exc).__name__)
        return unminted
    if live is None:
        return unminted
    try:
        minted = await context(
            agent_id=live.agent_id,
            deployment_id=live.deployment_id,
            event_id=live.event_id,
            mode="steer",
            owner=None,
            default=live.default,
            ttl_s=_ttl_s(self, remaining_s),
        )
    except Exception as exc:  # noqa: BLE001 -- includes a grant removed since open
        logger.warning("channel read steer mint failed: %s", type(exc).__name__)
        return unminted
    if minted is None:
        return unminted
    return event.model_copy(update={"channel_read": _capability(self, minted.token)})


def _settled_close(task: asyncio.Task[None]) -> None:
    constants._PENDING_CHANNEL_READ_CLOSES.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.warning("channel read settlement failed: %s", type(task.exception()).__name__)


def _settle_channel_read(self: Kernel, record: _AttemptChannelRead) -> None:
    """Revoke what an attempt opened, in the background; never raises.

    The attempt's end does not wait: the revocation runs as its own task,
    kept in ``_PENDING_CHANNEL_READ_CLOSES`` so it is not garbage collected,
    and a cancelled attempt still revokes."""

    task = asyncio.ensure_future(self._close_channel_read(record))
    constants._PENDING_CHANNEL_READ_CLOSES.add(task)
    task.add_done_callback(_settled_close)


async def _close_channel_read(self: Kernel, record: _AttemptChannelRead) -> None:
    """Delete the attempt's live records, then tombstone and revoke until settled.

    Each agent a mint was sent for gets its owner tombstoned first, so an open
    the API commits late is refused, and then a revoke by owner through the
    ledger's index, which covers a mint whose answer never arrived. Each
    opened logical turn is also revoked by the owner checked delete. A failed
    step is retained and retried until ``record.deadline`` (the token's
    expiry); deleting the live record (which only stops steer renewal) never
    counts as settling it. The record delete is owner checked, so a newer
    turn's record on the thread survives."""

    await record.stop_heartbeat()
    owner = record.owner
    for thread_key in dict.fromkeys(thread_key for _a, _t, thread_key in record.opened):
        await _try_once(
            partial(self._markers.take_channel_read_turn, thread_key, owner),
            "live record delete",
        )
    deadline = record.deadline or clock.time.time() + _MAX_TTL_S
    steps: list[Awaitable[None]] = []
    for agent_id, turn_key, _thread_key in dict.fromkeys(record.opened):
        steps.append(
            _revoke_until_settled(
                partial(self._markers.revoke_channel_read_owner, agent_id, turn_key, owner),
                "revoke",
                deadline,
            )
        )
    for agent_id in record.attempted:
        steps.append(_tombstone_then_revoke(self, agent_id, owner, deadline))
    await asyncio.gather(*steps)


async def _tombstone_then_revoke(
    self: Kernel, agent_id: uuid.UUID, owner: str, deadline: float
) -> None:
    # The tombstone outlives any token the owner could have been minted.
    await _revoke_until_settled(
        partial(self._markers.tombstone_channel_read_owner, agent_id, owner, _MAX_TTL_S),
        "owner tombstone",
        deadline,
    )
    await _revoke_until_settled(
        partial(self._markers.revoke_channel_read_by_owner, agent_id, owner),
        "revoke by owner",
        deadline,
    )


async def drain_pending_channel_read_closes(grace_s: float = _CLOSE_SHUTDOWN_GRACE_S) -> None:
    """On worker shutdown, attempt every retained revoke now, then give the
    settlements still in flight a short grace. One that still fails leaves its
    capability to expire at its stream deadline."""

    for retained in list(_RETAINED):
        retained.wake.set()
    pending = [task for task in constants._PENDING_CHANNEL_READ_CLOSES if not task.done()]
    if pending:
        await asyncio.wait(pending, timeout=grace_s)


async def _require_channel_read(
    self: Kernel,
    handle: SandboxHandle,
    remaining_s: float | None,
) -> bool:
    """Does THIS runner advertise channel read enforcement as literal ``true``?

    Read from the handle the event is about to be posted to, with its own
    token, exactly as ``_require_tool_access`` reads it. An unreadable status
    raises ``RunnerError``, which the caller retries like any turn the runner
    did not accept."""

    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token or None,
            remaining_s=2.0 if remaining_s is None else min(2.0, remaining_s),
        )
    except (ValueError, TypeError) as exc:
        raise RunnerError("runner status was not readable") from exc
    if not isinstance(status, dict):
        raise RunnerError("runner status was not a JSON object")
    return status.get(CHANNEL_READ_STATUS_FIELD) is True
