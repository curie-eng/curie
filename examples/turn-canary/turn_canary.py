"""Opt-in, bounded synthetic turn checker. @spec TURN-CANARY-1 TURN-CANARY-10"""

import asyncio
import fcntl
import json
import math
import os
import re
import secrets
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, TextIO
from urllib.parse import urlsplit

from aci_protocol import READER_CONTEXT, QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.events import ToolAccess
from channel_protocol import scoped_conversation_id


class Platform(Protocol):
    """Operator supplied authenticated platform boundary. @spec TURN-CANARY-1 TURN-CANARY-5"""

    cleanup_blocked: bool

    async def inventory(self) -> list[dict[str, str]]: ...
    async def read_only_supported(self) -> bool: ...
    async def quota_headroom(self) -> bool | None: ...
    async def enqueue(self, turn: QueuedTurn) -> None: ...
    async def replies(self, reply_ref: str, after: int) -> dict[str, Any]: ...
    async def reset(self, agent_id: str, thread_key: str) -> None: ...
    async def reset_state(self, agent_id: str, thread_key: str) -> dict[str, Any]: ...
    async def channel_identities(self, kind: str) -> list[dict[str, Any]]: ...
    async def adapter_health(self, url: str) -> tuple[str, int | None]: ...


Route = tuple[str, str, str]
METRIC_PREFIX = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
ADAPTER_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
MAX_ADAPTERS = 20
CONVERSATION_PREFIX = "eval:turn-canary-"
CONVERSATION_PREFIX_PATTERN = re.compile(r"eval:[A-Za-z0-9._-]{0,48}")
QUANTITY = re.compile(r"([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE]([+-]?[0-9]+))?([A-Za-z]{0,2})")
QUANTITY_SUFFIXES = {
    "": Decimal(1),
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "k": Decimal("1e3"),
    "K": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
    "Ki": Decimal(2**10),
    "Mi": Decimal(2**20),
    "Gi": Decimal(2**30),
    "Ti": Decimal(2**40),
    "Pi": Decimal(2**50),
    "Ei": Decimal(2**60),
}


def _route_label(row: Mapping[str, str]) -> str:
    return "|".join((row.get("kind", ""), row.get("address", ""), row.get("adapter", "")))


def parse_quantity(value: object) -> Decimal:
    """Read one Kubernetes quantity, such as 16, 1500m, or 512Mi. @spec TURN-CANARY-5"""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("quantity must be a string or integer")
    match = QUANTITY.fullmatch(str(value))
    if match is None or match.group(3) not in QUANTITY_SUFFIXES:
        raise ValueError("value is not a Kubernetes quantity")
    if match.group(2) is not None and match.group(3):
        raise ValueError("a quantity takes an exponent or a suffix, not both")
    try:
        number = Decimal(match.group(1))
        if match.group(2) is not None:
            number = number.scaleb(int(match.group(2)))
    except InvalidOperation as exc:
        raise ValueError("value is not a Kubernetes quantity") from exc
    return number * QUANTITY_SUFFIXES[match.group(3)]


def _check_conversation_prefix(prefix: str) -> str:
    if not CONVERSATION_PREFIX_PATTERN.fullmatch(prefix):
        raise ValueError("TURN_CANARY_CONVERSATION_PREFIX must match eval:[A-Za-z0-9._-]{0,48}")
    return prefix


def _check_prefix(prefix: str) -> str:
    if not METRIC_PREFIX.fullmatch(prefix):
        raise ValueError("metric prefix must be a valid Prometheus metric name")
    return prefix


@dataclass(frozen=True)
class Limits:
    """Finite operator bounds in seconds and selected routes. @spec TURN-CANARY-5"""

    cycle_interval: float = 300.0
    turn_deadline: float = 90.0
    cleanup_deadline: float = 60.0
    poll_period: float = 2.0
    max_targets: int = 5

    def __post_init__(self) -> None:
        if (
            not all(
                0 < value <= 3600
                for value in (
                    self.cycle_interval,
                    self.turn_deadline,
                    self.cleanup_deadline,
                    self.poll_period,
                )
            )
            or not 0 < self.max_targets <= 100
        ):
            raise ValueError("canary limits must be positive and bounded")


@dataclass
class CycleResult:
    """Safe, bounded observations. @spec TURN-CANARY-5 TURN-CANARY-6"""

    success: bool = False
    cleanup_degraded: bool = False
    capacity_skips: int = 0
    target_success: dict[str, bool] = field(default_factory=dict)
    target_capacity_skipped: dict[str, bool] = field(default_factory=dict)
    target_cleanup_degraded: dict[str, bool] = field(default_factory=dict)
    target_last_run: dict[str, float] = field(default_factory=dict)
    target_last_success: dict[str, float] = field(default_factory=dict)
    logs: list[dict[str, str]] = field(default_factory=list)
    last_run: float = field(default_factory=time.time)
    last_success: float | None = None
    binding_checks: dict[str, str] = field(default_factory=dict)
    binding_check_last_run: dict[str, float] = field(default_factory=dict)
    adapter_health: dict[str, str] = field(default_factory=dict)
    inventory_labels: set[str] | None = None
    rotation: int | None = None

    def record(self, phase: str, outcome: str, error: BaseException | None = None) -> None:
        item = {"phase": phase, "outcome": outcome}
        if error is not None:
            item["error_type"] = type(error).__name__
            status = getattr(error, "status_code", None)
            if isinstance(status, int):
                item["http_status"] = str(status)
        self.logs.append(item)


class StateJournal:
    """Locked, fsynced intent for an owned in-flight probe. @spec TURN-CANARY-4"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: TextIO | None = None
        self.data: dict[str, Any] = {}

    def __enter__(self) -> "StateJournal":
        lock_path = self.path.with_name(self.path.name + ".lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        state = os.fdopen(descriptor, "r+", encoding="utf-8")
        try:
            fcntl.flock(state, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                raw = self.path.read_text(encoding="utf-8")
                if not raw.strip():
                    raise ValueError("existing canary state is empty")
                data = json.loads(raw)
            else:
                data = {}
            pending = data.get("pending") if isinstance(data, dict) else None
            routes = data.get("pending_routes", {}) if isinstance(data, dict) else None

            def valid_timestamp(value: Any) -> bool:
                return type(value) in (int, float) and math.isfinite(value) and value > 0

            def valid_record(record: Any) -> bool:
                return (
                    isinstance(record, dict)
                    and isinstance(record.get("agent_id"), str)
                    and isinstance(record.get("thread_key"), str)
                    and ("checked_at" not in record or valid_timestamp(record["checked_at"]))
                )

            if (
                not isinstance(data, dict)
                or type(data.get("capacity_skips_total", 0)) is not int
                or data.get("capacity_skips_total", 0) < 0
                or not isinstance(data.get("target_last_success", {}), dict)
                or not all(
                    isinstance(route, str) and valid_timestamp(timestamp)
                    for route, timestamp in data.get("target_last_success", {}).items()
                )
                or (
                    data.get("last_success") is not None
                    and not valid_timestamp(data["last_success"])
                )
                or type(data.get("cleanup_blocked", False)) is not bool
                or type(data.get("rotation", 0)) is not int
                or data.get("rotation", 0) < 0
                or (pending is not None and not valid_record(pending))
                or not isinstance(routes, dict)
                or not all(
                    isinstance(route, str) and valid_record(record)
                    for route, record in routes.items()
                )
            ):
                raise ValueError("invalid state fields")
            self.data = data
            self._file = state
            return self
        except BaseException:
            state.close()
            raise

    def __exit__(self, *_args: object) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    @property
    def pending(self) -> dict[str, Any] | None:
        """A single route-less record written by an earlier checker version."""
        return self.data.get("pending")

    @property
    def pending_routes(self) -> dict[str, dict[str, Any]]:
        """Owned cleanups still unconfirmed, by route label. @spec TURN-CANARY-4"""
        routes: dict[str, dict[str, Any]] = self.data.setdefault("pending_routes", {})
        return routes

    @property
    def holds_every_route(self) -> bool:
        """An operator hold or a legacy route-less record. @spec TURN-CANARY-4"""
        return self.pending is not None or self.data.get("cleanup_blocked") is True

    @property
    def rotation(self) -> int:
        """The persisted discovery walk position. @spec TURN-CANARY-7"""
        return int(self.data.get("rotation", 0))

    def _write(self) -> None:
        if self._file is None:
            raise RuntimeError("state journal is not locked")
        descriptor, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                output.write(json.dumps(self.data, sort_keys=True) + "\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def record_intent(self, agent_id: str, thread_key: str, *, route: str) -> None:
        if self.pending is not None or route in self.pending_routes:
            raise RuntimeError("previous owned cleanup remains pending")
        self.pending_routes[route] = {"agent_id": agent_id, "thread_key": thread_key}
        self._write()

    def clear_intent(self, agent_id: str, thread_key: str, *, route: str) -> None:
        record = self.pending_routes.get(route)
        if record is None or (record["agent_id"], record["thread_key"]) != (agent_id, thread_key):
            raise RuntimeError("cleanup does not match the recorded owned route")
        del self.pending_routes[route]
        self._write()

    def clear_legacy(self) -> None:
        """Release the all-route hold that came with a legacy record. @spec TURN-CANARY-4"""
        self.data["pending"] = None
        self.data["cleanup_blocked"] = False
        self._write()

    def mark_checked(self, route: str | None, when: float) -> None:
        record = self.pending if route is None else self.pending_routes.get(route)
        if record is not None:
            record["checked_at"] = when
            self._write()

    def save_result(self, result: CycleResult) -> None:
        result.capacity_skips += self.data.get("capacity_skips_total", 0)
        result.last_success = result.last_success or self.data.get("last_success")
        result.target_last_success = {
            **self.data.get("target_last_success", {}),
            **result.target_last_success,
        }
        self.data.update(
            {
                "capacity_skips_total": result.capacity_skips,
                "last_success": result.last_success,
                "target_last_success": result.target_last_success,
            }
        )
        if result.rotation is not None:
            self.data["rotation"] = result.rotation
        self._write()


class _CycleJournal(StateJournal):
    """In-memory holds for a cycle run without a state file. @spec TURN-CANARY-4"""

    def __init__(self) -> None:
        super().__init__(Path("unused"))

    def _write(self) -> None:
        pass


def select_targets(
    rows: list[dict[str, str]], selected: list[tuple[str, str, str]]
) -> list[dict[str, str]]:
    """Select exact live bindings, rejecting every ambiguity. @spec TURN-CANARY-1"""
    if not selected or len(selected) != len(set(selected)):
        raise ValueError("selected routes must be nonempty and unique")
    result = []
    for key in selected:
        if len(key) != 3 or key[0] != "slack" or not all(key):
            raise ValueError("selected route must name an exact Slack triple")
        matches = [
            row for row in rows if (row.get("kind"), row.get("address"), row.get("adapter")) == key
        ]
        if len(matches) != 1 or matches[0].get("deployment_status") != "active":
            raise ValueError("selected route has no unique active deployment")
        if not matches[0].get("agent_id"):
            raise ValueError("selected route has no agent identity")
        result.append(matches[0])
    return result


def discover_targets(
    rows: list[dict[str, str]],
    *,
    rotation: int,
    max_targets: int,
    exclude: Iterable[Route] = (),
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """One rotating Slack route per agent from live rows. @spec TURN-CANARY-7"""
    if type(rotation) is not int or rotation < 0 or not 0 < max_targets <= 100:
        raise ValueError("discovery needs a nonnegative rotation and bounded targets")
    excluded = {tuple(route) for route in exclude}
    logs: list[dict[str, str]] = []
    counts: dict[str, int] = {}
    for row in rows:
        counts[_route_label(row)] = counts.get(_route_label(row), 0) + 1
    groups: dict[str, list[dict[str, str]]] = {}
    for label in sorted(label for label, count in counts.items() if count > 1):
        if label.startswith("slack|"):
            logs.append({"phase": "discovery", "outcome": "ambiguous"})
    for row in rows:
        key = (row.get("kind"), row.get("address"), row.get("adapter"))
        if (
            row.get("kind") == "slack"
            and row.get("deployment_status") == "active"
            and all(row.get(name) for name in ("agent_id", "address", "adapter"))
            and counts[_route_label(row)] == 1
            and key not in excluded
        ):
            groups.setdefault(row["agent_id"], []).append(row)
    agents = sorted(groups)
    if not agents:
        return [], logs
    slots = min(max_targets, len(agents))
    targets = []
    for index in range(slots):
        slot = rotation * slots + index
        routes = sorted(
            groups[agents[slot % len(agents)]],
            key=lambda row: (row["kind"], row["address"], row["adapter"]),
        )
        targets.append(routes[(slot // len(agents)) % len(routes)])
    return targets, logs


def binding_check_outcome(
    row: Mapping[str, str],
    identities: list[dict[str, Any]] | None,
    *,
    identity_kinds: Sequence[str],
) -> str:
    """Model-free configuration check for one binding. @spec TURN-CANARY-8"""
    if row.get("deployment_status") != "active":
        return "deployment_inactive"
    if row.get("kind") not in identity_kinds:
        return "ok"
    if identities is None:
        return "read_failed"
    adapter = row.get("adapter") or ""
    matches = [item for item in identities if adapter and item.get("name") == adapter]
    if not matches:
        return "identity_missing"
    if len(matches) > 1:
        return "identity_ambiguous"
    identity = matches[0]
    if identity.get("status") != "active":
        return "identity_inactive"
    if identity.get("installation_mismatch") is not False:
        return "installation_mismatch"
    reference = identity.get("credential_ref")
    if not isinstance(reference, str) or not reference.strip():
        return "credential_missing"
    return "ok"


async def probe_adapter(client: Any, url: str) -> tuple[str, int | None]:
    """One unauthenticated GET without redirects. @spec TURN-CANARY-9"""
    import httpx

    try:
        response = await client.get(url, follow_redirects=False)
    except httpx.HTTPError:
        return "unreachable", None
    status = response.status_code
    return ("ok" if 200 <= status < 300 else "http_status"), status


async def _probe_adapters(
    platform: Platform, adapters: Mapping[str, str], result: CycleResult
) -> None:
    for name, url in sorted(adapters.items()):
        try:
            outcome, status = await platform.adapter_health(url)
        except Exception as exc:  # noqa: BLE001 - an adapter probe failure is a down adapter
            outcome, status = "unreachable", None
            result.record("adapter", "unreachable", exc)
        result.adapter_health[name] = outcome
        item = {"phase": "adapter", "adapter": name, "outcome": outcome}
        if status is not None:
            item["http_status"] = str(status)
        result.logs.append(item)


async def _check_bindings(
    platform: Platform,
    rows: list[dict[str, str]],
    turned: set[str],
    identity_kinds: Sequence[str],
    result: CycleResult,
) -> None:
    checked = [row for row in rows if _route_label(row) not in turned]
    identities: dict[str, list[dict[str, Any]] | None] = {}
    for kind in sorted({row.get("kind", "") for row in checked} & set(identity_kinds)):
        try:
            read = await platform.channel_identities(kind)
            if not isinstance(read, list) or not all(isinstance(item, dict) for item in read):
                raise ValueError("malformed channel identity read")
            identities[kind] = read
        except Exception as exc:  # noqa: BLE001 - a failed read is that kind's outcome
            identities[kind] = None
            result.record("binding_check", "read_failed", exc)
    for row in checked:
        label = _route_label(row)
        outcome = binding_check_outcome(
            row, identities.get(row.get("kind", "")), identity_kinds=identity_kinds
        )
        if result.binding_checks.get(label, "ok") == "ok":
            result.binding_checks[label] = outcome
        result.binding_check_last_run[label] = time.time()


def build_turn(
    binding: dict[str, str],
    *,
    nonce: str,
    conversation_id: str,
    reply_ref: str,
    event_id: str,
    received_at: str,
) -> QueuedTurn:
    """One owned, read-only request on the installed wire. @spec TURN-CANARY-2"""
    if not conversation_id.startswith("eval:") or not nonce:
        raise ValueError("canary requires an owned conversation and nonce")
    parsed = uuid.UUID(reply_ref)
    if parsed.version != 4 or str(parsed) != reply_ref:
        raise ValueError("relay reply ref must be canonical UUIDv4")
    turn = QueuedTurn(
        event_id=event_id,
        conversation_id=conversation_id,
        author="turn-canary",
        text=(
            "Reply with exactly this nonce and no other text: "
            + nonce
            + ". Do not use tools, take actions, request approval, or make durable changes."
        ),
        reply_handle=ReplyHandle(
            kind="slack",
            channel=binding["address"],
            placeholder=reply_ref,
            adapter="curie-cluster-message",
            identity=binding["adapter"],
        ),
        received_at=received_at,
        source=TurnSource.SLACK,
        attachments=[],
        hook_run=None,
        tool_access=ToolAccess.READ_ONLY,
    )
    return QueuedTurn.model_validate_json(turn.model_dump_json(), context=READER_CONTEXT)


def relay_outcome(events: list[dict[str, Any]], *, nonce: str) -> bool:
    """Require an exact answer and delivered terminal event. @spec TURN-CANARY-3"""
    if not isinstance(events, list) or any(
        not isinstance(item, dict)
        or item.get("event") not in {"reply.update", "turn.completed", "turn.status"}
        for item in events
    ):
        return False
    updates = [item for item in events if item.get("event") == "reply.update"]
    terminal = [item for item in events if item.get("event") == "turn.completed"]
    return (
        bool(updates)
        and updates[-1].get("text") == nonce
        and len(terminal) == 1
        and terminal[0].get("outcome") == "delivered"
    )


def scoped_reset_key(binding: dict[str, str], conversation_id: str) -> str:
    """Compute only the route this probe minted. @spec TURN-CANARY-4"""
    if not conversation_id.startswith("eval:"):
        raise ValueError("reset requires an owned canary conversation")
    return scoped_conversation_id(
        "slack", binding["address"], conversation_id, identity=binding["adapter"]
    )


def cleanup_confirmed(state: dict[str, Any]) -> bool:
    """Unknown and absent routes are never release proof. @spec TURN-CANARY-4"""
    return (
        isinstance(state, dict)
        and state.get("requested") is False
        and state.get("route_existed") is True
    )


async def _await_relay(
    platform: Platform, reply_ref: str, nonce: str, deadline: float, poll: float
) -> bool:
    cursor = 0
    events: list[dict[str, Any]] = []
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        page = await platform.replies(reply_ref, cursor)
        if not isinstance(page, dict) or not isinstance(page.get("events"), list):
            raise ValueError("malformed relay page")
        next_cursor = page.get("next_cursor")
        if not isinstance(next_cursor, int) or next_cursor < cursor:
            raise ValueError("nonmonotonic relay cursor")
        events.extend(page["events"])
        cursor = next_cursor
        if page.get("terminal") or any(
            item.get("event") == "turn.completed" for item in events if isinstance(item, dict)
        ):
            return relay_outcome(events, nonce=nonce)
        await asyncio.sleep(min(poll, max(0, end - time.monotonic())))
    return False


async def _await_reset(
    platform: Platform, agent_id: str, key: str, deadline: float, poll: float
) -> dict[str, Any] | None:
    """Poll until the reset drains; None while it is still pending. @spec TURN-CANARY-4"""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        state = await platform.reset_state(agent_id, key)
        if not isinstance(state, dict):
            return None
        if state.get("requested") is False:
            return state
        await asyncio.sleep(min(poll, max(0, end - time.monotonic())))
    return None


def _drained(state: dict[str, Any] | None) -> bool:
    """A drained reset that recorded an outcome, released or no route. @spec TURN-CANARY-4"""
    return (
        state is not None
        and state.get("requested") is False
        and state.get("route_existed") in (True, False)
    )


async def _recheck_pending(
    platform: Platform,
    journal: StateJournal,
    result: CycleResult,
    *,
    limit: int,
    cleanup_deadline: float,
    poll_period: float,
) -> None:
    """Re-request owned resets left unconfirmed by earlier cycles. @spec TURN-CANARY-4"""
    entries: list[tuple[str | None, dict[str, Any]]] = sorted(
        journal.pending_routes.items(),
        key=lambda item: (item[1].get("checked_at", 0), item[0]),
    )
    if journal.pending is not None:
        entries.insert(0, (None, journal.pending))
    for route, record in entries[:limit]:
        agent_id, key = record["agent_id"], record["thread_key"]
        try:
            await platform.reset(agent_id, key)
            state = await _await_reset(platform, agent_id, key, cleanup_deadline, poll_period)
        except Exception as exc:  # noqa: BLE001 - an unreadable re-check keeps the hold
            result.record("cleanup", "recheck_failed", exc)
            state = None
        if _drained(state):
            if route is None:
                journal.clear_legacy()
            else:
                journal.clear_intent(agent_id, key, route=route)
                result.target_cleanup_degraded[route] = False
            result.record("cleanup", "recovered")
        else:
            journal.mark_checked(route, time.time())
            result.record("cleanup", "still_unconfirmed")


async def run_cycle(
    platform: Platform,
    selected: list[tuple[str, str, str]] | None,
    *,
    journal: StateJournal | None = None,
    turn_deadline: float = 90.0,
    cleanup_deadline: float = 60.0,
    poll_period: float = 2.0,
    max_targets: int = 5,
    discover: bool = False,
    exclude: Iterable[Route] = (),
    binding_checks: bool = False,
    identity_kinds: Sequence[str] = ("slack",),
    adapters: Mapping[str, str] | None = None,
    conversation_prefix: str = CONVERSATION_PREFIX,
) -> CycleResult:
    """Probe serially; hold only routes with uncertain cleanup.

    @spec TURN-CANARY-1 TURN-CANARY-10
    """
    Limits(
        turn_deadline=turn_deadline,
        cleanup_deadline=cleanup_deadline,
        poll_period=poll_period,
        max_targets=max_targets,
    )
    _check_conversation_prefix(conversation_prefix)
    if discover == (selected is not None):
        raise ValueError("configure exactly one of explicit routes or discovery")
    if journal is None:
        journal = _CycleJournal()
    result = CycleResult()
    try:
        await _run_cycle(
            platform,
            selected,
            journal,
            result,
            turn_deadline=turn_deadline,
            cleanup_deadline=cleanup_deadline,
            poll_period=poll_period,
            max_targets=max_targets,
            exclude=exclude,
            binding_checks=binding_checks,
            identity_kinds=identity_kinds,
            adapters=adapters,
            conversation_prefix=conversation_prefix,
        )
    finally:
        for route in journal.pending_routes:
            result.target_cleanup_degraded[route] = True
        result.cleanup_degraded = (
            bool(journal.pending_routes)
            or journal.holds_every_route
            or getattr(platform, "cleanup_blocked", False) is True
        )
    if result.success:
        result.last_success = time.time()
    return result


async def _run_cycle(
    platform: Platform,
    selected: list[tuple[str, str, str]] | None,
    journal: StateJournal,
    result: CycleResult,
    *,
    turn_deadline: float,
    cleanup_deadline: float,
    poll_period: float,
    max_targets: int,
    exclude: Iterable[Route],
    binding_checks: bool,
    identity_kinds: Sequence[str],
    adapters: Mapping[str, str] | None,
    conversation_prefix: str,
) -> None:
    if adapters:
        await _probe_adapters(platform, adapters, result)
    if journal.pending is not None or journal.pending_routes:
        await _recheck_pending(
            platform,
            journal,
            result,
            limit=max_targets,
            cleanup_deadline=cleanup_deadline,
            poll_period=poll_period,
        )
    held = getattr(platform, "cleanup_blocked", False) is True or journal.holds_every_route
    if held:
        result.record("cleanup", "previous_unconfirmed")
        if not binding_checks:
            return
    if selected is not None and len(selected) > max_targets:
        raise ValueError("selected routes exceed maximum")
    try:
        rows = await platform.inventory()
        if not isinstance(rows, list):
            raise ValueError("malformed inventory")
    except Exception as exc:  # noqa: BLE001 - inventory failures fail the whole cycle
        result.record("inventory", "failed", exc)
        return
    result.inventory_labels = {_route_label(row) for row in rows}
    targets: list[dict[str, str]] = []
    if not held:
        try:
            if selected is None:
                rotation = journal.rotation
                targets, discovery_logs = discover_targets(
                    rows, rotation=rotation, max_targets=max_targets, exclude=exclude
                )
                result.logs.extend(discovery_logs)
                if targets:
                    result.rotation = rotation + 1
                else:
                    result.record("discovery", "empty")
            else:
                targets = select_targets(rows, selected)
            if targets and not await platform.read_only_supported():
                result.record("admission", "read_only_unavailable")
                targets = []
        except Exception as exc:  # noqa: BLE001 - inventory failures fail the whole cycle
            result.record("inventory", "failed", exc)
            targets = []
    if targets:
        await _turn_targets(
            platform,
            targets,
            result,
            journal,
            turn_deadline,
            cleanup_deadline,
            poll_period,
            conversation_prefix,
        )
    if binding_checks:
        await _check_bindings(platform, rows, set(result.target_last_run), identity_kinds, result)


async def _turn_targets(
    platform: Platform,
    targets: list[dict[str, str]],
    result: CycleResult,
    journal: StateJournal,
    turn_deadline: float,
    cleanup_deadline: float,
    poll_period: float,
    conversation_prefix: str = CONVERSATION_PREFIX,
) -> None:
    """The owned, serial turn path of TURN-CANARY-2 to 4. @spec TURN-CANARY-4"""
    all_ok = True
    attempted = False
    for binding in targets:
        target_label = _route_label(binding)
        if target_label in journal.pending_routes:
            result.record("cleanup", "route_held")
            continue
        attempted = True
        result.target_last_run[target_label] = time.time()
        try:
            headroom = await platform.quota_headroom()
        except Exception as exc:  # noqa: BLE001 - unreadable quota skips enqueue
            result.record("capacity", "unreadable", exc)
            headroom = None
        if headroom is not True:
            result.capacity_skips += 1
            result.target_capacity_skipped[target_label] = True
            result.record("capacity", "skipped")
            continue
        result.target_capacity_skipped[target_label] = False
        conversation_id = conversation_prefix + str(uuid.uuid4())
        reply_ref = str(uuid.uuid4())
        nonce = secrets.token_urlsafe(24)
        turn = build_turn(
            binding,
            nonce=nonce,
            conversation_id=conversation_id,
            reply_ref=reply_ref,
            event_id="EvCANARY-" + uuid.uuid4().hex,
            received_at=datetime.now(UTC).isoformat(),
        )
        key = scoped_reset_key(binding, conversation_id)
        journal.record_intent(binding["agent_id"], key, route=target_label)
        enqueued = False
        try:
            await platform.enqueue(turn)
            enqueued = True
        except Exception as exc:  # noqa: BLE001 - uncertain XADD still requires cleanup
            result.record("enqueue", "failed", exc)
            result.target_success[target_label] = False
            all_ok = False
        if enqueued:
            try:
                result.target_success[target_label] = await _await_relay(
                    platform, reply_ref, nonce, turn_deadline, poll_period
                )
                result.record(
                    "turn", "delivered" if result.target_success[target_label] else "failed"
                )
            except Exception as exc:  # noqa: BLE001 - reset is still owed
                result.target_success[target_label] = False
                result.record("relay", "failed", exc)
            all_ok = all_ok and result.target_success[target_label]
            if result.target_success[target_label]:
                result.target_last_success[target_label] = time.time()
        try:
            await platform.reset(binding["agent_id"], key)
            state = await _await_reset(
                platform, binding["agent_id"], key, cleanup_deadline, poll_period
            )
            confirmed = state is not None and cleanup_confirmed(state)
            if confirmed:
                journal.clear_intent(binding["agent_id"], key, route=target_label)
        except Exception as exc:  # noqa: BLE001 - unknown reset state keeps the route held
            result.record("cleanup", "failed", exc)
            confirmed = False
        if confirmed:
            result.target_cleanup_degraded[target_label] = False
        else:
            all_ok = False
            result.record("cleanup", "unconfirmed")
    if not attempted:
        result.record("cleanup", "every_route_held")
    result.success = attempted and all_ok


class RuntimePlatform:
    """Explicit installation connections; no credentials are logged.

    @spec TURN-CANARY-1 TURN-CANARY-6
    """

    def __init__(
        self,
        *,
        database_url: str,
        valkey_url: str,
        api_url: str,
        api_key: str,
        stream: str,
        schema: str,
        read_only_qualified: bool,
        quota_url: str | None = None,
        quota_token: str | None = None,
        quota_resource: str = "count/pods",
        quota_min_free: int | str | Decimal = 1,
    ) -> None:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
            raise ValueError("invalid database schema")
        parsed = urlsplit(api_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("API URL must be an HTTP(S) origin without query or fragment")
        if not all((database_url, valkey_url, api_key, stream)):
            raise ValueError("database, Valkey, API key, and stream must be explicit")
        min_free = (
            quota_min_free
            if isinstance(quota_min_free, Decimal)
            else parse_quantity(quota_min_free)
        )
        if quota_url and (not quota_token or min_free <= 0):
            raise ValueError("quota check requires a token and positive minimum free capacity")
        import httpx
        import redis.asyncio as redis
        from sqlalchemy.ext.asyncio import create_async_engine

        self._engine = create_async_engine(database_url)
        self._redis = redis.from_url(valkey_url)
        self._http = httpx.AsyncClient(
            base_url=api_url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=10.0,
            follow_redirects=False,
        )
        self._quota_http = httpx.AsyncClient(timeout=10.0, follow_redirects=False)
        self._adapter_http = httpx.AsyncClient(timeout=5.0, follow_redirects=False)
        self._stream = stream
        self._schema = schema
        self._qualified = read_only_qualified
        self._quota_url = quota_url
        self._quota_token = quota_token
        self._quota_resource = quota_resource
        self._quota_min_free = min_free
        self.cleanup_blocked = False

    async def close(self) -> None:
        await self._adapter_http.aclose()
        await self._quota_http.aclose()
        await self._http.aclose()
        await self._redis.aclose()
        await self._engine.dispose()

    async def inventory(self) -> list[dict[str, str]]:
        """Read only current bindings with active deployments. @spec TURN-CANARY-1"""
        from sqlalchemy import text

        sql = text(f"""
            SELECT a.id AS agent_id, a.name AS agent, c.kind, c.address, c.adapter,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM {self._schema}.deployments d
                       WHERE d.agent_id = a.id AND d.status = 'active'
                   ) THEN 'active' ELSE 'inactive' END AS deployment_status
            FROM {self._schema}.agents a
            JOIN {self._schema}.agent_channels c ON c.agent_id = a.id
        """)
        async with self._engine.connect() as connection:
            rows = (await connection.execute(sql)).mappings().all()
        return [
            {key: str(value) if value is not None else "" for key, value in row.items()}
            for row in rows
        ]

    async def read_only_supported(self) -> bool:
        """Require operator qualification of the deployed fail-closed worker. @spec TURN-CANARY-2"""
        return self._qualified

    async def quota_headroom(self) -> bool | None:
        """Optional best-effort ResourceQuota observation. @spec TURN-CANARY-5"""
        if self._quota_url is None:
            return True
        try:
            response = await self._quota_http.get(
                self._quota_url, headers={"Authorization": "Bearer " + str(self._quota_token)}
            )
            if response.status_code != 200:
                return None
            body = response.json()
            items = body.get("items")
            if not isinstance(items, list) or not items:
                return None
            for item in items:
                status = item["status"]
                hard = parse_quantity(status["hard"][self._quota_resource])
                used = parse_quantity(status["used"][self._quota_resource])
                if hard - used < self._quota_min_free:
                    return False
            return True
        except (KeyError, TypeError, ValueError, AttributeError):
            return None

    async def enqueue(self, turn: QueuedTurn) -> None:
        """Append exactly one complete queued wire payload. @spec TURN-CANARY-2"""
        from aci_protocol import STREAM_PAYLOAD_FIELD

        wire = QueuedTurn.model_validate_json(turn.model_dump_json(), context=READER_CONTEXT)
        await self._redis.xadd(self._stream, {STREAM_PAYLOAD_FIELD: wire.model_dump_json()})

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        """Convert HTTP errors without URL, body, or credential text. @spec TURN-CANARY-6"""
        response = await self._http.request(method, path, **kwargs)
        if response.status_code >= 400:
            raise PlatformHttpError(response.status_code)
        return response.json()

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        payload = await self._request_json(method, path, **kwargs)
        if not isinstance(payload, dict):
            raise ValueError("platform response is not an object")
        return payload

    async def channel_identities(self, kind: str) -> list[dict[str, Any]]:
        """Read identity rows; never resolve a referenced secret. @spec TURN-CANARY-8"""
        payload = await self._request_json("GET", "/channel-identities", params={"provider": kind})
        if not isinstance(payload, list):
            raise ValueError("channel identity read is not a list")
        return payload

    async def adapter_health(self, url: str) -> tuple[str, int | None]:
        """Unauthenticated adapter health GET. @spec TURN-CANARY-9"""
        return await probe_adapter(self._adapter_http, url)

    async def replies(self, reply_ref: str, after: int) -> dict[str, Any]:
        try:
            return await self._request(
                "GET", f"/cluster-message-replies/{reply_ref}", params={"after": after}
            )
        except PlatformHttpError as exc:
            # The bucket is created by the first relay event, after enqueue.
            if exc.status_code == 404:
                return {"events": [], "next_cursor": after, "terminal": False}
            raise

    async def reset(self, agent_id: str, thread_key: str) -> None:
        from urllib.parse import quote

        await self._request(
            "POST", f"/agents/{quote(agent_id, safe='')}/threads/{quote(thread_key, safe='')}/reset"
        )

    async def reset_state(self, agent_id: str, thread_key: str) -> dict[str, Any]:
        from urllib.parse import quote

        return await self._request(
            "GET", f"/agents/{quote(agent_id, safe='')}/threads/{quote(thread_key, safe='')}/reset"
        )


class PlatformHttpError(RuntimeError):
    """An HTTP status without a URL or response body. @spec TURN-CANARY-6"""

    def __init__(self, status_code: int) -> None:
        super().__init__("platform HTTP failure")
        self.status_code = status_code


def _labels(**values: str) -> str:
    return ",".join(f"{name}={json.dumps(value)}" for name, value in values.items())


def _route_value(route: str) -> str:
    kind, address, adapter = route.split("|", 2)
    return json.dumps({"kind": kind, "address": address, "adapter": adapter}, sort_keys=True)


def prometheus_text(result: CycleResult, prefix: str = "turn_canary") -> str:
    """Portable bounded exposition of the last cycle. @spec TURN-CANARY-6 TURN-CANARY-10"""
    _check_prefix(prefix)

    def line(name: str, value: int | float, labels: str = "") -> str:
        return f"{prefix}_{name}{{{labels}}} {value}\n" if labels else f"{prefix}_{name} {value}\n"

    output = [
        line("cycle_success", int(result.success)),
        line("last_cycle_timestamp_seconds", result.last_run),
        line("cleanup_degraded", int(result.cleanup_degraded)),
        line("capacity_skips_total", result.capacity_skips),
    ]
    if result.last_success is not None:
        output.append(line("last_success_timestamp_seconds", result.last_success))
    if result.rotation is not None:
        output.append(line("rotation", result.rotation))
    for route, last_run in sorted(result.target_last_run.items()):
        label = _labels(route=_route_value(route))
        if route in result.target_success:
            output.append(line("target_success", int(result.target_success[route]), label))
        output.append(line("target_last_run_timestamp_seconds", last_run, label))
        if route in result.target_last_success:
            output.append(
                line(
                    "target_last_success_timestamp_seconds",
                    result.target_last_success[route],
                    label,
                )
            )
    for route, skipped in sorted(result.target_capacity_skipped.items()):
        label = _labels(route=_route_value(route))
        output.append(line("target_capacity_skipped", int(skipped), label))
    for route, degraded in sorted(result.target_cleanup_degraded.items()):
        label = _labels(route=_route_value(route))
        output.append(line("target_cleanup_degraded", int(degraded), label))
    for route, outcome in sorted(result.binding_checks.items()):
        label = _labels(route=_route_value(route), outcome=outcome)
        output.append(line("binding_check_success", int(outcome == "ok"), label))
        if route in result.binding_check_last_run:
            output.append(
                line(
                    "binding_check_last_run_timestamp_seconds",
                    result.binding_check_last_run[route],
                    _labels(route=_route_value(route)),
                )
            )
    for adapter, outcome in sorted(result.adapter_health.items()):
        output.append(
            line("adapter_up", int(outcome == "ok"), _labels(adapter=adapter, outcome=outcome))
        )
    return "".join(output)


class Monitor:
    """Non-overlapping cycle loop and merged last observation. @spec TURN-CANARY-10"""

    def __init__(
        self,
        run_once: Callable[[], Awaitable[CycleResult]] | None,
        limits: Limits,
        *,
        prefix: str = "turn_canary",
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[dict[str, str]], None] | None = None,
    ) -> None:
        self._run_once = run_once
        self._limits = limits
        self._prefix = _check_prefix(prefix)
        self._clock = clock
        self._log = log
        self._started = clock()
        self._last_finished: float | None = None
        self._view: CycleResult | None = None
        self.overruns = 0

    @property
    def stale_after(self) -> float:
        limits = self._limits
        cycle = limits.max_targets * (limits.turn_deadline + 2 * limits.cleanup_deadline)
        return 2 * limits.cycle_interval + cycle

    def healthy(self) -> bool:
        reference = self._last_finished if self._last_finished is not None else self._started
        return self._clock() - reference <= self.stale_after

    def observe(self, result: CycleResult) -> None:
        """Merge one cycle, keeping rotated routes until inventory drops them."""
        view = self._view or CycleResult()
        view.success = result.success
        view.cleanup_degraded = result.cleanup_degraded
        view.capacity_skips = result.capacity_skips
        view.last_run = result.last_run
        view.last_success = result.last_success or view.last_success
        view.rotation = result.rotation if result.rotation is not None else view.rotation
        view.adapter_health = dict(result.adapter_health)
        per_route = (
            "target_success",
            "target_last_run",
            "target_last_success",
            "target_capacity_skipped",
            "target_cleanup_degraded",
            "binding_checks",
            "binding_check_last_run",
        )
        for name in per_route:
            getattr(view, name).update(getattr(result, name))
        for route in result.target_last_run:
            view.binding_checks.pop(route, None)
            view.binding_check_last_run.pop(route, None)
        if result.inventory_labels is not None:
            for name in per_route:
                values = getattr(view, name)
                for route in [route for route in values if route not in result.inventory_labels]:
                    if name == "target_cleanup_degraded" and values[route]:
                        continue  # a pending cleanup stays visible after its binding goes
                    del values[route]
        self._view = view
        self._last_finished = self._clock()

    def metrics_text(self) -> str:
        body = prometheus_text(self._view, self._prefix) if self._view is not None else ""
        return body + f"{self._prefix}_cycle_overruns_total {self.overruns}\n"

    async def run_forever(self, stop: asyncio.Event) -> None:
        """One cycle at a time, at most once per interval. @spec TURN-CANARY-10"""
        if self._run_once is None:
            raise ValueError("monitor has no cycle to run")
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            started = loop.time()
            try:
                result = await self._run_once()
            except Exception as exc:  # noqa: BLE001 - a failed cycle is an observation
                result = CycleResult()
                result.record("cycle", "failed", exc)
            self.observe(result)
            if self._log is not None:
                for item in result.logs:
                    self._log(item)
            remaining = self._limits.cycle_interval - (loop.time() - started)
            if remaining <= 0:
                self.overruns += 1
                continue
            try:
                await asyncio.wait_for(stop.wait(), remaining)
            except TimeoutError:
                pass


HTTP_HEAD_LIMIT = 8192
HTTP_READ_TIMEOUT = 5.0


async def serve_http(monitor: Monitor, host: str, port: int) -> asyncio.Server:
    """Serve only GET /metrics and GET /healthz. @spec TURN-CANARY-10"""

    async def respond(writer: asyncio.StreamWriter, status: str, body: str, kind: str) -> None:
        payload = body.encode()
        writer.write(
            (
                f"HTTP/1.1 {status}\r\nContent-Type: {kind}\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
            ).encode()
            + payload
        )
        await writer.drain()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), HTTP_READ_TIMEOUT)
            parts = head.split(b"\r\n", 1)[0].decode("ascii").split(" ")
            if len(parts) != 3:
                return
            method, target, _version = parts
            path = target.split("?", 1)[0]
            plain = "text/plain; charset=utf-8"
            if path not in {"/metrics", "/healthz"}:
                await respond(writer, "404 Not Found", "not found\n", plain)
            elif method != "GET":
                await respond(writer, "405 Method Not Allowed", "method not allowed\n", plain)
            elif path == "/metrics":
                await respond(
                    writer,
                    "200 OK",
                    monitor.metrics_text(),
                    "text/plain; version=0.0.4; charset=utf-8",
                )
            elif monitor.healthy():
                await respond(writer, "200 OK", "ok\n", plain)
            else:
                await respond(writer, "503 Service Unavailable", "stale\n", plain)
        except (
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
            TimeoutError,
            UnicodeDecodeError,
            ConnectionError,
        ):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(handle, host, port, limit=HTTP_HEAD_LIMIT)


@dataclass(frozen=True)
class Config:
    """Validated environment configuration. @spec TURN-CANARY-7 TURN-CANARY-10"""

    database_url: str
    valkey_url: str
    api_url: str
    api_key: str
    stream: str
    schema: str
    state_path: str
    read_only_qualified: bool
    limits: Limits
    routes: list[Route] | None
    discover: bool
    exclude: list[Route]
    binding_checks: bool
    identity_kinds: tuple[str, ...]
    adapters: dict[str, str]
    listen: tuple[str, int] | None
    metric_prefix: str
    metrics_path: str | None
    quota_url: str | None
    quota_token: str | None
    quota_resource: str
    quota_min_free: Decimal
    conversation_prefix: str = CONVERSATION_PREFIX


def _routes(raw: str, name: str) -> list[Route]:
    value = json.loads(raw)
    if not isinstance(value, list) or not all(
        isinstance(item, list) and len(item) == 3 and all(isinstance(part, str) for part in item)
        for item in value
    ):
        raise ValueError(f"{name} must be a list of route triples")
    return [(item[0], item[1], item[2]) for item in value]


def _adapters(raw: str) -> dict[str, str]:
    value = json.loads(raw)
    if not isinstance(value, dict) or len(value) > MAX_ADAPTERS:
        raise ValueError("adapter health must be an object of at most 20 adapters")
    for name, url in value.items():
        if not isinstance(name, str) or not ADAPTER_NAME.fullmatch(name):
            raise ValueError("adapter health names must be short lowercase names")
        parsed = urlsplit(url) if isinstance(url, str) else None
        if (
            parsed is None
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("adapter health URLs must be HTTP(S) without user information")
    return dict(value)


def _listen(raw: str) -> tuple[str, int]:
    host, separator, port = raw.rpartition(":")
    host = host.strip("[]")
    if not separator or not host or not port.isdigit() or not 0 < int(port) <= 65535:
        raise ValueError("TURN_CANARY_LISTEN must be host:port")
    return host, int(port)


def parse_config(environ: Mapping[str, str]) -> Config:
    """Read explicit installation settings; nothing is defaulted from a tenant.

    @spec TURN-CANARY-7 TURN-CANARY-10
    """
    required = (
        "TURN_CANARY_DATABASE_URL",
        "TURN_CANARY_VALKEY_URL",
        "TURN_CANARY_API_URL",
        "TURN_CANARY_API_KEY",
        "TURN_CANARY_STREAM",
    )
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise ValueError("missing required canary configuration: " + ", ".join(missing))
    discover = environ.get("TURN_CANARY_DISCOVER") == "1"
    raw_routes = environ.get("TURN_CANARY_ROUTES")
    if discover == bool(raw_routes):
        raise ValueError("set exactly one of TURN_CANARY_ROUTES or TURN_CANARY_DISCOVER=1")
    state_path = environ.get("TURN_CANARY_STATE_PATH")
    if not state_path:
        raise ValueError("TURN_CANARY_STATE_PATH is required to retain cleanup hold across runs")
    identity_kinds = json.loads(environ.get("TURN_CANARY_IDENTITY_KINDS", '["slack"]'))
    if not isinstance(identity_kinds, list) or not all(
        isinstance(kind, str) and kind for kind in identity_kinds
    ):
        raise ValueError("TURN_CANARY_IDENTITY_KINDS must be a list of binding kinds")
    listen = environ.get("TURN_CANARY_LISTEN")
    return Config(
        database_url=environ["TURN_CANARY_DATABASE_URL"],
        valkey_url=environ["TURN_CANARY_VALKEY_URL"],
        api_url=environ["TURN_CANARY_API_URL"],
        api_key=environ["TURN_CANARY_API_KEY"],
        stream=environ["TURN_CANARY_STREAM"],
        schema=environ.get("TURN_CANARY_DB_SCHEMA", "curie"),
        state_path=state_path,
        read_only_qualified=environ.get("TURN_CANARY_READ_ONLY_QUALIFIED") == "1",
        limits=Limits(
            cycle_interval=float(environ.get("TURN_CANARY_CYCLE_INTERVAL", "300")),
            turn_deadline=float(environ.get("TURN_CANARY_TURN_DEADLINE", "90")),
            cleanup_deadline=float(environ.get("TURN_CANARY_CLEANUP_DEADLINE", "60")),
            poll_period=float(environ.get("TURN_CANARY_POLL_PERIOD", "2")),
            max_targets=int(environ.get("TURN_CANARY_MAX_TARGETS", "5")),
        ),
        routes=_routes(raw_routes, "TURN_CANARY_ROUTES") if raw_routes else None,
        discover=discover,
        exclude=_routes(environ.get("TURN_CANARY_DISCOVER_EXCLUDE", "[]"), "exclusions"),
        binding_checks=environ.get("TURN_CANARY_BINDING_CHECKS") == "1",
        identity_kinds=tuple(identity_kinds),
        adapters=_adapters(environ.get("TURN_CANARY_ADAPTER_HEALTH", "{}")),
        listen=_listen(listen) if listen else None,
        metric_prefix=_check_prefix(environ.get("TURN_CANARY_METRIC_PREFIX", "turn_canary")),
        metrics_path=environ.get("TURN_CANARY_METRICS_PATH") or None,
        quota_url=environ.get("TURN_CANARY_QUOTA_URL"),
        quota_token=environ.get("TURN_CANARY_QUOTA_TOKEN"),
        quota_resource=environ.get("TURN_CANARY_QUOTA_RESOURCE", "count/pods"),
        quota_min_free=parse_quantity(environ.get("TURN_CANARY_QUOTA_MIN_FREE", "1")),
        conversation_prefix=_check_conversation_prefix(
            environ.get("TURN_CANARY_CONVERSATION_PREFIX", CONVERSATION_PREFIX)
        ),
    )


def _print_log(item: dict[str, str]) -> None:
    print(json.dumps(item, sort_keys=True), flush=True)


async def main() -> int:
    """Run one cycle, or the exporter, from explicit settings. @spec TURN-CANARY-1 TURN-CANARY-10"""
    config = parse_config(os.environ)
    limits = config.limits
    platform = RuntimePlatform(
        database_url=config.database_url,
        valkey_url=config.valkey_url,
        api_url=config.api_url,
        api_key=config.api_key,
        stream=config.stream,
        schema=config.schema,
        read_only_qualified=config.read_only_qualified,
        quota_url=config.quota_url,
        quota_token=config.quota_token,
        quota_resource=config.quota_resource,
        quota_min_free=config.quota_min_free,
    )

    async def one_cycle() -> CycleResult:
        with StateJournal(Path(config.state_path)) as journal:
            result = await run_cycle(
                platform,
                config.routes,
                journal=journal,
                turn_deadline=limits.turn_deadline,
                cleanup_deadline=limits.cleanup_deadline,
                poll_period=limits.poll_period,
                max_targets=limits.max_targets,
                discover=config.discover,
                exclude=config.exclude,
                binding_checks=config.binding_checks,
                identity_kinds=config.identity_kinds,
                adapters=config.adapters,
                conversation_prefix=config.conversation_prefix,
            )
            journal.save_result(result)
        return result

    try:
        if config.listen is not None:
            import signal

            monitor = Monitor(one_cycle, limits, prefix=config.metric_prefix, log=_print_log)
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for signum in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(signum, stop.set)
            server = await serve_http(monitor, *config.listen)
            try:
                await monitor.run_forever(stop)
            finally:
                server.close()
                await server.wait_closed()
            return 0
        result = await one_cycle()
    finally:
        await platform.close()
    if config.metrics_path:
        Path(config.metrics_path).write_text(prometheus_text(result, config.metric_prefix))
    for item in result.logs:
        _print_log(item)
    return 0 if result.success else 1


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except Exception as exc:  # noqa: BLE001 - never print a transport exception's raw URL
        print(
            json.dumps({"phase": "startup", "outcome": "failed", "error_type": type(exc).__name__})
        )
        raise SystemExit(1) from None
