"""Opt-in, bounded synthetic turn checker. @spec TURN-CANARY-1 TURN-CANARY-17"""

import asyncio
import fcntl
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import sys
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, Protocol, TextIO
from urllib.parse import urlsplit

from aci_protocol import READER_CONTEXT, QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.events import ToolAccess
from channel_protocol import (
    ReplyEvent,
    ReplyTarget,
    ReplyUpdate,
    TurnCompleted,
    scoped_conversation_id,
)
from pydantic import TypeAdapter, ValidationError


def _load_probe_profiles_module() -> ModuleType:
    """Reuse only this exact shipped sibling, without search paths. @spec TURN-CANARY-17"""
    sibling = Path(__file__).with_name("probe_profiles.py").resolve(strict=True)
    for candidate in tuple(sys.modules.values()):
        if not isinstance(candidate, ModuleType):
            continue
        origin = candidate.__dict__.get("__file__")
        if isinstance(origin, str) and Path(origin).resolve() == sibling:
            sys.modules.setdefault("probe_profiles", candidate)
            return candidate
    name = "_curie_turn_canary_profiles_" + hashlib.sha256(str(sibling).encode()).hexdigest()
    if name in sys.modules:
        raise ImportError("turn canary profile module cache key is occupied")
    spec = importlib.util.spec_from_file_location(name, sibling)
    if spec is None or spec.loader is None:
        raise ImportError("turn canary profile sibling is not loadable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        if sys.modules.get(name) is module:
            del sys.modules[name]
        raise
    sys.modules.setdefault("probe_profiles", module)
    return module


if TYPE_CHECKING:
    from probe_profiles import (
        ProbeProfile,
        ProbeReason,
        parse_probe_profiles,
        validate_probe_profiles,
    )
else:
    _profiles = _load_probe_profiles_module()
    ProbeProfile = _profiles.ProbeProfile
    ProbeReason = _profiles.ProbeReason
    parse_probe_profiles = _profiles.parse_probe_profiles
    validate_probe_profiles = _profiles.validate_probe_profiles

_check_probe_profiles = validate_probe_profiles


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
MAX_BINDING_HEALTH = 100
ROUTE_LABEL_LAYOUTS = ("route", "split")


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


def _exclude_kinds(raw: str) -> tuple[str, ...]:
    """Distinct nonblank binding kinds, comma-separated. @spec TURN-CANARY-15"""
    kinds = tuple(kind.strip() for kind in raw.split(","))
    if not all(kinds) or len(set(kinds)) != len(kinds):
        raise ValueError("TURN_CANARY_EXCLUDE_BINDING_KINDS must list distinct binding kinds")
    return kinds


def _check_conversation_prefix(prefix: str) -> str:
    if not CONVERSATION_PREFIX_PATTERN.fullmatch(prefix):
        raise ValueError("TURN_CANARY_CONVERSATION_PREFIX must match eval:[A-Za-z0-9._-]{0,48}")
    return prefix


def _check_prefix(prefix: str) -> str:
    if not METRIC_PREFIX.fullmatch(prefix):
        raise ValueError("metric prefix must be a valid Prometheus metric name")
    return prefix


PROBE_SAFETY_SUFFIX = ". Do not use tools, take actions, request approval, or make durable changes."


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
    """Safe, bounded observations. @spec TURN-CANARY-5 TURN-CANARY-6 TURN-CANARY-16"""

    success: bool = False
    cleanup_degraded: bool = False
    capacity_skips: int = 0
    target_success: dict[str, bool] = field(default_factory=dict)
    target_delivery_success: dict[str, bool] = field(default_factory=dict)
    target_probe_success: dict[str, bool] = field(default_factory=dict)
    target_probe_profile: dict[str, bool] = field(default_factory=dict)
    target_last_delivery_success: dict[str, float] = field(default_factory=dict)
    target_last_probe_success: dict[str, float] = field(default_factory=dict)
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
    drift: dict[str, str] | None = None
    binding_adapter_health: dict[str, str] = field(default_factory=dict)
    uncovered_agents: list[str] | None = None
    route_agents: dict[str, str] = field(default_factory=dict)

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


def _turn_candidates(
    rows: list[dict[str, str]], excluded: set[tuple[str, ...]]
) -> list[dict[str, str]]:
    """Rows TURN-CANARY-7 accepts as turn candidates. @spec TURN-CANARY-7 TURN-CANARY-13"""
    counts: dict[str, int] = {}
    for row in rows:
        counts[_route_label(row)] = counts.get(_route_label(row), 0) + 1
    return [
        row
        for row in rows
        if row.get("kind") == "slack"
        and row.get("deployment_status") == "active"
        and all(row.get(name) for name in ("agent_id", "address", "adapter"))
        and counts[_route_label(row)] == 1
        and (row.get("kind"), row.get("address"), row.get("adapter")) not in excluded
    ]


def binding_drift(rows: list[dict[str, str]], declared: Sequence[Route]) -> dict[str, str]:
    """Declared against live triples, for declared kinds only. @spec TURN-CANARY-11"""
    wanted = {tuple(route) for route in declared}
    kinds = {route[0] for route in wanted}
    live = {
        (row.get("kind", ""), row.get("address", ""), row.get("adapter", ""))
        for row in rows
        if row.get("kind") in kinds
    }
    drift = {"|".join(route): "declared_only" for route in sorted(wanted - live)}
    drift.update({"|".join(route): "live_only" for route in sorted(live - wanted)})
    return drift


def binding_health_url(endpoint: str, path: str) -> str | None:
    """The endpoint origin plus the health path, or None when unusable. @spec TURN-CANARY-12"""
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def uncovered_agents(
    rows: list[dict[str, str]], *, exclude: Iterable[Route], health_path: str | None
) -> list[str]:
    """Active agents with no turn candidate or binding health URL. @spec TURN-CANARY-13"""
    active = {
        row["agent_id"]: row.get("agent", "")
        for row in rows
        if row.get("deployment_status") == "active" and row.get("agent_id")
    }
    covered = {row["agent_id"] for row in _turn_candidates(rows, {tuple(r) for r in exclude})}
    if health_path is not None:
        covered |= {
            row["agent_id"]
            for row in rows
            if row.get("deployment_status") == "active"
            and row.get("agent_id")
            and row.get("endpoint")
            and binding_health_url(row["endpoint"], health_path) is not None
        }
    return sorted({name for agent_id, name in active.items() if agent_id not in covered})


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
    for row in _turn_candidates(rows, excluded):
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


async def _probe_binding_endpoints(
    platform: Platform, rows: list[dict[str, str]], path: str, result: CycleResult
) -> None:
    """One GET per distinct derived URL; URLs stay out of output. @spec TURN-CANARY-12"""
    urls: dict[str, list[str]] = {}
    outcomes: dict[str, str] = {}
    for row in rows:
        endpoint = row.get("endpoint") or ""
        if row.get("deployment_status") != "active" or not endpoint:
            continue
        label = _route_label(row)
        url = binding_health_url(endpoint, path)
        if url is None:
            outcomes.setdefault(label, "endpoint_invalid")
            continue
        urls.setdefault(url, []).append(label)
    ordered = sorted(urls)
    if len(ordered) > MAX_BINDING_HEALTH:
        result.record("binding_adapter", "limit")
    for url in ordered[:MAX_BINDING_HEALTH]:
        try:
            outcome, status = await platform.adapter_health(url)
        except Exception as exc:  # noqa: BLE001 - a probe failure is a down adapter
            outcome, status = "unreachable", None
            result.record("binding_adapter", "unreachable", exc)
        item = {"phase": "binding_adapter", "outcome": outcome}
        if status is not None:
            item["http_status"] = str(status)
        result.logs.append(item)
        for label in urls[url]:
            if outcomes.get(label, "ok") == "ok":
                outcomes[label] = outcome
    result.binding_adapter_health.update(outcomes)


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
    profile: ProbeProfile | None = None,
) -> QueuedTurn:
    """One owned, read-only request on the installed wire. @spec TURN-CANARY-2 TURN-CANARY-16"""
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
            (
                profile.prompt if profile is not None
                else "Reply with exactly this nonce and no other text: " + nonce
            )
            + PROBE_SAFETY_SUFFIX
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


RelayReason = Literal[
    "exact_nonce", "nonce_mismatch", "missing_reply", "blank_reply",
    "missing_completion", "duplicate_completion", "not_delivered",
    "malformed_event", "wrong_target", "wrong_event_id",
    "malformed_page", "terminal_mismatch", "timeout",
]
_REPLY_EVENT: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)


@dataclass(frozen=True)
class RelayAssessment:
    """Independent lifecycle and probe evidence. @spec TURN-CANARY-3 TURN-CANARY-16"""

    exact_nonce: bool = False
    delivery_success: bool = False
    reason: RelayReason = "missing_completion"
    probe_success: bool = False
    probe_reason: ProbeReason | None = None


def _relay_failure(
    reason: RelayReason, profile: ProbeProfile | None, *, exact_nonce: bool = False,
) -> RelayAssessment:
    """Keep profile failure separate from legacy lifecycle reasons. @spec TURN-CANARY-16"""
    return RelayAssessment(
        exact_nonce=exact_nonce, reason=reason,
        probe_success=exact_nonce if profile is None else False,
        probe_reason="profile_mismatch" if profile is not None else None,
    )


def assess_relay(
    events: list[dict[str, Any]], *, nonce: str, turn: QueuedTurn | None = None,
    profile: ProbeProfile | None = None,
) -> RelayAssessment:
    """Validate every observed event; the runtime always supplies its turn.

    Without a turn, the Boolean compatibility helper can check only internal
    target consistency, not correlation to an enqueued request.
    @spec TURN-CANARY-3 TURN-CANARY-16
    """
    if profile is not None and turn is None:
        return _relay_failure("wrong_target", profile)
    if not isinstance(events, list):
        return _relay_failure("malformed_event", profile)
    try:
        parsed = [_REPLY_EVENT.validate_python(item) for item in events]
    except ValidationError:
        return _relay_failure("malformed_event", profile)
    expected = None
    if turn is not None:
        handle = turn.reply_handle
        if handle is None:
            return _relay_failure("wrong_target", profile)
        expected = ReplyTarget(
            kind=handle.kind, address=handle.channel,
            conversation_id=turn.conversation_id, reply_ref=handle.placeholder,
        )
    elif parsed:
        expected = parsed[0].target
    if any(event.target != expected for event in parsed):
        return _relay_failure("wrong_target", profile)
    completed = [event for event in parsed if isinstance(event, TurnCompleted)]
    if not completed:
        return _relay_failure("missing_completion", profile)
    if len(completed) != 1:
        return _relay_failure("duplicate_completion", profile)
    if turn is not None and completed[0].event_id != turn.event_id:
        return _relay_failure("wrong_event_id", profile)
    if completed[0].outcome != "delivered":
        return _relay_failure("not_delivered", profile)
    updates = [event for event in parsed if isinstance(event, ReplyUpdate)]
    # Delivery observations must not broaden the original exact nonce verdict:
    # its last update includes progress edits, and reply.post was never allowed.
    exact = (
        bool(updates)
        and updates[-1].text == nonce
        and all(
            event.event in {"reply.update", "turn.completed", "turn.status"} for event in parsed
        )
    )
    answers = [event for event in updates if event.progress is None]
    text = answers[-1].text if answers else None
    if text is None:
        return _relay_failure("missing_reply", profile, exact_nonce=exact)
    if not text.strip():
        return _relay_failure("blank_reply", profile, exact_nonce=exact)
    matched = profile.matches(text) if profile is not None else exact
    return RelayAssessment(
        exact, True, "exact_nonce" if exact else "nonce_mismatch", matched,
        ("profile_match" if matched else "profile_mismatch") if profile is not None else None,
    )


def relay_outcome(
    events: list[dict[str, Any]], *, nonce: str, turn: QueuedTurn | None = None
) -> bool:
    """Boolean compatibility for exact nonce success. @spec TURN-CANARY-3"""
    return assess_relay(events, nonce=nonce, turn=turn).exact_nonce


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
    platform: Platform, turn: QueuedTurn, nonce: str, deadline: float, poll: float,
    profile: ProbeProfile | None = None,
) -> RelayAssessment:
    """Consume the relay count cursor under the deadline.

    @spec TURN-CANARY-3 TURN-CANARY-16
    """
    assert turn.reply_handle is not None
    reply_ref = turn.reply_handle.placeholder
    assert reply_ref is not None
    cursor = 0
    events: list[dict[str, Any]] = []
    end = time.monotonic() + deadline
    try:
        async with asyncio.timeout(deadline):
            while time.monotonic() < end:
                page = await platform.replies(reply_ref, cursor)
                if (
                    not isinstance(page, dict)
                    or not isinstance(page.get("events"), list)
                    or type(page.get("next_cursor")) is not int
                    or type(page.get("terminal")) is not bool
                ):
                    return _relay_failure("malformed_page", profile)
                batch = page["events"]
                next_cursor = page["next_cursor"]
                if next_cursor != cursor + len(batch):
                    return _relay_failure("malformed_page", profile)
                events.extend(batch)
                observed = assess_relay(events, nonce=nonce, turn=turn, profile=profile)
                if observed.reason in {
                    "malformed_event", "wrong_target", "wrong_event_id", "duplicate_completion"
                }:
                    return observed
                completed = observed.reason != "missing_completion"
                # The production relay keeps awaiting-approval nonterminal so a
                # resumed turn can reuse the bucket. It still fails this probe.
                terminal = any(
                    item.get("event") == "turn.completed"
                    and item.get("outcome") != "awaiting-approval"
                    for item in events
                )
                if page["terminal"] != terminal:
                    return _relay_failure("terminal_mismatch", profile)
                if completed:
                    return observed
                cursor = next_cursor
                await asyncio.sleep(min(poll, max(0, end - time.monotonic())))
    except TimeoutError:
        pass
    return _relay_failure("timeout", profile)


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
    declared: Sequence[Route] | None = None,
    binding_health_path: str | None = None,
    coverage: bool = False,
    exclude_kinds: Sequence[str] = (),
    probe_profiles: Mapping[str, ProbeProfile] | None = None,
) -> CycleResult:
    """Probe serially; hold only routes with uncertain cleanup.

    @spec TURN-CANARY-1 TURN-CANARY-15 TURN-CANARY-16
    """
    Limits(
        turn_deadline=turn_deadline,
        cleanup_deadline=cleanup_deadline,
        poll_period=poll_period,
        max_targets=max_targets,
    )
    profiles = _check_probe_profiles(probe_profiles)
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
            declared=declared,
            binding_health_path=binding_health_path,
            coverage=coverage,
            exclude_kinds=exclude_kinds,
            probe_profiles=profiles,
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
    declared: Sequence[Route] | None,
    binding_health_path: str | None,
    coverage: bool,
    exclude_kinds: Sequence[str],
    probe_profiles: Mapping[str, ProbeProfile],
) -> None:
    """Inventory-to-probe cycle wiring. @spec TURN-CANARY-1 TURN-CANARY-16"""
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
    observes = binding_checks or declared is not None or binding_health_path or coverage
    if held:
        result.record("cleanup", "previous_unconfirmed")
        if not observes:
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
    if exclude_kinds:
        # @spec TURN-CANARY-15: excluded kinds leave before every observation,
        # on the declared side of drift as well as the live side.
        excluded = set(exclude_kinds)
        rows = [row for row in rows if row.get("kind") not in excluded]
        if declared is not None:
            declared = [route for route in declared if route[0] not in excluded]
    result.inventory_labels = {_route_label(row) for row in rows}
    for row in rows:
        result.route_agents.setdefault(_route_label(row), row.get("agent", ""))
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
            probe_profiles,
        )
    if binding_checks:
        await _check_bindings(platform, rows, set(result.target_last_run), identity_kinds, result)
    if declared is not None:
        result.drift = binding_drift(rows, declared)
    if binding_health_path is not None:
        await _probe_binding_endpoints(platform, rows, binding_health_path, result)
    if coverage:
        result.uncovered_agents = uncovered_agents(
            rows, exclude=exclude, health_path=binding_health_path
        )


async def _turn_targets(
    platform: Platform,
    targets: list[dict[str, str]],
    result: CycleResult,
    journal: StateJournal,
    turn_deadline: float,
    cleanup_deadline: float,
    poll_period: float,
    conversation_prefix: str = CONVERSATION_PREFIX,
    probe_profiles: Mapping[str, ProbeProfile] | None = None,
) -> None:
    """The owned, serial turn path. @spec TURN-CANARY-4 TURN-CANARY-16"""
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
        profile = (probe_profiles or {}).get(binding.get("agent", ""))
        result.target_probe_profile[target_label] = profile is not None
        result.target_probe_success[target_label] = False
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
            profile=profile,
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
            result.target_delivery_success[target_label] = False
            all_ok = False
        if enqueued:
            try:
                assessment = await _await_relay(
                    platform, turn, nonce, turn_deadline, poll_period, profile
                )
                result.target_success[target_label] = assessment.exact_nonce
                result.target_delivery_success[target_label] = assessment.delivery_success
                result.target_probe_success[target_label] = assessment.probe_success
                if assessment.delivery_success:
                    result.target_last_delivery_success[target_label] = time.time()
                if assessment.probe_success:
                    result.target_last_probe_success[target_label] = time.time()
                result.record("relay", assessment.reason)
                result.record(
                    "turn", "delivered" if result.target_success[target_label] else "failed"
                )
            except Exception as exc:  # noqa: BLE001 - reset is still owed
                result.target_success[target_label] = False
                result.target_delivery_success[target_label] = False
                result.record("relay", "failed", exc)
            all_ok = all_ok and result.target_success[target_label]
            if result.target_success[target_label]:
                result.target_last_success[target_label] = time.time()
        if profile is not None:
            result.record(
                "probe",
                ("profile_match" if result.target_probe_success[target_label]
                 else "profile_mismatch"),
            )
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
        quota_token_file: str | None = None,
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
        if quota_token and quota_token_file:
            raise ValueError("quota check takes a static token or a token file, not both")
        if quota_url and (not (quota_token or quota_token_file) or min_free <= 0):
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
        self._quota_token_file = quota_token_file
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
            SELECT a.id AS agent_id, a.name AS agent, c.kind, c.address, c.adapter, c.endpoint,
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
        token = self._quota_bearer()
        if token is None:
            return None
        try:
            response = await self._quota_http.get(
                self._quota_url, headers={"Authorization": "Bearer " + token}
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

    def _quota_bearer(self) -> str | None:
        """The static token, or the token file read afresh for this request.

        @spec TURN-CANARY-5
        """
        if self._quota_token_file is None:
            return self._quota_token
        try:
            token = Path(self._quota_token_file).read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError):
            return None
        return token or None

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


def _check_layout(labels: str) -> str:
    if labels not in ROUTE_LABEL_LAYOUTS:
        raise ValueError("route label layout must be route or split")
    return labels


def prometheus_text(
    result: CycleResult, prefix: str = "turn_canary", labels: str = "route"
) -> str:
    """Portable bounded exposition. @spec TURN-CANARY-6 TURN-CANARY-14 TURN-CANARY-16"""
    _check_prefix(prefix)
    _check_layout(labels)

    def line(name: str, value: int | float, labels: str = "") -> str:
        return f"{prefix}_{name}{{{labels}}} {value}\n" if labels else f"{prefix}_{name} {value}\n"

    def route_labels(route: str, **extra: str) -> str:
        if labels == "route":
            return _labels(route=_route_value(route), **extra)
        kind, address, adapter = route.split("|", 2)
        agent = result.route_agents.get(route, "")
        return _labels(agent=agent, kind=kind, address=address, adapter=adapter, **extra)

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
        label = route_labels(route)
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
    for route, delivered in sorted(result.target_delivery_success.items()):
        output.append(line("target_delivery_success", int(delivered), route_labels(route)))
    for name in ("target_probe_success", "target_probe_profile"):
        for route, value in sorted(getattr(result, name).items()):
            output.append(line(name, int(value), route_labels(route)))
    for name in ("target_last_delivery_success", "target_last_probe_success"):
        for route, timestamp in sorted(getattr(result, name).items()):
            output.append(line(name + "_timestamp_seconds", timestamp, route_labels(route)))
    for route, skipped in sorted(result.target_capacity_skipped.items()):
        label = route_labels(route)
        output.append(line("target_capacity_skipped", int(skipped), label))
    for route, degraded in sorted(result.target_cleanup_degraded.items()):
        label = route_labels(route)
        output.append(line("target_cleanup_degraded", int(degraded), label))
    for route, outcome in sorted(result.binding_checks.items()):
        label = route_labels(route, outcome=outcome)
        output.append(line("binding_check_success", int(outcome == "ok"), label))
        if route in result.binding_check_last_run:
            output.append(
                line(
                    "binding_check_last_run_timestamp_seconds",
                    result.binding_check_last_run[route],
                    route_labels(route),
                )
            )
    for adapter, outcome in sorted(result.adapter_health.items()):
        output.append(
            line("adapter_up", int(outcome == "ok"), _labels(adapter=adapter, outcome=outcome))
        )
    for route, outcome in sorted(result.binding_adapter_health.items()):
        output.append(
            line("binding_adapter_up", int(outcome == "ok"), route_labels(route, outcome=outcome))
        )
    if result.drift is not None:
        for route, side in sorted(result.drift.items()):
            output.append(line("binding_drift", 1, route_labels(route, side=side)))
        output.append(line("binding_drift_total", len(result.drift)))
    if result.uncovered_agents is not None:
        for agent in result.uncovered_agents:
            output.append(line("agent_uncovered", 1, _labels(agent=agent)))
        output.append(line("uncovered_agents", len(result.uncovered_agents)))
    return "".join(output)


class Monitor:
    """Non-overlapping cycle loop and merged last observation. @spec TURN-CANARY-10"""

    def __init__(
        self,
        run_once: Callable[[], Awaitable[CycleResult]] | None,
        limits: Limits,
        *,
        prefix: str = "turn_canary",
        labels: str = "route",
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[dict[str, str]], None] | None = None,
    ) -> None:
        self._run_once = run_once
        self._limits = limits
        self._prefix = _check_prefix(prefix)
        self._labels = _check_layout(labels)
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
        """Merge retained observations. @spec TURN-CANARY-10 TURN-CANARY-16"""
        view = self._view or CycleResult()
        view.success = result.success
        view.cleanup_degraded = result.cleanup_degraded
        view.capacity_skips = result.capacity_skips
        view.last_run = result.last_run
        view.last_success = result.last_success or view.last_success
        view.rotation = result.rotation if result.rotation is not None else view.rotation
        view.adapter_health = dict(result.adapter_health)
        if result.drift is not None:
            view.drift = dict(result.drift)
        if result.uncovered_agents is not None:
            view.uncovered_agents = list(result.uncovered_agents)
        if result.inventory_labels is not None:
            # Only the latest inventory names an agent; a route it no longer
            # lists must not keep a stale name. @spec TURN-CANARY-14
            view.route_agents = dict(result.route_agents)
        per_route = (
            "target_success",
            "target_delivery_success",
            "target_probe_success",
            "target_probe_profile",
            "target_last_delivery_success",
            "target_last_probe_success",
            "target_last_run",
            "target_last_success",
            "target_capacity_skipped",
            "target_cleanup_degraded",
            "binding_checks",
            "binding_check_last_run",
            "binding_adapter_health",
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
        body = (
            prometheus_text(self._view, self._prefix, self._labels)
            if self._view is not None
            else ""
        )
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
    """Validated configuration. @spec TURN-CANARY-7 TURN-CANARY-10 TURN-CANARY-16"""

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
    quota_token_file: str | None = None
    declared: list[Route] | None = None
    binding_health_path: str | None = None
    coverage: bool = False
    route_labels: str = "route"
    exclude_kinds: tuple[str, ...] = ()
    probe_profiles: dict[str, ProbeProfile] = field(default_factory=dict)


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


def _health_path(raw: str) -> str:
    if not raw.startswith("/") or raw.startswith("//") or "?" in raw or "#" in raw:
        raise ValueError("TURN_CANARY_BINDING_ADAPTER_HEALTH must be an absolute path")
    return raw



def _listen(raw: str) -> tuple[str, int]:
    host, separator, port = raw.rpartition(":")
    host = host.strip("[]")
    if not separator or not host or not port.isdigit() or not 0 < int(port) <= 65535:
        raise ValueError("TURN_CANARY_LISTEN must be host:port")
    return host, int(port)


def parse_config(environ: Mapping[str, str]) -> Config:
    """Read explicit installation settings; nothing is defaulted from a tenant.

    @spec TURN-CANARY-7 TURN-CANARY-10 TURN-CANARY-16
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
    if environ.get("TURN_CANARY_QUOTA_TOKEN") and environ.get("TURN_CANARY_QUOTA_TOKEN_FILE"):
        raise ValueError(
            "set at most one of TURN_CANARY_QUOTA_TOKEN or TURN_CANARY_QUOTA_TOKEN_FILE"
        )
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
        probe_profiles=(
            parse_probe_profiles(environ["TURN_CANARY_PROBE_PROFILES"])
            if "TURN_CANARY_PROBE_PROFILES" in environ else {}
        ),
        discover=discover,
        exclude=_routes(environ.get("TURN_CANARY_DISCOVER_EXCLUDE", "[]"), "exclusions"),
        binding_checks=environ.get("TURN_CANARY_BINDING_CHECKS") == "1",
        identity_kinds=tuple(identity_kinds),
        adapters=_adapters(environ.get("TURN_CANARY_ADAPTER_HEALTH", "{}")),
        listen=_listen(listen) if listen else None,
        metric_prefix=_check_prefix(environ.get("TURN_CANARY_METRIC_PREFIX", "turn_canary")),
        metrics_path=environ.get("TURN_CANARY_METRICS_PATH") or None,
        quota_url=environ.get("TURN_CANARY_QUOTA_URL"),
        quota_token=environ.get("TURN_CANARY_QUOTA_TOKEN") or None,
        quota_token_file=environ.get("TURN_CANARY_QUOTA_TOKEN_FILE") or None,
        quota_resource=environ.get("TURN_CANARY_QUOTA_RESOURCE", "count/pods"),
        quota_min_free=parse_quantity(environ.get("TURN_CANARY_QUOTA_MIN_FREE", "1")),
        conversation_prefix=_check_conversation_prefix(
            environ.get("TURN_CANARY_CONVERSATION_PREFIX", CONVERSATION_PREFIX)
        ),
        declared=(
            _routes(environ["TURN_CANARY_DECLARED_BINDINGS"], "TURN_CANARY_DECLARED_BINDINGS")
            if environ.get("TURN_CANARY_DECLARED_BINDINGS")
            else None
        ),
        binding_health_path=(
            _health_path(environ["TURN_CANARY_BINDING_ADAPTER_HEALTH"])
            if environ.get("TURN_CANARY_BINDING_ADAPTER_HEALTH")
            else None
        ),
        coverage=environ.get("TURN_CANARY_COVERAGE") == "1",
        route_labels=_check_layout(environ.get("TURN_CANARY_ROUTE_LABELS", "route")),
        exclude_kinds=(
            _exclude_kinds(environ["TURN_CANARY_EXCLUDE_BINDING_KINDS"])
            if environ.get("TURN_CANARY_EXCLUDE_BINDING_KINDS")
            else ()
        ),
    )


def _print_log(item: dict[str, str]) -> None:
    print(json.dumps(item, sort_keys=True), flush=True)


async def main() -> int:
    """Run one cycle, or the exporter, from explicit settings. @spec TURN-CANARY-1 TURN-CANARY-16"""
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
        quota_token_file=config.quota_token_file,
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
                declared=config.declared,
                binding_health_path=config.binding_health_path,
                coverage=config.coverage,
                exclude_kinds=config.exclude_kinds,
                probe_profiles=config.probe_profiles,
            )
            journal.save_result(result)
        return result

    try:
        if config.listen is not None:
            import signal

            monitor = Monitor(
                one_cycle,
                limits,
                prefix=config.metric_prefix,
                labels=config.route_labels,
                log=_print_log,
            )
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
        Path(config.metrics_path).write_text(
            prometheus_text(result, config.metric_prefix, config.route_labels)
        )
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
