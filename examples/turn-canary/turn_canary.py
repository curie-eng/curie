"""Opt-in, bounded synthetic turn checker. @spec TURN-CANARY-1 TURN-CANARY-6"""

import asyncio
import fcntl
import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, TextIO

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
    target_last_run: dict[str, float] = field(default_factory=dict)
    target_last_success: dict[str, float] = field(default_factory=dict)
    logs: list[dict[str, str]] = field(default_factory=list)
    last_run: float = field(default_factory=time.time)
    last_success: float | None = None

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
        state = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(state, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state.seek(0)
            raw = state.read().strip()
            data = json.loads(raw) if raw else {}
            pending = data.get("pending") if isinstance(data, dict) else None
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("capacity_skips_total", 0), int)
                or not isinstance(data.get("target_last_success", {}), dict)
                or data.get("cleanup_blocked", False) not in (True, False)
                or (
                    pending is not None
                    and (
                        not isinstance(pending, dict)
                        or not isinstance(pending.get("agent_id"), str)
                        or not isinstance(pending.get("thread_key"), str)
                    )
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
    def pending(self) -> dict[str, str] | None:
        return self.data.get("pending")

    def _write(self) -> None:
        if self._file is None:
            raise RuntimeError("state journal is not locked")
        self._file.seek(0)
        self._file.truncate()
        self._file.write(json.dumps(self.data, sort_keys=True) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def record_intent(self, agent_id: str, thread_key: str) -> None:
        if self.pending is not None:
            raise RuntimeError("previous owned cleanup remains pending")
        self.data["pending"] = {"agent_id": agent_id, "thread_key": thread_key}
        self._write()

    def clear_intent(self, agent_id: str, thread_key: str) -> None:
        if self.pending != {"agent_id": agent_id, "thread_key": thread_key}:
            raise RuntimeError("cleanup does not match the recorded owned route")
        self.data["pending"] = None
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
                "cleanup_blocked": result.cleanup_degraded or self.pending is not None,
                "capacity_skips_total": result.capacity_skips,
                "last_success": result.last_success,
                "target_last_success": result.target_last_success,
            }
        )
        self._write()


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


async def _await_cleanup(
    platform: Platform, agent_id: str, key: str, deadline: float, poll: float
) -> bool:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        state = await platform.reset_state(agent_id, key)
        if cleanup_confirmed(state):
            return True
        if state.get("requested") is False:
            return False
        await asyncio.sleep(min(poll, max(0, end - time.monotonic())))
    return False


async def run_cycle(
    platform: Platform,
    selected: list[tuple[str, str, str]],
    *,
    journal: StateJournal | None = None,
    turn_deadline: float = 90.0,
    cleanup_deadline: float = 60.0,
    poll_period: float = 2.0,
    max_targets: int = 5,
) -> CycleResult:
    """Probe serially; hold on uncertain cleanup. @spec TURN-CANARY-1 TURN-CANARY-6"""
    Limits(
        turn_deadline=turn_deadline,
        cleanup_deadline=cleanup_deadline,
        poll_period=poll_period,
        max_targets=max_targets,
    )
    result = CycleResult()
    if getattr(platform, "cleanup_blocked", False) or (journal is not None and journal.pending):
        result.cleanup_degraded = True
        result.record("cleanup", "previous_unconfirmed")
        return result
    if len(selected) > max_targets:
        raise ValueError("selected routes exceed maximum")
    try:
        targets = select_targets(await platform.inventory(), selected)
        if not await platform.read_only_supported():
            result.record("admission", "read_only_unavailable")
            return result
    except Exception as exc:  # noqa: BLE001 - inventory failures fail the whole cycle
        result.record("inventory", "failed", exc)
        return result
    all_ok = True
    for binding in targets:
        target_label = "|".join((binding["kind"], binding["address"], binding["adapter"]))
        result.target_last_run[target_label] = time.time()
        try:
            headroom = await platform.quota_headroom()
        except Exception as exc:  # noqa: BLE001 - unreadable quota skips enqueue
            result.record("capacity", "unreadable", exc)
            headroom = None
        if headroom is not True:
            result.capacity_skips += 1
            result.record("capacity", "skipped")
            all_ok = False
            continue
        conversation_id = "eval:" + str(uuid.uuid4())
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
        if journal is not None:
            journal.record_intent(binding["agent_id"], key)
        enqueued = False
        try:
            await platform.enqueue(turn)
            enqueued = True
        except Exception as exc:  # noqa: BLE001 - uncertain XADD still requires cleanup
            result.record("enqueue", "failed", exc)
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
            confirmed = await _await_cleanup(
                platform, binding["agent_id"], key, cleanup_deadline, poll_period
            )
            if confirmed and journal is not None:
                journal.clear_intent(binding["agent_id"], key)
        except Exception as exc:  # noqa: BLE001 - unknown reset state must stop the cycle
            result.record("cleanup", "failed", exc)
            confirmed = False
        if not confirmed:
            result.cleanup_degraded = True
            platform.cleanup_blocked = True
            result.record("cleanup", "unconfirmed")
            break
    result.success = all_ok and not result.cleanup_degraded
    if result.success:
        result.last_success = time.time()
    return result


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
        quota_min_free: int = 1,
    ) -> None:
        import re
        from urllib.parse import urlsplit

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
        if quota_url and (not quota_token or quota_min_free < 1):
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
        self._stream = stream
        self._schema = schema
        self._qualified = read_only_qualified
        self._quota_url = quota_url
        self._quota_token = quota_token
        self._quota_resource = quota_resource
        self._quota_min_free = quota_min_free
        self.cleanup_blocked = False

    async def close(self) -> None:
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
                hard = int(status["hard"][self._quota_resource])
                used = int(status["used"][self._quota_resource])
                if hard - used < self._quota_min_free:
                    return False
            return True
        except (KeyError, TypeError, ValueError):
            return None

    async def enqueue(self, turn: QueuedTurn) -> None:
        """Append exactly one complete queued wire payload. @spec TURN-CANARY-2"""
        from aci_protocol import STREAM_PAYLOAD_FIELD

        wire = QueuedTurn.model_validate_json(turn.model_dump_json(), context=READER_CONTEXT)
        await self._redis.xadd(self._stream, {STREAM_PAYLOAD_FIELD: wire.model_dump_json()})

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Convert HTTP errors without URL, body, or credential text. @spec TURN-CANARY-6"""
        response = await self._http.request(method, path, **kwargs)
        if response.status_code >= 400:
            raise PlatformHttpError(response.status_code)
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("platform response is not an object")
        return payload

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


def prometheus_text(result: CycleResult) -> str:
    """Portable bounded exposition of the last cycle. @spec TURN-CANARY-6"""

    def line(name: str, value: int | float) -> str:
        return f"turn_canary_{name} {value}\n"

    output = [
        line("cycle_success", int(result.success)),
        line("last_cycle_timestamp_seconds", result.last_run),
        line("cleanup_degraded", int(result.cleanup_degraded)),
        line("capacity_skips_total", result.capacity_skips),
    ]
    if result.last_success is not None:
        output.append(line("last_success_timestamp_seconds", result.last_success))
    for route, last_run in sorted(result.target_last_run.items()):
        kind, address, adapter = route.split("|", 2)
        labels = json.dumps({"kind": kind, "address": address, "adapter": adapter}, sort_keys=True)
        label = "route=" + json.dumps(labels)
        success = int(result.target_success.get(route, False))
        output.append(f"turn_canary_target_success{{{label}}} {success}\n")
        output.append(f"turn_canary_target_last_run_timestamp_seconds{{{label}}} {last_run}\n")
        if route in result.target_last_success:
            output.append(
                f"turn_canary_target_last_success_timestamp_seconds{{{label}}} "
                f"{result.target_last_success[route]}\n"
            )
    return "".join(output)


async def main() -> int:
    """Run one opt-in cycle from explicit environment bindings. @spec TURN-CANARY-1 TURN-CANARY-6"""
    import os

    required = (
        "TURN_CANARY_DATABASE_URL",
        "TURN_CANARY_VALKEY_URL",
        "TURN_CANARY_API_URL",
        "TURN_CANARY_API_KEY",
        "TURN_CANARY_STREAM",
        "TURN_CANARY_ROUTES",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("missing required canary configuration: " + ", ".join(missing))
    selected_raw = json.loads(os.environ["TURN_CANARY_ROUTES"])
    if not isinstance(selected_raw, list):
        raise ValueError("routes must be a list of triples")
    selected = [tuple(item) for item in selected_raw]
    state_path = os.environ.get("TURN_CANARY_STATE_PATH")
    if not state_path:
        raise ValueError("TURN_CANARY_STATE_PATH is required to retain cleanup hold across runs")
    limits = Limits(
        cycle_interval=float(os.environ.get("TURN_CANARY_CYCLE_INTERVAL", "300")),
        turn_deadline=float(os.environ.get("TURN_CANARY_TURN_DEADLINE", "90")),
        cleanup_deadline=float(os.environ.get("TURN_CANARY_CLEANUP_DEADLINE", "60")),
        poll_period=float(os.environ.get("TURN_CANARY_POLL_PERIOD", "2")),
        max_targets=int(os.environ.get("TURN_CANARY_MAX_TARGETS", "5")),
    )
    platform = RuntimePlatform(
        database_url=os.environ["TURN_CANARY_DATABASE_URL"],
        valkey_url=os.environ["TURN_CANARY_VALKEY_URL"],
        api_url=os.environ["TURN_CANARY_API_URL"],
        api_key=os.environ["TURN_CANARY_API_KEY"],
        stream=os.environ["TURN_CANARY_STREAM"],
        schema=os.environ.get("TURN_CANARY_DB_SCHEMA", "curie"),
        read_only_qualified=os.environ.get("TURN_CANARY_READ_ONLY_QUALIFIED") == "1",
        quota_url=os.environ.get("TURN_CANARY_QUOTA_URL"),
        quota_token=os.environ.get("TURN_CANARY_QUOTA_TOKEN"),
        quota_resource=os.environ.get("TURN_CANARY_QUOTA_RESOURCE", "count/pods"),
        quota_min_free=int(os.environ.get("TURN_CANARY_QUOTA_MIN_FREE", "1")),
    )
    state_file = Path(state_path)
    try:
        with StateJournal(state_file) as journal:
            platform.cleanup_blocked = journal.data.get("cleanup_blocked") is True
            result = await run_cycle(
                platform,
                selected,
                journal=journal,
                turn_deadline=limits.turn_deadline,
                cleanup_deadline=limits.cleanup_deadline,
                poll_period=limits.poll_period,
                max_targets=limits.max_targets,
            )
            journal.save_result(result)
    finally:
        await platform.close()
    metrics_path = os.environ.get("TURN_CANARY_METRICS_PATH")
    if metrics_path:
        Path(metrics_path).write_text(prometheus_text(result))
    for item in result.logs:
        print(json.dumps(item, sort_keys=True))
    return 0 if result.success else 1


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except Exception as exc:  # noqa: BLE001 - never print a transport exception's raw URL
        print(
            json.dumps({"phase": "startup", "outcome": "failed", "error_type": type(exc).__name__})
        )
        raise SystemExit(1) from None
