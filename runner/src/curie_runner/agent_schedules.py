"""Agent-managed schedules: client, config load and guidance (ADR 0238).

The runner reads the agent's schedule settings at boot from the dedicated
``/agents/<id>/schedules`` router, reached from the agent prefix of
``CURIE_STATE_URL`` with the boot read token. Writes go through four in-process
tools (``harness/claude/agent_schedule_tools.py``) with the turn's own
credential (``MemoryTurn.write_token``). This module stays free of the harness
SDK. With the toggle off, the store unreachable, or the fake model, nothing
mounts: no tools, no guidance.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp

from .learned_skills import _detail
from .memory import MEMORY_TOKEN_ENV
from .state import STATE_TOKEN_ENV, STATE_URL_ENV
from .tool_names import AGENT_SCHEDULES_SERVER_NAME as AGENT_SCHEDULES_SERVER_NAME

logger = logging.getLogger(__name__)

UNREACHABLE_LOG = "agent schedules store unreachable; starting without agent schedules"

_AGENT_PREFIX = re.compile(r"^(?P<prefix>.*/agents/[^/]+)/state(?:/.*)?\Z")
_TIMEOUT = aiohttp.ClientTimeout(total=15)


def schedules_guidance(timezone: str) -> str:
    """The system-prompt block for a session with the schedule tools mounted."""

    return f"""\
Schedules

You can create schedules that run you later with the agent_schedules tools: create_schedule, list_schedules, update_schedule, cancel_schedule and revert_schedule. A schedule is either recurring (a five-field cron) or a one-shot (a single run_at time). The default time zone is {timezone}; times you pass without a zone are read in it, and you may pass an IANA timezone to use another.
1. Create a schedule only when someone asks for one. The prompt you store runs later with no one watching, so write it as a complete instruction that stands on its own.
2. A local time can be ambiguous or nonexistent around a clock change. If the tool says so, do not guess: ask which time was meant and retry with an explicit offset.
3. If the request is unclear (how often, at what time, which zone), ask a clarifying question before creating anything.
4. After any create or change, read the result back to the channel: what will run, when the next run is, written in the schedule's zone as the tool states it, and the schedule id. Say that replying "cancel" removes it.
5. After creating a new recurring schedule, offer to do a test run now. Offer it; never run it unasked.
6. To change a schedule, list schedules first and update against the revision you read. On a conflict, read again and retry.
7. When someone asks to undo a change, use revert_schedule against the revision you read; it restores the previous configuration unless they name an earlier revision.
8. Scheduled runs cannot create, change or revert schedules. They can list and cancel."""  # noqa: E501


class AgentSchedulesError(RuntimeError):
    """An agent-schedules request failed. ``status`` is the HTTP status, if any.

    The message carries the API's ``detail`` only, never a token."""

    def __init__(self, detail: str, *, status: int | None = None) -> None:
        super().__init__(detail)
        self.status = status


@dataclass(frozen=True)
class AgentSchedule:
    id: str
    revision: int
    state: str
    prompt: str
    cron: str | None = None
    run_at: str | None = None
    timezone: str | None = None
    next_run_at: str | None = None
    next_run_local: str | None = None
    paused: bool = False
    destination: Mapping[str, Any] | None = None
    last_outcome: str | None = None


@dataclass(frozen=True)
class AgentSchedulesConfig:
    enabled: bool
    timezone: str
    max_active: int
    min_interval_minutes: int


def agent_schedules_url(state_url: str) -> str | None:
    """``<agent prefix>/schedules`` from a state URL, or None."""

    match = _AGENT_PREFIX.match(state_url or "")
    return f"{match.group('prefix')}/schedules" if match else None


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _parse(payload: object) -> AgentSchedule:
    if not isinstance(payload, Mapping):
        raise AgentSchedulesError("schedule is not a JSON object")
    try:
        destination = payload.get("destination")
        return AgentSchedule(
            id=str(payload["id"]),
            revision=int(payload["revision"]),
            state=str(payload["state"]),
            prompt=str(payload["prompt"]),
            cron=_opt_str(payload.get("cron")),
            run_at=_opt_str(payload.get("run_at")),
            timezone=_opt_str(payload.get("timezone")),
            next_run_at=_opt_str(payload.get("next_run_at")),
            next_run_local=_opt_str(payload.get("next_run_local")),
            paused=bool(payload.get("paused", False)),
            destination=destination if isinstance(destination, Mapping) else None,
            last_outcome=_opt_str(payload.get("last_outcome")),
        )
    except (KeyError, TypeError, ValueError):
        raise AgentSchedulesError("schedule is malformed") from None


def _parse_config(payload: object) -> AgentSchedulesConfig:
    if not isinstance(payload, Mapping):
        raise AgentSchedulesError("schedules config is malformed")
    try:
        return AgentSchedulesConfig(
            enabled=payload.get("enabled") is True,
            timezone=str(payload["timezone"]),
            max_active=int(payload["max_active"]),
            min_interval_minutes=int(payload["min_interval_minutes"]),
        )
    except (KeyError, TypeError, ValueError):
        raise AgentSchedulesError("schedules config is malformed") from None


class AgentSchedulesClient:
    """The agent's schedules router.

    Reads present ``read_token`` (the boot read token); writes present
    ``write_token()``, the turn's own credential, and are refused here without
    one. Both go as ``X-API-Key``.
    """

    def __init__(
        self, url: str, read_token: str | None, write_token: Callable[[], str | None]
    ) -> None:
        self._base = url.rstrip("/")
        self._read_token = read_token
        self._write_token = write_token

    def __repr__(self) -> str:
        return f"AgentSchedulesClient({self._base!r})"

    def _url(self, ref: str, action: str = "") -> str:
        return f"{self._base}/{quote(ref, safe='')}" + (f"/{action}" if action else "")

    async def _call(
        self,
        method: str,
        url: str,
        *,
        write: bool = False,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        token = self._write_token() if write else self._read_token
        if not token:
            raise AgentSchedulesError("no credential for this request", status=403)
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                # A resent write against an expected revision turns a committed
                # change into a false 409 refusal; aiohttp has no public switch.
                session._retry_connection = False  # noqa: SLF001
                async with session.request(
                    method, url, json=json_body, headers={"X-API-Key": token}
                ) as resp:
                    if resp.status >= 400:
                        raise AgentSchedulesError(await _detail(resp), status=resp.status)
                    return await resp.json()
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            raise AgentSchedulesError(
                f"agent schedules request failed error_class={type(exc).__name__}"
            ) from None

    async def _config_payload(self) -> Mapping[str, Any]:
        payload = await self._call("GET", self._base)
        if not isinstance(payload, Mapping):
            raise AgentSchedulesError("schedules config is malformed")
        return payload

    async def load_config(self) -> AgentSchedulesConfig:
        return _parse_config(await self._config_payload())

    async def list(self) -> list[AgentSchedule]:
        payload = await self._config_payload()
        items = payload.get("schedules")
        if not isinstance(items, list):
            raise AgentSchedulesError("schedules list is malformed")
        return [_parse(item) for item in items]

    async def create(
        self,
        prompt: str,
        cron: str | None = None,
        run_at: str | None = None,
        timezone: str | None = None,
    ) -> AgentSchedule:
        body: dict[str, Any] = {"prompt": prompt}
        for key, value in (("cron", cron), ("run_at", run_at), ("timezone", timezone)):
            if value is not None:
                body[key] = value
        return _parse(await self._call("POST", self._base, write=True, json_body=body))

    async def update(self, ref: str, expected_revision: int, **fields: Any) -> AgentSchedule:
        body: dict[str, Any] = {"expected_revision": expected_revision}
        body.update({k: v for k, v in fields.items() if v is not None})
        return _parse(await self._call("PATCH", self._url(ref), write=True, json_body=body))

    async def cancel(self, ref: str, expected_revision: int | None = None) -> AgentSchedule:
        body: dict[str, Any] = {}
        if expected_revision is not None:
            body["expected_revision"] = expected_revision
        return _parse(
            await self._call("POST", self._url(ref, "cancel"), write=True, json_body=body)
        )

    async def revert(
        self, ref: str, expected_revision: int, to_revision: int | None = None
    ) -> AgentSchedule:
        body: dict[str, Any] = {"expected_revision": expected_revision}
        if to_revision is not None:
            body["to_revision"] = to_revision
        return _parse(
            await self._call("POST", self._url(ref, "revert"), write=True, json_body=body)
        )


async def load_for_session(env: Mapping[str, str], fake_model: bool) -> AgentSchedulesConfig | None:
    """The session's schedule settings, or None when nothing should mount.

    The fake model and a boot without a state URL or read token make no call.
    A disabled toggle or any store failure degrades to None (one log line).
    """

    state_url = env.get(STATE_URL_ENV)
    token = env.get(MEMORY_TOKEN_ENV) or env.get(STATE_TOKEN_ENV)
    if fake_model or not state_url or not token:
        return None
    url = agent_schedules_url(state_url)
    if url is None:
        logger.warning("%s: the state URL has no agent prefix", UNREACHABLE_LOG)
        return None
    try:
        config = await AgentSchedulesClient(url, token, lambda: None).load_config()
    except AgentSchedulesError as exc:
        logger.warning("%s status=%s", UNREACHABLE_LOG, exc.status)
        return None
    return config if config.enabled else None
