"""Private nomination capture around the guarded runner, @spec AUTOMATED-REMEDIATION-6.

This composition grants no execution or connector authority. The injected
runner must guard every model start, including capacity admission, and every
delivery-bound control operation. Only the API decides nomination admission.
"""

from __future__ import annotations

import logging
import math
from collections.abc import AsyncIterator, Mapping
from types import TracebackType
from typing import NoReturn, Protocol
from urllib.parse import urlsplit

import aiohttp
from aci_protocol import Event, Final, OutboundEvent, TextDelta
from curie_protected_hooks.worker_verification import Dispatchable

from .protected_binding import _CURRENT_DELIVERY
from .protected_nomination_filter import NominationLineFilter, filter_final
from .runner_client import TimeoutResult, TurnStream

logger = logging.getLogger(__name__)


class NominationSubmitter(Protocol):
    """Submit exact captured bytes under the verified event's existing identity."""

    async def submit(self, *, event_id: str, block: str) -> None: ...


class GuardedRunner(Protocol):
    """The protected kernel's explicit runner operations; no dynamic forwarding.

    Implementations, not this capture layer, own start/admit/steer proof checks.
    Workspace, publication, executor and human-only operations are absent.
    """

    def turn_deadline_s(self, remaining_s: float | None) -> float: ...

    async def start_turn(
        self, base_url: str, event: Event, token: str | None = None, *,
        remaining_s: float | None = None, capacity_admission: bool = False,
        progress: Mapping[str, str] | None = None,
    ) -> TurnStream: ...

    async def status(
        self, base_url: str, *, token: str | None = None, remaining_s: float | None = None,
    ) -> dict[str, object]: ...

    async def steer(
        self, base_url: str, event: Event, token: str | None = None, *,
        remaining_s: float | None = None,
    ) -> bool: ...

    async def interrupt(
        self, base_url: str, reason: str, token: str | None = None,
    ) -> None: ...

    async def capacity_status(
        self, base_url: str, *, epoch: str | None = None, token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]: ...

    async def admit_turn(
        self, base_url: str, turn_epoch: str, *, allow: bool, token: str | None = None,
        remaining_s: float | None = None,
    ) -> None: ...

    async def timeout_turn(
        self, base_url: str, turn_epoch: str, *, token: str | None = None,
    ) -> TimeoutResult: ...


class NominationSubmissionError(RuntimeError):
    """A redacted transport/refusal failure; no response, token or cause chain."""


def _raise_detached(error: Exception) -> NoReturn:
    """Detach ambient caller data after Python attaches it at raise time."""
    try:
        raise error
    finally:
        error.__context__ = None
        error.__cause__ = None


class HttpNominationSubmitter:
    """One bounded POST with the worker-only token, no redirect or local retry.

    The URL/token come from the protected composition's provisioned worker
    configuration. They never ride to the runner, appear in repr, or become
    model data. API immutable byte-identical idempotency owns retry behavior.
    """

    __slots__ = ("__url", "__token", "__timeout")

    def __init__(self, base_url: str, worker_token: str, *, timeout_s: float = 5.0) -> None:
        try:
            parsed = urlsplit(base_url)
            valid = (
                parsed.scheme in {"http", "https"} and bool(parsed.hostname)
                and parsed.username is None and parsed.password is None
                and not parsed.query and not parsed.fragment
                and bool(worker_token) and "\r" not in worker_token and "\n" not in worker_token
                and math.isfinite(timeout_s) and 0 < timeout_s <= 10.0
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            _raise_detached(ValueError("nomination submitter configuration is invalid"))
        self.__url = base_url.rstrip("/") + "/v1/internal/remediation/nominations"
        self.__token = worker_token
        self.__timeout = aiohttp.ClientTimeout(total=timeout_s, connect=timeout_s)

    async def submit(self, *, event_id: str, block: str) -> None:
        """Send only the closed route body; deliberately never read an error body."""
        failed = False
        try:
            async with aiohttp.ClientSession(timeout=self.__timeout, trust_env=False) as session:
                async with session.post(
                    self.__url, json={"event_id": event_id, "block": block},
                    headers={"X-Curie-Worker-Token": self.__token}, allow_redirects=False,
                ) as response:
                    failed = response.status != 200
        except (aiohttp.ClientError, TimeoutError, ValueError):
            failed = True
        # Even outside this handler, Python attaches an active caller exception
        # at raise time. Detach the complete graph after that attachment; merely
        # using "from None" would hide, but retain, sensitive underlying data.
        if failed:
            _raise_detached(NominationSubmissionError("nomination submission failed"))


class NominationCapturingStream(TurnStream):
    """Filter public frames while leaving the runner's transcript unchanged."""

    def __init__(
        self, stream: TurnStream, submitter: NominationSubmitter, event_id: str | None,
    ) -> None:
        # Parent response, deadline and cleanup belong only to the delegated
        # stream; this subclass never initializes another transport/budget.
        self._stream = stream
        self._submitter = submitter
        self._event_id = event_id
        self.turn_epoch = stream.turn_epoch
        self._iterated = False
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[OutboundEvent]:
        if self._iterated or self._closed:
            return
        self._iterated = True
        line_filter = NominationLineFilter()
        async for frame in self._stream:
            if self._closed:
                return
            if isinstance(frame, TextDelta):
                public = line_filter.feed(frame.text)
                if public:
                    yield frame.model_copy(update={"text": public})
            elif isinstance(frame, Final):
                tail = line_filter.finish()
                final = filter_final(frame.text, frame.status)
                if final.submission is not None:
                    if self._event_id is None:
                        logger.warning("nomination_unattributed")
                    else:
                        await self._submitter.submit(
                            event_id=self._event_id, block=final.submission
                        )
                if tail:
                    yield TextDelta(text=tail)
                yield frame.model_copy(update={"text": final.reply})
                return
            else:
                yield frame
        # EOF without Final is never a successful nomination submission.
        tail = line_filter.finish()
        if tail:
            yield TextDelta(text=tail)

    def close(self) -> None:
        self._closed = True
        self._stream.close()

    async def __aenter__(self) -> NominationCapturingStream:
        await self._stream.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            await self._stream.__aexit__(exc_type, exc, tb)
        finally:
            self._closed = True


class NominationCapturingRunner:
    """Capture wraps the returned stream; the injected guard runs before start."""

    def __init__(self, runner: GuardedRunner, submitter: NominationSubmitter) -> None:
        self._runner = runner
        self._submitter = submitter

    async def start_turn(
        self, base_url: str, event: Event, token: str | None = None, *,
        remaining_s: float | None = None, capacity_admission: bool = False,
        progress: Mapping[str, str] | None = None,
    ) -> NominationCapturingStream:
        """Snapshot exact Dispatchable attribution before awaiting the guarded start."""
        delivery = _CURRENT_DELIVERY.get()
        event_id = (
            delivery.envelope.as_dict()["event_id"] if type(delivery) is Dispatchable else None
        )
        stream = await self._runner.start_turn(
            base_url, event, token, remaining_s=remaining_s,
            capacity_admission=capacity_admission, progress=progress,
        )
        return NominationCapturingStream(stream, self._submitter, event_id)

    def turn_deadline_s(self, remaining_s: float | None) -> float:
        return self._runner.turn_deadline_s(remaining_s)

    async def status(
        self, base_url: str, *, token: str | None = None, remaining_s: float | None = None,
    ) -> dict[str, object]:
        return await self._runner.status(base_url, token=token, remaining_s=remaining_s)

    async def steer(
        self, base_url: str, event: Event, token: str | None = None, *,
        remaining_s: float | None = None,
    ) -> bool:
        return await self._runner.steer(base_url, event, token, remaining_s=remaining_s)

    async def interrupt(
        self, base_url: str, reason: str, token: str | None = None,
    ) -> None:
        await self._runner.interrupt(base_url, reason, token)

    async def capacity_status(
        self, base_url: str, *, epoch: str | None = None, token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        return await self._runner.capacity_status(
            base_url, epoch=epoch, token=token, remaining_s=remaining_s,
        )

    async def admit_turn(
        self, base_url: str, turn_epoch: str, *, allow: bool, token: str | None = None,
        remaining_s: float | None = None,
    ) -> None:
        await self._runner.admit_turn(
            base_url, turn_epoch, allow=allow, token=token, remaining_s=remaining_s,
        )

    async def timeout_turn(
        self, base_url: str, turn_epoch: str, *, token: str | None = None,
    ) -> TimeoutResult:
        return await self._runner.timeout_turn(base_url, turn_epoch, token=token)
