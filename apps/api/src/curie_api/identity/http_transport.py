"""Standalone HTTP identity exchange. @spec HTTP-IDENTITY-TRANSPORT-1 to -5."""

from __future__ import annotations

import asyncio
import logging
import re
import ssl
import threading
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from typing import Literal, Never, SupportsIndex
from urllib.parse import urlsplit

import httpx

from curie_api.schemas.http_identity import (
    MAX_RESPONSE_BYTES,
    FactsRefusal,
    FreshnessLimits,
    LookupRequestV1,
    ValidatedFacts,
    validate_lookup,
)

Result = ValidatedFacts | FactsRefusal
_guarded: ContextVar[bool] = ContextVar("identity_dependency_logs", default=False)


class _DependencyFilter(logging.Filter):
    """@spec HTTP-IDENTITY-TRANSPORT-5."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not _guarded.get()


_filter = _DependencyFilter()
_filter_lock = threading.Lock()


@contextmanager
def _dependency_log_guard() -> Iterator[None]:
    """@spec HTTP-IDENTITY-TRANSPORT-5."""
    with _filter_lock:
        for name in ("httpcore.connection", "httpcore.http11", "httpx"):
            logging.getLogger(name).addFilter(_filter)
    token = _guarded.set(True)
    try:
        yield
    finally:
        _guarded.reset(token)


@dataclass(frozen=True, slots=True)
class LookupTarget:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""

    endpoint: str
    tenant_id: str
    expected_issuer: str
    authority_version: int


class ProviderBearer:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""

    __slots__ = ("__value",)
    __value: str

    def __init__(self, value: str) -> None:
        if type(value) is not str or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", value) is None:
            raise ValueError("invalid_lookup_context") from None
        object.__setattr__(self, "_ProviderBearer__value", value)

    @property
    def value(self) -> str:
        return self.__value

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("immutable_provider_bearer")

    def __repr__(self) -> str:
        return "ProviderBearer()"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        raise TypeError("provider_bearer_not_serializable")


@dataclass(frozen=True, slots=True)
class CloseReport:
    """@spec HTTP-IDENTITY-TRANSPORT-4."""

    state: Literal["open", "closing", "closed"]
    pending_exchanges: int
    pending_parsers: int
    worker_joined: bool
    close_failed: bool


class IdentityTransportInternalError(RuntimeError):
    """@spec HTTP-IDENTITY-TRANSPORT-2."""

    def __init__(self) -> None:
        super().__init__("identity_transport_internal_error")


class IdentityShutdownIncomplete(RuntimeError):
    """@spec HTTP-IDENTITY-TRANSPORT-4."""

    def __init__(self, report: CloseReport) -> None:
        self.report = report
        super().__init__(report)


class IdentityClientClosed(RuntimeError):
    """@spec HTTP-IDENTITY-TRANSPORT-4."""

    def __init__(self) -> None:
        super().__init__("identity_client_closed")


@dataclass(eq=False, repr=False)
class _Operation:
    """@spec HTTP-IDENTITY-TRANSPORT-3, HTTP-IDENTITY-TRANSPORT-4."""

    task: asyncio.Task[Result] | None = None
    transport: httpx.AsyncHTTPTransport | None = None
    response: httpx.Response | None = None
    closer: asyncio.Task[None] | None = None
    parser: Future[Result] | None = None
    network_closed: bool = False
    close_failed: bool = False


def _verified(context: ssl.SSLContext | None) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    if context is not None and (
        not isinstance(context, ssl.SSLContext)
        or context.verify_mode != ssl.CERT_REQUIRED
        or not context.check_hostname
    ):
        raise ValueError("invalid_lookup_context") from None


def _endpoint(value: str, allow_http: bool) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    if type(value) is not str or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError
    parts = urlsplit(value)
    if (
        not parts.hostname or "@" in parts.netloc or "?" in value or "#" in value
        or parts.netloc.endswith(":") or (parts.port is not None and parts.port < 1)
        or not (
            parts.scheme == "https"
            or (allow_http and parts.scheme == "http" and parts.hostname in {"127.0.0.1", "::1"})
        )
    ):
        raise ValueError
    # Have the public URL parser reject URL syntax it cannot represent before admission.
    parsed = httpx.URL(value)
    if not parsed.is_absolute_url or not parsed.host:
        raise ValueError


def _fresh(result: Result) -> Result:
    """@spec HTTP-IDENTITY-TRANSPORT-3."""
    if isinstance(result, ValidatedFacts):
        now = datetime.now(UTC)
        seconds = (now.toordinal() - datetime(1970, 1, 1).toordinal()) * 86_400
        seconds += now.hour * 3600 + now.minute * 60 + now.second
        exact = Fraction(seconds) + Fraction(now.microsecond, 1_000_000)
        # T1 accepts equality; expiration occurs strictly after the expiry instant.
        if exact > result.identity_expires_at:
            return FactsRefusal(code="identity_stale")
        if result.membership_expires_at is not None and exact > result.membership_expires_at:
            return FactsRefusal(code="membership_stale")
    return result


def _valid_headers(response: httpx.Response) -> bool:
    """@spec HTTP-IDENTITY-TRANSPORT-2."""
    types = response.headers.get_list("content-type")
    encodings = response.headers.get_list("content-encoding")
    return (
        len(types) == 1
        and re.fullmatch(
            r'application/json[ \t]*(?:;[ \t]*charset[ \t]*=[ \t]*(?:utf-8|"utf-8")[ \t]*)?',
            types[0], re.IGNORECASE,
        ) is not None
        and (
            not encodings
            or (len(encodings) == 1 and encodings[0].strip(" \t").lower() == "identity")
        )
    )


class HttpIdentityClient:
    """@spec HTTP-IDENTITY-TRANSPORT-1 to HTTP-IDENTITY-TRANSPORT-5."""

    def __init__(
        self, *, tls_context: ssl.SSLContext | None = None, allow_loopback_http: bool = False,
    ) -> None:
        _verified(tls_context)
        if type(allow_loopback_http) is not bool:
            raise ValueError("invalid_lookup_context") from None
        self._tls = tls_context
        self._allow_http = allow_loopback_http
        self._closing = False
        self._operations: set[_Operation] = set()
        self._parsers: set[Future[Result]] = set()
        self._worker: ThreadPoolExecutor | None = None
        self._joiner: threading.Thread | None = None
        self._changed = asyncio.Event()

    def _notify(self) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-3, HTTP-IDENTITY-TRANSPORT-4."""
        self._changed.set()
        self._changed = asyncio.Event()

    def _release(self, operation: _Operation) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        if (
            operation.task is not None and operation.task.done()
            and operation.network_closed and operation.parser is None
        ):
            self._operations.discard(operation)
        self._notify()

    async def lookup(
        self, *, target: LookupTarget, bearer: ProviderBearer,
        request: LookupRequestV1, limits: FreshnessLimits,
    ) -> Result:
        """@spec HTTP-IDENTITY-TRANSPORT-1 to HTTP-IDENTITY-TRANSPORT-5."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2.0
        unavailable = FactsRefusal(code="unavailable")
        if self._closing:
            return unavailable
        try:
            _verified(self._tls)
            if not isinstance(target, LookupTarget) or not isinstance(bearer, ProviderBearer):
                raise ValueError
            captured = LookupTarget(
                target.endpoint, target.tenant_id, target.expected_issuer, target.authority_version,
            )
            _endpoint(captured.endpoint, self._allow_http)
            secret = ProviderBearer(bearer.value).value
            if not isinstance(request, LookupRequestV1) or not isinstance(limits, FreshnessLimits):
                raise ValueError
            req = LookupRequestV1.model_validate(request.model_dump(warnings=False))
            budget = FreshnessLimits.model_validate(limits.model_dump(warnings=False))
            if (
                type(captured.authority_version) is not int
                or captured.tenant_id != req.scope.tenant_id
                or captured.expected_issuer != req.scope.expected_issuer
                or captured.authority_version != req.authority_version
            ):
                raise ValueError
            body = req.model_dump_json().encode("utf-8")
        except (ValueError, TypeError, AttributeError, RecursionError, httpx.InvalidURL):
            raise ValueError("invalid_lookup_context") from None
        operation: _Operation | None = None
        try:
            async with asyncio.timeout_at(deadline):
                while len(self._operations) >= 2 and not self._closing:
                    await self._changed.wait()
                if self._closing or loop.time() >= deadline:
                    return unavailable
                operation = _Operation()
                self._operations.add(operation)
                with _dependency_log_guard():
                    task = asyncio.create_task(
                        self._run(
                            operation, captured.endpoint, secret, body, req, budget, deadline,
                        ),
                        name="identity-exchange",
                    )
                operation.task = task
                task.add_done_callback(lambda done: self._operation_done(operation, done))
                result = await asyncio.shield(task)
                if self._closing or loop.time() >= deadline:
                    return unavailable
                result = _fresh(result)
                return unavailable if loop.time() >= deadline else result
        except TimeoutError:
            if operation is not None:
                self._cancel(operation)
            return unavailable
        except asyncio.CancelledError:
            if operation is not None:
                self._cancel(operation)
            caller = asyncio.current_task()
            if self._closing and caller is not None and not caller.cancelling():
                # An owned exchange can be cancelled by shutdown or failed close.
                # Only cancellation requested by this caller propagates to it.
                return unavailable
            raise
        except Exception:  # noqa: BLE001 - sanitize unexpected internal failures at the boundary.
            raise IdentityTransportInternalError() from None

    def _operation_done(self, operation: _Operation, task: asyncio.Task[Result]) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        if not task.cancelled():
            task.exception()
        if operation.closer is None:
            self._start_close(operation)
        self._release(operation)

    def _cancel(self, operation: _Operation) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        if (
            operation.task is not None
            and not operation.task.done() and not operation.task.cancelling()
        ):
            operation.task.cancel()
        if operation.parser is not None:
            operation.parser.cancel()

    async def _run(
        self, operation: _Operation, endpoint: str, secret: str, body: bytes,
        request: LookupRequestV1, limits: FreshnessLimits, deadline: float,
    ) -> Result:
        """@spec HTTP-IDENTITY-TRANSPORT-1 to HTTP-IDENTITY-TRANSPORT-5."""
        try:
            if self._closing or asyncio.get_running_loop().time() >= deadline:
                return FactsRefusal(code="unavailable")
            operation.transport = httpx.AsyncHTTPTransport(
                verify=self._tls if self._tls is not None else True,
                trust_env=False, http1=True, http2=False, retries=0,
            )
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return FactsRefusal(code="unavailable")
            outbound = httpx.Request(
                "POST", endpoint, content=body,
                headers={
                    "Content-Type": "application/json", "Accept": "application/json",
                    "Accept-Encoding": "identity", "Authorization": "Bearer " + secret,
                },
                extensions={"timeout": httpx.Timeout(remaining).as_dict()},
            )
            operation.response = await operation.transport.handle_async_request(outbound)
            response = operation.response
            if response.status_code != 200:
                return FactsRefusal(code="unavailable")
            if not _valid_headers(response):
                return FactsRefusal(code="invalid_response")
            length = response.headers.get("content-length")
            if length is not None and length.isascii() and length.isdecimal():
                # Compare decimal strings without converting an unbounded integer.
                digits = length.lstrip("0") or "0"
                if len(digits) > 5 or (len(digits) == 5 and digits > "65536"):
                    return FactsRefusal(code="invalid_response")
            entity = bytearray()
            async for chunk in response.aiter_raw():
                if len(entity) + len(chunk) > MAX_RESPONSE_BYTES:
                    return FactsRefusal(code="invalid_response")
                entity.extend(chunk)
            while self._parsers and not self._closing:
                await self._changed.wait()
            if self._closing or asyncio.get_running_loop().time() >= deadline:
                return FactsRefusal(code="unavailable")
            if self._worker is None:
                self._worker = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="identity-parser",
                )
            loop = asyncio.get_running_loop()

            def parse() -> Result:
                """@spec HTTP-IDENTITY-TRANSPORT-3."""
                return validate_lookup(bytes(entity), request, limits, now=datetime.now(UTC))

            future = self._worker.submit(parse)
            operation.parser = future
            self._parsers.add(future)
            future.add_done_callback(
                lambda done: loop.call_soon_threadsafe(self._parser_done, operation, done)
            )
            wrapped = asyncio.wrap_future(future)
            wrapped.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            return await asyncio.shield(wrapped)
        except httpx.TransportError:
            return FactsRefusal(code="unavailable")
        finally:
            self._start_close(operation)
            assert operation.closer is not None
            await asyncio.shield(operation.closer)

    def _parser_done(self, operation: _Operation, future: Future[Result]) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-3, HTTP-IDENTITY-TRANSPORT-4."""
        if not future.cancelled():
            future.exception()
        self._parsers.discard(future)
        operation.parser = None
        self._release(operation)

    def _start_close(self, operation: _Operation) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4, HTTP-IDENTITY-TRANSPORT-5."""
        if (
            operation.network_closed
            or (operation.closer is not None and not operation.closer.done())
        ):
            return
        with _dependency_log_guard():
            operation.closer = asyncio.create_task(
                self._close_network(operation), name="identity-close",
            )

    async def _close_network(self, operation: _Operation) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4, HTTP-IDENTITY-TRANSPORT-5."""
        try:
            if operation.response is not None:
                await operation.response.aclose()
                operation.response = None
            if operation.transport is not None:
                await operation.transport.aclose()
                operation.transport = None
            operation.network_closed = True
            operation.close_failed = False
        except Exception:  # noqa: BLE001 - retain failed ownership without exposing diagnostics.
            operation.close_failed = True
            self._begin_close()
        finally:
            self._release(operation)

    def _begin_close(self) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        if self._closing:
            return
        self._closing = True
        self._notify()
        for operation in tuple(self._operations):
            self._cancel(operation)
        if self._worker is not None:
            worker = self._worker
            loop = asyncio.get_running_loop()

            def join() -> None:
                """@spec HTTP-IDENTITY-TRANSPORT-4."""
                worker.shutdown(wait=True, cancel_futures=True)
                loop.call_soon_threadsafe(self._notify)

            self._joiner = threading.Thread(target=join, name="identity-parser-joiner")
            self._joiner.start()

    def close_status(self) -> CloseReport:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        joined = self._worker is None or (self._joiner is not None and not self._joiner.is_alive())
        failed = any(operation.close_failed for operation in self._operations)
        state: Literal["open", "closing", "closed"] = "open"
        if self._closing:
            drained = not self._operations and not self._parsers and joined and not failed
            state = "closed" if drained else "closing"
        return CloseReport(state, len(self._operations), len(self._parsers), joined, failed)

    async def aclose(self) -> CloseReport:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        deadline = asyncio.get_running_loop().time() + 0.250
        self._begin_close()
        for operation in tuple(self._operations):
            if operation.close_failed:
                self._start_close(operation)
        try:
            async with asyncio.timeout_at(deadline):
                while self.close_status().state != "closed":
                    # The joiner can signal just before its thread actually exits.
                    try:
                        async with asyncio.timeout(0.01):
                            await self._changed.wait()
                    except TimeoutError:
                        pass
        except TimeoutError:
            pass
        return self.close_status()

    async def __aenter__(self) -> HttpIdentityClient:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        if self._closing:
            raise IdentityClientClosed() from None
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, traceback: object,
    ) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        report = await self.aclose()
        if exc is None and report.state != "closed":
            raise IdentityShutdownIncomplete(report) from None
