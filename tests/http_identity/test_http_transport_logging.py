"""Emitter-level privacy with real HTTPCore DEBUG and queued logging.

@spec HTTP-IDENTITY-TRANSPORT-5
"""

from __future__ import annotations

import asyncio
import logging
import logging.handlers
import queue
from typing import Any

import httpx
import pytest

EMITTERS = ("httpcore.connection", "httpcore.http11", "httpx")


class Capture(logging.Handler):
    """@spec HTTP-IDENTITY-TRANSPORT-5: records are formatted on a different logging thread."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-5."""
        self.messages.append(record.name + ": " + self.format(record))


@pytest.fixture
def queued_logs() -> Any:
    """@spec HTTP-IDENTITY-TRANSPORT-5: filtering must happen before QueueHandler emits."""
    channel: queue.Queue[Any] = queue.Queue()
    capture = Capture()
    handler = logging.handlers.QueueHandler(channel)
    listener = logging.handlers.QueueListener(channel, capture)
    loggers = [logging.getLogger(name) for name in EMITTERS]
    saved = [(logger, logger.level, list(logger.handlers), logger.propagate) for logger in loggers]
    try:
        for logger in loggers:
            logger.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        listener.start()
        yield capture, loggers
    finally:
        listener.stop()
        for logger, level, handlers, propagate in saved:
            logger.setLevel(level)
            logger.handlers[:] = handlers
            logger.propagate = propagate


@pytest.mark.parametrize("provider", ["success", "refused", "protocol_error", "cancelled"])
def test_actual_emitters_suppress_reflection_before_queue_while_unrelated_http_remains_visible(
    support: Any,
    run_transport: Any,
    queued_logs: Any,
    provider: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-5: reason/header/error reflection cannot leak a bearer."""
    module = support.api()
    capture, loggers = queued_logs
    configurations = [(logger.level, list(logger.handlers), logger.propagate) for logger in loggers]

    async def scenario(rig: Any) -> None:
        release, arrived = rig.event(), asyncio.Event()
        reflected = (support.token + support.marker).encode()

        async def hostile(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-5: send provider-controlled wire diagnostics."""
            arrived.set()
            if provider == "cancelled":
                await release.wait()
            if provider == "protocol_error":
                writer.write(b"HTTP/1.1 200 " + reflected + b"\r\n" + reflected + b"\r\n\r\n")
            elif provider == "refused":
                writer.write(
                    support.wire(
                        reflected,
                        status=b"403 " + reflected,
                        headers=[
                            (b"Content-Type", b"text/plain"),
                            (b"X-Reflection", reflected),
                        ],
                    )
                )
            else:
                writer.write(
                    support.wire(
                        support.response(),
                        status=b"200 " + reflected,
                        headers=[
                            (b"Content-Type", b"application/json"),
                            (b"X-Reflection", reflected),
                        ],
                    )
                )
            await writer.drain()

        listener = await rig.listen(hostile)
        unrelated = await rig.listen(
            payload=b"HTTP/1.1 200 Ordinary\r\nContent-Length: 2\r\n\r\nok"
        )
        client = rig.client(module, allow_loopback_http=True)
        protected = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await asyncio.wait_for(arrived.wait(), 1)
        # This separate task inherits the caller's unguarded context. It must be visible
        # while another request is inside the protected lookup, including cancellation.
        async with httpx.AsyncClient(trust_env=False) as ordinary:
            reply = await ordinary.get(unrelated.url)
            assert reply.content == b"ok"
        if provider == "cancelled":
            protected.cancel()
            with pytest.raises(asyncio.CancelledError):
                await protected
            release.set()
        else:
            result = await protected
            if provider == "success":
                assert isinstance(result, support.facts().ValidatedFacts)
            else:
                support.refusal(result, "unavailable")
            assert support.token not in repr(result) and support.marker not in repr(result)
        assert support.token not in repr(client.close_status())
        assert all(support.token not in task.get_name() for task in asyncio.all_tasks())
        # Prove actual emitter records arrive again in the caller's restored context.
        for logger in loggers:
            logger.debug("OrdinaryAfterProtectedLookup")
        await client.aclose()

    run_transport(scenario)
    # Stop/wait is owned by fixture teardown; a FIFO sentinel is not needed to
    # validate absence because these handlers enqueue synchronously before return.
    queued_handler = next(
        h for h in loggers[0].handlers if isinstance(h, logging.handlers.QueueHandler)
    )
    queued_handler.queue.join()
    text = "\n".join(capture.messages)
    assert support.token not in text and support.marker not in text
    assert "HTTP Request:" in text and "Ordinary" in text
    assert "httpcore.connection: connect_tcp.started" in text
    assert "httpcore.http11: receive_response_headers.complete" in text
    for name in EMITTERS:
        assert any(message.startswith(name + ":") for message in capture.messages)
        assert name + ": OrdinaryAfterProtectedLookup" in text
    assert [
        (logger.level, list(logger.handlers), logger.propagate) for logger in loggers
    ] == configurations


def test_concurrent_clients_keep_one_stable_guard_and_distinct_credentials(
    support: Any,
    run_transport: Any,
    queued_logs: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-5."""
    module = support.api()
    capture, loggers = queued_logs

    async def scenario(rig: Any) -> None:
        listener = await rig.listen()
        clients = [rig.client(module, allow_loopback_http=True) for _ in range(2)]
        tokens = [support.token, "Independent.Second.Provider.Secret=="]

        async def lookup(index: int) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-5."""
            return await clients[index].lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(tokens[index]),
                request=support.request(),
                limits=support.limits(),
            )

        assert isinstance(await lookup(0), support.facts().ValidatedFacts)
        stable = [tuple(map(id, logger.filters)) for logger in loggers]
        for _ in range(2):
            for result in await asyncio.gather(lookup(0), lookup(1)):
                assert isinstance(result, support.facts().ValidatedFacts)
            assert [tuple(map(id, logger.filters)) for logger in loggers] == stable
        seen = [headers["authorization"] for _, headers, _ in listener.requests]
        assert seen.count("Bearer " + tokens[0]) == 3
        assert seen.count("Bearer " + tokens[1]) == 2
        for index in (0, 1):
            await clients[index].aclose()
            assert [tuple(map(id, logger.filters)) for logger in loggers] == stable
        for logger in loggers:
            logger.debug("OrdinaryAfterConcurrentClients")

    run_transport(scenario)
    queued_handler = next(
        h for h in loggers[0].handlers if isinstance(h, logging.handlers.QueueHandler)
    )
    queued_handler.queue.join()
    text = "\n".join(capture.messages)
    assert support.token not in text and "Independent.Second.Provider.Secret" not in text
    assert "OrdinaryAfterConcurrentClients" in text


def test_private_dependency_guard_restores_nested_tokens_and_child_cleanup_context(
    support: Any,
    run_transport: Any,
    queued_logs: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-5: an inner reset cannot unguard its parent."""
    module = support.api()
    capture, loggers = queued_logs

    async def scenario(rig: Any) -> None:
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        assert isinstance(
            await client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            ),
            support.facts().ValidatedFacts,
        )
        guard = module._dependency_log_guard
        release_child, release_unrelated = rig.event(), rig.event()

        async def emit_after(event: Any, message: str) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-5."""
            await event.wait()
            for logger in loggers:
                logger.debug(message)

        unrelated = rig.task(emit_after(release_unrelated, "OrdinaryDuringNestedGuard"))
        stable = [tuple(map(id, logger.filters)) for logger in loggers]
        with guard():
            for logger in loggers:
                logger.debug("OuterGuardSecret " + support.token)
            with guard():
                for logger in loggers:
                    logger.debug("InnerGuardSecret " + support.token)
            for logger in loggers:
                logger.debug("OuterAfterInnerSecret " + support.token)
            child = rig.task(emit_after(release_child, "OwnedChildSecret " + support.token))
            release_unrelated.set()
            await unrelated
        for logger in loggers:
            logger.debug("OrdinaryAfterNestedGuard")
        release_child.set()
        await child
        assert [tuple(map(id, logger.filters)) for logger in loggers] == stable

    run_transport(scenario)
    handler = next(h for h in loggers[0].handlers if isinstance(h, logging.handlers.QueueHandler))
    handler.queue.join()
    text = "\n".join(capture.messages)
    assert support.token not in text
    assert "OrdinaryDuringNestedGuard" in text and "OrdinaryAfterNestedGuard" in text
    assert "OuterGuardSecret" not in text and "InnerGuardSecret" not in text
    assert "OuterAfterInnerSecret" not in text and "OwnedChildSecret" not in text
