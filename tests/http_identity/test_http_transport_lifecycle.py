"""Deadline, cancellation and actual retained ownership through the pure parser seam.

@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest


async def wait_thread_event(event: threading.Event) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: no default executor is introduced by the test."""
    async with asyncio.timeout(1):
        while not event.is_set():
            await asyncio.sleep(0.005)


def test_timed_out_parser_retains_its_physical_slot_and_never_delivers_late_facts(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        original = support.facts().validate_lookup
        entered, release = threading.Event(), rig.event(thread=True)
        calls: list[int] = []

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3: this seam alone intentionally stalls."""
            calls.append(threading.get_ident())
            if len(calls) == 1:
                entered.set()
                assert release.wait(8), "fixture failed to release the parser"
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        kwargs = {
            "target": support.target(module, listener.url),
            "bearer": module.ProviderBearer(support.token),
            "request": support.request(),
            "limits": support.limits(),
        }
        start = asyncio.get_running_loop().time()
        first = rig.task(client.lookup(**kwargs))
        await wait_thread_event(entered)
        first_result = await first
        support.refusal(first_result, "unavailable")
        assert 1.8 <= asyncio.get_running_loop().time() - start < 2.35
        assert client.close_status().pending_parsers == 1
        second_start = asyncio.get_running_loop().time()
        second_result = await client.lookup(**kwargs)
        support.refusal(second_result, "unavailable")
        assert 1.8 <= asyncio.get_running_loop().time() - second_start < 2.35
        assert len(calls) == 1, "cancelled asyncio wrapper released a still-running parser slot"
        assert client.close_status().pending_parsers == 1
        assert first.result() is first_result
        release.set()
        async with asyncio.timeout(1):
            while client.close_status().pending_parsers:
                await asyncio.sleep(0.005)
        third = await client.lookup(**kwargs)
        assert isinstance(third, module.HttpIdentityObservation)
        assert len(calls) == 2 and len(set(calls)) == 1
        assert first.result() is first_result and second_result.code == "unavailable"

    run_transport(scenario)


@pytest.mark.parametrize("phase", ["headers", "body", "tls"])
def test_caller_cancellation_during_real_io_propagates_without_retry_and_eventually_drains(
    support: Any,
    run_transport: Any,
    phase: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = asyncio.Event(), rig.event()

        async def stall(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            if phase == "body":
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Content-Length: 100\r\n\r\n{"
                )
                await writer.drain()
            elif phase == "tls":
                assert await reader.read(1)
            entered.set()
            await release.wait()

        listener = await rig.listen(stall, parse_request=phase != "tls")
        client = rig.client(module, allow_loopback_http=phase != "tls")
        url = listener.url.replace("http:", "https:") if phase == "tls" else listener.url
        task = rig.task(
            client.lookup(
                target=support.target(module, url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await asyncio.wait_for(entered.wait(), 1)
        start = asyncio.get_running_loop().time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert asyncio.get_running_loop().time() - start < 0.35
        assert listener.connections == 1
        release.set()
        report = await client.aclose()
        assert report.state in {"closing", "closed"}
        assert report.pending_parsers == 0

    run_transport(scenario)


def test_two_operation_slots_cancelled_waiter_snapshot_and_close_wakeup(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()
        two_arrived = asyncio.Event()
        arrivals = 0

        async def block(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            nonlocal arrivals
            arrivals += 1
            if arrivals == 2:
                two_arrived.set()
            if arrivals <= 2:
                await release.wait()
            writer.write(support.wire(support.response()))
            await writer.drain()

        listener = await rig.listen(block)
        client = rig.client(module, allow_loopback_http=True)
        kwargs = {
            "target": support.target(module, listener.url),
            "bearer": module.ProviderBearer(support.token),
            "request": support.request(),
            "limits": support.limits(),
        }
        first, second = [rig.task(client.lookup(**kwargs)) for _ in range(2)]
        await asyncio.wait_for(two_arrived.wait(), 1)
        cancelled = rig.task(client.lookup(**kwargs))
        await asyncio.sleep(0.03)
        assert listener.connections == 2
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        captured = support.request()
        third = rig.task(client.lookup(**{**kwargs, "request": captured}))
        await asyncio.sleep(0.03)
        # Bypass frozen-model protection to distinguish a deep capture from an alias.
        object.__setattr__(captured.scope.sender.namespace, "key", "ChangedAfterLookupEntry")
        object.__setattr__(captured, "authority_version", 99)
        release.set()
        for result in await asyncio.gather(first, second, third):
            assert isinstance(result, module.HttpIdentityObservation)
        assert listener.connections == 3
        third_wire = __import__("json").loads(listener.requests[2][2])
        assert third_wire["authority_version"] == 7
        assert third_wire["scope"]["sender"]["namespace"]["key"] == "ExampleWorkspace"

        # Reoccupy both slots, then close with two admission waiters.
        other_release = rig.event()

        async def hang(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            await other_release.wait()

        other = await rig.listen(hang)
        next_kwargs = {**kwargs, "target": support.target(module, other.url)}
        active = [rig.task(client.lookup(**next_kwargs)) for _ in range(2)]
        async with asyncio.timeout(1):
            while other.connections != 2:
                await asyncio.sleep(0.005)
        waiting = [rig.task(client.lookup(**next_kwargs)) for _ in range(2)]
        await asyncio.sleep(0.02)
        start = asyncio.get_running_loop().time()
        report = await client.aclose()
        assert asyncio.get_running_loop().time() - start < 0.4
        assert report.state in {"closing", "closed"}
        for task in waiting:
            support.refusal(await asyncio.wait_for(task, 0.4), "unavailable")
        assert other.connections == 2
        for task in active:
            try:
                support.refusal(await asyncio.wait_for(task, 0.4), "unavailable")
            except asyncio.CancelledError:
                pass  # Close cancels the exchange; caller cancellation is tested separately.
        # Close is checked before invalid input is considered.
        support.refusal(
            await client.lookup(
                target=None,
                bearer=None,
                request=None,
                limits=None,
            ),
            "unavailable",
        )

    run_transport(scenario)


def test_waiting_for_operation_slot_has_no_fresh_two_second_budget(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: admission time consumes the original deadline."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()
        exchange_starts: list[tuple[float, dict[str, float]]] = []
        original = httpx.AsyncHTTPTransport.handle_async_request

        async def exchange(transport: Any, request: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3: observe starts, delegating real HTTP I/O."""
            exchange_starts.append(
                (asyncio.get_running_loop().time(), dict(request.extensions["timeout"]))
            )
            return await original(transport, request)

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", exchange)

        async def stall(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            await release.wait()

        listener = await rig.listen(stall)
        client = rig.client(module, allow_loopback_http=True)
        kwargs = {
            "target": support.target(module, listener.url),
            "bearer": module.ProviderBearer(support.token),
            "request": support.request(),
            "limits": support.limits(),
        }
        active = [rig.task(client.lookup(**kwargs)) for _ in range(2)]
        async with asyncio.timeout(1):
            while listener.connections != 2:
                await asyncio.sleep(0.005)
        assert len(exchange_starts) == 2
        start = asyncio.get_running_loop().time()
        queued = rig.task(client.lookup(**kwargs))
        support.refusal(await queued, "unavailable")
        assert 1.8 <= asyncio.get_running_loop().time() - start < 2.35
        # The older operations may drain just before this waiter's D and allow
        # one brief exchange. Neither admission nor that exchange restarts D.
        at_return = tuple(exchange_starts)
        assert 2 <= len(at_return) <= 3
        for began, timeout in at_return[2:]:
            assert began < start + 2.0
            assert all(0 < value <= start + 2.0 - began + 0.01 for value in timeout.values())
        await asyncio.sleep(0.03)
        assert tuple(exchange_starts) == at_return
        # A connection begun before D can be accepted by the listener after
        # lookup returns; server scheduling does not mark the exchange start.
        assert 2 <= listener.connections <= 3
        for result in await asyncio.gather(*active):
            support.refusal(result, "unavailable")

    run_transport(scenario)


def test_cancelled_parser_retained_through_repeated_bounded_and_cancelled_close(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = threading.Event(), rig.event(thread=True)
        original = support.facts().validate_lookup

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            entered.set()
            assert release.wait(8)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        task = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await wait_thread_event(entered)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.close_status().pending_parsers == 1
        waiting = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        async with asyncio.timeout(1):
            while listener.connections != 2:
                await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert client.close_status().pending_parsers == 1
        for _ in range(2):
            # A loop heartbeat proves shutdown does not synchronously join the worker.
            heartbeat = rig.task(asyncio.sleep(0.02, result="responsive"))
            start = asyncio.get_running_loop().time()
            report = await client.aclose()
            assert asyncio.get_running_loop().time() - start < 0.4
            assert await heartbeat == "responsive"
            assert report.state == "closing" and report.pending_parsers == 1
            assert not report.worker_joined and not report.close_failed
        threads = [t for t in threading.enumerate() if t.ident not in rig.thread_ids]
        assert 1 <= len(threads) <= 2, "shutdown created per-attempt workers/joiners"
        for _ in range(2):
            closer = rig.task(client.aclose())
            await asyncio.sleep(0.02)
            closer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closer
            assert client.close_status().state == "closing"
            assert client.close_status().pending_parsers == 1
        support.refusal(
            await client.lookup(target=None, bearer=None, request=None, limits=None), "unavailable"
        )
        release.set()
        async with asyncio.timeout(1.5):
            while (report := await client.aclose()).state != "closed":
                await asyncio.sleep(0.01)
        assert report.worker_joined and report.pending_parsers == report.pending_exchanges == 0
        assert support.report(await client.aclose()) == support.report(report)

    run_transport(scenario)


@pytest.mark.parametrize("kind", ["identity", "membership", "both"])
def test_final_return_rechecks_fresh_utc_after_pure_validation(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: facts that expire during validation cannot escape."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        original = support.facts().validate_lookup
        seen: list[Any] = []

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3: delay only after actual T1 validation."""
            result = original(*args, **kwargs)
            seen.append(result)
            # Sleep in the owned parser thread, never the event loop.
            threading.Event().wait(0.55)
            return result

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        budget = support.limits(
            **{
                **({"max_identity_age_ms": 400} if kind in {"identity", "both"} else {}),
                **({"max_membership_age_ms": 400} if kind in {"membership", "both"} else {}),
            }
        )
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=budget,
        )
        assert len(seen) == 1 and isinstance(seen[0], support.facts().ValidatedFacts)
        support.refusal(result, "membership_stale" if kind == "membership" else "identity_stale")

    run_transport(scenario)


def test_parse_now_is_sampled_when_worker_starts_after_slot_wait(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: queued parse cannot reuse an earlier UTC sample."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        original = support.facts().validate_lookup
        entered, release = threading.Event(), rig.event(thread=True)
        samples: list[datetime] = []

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            samples.append(kwargs["now"])
            if len(samples) == 1:
                entered.set()
                assert release.wait(8)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        kwargs = {
            "target": support.target(module, listener.url),
            "bearer": module.ProviderBearer(support.token),
            "request": support.request(),
            "limits": support.limits(),
        }
        first = rig.task(client.lookup(**kwargs))
        await wait_thread_event(entered)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = rig.task(client.lookup(**kwargs))
        await asyncio.sleep(0.25)
        released_at = datetime.now(UTC)
        release.set()
        assert isinstance(await second, module.HttpIdentityObservation)
        assert len(samples) == 2 and samples[1] >= released_at

    run_transport(scenario)


@pytest.mark.parametrize("parser_result", ["facts", "refusal"])
def test_deadline_precedes_late_parser_facts_or_refusal(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    parser_result: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: even a definitive refusal cannot reset D."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event(thread=True)
        entered = threading.Event()
        original = support.facts().validate_lookup

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            result = (
                original(*args, **kwargs)
                if parser_result == "facts"
                else support.facts().FactsRefusal(code="scope_mismatch")
            )
            entered.set()
            assert release.wait(8)
            return result

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        task = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await wait_thread_event(entered)
        support.refusal(await task, "unavailable")
        release.set()
        await asyncio.sleep(0.03)
        support.refusal(task.result(), "unavailable")

    run_transport(scenario)


def test_unexpected_pure_parser_exception_is_sanitized_and_cleanup_still_runs(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()
    caplog.set_level(logging.DEBUG)

    def parse(*args: Any, **kwargs: Any) -> Any:
        """@spec HTTP-IDENTITY-TRANSPORT-2."""
        raise RuntimeError(support.marker + support.token)

    monkeypatch.setattr(module, "validate_lookup", parse)

    async def scenario(rig: Any) -> None:
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        with pytest.raises(module.IdentityTransportInternalError) as error:
            await client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        assert str(error.value) == "identity_transport_internal_error"
        assert error.value.__cause__ is None and error.value.__suppress_context__
        assert support.marker not in repr(error.value)
        assert support.token not in repr(client.close_status())

    run_transport(scenario)
    assert support.marker not in caplog.text and support.token not in caplog.text


@pytest.mark.parametrize("body_failure", ["none", "error", "cancellation"])
def test_context_exit_reports_incomplete_shutdown_or_preserves_body_exception(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    body_failure: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-4: context exit never turns a blocked worker into closed."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = threading.Event(), rig.event(thread=True)
        original = support.facts().validate_lookup

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            entered.set()
            assert release.wait(8)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        body_error = (
            asyncio.CancelledError()
            if body_failure == "cancellation"
            else RuntimeError("Synthetic body exception")
        )
        expected = type(body_error) if body_failure != "none" else module.IdentityShutdownIncomplete
        with pytest.raises(expected) as error:
            async with client:
                task = rig.task(
                    client.lookup(
                        target=support.target(module, listener.url),
                        bearer=module.ProviderBearer(support.token),
                        request=support.request(),
                        limits=support.limits(),
                    )
                )
                await wait_thread_event(entered)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                if body_failure != "none":
                    raise body_error
        if body_failure != "none":
            assert error.value is body_error
        else:
            assert support.token not in repr(error.value)
            assert support.marker not in str(error.value)
        assert client.close_status().state == "closing"
        assert client.close_status().pending_parsers == 1
        with pytest.raises(module.IdentityClientClosed) as closed:
            await client.__aenter__()
        assert support.token not in str(closed.value)
        release.set()

    run_transport(scenario)


@pytest.mark.parametrize("body_failure", ["error", "cancellation"])
def test_cancellation_during_context_exit_preserves_original_body_exception_and_drains(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    body_failure: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-4: cancellation during exit preserves the body error."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = threading.Event(), rig.event(thread=True)
        original = support.facts().validate_lookup

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-4: hold actual owned parser work through exit."""
            entered.set()
            assert release.wait(8), "fixture failed to release the parser"
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        body_error = (
            asyncio.CancelledError("Synthetic original body cancellation")
            if body_failure == "cancellation"
            else RuntimeError("Synthetic original body exception")
        )

        async def context_user() -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            async with client:
                rig.task(
                    client.lookup(
                        target=support.target(module, listener.url),
                        bearer=module.ProviderBearer(support.token),
                        request=support.request(),
                        limits=support.limits(),
                    )
                )
                await wait_thread_event(entered)
                raise body_error

        task = rig.task(context_user())
        async with asyncio.timeout(1):
            while client.close_status().state != "closing":
                await asyncio.sleep(0.005)
        # The public closing state proves exit has begun its bounded close wait.
        assert client.close_status().pending_parsers == 1
        task.cancel("Synthetic later cancellation during context exit")
        observed: BaseException | None = None
        try:
            await task
        except (RuntimeError, asyncio.CancelledError) as error:
            observed = error
        report = client.close_status()
        assert report.state == "closing" and report.pending_parsers == 1
        assert not report.worker_joined and not report.close_failed
        assert listener.connections == 1
        release.set()
        async with asyncio.timeout(1.5):
            while (report := await client.aclose()).state != "closed":
                await asyncio.sleep(0.01)
        assert report.worker_joined and not report.close_failed
        assert report.pending_exchanges == report.pending_parsers == 0
        assert observed is body_error, f"context exit replaced the body error: {observed!r}"

    run_transport(scenario)


def test_unused_client_closes_without_starting_worker_and_remains_closed(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-4: no-worker shutdown is complete and idempotent."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        client = rig.client(module)
        initial = client.close_status()
        assert initial.state == "open" and initial.worker_joined
        assert initial.pending_exchanges == initial.pending_parsers == 0
        first = await client.aclose()
        assert first.state == "closed" and first.worker_joined and not first.close_failed
        assert support.report(await client.aclose()) == support.report(first)
        assert support.report(client.close_status()) == support.report(first)
        support.refusal(
            await client.lookup(target=None, bearer=None, request=None, limits=None), "unavailable"
        )

    run_transport(scenario)


def test_close_racing_active_parser_retains_the_worker_and_wakes_its_lookup(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-4: close owns live parse work without late delivery."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = threading.Event(), rig.event(thread=True)
        original = support.facts().validate_lookup

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-4."""
            entered.set()
            assert release.wait(8)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        task = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await wait_thread_event(entered)
        report = await client.aclose()
        assert report.state == "closing" and report.pending_parsers == 1
        assert not report.worker_joined
        try:
            support.refusal(await asyncio.wait_for(task, 0.4), "unavailable")
        except asyncio.CancelledError:
            pass
        assert task.done()
        release.set()

    run_transport(scenario)


def test_external_transport_close_failure_is_sanitized_retained_and_retryable(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-4 HTTP-IDENTITY-TRANSPORT-5.

    The sole external fault injection fails the first public HTTPX transport
    close, then invokes the real close on retry. The HTTP exchange stays real.
    """
    module = support.api()
    caplog.set_level(logging.DEBUG)

    async def scenario(rig: Any) -> None:
        original = httpx.AsyncHTTPTransport.aclose
        failed = False
        failed_transport: list[Any] = []
        acknowledged: list[Any] = []

        async def close(transport: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4: fail once, retaining the actual resource."""
            nonlocal failed
            if not failed:
                failed = True
                failed_transport.append(transport)
                raise RuntimeError(support.marker + support.token)
            await original(transport)
            acknowledged.append(transport)

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "aclose", close)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        try:
            result = await client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
            if isinstance(result, module.HttpIdentityObservation):
                assert failed_transport[0] in acknowledged
            else:
                support.refusal(result, "unavailable")
        except module.IdentityTransportInternalError as error:
            assert str(error) == "identity_transport_internal_error"
            assert error.__cause__ is None and error.__suppress_context__
        assert failed and listener.connections == 1
        report = client.close_status()
        # If the owner already performed an actual successful retry, closed
        # ownership is legitimate. Otherwise the failure must remain visible.
        if failed_transport[0] not in acknowledged:
            assert report.close_failed
            assert report.state == "closing" and report.pending_exchanges >= 1
        assert support.marker not in repr(report) and support.token not in repr(report)
        start = asyncio.get_running_loop().time()
        first = await client.aclose()
        assert asyncio.get_running_loop().time() - start < 0.4
        async with asyncio.timeout(1):
            while (report := await client.aclose()).state != "closed":
                assert report.close_failed or report.pending_exchanges or not report.worker_joined
                await asyncio.sleep(0.01)
        assert first.state in {"closing", "closed"}
        assert not report.close_failed and report.pending_exchanges == report.pending_parsers == 0
        assert report.worker_joined
        assert len(failed_transport) == 1
        assert failed_transport[0] in acknowledged

    run_transport(scenario)
    assert support.marker not in caplog.text and support.token not in caplog.text


def test_completed_facts_are_withheld_until_actual_network_close_and_discarded_at_deadline(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3 HTTP-IDENTITY-TRANSPORT-4."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        entered, release = asyncio.Event(), rig.event()
        original_close = httpx.AsyncHTTPTransport.aclose
        original_parse = support.facts().validate_lookup
        seen: list[Any] = []
        acknowledged: list[Any] = []

        async def close(transport: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-4: delay then perform actual public HTTPX close."""
            entered.set()
            await release.wait()
            await original_close(transport)
            acknowledged.append(transport)

        def parse(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3: retain the pure T1 seam result."""
            facts = original_parse(*args, **kwargs)
            seen.append(facts)
            return facts

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "aclose", close)
        monkeypatch.setattr(module, "validate_lookup", parse)
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        start = asyncio.get_running_loop().time()
        task = rig.task(
            client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert len(seen) == 1 and isinstance(seen[0], support.facts().ValidatedFacts)
        result = await task
        support.refusal(result, "unavailable")
        assert 1.8 <= asyncio.get_running_loop().time() - start < 2.35
        assert not acknowledged and client.close_status().pending_exchanges == 1
        report = await client.aclose()
        assert report.state == "closing" and report.pending_exchanges == 1
        assert not report.close_failed
        release.set()
        async with asyncio.timeout(1.5):
            while (report := await client.aclose()).state != "closed":
                await asyncio.sleep(0.01)
        assert len(acknowledged) == 1
        assert report.pending_exchanges == report.pending_parsers == 0 and report.worker_joined
        assert task.result() is result
        support.refusal(task.result(), "unavailable")

    run_transport(scenario)
