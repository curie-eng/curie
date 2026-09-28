"""Authenticated egress is a typed, bounded channel-protocol boundary."""

from __future__ import annotations

import json
import socket
import threading
import urllib.parse
import warnings
from collections.abc import Callable
from http.client import HTTPResponse, RemoteDisconnected

import pytest
from _support import (
    EGRESS_SECRET,
    INBOX,
    MailState,
    completed,
    get,
    post_bytes,
    post_event,
    update,
    wait_until,
)
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.egress import MAX_CONCURRENT_REQUESTS


def _seed(mail: MailState, adapter: MailAdapter) -> None:
    mail.add_inbound("msg-1", "thr-1")
    adapter.poll_once()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        {"version": "0.9", "event": "turn.status", "target": {}},
        {
            "version": "1.0",
            "event": "unknown.event",
            "target": {
                "kind": "email",
                "address": INBOX,
                "conversation_id": "thr-1",
                "reply_ref": "msg-1",
            },
        },
        {
            **update("forged"),
            "target": {**update("forged")["target"], "kind": "discord"},
        },
        {
            **update("forged"),
            "target": {**update("forged")["target"], "address": "other@example.com"},
        },
    ],
)
def test_invalid_authenticated_events_are_4xx_and_have_no_side_effect(
    mail: MailState,
    adapter: MailAdapter,
    egress_url: str,
    payload: object,
) -> None:
    _seed(mail, adapter)

    status, _ = post_bytes(egress_url, json.dumps(payload).encode())

    assert 400 <= status < 500
    assert mail.replies == []
    # Rejection occurred before reply state mutation: a later valid completion
    # must not contain the forged update text.
    assert post_event(egress_url, completed("ev-valid"))[0] == 200
    assert "forged" not in mail.replies[0][1]


def test_malformed_authenticated_json_is_4xx_and_has_no_side_effect(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    _seed(mail, adapter)

    status, _ = post_bytes(egress_url, b'{"version":')

    assert 400 <= status < 500
    assert mail.replies == []


def test_oversize_authenticated_body_is_rejected_before_json_allocation(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    _seed(mail, adapter)
    oversized = json.dumps({**update("x"), "text": "x" * (2 * 1024 * 1024)}).encode()

    status, _ = post_bytes(egress_url, oversized)

    assert status in {400, 413}
    assert mail.replies == []


def test_oversize_rejection_survives_a_raised_configured_limit(
    mail: MailState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    instance = make_adapter(max_reply_bytes=16 * 1024 * 1024)
    try:
        _seed(mail, instance)
        url = serve_egress(instance) + "/"

        status, _ = post_bytes(url, b"x" * (17 * 1024 * 1024))

        assert status == 413
        assert mail.replies == []
    finally:
        instance.shutdown.set()


def test_incomplete_headers_release_the_bounded_request_slots(
    egress_url: str,
) -> None:
    parsed = urllib.parse.urlsplit(egress_url)
    sockets: list[socket.socket] = []
    stop = threading.Event()
    tricklers: list[threading.Thread] = []

    def trickle(connection: socket.socket) -> None:
        while not stop.wait(0.25):
            try:
                connection.sendall(b"x")
            except OSError:
                return

    try:
        for _ in range(MAX_CONCURRENT_REQUESTS):
            connection = socket.create_connection((parsed.hostname or "127.0.0.1", parsed.port))
            connection.sendall(b"POST / HTTP/1.1\r\nHost: adapter\r\n")
            sockets.append(connection)
            thread = threading.Thread(target=trickle, args=(connection,), daemon=True)
            thread.start()
            tricklers.append(thread)

        assert wait_until(lambda: get(egress_url)[0] == 503)
        assert wait_until(lambda: get(egress_url + "healthz")[0] == 200, timeout=4.0)
    finally:
        stop.set()
        for connection in sockets:
            connection.close()
        for thread in tricklers:
            thread.join(timeout=1)


def test_chunked_authenticated_body_is_rejected(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    _seed(mail, adapter)

    parsed = urllib.parse.urlsplit(egress_url)
    # Transfer-Encoding is refused before a body is read.  Do not add unread
    # bytes merely to exercise that header-level guard: closing on them can
    # reset the response this test is meant to observe.
    request = (
        b"POST / HTTP/1.1\r\n"
        b"Host: adapter\r\n"
        + f"X-Curie-Adapter-Secret: {EGRESS_SECRET}\r\n".encode()
        + b"Transfer-Encoding: chunked\r\n\r\n"
    )
    # A clean disconnect is not a refusal, so it is never accepted as a pass.
    # It is retried because a loaded runner can miss the adapter's two-second
    # incomplete-header deadline before the handler parses the request. A
    # handler crash before the response can surface the same way, so the retry
    # bounds that exposure rather than eliminating it: three clean disconnects
    # in a row still fail.
    disconnect_attempts: list[int] = []
    for attempt in range(1, 4):
        status = 0
        remote_disconnected = False
        try:
            with socket.create_connection(
                (parsed.hostname or "127.0.0.1", parsed.port), timeout=10
            ) as connection:
                connection.sendall(request)
                response = HTTPResponse(connection)
                response.begin()
                status = response.status
                response.read()
        # RemoteDisconnected subclasses OSError, so this clause must come first.
        except RemoteDisconnected:
            remote_disconnected = True
        except OSError as error:
            pytest.fail(
                "chunked request transport failed before a response on attempt "
                f"{attempt}: {type(error).__name__}: {error}"
            )
        if remote_disconnected:
            disconnect_attempts.append(attempt)
            continue
        assert status == 400, (
            f"chunked request was not refused with 400 on attempt {attempt}: "
            f"status={status}"
        )
        if disconnect_attempts:
            # stacklevel=1 attributes the warning to this warn call inside the
            # test; stacklevel=2 would point one frame up, into pytest's runner.
            warnings.warn(
                "chunked request received 400 only after RemoteDisconnected on "
                f"attempt(s) {', '.join(str(number) for number in disconnect_attempts)}",
                stacklevel=1,
            )
        break
    else:
        pytest.fail(
            "chunked request never received the 400 refusal after 3 attempts; "
            "transport_error=RemoteDisconnected"
        )

    assert mail.replies == []
