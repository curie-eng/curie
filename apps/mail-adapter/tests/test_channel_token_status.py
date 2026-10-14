"""Credential visibility through real adapter probes and platform HTTP responses."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pytest
from _support import IngressState, MailState, get
from curie_api.channel_token import CHANNEL_ENQUEUE_SCOPE, mint
from curie_mail_adapter.adapter import MailAdapter


def token(exp: int) -> str:
    return mint(
        "test-platform-key",
        channel_id="00000000-0000-0000-0000-000000000001",
        generation=1,
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=exp,
    )


@pytest.mark.parametrize(
    "offset,expected,ready", [(3600, "ok", 200), (60, "expiring", 200), (-1, "expired", 503)]
)
def test_token_expiry_is_visible_before_any_inbound_mail(
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    mail: MailState,
    offset: int,
    expected: str,
    ready: int,
) -> None:
    exp = int(time.time()) + offset
    secret = token(exp)
    adapter = make_adapter(channel_token=secret)
    try:
        adapter.startup()
        url = serve_egress(adapter)
        assert get(url + "/healthz")[0] == 200
        status, body = get(url + "/statusz")
        assert status == 200
        assert body["channel_token"] == {"present": True, "exp": exp, "state": expected}
        assert body["last_ingress_status"] is None
        ready_status, ready_body = get(url + "/readyz")
        assert ready_status == ready
        assert ready_body == {"status": "ready" if ready == 200 else "starting"}
        assert secret not in str(body)
        assert "test-platform-key" not in str(body)
    finally:
        adapter.close()


def test_platform_rejection_degrades_probe_and_success_recovers(
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    mail: MailState,
    ingress: IngressState,
    caplog: pytest.LogCaptureFixture,
) -> None:
    exp = int(time.time()) + 3600
    secret = token(exp)
    adapter = make_adapter(channel_token=secret)
    try:
        adapter.startup()
        url = serve_egress(adapter)
        turn = {"delivery_id": "msg-rejected", "conversation_id": "thr-rejected"}
        ingress.response = (401, {"detail": "invalid token"})
        with caplog.at_level(logging.WARNING, logger="curie_mail_adapter.adapter"):
            adapter.post_turn(turn)
            adapter.post_turn(turn)
        assert get(url + "/readyz") == (503, {"status": "starting"})
        body = get(url + "/statusz")[1]
        assert body["channel_token"]["state"] == "rejected"
        assert body["last_ingress_status"] == 401
        lines = [
            r.getMessage() for r in caplog.records if "channel token rejected" in r.getMessage()
        ]
        assert len(lines) == 1
        assert "re-mint" in lines[0] and "/channels/token" in lines[0]
        assert secret not in "\n".join(lines)
        ingress.response = (503, {"detail": "temporarily unavailable"})
        adapter.post_turn(turn)
        assert get(url + "/readyz") == (503, {"status": "starting"})
        assert get(url + "/statusz")[1]["channel_token"]["state"] == "rejected"
        assert get(url + "/statusz")[1]["last_ingress_status"] == 503
        ingress.response = (200, {"queued": True})
        adapter.post_turn(turn)
        assert get(url + "/readyz") == (200, {"status": "ready"})
        assert get(url + "/statusz")[1]["channel_token"]["state"] == "ok"
        assert get(url + "/statusz")[1]["last_ingress_status"] == 200
    finally:
        adapter.close()


@pytest.mark.parametrize("secret", ["", "opaque", "chn.eyJleHAiOnRydWV9.signature"])
def test_unreadable_token_fails_closed_without_exposing_input(
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    secret: str,
) -> None:
    adapter = make_adapter(channel_token=secret)
    try:
        adapter.startup()
        url = serve_egress(adapter)
        assert get(url + "/readyz") == (503, {"status": "starting"})
        body = get(url + "/statusz")[1]
        assert body["channel_token"] == {
            "present": bool(secret),
            "exp": None,
            "state": "invalid" if secret else "missing",
        }
        if secret:
            assert secret not in str(body)
    finally:
        adapter.close()


def test_expiry_is_rechecked_on_probe_without_restart_or_incoming_mail(
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = make_adapter(channel_token=token(int(time.time()) + 2))
    try:
        adapter.startup()
        url = serve_egress(adapter)
        assert get(url + "/readyz") == (200, {"status": "ready"})
        time.sleep(2)
        assert get(url + "/readyz") == (503, {"status": "starting"})
        assert get(url + "/statusz")[1]["channel_token"]["state"] == "expired"
    finally:
        adapter.close()


def test_disabled_ingress_does_not_require_valid_token_for_egress_readiness(
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = make_adapter(channel_token=token(1), ingress_enabled=False)
    try:
        adapter.startup()
        url = serve_egress(adapter)
        assert get(url + "/readyz") == (200, {"status": "ready"})
        assert get(url + "/statusz")[1]["channel_token"]["state"] == "disabled"
    finally:
        adapter.close()
