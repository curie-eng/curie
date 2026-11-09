"""Self-rotation through the real HTTP client and durable credential store."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from _support import get
from curie_api import adapter_principal, channel_token
from curie_channel_gateway.mail.adapter import MailAdapter

BINDING = "00000000-0000-4000-8000-000000000001"
OTHER_BINDING = "00000000-0000-4000-8000-000000000002"
KEY = "rotation-test-signing-key"
SUBJECT = "acme-mail-adapter"
HEADER = "X-Curie-Adapter-Principal"


class RotationAPI:
    def __init__(self) -> None:
        self.now = int(time.time())
        self.generation = 1
        self.calls: list[str] = []
        self.turns: list[tuple[str, int]] = []
        self.fail = False
        self.rate_limit_next = False
        self.rate_limited = threading.Event()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.principal = self.adp(self.now + 60)
        self.channel = self.chn(self.now + 60)
        self.successor_bindings = [BINDING]
        self.principal_response: str | None = None
        self.channel_response: str | None = None
        self.url = ""

    def adp(self, exp: int, *, subject: str = SUBJECT, bindings: list[str] | None = None) -> str:
        return adapter_principal.mint(
            KEY, subject=subject, bindings=bindings if bindings is not None else [BINDING], exp=exp
        )

    def chn(self, exp: int) -> str:
        return channel_token.mint(
            KEY,
            channel_id=BINDING,
            generation=self.generation,
            scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
            exp=exp,
        )


@pytest.fixture
def rotation_api() -> Iterator[RotationAPI]:
    api = RotationAPI()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            pass

        def do_POST(self) -> None:
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            api.calls.append(self.path)
            code, body = 200, {}
            if self.path == "/channels/turns":
                api.entered.set()
                assert api.release.wait(10)
                accepted = channel_token.verify(
                    self.headers.get("X-API-Key", ""),
                    KEY,
                    channel_id=BINDING,
                    generation=api.generation,
                    scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
                    now=api.now,
                )
                code = 200 if accepted else 401
                if api.rate_limit_next:
                    api.rate_limit_next = False
                    code = 429
                    api.rate_limited.set()
                api.turns.append((data["delivery_id"], code))
            elif api.fail:
                code = 503
            else:
                claims = adapter_principal.verify(
                    self.headers.get(HEADER, ""),
                    KEY,
                    scope="channels:token",
                    now=api.now,
                )
                if claims is None:
                    code = 401
                elif self.path == "/approvals/principals/adapter/rotate":
                    api.principal = api.principal_response or api.adp(
                        api.now + data["ttl_s"], bindings=api.successor_bindings
                    )
                    code, body = 201, {"token": api.principal}
                elif self.path == "/channels/token":
                    assert data["kind"] == "email"
                    api.generation += 1
                    api.channel = api.channel_response or api.chn(api.now + data["ttl_s"])
                    body = {"token": api.channel}
                else:
                    code = 404
            encoded = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(encoded)))
            if code == 429:
                self.send_header("Retry-After", "10")
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    api.url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield api
    finally:
        api.release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def rotating(
    make_adapter: Callable[..., MailAdapter],
    api: RotationAPI,
    **kwargs: Any,
) -> MailAdapter:
    settings: dict[str, Any] = dict(
        channel_token=api.channel,
        adapter_principal=api.principal,
        api_base_url=api.url,
        self_rotate=True,
        channel_token_ttl_seconds=60,
        principal_ttl_seconds=60,
    )
    settings.update(kwargs)
    return make_adapter(**settings)


def test_rotation_is_opt_in(make_adapter: Callable[..., MailAdapter], tmp_path: Path) -> None:
    adapter = make_adapter()
    try:
        adapter.credentials.renew_once()
        assert adapter.credentials.channel_token == adapter.config.channel_token
        assert list(tmp_path.glob("*.credentials.sqlite3")) == []
    finally:
        adapter.close()


def test_schedule_renews_both_before_expiry_and_reloads_after_restart(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    tmp_path: Path,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    initial_channel, initial_principal = api.channel, api.principal
    state_path = adapter.config.state_path
    try:
        adapter.credentials.renew_once(now=api.now + 29)
        assert api.calls == []
        api.now += 30
        adapter.credentials.renew_once(now=api.now)
        assert api.calls == ["/approvals/principals/adapter/rotate", "/channels/token"]
        assert adapter.credentials.channel_token == api.channel != initial_channel
        assert adapter.credentials.adapter_principal == api.principal != initial_principal
        credential_path = Path(state_path + ".credentials.sqlite3")
        assert credential_path.stat().st_mode & 0o777 == 0o600
        with sqlite3.connect(state_path) as db:
            assert api.channel not in "\n".join(db.iterdump())
    finally:
        adapter.close()
    restored = make_adapter(
        channel_token=initial_channel,
        adapter_principal=initial_principal,
        api_base_url=api.url,
        self_rotate=True,
        state_path=state_path,
        channel_token_ttl_seconds=60,
        principal_ttl_seconds=60,
    )
    try:
        assert restored.credentials.channel_token == api.channel
        assert restored.credentials.adapter_principal == api.principal
        assert restored.post_turn({"delivery_id": "after-restart"}) == "accepted"
    finally:
        restored.close()


def test_rotation_waits_for_inflight_ingress_and_new_requests_use_successor(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    api.release.clear()
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(adapter.post_turn, {"delivery_id": "before"})
            assert api.entered.wait(5)
            api.now += 30
            renewal = pool.submit(adapter.credentials.renew_once, now=api.now)
            try:
                with pytest.raises(TimeoutError):
                    renewal.result(timeout=0.1)
                assert api.calls == ["/channels/turns"]
            finally:
                api.release.set()
            assert first.result(timeout=10) == "accepted"
            renewal.result(timeout=10)
            assert pool.submit(adapter.post_turn, {"delivery_id": "after"}).result(10) == "accepted"
        assert api.turns == [("before", 200), ("after", 200)]
        assert api.generation == 2
    finally:
        api.release.set()
        adapter.close()


def test_failures_back_off_recover_and_never_log_credentials(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    original = (api.channel, api.principal)
    try:
        api.now += 30
        api.fail = True
        with caplog.at_level(logging.WARNING):
            adapter.credentials.renew_once(now=api.now)
            assert len(api.calls) == 2
            adapter.credentials.renew_once(now=api.now + 0.5)
            assert len(api.calls) == 2
            api.now += 1
            adapter.credentials.renew_once(now=api.now)
            assert len(api.calls) == 4
            adapter.credentials.renew_once(now=api.now + 1)
            assert len(api.calls) == 4
        assert "expires_in_seconds=30" in caplog.text
        assert all(secret not in caplog.text for secret in original)
        api.fail = False
        api.now += 2
        adapter.credentials.renew_once(now=api.now)
        assert adapter.credentials.channel_token != original[0]
        assert adapter.credentials.adapter_principal != original[1]
        assert adapter.post_turn({"delivery_id": "recovered"}) == "accepted"
    finally:
        adapter.close()


def test_expired_principal_degrades_readiness_without_logging_secrets(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    api = rotation_api
    api.principal = api.adp(int(time.time()) - 1)
    adapter = rotating(make_adapter, api)
    try:
        adapter.ready.set()
        url = serve_egress(adapter)
        assert get(url + "/readyz")[0] == 503
        assert get(url + "/healthz")[0] == 200
        assert api.principal not in str(get(url + "/statusz"))
    finally:
        adapter.close()


def test_failed_persistence_keeps_revoking_channel_successor_live_and_retries_storage(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = rotation_api
    api.principal = api.adp(api.now + 600)
    adapter = rotating(make_adapter, api)
    old = api.channel
    path = adapter.config.state_path + ".credentials.sqlite3"
    blocker = sqlite3.connect(path)
    try:
        blocker.execute("BEGIN EXCLUSIVE")
        api.now += 30
        with caplog.at_level(logging.ERROR):
            adapter.credentials.renew_once(now=api.now)
        assert adapter.credentials.channel_token == api.channel != old
        assert adapter.credentials.status()["persistence_pending"] is True
        assert "kind=storage" in caplog.text and old not in caplog.text
        assert adapter.post_turn({"delivery_id": "storage-down"}) == "accepted"
        blocker.rollback()
        adapter.credentials.renew_once(now=api.now + 1)
        assert adapter.credentials.status()["persistence_pending"] is False
        assert api.calls.count("/channels/token") == 1
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT channel FROM credentials").fetchone()[0] == api.channel
    finally:
        blocker.close()
        adapter.close()


@pytest.mark.parametrize("damage", ["permissions", "corrupt", "subject", "binding", "context"])
def test_unsafe_or_malformed_snapshots_refuse_boot(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    damage: str,
) -> None:
    api = rotation_api
    first = rotating(make_adapter, api)
    path = Path(first.config.state_path + ".credentials.sqlite3")
    first.close()
    if damage == "permissions":
        path.chmod(0o644)
    elif damage == "corrupt":
        path.write_bytes(b"not a SQLite database")
    else:
        with sqlite3.connect(path) as db:
            if damage == "subject":
                db.execute(
                    "UPDATE credentials SET principal=?", (api.adp(api.now + 60, subject="other"),)
                )
            elif damage == "binding":
                db.execute(
                    "UPDATE credentials SET channel=?",
                    (
                        channel_token.mint(
                            KEY,
                            channel_id="other",
                            generation=2,
                            scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
                            exp=api.now + 60,
                        ),
                    ),
                )
            else:
                db.execute("UPDATE credentials SET context='another inbox'")
    with pytest.raises(RuntimeError, match="credential storage") as error:
        rotating(make_adapter, api)
    assert api.channel not in str(error.value) and api.principal not in str(error.value)


def test_expired_snapshot_yields_to_current_bootstrap_credentials(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    path = adapter.config.state_path + ".credentials.sqlite3"
    adapter.close()
    with sqlite3.connect(path) as db:
        db.execute("UPDATE credentials SET channel=?, principal=?", (api.chn(1), api.adp(1)))
    restored = rotating(make_adapter, api)
    try:
        assert restored.credentials.channel_token == api.channel
        assert restored.credentials.adapter_principal == api.principal
    finally:
        restored.close()


def test_newer_bootstrap_credentials_supersede_snapshot(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    adapter.close()
    api.generation += 1
    api.channel = api.chn(api.now + 120)
    api.principal = api.adp(api.now + 120)
    restored = rotating(make_adapter, api)
    try:
        assert restored.credentials.channel_token == api.channel
        assert restored.credentials.adapter_principal == api.principal
    finally:
        restored.close()


def test_rejected_channel_is_renewed_early_and_probe_recovers_before_new_mail(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    try:
        adapter.ready.set()
        url = serve_egress(adapter)
        api.generation += 1
        assert adapter.post_turn({"delivery_id": "revoked"}) == "retry"
        assert get(url + "/readyz")[0] == 503
        adapter.credentials.renew_once(now=api.now)
        assert api.calls.count("/channels/token") == 1
        assert get(url + "/readyz")[0] == 200
        assert adapter.post_turn({"delivery_id": "after-recovery"}) == "accepted"
    finally:
        adapter.close()


@pytest.fixture
def rotating_main(
    rotation_api: RotationAPI,
    tmp_path: Path,
) -> Iterator[tuple[subprocess.Popen[str], Path]]:
    path = tmp_path / "main-state.sqlite3"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "CURIE_API_URL": rotation_api.url,
        "CURIE_CHANNEL_TOKEN": rotation_api.channel,
        "CURIE_ADAPTER_PRINCIPAL": rotation_api.principal,
        "CURIE_EGRESS_SECRET": "main-test-egress-secret",
        "AGENTMAIL_API_KEY": "main-test-provider-key",
        "AGENTMAIL_INBOX": "acme@example.test",
        "ADAPTER_INGRESS_ENABLED": "false",
        "CURIE_MAIL_SELF_ROTATE": "true",
        "CURIE_MAIL_STATE_PATH": str(path),
        "CURIE_MAIL_PORT": "0",
    }
    initial = (rotation_api.channel, rotation_api.principal)
    process = subprocess.Popen(
        [sys.executable, "-m", "curie_mail_adapter"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process, path
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            _out, logs = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
            raise RuntimeError("mail adapter process did not stop") from None
        assert all(
            token not in logs for token in (*initial, rotation_api.channel, rotation_api.principal)
        )
        assert process.returncode == 0


def test_process_entrypoint_rotates_with_ingress_disabled(
    rotating_main: tuple[subprocess.Popen[str], Path],
    rotation_api: RotationAPI,
) -> None:
    process, path = rotating_main
    deadline = time.monotonic() + 10
    snapshot = Path(str(path) + ".credentials.sqlite3")
    while True:
        assert process.poll() is None, "mail adapter exited before credential renewal"
        assert time.monotonic() < deadline, "mail adapter did not persist independent renewal"
        if len(rotation_api.calls) >= 2:
            with sqlite3.connect(snapshot) as db:
                channel, principal = db.execute(
                    "SELECT channel, principal FROM credentials"
                ).fetchone()
            if rotation_api.generation >= 2 and (channel, principal) == (
                rotation_api.channel,
                rotation_api.principal,
            ):
                break
        threading.Event().wait(0.02)
    assert channel_token.verify(
        channel,
        KEY,
        channel_id=BINDING,
        generation=rotation_api.generation,
        scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
    )
    assert adapter_principal.verify(principal, KEY, scope="channels:token") is not None


def test_ingress_backoff_does_not_block_credential_renewal(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    api.rate_limit_next = True
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(adapter.post_turn, {"delivery_id": "quota-backoff"})
            assert api.rate_limited.wait(5)
            api.now += 30
            renewal = pool.submit(adapter.credentials.renew_once, now=api.now)
            try:
                renewal.result(timeout=2)
                assert not pending.done()
                assert api.generation == 2
                assert adapter.credentials.channel_token == api.channel
                assert adapter.credentials.adapter_principal == api.principal
            finally:
                adapter.shutdown.set()
            assert pending.result(timeout=5) == "retry"
    finally:
        adapter.close()


def test_narrowed_principal_rotates_and_restores_while_serving_the_channel(
    make_adapter: Callable[..., MailAdapter], rotation_api: RotationAPI
) -> None:
    api = rotation_api
    api.principal = api.adp(api.now + 60, bindings=[BINDING, OTHER_BINDING])
    initial_principal, initial_channel = api.principal, api.channel
    adapter = rotating(make_adapter, api)
    path = adapter.config.state_path
    try:
        api.now += 30
        adapter.credentials.renew_once(now=api.now)
        narrowed = adapter.credentials.adapter_principal
        verified = adapter_principal.verify(narrowed, KEY, scope="channels:token", now=api.now)
        assert verified is not None and {str(value) for value in verified.bindings} == {BINDING}
        assert adapter.post_turn({"delivery_id": "after-narrowing"}) == "accepted"
    finally:
        adapter.close()
    restored = rotating(
        make_adapter,
        api,
        channel_token=initial_channel,
        adapter_principal=initial_principal,
        state_path=path,
    )
    try:
        assert restored.credentials.adapter_principal == narrowed
        assert restored.post_turn({"delivery_id": "after-narrowed-restart"}) == "accepted"
        api.now += 30
        restored.credentials.renew_once(now=api.now)
        assert restored.credentials.adapter_principal == api.principal != narrowed
    finally:
        restored.close()


def test_day_long_principal_renews_at_half_life(
    make_adapter: Callable[..., MailAdapter], rotation_api: RotationAPI
) -> None:
    api = rotation_api
    api.principal = api.adp(api.now + 86400)
    api.channel = api.chn(api.now + 604800)
    adapter = rotating(make_adapter, api, principal_ttl_seconds=86400)
    initial = api.principal
    try:
        adapter.credentials.renew_once(now=api.now + 43199)
        assert api.calls == []
        api.now += 43200
        adapter.credentials.renew_once(now=api.now)
        assert api.calls == ["/approvals/principals/adapter/rotate"]
        assert adapter.credentials.adapter_principal == api.principal != initial
    finally:
        adapter.close()


@pytest.mark.parametrize("damage", ["subject", "wider", "unserved"])
def test_invalid_principal_successor_is_refused(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    damage: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = rotation_api
    api.channel = api.chn(api.now + 600)
    bindings = [BINDING, OTHER_BINDING] if damage == "wider" else [BINDING]
    if damage == "unserved":
        api.principal = api.adp(api.now + 60, bindings=[BINDING, OTHER_BINDING])
        bindings = [OTHER_BINDING]
    api.principal_response = api.adp(
        api.now + 90,
        subject="other-adapter" if damage == "subject" else SUBJECT,
        bindings=bindings,
    )
    adapter = rotating(make_adapter, api)
    original = adapter.credentials.adapter_principal
    try:
        api.now += 30
        with caplog.at_level(logging.ERROR):
            adapter.credentials.renew_once(now=api.now)
        assert adapter.credentials.adapter_principal == original
        with sqlite3.connect(adapter.config.state_path + ".credentials.sqlite3") as db:
            assert db.execute("SELECT principal FROM credentials").fetchone()[0] == original
        assert "kind=adp" in caplog.text
        assert original not in caplog.text and api.principal_response not in caplog.text
    finally:
        adapter.close()


@pytest.mark.parametrize("damage", ["lower-generation", "same-generation", "binding"])
def test_invalid_channel_successor_is_refused(
    make_adapter: Callable[..., MailAdapter],
    rotation_api: RotationAPI,
    damage: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = rotation_api
    api.generation = 2
    api.channel = api.chn(api.now + 60)
    api.principal = api.adp(api.now + 600)
    api.channel_response = channel_token.mint(
        KEY,
        channel_id=OTHER_BINDING if damage == "binding" else BINDING,
        generation={"lower-generation": 1, "same-generation": 2, "binding": 3}[damage],
        scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
        exp=api.now + 90,
    )
    adapter = rotating(make_adapter, api)
    original = adapter.credentials.channel_token
    try:
        api.now += 30
        with caplog.at_level(logging.ERROR):
            adapter.credentials.renew_once(now=api.now)
        assert adapter.credentials.channel_token == original
        with sqlite3.connect(adapter.config.state_path + ".credentials.sqlite3") as db:
            assert db.execute("SELECT channel FROM credentials").fetchone()[0] == original
        assert "kind=chn" in caplog.text
        assert original not in caplog.text and api.channel_response not in caplog.text
    finally:
        adapter.close()


def test_symlinked_snapshot_refuses_boot_without_touching_target(
    make_adapter: Callable[..., MailAdapter], rotation_api: RotationAPI
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    path = Path(adapter.config.state_path + ".credentials.sqlite3")
    adapter.close()
    target = path.with_name("outside.sqlite3")
    path.replace(target)
    original = target.read_bytes()
    path.symlink_to(target.name)
    with pytest.raises(RuntimeError, match="credential storage"):
        rotating(make_adapter, api)
    assert path.is_symlink()
    assert target.read_bytes() == original


@pytest.mark.parametrize("damage", ["wider", "unserved"])
def test_snapshot_cannot_widen_principal_or_drop_the_channel_binding(
    make_adapter: Callable[..., MailAdapter], rotation_api: RotationAPI, damage: str
) -> None:
    api = rotation_api
    if damage == "unserved":
        api.principal = api.adp(api.now + 60, bindings=[BINDING, OTHER_BINDING])
    adapter = rotating(make_adapter, api)
    path = adapter.config.state_path + ".credentials.sqlite3"
    adapter.close()
    bindings = [BINDING, OTHER_BINDING] if damage == "wider" else [OTHER_BINDING]
    with sqlite3.connect(path) as db:
        db.execute(
            "UPDATE credentials SET principal=?", (api.adp(api.now + 90, bindings=bindings),)
        )
    with pytest.raises(RuntimeError, match="credential storage"):
        rotating(make_adapter, api)


def test_storage_failure_does_not_prevent_principal_renewal(
    make_adapter: Callable[..., MailAdapter], rotation_api: RotationAPI
) -> None:
    api = rotation_api
    adapter = rotating(make_adapter, api)
    initial_channel, initial_principal = api.channel, api.principal
    path = adapter.config.state_path + ".credentials.sqlite3"
    blocker = sqlite3.connect(path)
    try:
        blocker.execute("BEGIN EXCLUSIVE")
        api.now += 30
        adapter.credentials.renew_once(now=api.now)
        first = adapter.credentials.adapter_principal
        assert first != initial_principal
        assert adapter.credentials.channel_token == initial_channel
        assert adapter.credentials.status()["persistence_pending"] is True
        api.now += 30
        adapter.credentials.renew_once(now=api.now)
        assert adapter.credentials.adapter_principal == api.principal != first
        assert api.calls.count("/approvals/principals/adapter/rotate") == 2
        assert "/channels/token" not in api.calls
        blocker.rollback()
        adapter.credentials.renew_once(now=api.now + 1)
        assert adapter.credentials.status()["persistence_pending"] is True
        adapter.credentials.renew_once(now=api.now + 2)
        assert adapter.credentials.status()["persistence_pending"] is False
        assert adapter.credentials.channel_token == api.channel != initial_channel
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT principal FROM credentials").fetchone()[0] == api.principal
    finally:
        blocker.close()
        adapter.close()
