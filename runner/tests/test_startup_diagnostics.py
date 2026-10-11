"""SDK initialization records and the bounded, redacted stderr tail (#4235).

The stub cases drive the real ClaudeAgentSession with only its SDK client
replaced, so the adapter's env snapshot and restore, the installed stderr
callable, and the diagnostics window run as in production. The real-process
cases replace nothing in the adapter, SDK client, or process transport.

Measured on claude-agent-sdk 0.2.163 with its bundled CLI: connect first runs
``<cli_path> -v`` with stderr piped and never handed to ``options.stderr``,
then spawns the CLI with stderr piped only because ``options.stderr`` is set.
A main process that exits nonzero before answering initialize raises
``ProcessError`` (``exit_code`` set) after the SDK's own disconnect has drained
stderr through the callback, and the transport is cleared. With
``can_use_tool`` set, ``_configure_can_use_tool`` hands the transport a
``dataclasses.replace`` copy of the options.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import sys
import textwrap
from dataclasses import replace
from pathlib import Path
from typing import Any

import anyio
import claude_agent_sdk
import pytest
from claude_agent_sdk import ProcessError
from claude_agent_sdk.types import PermissionResultAllow
from curie_runner.adapter import ClaudeAgentSession, build_options

# The issue fixes these values. Pinned literally, not imported, so this module
# still collects when the fix pin reverses the implementation.
PHASE = "sdk_initialize"
STDERR_TAIL_LIMIT_BYTES = 8192

_REAL_CLI_TIMEOUT_S = 60
# Synthetic credentials shaped to the bearer_token and api_key rules in
# curie_telemetry.redact; neither is a real or real-looking value.
_BEARER = "Bearer FAKEbearerTOKENfixture000"
_API_KEY = "sk-ant-FAKEapiKEYfixture0000000000"
_SECRETS = (_BEARER, _API_KEY, "FAKEbearerTOKENfixture000")


class SyntheticConnectError(RuntimeError):
    pass


class _StubClient:
    """Stands in for ClaudeSDKClient; feeds the session's installed callback."""

    def __init__(self, session: ClaudeAgentSession, lines: list[str], outcome: Any) -> None:
        self._stderr = session._options.stderr  # noqa: SLF001
        self._lines = lines
        self._outcome = outcome
        self.entered = asyncio.Event()

    async def connect(self) -> None:
        assert self._stderr is not None
        for line in self._lines:
            self._stderr(line)
        self.entered.set()
        if self._outcome == "hang":
            await asyncio.Event().wait()
        elif isinstance(self._outcome, BaseException):
            raise self._outcome

    async def disconnect(self) -> None:
        return None


def _session_with_stub(lines: list[str], outcome: Any) -> tuple[ClaudeAgentSession, _StubClient]:
    session = ClaudeAgentSession(
        build_options(
            plugins=[],
            model=None,
            system_prompt=None,
            max_turns=1,
            max_budget_usd=None,
            resume=None,
        )
    )
    stub = _StubClient(session, lines, outcome)
    session._client = stub  # type: ignore[assignment]  # noqa: SLF001
    return session, stub


def _records(caplog: pytest.LogCaptureFixture, prefix: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage().startswith(prefix)]


def _starts(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return _records(caplog, "sdk initialize start")


def _finishes(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return _records(caplog, "sdk initialize finish")


def _tails(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return _records(caplog, "sdk initialize stderr tail")


def _tail_text(record: logging.LogRecord) -> str:
    # The message is a header line followed by the tail itself.
    return record.getMessage().split("\n", 1)[1]


def _all_log_text(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(r.getMessage() for r in caplog.records)


def _assert_record_shape(record: logging.LogRecord, outcome: str) -> None:
    assert record.phase == PHASE  # type: ignore[attr-defined]
    assert record.outcome == outcome  # type: ignore[attr-defined]
    assert isinstance(record.elapsed_ms, int)  # type: ignore[attr-defined]
    assert record.elapsed_ms >= 0  # type: ignore[attr-defined]
    assert record.sdk_version == claude_agent_sdk.__version__  # type: ignore[attr-defined]


def _assert_late_line_forwarded_not_retained(
    session: ClaudeAgentSession,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    capsys.readouterr()
    before = len(caplog.records)
    callback = session._options.stderr  # noqa: SLF001
    assert callback is not None
    callback(f"late diagnostic after connect {_BEARER}")
    assert capsys.readouterr().err == f"late diagnostic after connect {_BEARER}\n"
    assert len(caplog.records) == before
    assert session._stderr_tail.finish() == ""  # noqa: SLF001


def test_successful_connect_records_timing_and_discards_stderr(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    session, _ = _session_with_stub([f"startup noise {_BEARER}"], outcome=None)
    env_before = dict(os.environ)

    anyio.run(session.connect)

    assert dict(os.environ) == env_before
    (start,) = _starts(caplog)
    assert start.phase == PHASE  # type: ignore[attr-defined]
    assert start.sdk_version == claude_agent_sdk.__version__  # type: ignore[attr-defined]
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "success")
    assert not hasattr(finish, "error_class")
    assert _tails(caplog) == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    # Every line still reaches stderr, as the inherited fd 2 did before.
    assert capsys.readouterr().err == f"startup noise {_BEARER}\n"
    _assert_late_line_forwarded_not_retained(session, caplog, capsys)


def test_failing_connect_emits_one_redacted_tail_and_the_original_exception(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    boom = SyntheticConnectError("message text that must not be logged")
    session, _ = _session_with_stub(
        [
            "first diagnostic line",
            f"Authorization: {_BEARER}",
            f"ANTHROPIC_API_KEY value {_API_KEY}",
        ],
        outcome=boom,
    )
    env_before = dict(os.environ)

    with pytest.raises(SyntheticConnectError) as raised:
        anyio.run(session.connect)

    assert raised.value is boom
    assert dict(os.environ) == env_before
    assert len(_starts(caplog)) == 1
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "failure")
    assert finish.error_class == "SyntheticConnectError"  # type: ignore[attr-defined]
    (tail,) = _tails(caplog)
    assert tail.levelno == logging.WARNING
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == [tail]
    assert _tail_text(tail) == (
        "first diagnostic line\n"
        "Authorization: [REDACTED:bearer_token]\n"
        "ANTHROPIC_API_KEY value [REDACTED:api_key]"
    )
    logged = _all_log_text(caplog)
    for secret in _SECRETS:
        assert secret not in logged
    assert "must not be logged" not in logged
    _assert_late_line_forwarded_not_retained(session, caplog, capsys)


@pytest.mark.parametrize(
    ("lines", "newest"),
    [
        pytest.param(
            [f"line-{index:04d} " + "x" * 40 for index in range(400)],
            "line-0399 " + "x" * 40,
            id="many-lines",
        ),
        pytest.param(["head " + "y" * 20_000 + " NEWEST"], "y NEWEST", id="single-huge-line"),
        # 1 + 5000 * 3 + newline bytes: the cut lands two bytes into a character.
        pytest.param(["a" + "€" * 5000], "€€", id="three-byte-straddle"),
        # 1 + 3000 * 4 + newline bytes: the cut lands one byte into a character.
        pytest.param(["a" + "\U0001f600" * 3000], "\U0001f600\U0001f600", id="four-byte-straddle"),
    ],
)
def test_oversized_and_multibyte_tail_keeps_newest_bytes_on_a_character_boundary(
    caplog: pytest.LogCaptureFixture, lines: list[str], newest: str
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    session, _ = _session_with_stub(lines, outcome=SyntheticConnectError())

    with pytest.raises(SyntheticConnectError):
        anyio.run(session.connect)

    (tail,) = _tails(caplog)
    text = _tail_text(tail)
    encoded = text.encode("utf-8")
    assert len(encoded) <= STDERR_TAIL_LIMIT_BYTES
    assert encoded.decode("utf-8") == text
    assert text.endswith(newest)
    assert "line-0000" not in text
    assert "head " not in text
    assert "�" not in text
    if "€" in lines[-1] or "\U0001f600" in lines[-1]:
        # Only the partial character's continuation bytes (at most three) and
        # the trailing newline are dropped, never a whole character more.
        assert len(encoded) >= STDERR_TAIL_LIMIT_BYTES - 4
        assert set(text) <= {"€", "\U0001f600"}


# Synthetic key body lines; not a real or real-looking key.
_PEM_BODY = tuple(f"FAKEKEYBODYLINE{index:03d}" + "A" * 48 for index in range(4))
_PEM_PLACEHOLDER = "[REDACTED:pem_private_key]"
# Markers are assembled at runtime so the secret scanners do not read this
# synthetic fixture as a committed key.
_DASHES = "-" * 5
_RSA_BEGIN = f"{_DASHES}BEGIN RSA PRIVATE KEY{_DASHES}"
_RSA_END = f"{_DASHES}END RSA PRIVATE KEY{_DASHES}"
_PKCS8_BEGIN = f"{_DASHES}BEGIN PRIVATE KEY{_DASHES}"


@pytest.mark.parametrize(
    "opening",
    [
        f"key follows: {_RSA_BEGIN}",
        # Generic assignment rules consume the BEGIN marker itself when the
        # opening line is shaped like a config value.
        f"private_key: {_RSA_BEGIN}",
        f"PRIVATE_KEY={_PKCS8_BEGIN}",
    ],
    ids=["prose-prefix", "colon-field", "env-assignment"],
)
def test_multiline_private_key_is_never_retained_line_by_line(
    caplog: pytest.LogCaptureFixture, opening: str
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    session, _ = _session_with_stub(
        [
            "before the key",
            opening,
            *_PEM_BODY,
            f"{_RSA_END} trailer text",
            "after the key",
        ],
        outcome=SyntheticConnectError(),
    )

    with pytest.raises(SyntheticConnectError):
        anyio.run(session.connect)

    prefix = opening.split(f"{_DASHES}BEGIN", 1)[0]
    (tail,) = _tails(caplog)
    assert _tail_text(tail) == (
        f"before the key\n{prefix}{_PEM_PLACEHOLDER}\n trailer text\nafter the key"
    )
    logged = _all_log_text(caplog)
    assert logged.count(_PEM_PLACEHOLDER) == 1
    assert "FAKEKEYBODYLINE" not in logged
    assert f"PRIVATE KEY{_DASHES}" not in logged


def test_unclosed_private_key_does_not_suppress_the_next_capture_window(
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, _ = _session_with_stub([], outcome=None)
    tail = session._stderr_tail  # noqa: SLF001

    tail.start()
    tail(_PKCS8_BEGIN)
    tail(_PEM_BODY[0])
    assert tail.finish() == _PEM_PLACEHOLDER
    tail.start()
    tail("plain diagnostic")
    assert tail.finish() == "plain diagnostic"
    capsys.readouterr()


class _BufferProbe(bytearray):
    """Records the buffer length after every in-place mutation."""

    lengths: list[int]

    def __iadd__(self, other: Any) -> _BufferProbe:
        result = super().__iadd__(other)
        self.lengths.append(len(self))
        return result

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(key, value)
        self.lengths.append(len(self))

    def __delitem__(self, key: Any) -> None:
        super().__delitem__(key)
        self.lengths.append(len(self))


def test_tail_buffer_never_exceeds_the_limit_during_a_single_call(
    capsys: pytest.CaptureFixture[str],
) -> None:
    session, _ = _session_with_stub([], outcome=None)
    tail = session._stderr_tail  # noqa: SLF001
    probe = _BufferProbe()
    probe.lengths = []

    tail.start()
    tail._buffer = probe  # noqa: SLF001
    lines = [
        *[f"filler-{index:03d} " + "x" * 60 for index in range(200)],
        "huge " + "z" * 200_000,
        "€" * 3000,
        "small after huge",
    ]
    for line in lines:
        tail(line)
        assert tail._buffer is probe  # noqa: SLF001
        assert len(probe) <= STDERR_TAIL_LIMIT_BYTES
    text = tail.finish()
    capsys.readouterr()

    assert probe.lengths
    assert max(probe.lengths) <= STDERR_TAIL_LIMIT_BYTES
    assert text.endswith("small after huge")


def test_cancelled_connect_propagates_discards_stderr_and_restores_env(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    session, stub = _session_with_stub([f"stalled before initialize {_BEARER}"], outcome="hang")
    env_before = dict(os.environ)

    async def scenario() -> None:
        task = asyncio.create_task(session.connect())
        entered = asyncio.create_task(stub.entered.wait())
        await asyncio.wait({task, entered}, return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            # connect failed before reaching the client: surface that error
            # rather than waiting for a stall that will never happen.
            entered.cancel()
            await task
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert dict(os.environ) == env_before
    assert len(_starts(caplog)) == 1
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "cancelled")
    assert not hasattr(finish, "error_class")
    assert _tails(caplog) == []
    for secret in _SECRETS:
        assert secret not in _all_log_text(caplog)
    _assert_late_line_forwarded_not_retained(session, caplog, capsys)


def _write_cli_fixture(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    path.chmod(0o755)
    return path


def _failing_cli(tmp_path: Path) -> Path:
    return _write_cli_fixture(
        tmp_path / "failing-cli",
        f"""
        import sys
        if sys.argv[1:] == ["-v"]:
            print("9.9.9 (Claude Code)")
            sys.exit(0)
        sys.stderr.write("synthetic diagnostic: upstream refused Authorization: {_BEARER}\\n")
        sys.stderr.flush()
        sys.exit(7)
        """,
    )


async def _allow(*_args: Any) -> PermissionResultAllow:
    return PermissionResultAllow()


def _real_options(cli_path: Path | None, *, with_permission_callback: bool, **env: str) -> Any:
    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=1,
        max_budget_usd=None,
        resume=None,
        env={
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
            **env,
        },
        can_use_tool=_allow if with_permission_callback else None,
    )
    return options if cli_path is None else replace(options, cli_path=str(cli_path))


@pytest.mark.parametrize("with_permission_callback", [False, True], ids=["plain", "can-use-tool"])
def test_real_sdk_process_failure_retains_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    with_permission_callback: bool,
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    session = ClaudeAgentSession(
        _real_options(_failing_cli(tmp_path), with_permission_callback=with_permission_callback)
    )
    env_before = dict(os.environ)

    async def scenario() -> None:
        with anyio.fail_after(_REAL_CLI_TIMEOUT_S):
            await session.connect()

    with pytest.raises(ProcessError) as raised:
        anyio.run(scenario)

    assert raised.value.exit_code == 7
    assert dict(os.environ) == env_before
    assert len(_starts(caplog)) == 1
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "failure")
    assert finish.error_class == "ProcessError"  # type: ignore[attr-defined]
    expected = "synthetic diagnostic: upstream refused Authorization: [REDACTED:bearer_token]"
    (tail,) = _tails(caplog)
    assert _tail_text(tail) == expected
    runner_records = [r for r in caplog.records if r.name.startswith("curie_runner")]
    assert sum(r.getMessage().count(expected) for r in runner_records) == 1
    for secret in _SECRETS:
        assert secret not in _all_log_text(caplog)


# Measured on claude-agent-sdk 0.2.163: SubprocessCLITransport._handle_stderr
# frames stderr chunks into lines and, once more than max_buffer_size
# characters are pending with no newline, emits the pending text as a line of
# its own. The rest of that line arrives later as a separate callback, so a
# token written across the flush point is split over two callbacks.
_PARTIAL_FLUSH_CHARS = 4096
_CAPTURE_STOPPED = "[stderr capture stopped: a line reached the SDK buffer limit]"
_STRADDLE_HEAD = "Bearer FAKEbear"
_STRADDLE_REST = "erTOKENfixture000 rest of the long line"


def _partial_flush_cli(tmp_path: Path) -> Path:
    # The first write ends mid-token and is longer than the flush threshold;
    # the pause lets the SDK read it as one chunk and flush it before the rest.
    return _write_cli_fixture(
        tmp_path / "partial-flush-cli",
        f"""
        import sys, time
        if sys.argv[1:] == ["-v"]:
            print("9.9.9 (Claude Code)")
            sys.exit(0)
        sys.stderr.write("before the long line\\n")
        sys.stderr.write("p" * {_PARTIAL_FLUSH_CHARS + 16} + "{_STRADDLE_HEAD}")
        sys.stderr.flush()
        time.sleep(0.5)
        sys.stderr.write("{_STRADDLE_REST}\\n")
        sys.stderr.write("body line after the long line\\n")
        sys.stderr.flush()
        sys.exit(7)
        """,
    )


def test_real_sdk_partial_flush_stops_retention_without_leaking_a_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    session = ClaudeAgentSession(
        replace(
            _real_options(_partial_flush_cli(tmp_path), with_permission_callback=False),
            max_buffer_size=_PARTIAL_FLUSH_CHARS,
        )
    )

    async def scenario() -> None:
        with anyio.fail_after(_REAL_CLI_TIMEOUT_S):
            await session.connect()

    with pytest.raises(ProcessError) as raised:
        anyio.run(scenario)

    assert raised.value.exit_code == 7
    forwarded = capsys.readouterr().err
    # The SDK really split the token: no forwarded line carries it whole.
    assert _STRADDLE_HEAD + _STRADDLE_REST not in forwarded
    assert f"{_STRADDLE_HEAD}\n{_STRADDLE_REST}\n" in forwarded
    (tail,) = _tails(caplog)
    assert _tail_text(tail) == f"before the long line\n{_CAPTURE_STOPPED}"
    logged = _all_log_text(caplog)
    assert logged.count("before the long line") == 1
    assert logged.count(_CAPTURE_STOPPED) == 1
    for fragment in ("FAKEbear", "erTOKENfixture000", "rest of the long line", "ppp"):
        assert fragment not in logged
    assert "body line after" not in logged


def test_line_below_the_partial_flush_threshold_is_still_retained(
    capsys: pytest.CaptureFixture[str],
) -> None:
    session = ClaudeAgentSession(
        replace(
            build_options(
                plugins=[],
                model=None,
                system_prompt=None,
                max_turns=1,
                max_budget_usd=None,
                resume=None,
            ),
            max_buffer_size=100,
        )
    )
    tail = session._stderr_tail  # noqa: SLF001

    tail.start()
    tail("a" * 99)
    tail("next line")
    assert tail.finish() == "a" * 99 + "\nnext line"
    tail.start()
    tail("b" * 100)
    tail("ignored after the stop")
    assert tail.finish() == _CAPTURE_STOPPED
    tail.start()
    tail("retained again in a new window")
    assert tail.finish() == "retained again in a new window"
    capsys.readouterr()


def _protocol_cli(tmp_path: Path) -> Path:
    # Answers initialize, then writes stderr once connect has returned: the line
    # can only arrive through the callback on the transport's options copy.
    return _write_cli_fixture(
        tmp_path / "protocol-cli",
        f"""
        import json, sys, time
        if sys.argv[1:] == ["-v"]:
            print("9.9.9 (Claude Code)")
            sys.exit(0)
        for raw in sys.stdin:
            message = json.loads(raw)
            if message.get("type") != "control_request":
                continue
            sys.stderr.write("during initialize {_BEARER}\\n")
            sys.stderr.flush()
            response = {{
                "type": "control_response",
                "response": {{
                    "subtype": "success",
                    "request_id": message["request_id"],
                    "response": {{}},
                }},
            }}
            sys.stdout.write(json.dumps(response) + "\\n")
            sys.stdout.flush()
            time.sleep(0.3)
            sys.stderr.write("after connect {_BEARER}\\n")
            sys.stderr.flush()
        """,
    )


def test_real_sdk_post_connect_stderr_reaches_stderr_through_the_options_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    session = ClaudeAgentSession(
        _real_options(_protocol_cli(tmp_path), with_permission_callback=True)
    )

    async def scenario() -> str:
        seen = ""
        with anyio.fail_after(_REAL_CLI_TIMEOUT_S):
            await session.connect()
            try:
                transport_options = session._client._transport._options  # type: ignore[union-attr]  # noqa: SLF001
                assert transport_options is not session._options  # noqa: SLF001
                assert transport_options.stderr is session._stderr_tail  # noqa: SLF001
                while "after connect" not in seen:
                    await anyio.sleep(0.05)
                    seen += capsys.readouterr().err
            finally:
                await session.close()
        return seen

    seen = anyio.run(scenario)

    assert f"during initialize {_BEARER}\n" in seen
    assert f"after connect {_BEARER}\n" in seen
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "success")
    assert _tails(caplog) == []
    assert "after connect" not in _all_log_text(caplog)
    assert session._stderr_tail.finish() == ""  # noqa: SLF001


def _closed_local_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


@pytest.mark.parametrize("with_permission_callback", [False, True], ids=["plain", "can-use-tool"])
def test_real_cli_initialize_without_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    with_permission_callback: bool,
) -> None:
    caplog.set_level(logging.INFO, logger="curie_runner")
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home / ".claude"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    # Nothing listens on the base URL, so no model request could succeed; none
    # is sent because the session only initializes and disconnects.
    dead_base_url = f"http://127.0.0.1:{_closed_local_port()}"
    monkeypatch.setenv("ANTHROPIC_API_KEY", _API_KEY)
    monkeypatch.setenv("ANTHROPIC_BASE_URL", dead_base_url)
    session = ClaudeAgentSession(
        _real_options(
            None,
            with_permission_callback=with_permission_callback,
            ANTHROPIC_API_KEY=_API_KEY,
            ANTHROPIC_BASE_URL=dead_base_url,
        )
    )

    async def scenario() -> None:
        with anyio.fail_after(_REAL_CLI_TIMEOUT_S):
            await session.connect()
            try:
                transport_options = session._client._transport._options  # type: ignore[union-attr]  # noqa: SLF001
                assert transport_options.stderr is session._stderr_tail  # noqa: SLF001
                if with_permission_callback:
                    assert transport_options is not session._options  # noqa: SLF001
                _assert_late_line_forwarded_not_retained(session, caplog, capsys)
            finally:
                await session.close()

    anyio.run(scenario)

    assert len(_starts(caplog)) == 1
    (finish,) = _finishes(caplog)
    _assert_record_shape(finish, "success")
    assert _tails(caplog) == []
    for secret in _SECRETS:
        assert secret not in _all_log_text(caplog)
