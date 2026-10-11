"""Bounded, redacted diagnostics for the Claude SDK initialization (#4235).

A stalled or failing ``ClaudeSDKClient.connect`` used to leave no evidence: the
adapter recorded no timing and the CLI's stderr went to the runner's inherited
fd 2 with nothing retained. This module adds the two pieces of evidence.

``StderrTail`` is the one ``ClaudeAgentOptions.stderr`` callable a session
installs before it builds its SDK client. The SDK pipes CLI stderr only when
that option is set, and it copies the options (``_configure_can_use_tool``
with a permission callback, ``apply_materialized_options`` on a store resume)
so the transport holds a copy for the whole process lifetime. The callable
therefore always forwards every line to the destination it replaced, and only
while ``connect`` runs does it also keep a redacted copy of the newest bytes.

``sdk_initialize_diagnostics`` wraps ``connect`` with a start and a finish
record and emits the tail as one WARNING, only on failure. Records carry the
fixed phase, the outcome, monotonic elapsed milliseconds, the SDK version and,
on failure only, the exception class. Never options, env, prompts, or
exception text: the SDK's message text can carry a header value.

Redaction is per callback line, plus the private key block tracker and the
stop after a line long enough to be a forced partial flush. A value whose
recognizable prefix and payload arrive in different callbacks (for example a
flush whose pending text ends in whitespace, which the SDK strips) is not
redacted here, the same as anywhere else that reads CLI stderr.
"""

from __future__ import annotations

import contextlib
import logging
import re
import sys
import time
from collections.abc import Callable, Iterator

import anyio
from claude_agent_sdk import __version__ as SDK_VERSION
from curie_telemetry.redact import REDACTION_RULES, redact_text

logger = logging.getLogger(__name__)

PHASE = "sdk_initialize"
STDERR_TAIL_LIMIT_BYTES = 8192

# The SDK hands stderr one line at a time, so the pem_private_key rule, which
# needs the whole block, never matches a multiline key. These markers mirror
# that rule's delimiters; the tail tracks an open block across lines itself.
_PEM_BEGIN = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
_PEM_END = re.compile(r"-----END [A-Z ]*PRIVATE KEY-----")
_PEM_PLACEHOLDER = next(
    rule.placeholder for rule in REDACTION_RULES if rule.name == "pem_private_key"
)
# Retained in place of a line that may be a forced partial flush, and of
# everything after it in the same capture window.
CAPTURE_STOPPED_MARKER = "[stderr capture stopped: a line reached the SDK buffer limit]"


class StderrTail:
    """Forward every CLI stderr line; retain a redacted tail while capturing."""

    def __init__(
        self, passthrough: Callable[[str], None] | None, *, partial_line_chars: int
    ) -> None:
        # A session built from another session's options must not stack tails:
        # only the outermost one would see a capture window.
        if isinstance(passthrough, StderrTail):
            passthrough = passthrough.passthrough
        self._passthrough: Callable[[str], None] | None = passthrough
        # The SDK transport flushes a line without its newline once more than
        # max_buffer_size characters are pending, so a secret or a BEGIN marker
        # can straddle two callbacks and no per-line rule sees it whole.
        self._partial_line_chars = partial_line_chars
        self._capturing = False
        self._in_private_key = False
        self._stopped = False
        self._buffer = bytearray()

    @property
    def passthrough(self) -> Callable[[str], None] | None:
        return self._passthrough

    def __call__(self, line: str) -> None:
        # Retain before forwarding: the SDK swallows a raising callback, and a
        # caller's passthrough failing must not cost the diagnostic.
        if self._capturing:
            self._retain(line)
        if self._passthrough is not None:
            self._passthrough(line)
        else:
            # The SDK hands lines without their newline. Unpiped, the CLI wrote
            # straight to the inherited fd 2, so keep that output shape.
            sys.stderr.write(line + "\n")

    def _retain(self, line: str) -> None:
        # A line this long may be a forced partial flush whose remainder
        # arrives as the next line, so neither part can be redacted. Keep a
        # fixed marker and nothing more until the next capture window.
        if self._stopped:
            return
        if len(line) >= self._partial_line_chars:
            self._stopped = True
            self._append(CAPTURE_STOPPED_MARKER.encode("utf-8") + b"\n")
            return
        # Redact before the text is retained anywhere. A private key block
        # spans lines: everything from an unclosed BEGIN marker through the
        # matching END marker is replaced by one placeholder.
        if self._in_private_key:
            end = _PEM_END.search(line)
            if end is None:
                return
            self._in_private_key = False
            line = line[end.end() :]
            if not line:
                return
        # Find block boundaries on the raw line: a generic rule such as
        # secret_assignment can consume the BEGIN marker itself. Only the last
        # BEGIN can be unclosed; redact_text replaces any complete block.
        begin = next(reversed(list(_PEM_BEGIN.finditer(line))), None)
        if begin is not None and _PEM_END.search(line, begin.end()) is None:
            self._in_private_key = True
            text = redact_text(line[: begin.start()]) + _PEM_PLACEHOLDER
        else:
            text = redact_text(line)
        self._append(text.encode("utf-8") + b"\n")

    def _append(self, encoded: bytes) -> None:
        # Keep the newest bytes without the buffer ever exceeding the limit. A
        # cut can land inside a multibyte character; skip its UTF-8
        # continuation bytes so the tail always decodes.
        if len(encoded) >= STDERR_TAIL_LIMIT_BYTES:
            cut = len(encoded) - STDERR_TAIL_LIMIT_BYTES
            while cut < len(encoded) and encoded[cut] & 0xC0 == 0x80:
                cut += 1
            self._buffer[:] = encoded[cut:]
            return
        excess = len(self._buffer) + len(encoded) - STDERR_TAIL_LIMIT_BYTES
        if excess > 0:
            while excess < len(self._buffer) and self._buffer[excess] & 0xC0 == 0x80:
                excess += 1
            del self._buffer[:excess]
        self._buffer += encoded

    def start(self) -> None:
        self._buffer.clear()
        self._in_private_key = False
        self._stopped = False
        self._capturing = True

    def finish(self) -> str:
        """Stop capturing and hand back the retained tail, clearing it."""

        self._capturing = False
        self._in_private_key = False
        self._stopped = False
        tail = self._buffer.decode("utf-8").rstrip("\n")
        self._buffer.clear()
        return tail

    def discard(self) -> None:
        self._capturing = False
        self._in_private_key = False
        self._stopped = False
        self._buffer.clear()


def _finish(outcome: str, started: float, error_class: str | None = None) -> None:
    elapsed_ms = max(0, int((time.monotonic() - started) * 1000))
    fields: dict[str, object] = {
        "phase": PHASE,
        "outcome": outcome,
        "elapsed_ms": elapsed_ms,
        "sdk_version": SDK_VERSION,
    }
    if error_class is None:
        logger.info(
            "sdk initialize finish phase=%s outcome=%s elapsed_ms=%d sdk_version=%s",
            PHASE,
            outcome,
            elapsed_ms,
            SDK_VERSION,
            extra=fields,
        )
        return
    fields["error_class"] = error_class
    logger.info(
        "sdk initialize finish phase=%s outcome=%s elapsed_ms=%d sdk_version=%s error_class=%s",
        PHASE,
        outcome,
        elapsed_ms,
        SDK_VERSION,
        error_class,
        extra=fields,
    )


@contextlib.contextmanager
def sdk_initialize_diagnostics(tail: StderrTail) -> Iterator[None]:
    """Time one SDK initialization and keep its stderr tail only on failure.

    Exceptions propagate unchanged: no wrapping, retry, or timeout. Must be
    entered inside the event loop that runs ``connect``.
    """

    cancelled = anyio.get_cancelled_exc_class()
    logger.info(
        "sdk initialize start phase=%s sdk_version=%s",
        PHASE,
        SDK_VERSION,
        extra={"phase": PHASE, "sdk_version": SDK_VERSION},
    )
    started = time.monotonic()
    tail.start()
    try:
        yield
    except cancelled:
        tail.discard()
        _finish("cancelled", started)
        raise
    except BaseException as exc:
        diagnostic = tail.finish()
        _finish("failure", started, type(exc).__name__)
        if diagnostic:
            logger.warning(
                "sdk initialize stderr tail phase=%s bytes=%d\n%s",
                PHASE,
                len(diagnostic.encode("utf-8")),
                diagnostic,
                extra={"phase": PHASE},
            )
        raise
    else:
        tail.discard()
        _finish("success", started)
