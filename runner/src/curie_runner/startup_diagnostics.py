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

Held values and supported encodings are scrubbed before forwarding or retention.
An unfinished held-value prefix is retained after a forced partial flush;
private key blocks are tracked across lines. Diagnostics remain byte bounded.
"""

from __future__ import annotations

import contextlib
import logging
import re
import sys
import time
from collections.abc import Callable, Iterator

import anyio
from curie_telemetry.redact import (
    REDACTION_RULES,
    held_secret_literals,
    redact_text,
    registered_held_secrets,
)

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
    """Redact CLI stderr before forwarding and retaining a bounded tail."""

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
        self._pending = ""

    @property
    def passthrough(self) -> Callable[[str], None] | None:
        return self._passthrough

    def __call__(self, line: str) -> None:
        partial = len(line) >= self._partial_line_chars
        raw = self._pending + line
        self._pending = ""
        literals = held_secret_literals(registered_held_secrets())
        # Replace complete values before looking for an unfinished suffix, so
        # overlapping prefixes never retain part of a complete credential.
        for literal in literals:
            raw = raw.replace(literal, "[REDACTED:held_secret]")
        if partial:
            keep = max(
                (
                    size
                    for literal in literals
                    for size in range(1, min(len(raw), len(literal) - 1) + 1)
                    if raw.endswith(literal[:size])
                ),
                default=0,
            )
            if keep:
                self._pending = raw[-keep:]
                raw = raw[:-keep]
        if raw:
            self._emit(self._sanitize(raw), partial=partial)

    def _emit(self, text: str, *, partial: bool = False) -> None:
        if self._capturing and not self._stopped:
            if partial:
                self._stopped = True
                self._append(CAPTURE_STOPPED_MARKER.encode("utf-8") + b"\n")
            elif text:
                self._append(text.encode("utf-8") + b"\n")
        if not text:
            return
        if self._passthrough is not None:
            self._passthrough(text)
        else:
            sys.stderr.write(text + "\n")

    def _sanitize(self, line: str) -> str:
        if self._in_private_key:
            end = _PEM_END.search(line)
            if end is None:
                return ""
            self._in_private_key = False
            line = line[end.end() :]
        begin = next(reversed(list(_PEM_BEGIN.finditer(line))), None)
        if begin is not None and _PEM_END.search(line, begin.end()) is None:
            self._in_private_key = True
            return redact_text(line[: begin.start()]) + _PEM_PLACEHOLDER
        return redact_text(line)

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

        if self._pending:
            self._pending = ""
            self._emit("[REDACTED:held_secret]")
        self._capturing = False
        self._stopped = False
        tail = self._buffer.decode("utf-8").rstrip("\n")
        self._buffer.clear()
        return tail

    def discard(self) -> None:
        self._pending = ""
        self._capturing = False
        self._stopped = False
        self._buffer.clear()


def _finish(outcome: str, started: float, sdk_version: str, error_class: str | None = None) -> None:
    elapsed_ms = max(0, int((time.monotonic() - started) * 1000))
    fields: dict[str, object] = {
        "phase": PHASE,
        "outcome": outcome,
        "elapsed_ms": elapsed_ms,
        "sdk_version": sdk_version,
    }
    if error_class is None:
        logger.info(
            "sdk initialize finish phase=%s outcome=%s elapsed_ms=%d sdk_version=%s",
            PHASE,
            outcome,
            elapsed_ms,
            sdk_version,
            extra=fields,
        )
        return
    fields["error_class"] = error_class
    logger.info(
        "sdk initialize finish phase=%s outcome=%s elapsed_ms=%d sdk_version=%s error_class=%s",
        PHASE,
        outcome,
        elapsed_ms,
        sdk_version,
        error_class,
        extra=fields,
    )


@contextlib.contextmanager
def sdk_initialize_diagnostics(tail: StderrTail, sdk_version: str) -> Iterator[None]:
    """Time one SDK initialization and keep its stderr tail only on failure.

    ``sdk_version`` comes from the adapter, which owns the SDK import, so
    this module stays outside the harness SDK containment set.

    Exceptions propagate unchanged: no wrapping, retry, or timeout. Must be
    entered inside the event loop that runs ``connect``.
    """

    cancelled = anyio.get_cancelled_exc_class()
    logger.info(
        "sdk initialize start phase=%s sdk_version=%s",
        PHASE,
        sdk_version,
        extra={"phase": PHASE, "sdk_version": sdk_version},
    )
    started = time.monotonic()
    tail.start()
    try:
        yield
    except cancelled:
        tail.discard()
        _finish("cancelled", started, sdk_version)
        raise
    except BaseException as exc:
        diagnostic = tail.finish()
        _finish("failure", started, sdk_version, type(exc).__name__)
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
        _finish("success", started, sdk_version)
