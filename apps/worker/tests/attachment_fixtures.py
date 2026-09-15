"""Shared ports and helpers for the inbound-attachment tests (#2567, S3).

``test_attachments.py`` drives the resolve/cap/refuse behavior and
``test_attachment_retention.py`` drives the sibling retention ledger; both need
the SAME object-store port and the same fake Slack download, and the two suites
assert against each other's invariants (a refusal leaves no object; a reap
deletes exactly the objects a resolve wrote). Two copies of these fakes would be
free to drift apart, which is precisely what ``tests/conftest.py`` records having
already happened once to the Valkey fixtures.

Not a conftest: these are plain classes a test constructs, not fixtures pytest
should inject, and keeping them out of ``conftest.py`` keeps them off every other
worker test's collection path. Imported the way ``sandbox/resilience_fixtures.py``
is -- via a ``sys.path`` insert, because importlib import mode does not add the
test directory to ``sys.path``.

**What is a fake here and why.** Slack only. ``FakeSlackFiles`` stands in for
the external service the worker downloads from (AGENTS.md permits mocking Slack
and only such services). ``RetainingObjectStore`` is NOT a mock: it is a
conforming implementation of the worker's own ``WorkspaceObjectPort``, so every
assertion is about a key that really got written and bytes that really moved --
the same arrangement ``test_workspace.py`` uses for the workspace lane.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from typing import Any

THREAD_KEY = "slack:C1:1700000000.000100"
AGENT_ID = "11111111-1111-4111-8111-111111111111"


class RetainingObjectStore:
    """A conforming ``WorkspaceObjectPort`` that keeps whatever it consumed.

    ``put_stream`` accumulates chunk by chunk into the stored value, so a
    generator that raises mid-upload leaves the bytes it had already yielded
    behind under that key. The port's contract promises no rollback, so this is
    a legal implementation -- and it is the one that makes "fail closed" mean
    something: the resolver has to remove its own half-written object rather
    than assume the backing store did it.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_inputs: list[object] = []
        self.signed: list[tuple[str, int]] = []
        self.deleted: list[str] = []

    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        # Handing bytes here is a whole-object buffer masquerading as a stream.
        assert not isinstance(chunks, (bytes, bytearray, memoryview))
        self.put_inputs.append(chunks)
        self.objects[key] = b""
        for chunk in chunks:
            self.objects[key] += chunk

    def get_stream(self, key: str) -> Iterator[bytes]:
        payload = self.objects[key]
        midpoint = max(1, len(payload) // 2)
        yield payload[:midpoint]
        yield payload[midpoint:]

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        assert key in self.objects, "a capability was signed for an object never written"
        self.signed.append((key, expires_seconds))
        return f"https://objects.example.com/{key}?one-object=yes"

    def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.objects.pop(key, None)

    def list_keys(self, prefix: str) -> Iterator[str]:
        needle = f"{prefix.strip('/')}/"
        yield from sorted(key for key in self.objects if key.startswith(needle))


class FakeSlackFiles:
    """The one mocked collaborator: Slack's file download, chunk by chunk.

    ``fetch`` is a generator on purpose. It records every chunk it actually
    handed out, so a caller that reads the whole body before measuring it is
    distinguishable from one that stops at the cap.
    """

    def __init__(self, payloads: dict[str, list[bytes]] | None = None) -> None:
        self.payloads: dict[str, list[bytes]] = dict(payloads or {})
        self.requested: list[str] = []
        self.delivered: dict[str, list[bytes]] = {}
        #: file id -> exception raised instead of yielding anything.
        self.failures: dict[str, BaseException] = {}

    def fetch(self, file_id: str) -> Iterator[bytes]:
        self.requested.append(file_id)
        failure = self.failures.get(file_id)
        if failure is not None:
            raise failure
        for chunk in self.payloads[file_id]:
            self.delivered.setdefault(file_id, []).append(chunk)
            yield chunk


class MovableClock:
    """A wall clock the test advances, matching ``wall_clock`` in the workspace lane.

    Anchored at the REAL current time rather than a fixed literal. ``WorkspaceRef``
    computes ``expires_in_seconds`` against ``time.time()`` and takes no injected
    clock, so a minted reference is only "not yet expired" if the clock the
    coordinator mints from is roughly now. A frozen 2023 literal would make every
    freshly minted capability read as already lapsed.
    """

    def __init__(self, now: float | None = None) -> None:
        self.now = time.time() if now is None else now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def limits(module: Any, **overrides: Any) -> Any:
    """``AttachmentLimits`` with small, test-legible bounds.

    Every field is named explicitly rather than relying on the dataclass
    defaults: a test that pins the cap must fail when the cap moves, not inherit
    whatever the production default became.
    """

    values: dict[str, Any] = {
        "max_file_bytes": 64,
        "read_chunk_bytes": 16,
        "reference_ttl_seconds": 300,
        "retention_ttl_seconds": 3600,
        "max_files": 10,
    }
    values.update(overrides)
    return module.AttachmentLimits(**values)


def chunked(payload: bytes, size: int) -> list[bytes]:
    return [payload[at : at + size] for at in range(0, len(payload), size)] or [b""]
