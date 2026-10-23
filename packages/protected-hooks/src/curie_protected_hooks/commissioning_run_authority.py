"""Durable first-trial budget fence, not a permit issuer, @spec COMMISSIONING-SERIES-2.

Later trial progression needs genuine start/result verification and is not
exposed here. A fresh invocation can inspect custody for cleanup only.
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from .commissioning_local_ownership import OwnershipLedger, OwnershipRefused
from .commissioning_run_records import (
    COUNTERS,
    RunEvent,
    RunIntent,
    RunPlan,
    parse_run_event,
    parse_run_intent,
    parse_run_plan,
    validate_run_plan,
)
from .commissioning_run_storage import (
    _STORAGE_ERRORS,
    RunCustodyRefused,
    _CustodyStore,
    _require,
)

__all__ = ["RunCustody", "RunCustodyRefused", "RunRecovery", "RunView", "open_run_for_cleanup"]
_ERRORS = (RunCustodyRefused, OwnershipRefused, *_STORAGE_ERRORS)
_ZERO = "0" * 64


def _clock_ns() -> int:
    # Python documents CLOCK_BOOTTIME as including suspend:
    # https://docs.python.org/3.13/library/time.html#time.CLOCK_BOOTTIME
    # Apple documents mach_continuous_time as including sleep and identifies
    # CLOCK_MONOTONIC_RAW as its nanosecond equivalent:
    # https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
    # https://developer.apple.com/documentation/driverkit/c-runtime-support
    name = "CLOCK_BOOTTIME" if sys.platform == "linux" else "CLOCK_MONOTONIC_RAW"
    _require(sys.platform in {"linux", "darwin"} and hasattr(time, name))
    result = time.clock_gettime_ns(getattr(time, name))
    _require(type(result) is int and result >= 0)
    return result


def _encode(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


@dataclass(frozen=True, slots=True, repr=False)
class RunView:
    """Inert retained state; this is never a successful verdict, @spec COMMISSIONING-SERIES-2."""

    intent: RunIntent
    plan: RunPlan | None
    events: tuple[RunEvent, ...]
    journal_phase: str
    charged_limits: tuple[tuple[str, int], ...]

    def __repr__(self) -> str:
        """Do not expose identities or custody contents, @spec COMMISSIONING-SERIES-2."""
        return "RunView()"


def _load(store: _CustodyStore, *, pending_seal: bool = False) -> RunView:
    store.check()
    intent_digest = store.pointer("intent")
    intent = parse_run_intent(store.record(intent_digest))
    plan_digest = store.pointer("plan") if store.has_plan() else None
    plan = parse_run_plan(store.record(plan_digest)) if plan_digest is not None else None
    if plan is not None:
        validate_run_plan(intent, plan)
    head = store.pointer("head")
    references = {intent_digest} | ({plan_digest} if plan_digest is not None else set())
    backward: list[RunEvent] = []
    while head != _ZERO:
        _require(head not in references and len(backward) < 4096)
        references.add(head)
        event = parse_run_event(store.record(head))
        backward.append(event)
        head = event.as_dict()["previous_digest"]
    _require(store.record_names() == references)
    events = tuple(reversed(backward))
    _require(plan is None or bool(events) or pending_seal)
    phase, elapsed, charged = "intent", 0, False
    i = intent.as_dict()
    for revision, event in enumerate(events, 1):
        e = event.as_dict()
        _require(e["run_id"] == i["run_id"] and e["revision"] == str(revision))
        _require(int(e["elapsed_ms"]) >= elapsed)
        elapsed = int(e["elapsed_ms"])
        kind = e["kind"]
        expected_subject = plan_digest
        if kind == "prepared":
            _require(phase == "intent" and plan is not None and revision == 1)
            phase = "prepared"
        elif kind == "running":
            _require(phase == "prepared")
            phase = "running"
        elif kind == "charged":
            _require(phase == "running" and not charged and e["ordinal"] == "1")
            expected_subject = hashlib.sha256(_encode(i["trials"][0])).hexdigest()
            charged = True
        elif kind == "closing":
            _require(phase in {"prepared", "running"})
            phase = "closing"
        elif kind == "failed":
            _require(phase == "closing" or (phase == "intent" and revision == 1))
            expected_subject = plan_digest or intent_digest
            phase = "failed"
        else:
            # No caller-supplied digest can enable unchecked start, settlement,
            # a later ordinal, or a closed/accepted run in this foundation.
            raise RunCustodyRefused()
        _require(e["subject_digest"] == expected_subject)
    values = tuple((key, int(i["trials"][0]["limits"][key]) if charged else 0) for key in COUNTERS)
    return RunView(intent, plan, events, phase, values)


class RunRecovery:
    """An exclusive read/cleanup context with no positive methods, @spec COMMISSIONING-SERIES-2."""

    def __init__(self, store: _CustodyStore):
        self._store = store

    def __enter__(self) -> Self:
        """Keep the custody lock through external owned cleanup, @spec COMMISSIONING-SERIES-2."""
        return self

    def __exit__(self, *args: object) -> None:
        """Release only descriptors, never history or budget, @spec COMMISSIONING-SERIES-2."""
        self.close()

    def view(self) -> RunView:
        """Read retained records without a clock/permit grant, @spec COMMISSIONING-SERIES-2."""
        try:
            return _load(self._store)
        except _ERRORS:
            raise RunCustodyRefused() from None

    def close(self) -> None:
        """No explicit flock unlock in forked copies, @spec COMMISSIONING-SERIES-2."""
        self._store.close()


def open_run_for_cleanup(ledger: OwnershipLedger) -> RunRecovery:
    """Existing custody is never a positive-work resume, @spec COMMISSIONING-SERIES-2."""
    _require(type(ledger) is OwnershipLedger)
    store: _CustodyStore | None = None
    try:
        store = _CustodyStore(ledger, create=False)
        _load(store)
        return RunRecovery(store)
    except BaseException as error:
        if store is not None:
            store.close()
        if isinstance(error, _ERRORS):
            raise RunCustodyRefused() from None
        raise


class RunCustody:
    """One live process's additional budget fence, @spec COMMISSIONING-SERIES-2.

    No API here issues a permit, validates real effects, settles a trial or
    publishes acceptance. The first charge is durable accounting only.
    """

    def __init__(self, store: _CustodyStore, started_ns: int, view: RunView):
        self._store = store
        self._started_ns = self._last_ns = started_ns
        self._mutex = threading.Lock()
        self._closed = self._poisoned = False
        self._head = _ZERO
        self._intent_digest = view.intent.digest
        self._plan_digest: str | None = None
        self._limit_ns = int(view.intent.as_dict()["max_elapsed_ms"]) * 1000000

    @property
    def root(self) -> Path:
        """The fixed owned child, not a caller-selected phase, @spec COMMISSIONING-SERIES-2."""
        return self._store.root

    @classmethod
    def create(cls, ledger: OwnershipLedger, intent: RunIntent) -> Self:
        """Create once; existing installation history refuses, @spec COMMISSIONING-SERIES-2."""
        _require(type(ledger) is OwnershipLedger and type(intent) is RunIntent)
        store: _CustodyStore | None = None
        try:
            started = _clock_ns()
            store = _CustodyStore(ledger, create=True, intent=intent)
            return cls(store, started, _load(store))
        except BaseException as error:
            if store is not None:
                store.close()
            if isinstance(error, _ERRORS):
                raise RunCustodyRefused() from None
            raise

    def __enter__(self) -> Self:
        """The same live process only, @spec COMMISSIONING-SERIES-2."""
        _require(not self._closed and self._store.same_process())
        return self

    def __exit__(self, *args: object) -> None:
        """Normal exit does not erase an uncertain run, @spec COMMISSIONING-SERIES-2."""
        self.close()

    @contextmanager
    def _operation(self, *, positive: bool) -> Iterator[RunView]:
        # Check fork revocation before touching an inherited, possibly locked
        # thread mutex. Busy callers get no authority and do not poison its owner.
        _require(not self._closed and self._store.same_process())
        _require(self._mutex.acquire(blocking=False))
        try:
            if positive:
                _require(not self._poisoned)
            view = _load(self._store)
            head = view.events[-1].digest if view.events else _ZERO
            _require(head == self._head and view.intent.digest == self._intent_digest)
            _require((view.plan.digest if view.plan else None) == self._plan_digest)
            yield view
        except _ERRORS:
            self._poisoned = True
            raise RunCustodyRefused() from None
        finally:
            self._mutex.release()

    def _elapsed(self, *, positive: bool) -> int:
        now = _clock_ns()
        _require(now >= self._last_ns)
        self._last_ns = now
        if positive:
            _require(now - self._started_ns < self._limit_ns)
        return (now - self._started_ns) // 1000000

    def _append(self, kind: str, ordinal: str, subject: str, elapsed: int) -> RunEvent:
        current = _load(self._store, pending_seal=kind == "prepared")
        event = parse_run_event(
            _encode(
                {
                    "schema_version": 1,
                    "record_type": "commissioning_run_event",
                    "run_id": current.intent.as_dict()["run_id"],
                    "revision": str(len(current.events) + 1),
                    "previous_digest": self._head,
                    "kind": kind,
                    "ordinal": ordinal,
                    "subject_digest": subject,
                    "elapsed_ms": str(elapsed),
                }
            )
        )
        self._store.append(event, self._head)
        self._head = event.digest
        _load(self._store)
        return event

    def seal(self, plan: RunPlan) -> None:
        """Bind observed preparation to its immutable intent, @spec COMMISSIONING-SERIES-2."""
        with self._operation(positive=True) as view:
            _require(view.journal_phase == "intent" and view.plan is None)
            validate_run_plan(view.intent, plan)
            elapsed = self._elapsed(positive=True)
            self._store.set_plan(plan)
            self._plan_digest = plan.digest
            self._append("prepared", "0", plan.digest, elapsed)
            self._elapsed(positive=True)

    def start(self) -> None:
        """One irreversible start; no permit is created, @spec COMMISSIONING-SERIES-2."""
        with self._operation(positive=True) as view:
            _require(view.journal_phase == "prepared" and view.plan is not None)
            assert view.plan is not None
            self._append("running", "0", view.plan.digest, self._elapsed(positive=True))
            self._elapsed(positive=True)

    def charge_first_trial(self) -> RunEvent:
        """Burn the entire first envelope before acknowledging it.

        This returns retained accounting, not a dispatch token. No replay,
        later-trial progression or success setter exists. @spec COMMISSIONING-SERIES-2.
        """
        with self._operation(positive=True) as view:
            _require(view.journal_phase == "running" and not any(n for _, n in view.charged_limits))
            row = view.intent.as_dict()["trials"][0]
            event = self._append(
                "charged",
                "1",
                hashlib.sha256(_encode(row)).hexdigest(),
                self._elapsed(positive=True),
            )
            self._elapsed(positive=True)
            return event

    def view(self) -> RunView:
        """Status stays readable after deadline/positive refusal, @spec COMMISSIONING-SERIES-2."""
        with self._operation(positive=False) as view:
            return view

    def close(self) -> None:
        """Release owned FDs without deleting records or refunding, @spec COMMISSIONING-SERIES-2."""
        if not self._store.same_process():
            self._closed = True
            self._store.close()
            return
        with self._mutex:
            if not self._closed:
                self._closed = True
                self._store.close()
