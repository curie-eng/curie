"""Pinned preliminary operator history; no acceptance, @spec COMMISSIONING-SERIES-8."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Self
from uuid import uuid4

from .commissioning_local_ownership import OwnershipLedger
from .commissioning_operator_records import (
    OperatorEvent,
    OperatorRecipe,
    parse_operator_event,
    parse_operator_recipe,
)
from .commissioning_run_authority import (
    _ERRORS,
    _ZERO,
    RunCustody,
    RunRecovery,
    RunView,
    _clock_ns,
    _encode,
    _load,
)
from .commissioning_run_records import RunIntent
from .commissioning_run_storage import RunCustodyRefused, _CustodyStore, _require


@dataclass(frozen=True, slots=True, repr=False)
class OperatorView:
    """Inert history; never a completeness/close verdict, @spec COMMISSIONING-SERIES-8."""

    recipe: OperatorRecipe
    events: tuple[OperatorEvent, ...]

    def __repr__(self) -> str:
        return "OperatorView()"


def _operator_load(store: _CustodyStore, run: RunView) -> OperatorView:
    store.check()
    recipe_digest = store.pointer("operator-recipe")
    recipe = parse_operator_recipe(store.operator_record(recipe_digest))
    intent = run.intent.as_dict()
    _require(intent["recipe_digest"] == recipe.digest)
    head = store.pointer("operator-head")
    references = {recipe_digest}
    backward: list[OperatorEvent] = []
    while head != _ZERO:
        _require(head not in references and len(backward) < 4096)
        references.add(head)
        event = parse_operator_event(store.operator_record(head))
        backward.append(event)
        head = event.as_dict()["previous_digest"]
    _require(store.operator_names() == references)
    events = tuple(reversed(backward))
    _validate_events(recipe, events, intent["run_id"])
    return OperatorView(recipe, events)


def _validate_events(
    recipe: OperatorRecipe, events: tuple[OperatorEvent, ...], run_id: str
) -> None:
    attempts: dict[str, tuple[dict[str, Any], str | None, str]] = {}
    roles = {row["role"]: row for row in recipe.as_dict()["roles"]}
    counts = dict.fromkeys(roles, 0)
    identities: set[tuple[str, str, str]] = set()
    elapsed = 0
    for revision, event in enumerate(events, 1):
        row = event.as_dict()
        _require(row["run_id"] == run_id and row["revision"] == str(revision))
        _require(int(row["elapsed_ms"]) >= elapsed)
        elapsed = int(row["elapsed_ms"])
        ticket, kind, role = row["intent_id"], row["kind"], row["role"]
        if kind == "intended":
            _require(ticket not in attempts)
            counts[role] += 1
            _require(counts[role] <= int(roles[role]["max_connections"]))
            attempts[ticket] = (row, None, kind)
            continue
        _require(ticket in attempts)
        original, binding, previous = attempts[ticket]
        _require(all(row[key] == original[key] for key in ("role", "phase", "ordinal")))
        if kind == "bound":
            _require(previous == "intended")
            identity = row["identity"]
            _require(identity["credential"] == roles[role]["credential"])
            broker = identity["broker_identity"]
            key = (broker["instance_id"], broker["run_id"], identity["client_id"])
            _require(key not in identities)
            identities.add(key)
            binding = event.digest
        else:
            _require(previous in {"intended", "bound"} and row["binding_digest"] == binding)
            _require(kind == "uncertain" or previous == "bound")
        attempts[ticket] = (original, binding, kind)


class OperatorRecovery(RunRecovery):
    """Cleanup inspection under the original lock, @spec COMMISSIONING-SERIES-8."""

    def view(self) -> RunView:
        """Validate both lanes on status reads, @spec COMMISSIONING-SERIES-8."""
        try:
            run = _load(self._store)
            _operator_load(self._store, run)
            return run
        except _ERRORS:
            raise RunCustodyRefused() from None

    def operator_view(self) -> OperatorView:
        """No fresh handle or repaired intent, @spec COMMISSIONING-SERIES-8."""
        try:
            return _operator_load(self._store, _load(self._store))
        except _ERRORS:
            raise RunCustodyRefused() from None


def open_operator_run_for_cleanup(ledger: OwnershipLedger) -> OperatorRecovery:
    """Explicit v2 cleanup reader; never resumes, @spec COMMISSIONING-SERIES-8."""
    store = None
    try:
        store = _CustodyStore(ledger, create=False, format_version=2)
        _operator_load(store, _load(store))
        return OperatorRecovery(store)
    except BaseException as error:
        if store is not None:
            store.close()
        if isinstance(error, _ERRORS):
            raise RunCustodyRefused() from None
        raise


class OperatorRunCustody(RunCustody):
    """Additional bounded registry, no new trial/phase grant, @spec COMMISSIONING-SERIES-8."""

    def __init__(self, store: _CustodyStore, started_ns: int, view: RunView):
        super().__init__(store, started_ns, view)
        self._operator_head = _ZERO

    @classmethod
    def create(
        cls, ledger: OwnershipLedger, intent: RunIntent, recipe: OperatorRecipe | None = None
    ) -> Self:
        """Commit the complete operator recipe before opening, @spec COMMISSIONING-SERIES-8."""
        _require(
            type(ledger) is OwnershipLedger
            and type(intent) is RunIntent
            and type(recipe) is OperatorRecipe
        )
        assert recipe is not None
        _require(intent.as_dict()["recipe_digest"] == recipe.digest)
        store = None
        try:
            started = _clock_ns()
            store = _CustodyStore(
                ledger, create=True, intent=intent, format_version=2, operator_recipe=recipe
            )
            view = _load(store)
            _operator_load(store, view)
            return cls(store, started, view)
        except BaseException as error:
            if store is not None:
                store.close()
            if isinstance(error, _ERRORS):
                raise RunCustodyRefused() from None
            raise

    @contextmanager
    def _operation(self, *, positive: bool) -> Iterator[RunView]:
        with super()._operation(positive=positive) as run:
            view = _operator_load(self._store, run)
            _require((view.events[-1].digest if view.events else _ZERO) == self._operator_head)
            yield run

    def operator_view(self) -> OperatorView:
        """History only, including uncertain attempts, @spec COMMISSIONING-SERIES-8."""
        with self._operation(positive=False) as run:
            return _operator_load(self._store, run)

    def _operator_append(self, row: dict[str, Any], elapsed: int, run: RunView) -> OperatorEvent:
        view = _operator_load(self._store, run)
        event = parse_operator_event(
            _encode(
                row
                | {
                    "schema_version": 2,
                    "record_type": "commissioning_operator_event",
                    "run_id": run.intent.as_dict()["run_id"],
                    "revision": str(len(view.events) + 1),
                    "previous_digest": self._operator_head,
                    "elapsed_ms": str(elapsed),
                }
            )
        )
        # Refuse semantic corruption before any file or head mutation. A failed
        # measured binding must leave the original uncertain intent inspectable.
        _validate_events(view.recipe, (*view.events, event), run.intent.as_dict()["run_id"])
        self._store.operator_append(event, self._operator_head)
        self._operator_head = event.digest
        _operator_load(self._store, run)
        return event

    def intend_observer(self, role: str) -> str:
        """A durable attempt, never permission to read, @spec COMMISSIONING-SERIES-8."""
        _require(type(role) is str and role in {"evidence-observer", "acl-observer"})
        with self._operation(positive=True) as run:
            _require(run.journal_phase in {"intent", "prepared", "running"})
            view = _operator_load(self._store, run)
            limit = next(row for row in view.recipe.as_dict()["roles"] if row["role"] == role)
            count = sum(
                e.as_dict()["kind"] == "intended" and e.as_dict()["role"] == role
                for e in view.events
            )
            _require(count < int(limit["max_connections"]))
            ticket = str(uuid4())
            self._operator_append(
                {
                    "kind": "intended",
                    "intent_id": ticket,
                    "role": role,
                    "phase": "trial" if run.journal_phase == "running" else "setup",
                    "ordinal": "1" if run.journal_phase == "running" else "0",
                    "binding_digest": None,
                    "identity": None,
                },
                self._elapsed(positive=True),
                run,
            )
            self._elapsed(positive=True)
            return ticket

    def _record_transport(
        self, ticket: str, *, kind: str, identity: dict[str, Any] | None = None
    ) -> OperatorEvent:
        # Private measured-adapter seam. These records do not themselves claim
        # independently measured rights, full membership or successful cleanup.
        _require(kind in {"bound", "closed", "uncertain"})
        with self._operation(positive=kind == "bound") as run:
            rows = [
                e
                for e in _operator_load(self._store, run).events
                if e.as_dict()["intent_id"] == ticket
            ]
            _require(bool(rows))
            last = rows[-1].as_dict()
            _require(
                last["kind"] == "intended"
                if kind == "bound"
                else last["kind"] in {"intended", "bound"}
            )
            _require(kind != "closed" or last["kind"] == "bound")
            binding = rows[-1].digest if last["kind"] == "bound" else None
            original = rows[0].as_dict()
            result = self._operator_append(
                {key: original[key] for key in ("intent_id", "role", "phase", "ordinal")}
                | {"kind": kind, "binding_digest": binding, "identity": identity},
                self._elapsed(positive=kind == "bound"),
                run,
            )
            if kind == "bound":
                self._elapsed(positive=True)
            return result
