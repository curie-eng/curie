"""The forge driver protocol the forge-neutral factory loop observes through.

The `forge-loop` scenario in factory_e2e.py drives a tracker and a code host
only through a ForgeDriver: it opens and labels the ticket, reads the change
request the WorkItem published, reads and sets CI on its head, posts a review
as the test actor, merges, and resets, pins and restores the fixture. The
WorkItem side is the platform's own api and CLI, so it is not here. One entry
module per forge in this directory implements the protocol and registers
itself; a forge whose driver is not written yet registers a driver that raises
ForgeNotInstalled.

This module imports nothing from the harness, so a driver can be type checked
and loaded on its own.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from typing import Any, NoReturn, Protocol


@dataclasses.dataclass(frozen=True)
class ChangeRef:
    """The change request (a pull request on GitHub) a WorkItem published."""

    number: int
    url: str


class ForgeNotInstalled(Exception):
    """The forge has an entry but no driver yet. Nothing was installed."""


class ForgeDriver(Protocol):
    """The forge steps the loop needs. Each forge entry implements them once."""

    forge: str
    # "<tracker>+<code host>", as the acceptance table keys its entries.
    pairing: str

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        """Open the ticket with the factory label, prove admission; the WorkItem id."""
        ...

    def await_change(self, work_item_id: str) -> ChangeRef | None:
        """The change the WorkItem records, or None when its request ends without one."""
        ...

    def change_head(self, change: ChangeRef) -> tuple[str | None, int | None]:
        """The change's head commit and its commit count."""
        ...

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        """Make CI on ``sha`` fail as the test actor; what was set."""
        ...

    def ci_state(self, sha: str) -> dict[str, Any]:
        """CI on ``sha``: ``state`` is failing, pending, passing, none or unreadable."""
        ...

    def post_review(self, change: ChangeRef, text: str) -> dict[str, Any]:
        """Request a change as the test actor; delivery facts in the result."""
        ...

    def merge(self, change: ChangeRef) -> dict[str, Any]:
        """Merge as the test actor; ``status_code`` and ``merged`` in the result."""
        ...

    def change_merged(self, change: ChangeRef) -> bool | None:
        """Whether the forge reads the change as merged; None when unreadable."""
        ...

    def changes_opened(self) -> list[int]:
        """Every change number the run opened on the fixture."""
        ...

    def default_branch_head(self) -> str:
        """The fixture's default branch head commit."""
        ...

    def reset_fixture(self) -> dict[str, Any]:
        """Return the fixture to its known start state; what was reset."""
        ...

    def pin_fixture_base(self) -> None:
        """Remember the fixture's default branch head now and put it back on teardown."""
        ...

    def teardowns(self) -> tuple[tuple[str, Callable[[], Any]], ...]:
        """Named undo steps of the driver's own, pushed right after the fixture reset."""
        ...

    def tracker_evidence(self, change: ChangeRef | None, work_item_id: str) -> dict[str, Any]:
        """What the tracker shows for the run once the merge is observed; {} when nothing."""
        ...

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        """Every way ``evidence`` falls short, one line each. Empty means it passed."""
        ...


def not_installed_driver(kind: str) -> Callable[[Any], NoReturn]:
    """A driver factory for a forge entry whose driver is not written yet."""

    def refuse(_preflight: Any) -> NoReturn:
        raise ForgeNotInstalled(f"{kind} forge driver is not installed")

    return refuse
