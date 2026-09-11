"""Offline recovery for a mail-adapter SQLite copy that has crossed the byte budget.

Operate on a copy, never on a live writer. Compaction frees pages; VACUUM is
what returns them to the filesystem and is what the startup size check sees.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from .config import MailAdapterConfig
from .state import MailState


def recover_state_file(
    path: str,
    *,
    max_pending: int,
    max_bytes: int,
    keep: int | None = None,
) -> dict[str, Any]:
    """Compact terminal completion_events on ``path`` and VACUUM it in place."""
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"mail state {target} does not exist")
    before = target.stat().st_size
    before_rows = _count_completions(target)
    state = MailState(
        str(target),
        max_pending=max_pending,
        max_bytes=max_bytes,
        terminal_completion_max=keep,
        enforce_size=False,
    )
    try:
        with state.transaction() as connection:
            state._compact_terminal_completions(connection)
        survivors = int(
            state.connection.execute("SELECT count(*) FROM completion_events").fetchone()[0]
        )
        state.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        state.close()
    _vacuum_file(target)
    after = target.stat().st_size
    evicted = max(0, before_rows - survivors)
    return {
        "after_bytes": after,
        "before_bytes": before,
        "evicted": evicted,
        "max_bytes": max_bytes,
        "path": str(target),
        "survivors": survivors,
        "under_budget": after <= max_bytes,
    }


def _count_completions(path: Path) -> int:
    connection = sqlite3.connect(str(path))
    try:
        row = connection.execute("SELECT count(*) FROM completion_events").fetchone()
        return int(row[0]) if row else 0
    finally:
        connection.close()


def _vacuum_file(target: Path) -> None:
    """Rebuild the file after the adapter connection has released it."""
    vacuumed = target.with_name(f"{target.name}.vacuum")
    if vacuumed.exists():
        vacuumed.unlink()
    connection = sqlite3.connect(str(target))
    try:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        escaped = str(vacuumed).replace("'", "''")
        connection.execute(f"VACUUM INTO '{escaped}'")
    finally:
        connection.close()
    os.replace(vacuumed, target)
    for suffix in ("-wal", "-shm"):
        companion = Path(f"{target}{suffix}")
        if companion.exists():
            companion.unlink()
    os.chmod(target, 0o600)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m curie_mail_adapter recover",
        description=(
            "Compact terminal completion_events and VACUUM an offline copy of "
            "the mail-adapter SQLite file. Do not point this at a live writer."
        ),
    )
    parser.add_argument("--state", required=True, help="path to the offline copy")
    parser.add_argument("--max-bytes", type=int, default=None)
    parser.add_argument("--max-pending", type=int, default=None)
    parser.add_argument(
        "--keep",
        type=int,
        default=None,
        help="terminal completion rows to retain; default is the derived cap",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    config = MailAdapterConfig()
    max_bytes = args.max_bytes if args.max_bytes is not None else config.max_state_bytes
    max_pending = (
        args.max_pending if args.max_pending is not None else config.max_pending_deliveries
    )
    if max_bytes <= 0 or max_pending <= 0:
        print("max-bytes and max-pending must be greater than zero", file=sys.stderr)
        return 2
    try:
        result = recover_state_file(
            args.state,
            max_pending=max_pending,
            max_bytes=max_bytes,
            keep=args.keep,
        )
    except FileNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    else:
        print(
            f"recovered {result['path']}: {result['before_bytes']} -> "
            f"{result['after_bytes']} bytes, evicted {result['evicted']} "
            "terminal completions"
        )
        if not result["under_budget"]:
            print(
                f"still larger than {result['max_bytes']} bytes; raise CURIE_MAIL_MAX_STATE_BYTES",
                file=sys.stderr,
            )
    return 0 if result["under_budget"] else 1
