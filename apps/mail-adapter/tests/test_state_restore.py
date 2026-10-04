"""SQLite file-copy restore preserves pending work and dedup receipts.

This is the mail adapter's existing supported restore mechanism (stop the
writer, copy the file, open the copy before the writer starts). #2427 uses it
as synthetic delivery-state; it does not add a second store or replay path.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from curie_mail_adapter.state import MailState


def test_copied_sqlite_file_keeps_pending_work_and_refuses_duplicate_ids(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source" / "state.sqlite3"
    target = tmp_path / "target" / "state.sqlite3"
    target.parent.mkdir()
    state = MailState(str(source), max_pending=20, max_bytes=8 * 1024 * 1024)
    assert state.admit({"message_id": "msg-pending", "thread_id": "thr-pending"}) == "admitted"
    assert state.record_terminal("msg-known", "accepted") == "admitted"
    state.connection.execute("PRAGMA wal_checkpoint(FULL)")
    state.connection.close()

    shutil.copy2(source, target)

    restored = MailState(str(target), max_pending=20, max_bytes=8 * 1024 * 1024)
    assert [row["message_id"] for row in restored.pending()] == ["msg-pending"]
    assert "msg-known" in restored.known_message_ids()
    assert restored.admit({"message_id": "msg-pending", "thread_id": "thr-pending"}) == "known"
    assert restored.admit({"message_id": "msg-known", "thread_id": "thr-other"}) == "known"
