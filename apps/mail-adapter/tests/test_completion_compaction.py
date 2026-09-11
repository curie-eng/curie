"""Bounded compaction of terminal completion_events, and offline over-budget recovery.

#2509: delivered and deleted completion rows were immortal, so admission returned
full forever and a lowered CURIE_MAIL_MAX_STATE_BYTES refused to boot. These
tests drive the real MailState and MailAdapter APIs (and the recover entry
point). They do not patch internals.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from curie_mail_adapter.adapter import EMPTY_REPLY_TEXT, EVENT_MARKER, MailAdapter
from curie_mail_adapter.agentmail import AgentMailClient
from curie_mail_adapter.state import MailState

SMALL_CAP = 128 * 1024
TERMINAL_COMPLETION_MAX = 4096


def derived_completion_cap(max_bytes: int = SMALL_CAP) -> int:
    page_size = 4096
    return max(8, min(TERMINAL_COMPLETION_MAX, max(1, max_bytes // page_size) // 4))


def closed_world_env(home: str) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": home,
        "PYTHONUNBUFFERED": "1",
    }
    if os.environ.get("PYTHONPATH"):
        env["PYTHONPATH"] = os.environ["PYTHONPATH"]
    return env


def open_state(
    path: Path,
    *,
    max_bytes: int = SMALL_CAP,
    max_pending: int = 1000,
    **kwargs: Any,
) -> MailState:
    return MailState(str(path), max_pending=max_pending, max_bytes=max_bytes, **kwargs)


def completion_rows(state: MailState) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in state.connection.execute(
        "SELECT event_id, delivered, deleted, lease_owner, lease_until, updated_at "
        "FROM completion_events ORDER BY updated_at, event_id"
    ):
        out[row[0]] = {
            "delivered": int(row[1]),
            "deleted": int(row[2]),
            "lease_owner": row[3],
            "lease_until": row[4],
            "updated_at": row[5],
        }
    return out


def classify(row: dict[str, Any], now: float) -> str:
    if row["deleted"] == 1 and row["delivered"] == 1:
        return "delivered+deleted"
    if row["deleted"] == 1:
        return "deleted"
    if row["delivered"] == 1:
        return "delivered"
    if row["lease_owner"] and float(row["lease_until"] or 0) > now:
        return "leased"
    return "unresolved"


def terminal_count(state: MailState) -> int:
    return int(
        state.connection.execute(
            "SELECT count(*) FROM completion_events WHERE delivered=1 OR deleted=1"
        ).fetchone()[0]
    )


def set_updated_at(state: MailState, event_id: str, when: float) -> None:
    with state.transaction() as connection:
        connection.execute(
            "UPDATE completion_events SET updated_at=? WHERE event_id=?",
            (when, event_id),
        )


def pages(state: MailState) -> dict[str, int]:
    connection = state.connection
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
    freelist = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "page_size": page_size,
        "page_count": page_count,
        "freelist": freelist,
        "used_bytes": page_size * (page_count - freelist),
        "file_bytes": state.path.stat().st_size,
    }


def fill_delivered_until_full(state: MailState, prefix: str = "a") -> int:
    """Grow only completion_events until admit returns full. Withdraw each probe."""
    n = 0
    while True:
        probe = f"{prefix}-probe-{n}"
        if state.admit({"message_id": probe, "thread_id": "t"}) == "full":
            return n
        with state.transaction() as connection:
            connection.execute("DELETE FROM deliveries WHERE message_id=?", (probe,))
        state.claim_event(
            f"{prefix}-{n:07d}",
            "conversation-" + "x" * 40,
            "reply-" + "y" * 40,
            "owner",
        )
        state.finish_event(f"{prefix}-{n:07d}")
        n += 1
        assert n < 200_000


def test_real_transitions_identify_each_terminal_class_separately(tmp_path: Path) -> None:
    """delivered=1 OR deleted=1; the accidental both-flag row is not the only class."""
    state = open_state(tmp_path / "s.sqlite3")
    assert state.claim_event("ev-unres", "thr", "msg", "ownerA") == "claimed"
    state.release_event("ev-unres", "ownerA")
    assert state.claim_event("ev-leased", "thr", "msg", "ownerA") == "claimed"
    assert state.claim_event("ev-leased", "thr", "msg", "ownerB") == "busy"
    assert state.claim_event("ev-deliv", "thr", "msg", "ownerA") == "claimed"
    state.finish_event("ev-deliv")
    assert state.claim_event("ev-deliv", "thr", "msg", "ownerB") == "done"
    assert state.claim_event("ev-del", "thr", "msg", "ownerA") == "claimed"
    state.delete_event("ev-del", "thr", "msg")
    assert state.claim_event("ev-del", "thr", "msg", "ownerB") == "deleted"
    assert state.claim_event("ev-both", "thr", "msg", "ownerA") == "claimed"
    state.finish_event("ev-both")
    state.delete_event("ev-both", "thr", "msg")
    assert state.claim_event("ev-both", "thr", "msg", "ownerB") == "deleted"
    assert state.claim_event("ev-expired", "thr", "msg", "ownerA") == "claimed"
    with state.transaction() as connection:
        connection.execute(
            "UPDATE completion_events SET lease_until=? WHERE event_id='ev-expired'",
            (time.time() - 1,),
        )
    assert state.claim_event("ev-expired", "thr", "msg", "ownerB") == "claimed"

    table = completion_rows(state)
    classes = {key: classify(value, time.time()) for key, value in table.items()}
    assert classes == {
        "ev-unres": "unresolved",
        "ev-leased": "leased",
        "ev-deliv": "delivered",
        "ev-del": "deleted",
        "ev-both": "delivered+deleted",
        "ev-expired": "leased",
    }
    assert table["ev-deliv"]["lease_owner"] is None
    assert table["ev-del"]["lease_owner"] is None
    state.close()

    reopened = open_state(tmp_path / "s.sqlite3")
    after = completion_rows(reopened)
    assert after["ev-leased"]["lease_owner"] is None
    assert reopened.claim_event("ev-leased", "thr", "msg", "ownerC") == "claimed"
    reopened.close()


def test_oldest_terminal_rows_are_evicted_unresolved_and_leased_survive(
    tmp_path: Path,
) -> None:
    state = open_state(tmp_path / "s.sqlite3", terminal_completion_max=1_000_000)
    cap = derived_completion_cap()
    base = 1_000_000.0
    delivered = cap + 4
    for index in range(delivered):
        event_id = f"d{index:02d}"
        state.claim_event(event_id, "thr", "msg", "o")
        state.finish_event(event_id)
        set_updated_at(state, event_id, base + index)
    for index in range(3):
        event_id = f"x{index}"
        state.claim_event(event_id, "thr", "msg", "o")
        state.delete_event(event_id, "thr", "msg")
        set_updated_at(state, event_id, base - 10 + index)
    for index in range(2):
        event_id = f"u{index}"
        state.claim_event(event_id, "thr", "msg", "o")
        state.release_event(event_id, "o")
        set_updated_at(state, event_id, base - 100)
    for index in range(2):
        event_id = f"l{index}"
        state.claim_event(event_id, "thr", "msg", "leaser")
        set_updated_at(state, event_id, base - 100)
    state.terminal_completion_max = cap
    assert state.admit({"message_id": "kick", "thread_id": "t"}) == "admitted"

    after = completion_rows(state)
    assert {key for key in after if key[0] in "ul"} == {"u0", "u1", "l0", "l1"}
    terminals = [key for key in after if key[0] in "dx"]
    assert len(terminals) == cap
    assert terminals == [f"d{index:02d}" for index in range(delivered - cap, delivered)]
    assert "x0" not in after
    assert terminal_count(state) == cap
    state.close()


def test_equal_updated_at_breaks_ties_on_event_id(tmp_path: Path) -> None:
    state = open_state(tmp_path / "s.sqlite3", terminal_completion_max=1_000_000)
    for name in ("b", "a", "c"):
        state.claim_event(name, "thr", "msg", "o")
        state.finish_event(name)
        set_updated_at(state, name, 5.0)
    state.terminal_completion_max = 1
    assert state.admit({"message_id": "kick", "thread_id": "t"}) == "admitted"
    assert list(completion_rows(state)) == ["c"]
    state.close()


def test_literal_and_predicate_would_leave_delivered_only_rows(
    tmp_path: Path,
) -> None:
    """Negative: requiring both flags would not bound the table the issue named."""
    state = open_state(tmp_path / "s.sqlite3")
    cap = state.terminal_completion_max
    for index in range(cap + 5):
        event_id = f"d{index:02d}"
        state.claim_event(event_id, "thr", "msg", "o")
        state.finish_event(event_id)
    both = int(
        state.connection.execute(
            "SELECT count(*) FROM completion_events WHERE delivered=1 AND deleted=1"
        ).fetchone()[0]
    )
    assert both == 0
    assert terminal_count(state) == cap
    state.close()


def test_filling_delivered_completions_does_not_permanently_refuse_ingress(
    tmp_path: Path,
) -> None:
    state = open_state(tmp_path / "s.sqlite3")
    cap = state.terminal_completion_max
    for index in range(cap + 50):
        state.claim_event(f"e{index:05d}", "thr", "msg", "o")
        state.finish_event(f"e{index:05d}")
    assert terminal_count(state) <= cap
    assert state.admit({"message_id": "late", "thread_id": "t"}) == "admitted"
    state.close()


def test_reverting_the_compact_leaves_a_full_file_refusing_ingress(tmp_path: Path) -> None:
    """Falsifiable negative: without a bound, delivered rows fill the page budget."""
    path = tmp_path / "s.sqlite3"
    state = open_state(path, terminal_completion_max=1_000_000)
    fill_delivered_until_full(state)
    assert state.admit({"message_id": "late", "thread_id": "t"}) == "full"
    assert terminal_count(state) > state.terminal_receipt_max
    state.close()


def test_a_file_already_full_of_delivered_rows_recovers_on_reopen(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite3"
    fat = open_state(path, terminal_completion_max=1_000_000)
    fill_delivered_until_full(fat)
    assert fat.admit({"message_id": "blocked", "thread_id": "t"}) == "full"
    fat.close()
    recovered = open_state(path)
    assert recovered.admit({"message_id": "late", "thread_id": "t"}) == "admitted"
    assert terminal_count(recovered) <= recovered.terminal_completion_max
    recovered.close()


def test_unresolved_and_leased_rows_are_not_evicted_to_admit(tmp_path: Path) -> None:
    state = open_state(tmp_path / "s.sqlite3")
    n = 0
    while True:
        probe = f"probe-{n}"
        if state.admit({"message_id": probe, "thread_id": "t"}) == "full":
            break
        with state.transaction() as connection:
            connection.execute("DELETE FROM deliveries WHERE message_id=?", (probe,))
        event_id = f"u{n:07d}"
        state.claim_event(event_id, "c" + "x" * 40, "r" + "y" * 40, "owner")
        if n % 2 == 0:
            state.release_event(event_id, "owner")
        n += 1
        assert n < 200_000
    remaining = completion_rows(state)
    assert remaining
    assert all(classify(row, time.time()) in {"unresolved", "leased"} for row in remaining.values())
    assert state.admit({"message_id": "newer", "thread_id": "t"}) == "full"
    assert set(completion_rows(state)) == set(remaining)
    state.close()


def test_delete_frees_pages_for_reuse_but_does_not_shrink_the_file(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite3"
    fat = open_state(path, terminal_completion_max=1_000_000)
    n = fill_delivered_until_full(fat)
    fat.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    full = pages(fat)
    assert full["used_bytes"] >= SMALL_CAP or full["page_count"] * full["page_size"] >= SMALL_CAP
    fat.close()

    recovered = open_state(path)
    recovered.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    freed = pages(recovered)
    assert freed["freelist"] > 0
    assert freed["file_bytes"] == full["file_bytes"]
    assert freed["used_bytes"] < full["used_bytes"]
    assert recovered.admit({"message_id": "late", "thread_id": "t"}) == "admitted"
    before_refill = pages(recovered)
    for index in range(min(n // 4, recovered.terminal_completion_max)):
        recovered.claim_event(f"b-{index:07d}", "c" + "x" * 40, "r" + "y" * 40, "owner")
        recovered.finish_event(f"b-{index:07d}")
    recovered.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    refilled = pages(recovered)
    assert refilled["page_count"] == before_refill["page_count"]
    recovered.close()


class _StubClient(AgentMailClient):
    def __init__(self, config: Any) -> None:
        super().__init__(config)
        self.thread_status = 200
        self.thread_messages: list[dict[str, Any]] = []
        self.thread_body: Any = None
        self.get_thread_calls = 0
        self.replies: list[tuple[str, str]] = []

    def get_thread(self, thread_id: str) -> tuple[int, Any]:
        self.get_thread_calls += 1
        if self.thread_status == 200:
            return 200, {"thread_id": thread_id, "messages": list(self.thread_messages)}
        return self.thread_status, self.thread_body

    def reply(self, message_id: str, text: str) -> tuple[int, Any]:
        self.replies.append((message_id, text))
        return 200, {"message_id": "sent-1"}


def _adapter(make_config: Any, tmp_path: Path, name: str) -> tuple[MailAdapter, _StubClient]:
    config = make_config(
        max_state_bytes=SMALL_CAP,
        state_path=str(tmp_path / f"{name}.sqlite3"),
    )
    client = _StubClient(config)
    return MailAdapter(config, client=client), client


def _admit_turn(adapter: MailAdapter, conversation_id: str, reply_ref: str) -> None:
    adapter.state.admit({"message_id": reply_ref, "thread_id": conversation_id})
    adapter.state.store_turn(
        reply_ref, {"conversation_id": conversation_id, "reply_ref": reply_ref, "text": "hi"}
    )
    adapter.state.record_text(conversation_id, reply_ref, "answer", append=False, max_bytes=1 << 20)


def _overflow_terminal_cap(adapter: MailAdapter) -> str:
    """Send one more completion than the cap; return the oldest event id."""
    cap = adapter.state.terminal_completion_max
    oldest = "ev-000"
    for index in range(cap + 1):
        event_id = f"ev-{index:03d}"
        conversation_id = f"thr-{index:03d}"
        reply_ref = f"msg-{index:03d}"
        if index == 0:
            oldest = event_id
        _admit_turn(adapter, conversation_id, reply_ref)
        assert adapter.send_reply(event_id, conversation_id, reply_ref) == 200
    return oldest


def test_retained_delivered_row_short_circuits_without_provider(
    make_config: Any, tmp_path: Path
) -> None:
    adapter, client = _adapter(make_config, tmp_path, "retain")
    _admit_turn(adapter, "thr", "msg")
    assert adapter.send_reply("ev1", "thr", "msg") == 200
    assert len(client.replies) == 1 and EVENT_MARKER in client.replies[0][1]
    calls = client.get_thread_calls
    assert adapter.send_reply("ev1", "thr", "msg") == 200
    assert client.get_thread_calls == calls and len(client.replies) == 1
    adapter.close()


def test_evicted_delivered_row_recovers_from_provider_witness(
    make_config: Any, tmp_path: Path
) -> None:
    adapter, client = _adapter(make_config, tmp_path, "witness")
    oldest = _overflow_terminal_cap(adapter)
    assert oldest not in completion_rows(adapter.state)
    sent = next(text for message_id, text in client.replies if oldest in text)
    client.thread_messages = [{"text": sent}]
    calls = client.get_thread_calls
    replies = len(client.replies)
    assert adapter.send_reply(oldest, "thr-000", "msg-000") == 200
    assert client.get_thread_calls == calls + 1
    assert len(client.replies) == replies
    assert classify(completion_rows(adapter.state)[oldest], time.time()) == "delivered"
    adapter.close()


def test_evicted_delivered_row_without_witness_sends_an_empty_duplicate(
    make_config: Any, tmp_path: Path
) -> None:
    """Replay horizon is the provider marker: eviction plus a lost witness resends."""
    adapter, client = _adapter(make_config, tmp_path, "nowitness")
    oldest = _overflow_terminal_cap(adapter)
    replies = len(client.replies)
    client.thread_messages = []
    status = adapter.send_reply(oldest, "thr-000", "msg-000")
    assert status == 200
    assert len(client.replies) == replies + 1
    assert client.replies[-1][1].startswith(EMPTY_REPLY_TEXT)
    adapter.close()


def test_retained_row_still_blocks_resend_when_the_witness_is_gone(
    make_config: Any, tmp_path: Path
) -> None:
    adapter, client = _adapter(make_config, tmp_path, "control")
    _admit_turn(adapter, "thr", "msg")
    assert adapter.send_reply("ev1", "thr", "msg") == 200
    client.thread_messages = []
    assert adapter.send_reply("ev1", "thr", "msg") == 200
    assert len(client.replies) == 1
    adapter.close()


def test_evicted_tombstone_replays_confirmed_404_and_refuses_ambiguous(
    make_config: Any, tmp_path: Path
) -> None:
    adapter, client = _adapter(make_config, tmp_path, "tomb")
    cap = adapter.state.terminal_completion_max
    client.thread_status, client.thread_body = 404, {"error": "not found"}
    for index in range(cap + 1):
        event_id = f"ev-{index:03d}"
        _admit_turn(adapter, f"thr-{index:03d}", f"msg-{index:03d}")
        assert adapter.send_reply(event_id, f"thr-{index:03d}", f"msg-{index:03d}") == 410
    assert "ev-000" not in completion_rows(adapter.state)
    assert adapter.send_reply("ev-000", "thr-000", "msg-000") == 410
    assert client.replies == []
    adapter.state.terminal_completion_max = 0
    assert adapter.state.admit({"message_id": "evict-kick", "thread_id": "t"}) == "admitted"
    assert "ev-000" not in completion_rows(adapter.state)
    client.thread_status, client.thread_body = 404, "<html>edge</html>"
    assert adapter.send_reply("ev-000", "thr-000", "msg-000") == 502
    client.thread_status, client.thread_body = 500, None
    assert adapter.send_reply("ev-000", "thr-000", "msg-000") == 502
    client.thread_status, client.thread_messages = 200, []
    assert adapter.send_reply("ev-000", "thr-000", "msg-000") == 200
    assert len(client.replies) == 1
    assert client.replies[0][1].startswith(EMPTY_REPLY_TEXT)
    adapter.close()


def test_boot_refusal_survives_delete_alone_and_recover_shrinks_a_copy(
    tmp_path: Path,
) -> None:
    from curie_mail_adapter.recover import recover_state_file

    path = tmp_path / "s.sqlite3"
    state = open_state(path, terminal_completion_max=1_000_000)
    fill_delivered_until_full(state)
    state.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    state.close()
    lowered = SMALL_CAP // 2
    with pytest.raises(RuntimeError, match="CURIE_MAIL_MAX_STATE_BYTES"):
        open_state(path, max_bytes=lowered)

    live_bytes = path.read_bytes()
    copy = tmp_path / "copy.sqlite3"
    shutil.copy2(path, copy)
    with pytest.raises(RuntimeError, match="CURIE_MAIL_MAX_STATE_BYTES"):
        open_state(copy, max_bytes=lowered)

    result = recover_state_file(str(copy), max_pending=1000, max_bytes=lowered)
    assert path.read_bytes() == live_bytes
    assert copy.stat().st_size < lowered
    assert result["under_budget"] is True
    assert result["evicted"] > 0
    recovered = open_state(copy, max_bytes=lowered)
    assert recovered.admit({"message_id": "after", "thread_id": "t"}) == "admitted"
    recovered.close()
    with pytest.raises(RuntimeError, match="CURIE_MAIL_MAX_STATE_BYTES"):
        open_state(path, max_bytes=lowered)


def test_recover_entry_point_compacts_a_copy_without_credentials(
    tmp_path: Path,
) -> None:
    live = tmp_path / "live.sqlite3"
    state = open_state(live, terminal_completion_max=1_000_000)
    fill_delivered_until_full(state)
    state.claim_event("keep-unresolved", "c", "r", "o")
    state.release_event("keep-unresolved", "o")
    state.claim_event("keep-leased", "c", "r", "o")
    state.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    state.close()
    live_bytes = live.read_bytes()
    copy = tmp_path / "copy.sqlite3"
    shutil.copy2(live, copy)
    lowered = SMALL_CAP // 2
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "curie_mail_adapter",
            "recover",
            "--state",
            str(copy),
            "--max-bytes",
            str(lowered),
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=closed_world_env(str(tmp_path)),
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    payload = json.loads(proc.stdout)
    assert payload["under_budget"] is True
    assert live.read_bytes() == live_bytes
    check = open_state(copy, max_bytes=lowered)
    survivors = completion_rows(check)
    assert "keep-unresolved" in survivors
    assert "keep-leased" in survivors
    assert classify(survivors["keep-leased"], time.time()) in {"leased", "unresolved"}
    check.close()


def test_recover_usage_error_is_exit_2() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "curie_mail_adapter", "recover"],
        check=False,
        capture_output=True,
        text=True,
        env=closed_world_env("/tmp"),
    )
    assert proc.returncode == 2
