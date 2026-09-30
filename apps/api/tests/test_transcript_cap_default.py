"""#3301: the default per-thread transcript cap holds a dark-factory turn.

The runner bounds each turn to the advertised cap less its 8 KiB append reserve.
A factory turn with three plan-review rounds keeps well over 56 KiB after
bounding, so the old 64 KiB default refused it at the end of the run. The
runner suite (``test_history_capacity_3301``) persists that turn at this cap.
"""

from __future__ import annotations

from curie_api.config import Settings


def test_default_transcript_cap_is_sized_for_a_factory_turn(monkeypatch) -> None:
    monkeypatch.delenv("TRANSCRIPT_MAX_THREAD_BYTES", raising=False)
    assert Settings().transcript_max_thread_bytes == 16 * 1024 * 1024


def test_transcript_cap_reads_the_chart_env(monkeypatch) -> None:
    monkeypatch.setenv("TRANSCRIPT_MAX_THREAD_BYTES", "33554432")
    assert Settings().transcript_max_thread_bytes == 33554432
