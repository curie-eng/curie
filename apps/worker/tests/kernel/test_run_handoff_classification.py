"""Run handoff telemetry is not a publication side effect."""

from curie_worker.kernel.publication import _read_only_no_change_tools


def test_handoff_telemetry_preserves_no_change_follow_up() -> None:
    assert _read_only_no_change_tools(
        frozenset({"Read", "mcp__curie__report_run_handoff"}), False
    )
    assert not _read_only_no_change_tools(
        frozenset({"Edit", "mcp__curie__report_run_handoff"}), False
    )
    assert not _read_only_no_change_tools(
        frozenset({"Read", "mcp__curie__report_run_handoff"}), True
    )
