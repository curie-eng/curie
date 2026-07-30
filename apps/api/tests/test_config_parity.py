"""API/worker dead-letter graveyard name parity (#668).

The API's dead-letter watcher (#531) and the worker's delivery-cap
dead-letterer (#505, ADR-0039) must agree on the graveyard stream name, or the
watcher reads a stream the worker never writes to and every dead-letter goes
unobserved. This module is the parity contract: `Settings.dead_letter_stream_name()`
(apps/api) and `WorkerConfig.dead_letter_stream_name()` (apps/worker) must
resolve to the SAME value under every operator override, including
`CURIE_STREAM` alone (today the API only reads `RUNS_STREAM` for its base
stream, so overriding `CURIE_STREAM` diverges the two lanes).

Pure unit tests: no fixtures, no Postgres/Valkey/network. `get_settings()` is
`lru_cache`d, so every case constructs `Settings()` directly for a fresh read
of the env this case set up.
"""

from __future__ import annotations

import pytest
from curie_api.config import Settings
from curie_worker.config import WorkerConfig


def _clear_stream_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "RUNS_STREAM",
        "CURIE_STREAM",
        "CURIE_DEAD_LETTER_STREAM",
        "RESUME_DEAD_LETTER_STREAM",
    ):
        monkeypatch.delenv(name, raising=False)


def test_no_overrides_agree_on_the_default_graveyard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean env: both lanes derive `curie:runs:dead` from the shared default."""
    _clear_stream_env(monkeypatch)

    api_name = Settings().dead_letter_stream_name()
    worker_name = WorkerConfig().dead_letter_stream_name()

    assert api_name == worker_name == "curie:runs:dead"


def test_dead_letter_stream_override_agrees_across_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit CURIE_DEAD_LETTER_STREAM reaches both lanes identically."""
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "operations:dead")

    api_name = Settings().dead_letter_stream_name()
    worker_name = WorkerConfig().dead_letter_stream_name()

    assert api_name == worker_name == "operations:dead"


def test_curie_stream_override_alone_agrees_across_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Overriding only CURIE_STREAM (no explicit dead-letter override) must
    still derive the same graveyard name on both lanes.

    This is the case that fails today: the API's `runs_stream` currently reads
    only `RUNS_STREAM`, so an operator who overrides `CURIE_STREAM` (the
    worker's base-stream var) moves the worker's graveyard to
    `operations:dead` while the API's watcher stays on `curie:runs:dead`.
    """
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("CURIE_STREAM", "operations")

    api_name = Settings().dead_letter_stream_name()
    worker_name = WorkerConfig().dead_letter_stream_name()

    assert api_name == worker_name == "operations:dead"


def test_runs_stream_takes_precedence_over_curie_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical RUNS_STREAM precedence is preserved once AliasChoices lands.

    With both `RUNS_STREAM` and `CURIE_STREAM` set, the API's `runs_stream`
    must resolve to the `RUNS_STREAM` value -- the narrower, historically
    supported override wins over the newer shared alias. Today, with no
    AliasChoices in place, `runs_stream` reads only `RUNS_STREAM`, so this
    already passes; it guards the ordering once the alias is added.
    """
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("RUNS_STREAM", "runs-legacy")
    monkeypatch.setenv("CURIE_STREAM", "operations")

    assert Settings().runs_stream == "runs-legacy"


class TestResumeDeadLetterStreamCoherence:
    """The resume reconciler's backstop (#532) reads
    `resume_dead_letter_stream or dead_letter_stream_name()` (see main.py) to
    pick the graveyard it scans. That expression must land on the SAME stream
    the worker actually writes dead-letters to.
    """

    def test_empty_resume_override_falls_back_to_the_shared_graveyard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No RESUME_DEAD_LETTER_STREAM: the fallback derives the worker's
        graveyard name from CURIE_DEAD_LETTER_STREAM, same as the watcher."""
        _clear_stream_env(monkeypatch)
        monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "operations:dead")

        settings = Settings()
        resolved = settings.resume_dead_letter_stream or settings.dead_letter_stream_name()

        assert resolved == WorkerConfig().dead_letter_stream_name() == "operations:dead"

    def test_explicit_resume_override_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A narrower RESUME_DEAD_LETTER_STREAM override beats the derived name."""
        _clear_stream_env(monkeypatch)
        monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "operations:dead")
        monkeypatch.setenv("RESUME_DEAD_LETTER_STREAM", "custom:grave")

        settings = Settings()
        resolved = settings.resume_dead_letter_stream or settings.dead_letter_stream_name()

        assert resolved == "custom:grave"
