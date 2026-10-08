"""API/worker dead-letter graveyard name parity (#668).

The API's dead-letter watcher (#531) and the worker's delivery-cap
dead-letterer (#505, ADR-0039) must agree on the graveyard stream name, or the
watcher reads a stream the worker never writes to and every dead-letter goes
unobserved. This module is the parity contract: `Settings.dead_letter_stream_name()`
(apps/api) and `WorkerConfig.dead_letter_stream_name()` (apps/worker) must
resolve to the SAME value under every operator override, including
`CURIE_STREAM` alone. It also covers the runs and eval stream names (#3565):
the API's producers and the worker's consumers must resolve the same names.

Pure unit tests: no fixtures, no Postgres/Valkey/network. `get_settings()` is
`lru_cache`d, so every case constructs `Settings()` directly for a fresh read
of the env this case set up.
"""

from __future__ import annotations

import json

import pytest
from curie_api.config import Settings
from curie_dispatcher.config import DispatcherConfig
from curie_worker import workitem_dispatch as worker_workitem_dispatch
from curie_worker.config import WorkerConfig
from pydantic import ValidationError


def test_work_item_acquire_lease_defaults_to_sixty_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS", raising=False)
    monkeypatch.delenv("WORK_ITEM_ACQUIRE_LEASE_SECONDS", raising=False)

    assert Settings().work_item_acquire_lease_seconds == 60


def test_work_item_acquire_lease_rejects_less_than_two_renewal_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS", "39")

    with pytest.raises(ValidationError, match="CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS"):
        Settings()


def test_minimum_work_item_acquire_lease_covers_two_worker_renewal_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS", "40")

    assert Settings().work_item_acquire_lease_seconds == 40
    assert 40 >= 2 * worker_workitem_dispatch._ACQUIRE_RENEW_INTERVAL_S


def _clear_stream_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "RUNS_STREAM",
        "CURIE_STREAM",
        "CURIE_EVAL_STREAM",
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


def test_conflicting_runs_stream_and_curie_stream_is_refused_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker and dispatcher read only CURIE_STREAM, so a RUNS_STREAM that
    disagrees would split the lanes; boot refuses it (#3565)."""
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("RUNS_STREAM", "runs-legacy")
    monkeypatch.setenv("CURIE_STREAM", "operations")

    with pytest.raises(ValidationError, match="CURIE_STREAM"):
        Settings()


def test_runs_stream_alone_is_still_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("RUNS_STREAM", "runs-legacy")

    assert Settings().runs_stream == "runs-legacy"


def test_agreeing_runs_stream_and_curie_stream_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("RUNS_STREAM", "operations")
    monkeypatch.setenv("CURIE_STREAM", "operations")
    monkeypatch.setenv("CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "parity-attester-secret-3")

    assert (
        Settings().runs_stream
        == WorkerConfig().stream
        == DispatcherConfig().stream
        == "operations"
    )


def test_eval_stream_default_agrees_across_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_stream_env(monkeypatch)

    assert Settings().eval_stream == WorkerConfig().eval_stream == "curie:evals"


def test_eval_stream_override_agrees_across_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_stream_env(monkeypatch)
    monkeypatch.setenv("CURIE_EVAL_STREAM", "operations:evals")

    assert Settings().eval_stream == WorkerConfig().eval_stream == "operations:evals"


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

    def test_explicit_resume_override_wins(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A narrower RESUME_DEAD_LETTER_STREAM override beats the derived name."""
        _clear_stream_env(monkeypatch)
        monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "operations:dead")
        monkeypatch.setenv("RESUME_DEAD_LETTER_STREAM", "custom:grave")

        settings = Settings()
        resolved = settings.resume_dead_letter_stream or settings.dead_letter_stream_name()

        assert resolved == "custom:grave"


def test_the_three_lanes_parse_one_slack_identity_declaration_alike(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0168 decision 1: the chart renders ONE `CURIE_SLACK_IDENTITIES`
    string into the dispatcher, the worker and the API, and each must read the
    same identities from it. Decisions 2 and 5 then pick tokens by these names."""

    declaration = json.dumps(
        [
            {
                "name": "default",
                "app_token_env": "SLACK_APP_TOKEN",
                "bot_token_env": "SLACK_BOT_TOKEN",
                "signing_secret_env": "SLACK_SIGNING_SECRET",
            },
            {
                "name": "second",
                "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
                "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
                "signing_secret_env": None,
            },
        ]
    )
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", declaration)
    # The dispatcher refuses to boot without an independent attester secret.
    monkeypatch.setenv("CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "parity-attester-secret")

    api = Settings().slack_identities
    worker = WorkerConfig().slack_identities
    dispatcher = DispatcherConfig().slack_identities

    assert api == worker == dispatcher
    assert [identity.name for identity in api] == ["default", "second"]


def test_a_bare_slack_identities_env_var_is_ignored_by_all_three_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only `CURIE_SLACK_IDENTITIES` is the chart's reserved name (ADR-0168
    decision 1); a same-named `SLACK_IDENTITIES` elsewhere in the pod env (an
    operator's `.env`, `api.extraEnv`, ...) must not be read by any of the
    three, or the API could admit names the dispatcher and worker never see."""

    monkeypatch.delenv("CURIE_SLACK_IDENTITIES", raising=False)
    monkeypatch.setenv(
        "SLACK_IDENTITIES",
        json.dumps(
            [
                {
                    "name": "default",
                    "app_token_env": "SLACK_APP_TOKEN",
                    "bot_token_env": "SLACK_BOT_TOKEN",
                    "signing_secret_env": "SLACK_SIGNING_SECRET",
                },
                {
                    "name": "second",
                    "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
                    "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
                    "signing_secret_env": None,
                },
            ]
        ),
    )
    monkeypatch.setenv("CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "parity-attester-secret-2")

    assert Settings().slack_identities == ()
    assert WorkerConfig().slack_identities == ()
    assert DispatcherConfig().slack_identities == ()
