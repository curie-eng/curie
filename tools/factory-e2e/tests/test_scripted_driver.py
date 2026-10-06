"""Failure contracts for the disposable scripted factory driver.

These tests exercise driver validation and lifecycle bookkeeping. The real
kind scenario remains the proof that the product completes an issue.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import scripted_kind as sk


def _refuse_external_command(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("configuration must refuse before an external command")


def test_missing_replay_transcript_refuses_before_resource_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "missing-transcript.json"
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--transcript", str(missing)])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "transcript" in capsys.readouterr().err.lower()


def test_record_without_upstream_key_refuses_before_resource_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("CURIE_FACTORY_RECORD_KEY", raising=False)
    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.subprocess, "run", _refuse_external_command)
    args = fe.parse_args(["scripted", "--record", "--transcript", str(tmp_path / "record.json")])

    assert sk.main(args) == fe.EXIT_CONFIG
    assert "CURIE_FACTORY_RECORD_KEY" in capsys.readouterr().err
