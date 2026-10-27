"""Public learning-eval contracts for #4780.

Serialized observations below test evidence consistency, never provider
authenticity or payment. Live observations are reused from an explicitly
selected bounded campaign. This suite never starts a paid campaign.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import types
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from curie_worker.eval.learning import (
    load_calibration,
    load_cases,
    main,
    verify_calibration,
    verify_cleanup,
    verify_negative,
    verify_report,
)
from curie_worker.eval.learning_judge import JudgeSpec, LearningJudgeScorer
from curie_worker.eval.models import EvalCase, Grader, GraderKind
from curie_worker.eval.scorer import ScoreResult

ROOT = Path(__file__).resolve().parents[4]
FIXTURES = ROOT / "apps/worker/src/curie_worker/eval/fixtures"
CASE_IDS = tuple(f"F{family}-P{phrase}" for family in range(1, 9) for phrase in range(1, 4))
CLASSES = ("skill", "memory-fact", "instruction", "schedule", "clarify", "nothing")
EXPECTED = (
    "skill",
    "instruction",
    "memory-fact",
    "schedule",
    "schedule",
    "clarify",
    "nothing",
    "skill",
)
ACTOR = "anthropic/claude-haiku-5.5"
JUDGE = "anthropic/claude-sonnet-5.5"
THRESHOLDS = {
    "precision": 0.9,
    "recall": 0.8,
    "falsewrite": 0.05,
    "correction_duplicates": 0,
    "heldout_control_improvement": 0.0,
    "scheduled_deliveries": 1,
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _write(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(value))
    return path


def _score(result: ScoreResult, passed: bool) -> None:
    assert isinstance(result, ScoreResult)
    assert result.passed is passed, result.detail
    if not passed:
        assert result.detail and result.detail.strip()


@pytest.mark.parametrize(
    "loader,name",
    [(load_cases, "learning-cases.json"), (load_calibration, "learning-calibration.json")],
)
def test_committed_fixtures_are_live_inputs(loader: Callable[..., Any], name: str) -> None:
    rows = loader(FIXTURES / name)
    assert isinstance(rows, tuple)
    assert rows and all(isinstance(row, dict) for row in rows)


def test_cases_have_all_eight_families_and_three_primary_paraphrases() -> None:
    rows = load_cases(FIXTURES / "learning-cases.json")
    assert len(rows) == 24
    assert {row["id"] for row in rows} == set(CASE_IDS)
    assert Counter(str(row["id"]).split("-")[0] for row in rows) == {
        f"F{family}": 3 for family in range(1, 9)
    }


def test_calibration_has_two_examples_in_each_required_group() -> None:
    rows = load_calibration(FIXTURES / "learning-calibration.json")
    groups = Counter(row["group"] for row in rows)
    assert set(groups) == {"good", "omitted", "obsolete", "duplicate", "fabricated"}
    assert all(count >= 2 for count in groups.values())


@pytest.mark.parametrize("loader", [load_cases, load_calibration])
@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "directory",
        "invalid-json",
        "invalid-utf8",
        "duplicate-key",
        "null",
        "empty",
        "version",
        "boolean-version",
    ],
)
def test_required_fixture_inputs_fail_closed(
    tmp_path: Path, loader: Callable[..., Any], kind: str
) -> None:
    path = tmp_path / "input.json"
    if kind == "directory":
        path.mkdir()
    elif kind != "missing":
        data = {
            "invalid-json": b"{",
            "invalid-utf8": b'{"version":1,"value":"\xff"}',
            "duplicate-key": b'{"version":1,"version":1,"families":[]}',
            "null": b"null",
            "empty": b"{}",
            "version": b'{"version":2,"families":[],"examples":[]}',
            "boolean-version": b'{"version":true,"families":[],"examples":[]}',
        }[kind]
        path.write_bytes(data)
    with pytest.raises((ValueError, OSError, UnicodeError)):
        loader(path)


@pytest.mark.parametrize(
    "loader,name",
    [(load_cases, "learning-cases.json"), (load_calibration, "learning-calibration.json")],
)
def test_fixture_reader_propagates_unreadable_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, loader: Callable[..., Any], name: str
) -> None:
    path = tmp_path / name
    path.write_bytes((FIXTURES / name).read_bytes())
    original = Path.open

    def denied(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self == path:
            raise PermissionError("fixture read refused")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied)
    with pytest.raises((ValueError, OSError)):
        loader(path)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-family",
        "duplicate-case",
        "missing-paraphrase",
        "wrong-family",
        "unknown-predicate",
        "missing-predicate-argument",
    ],
)
def test_corpus_reader_rejects_incomplete_or_unscorable_inventory(
    tmp_path: Path, mutation: str
) -> None:
    document = json.loads((FIXTURES / "learning-cases.json").read_bytes())
    families = document["families"]
    if mutation == "missing-family":
        families.pop()
    elif mutation == "duplicate-case":
        families[0]["scenarios"][1]["id"] = families[0]["scenarios"][0]["id"]
    elif mutation == "missing-paraphrase":
        families[0]["scenarios"].pop()
    elif mutation == "wrong-family":
        families[0]["scenarios"][0]["id"] = "F9-P1"
    else:
        predicates = families[0]["scenarios"][0]["decisions"][0]["predicates"]
        if mutation == "unknown-predicate":
            predicates.append({"name": "trust_reply", "class": "skill", "target": "procedure"})
        else:
            predicates.append({"name": "active_count", "class": "skill"})
    with pytest.raises(ValueError):
        load_cases(_write(tmp_path / "cases.json", document))


def _module_exit(argv: list[str]) -> int:
    try:
        return main(argv)
    except SystemExit as exc:
        assert isinstance(exc.code, int)
        return exc.code


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["unknown"],
        ["run"],
        ["run", "--curie-bin", "curie", "--output-dir", "output", "--dry-run"],
        ["run", "--curie-bin", "/does-not-exist", "--output-dir", "output", "--dry-run"],
        ["verify-report", "report.json"],
        ["verify-calibration", "report.json"],
        ["verify-negative", "report.json"],
        ["verify-cleanup", "report.json"],
    ],
)
def test_module_parser_requires_named_command_and_absolute_pinned_executable(
    args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert _module_exit(args) != 0
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "validator", [verify_report, verify_calibration, verify_negative, verify_cleanup]
)
@pytest.mark.parametrize(
    "data",
    [
        None,
        b"{",
        b"null",
        b"{}",
        b'{"report_version":true}',
        b'{"report_version":2}',
        b'{"report_version":1,"report_version":1}',
        b'{"report_version":1,"seed":"\xff"}',
    ],
)
def test_public_validators_reject_missing_and_malformed_reports(
    tmp_path: Path, validator: Callable[[Path], ScoreResult], data: bytes | None
) -> None:
    path = tmp_path / "report.json"
    if data is not None:
        path.write_bytes(data)
    _score(validator(path), False)


@pytest.mark.parametrize(
    "validator", [verify_report, verify_calibration, verify_negative, verify_cleanup]
)
def test_public_validators_reject_unreadable_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, validator: Callable[[Path], ScoreResult]
) -> None:
    path = _write(tmp_path / "report.json", {"report_version": 1})
    original = Path.open

    def denied(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self == path:
            raise PermissionError("report read refused")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied)
    _score(validator(path), False)


def _evidence(report: dict[str, Any], kind: str, value: dict[str, Any]) -> str:
    evidence_id = f"{kind}-{len(report['evidence']) + 1}"
    report["evidence"][evidence_id] = {
        "kind": kind,
        "sha256": _sha(_json_bytes(value)),
        "value": value,
    }
    return evidence_id


def _value(report: dict[str, Any], ref: str) -> dict[str, Any]:
    return report["evidence"][ref]["value"]


def _rehash(report: dict[str, Any]) -> None:
    for record in report["evidence"].values():
        record["sha256"] = _sha(_json_bytes(record["value"]))


def _invocation(
    report: dict[str, Any], scope: dict[str, Any], args: list[str], result: object
) -> tuple[str, str]:
    result_ref = _evidence(report, "cli-result", {**scope, "result": result})
    invocation_id = f"invocation-{len(report['invocations']) + 1}"
    report["invocations"].append(
        {
            **scope,
            "invocation_id": invocation_id,
            "argv": [report["binary"]["path"], *args],
            "exit_code": 0,
            "binary": copy.deepcopy(report["binary"]),
            "source_root": report["source_root"],
            "source_tree_sha256": report["source_tree_sha256"],
            "cwd": report["source_root"],
            "result_ref": result_ref,
        }
    )
    return invocation_id, result_ref


def _observation(
    report: dict[str, Any], scope: dict[str, Any], *, value: str, present: bool, output: str
) -> str:
    fact_id = f"fact-{scope['case_id']}"
    facts = [_fact_row(fact_id, value)] if present else []
    _, snapshot = _invocation(
        report,
        scope,
        ["--json", "local", "memory", scope["agent_id"]],
        {
            "agent": scope["agent_id"],
            "entries": [],
            "facts": facts,
        },
    )
    index = len(report["invocations"]) + 1
    thread = f"1800000000.{index:06d}"
    invocation_id, result_ref = _invocation(
        report,
        scope,
        [
            "--json",
            "local",
            "message",
            "What is the reference value?",
            "--agent",
            scope["agent_id"],
            "--channel",
            "C0EXAMPLE2",
            "--thread",
            thread,
        ],
        {"reply": output, "thread": thread, "finalized": True},
    )
    report["invocations"][-1]["cwd"] = report["output_dir"]
    _, after_snapshot = _invocation(
        report,
        scope,
        ["--json", "local", "memory", scope["agent_id"]],
        {
            "agent": scope["agent_id"],
            "entries": [],
            "facts": facts,
        },
    )
    return _evidence(
        report,
        "retention-probe",
        {
            **scope,
            "fact_id": fact_id,
            "required_value": value,
            "input": "What is the reference value?",
            "output": output,
            "thread": thread,
            "channel": "C0EXAMPLE2",
            "teaching_channel": "C0EXAMPLE1",
            "before_snapshot_ref": snapshot,
            "snapshot_ref": after_snapshot,
            "invocation_id": invocation_id,
            "result_ref": result_ref,
            "binding_key": f"slack/C0EXAMPLE2/{thread}",
            "state_caller": "platform",
            "transcript": {"messages": [{"role": "assistant", "content": output}]},
            "retention_scorer": {
                "version": 1,
                "kind": "exact-value-and-active-fact",
                "required_value": value,
            },
            "retention": "PASS" if present and value in output else "FAIL",
            "no_recall": value not in output,
            "no_fabrication": not re.search(r"[a-z]+-[0-9a-f]{6}", output),
            "guidance_sha256": report["guidance_sha256"],
            "bundle_sha256": report["bundle_sha256"],
        },
    )


def _skill_row(item_id: str) -> dict[str, Any]:
    return {
        "id": item_id,
        "slug": "sample-procedure",
        "revision": 1,
        "description": "A saved procedure",
        "author": "U0EXAMPLE1",
        "updated_at": "2026-01-01T11:00:00Z",
    }


def _fact_row(item_id: str, statement: str) -> dict[str, Any]:
    return {
        "id": item_id,
        "statement": statement,
        "scope": "agent",
        "channel": None,
        "author": "U0EXAMPLE1",
        "stated_at": "2026-01-01T11:00:00Z",
    }


def _schedule_row(item_id: str, *, state: str = "terminal") -> dict[str, Any]:
    return {
        "id": item_id,
        "revision": 1,
        "state": state,
        "cron": None,
        "run_at": "2026-01-01T12:00:00Z",
        "timezone": "Etc/UTC",
        "prompt": f"Post token-{item_id}",
        "created_by": "U0EXAMPLE1",
    }


def _bind_judge(report: dict[str, Any], row: dict[str, Any], rubric: str) -> None:
    """Synthetic test data: bind one judge generation to a recorded decision.

    The prompt is the real one built by LearningJudgeScorer for the recorded evidence.
    """
    sequence = 101 + sum(item["purpose"] == "judge" for item in report["generations"])
    request = f"synthetic teaching request for {row['case_id']} {row['decision_id']}"
    meaning = f"synthetic expected meaning for {row['case_id']} {row['decision_id']}"
    evidence = {
        "output": "completed",
        "trajectory": [],
        "refs": {"request:teaching": request, "expected:meaning": meaning},
    }
    completion = json.dumps(
        {
            "verdict": "PASS",
            "action": row["expected"],
            "reason": "Supported.",
            "evidence_refs": ["request:teaching", "expected:meaning"],
        }
    )
    prompts: list[str] = []

    def complete(prompt: str) -> str:
        prompts.append(prompt)
        return completion

    spec = JudgeSpec(rubric=rubric, evidence=evidence, deterministic=ScoreResult(passed=True))
    case = EvalCase(
        id=row["case_id"], input=request, grader=Grader(kind=GraderKind.EXACT, expected="x")
    )
    LearningJudgeScorer({case.id: spec}, complete=complete).score(case, "completed", [])
    generation_id = f"gen-judge-{row['case_id']}-{row['decision_id']}"
    operation_id = f"judge-operation-{row['case_id']}-{row['decision_id']}"
    scope = {key: row[key] for key in ("repetition", "case_id", "agent_id")}
    report["generations"].append(
        {
            "id": generation_id,
            "model": JUDGE,
            "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            "cost_usd": "0.01",
            "purpose": "judge",
            "prompt_sha256": _sha(prompts[0].encode()),
            "call_sequence": sequence,
            "completion_sha256": _sha(completion.encode()),
            "ledger_operation_id": operation_id,
            **scope,
        }
    )
    report["accounting"]["operations"].append(
        {
            "id": operation_id,
            "generation_id": generation_id,
            "reserve_usd": "0.02",
            "settled_usd": "0.01",
            "status": "settled",
        }
    )
    row["judge_ref"] = {
        "generation_id": generation_id,
        "case_id": row["case_id"],
        "decision_id": row["decision_id"],
        "call_sequence": sequence,
        "prompt_sha256": _sha(prompts[0].encode()),
        "completion_sha256": _sha(completion.encode()),
        "completion": completion,
        "evidence": evidence,
    }


def _extra_decision(
    report: dict[str, Any], base: dict[str, Any], decision_id: str, kind: str, rubric: str
) -> None:
    """Synthetic test data: a further labeled decision of the same case (consent, correct)."""
    row = {
        **copy.deepcopy(base),
        "decision_id": decision_id,
        "expected": kind,
        "predicted": [kind],
        "distractor": False,
        "attempts": [],
    }
    row.pop("judge_ref", None)
    if kind == "nothing":
        row["mutations"] = []
        observation = copy.deepcopy(_value(report, base["evidence_ref"]))
        observation.update(after=copy.deepcopy(observation["before"]), semantic_action="nothing")
        row["evidence_ref"] = _evidence(report, "teaching-observation", observation)
    report["decisions"].append(row)
    _bind_judge(report, row, rubric)


def _fact_correction_snapshots(scope: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    fact_id = f"fact-correction-{scope['case_id']}"
    obsolete = _fact_row(fact_id, "cabinet-0a0a0a is violet-111111")
    corrected = {**obsolete, "statement": "cabinet-0a0a0a is violet-222222"}
    return (
        {"skills": [], "facts": [obsolete], "schedules": []},
        {"skills": [], "facts": [corrected], "schedules": []},
    )


def _fact_correction_predicates(scope: dict[str, Any]) -> list[dict[str, Any]]:
    target = f"fact-correction-{scope['case_id']}"
    return [
        {"name": "active_count", "class": "memory-fact", "target": target, "value": 1},
        {
            "name": "statement_equals",
            "class": "memory-fact",
            "target": target,
            "value": "cabinet-0a0a0a is violet-222222",
        },
        {"name": "scope_equals", "class": "memory-fact", "target": target, "value": "agent"},
    ]


def _control_check_for(control_id: str, scope: dict[str, Any]) -> dict[str, Any]:
    """Synthetic test data: one check that recomputes to PASS for the control."""
    if control_id == "fact-correction":
        # Shape chosen by the tests: fixture predicates replayed on before/after snapshots.
        before, after = _fact_correction_snapshots(scope)
        return {
            "kind": "predicates",
            "predicates": _fact_correction_predicates(scope),
            "before": before,
            "after": after,
        }
    return {"kind": "contains", "reply": "violet-a1b2c1", "expected": "violet"}


def _add_controls(report: dict[str, Any], repetition: int) -> None:
    for phrase in range(1, 4):
        case_id = f"F1-P{phrase}-R{repetition}"
        scope = {"repetition": repetition, "case_id": case_id, "agent_id": f"agent-{case_id}"}
        task = ("row-ordering", "unit-routing", "check-digit")[phrase - 1]
        skill_id = f"skill-{case_id}"
        deletion_id, _ = _invocation(
            report,
            scope,
            ["--json", "local", "skills", scope["agent_id"], "delete", skill_id],
            {"deleted": True, "id": skill_id},
        )
        for arm in ("learned", "removed"):
            probe_ids = []
            for trial in range(10):
                probe_ids.append(
                    _evidence(
                        report,
                        "procedure-probe",
                        {
                            **scope,
                            "task_id": task,
                            "arm": arm,
                            "skill_id": skill_id,
                            "deletion_invocation": deletion_id,
                            "pair_id": f"pair-{trial}",
                            "thread": f"1800000001.{len(report['evidence']):06d}",
                            "input": f"transform {arm} input {trial}",
                            "output": "correct" if arm == "learned" else "unknown",
                            "expected_output": "correct",
                            "success": arm == "learned",
                            "before": {
                                "skills": [_skill_row(skill_id)] if arm == "learned" else []
                            },
                            "after": {"skills": [_skill_row(skill_id)] if arm == "learned" else []},
                            "guidance_sha256": report["guidance_sha256"],
                            "bundle_sha256": report["bundle_sha256"],
                        },
                    )
                )
            report["procedure_probes"]["rows"].append(
                {
                    **scope,
                    "task_id": task,
                    "arm": arm,
                    "successes": 10 if arm == "learned" else 0,
                    "trials": 10,
                    "probe_ids": probe_ids,
                }
            )
        case_id = f"F3-P{phrase}-R{repetition}"
        scope = {"repetition": repetition, "case_id": case_id, "agent_id": f"agent-{case_id}"}
        fact_id = f"fact-{case_id}"
        value = f"violet-a1b2c{phrase}"
        before = _observation(report, scope, value=value, present=True, output=value)
        deletion_id, deletion_result = _invocation(
            report,
            scope,
            ["--json", "local", "memory", scope["agent_id"], "--delete", fact_id],
            {
                "deleted": True,
                "agent": scope["agent_id"],
                "id": fact_id,
                "scope": "agent",
                "channel": None,
            },
        )
        after = _observation(report, scope, value=value, present=False, output="I do not know.")
        deletion = next(row for row in report["invocations"] if row["invocation_id"] == deletion_id)
        report["memory_negative"]["rows"].append(
            {
                **scope,
                "fact_id": fact_id,
                "deletion_invocation": {
                    "invocation_id": deletion_id,
                    "argv": deletion["argv"],
                    "exit_code": 0,
                    "result_ref": deletion_result,
                },
                "retention_before": "PASS",
                "retention_after": "FAIL",
                "before_evidence": before,
                "after_evidence": after,
            }
        )
        case_id = f"F4-P{phrase}-R{repetition}"
        scope = {"repetition": repetition, "case_id": case_id, "agent_id": f"agent-{case_id}"}
        for due in (True, False):
            schedule_id = f"schedule-{case_id}-{'due' if due else 'cancelled'}"
            receipt = "2026-01-01T12:00:01Z"
            delivery_id = f"delivery-{schedule_id}"
            schedule_ref = _evidence(
                report,
                "schedule-observation",
                {
                    **scope,
                    "schedule_id": schedule_id,
                    "task_token": f"token-{schedule_id}",
                    "channel": "C0EXAMPLE1",
                    "zone": "Etc/UTC",
                    "kind": "one-shot",
                    "due_at": "2026-01-01T12:00:00Z",
                    "due": due,
                    "state": "terminal" if due else "cancelled",
                    "normal_poll_seconds": 30,
                    "observation_start": "2026-01-01T11:59:00Z",
                    "observation_end": "2026-01-01T12:02:00Z",
                    "tolerance_seconds": 120,
                    "delivery_route": {"channel": "C0EXAMPLE1", "worker_slack_api_port": 31002},
                    "collector_receipts": [
                        {
                            "id": delivery_id,
                            "channel": "C0EXAMPLE1",
                            "text": f"token-{schedule_id}",
                            "received_at": receipt,
                            "synthetic": False,
                            "updates": [],
                        }
                    ]
                    if due
                    else [],
                    "cli_snapshot": {
                        "agent_schedules": [
                            _schedule_row(schedule_id, state="terminal" if due else "cancelled")
                        ]
                    },
                },
            )
            report["deliveries"]["rows"].append(
                {
                    **scope,
                    "schedule_id": schedule_id,
                    "due": due,
                    "delivery_count": int(due),
                    "receipt_times": [receipt] if due else [],
                    "delivery_ids": [delivery_id] if due else [],
                    "schedule_evidence": schedule_ref,
                }
            )


@pytest.fixture
def candidate_binary() -> Path:
    """A required candidate binary is supplied by the implementation driver."""
    configured = os.environ.get("CURIE_LEARNING_EVAL_BIN")
    if configured:
        assert Path(configured).is_absolute(), "candidate CLI path must be absolute"
    candidates = (
        [Path(configured)]
        if configured
        else [ROOT / "cli/target/debug/curie", ROOT / "cli/target/release/curie"]
    )
    binary = next((path.resolve() for path in candidates if path.is_file()), None)
    assert binary is not None, (
        "build the candidate CLI or set CURIE_LEARNING_EVAL_BIN to its absolute path"
    )
    assert binary.is_absolute() and os.access(binary, os.X_OK)
    return binary


@pytest.fixture
def frozen_report(tmp_path: Path) -> dict[str, Any]:
    """Version-1 consistency vector. Generation identifiers are placeholders.

    This is serialized test data, not a captured campaign or paid-call proof.
    The outer report envelope uses explicit test assumptions where the plan
    fixes semantics without naming JSON keys. Auxiliary row fields are exact.
    """
    artifact = tmp_path / "synthetic-identity-artifact"
    version = "curie 0.0.0-synthetic-consistency-artifact"
    artifact_bytes = (
        "#!/bin/sh\n"
        'if [ "$#" = 1 ] && [ "$1" = --version ]; then\n'
        f"  printf '%s\\n' '{version}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 98\n"
    ).encode()
    artifact.write_bytes(artifact_bytes)
    artifact.chmod(0o755)
    binary = {"path": str(artifact), "sha256": _sha(artifact_bytes), "version": version}
    paths = [
        "cli/src/args/dev.rs",
        "cli/src/dispatch/dev_action.rs",
        "cli/scripts/learning-eval.sh",
        "apps/worker/src/curie_worker/eval/learning.py",
        "apps/worker/src/curie_worker/eval/learning_metrics.py",
        "apps/worker/src/curie_worker/eval/learning_judge.py",
        "compose.dev.yaml",
        "uv.lock",
        "cli/Cargo.lock",
        "runner/Dockerfile",
        "apps/worker/Dockerfile",
    ]
    paths += [str(path.relative_to(ROOT)) for path in FIXTURES.rglob("*") if path.is_file()]
    manifest = [
        {"path": path, "sha256": _sha((ROOT / path).read_bytes())} for path in sorted(paths)
    ]
    source_sha = _sha(_json_bytes(manifest))
    report: dict[str, Any] = {
        "report_version": 1,
        "repetitions": 1,
        "seed": "fixed-test-observation",
        "data_origin": "synthetic-consistency-vector",
        "implementation_verification": "PASS",
        "acceptance": "PASS",
        "exit_code": 0,
        # Agent-behavior failures, kept apart from harness integrity failures.
        "outcome_failures": [],
        "binary": binary,
        "source_root": str(ROOT),
        "source_tree_sha256": source_sha,
        "source_manifest": manifest,
        "build_association": {
            "candidate_commit": "a" * 40,
            "input_manifest_sha256": source_sha,
            "binary_sha256": binary["sha256"],
        },
        "fixtures": {
            name: {"path": str(FIXTURES / name), "sha256": _sha((FIXTURES / name).read_bytes())}
            for name in ("learning-cases.json", "learning-calibration.json")
        },
        "bundle_sha256": _sha(
            _json_bytes(
                [
                    {
                        "path": str(path.relative_to(FIXTURES / "learning-bundle")),
                        "sha256": _sha(path.read_bytes()),
                    }
                    for path in sorted((FIXTURES / "learning-bundle").rglob("*"))
                    if path.is_file()
                ]
            )
        ),
        "guidance_sha256": _sha(
            json.loads((FIXTURES / "learning-cases.json").read_bytes())["guidance"]["text"].encode()
        ),
        "output_dir": str(tmp_path),
        "thresholds": dict(THRESHOLDS),
        "custom_thresholds": False,
        "models": {"actor": ACTOR, "judge": JUDGE, "resolved_hostname": "openrouter.ai"},
        "capabilities": {
            kind: {"available": True, "evidence_ref": "discovery"} for kind in CLASSES[:4]
        },
        "evidence": {},
        "invocations": [],
        "cases": [],
        "decisions": [],
        "procedure_probes": {"version": 1, "rows": []},
        "memory_negative": {"version": 1, "rows": []},
        "deliveries": {"version": 1, "rows": []},
        "generations": [],
        # Synthetic test data: recorded private worker attempt cap and schedule reservations.
        "worker": {"max_attempts": 1},
        "controls": [],
        "schedule_reservations": [],
        "calibration": {"version": 1, "rows": [], "deterministic_controls": []},
        "accounting": {
            "budget_usd": "7.50",
            "baseline_usage_usd": "10.00",
            "end_usage_usd": "10.70",
            "charged_usd": "0.70",
            "unresolved_reservations_usd": "0.00",
            "shutdown_and_inflight_reserve_usd": "0.01",
            "operations": [],
            "basis": "conservative-shared-key-upper-bound",
        },
        "cleanup": {
            "verified": True,
            "problems": [],
            "ownership_manifest": {
                "project": "curie-check-example",
                "daemon_id": "example-daemon",
                "containers": [],
                "runners": [],
                "resolver_containers": [],
                "volumes": [],
                "networks": [],
                "processes": [],
            },
            "unrelated_before": [],
            "unrelated_after": [],
        },
        "collector": {
            "port_b": 31002,
            "bind_errors": 0,
            "recovered_bind_retries": 0,
            "delivery_windows": [
                {
                    "repetition": 1,
                    "opened_at": "2026-01-01T11:59:00Z",
                    "closed_at": "2026-01-01T12:02:30Z",
                    "message_children_alive": 0,
                }
            ],
            "synthetic_transport_receipt": {
                "synthetic": True,
                "received_at": "2026-01-01T11:58:00Z",
            },
        },
    }
    memory_schema = json.loads((ROOT / "cli/schema/memory.schema.json").read_bytes())
    memory_schema["$defs"]["fact"]["properties"].update(
        {
            "kind": {"type": "string", "enum": ["fact", "instruction"]},
            "archived": {"type": "boolean"},
        }
    )
    discovery = _evidence(
        report,
        "discovery",
        {
            "binary": binary,
            "source_tree_sha256": source_sha,
            "exit_code": 0,
            "memory_schema": memory_schema,
            "memory_schema_sha256": _sha(_json_bytes(memory_schema)),
            "skills_list": [],
            "agent_schedules": [],
            "overrides": {
                "memory_writes": True,
                "learned_skills": True,
                "agent_schedules": True,
                "agent_wide_learning": True,
            },
        },
    )
    for capability in report["capabilities"].values():
        capability["evidence_ref"] = discovery
    report["memory_schema"] = {
        "id": memory_schema["$id"],
        "sha256": _sha(_json_bytes(memory_schema)),
        "evidence_ref": discovery,
    }
    fixture_cases = {row["id"]: row for row in load_cases(FIXTURES / "learning-cases.json")}
    for index, case_id in enumerate(CASE_IDS, 1):
        family = int(case_id[1])
        scope = {"repetition": 1, "case_id": f"{case_id}-R1", "agent_id": f"agent-{case_id}-R1"}
        kind = EXPECTED[family - 1]
        invocation_id, result_ref = _invocation(
            report,
            scope,
            [
                "--json",
                "local",
                "message",
                f"primary input {case_id}",
                "--agent",
                scope["agent_id"],
                "--channel",
                "C0EXAMPLE1",
                "--thread",
                f"1800000000.{index:06d}",
            ],
            {"reply": "completed", "thread": f"1800000000.{index:06d}", "finalized": True},
        )
        report["invocations"][-1]["cwd"] = str(tmp_path)
        state = {"skills": [], "facts": [], "schedules": []}
        after = copy.deepcopy(state)
        mutations = []
        if kind in CLASSES[:4]:
            if kind == "skill":
                item = _skill_row(f"item-{case_id}")
            elif kind == "schedule":
                item = _schedule_row(f"item-{case_id}", state="active")
            else:
                item = _fact_row(f"item-{case_id}", "A synthetic retained value.")
                if kind == "instruction":
                    item.update(kind="instruction", archived=False)
            after[
                {
                    "skill": "skills",
                    "memory-fact": "facts",
                    "instruction": "facts",
                    "schedule": "schedules",
                }[kind]
            ].append(item)
            mutations = [
                {
                    "kind": kind,
                    "item_id": item["id"],
                    "operation": "create",
                    "before": None,
                    "after": item,
                }
            ]
        observation_ref = _evidence(
            report,
            "teaching-observation",
            {
                **scope,
                "before": state,
                "after": after,
                "transcript": {"messages": []},
                "output": "completed",
                "semantic_action": kind,
                "deterministic": True,
                "semantic": "PASS",
            },
        )
        report["cases"].append(
            {
                **scope,
                "fixture_id": case_id,
                "primary": True,
                "invocation_id": invocation_id,
                "reply_ref": result_ref,
                "transcript_ref": observation_ref,
                "actor_generation_id": f"gen-placeholder-actor-{index}",
                "stub_port": 31002,
                "stub_bind_succeeded": True,
            }
        )
        report["decisions"].append(
            {
                **scope,
                "fixture_id": case_id,
                "decision_id": fixture_cases[case_id]["decisions"][0]["id"],
                "expected": kind,
                "predicted": [kind],
                "status": "PASS",
                "mutations": mutations,
                "attempts": [],
                "distractor": family == 7,
                "correction_duplicates": 0,
                "evidence_ref": observation_ref,
                "detail": "serialized observation",
            }
        )
        report["generations"].append(
            {
                "id": f"gen-placeholder-actor-{index}",
                "model": ACTOR,
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
                "cost_usd": "0.01",
                "purpose": "actor",
                **scope,
            }
        )
        operation_id = f"actor-operation-{index}"
        report["generations"][-1]["ledger_operation_id"] = operation_id
        report["accounting"]["operations"].append(
            {
                "id": operation_id,
                "generation_id": f"gen-placeholder-actor-{index}",
                "invocation_id": invocation_id,
                "reserve_usd": "0.02",
                "settled_usd": "0.01",
                "status": "settled",
                # Synthetic test data: two equal usage readings at least 10 s apart.
                "usage_polls": [
                    {"polled_at": "2026-01-01T11:57:00Z", "usage_usd": "10.01"},
                    {"polled_at": "2026-01-01T11:57:12Z", "usage_usd": "10.01"},
                ],
            }
        )
        # Synthetic test data: one judge generation bound to each recorded semantic PASS.
        decision_cases = fixture_cases[case_id]["decisions"]
        base = report["decisions"][-1]
        _bind_judge(report, base, decision_cases[0]["rubric"])
        if family == 1:
            for decision_id, extra_kind in (
                ("consent-offer", "nothing"),
                ("consent-refusal", "nothing"),
                ("consent-approval", "skill"),
            ):
                _extra_decision(report, base, decision_id, extra_kind, decision_cases[0]["rubric"])
        if family == 8:
            _extra_decision(report, base, "correct", "skill", decision_cases[1]["rubric"])
        # Synthetic test data: fixture-derived control records with bound evidence.
        control_ids = {
            2: ("instruction-correction", "conflicting-request"),
            3: ("fact-correction",),
            5: ("revise-and-cancel",),
            8: ("revert",),
        }
        for control_id in control_ids.get(family, ()):
            report["controls"].append(
                {
                    **scope,
                    "fixture_id": case_id,
                    "control_id": control_id,
                    "status": "PASS",
                    "evidence_ref": _evidence(
                        report,
                        "control-observation",
                        {
                            **scope,
                            "control_id": control_id,
                            "status": "PASS",
                            # Synthetic test data: one check that recomputes to PASS.
                            "checks": [_control_check_for(control_id, scope)],
                        },
                    ),
                }
            )
        if kind == "schedule":
            report["schedule_reservations"].append(
                {
                    "repetition": 1,
                    "case_id": scope["case_id"],
                    "schedule_id": item["id"],
                    "disposition": "reserved",
                }
            )
    _add_controls(report, 1)
    # Wire shapes: https://openrouter.ai/docs/api/reference/overview and
    # https://openrouter.ai/docs/api/api-reference/generations/get-generation
    # These placeholders exercise parsing and bindings, not provider authenticity.
    for index, example in enumerate(load_calibration(FIXTURES / "learning-calibration.json"), 1):
        group = example["group"]
        prompt = json.dumps(
            {"rubric": example["rubric"], "evidence": example["evidence"]}, ensure_ascii=False
        )
        completion = json.dumps(
            {
                "verdict": example["expected"],
                "action": example["action"],
                "reason": "Supported by evidence.",
                "evidence_refs": list(example["evidence"]["refs"]),
            }
        )
        generation_id = f"gen-placeholder-judge-{index}"
        operation_id = f"calibration-operation-{index}"
        report["calibration"]["rows"].append(
            {
                "id": example["id"],
                "group": group,
                "expected": example["expected"],
                "verdict": example["expected"],
                "evidence": copy.deepcopy(example["evidence"]),
                "deterministic": copy.deepcopy(example["deterministic"]),
                "prompt": prompt,
                "prompt_sha256": _sha(prompt.encode()),
                "call_sequence": index,
                "completion": completion,
                "completion_sha256": _sha(completion.encode()),
                "generation_id": generation_id,
                "ledger_operation_id": operation_id,
                "model": JUDGE,
                "usage": {"prompt_tokens": 20, "completion_tokens": 20},
                "source": "provider",
            }
        )
        report["generations"].append(
            {
                "id": generation_id,
                "model": JUDGE,
                "usage": {"prompt_tokens": 20, "completion_tokens": 20},
                "cost_usd": "0.01",
                "purpose": "calibration",
                "prompt_sha256": _sha(prompt.encode()),
                "call_sequence": index,
                "completion_sha256": _sha(completion.encode()),
                "ledger_operation_id": operation_id,
            }
        )
        report["accounting"]["operations"].append(
            {
                "id": operation_id,
                "generation_id": generation_id,
                "reserve_usd": "0.02",
                "settled_usd": "0.01",
                "status": "settled",
            }
        )
    report["calibration"]["deterministic_controls"] = [
        {"id": group, "deterministic": "FAIL", "judge_called": False, "verdict": "FAIL"}
        for group in ("omitted", "obsolete", "duplicate", "fabricated")
    ]
    _derive(report)
    return report


def _summary(rows: list[dict[str, Any]], thresholds: dict[str, Any]) -> dict[str, Any]:
    """Independent expected arithmetic, never calls a product metric helper."""
    metrics = {}
    eligible = [row for row in rows if row["status"] != "NOT_AVAILABLE"]
    excluded = [row for row in rows if row["status"] == "NOT_AVAILABLE"]
    for kind in CLASSES:
        tp = sum(row["expected"] == kind and kind in row["predicted"] for row in eligible)
        fp = sum(row["expected"] != kind and kind in row["predicted"] for row in eligible)
        fn = sum(row["expected"] == kind and kind not in row["predicted"] for row in eligible)
        metrics[kind] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "support": tp + fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "errors": sum(row["expected"] == kind and row["status"] == "ERROR" for row in eligible),
            "unavailable": sum(row["expected"] == kind for row in excluded),
        }
    distractors = [row for row in eligible if row["distractor"]]
    falsewrites = sum(bool(row["mutations"]) for row in distractors)
    falsewrite_rate = falsewrites / len(distractors) if distractors else None
    duplicates = sum(
        row["correction_duplicates"] for row in eligible if row["decision_id"] == "correct"
    )
    gates = {}
    for name in ("precision", "recall"):
        values = [row[name] for row in metrics.values()]
        if excluded:
            gates[name] = {"status": "NOT_AVAILABLE", "value": None}
        elif any(value is None for value in values):
            gates[name] = {"status": "FAIL", "value": None}
        else:
            value = min(values)
            gates[name] = {
                "status": "PASS"
                if Decimal(str(value)) >= Decimal(str(thresholds[name]))
                else "FAIL",
                "value": value,
            }
    gates["falsewrite"] = {
        "status": "PASS"
        if falsewrite_rate is not None
        and Decimal(str(falsewrite_rate)) <= Decimal(str(thresholds["falsewrite"]))
        else "FAIL",
        "value": falsewrite_rate,
    }
    corrections = [
        row
        for row in eligible
        if row["fixture_id"].startswith("F8-") and row["decision_id"] == "correct"
    ]
    if not corrections:
        gates["correction_duplicates"] = {"status": "NOT_AVAILABLE", "value": None}
    else:
        gates["correction_duplicates"] = {
            "status": "PASS" if duplicates <= thresholds["correction_duplicates"] else "FAIL",
            "value": duplicates,
        }
    off_target: dict[str, dict[str, int]] = {}
    for row in excluded:
        for mutation in row["mutations"]:
            counts = off_target.setdefault(row["expected"], {})
            counts[mutation["kind"]] = counts.get(mutation["kind"], 0) + 1
    confusion: dict[str, dict[str, int]] = {}
    for row in eligible:
        counts = confusion.setdefault(row["expected"], {})
        for predicted in row["predicted"]:
            counts[predicted] = counts.get(predicted, 0) + 1
    return {
        "per_class": metrics,
        "gates": gates,
        "confusion": confusion,
        "distractor": {
            "evaluated": len(distractors),
            "falsewrite_count": falsewrites,
            "falsewrite_rate": falsewrite_rate,
            "attempted_write_rate": sum(bool(row["attempts"]) for row in distractors)
            / len(distractors)
            if distractors
            else None,
        },
        "unavailable_off_target_writes": off_target,
    }


def _derive(report: dict[str, Any]) -> None:
    report["metrics"] = _summary(report["decisions"], report["thresholds"])
    report["metrics"]["repetitions"] = [
        {
            "repetition": repetition,
            **_summary(
                [row for row in report["decisions"] if row["repetition"] == repetition],
                report["thresholds"],
            ),
        }
        for repetition in range(1, report["repetitions"] + 1)
    ]
    due = [row for row in report["deliveries"]["rows"] if row["due"]]
    once = sum(row["delivery_count"] == 1 for row in due)
    report["delivery_metrics"] = {
        "due_schedules": len(due),
        "delivered_exactly_once": once,
        "delivery_rate": once / len(due) if due else None,
    }
    _rehash(report)


def _check(
    tmp_path: Path,
    report: dict[str, Any],
    validator: Callable[[Path], ScoreResult] = verify_report,
    *,
    passed: bool,
) -> None:
    _score(validator(_write(tmp_path / "report.json", report)), passed)


def test_frozen_complete_report_is_a_valid_consistency_vector(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)


def _duplicate_repetition(report: dict[str, Any], repetition: int = 2) -> None:
    """Copy observations with fresh identities while retaining one accounting ledger."""
    original = copy.deepcopy(report)
    replacements = {
        key: f"r{repetition}-{key}"
        for key, record in original["evidence"].items()
        if record["value"].get("repetition") == 1
    }
    replacements.update(
        {
            row["invocation_id"]: f"r{repetition}-{row['invocation_id']}"
            for row in original["invocations"]
            if row["repetition"] == 1
        }
    )
    replacements.update(
        {
            row["id"]: f"r{repetition}-{row['id']}"
            for row in original["generations"]
            if row["purpose"] in ("actor", "judge") and row["repetition"] == 1
        }
    )
    actor_operations = {
        row["ledger_operation_id"]
        for row in original["generations"]
        if row["purpose"] in ("actor", "judge") and row["repetition"] == 1
    }
    replacements.update({key: f"r{repetition}-{key}" for key in actor_operations})

    def remap(value: Any) -> Any:
        if isinstance(value, str):
            if value in replacements:
                return replacements[value]
            value = value.replace("-R1", f"-R{repetition}")
            return re.sub(
                r"(?<![0-9])(180000000[01])\.([0-9]{6})(?![0-9])",
                lambda match: f"{int(match[1]) + 1000 * (repetition - 1)}.{match[2]}",
                value,
            )
        if isinstance(value, list):
            return [remap(item) for item in value]
        if isinstance(value, dict):
            return {
                key: repetition if key == "repetition" else remap(item)
                for key, item in value.items()
            }
        return value

    for ref in replacements:
        if ref in original["evidence"]:
            report["evidence"][replacements[ref]] = remap(original["evidence"][ref])
    for key in ("cases", "decisions", "invocations"):
        report[key].extend(remap([row for row in original[key] if row["repetition"] == 1]))
    report["generations"].extend(
        remap(
            [
                row
                for row in original["generations"]
                if row["purpose"] in ("actor", "judge") and row["repetition"] == 1
            ]
        )
    )
    report["accounting"]["operations"].extend(
        remap(
            [row for row in original["accounting"]["operations"] if row["id"] in actor_operations]
        )
    )
    report["controls"].extend(
        remap([row for row in original["controls"] if row["repetition"] == 1])
    )
    report["schedule_reservations"].extend(
        remap([row for row in original["schedule_reservations"] if row["repetition"] == 1])
    )
    for key in ("procedure_probes", "memory_negative", "deliveries"):
        report[key]["rows"].extend(
            remap([row for row in original[key]["rows"] if row["repetition"] == 1])
        )
    report["repetitions"] = repetition
    report["collector"]["delivery_windows"].append(
        {**report["collector"]["delivery_windows"][0], "repetition": repetition}
    )
    charge = Decimal("0.10") + Decimal("0.60") * repetition
    report["accounting"]["end_usage_usd"] = str(Decimal("10.00") + charge)
    report["accounting"]["charged_usd"] = str(charge)
    _derive(report)


def test_n_requires_all_24_instances_in_every_requested_repetition(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    _duplicate_repetition(report)
    _check(tmp_path, report, passed=True)
    report["cases"].pop()
    _check(tmp_path, report, passed=False)


def test_pooled_procedure_improvement_cannot_rescue_a_failed_repetition(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _duplicate_repetition(report)
    _check(tmp_path, report, passed=True)
    learned = next(
        row
        for row in report["procedure_probes"]["rows"]
        if row["repetition"] == 2 and row["arm"] == "learned"
    )
    learned["successes"] = 0
    for ref in learned["probe_ids"]:
        probe = _value(report, ref)
        probe.update(success=False, output="unknown")
    _derive(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("gate", ["precision", "recall", "falsewrite", "correction_duplicates"])
def test_report_recomputes_threshold_gate_values_instead_of_trusting_pass(
    tmp_path: Path, frozen_report: dict[str, Any], gate: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    report["metrics"]["gates"][gate]["value"] = -1
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("kind", ["precision", "recall"])
@pytest.mark.parametrize("threshold", [0.0, 0.9, 1.0])
def test_report_honors_inclusive_configured_classification_thresholds(
    tmp_path: Path, frozen_report: dict[str, Any], kind: str, threshold: float
) -> None:
    report = copy.deepcopy(frozen_report)
    report["thresholds"][kind] = threshold
    report["custom_thresholds"] = report["thresholds"] != THRESHOLDS
    _derive(report)
    _check(tmp_path, report, passed=True)


def test_duplicate_limit_is_inclusive_at_zero_and_at_custom_limit(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    correction = next(
        row
        for row in reversed(report["decisions"])
        if row["fixture_id"].startswith("F8-") and row["decision_id"] == "correct"
    )
    correction["correction_duplicates"] = 1
    # The F8 seed decision shares this evidence record; give the correction its own copy.
    observation = {**_value(report, correction["evidence_ref"]), "correction_duplicates": 1}
    correction["evidence_ref"] = _evidence(report, "teaching-observation", observation)
    _derive(report)
    _check(tmp_path, report, passed=False)
    report["thresholds"]["correction_duplicates"] = 1
    report["custom_thresholds"] = True
    _derive(report)
    _check(tmp_path, report, passed=True)


def test_pooled_exact_point_eight_recall_cannot_hide_a_failed_repetition(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    for repetition in range(2, 6):
        _duplicate_repetition(report, repetition)
    _check(tmp_path, report, passed=True)
    for row in report["decisions"]:
        if row["repetition"] == 1 and row["expected"] == "memory-fact":
            row.update(predicted=[], mutations=[], status="FAIL")
            observation = _value(report, row["evidence_ref"])
            observation["after"] = copy.deepcopy(observation["before"])
            observation["deterministic"] = False
    _derive(report)
    assert report["metrics"]["per_class"]["memory-fact"]["recall"] == 0.8
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "damage",
    ["changed-binary-bytes", "changed-version", "changed-source-digest", "missing-source-manifest"],
)
def test_identity_verification_rechecks_bytes_and_build_association(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    if damage == "changed-binary-bytes":
        binary = Path(report["binary"]["path"])
        binary.write_bytes(binary.read_bytes() + b"\n# changed artifact\n")
    elif damage == "changed-version":
        report["binary"]["version"] = "curie changed-version"
    elif damage == "changed-source-digest":
        report["source_tree_sha256"] = "0" * 64
    else:
        report.pop("source_manifest")
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "damage",
    [
        "zero-n",
        "boolean-n",
        "duplicate-decision",
        "wrong-instance-suffix",
        "cross-repetition-decision",
        "pooled-only-summary",
    ],
)
def test_report_rejects_invalid_repetition_and_decision_identity(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    if damage == "zero-n":
        report["repetitions"] = 0
    elif damage == "boolean-n":
        report["repetitions"] = True
    elif damage == "duplicate-decision":
        report["decisions"].append(copy.deepcopy(report["decisions"][0]))
    elif damage == "wrong-instance-suffix":
        report["decisions"][0]["case_id"] = "F1-P1"
    elif damage == "cross-repetition-decision":
        report["decisions"][0]["repetition"] = 2
    else:
        report["metrics"].pop("repetitions")
    _check(tmp_path, report, passed=False)


def _current_base(report: dict[str, Any]) -> None:
    absent = {"skill", "instruction", "schedule"}
    for kind in absent:
        ref = _evidence(
            report,
            "capability-absence",
            {
                "capability": kind,
                "exit_code": 0,
                "binary": report["binary"],
                "reason": "closed-memory-schema-omits-kind"
                if kind == "instruction"
                else "command-or-required-output-absent",
            },
        )
        report["capabilities"][kind] = {"available": False, "evidence_ref": ref}
    for row in report["decisions"]:
        if row["expected"] in absent:
            row.update(status="NOT_AVAILABLE", predicted=[], mutations=[])
            _value(report, row["evidence_ref"])["after"] = {
                "skills": [],
                "facts": [],
                "schedules": [],
            }
    report["decisions"] = [
        row
        for row in report["decisions"]
        if not (row["decision_id"].startswith("consent-") and row["expected"] == "nothing")
    ]
    report["schedule_reservations"] = []
    for section in ("procedure_probes", "deliveries"):
        report[section]["rows"] = []
    report["acceptance"] = "NOT_AVAILABLE"
    _derive(report)


def test_current_base_keeps_all_24_executions_and_null_global_gates(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _current_base(report)
    _check(tmp_path, report, passed=True)
    assert len(report["cases"]) == 24
    assert report["metrics"]["gates"]["precision"] == {"status": "NOT_AVAILABLE", "value": None}
    assert report["metrics"]["gates"]["recall"] == {"status": "NOT_AVAILABLE", "value": None}
    assert report["metrics"]["gates"]["correction_duplicates"] == {
        "status": "NOT_AVAILABLE",
        "value": None,
    }
    assert report["delivery_metrics"]["delivery_rate"] is None
    assert report["metrics"]["distractor"]["evaluated"] == 3
    report["acceptance"] = "PASS"
    _check(tmp_path, report, passed=False)


def test_correction_duplicates_pass_is_rejected_when_no_correction_row_was_evaluated(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _current_base(report)
    _check(tmp_path, report, passed=True)
    assert not any(
        row["fixture_id"].startswith("F8-") and row["status"] != "NOT_AVAILABLE"
        for row in report["decisions"]
    )
    report["metrics"]["gates"]["correction_duplicates"] = {"status": "PASS", "value": 0}
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("prediction", ["nothing", "clarify"])
def test_expected_unavailable_rows_cannot_acquire_no_write_predictions(
    tmp_path: Path, frozen_report: dict[str, Any], prediction: str
) -> None:
    report = copy.deepcopy(frozen_report)
    _current_base(report)
    _check(tmp_path, report, passed=True)
    report["decisions"][0]["predicted"] = [prediction]
    _derive(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "damage",
    [
        "dropped-actor-input",
        "lost-distractor",
        "store-error-as-unavailable",
        "fabricated-global-value",
        "empty-without-absence-proof",
    ],
)
def test_unavailability_does_not_swallow_executions_reads_or_denominators(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    report = copy.deepcopy(frozen_report)
    _current_base(report)
    _check(tmp_path, report, passed=True)
    if damage == "dropped-actor-input":
        report["cases"].pop(0)
    elif damage == "lost-distractor":
        report["decisions"][18]["status"] = "NOT_AVAILABLE"
        _derive(report)
    elif damage == "store-error-as-unavailable":
        report["capabilities"]["memory-fact"] = {"available": False, "reason": "CLI returned 1"}
    elif damage == "fabricated-global-value":
        report["metrics"]["gates"]["precision"]["value"] = 1.0
    else:
        report["capabilities"]["schedule"] = {"available": True}
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("section", ["procedure_probes", "memory_negative", "deliveries"])
@pytest.mark.parametrize(
    "damage",
    [
        "missing-section",
        "version",
        "boolean-version",
        "missing-row",
        "duplicate-row",
        "unknown-field",
        "missing-field",
        "boolean-repetition",
        "cross-agent",
        "cross-case",
        "null-reference",
    ],
)
def test_versioned_auxiliary_sections_fail_closed(
    tmp_path: Path, frozen_report: dict[str, Any], section: str, damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    rows = report[section]["rows"]
    if damage == "missing-section":
        del report[section]
    elif damage in {"version", "boolean-version"}:
        report[section]["version"] = 2 if damage == "version" else True
    elif damage == "missing-row":
        rows.pop(0)
    elif damage == "duplicate-row":
        rows.append(copy.deepcopy(rows[0]))
    elif damage == "unknown-field":
        rows[0]["unexpected"] = "ignored evidence"
    elif damage == "missing-field":
        del rows[0]["agent_id"]
    elif damage == "boolean-repetition":
        rows[0]["repetition"] = True
    elif damage == "cross-agent":
        rows[0]["agent_id"] = rows[-1]["agent_id"]
    elif damage == "cross-case":
        rows[0]["case_id"] = "F7-P1-R1"
    else:
        rows[0][
            {
                "procedure_probes": "probe_ids",
                "memory_negative": "before_evidence",
                "deliveries": "schedule_evidence",
            }[section]
        ] = None
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "damage",
    [
        "missing-case",
        "duplicate-case",
        "wrong-family",
        "missing-turn",
        "unfinalized-turn",
        "missing-usage",
        "wrong-model",
        "wrong-invocation-bin",
        "wrong-source",
        "missing-build",
        "wrong-build",
        "wrong-fixture-digest",
        "fabricated-metric",
        "missing-raw-evidence",
        "changed-raw-evidence",
        "missing-transcript",
    ],
)
def test_report_recomputes_inventory_identity_and_raw_evidence(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["cases"][0]
    if damage == "missing-case":
        report["cases"].pop()
    elif damage == "duplicate-case":
        report["cases"].append(copy.deepcopy(row))
    elif damage == "wrong-family":
        row["fixture_id"] = "F9-P1"
    elif damage == "missing-turn":
        row.pop("invocation_id")
    elif damage == "unfinalized-turn":
        _value(report, row["reply_ref"])["result"]["finalized"] = False
        _rehash(report)
    elif damage == "missing-usage":
        report["generations"][0].pop("usage")
    elif damage == "wrong-model":
        report["generations"][0]["model"] = JUDGE
    elif damage == "wrong-invocation-bin":
        report["invocations"][0]["argv"][0] = "/example/other-curie"
    elif damage == "wrong-source":
        report["invocations"][0]["source_tree_sha256"] = "0" * 64
    elif damage == "missing-build":
        report.pop("build_association")
    elif damage == "wrong-build":
        report["build_association"]["binary_sha256"] = "0" * 64
    elif damage == "wrong-fixture-digest":
        report["fixtures"]["learning-cases.json"]["sha256"] = "0" * 64
    elif damage == "fabricated-metric":
        report["metrics"]["per_class"]["skill"]["tp"] += 1
    elif damage == "missing-raw-evidence":
        del report["evidence"][row["reply_ref"]]
    elif damage == "changed-raw-evidence":
        _value(report, row["reply_ref"])["result"]["reply"] = "changed"
    else:
        _value(report, row["transcript_ref"]).pop("transcript")
        _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "successes,threshold,passed", [(5, 0.5, False), (6, 0.5, True), (0, 0.0, False), (1, 0.0, True)]
)
def test_procedure_gate_uses_exact_strict_per_task_boundary(
    tmp_path: Path, frozen_report: dict[str, Any], successes: int, threshold: float, passed: bool
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    report["thresholds"]["heldout_control_improvement"] = threshold
    report["custom_thresholds"] = threshold != 0
    learned = report["procedure_probes"]["rows"][0]
    learned["successes"] = successes
    for index, ref in enumerate(learned["probe_ids"]):
        probe = _value(report, ref)
        probe["success"] = index < successes
        probe["output"] = "correct" if probe["success"] else "unknown"
    _rehash(report)
    _check(tmp_path, report, passed=passed)


@pytest.mark.parametrize(
    "damage",
    [
        "fabricated-success",
        "boolean-trials",
        "unequal-trials",
        "duplicate-probe",
        "same-thread",
        "same-input",
        "wrong-item",
        "changed-guidance",
        "changed-bundle",
    ],
)
def test_procedure_control_requires_equivalent_fresh_bound_arms(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    learned, removed = report["procedure_probes"]["rows"][:2]
    positive = _value(report, learned["probe_ids"][0])
    negative = _value(report, removed["probe_ids"][0])
    if damage == "fabricated-success":
        learned["successes"] -= 1
    elif damage == "boolean-trials":
        learned["trials"] = True
    elif damage == "unequal-trials":
        removed["trials"] -= 1
        removed["probe_ids"].pop()
    elif damage == "duplicate-probe":
        removed["probe_ids"][1] = removed["probe_ids"][0]
    elif damage == "same-thread":
        negative["thread"] = positive["thread"]
    elif damage == "same-input":
        negative["input"] = positive["input"]
    elif damage == "wrong-item":
        negative["skill_id"] = "different-immutable-skill"
    else:
        negative["guidance_sha256" if damage == "changed-guidance" else "bundle_sha256"] = "0" * 64
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("before", ["PASS", "FAIL", "ERROR", "NOT_AVAILABLE"])
@pytest.mark.parametrize("after", ["PASS", "FAIL", "ERROR", "NOT_AVAILABLE"])
def test_memory_negative_requires_positive_to_failed_retention_flip(
    tmp_path: Path, frozen_report: dict[str, Any], before: str, after: str
) -> None:
    _check(tmp_path, frozen_report, verify_negative, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["memory_negative"]["rows"][0]
    row["retention_before"], row["retention_after"] = before, after
    _check(tmp_path, report, verify_negative, passed=(before, after) == ("PASS", "FAIL"))


@pytest.mark.parametrize("validator", [verify_negative, verify_report])
@pytest.mark.parametrize(
    "damage",
    [
        "wrong-fact",
        "wrong-agent",
        "wrong-delete-argv",
        "failed-delete",
        "missing-native-result",
        "fact-remains",
        "answer-in-prompt",
        "answer-recalled",
        "changed-rubric",
        "changed-guidance",
        "changed-bundle",
        "same-thread",
        "same-channel",
        "unsafe-fabrication",
        "boolean-exit",
    ],
)
def test_memory_negative_checks_native_deletion_and_unchanged_retention_scorer(
    tmp_path: Path,
    frozen_report: dict[str, Any],
    validator: Callable[[Path], ScoreResult],
    damage: str,
) -> None:
    _check(tmp_path, frozen_report, validator, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["memory_negative"]["rows"][0]
    before = _value(report, row["before_evidence"])
    after = _value(report, row["after_evidence"])
    if damage == "wrong-fact":
        row["fact_id"] = "different-fact"
    elif damage == "wrong-agent":
        row["agent_id"] = "different-agent"
    elif damage == "wrong-delete-argv":
        row["deletion_invocation"]["argv"][-1] = "different-fact"
    elif damage == "failed-delete":
        row["deletion_invocation"]["exit_code"] = 1
    elif damage == "missing-native-result":
        del report["evidence"][row["deletion_invocation"]["result_ref"]]
    elif damage == "fact-remains":
        _value(report, after["snapshot_ref"])["result"]["facts"] = copy.deepcopy(
            _value(report, before["snapshot_ref"])["result"]["facts"]
        )
    elif damage == "answer-in-prompt":
        after["input"] += " " + before["required_value"]
    elif damage == "answer-recalled":
        after["output"] = before["required_value"]
    elif damage == "changed-rubric":
        after["retention_scorer"]["required_value"] = "different-value"
    elif damage in {"changed-guidance", "changed-bundle"}:
        after["guidance_sha256" if damage == "changed-guidance" else "bundle_sha256"] = "0" * 64
    elif damage == "same-thread":
        after["thread"] = before["thread"]
    elif damage == "same-channel":
        after["channel"] = after["teaching_channel"]
    elif damage == "unsafe-fabrication":
        after["no_fabrication"] = False
    else:
        row["deletion_invocation"]["exit_code"] = False
    _rehash(report)
    _check(tmp_path, report, validator, passed=False)


@pytest.mark.parametrize("count,passed", [(0, False), (1, True), (2, False)])
def test_delivery_requires_exactly_one_final_post_per_due_schedule(
    tmp_path: Path, frozen_report: dict[str, Any], count: int, passed: bool
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["deliveries"]["rows"][0]
    observation = _value(report, row["schedule_evidence"])
    receipt = observation["collector_receipts"][0]
    observation["collector_receipts"] = [
        {**receipt, "id": f"post-{index}"} for index in range(count)
    ]
    row["delivery_count"] = count
    row["delivery_ids"] = [item["id"] for item in observation["collector_receipts"]]
    row["receipt_times"] = [item["received_at"] for item in observation["collector_receipts"]]
    _derive(report)
    _check(tmp_path, report, passed=passed)


@pytest.mark.parametrize(
    "damage",
    [
        "early",
        "late",
        "no-two-poll-window",
        "wrong-channel",
        "wrong-token",
        "synthetic",
        "duplicate-id",
        "unsorted-times",
        "invalid-time",
        "boolean-count",
        "nonterminal",
        "cancelled-delivered",
        "zero-denominator",
        "fabricated-rate",
        "wrong-delivery-route",
        "receipt-outside-window",
        "children-alive-window",
    ],
)
def test_delivery_is_recomputed_from_real_clock_state_and_collector_receipts(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["deliveries"]["rows"][0]
    observation = _value(report, row["schedule_evidence"])
    receipt = observation["collector_receipts"][0]
    if damage in {"early", "late", "invalid-time"}:
        time = {
            "early": "2026-01-01T11:59:59Z",
            "late": "2026-01-01T12:03:00Z",
            "invalid-time": "yesterday",
        }[damage]
        row["receipt_times"][0] = receipt["received_at"] = time
    elif damage == "no-two-poll-window":
        observation["observation_end"] = "2026-01-01T12:00:02Z"
    elif damage == "wrong-channel":
        receipt["channel"] = "C0EXAMPLE2"
    elif damage == "wrong-token":
        receipt["text"] = "unrelated token"
    elif damage == "synthetic":
        receipt["synthetic"] = True
    elif damage == "duplicate-id":
        row["delivery_count"] = 2
        row["delivery_ids"] *= 2
        row["receipt_times"] *= 2
        observation["collector_receipts"] *= 2
    elif damage == "unsorted-times":
        row["delivery_count"] = 2
        row["delivery_ids"] = ["first", "second"]
        row["receipt_times"] = ["2026-01-01T12:00:02Z", "2026-01-01T12:00:01Z"]
    elif damage == "boolean-count":
        row["delivery_count"] = True
    elif damage == "nonterminal":
        observation["state"] = "active"
    elif damage == "cancelled-delivered":
        row["due"] = observation["due"] = False
        observation["state"] = "cancelled"
    elif damage == "zero-denominator":
        report["deliveries"]["rows"] = []
    elif damage == "fabricated-rate":
        report["delivery_metrics"]["delivery_rate"] = 0.5
    elif damage == "wrong-delivery-route":
        observation["delivery_route"]["worker_slack_api_port"] = report["collector"]["port_b"] + 1
    elif damage == "receipt-outside-window":
        report["collector"]["delivery_windows"][0]["closed_at"] = "2026-01-01T12:00:00Z"
    else:
        report["collector"]["delivery_windows"][0]["message_children_alive"] = 1
    _rehash(report)
    _check(tmp_path, report, passed=False)


def test_placeholder_updates_do_not_increment_final_delivery_count(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    observation = _value(report, report["deliveries"]["rows"][0]["schedule_evidence"])
    observation["collector_receipts"][0]["updates"] = [
        {"received_at": "2026-01-01T12:00:02Z", "text": observation["task_token"]},
        {"received_at": "2026-01-01T12:00:03Z", "text": observation["task_token"]},
    ]
    _rehash(report)
    _check(tmp_path, report, passed=True)


@pytest.mark.parametrize(
    "damage",
    [
        "missing-group",
        "wrong-verdict",
        "missing-binding",
        "duplicate-binding",
        "conflicting-completion",
        "wrong-prompt-sha",
        "wrong-completion-sha",
        "missing-generation",
        "duplicate-generation",
        "wrong-model",
        "missing-usage",
        "missing-ledger",
        "synthetic",
        "deterministic-called-judge",
        "unclear",
        "malformed-completion",
    ],
)
def test_calibration_binds_all_five_groups_to_unique_provider_observations(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, verify_calibration, passed=True)
    report = copy.deepcopy(frozen_report)
    rows = report["calibration"]["rows"]
    row = rows[0]
    generation = next(item for item in report["generations"] if item["id"] == row["generation_id"])
    if damage == "missing-group":
        report["calibration"]["rows"] = [item for item in rows if item["group"] != "obsolete"]
    elif damage == "wrong-verdict":
        row["verdict"] = "FAIL"
    elif damage == "missing-binding":
        row.pop("call_sequence")
    elif damage == "duplicate-binding":
        rows[1]["prompt_sha256"] = row["prompt_sha256"]
        rows[1]["call_sequence"] = row["call_sequence"]
    elif damage == "conflicting-completion":
        generation["completion_sha256"] = "0" * 64
    elif damage in {"wrong-prompt-sha", "wrong-completion-sha"}:
        row["prompt_sha256" if damage == "wrong-prompt-sha" else "completion_sha256"] = "0" * 64
    elif damage == "missing-generation":
        report["generations"].remove(generation)
    elif damage == "duplicate-generation":
        report["generations"].append(copy.deepcopy(generation))
    elif damage == "wrong-model":
        generation["model"] = ACTOR
    elif damage == "missing-usage":
        generation.pop("usage")
    elif damage == "missing-ledger":
        report["accounting"]["operations"] = []
    elif damage == "synthetic":
        row["source"] = "synthetic"
    elif damage == "deterministic-called-judge":
        report["calibration"]["deterministic_controls"][0]["judge_called"] = True
    else:
        row["completion"] = (
            '{"verdict":"UNCLEAR","action":null,"reason":"uncertain","evidence_refs":[]}'
            if damage == "unclear"
            else "{"
        )
        row["completion_sha256"] = generation["completion_sha256"] = _sha(
            row["completion"].encode()
        )
    _check(tmp_path, report, verify_calibration, passed=False)


def test_identical_calibration_prompts_remain_distinct_by_sequence(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, verify_calibration, passed=True)
    report = copy.deepcopy(frozen_report)
    first, second = report["calibration"]["rows"][:2]
    second["prompt"] = first["prompt"]
    second["prompt_sha256"] = first["prompt_sha256"]
    generation = next(row for row in report["generations"] if row["id"] == second["generation_id"])
    generation["prompt_sha256"] = second["prompt_sha256"]
    assert first["call_sequence"] != second["call_sequence"]
    _check(tmp_path, report, verify_calibration, passed=True)


@pytest.mark.parametrize(
    "budget,passed",
    [("4.50", True), ("5.00", True), ("7.50", True), ("7.51", False)],
)
def test_report_campaign_budget_respects_task_cap(
    tmp_path: Path, frozen_report: dict[str, Any], budget: str, passed: bool
) -> None:
    report = copy.deepcopy(frozen_report)
    report["accounting"]["budget_usd"] = budget
    _check(tmp_path, report, passed=passed)


@pytest.mark.parametrize(
    "damage",
    [
        "unknown-cost",
        "negative-cost",
        "unsettled",
        "counter-reset",
        "over-budget",
        "repetition-budget-reset",
        "no-overshoot-margin",
        "catalog-cost-as-billed",
        "shared-spend-subtracted",
        "wrong-host",
    ],
)
def test_accounting_evidence_never_turns_unknown_or_shared_spend_into_free_calls(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    accounting = report["accounting"]
    if damage == "unknown-cost":
        report["generations"][0]["cost_usd"] = None
    elif damage == "negative-cost":
        report["generations"][0]["cost_usd"] = "-0.01"
    elif damage == "unsettled":
        accounting["unresolved_reservations_usd"] = "0.10"
    elif damage == "counter-reset":
        accounting["end_usage_usd"] = "9.00"
    elif damage == "over-budget":
        accounting["charged_usd"] = "7.51"
    elif damage == "repetition-budget-reset":
        accounting["budget_usd"] = "10.00"
    elif damage == "no-overshoot-margin":
        accounting["shutdown_and_inflight_reserve_usd"] = "0.00"
    elif damage == "catalog-cost-as-billed":
        accounting["basis"] = "catalog-price-estimate"
    elif damage == "shared-spend-subtracted":
        accounting["end_usage_usd"] = "11.00"
    else:
        report["models"]["resolved_hostname"] = "proxy.example.com"
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize(
    "damage", ["missing-manifest", "missing-daemon", "resolver-unowned", "cleanup-boolean-only"]
)
def test_cleanup_refuses_missing_ownership_before_any_absence_claim(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    report = copy.deepcopy(frozen_report)
    cleanup = report["cleanup"]
    if damage == "missing-manifest":
        cleanup.pop("ownership_manifest")
    elif damage == "missing-daemon":
        cleanup["ownership_manifest"].pop("daemon_id")
    elif damage == "resolver-unowned":
        cleanup["ownership_manifest"]["resolver_containers"] = [{"id": "resolver-example"}]
    else:
        report["cleanup"] = {"verified": True}
    _check(tmp_path, report, verify_cleanup, passed=False)


def test_cleanup_cannot_accept_recorded_success_when_daemon_is_unavailable(
    tmp_path: Path, frozen_report: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    tools = tmp_path / "unavailable-external-docker"
    tools.mkdir()
    marker = tmp_path / "docker-observed"
    docker = tools / "docker"
    docker.write_text(
        '#!/bin/sh\nprintf called >> "$LEARNING_TEST_DOCKER_CALL"\nexit 69\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", str(tools))
    monkeypatch.setenv("LEARNING_TEST_DOCKER_CALL", str(marker))
    _check(tmp_path, frozen_report, verify_cleanup, passed=False)
    assert marker.exists(), "cleanup must attempt the independent external observation"


LIVE_REPORT = os.environ.get("CURIE_LEARNING_EVAL_REPORT")


@pytest.mark.skipif(
    not LIVE_REPORT,
    reason="opt in by supplying one bounded campaign with CURIE_LEARNING_EVAL_REPORT",
)
class TestRecordedCampaign:
    """An opt-in selects live evidence; missing selected prerequisites are errors."""

    @pytest.fixture
    def campaign(self, candidate_binary: Path) -> tuple[Path, dict[str, Any], Path]:
        assert LIVE_REPORT is not None
        path = Path(LIVE_REPORT).resolve(strict=True)
        report = json.loads(path.read_bytes())
        assert report["binary"]["path"] == str(candidate_binary)
        _score(verify_report(path), True)
        return path, report, candidate_binary

    def test_all_four_public_validators_and_actual_cli_dispatch(
        self, campaign: tuple[Path, dict[str, Any], Path]
    ) -> None:
        path, _, binary = campaign
        for name, validator in (
            ("report", verify_report),
            ("calibration", verify_calibration),
            ("negative", verify_negative),
            ("cleanup", verify_cleanup),
        ):
            _score(validator(path), True)
            completed = subprocess.run(
                [str(binary), "--json", "dev", "learning-eval-verify", name, str(path)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert completed.returncode == 0, f"{name} validator failed"
            assert json.loads(completed.stdout)["passed"] is True

    def test_every_repetition_has_all_24_actual_primary_children_and_fresh_agents(
        self, campaign: tuple[Path, dict[str, Any], Path]
    ) -> None:
        _, report, _ = campaign
        identities = set()
        for repetition in range(1, report["repetitions"] + 1):
            rows = [
                row for row in report["cases"] if row["repetition"] == repetition and row["primary"]
            ]
            assert len(rows) == 24
            assert {row["fixture_id"] for row in rows} == set(CASE_IDS)
            for row in rows:
                assert row["agent_id"] not in identities
                identities.add(row["agent_id"])
                invocation = next(
                    item
                    for item in report["invocations"]
                    if item["invocation_id"] == row["invocation_id"]
                )
                assert invocation["cwd"] == report["output_dir"]
                assert invocation["argv"][0] == report["binary"]["path"]
                thread = invocation["argv"][invocation["argv"].index("--thread") + 1]
                assert re.fullmatch(r"[0-9]+\.[0-9]{6}", thread)
                assert isinstance(row["actor_record_id"], str) and row["actor_record_id"]
                generation = next(
                    item for item in report["generations"] if item["id"] == row["actor_record_id"]
                )
                assert generation["generation_id"] is None
                assert generation["identity_basis"] == "configured-route"
                assert generation["usage"]["source"] == "shared-key-delta"
                assert row["stub_port"] == report["collector"]["port_b"]
                assert row["stub_bind_succeeded"] is True

    def test_collector_liveness_and_synthetic_transport_are_separate_from_delivery(
        self, campaign: tuple[Path, dict[str, Any], Path]
    ) -> None:
        _, report, _ = campaign
        collector = report["collector"]
        assert "port_a" not in collector and "alive_for_primary_children" not in collector
        for row in report["cases"]:
            if row["primary"]:
                assert row["stub_port"] == collector["port_b"]
        assert collector["bind_errors"] == 0
        assert isinstance(collector["recovered_bind_retries"], int)
        assert collector["recovered_bind_retries"] >= 0
        assert collector["synthetic_transport_receipt"]["synthetic"] is True
        assert collector["synthetic_transport_receipt"]["received_at"]
        for repetition in range(1, report["repetitions"] + 1):
            windows = [
                window
                for window in collector["delivery_windows"]
                if window["repetition"] == repetition
            ]
            assert any(window["message_children_alive"] == 0 for window in windows)
        for row in report["deliveries"]["rows"]:
            observation = _value(report, row["schedule_evidence"])
            assert all(not receipt["synthetic"] for receipt in observation["collector_receipts"])

    def test_real_cli_lifecycle_uses_one_override_and_one_budget(
        self, campaign: tuple[Path, dict[str, Any], Path]
    ) -> None:
        _, report, _ = campaign
        invocations = report["invocations"]
        starts = [row for row in invocations if "local" in row["argv"] and "up" in row["argv"]]
        stops = [row for row in invocations if "local" in row["argv"] and "down" in row["argv"]]
        assert len(starts) == len(stops) == report["repetitions"]
        for start, stop in zip(starts, stops, strict=True):
            assert "--build" in start["argv"]
            assert start["argv"][start["argv"].index("--model") + 1] == ACTOR
            assert start["compose_files"] == stop["compose_files"]
            assert len(start["compose_files"]) == 2
            assert start["compose_project"] == stop["compose_project"]
            assert start["compose_disable_env_file"] is True
            assert stop["compose_disable_env_file"] is True
        assert Decimal(report["accounting"]["budget_usd"]) <= Decimal("7.50")
        assert report["accounting"]["basis"] != "catalog-price-estimate"

    def test_cleanup_queries_actual_daemon_and_rejects_mismatched_daemon(
        self, campaign: tuple[Path, dict[str, Any], Path], tmp_path: Path
    ) -> None:
        path, original, _ = campaign
        _score(verify_cleanup(path), True)
        report = copy.deepcopy(original)
        report["cleanup"]["ownership_manifest"]["daemon_id"] = "different-daemon"
        _check(tmp_path, report, verify_cleanup, passed=False)

    def test_owned_resources_are_actually_absent_and_removals_target_owned_ids(
        self, campaign: tuple[Path, dict[str, Any], Path]
    ) -> None:
        _, report, _ = campaign
        manifest = report["cleanup"]["ownership_manifest"]

        def docker(*args: str) -> set[str]:
            result = subprocess.run(
                ["docker", *args],
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0, "required caller-daemon observation failed"
            return set(result.stdout.splitlines())

        assert docker("info", "--format", "{{.ID}}") == {manifest["daemon_id"]}
        inventories = {
            "containers": docker("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}"),
            "volumes": docker("volume", "ls", "--format", "{{.Name}}"),
            "networks": docker("network", "ls", "--no-trunc", "--format", "{{.ID}}"),
        }
        assert manifest["resolver_containers"], "resolver preflight ownership must be retained"
        for kind in ("containers", "runners", "resolver_containers", "volumes", "networks"):
            inventory = inventories[
                "containers" if kind in {"runners", "resolver_containers"} else kind
            ]
            identity_key = "name" if kind == "volumes" else "id"
            assert all(row[identity_key] not in inventory for row in manifest[kind])
        for removal in manifest.get("removals", []):
            assert removal["kind"] in {
                "containers",
                "runners",
                "resolver_containers",
                "volumes",
                "networks",
                "images",
            }
            owned = {row.get("id", row.get("name")) for row in manifest.get(removal["kind"], [])}
            assert removal["id"] in owned, "removal targets a resource the manifest does not own"
        for process in manifest["processes"]:
            stat = Path(f"/proc/{process['pid']}/stat")
            if stat.exists():
                fields = stat.read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                assert fields[19] != str(process["start_time_ticks"]), (
                    "owned process is still running"
                )


def test_final_charge_below_the_recorded_campaign_usage_delta_fails(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    accounting = report["accounting"]
    delta = Decimal(accounting["end_usage_usd"]) - Decimal(accounting["baseline_usage_usd"])
    accounting["charged_usd"] = str(delta - Decimal("0.01"))
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("charge", ["0.00", "0.01", None])
def test_unresolved_operation_must_stay_charged_at_its_full_reservation(
    tmp_path: Path, frozen_report: dict[str, Any], charge: str | None
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    operation = report["accounting"]["operations"][0]
    operation.pop("settled_usd", None)
    operation.update(status="unresolved", reserve_usd="0.05", charged_usd=charge)
    _rehash(report)
    _check(tmp_path, report, passed=False)


def test_judged_semantic_pass_without_a_matching_judge_generation_binding_fails(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    missing = copy.deepcopy(frozen_report)
    del missing["decisions"][0]["judge_ref"]
    _rehash(missing)
    _check(tmp_path, missing, passed=False)
    mismatched = copy.deepcopy(frozen_report)
    mismatched["decisions"][0]["judge_ref"]["completion_sha256"] = _sha(b"another completion")
    _rehash(mismatched)
    _check(tmp_path, mismatched, passed=False)
    wrong_verdict = copy.deepcopy(frozen_report)
    ref = wrong_verdict["decisions"][0]["judge_ref"]
    ref["completion"] = json.dumps({"verdict": "FAIL", "action": "skill", "reason": "x"})
    ref["completion_sha256"] = _sha(ref["completion"].encode())
    for generation in wrong_verdict["generations"]:
        if generation["id"] == ref["generation_id"]:
            generation["completion_sha256"] = ref["completion_sha256"]
    _rehash(wrong_verdict)
    _check(tmp_path, wrong_verdict, passed=False)


def test_live_campaign_decision_with_predicates_unlike_the_fixture_derived_ones_fails(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    report["data_origin"] = "live-campaign"
    report["decisions"][0]["admitted_predicates"] = [
        {"name": "statement_equals", "class": "memory-fact", "target": "invented", "value": "x"}
    ]
    _rehash(report)
    _check(tmp_path, report, passed=False)


def test_cli_verify_report_rejects_a_report_whose_data_origin_is_not_live_campaign(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    assert frozen_report["data_origin"] != "live-campaign"
    path = _write(tmp_path / "report.json", frozen_report)
    assert _module_exit(["verify-report", str(path)]) != 0


@pytest.mark.parametrize("attempt", ["success", "error", "unmatched"])
def test_forbidden_persistence_attempt_cannot_be_recorded_as_a_passing_decision(
    tmp_path: Path, frozen_report: dict[str, Any], attempt: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    # The F7 distractor fixture forbids persistence attempts.
    row = next(item for item in report["decisions"] if item["fixture_id"].startswith("F7-"))
    assert row["status"] == "PASS"
    row["attempts"] = [{"kind": "memory-fact", "operation": "create", "outcome": attempt}]
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("attempts", [0, 2, 3, True, "1", None, "missing"])
def test_report_must_record_one_attempt_for_the_private_worker(
    tmp_path: Path, frozen_report: dict[str, Any], attempts: object
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    if attempts == "missing":
        del report["worker"]
    else:
        report["worker"]["max_attempts"] = attempts
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("damage", ["no-polls", "one-poll", "too-close", "unequal", "zero-usage"])
def test_actor_settlement_needs_a_stable_second_usage_poll(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    operation = next(
        row for row in report["accounting"]["operations"] if row["id"].startswith("actor-operation")
    )
    polls = operation["usage_polls"]
    if damage == "no-polls":
        del operation["usage_polls"]
    elif damage == "one-poll":
        del polls[1]
    elif damage == "too-close":
        polls[1]["polled_at"] = "2026-01-01T11:57:03Z"
    elif damage == "unequal":
        polls[1]["usage_usd"] = "10.02"
    else:
        polls[0]["usage_usd"] = polls[1]["usage_usd"] = "10.00"
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("damage", ["unreserved", "missing-row"])
def test_active_schedule_must_be_reserved_or_cancelled(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    assert report["schedule_reservations"]
    if damage == "unreserved":
        report["schedule_reservations"][0]["disposition"] = "unreserved"
    else:
        report["schedule_reservations"].pop(0)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("ref", ["request:teaching", "expected:meaning"])
def test_judge_evidence_without_the_request_or_expected_meaning_fails(
    tmp_path: Path, frozen_report: dict[str, Any], ref: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    del report["decisions"][0]["judge_ref"]["evidence"]["refs"][ref]
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("damage", ["other-case", "other-decision", "shared-generation"])
def test_judge_reference_must_bind_to_its_own_decision(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    first, second = report["decisions"][0], report["decisions"][-1]
    assert (first["case_id"], first["decision_id"]) != (second["case_id"], second["decision_id"])
    if damage == "other-case":
        first["judge_ref"]["case_id"] = second["case_id"]
    elif damage == "other-decision":
        first["judge_ref"]["decision_id"] = second["decision_id"]
    else:
        second["judge_ref"] = {
            **copy.deepcopy(first["judge_ref"]),
            "case_id": second["case_id"],
            "decision_id": second["decision_id"],
        }
    _check(tmp_path, report, passed=False)


def test_report_without_any_f8_correct_decision_fails_even_with_seed_rows(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    kept = [row for row in report["decisions"] if row["decision_id"] != "correct"]
    assert any(row["decision_id"] == "seed" for row in kept)
    assert len(kept) < len(report["decisions"])
    report["decisions"] = kept
    _derive(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("section", ["procedure_probes", "memory_negative", "deliveries"])
def test_report_missing_a_fixture_derived_control_fails(
    tmp_path: Path, frozen_report: dict[str, Any], section: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    report[section]["rows"].pop(0)
    _derive(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("validator", [verify_negative, verify_report])
def test_memory_negative_reply_with_another_nonce_shaped_value_is_a_fabrication(
    tmp_path: Path, frozen_report: dict[str, Any], validator: Callable[[Path], ScoreResult]
) -> None:
    _check(tmp_path, frozen_report, validator, passed=True)
    report = copy.deepcopy(frozen_report)
    row = report["memory_negative"]["rows"][0]
    after = _value(report, row["after_evidence"])
    required = _value(report, row["before_evidence"])["required_value"]
    assert re.fullmatch(r"violet-[0-9a-f]{6}", required)
    after["output"] = "I think it is violet-0f0f0f."
    after["no_recall"] = True
    after["no_fabrication"] = True
    _rehash(report)
    _check(tmp_path, report, validator, passed=False)


@pytest.mark.parametrize("decision_id", ["consent-offer", "consent-refusal", "consent-approval"])
def test_report_missing_an_f1_consent_decision_fails(
    tmp_path: Path, frozen_report: dict[str, Any], decision_id: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    before = len(report["decisions"])
    report["decisions"] = [
        row
        for row in report["decisions"]
        if not (row["decision_id"] == decision_id and row["fixture_id"] == "F1-P1")
    ]
    assert len(report["decisions"]) == before - 1
    _derive(report)
    _check(tmp_path, report, passed=False)


def _actor_operation(report: dict[str, Any]) -> dict[str, Any]:
    return next(
        row for row in report["accounting"]["operations"] if row["id"].startswith("actor-operation")
    )


@pytest.mark.parametrize("damage", ["nonzero-exit", "not-finalized"])
def test_unfinalized_turn_cannot_settle_below_its_reservation(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    operation = _actor_operation(report)
    invocation = next(
        row for row in report["invocations"] if row["invocation_id"] == operation["invocation_id"]
    )
    if damage == "nonzero-exit":
        invocation["exit_code"] = 1
    else:
        case = next(
            row for row in report["cases"] if row["invocation_id"] == invocation["invocation_id"]
        )
        _value(report, case["reply_ref"])["result"]["finalized"] = False
    _rehash(report)
    _check(tmp_path, report, passed=False)
    # Held unresolved at its full reservation, the accounting is honest: the report still
    # fails, but because the primary turn did not finalize, not because of settlement.
    operation.pop("usage_polls")
    operation.update(
        status="unresolved",
        settled_usd=operation["reserve_usd"],
        charged_usd=operation["reserve_usd"],
    )
    _rehash(report)
    result = verify_report(_write(tmp_path / "report.json", report))
    assert result.passed is False
    assert "settle" not in result.detail
    assert "finaliz" in result.detail or "turn failed" in result.detail


@pytest.mark.parametrize("fixture_id", ["F1-P1", "F6-P2", "F7-P3"])
def test_passing_durable_or_judged_decision_cannot_drop_semantic_or_judge_reference(
    tmp_path: Path, frozen_report: dict[str, Any], fixture_id: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    no_semantic = copy.deepcopy(frozen_report)
    row = next(item for item in no_semantic["decisions"] if item["fixture_id"] == fixture_id)
    del _value(no_semantic, row["evidence_ref"])["semantic"]
    _rehash(no_semantic)
    _check(tmp_path, no_semantic, passed=False)
    no_judge = copy.deepcopy(frozen_report)
    row = next(item for item in no_judge["decisions"] if item["fixture_id"] == fixture_id)
    del row["judge_ref"]
    _check(tmp_path, no_judge, passed=False)


@pytest.mark.parametrize(
    "control_id",
    [
        "instruction-correction",
        "conflicting-request",
        "fact-correction",
        "revise-and-cancel",
        "revert",
    ],
)
def test_report_missing_a_fixture_derived_control_record_fails(
    tmp_path: Path, frozen_report: dict[str, Any], control_id: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    before = len(report["controls"])
    report["controls"] = [row for row in report["controls"] if row["control_id"] != control_id]
    assert len(report["controls"]) < before
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("damage", ["no-ref", "dangling-ref"])
def test_control_claiming_pass_without_bound_evidence_fails(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    control = report["controls"][0]
    assert control["status"] == "PASS"
    if damage == "no-ref":
        del control["evidence_ref"]
    else:
        control["evidence_ref"] = "control-observation-missing"
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("damage", ["obsolete-and-corrected", "channel-scope-row"])
def test_fact_correction_control_replays_fixture_predicates_and_rejects_a_bad_after_state(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    _check(tmp_path, frozen_report, passed=True)
    report = copy.deepcopy(frozen_report)
    control = next(row for row in report["controls"] if row["control_id"] == "fact-correction")
    observation = _value(report, control["evidence_ref"])
    check = observation["checks"][0]
    assert check["kind"] == "predicates"
    row = check["after"]["facts"][0]
    if damage == "obsolete-and-corrected":
        row["statement"] = "cabinet-0a0a0a is violet-222222, was violet-111111"
    else:
        # The predicates target the fact by id, so the corrected row itself moves to
        # channel scope; scope_equals agent must reject it.
        row.update(scope="channel", channel={"kind": "slack", "address": "C0EXAMPLE1"})
    assert control["status"] == "PASS"
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("validator", [verify_calibration, verify_report])
@pytest.mark.parametrize("refs", ["empty", "unknown-key"])
def test_calibration_pass_needs_nonempty_refs_naming_keys_in_its_evidence(
    tmp_path: Path,
    frozen_report: dict[str, Any],
    validator: Callable[[Path], ScoreResult],
    refs: str,
) -> None:
    _check(tmp_path, frozen_report, validator, passed=True)
    report = copy.deepcopy(frozen_report)
    row = next(item for item in report["calibration"]["rows"] if item["verdict"] == "PASS")
    completion = json.loads(row["completion"])
    completion["evidence_refs"] = [] if refs == "empty" else ["state:not-in-this-example"]
    row["completion"] = json.dumps(completion)
    row["completion_sha256"] = _sha(row["completion"].encode())
    for generation in report["generations"]:
        if generation["id"] == row["generation_id"]:
            generation["completion_sha256"] = row["completion_sha256"]
    _check(tmp_path, report, validator, passed=False)


def test_teardown_records_a_runner_started_while_the_stack_stops(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A turn still queued in the worker can start a claim runner while compose
    stops the stack; `down` re-observes so the leftover sweep owns and removes it."""

    from curie_worker.eval import learning

    project = "curie-learning-abc-r1a1"
    started = {"runner": False}
    issued: list[tuple[str, ...]] = []

    def docker_lines(*args: str) -> list[str]:
        issued.append(args)
        joined = " ".join(args)
        if not started["runner"]:
            return []
        if "network=" + project + "_runner" in joined:
            return ["late-runner"]
        if args[:2] == ("volume", "ls") and f"curietech.ai/stack={project}_runner" in joined:
            return ["curie-input-late"]
        if args[:2] == ("network", "ls") and "com.docker.compose.project" in joined:
            return ["runner-network"]
        return []

    monkeypatch.setattr(learning, "_docker_lines", docker_lines)
    campaign = object.__new__(learning._Campaign)
    campaign.ownership = learning._Ownership(project_prefix="curie-learning-abc")
    campaign.stack = learning._Stack(
        project=project,
        tag="learning-abc",
        override=tmp_path / "override.yaml",
        ports={},
        stub_port=0,
        stub_host="127.0.0.1",
        repetition=1,
    )
    campaign.failures = []
    campaign.ledger = None

    def local_down(*_args: Any, **_kwargs: Any) -> tuple[int, str, str, str, str]:
        started["runner"] = True
        return 0, "", "", "", ""

    monkeypatch.setattr(campaign, "cli", local_down)
    monkeypatch.setattr(campaign, "compose_argv", lambda *args: list(args))
    monkeypatch.setattr(campaign, "compose_extra", lambda: {})
    campaign.down(1)

    assert campaign.ownership.runners == {"late-runner": project}
    assert campaign.ownership.volumes == {"curie-input-late": project}
    assert campaign.ownership.networks == {"runner-network": project}
    assert campaign.stack is None


def test_teardown_finalizes_when_the_post_down_observation_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    after_down = {"value": False}
    closed: list[str] = []

    def docker_lines(*args: str) -> list[str]:
        if after_down["value"]:
            raise subprocess.TimeoutExpired(["docker", *args], 240)
        return []

    monkeypatch.setattr(learning, "_docker_lines", docker_lines)
    campaign = object.__new__(learning._Campaign)
    campaign.ownership = learning._Ownership(project_prefix="curie-learning-abc")
    campaign.stack = learning._Stack(
        project="curie-learning-abc-r1a1",
        tag="learning-abc",
        override=tmp_path / "override.yaml",
        ports={},
        stub_port=0,
        stub_host="127.0.0.1",
        repetition=1,
    )
    campaign.failures = []
    campaign.ledger = object()

    def local_down(*_args: Any, **_kwargs: Any) -> tuple[int, str, str, str, str]:
        after_down["value"] = True
        return 0, "", "", "", ""

    monkeypatch.setattr(campaign, "cli", local_down)
    monkeypatch.setattr(campaign, "compose_argv", lambda *args: list(args))
    monkeypatch.setattr(campaign, "compose_extra", lambda: {})
    monkeypatch.setattr(campaign, "close_fires", closed.append)
    campaign.down(1)

    assert closed == ["stack torn down before the fire was observed"]
    assert campaign.stack is None
    assert any("after local down" in failure for failure in campaign.failures)
    # Partial discovery must also keep cleanup from being declared verified.
    assert campaign.ownership.incomplete
    monkeypatch.setattr(campaign, "close_window", lambda: None)
    monkeypatch.setattr(campaign.ownership, "remove_leftovers", lambda: [])
    monkeypatch.setattr(learning, "_check_cleanup_live", lambda _report: None)
    campaign.o = learning._Options(
        curie_bin=tmp_path / "curie",
        repetitions=1,
        budget=Decimal("1"),
        output_dir=tmp_path,
        thresholds={},
        dry_run=False,
        json=False,
    )
    cleanup = campaign.teardown()
    assert cleanup["verified"] is False
    assert any("after local down" in problem for problem in cleanup["problems"])


@pytest.mark.parametrize("damage", ["none", "unverified", "problems-but-verified", "no-problems"])
def test_cleanup_validator_fails_closed_on_recorded_incomplete_discovery(
    tmp_path: Path,
    frozen_report: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    """With every recorded resource absent, a teardown that could not observe
    everything it owned must still fail the cleanup validator."""

    from curie_worker.eval import learning

    monkeypatch.setattr(learning, "_check_cleanup_live", lambda _report: None)
    report = copy.deepcopy(frozen_report)
    cleanup = report["cleanup"]
    missed = "ownership not observed after local down: timed out"
    if damage == "unverified":
        cleanup["verified"] = False
        cleanup["problems"] = [missed]
    elif damage == "problems-but-verified":
        cleanup["problems"] = [missed]
    elif damage == "no-problems":
        cleanup.pop("problems")
    _check(tmp_path, report, verify_cleanup, passed=damage == "none")


def test_private_worker_claim_deadline_stays_below_its_lock_ttl(tmp_path: Path) -> None:
    from curie_worker.eval import learning

    stack = learning._Stack(
        project="curie-learning-abc-r1a1",
        tag="learning-abc",
        override=tmp_path / "override.yaml",
        ports={
            "postgres": {5432: 41001},
            "valkey": {6379: 41002},
            "rustfs": {9000: 41003},
            "otel-collector": {4318: 41004},
            "curie-api": {8000: 41005},
        },
        stub_port=41006,
        stub_host="127.0.0.1",
    )
    stack.write_override()
    text = stack.override.read_text(encoding="utf-8")
    worker = text.split("  curie-worker:", 1)[1].split("\nnetworks:", 1)[0]
    claim = re.search(r'CURIE_CLAIM_TIMEOUT_SECONDS: "(\d+)"', worker)
    lock = re.search(r'LOCK_TTL_MS: "(\d+)"', worker)
    assert claim is not None and lock is not None
    assert int(claim.group(1)) > 90
    assert int(claim.group(1)) * 1000 < int(lock.group(1))
    assert 'CURIE_MAX_ATTEMPTS: "1"' in worker


# Harness integrity versus agent outcome --------------------------------------------


def _record_verdict(
    report: dict[str, Any], acceptance: str, exit_code: int, verification: str = "PASS"
) -> None:
    report.update(
        acceptance=acceptance, exit_code=exit_code, implementation_verification=verification
    )


def _fail_memory_fact_decision(report: dict[str, Any]) -> dict[str, Any]:
    """The actor stored nothing for one F3 primary: a consistent deterministic FAIL."""
    row = next(
        row
        for row in report["decisions"]
        if row["fixture_id"] == "F3-P1" and row["expected"] == "memory-fact"
    )
    row.update(predicted=[], mutations=[], status="FAIL")
    observation = _value(report, row["evidence_ref"])
    observation["after"] = copy.deepcopy(observation["before"])
    observation["deterministic"] = False
    _derive(report)
    return row


@pytest.mark.parametrize(
    "acceptance,exit_code,verification,passed",
    [
        ("FAIL", 3, "PASS", True),
        ("PASS", 0, "PASS", False),
        ("FAIL", 0, "PASS", False),
        ("FAIL", 1, "PASS", False),
        ("FAIL", 1, "FAIL", False),
        ("NOT_AVAILABLE", 0, "PASS", False),
    ],
)
def test_failed_agent_decision_is_an_outcome_not_an_integrity_failure(
    tmp_path: Path,
    frozen_report: dict[str, Any],
    acceptance: str,
    exit_code: int,
    verification: str,
    passed: bool,
) -> None:
    """Consistent evidence of a misbehaving actor verifies; only a recorded verdict
    that disagrees with the recomputed acceptance fails the validator."""
    report = copy.deepcopy(frozen_report)
    _fail_memory_fact_decision(report)
    report["outcome_failures"] = ["F3-P1-R1 memory-fact decision is FAIL"]
    _record_verdict(report, acceptance, exit_code, verification)
    _check(tmp_path, report, passed=passed)


def test_failed_decision_recomputes_failed_acceptance_without_a_recorded_list(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _fail_memory_fact_decision(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


@pytest.mark.parametrize(
    "acceptance,exit_code,passed", [("PASS", 0, False), ("FAIL", 3, True), ("FAIL", 0, False)]
)
def test_recorded_outcome_failures_alone_decide_acceptance(
    tmp_path: Path, frozen_report: dict[str, Any], acceptance: str, exit_code: int, passed: bool
) -> None:
    """A turn that did not finalize or an off-target schedule leaves every decision
    passing; the recorded outcome failure list still forbids acceptance PASS."""
    report = copy.deepcopy(frozen_report)
    report["outcome_failures"] = ["F2-P1-R1 turn did not finalize: runner-error"]
    _record_verdict(report, acceptance, exit_code)
    _check(tmp_path, report, passed=passed)


@pytest.mark.parametrize(
    "acceptance,exit_code,passed",
    [("NOT_AVAILABLE", 0, True), ("FAIL", 3, False), ("PASS", 0, False)],
)
def test_incomplete_capabilities_keep_acceptance_not_available_despite_outcomes(
    tmp_path: Path, frozen_report: dict[str, Any], acceptance: str, exit_code: int, passed: bool
) -> None:
    report = copy.deepcopy(frozen_report)
    _current_base(report)
    _fail_memory_fact_decision(report)
    report["outcome_failures"] = ["F3-P1-R1 memory-fact decision is FAIL"]
    _record_verdict(report, acceptance, exit_code)
    _check(tmp_path, report, passed=passed)


# Memory negative: demonstrable rows ----------------------------------------------


def _rewrite_probe(report: dict[str, Any], ref: str, *, present: bool, output: str) -> None:
    """Re-record one retention probe consistently: snapshot, reply and derived flags."""
    probe = _value(report, ref)
    value = probe["required_value"]
    fact = _fact_row(probe["fact_id"], value)
    _value(report, probe["snapshot_ref"])["result"]["facts"] = [fact] if present else []
    _value(report, probe["result_ref"])["result"]["reply"] = output
    probe.update(
        output=output,
        transcript={"messages": [{"role": "assistant", "content": output}]},
        retention="PASS" if present and value in output else "FAIL",
        no_recall=value not in output,
        no_fabrication=_no_fabrication(probe["input"], value, output),
    )


def _no_fabrication(text: str, value: str, output: str) -> bool:
    """The reply neither states the value nor a nonce-shaped token unseen in the input."""
    unseen = [token for token in re.findall(r"[a-z]+-[0-9a-f]{6}", output) if token not in text]
    return value not in output and not unseen


def _not_stored(report: dict[str, Any], index: int) -> None:
    """The teach never stored the fact, so the deletion flip is not demonstrable."""
    row = report["memory_negative"]["rows"][index]
    _rewrite_probe(report, row["before_evidence"], present=False, output="I do not know.")
    row["retention_before"] = "FAIL"


def _recalled_after_delete(report: dict[str, Any], index: int) -> None:
    """The fact is gone from memory but the reply still recalls it: no safe flip."""
    row = report["memory_negative"]["rows"][index]
    required = _value(report, row["before_evidence"])["required_value"]
    _rewrite_probe(report, row["after_evidence"], present=False, output=required)
    row["retention_after"] = "FAIL"


@pytest.mark.parametrize(
    "rows,passed",
    [
        (("not-stored", "flipped", "flipped"), True),
        (("not-stored", "not-stored", "flipped"), True),
        (("not-stored", "recalled", "flipped"), False),
        (("flipped", "recalled", "flipped"), False),
        (("not-stored", "not-stored", "not-stored"), False),
    ],
)
def test_negative_validator_needs_one_demonstrable_flip_and_no_unsafe_one(
    tmp_path: Path, frozen_report: dict[str, Any], rows: tuple[str, ...], passed: bool
) -> None:
    _check(tmp_path, frozen_report, verify_negative, passed=True)
    report = copy.deepcopy(frozen_report)
    for index, state in enumerate(rows):
        if state == "not-stored":
            _not_stored(report, index)
        elif state == "recalled":
            _recalled_after_delete(report, index)
    _rehash(report)
    _check(tmp_path, report, verify_negative, passed=passed)


@pytest.mark.parametrize("damage", ["recalled", "not-stored"])
def test_every_memory_negative_row_still_feeds_acceptance(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    """verify-report keeps the strict every-row rule: any row that did not flip safely
    is an outcome failure, recorded as acceptance FAIL with exit code 3."""
    report = copy.deepcopy(frozen_report)
    (_recalled_after_delete if damage == "recalled" else _not_stored)(report, 0)
    _rehash(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


# Campaign report classification ------------------------------------------------------


def _bare_campaign(tmp_path: Path) -> Any:
    from curie_worker.eval import learning

    options = learning._Options(
        curie_bin=tmp_path / "curie",
        repetitions=1,
        budget=Decimal("1"),
        output_dir=tmp_path,
        thresholds=dict(THRESHOLDS),
        dry_run=False,
        json=False,
    )
    campaign = learning._Campaign(options, None)
    campaign.capabilities = {kind: {"available": True} for kind in CLASSES[:4]}
    return campaign


@pytest.mark.parametrize(
    "integrity,outcome,verification,acceptance,exit_code",
    [
        ([], [], "PASS", "PASS", 0),
        ([], ["F3-P1-R1 decision is FAIL"], "PASS", "FAIL", 3),
        (["final accounting: closed"], [], "FAIL", "FAIL", 1),
        (["final accounting: closed"], ["F3-P1-R1 decision is FAIL"], "FAIL", "FAIL", 1),
    ],
)
def test_campaign_report_separates_integrity_from_outcome_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    integrity: list[str],
    outcome: list[str],
    verification: str,
    acceptance: str,
    exit_code: int,
) -> None:
    from curie_worker.eval import learning

    # The self-check validates the written report; this test pins only the verdict.
    monkeypatch.setattr(
        learning, "verify_report", lambda _path: ScoreResult(passed=True, detail="stub")
    )
    campaign = _bare_campaign(tmp_path)
    campaign.failures = list(integrity)
    campaign.outcome_failures = list(outcome)
    report = campaign.report("a" * 40, {"verified": True, "problems": []})
    assert report["implementation_verification"] == verification
    assert report["acceptance"] == acceptance
    assert report["exit_code"] == exit_code
    assert report["outcome_failures"] == outcome
    written = json.loads((tmp_path / "report.json").read_bytes())
    assert written["outcome_failures"] == outcome
    assert written["exit_code"] == exit_code


def test_campaign_report_keeps_not_available_when_a_capability_is_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    monkeypatch.setattr(
        learning, "verify_report", lambda _path: ScoreResult(passed=True, detail="stub")
    )
    campaign = _bare_campaign(tmp_path)
    campaign.capabilities["skill"] = {"available": False}
    campaign.failures = []
    campaign.outcome_failures = ["F3-P1-R1 decision is FAIL"]
    report = campaign.report("a" * 40, {"verified": True, "problems": []})
    assert report["implementation_verification"] == "PASS"
    assert report["acceptance"] == "NOT_AVAILABLE"
    assert report["exit_code"] == 0
    assert report["outcome_failures"] == ["F3-P1-R1 decision is FAIL"]


# Terminal failed turn settlement ----------------------------------------------------


class _Clock:
    """Fake monotonic clock and UTC wall clock that advance only on sleep."""

    def __init__(self) -> None:
        self.seconds = 0.0

    def sleep(self, seconds: float) -> None:
        self.seconds += seconds

    def monotonic(self) -> float:
        return self.seconds

    def time(self) -> float:
        return 1_800_000_000 + self.seconds

    def now(self) -> str:
        moment = datetime(2026, 1, 1, 12, 0, tzinfo=UTC) + timedelta(seconds=self.seconds)
        return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class _ScriptedProvider:
    """Shared-key usage readings in order; the last reading repeats."""

    def __init__(self, clock: _Clock, readings: list[str]) -> None:
        self.clock = clock
        self.readings = [Decimal(value) for value in readings]
        self.calls: list[float] = []

    def usage(self) -> Decimal:
        self.calls.append(self.clock.seconds)
        if len(self.readings) > 1:
            return self.readings.pop(0)
        return self.readings[0]


def _clocked(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    from curie_worker.eval import learning

    clock = _Clock()
    monkeypatch.setattr(
        learning,
        "time",
        types.SimpleNamespace(sleep=clock.sleep, monotonic=clock.monotonic, time=clock.time),
    )
    monkeypatch.setattr(learning, "_now", clock.now)
    return clock


def _ledger(tmp_path: Path, provider: _ScriptedProvider) -> tuple[Any, str]:
    from curie_worker.eval import learning

    ledger = learning._Ledger(tmp_path / "ledger.sqlite", Decimal("7.50"), provider)
    scope = {"repetition": 1, "case_id": "F2-P1-R1", "agent_id": "agent-F2-P1-R1"}
    return ledger, ledger.admit("actor", Decimal("0.50"), scope)


def _operation(ledger: Any, identity: str) -> dict[str, Any]:
    return next(row for row in ledger.operations() if row["id"] == identity)


def test_terminal_turn_with_a_running_runner_stays_unresolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clock = _clocked(monkeypatch)
    provider = _ScriptedProvider(clock, ["10.00"])
    ledger, identity = _ledger(tmp_path, provider)
    ledger.settle_terminal(identity, Decimal("10.00"), record=identity, runner_absent=lambda: False)
    operation = _operation(ledger, identity)
    assert operation["status"] == "unresolved"
    assert Decimal(operation["settled_usd"]) == Decimal("0.50")
    assert "runner_absent_at" not in operation


@pytest.mark.parametrize(
    "readings,expected",
    [
        (["10.00"], Decimal("0")),
        (["10.00", "10.00", "10.05", "10.07", "10.07"], Decimal("0.07")),
    ],
)
def test_terminal_turn_settles_observed_usage_once_the_runner_is_gone(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, readings: list[str], expected: Decimal
) -> None:
    from curie_worker.eval import learning

    clock = _clocked(monkeypatch)
    provider = _ScriptedProvider(clock, readings)
    ledger, identity = _ledger(tmp_path, provider)
    observed: list[str] = []

    def runner_absent() -> bool:
        observed.append(clock.now())
        return True

    settled = ledger.settle_terminal(
        identity, Decimal("10.00"), record=identity, runner_absent=runner_absent
    )
    operation = _operation(ledger, identity)
    assert operation["status"] == "settled"
    assert Decimal(operation["settled_usd"]) == expected
    assert Decimal(str(settled)) == expected
    assert operation["record_id"] == identity
    assert len(observed) >= 1
    absent_at = operation["runner_absent_at"]
    assert isinstance(absent_at, str) and absent_at.endswith("Z")
    moment = learning._utc(absent_at, "runner absent")
    polls = operation["usage_polls"]
    assert len(polls) >= 2
    assert all(learning._utc(poll["polled_at"], "poll") >= moment for poll in polls)
    last, previous = polls[-1], polls[-2]
    assert Decimal(last["usage_usd"]) == Decimal(previous["usage_usd"])
    gap = learning._utc(last["polled_at"], "poll") - learning._utc(previous["polled_at"], "poll")
    assert gap.total_seconds() >= learning.STABLE_POLL_GAP_SECONDS
    assert Decimal(last["usage_usd"]) - Decimal("10.00") == expected


def test_terminal_turn_whose_usage_never_stabilizes_stays_unresolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clock = _clocked(monkeypatch)
    # Every reading differs until long after STABLE_SETTLE_SECONDS.
    provider = _ScriptedProvider(clock, ["10.00", "10.00", *(f"10.{n:02d}" for n in range(1, 99))])
    ledger, identity = _ledger(tmp_path, provider)
    ledger.settle_terminal(identity, Decimal("10.00"), record=identity, runner_absent=lambda: True)
    operation = _operation(ledger, identity)
    assert operation["status"] == "unresolved"
    assert Decimal(operation["settled_usd"]) == Decimal("0.50")


# Accounting validator: terminal settled actor operations ------------------------------


def _terminal_operation(report: dict[str, Any]) -> dict[str, Any]:
    """A further actor turn the worker delivered as a terminal runner-error, settled
    after the runner was observed absent: synthetic test data."""
    scope = {"repetition": 1, "case_id": "F2-P1-R1", "agent_id": "agent-F2-P1-R1"}
    invocation_id, _ = _invocation(
        report,
        scope,
        [
            "--json",
            "local",
            "message",
            "a follow-up probe",
            "--agent",
            scope["agent_id"],
            "--channel",
            "C0EXAMPLE2",
            "--thread",
            "1800000009.000001",
        ],
        {
            "reply": "The run failed.",
            "thread": "1800000009.000001",
            "finalized": False,
            "failed": True,
            "failure_class": "runner-error",
        },
    )
    report["invocations"][-1]["exit_code"] = 1
    operation = {
        "id": "actor-operation-900",
        "generation_id": None,
        "record_id": "actor-operation-900",
        "invocation_id": invocation_id,
        "finalized": False,
        "terminal_failure_class": "runner-error",
        "runner_absent_at": "2026-01-01T11:58:00.000000Z",
        "reserve_usd": "0.50",
        "settled_usd": "0.01",
        "charged_usd": "0.01",
        "status": "settled",
        "usage_polls": [
            {"polled_at": "2026-01-01T11:58:01.000000Z", "usage_usd": "10.01"},
            {"polled_at": "2026-01-01T11:58:13.000000Z", "usage_usd": "10.01"},
        ],
    }
    report["accounting"]["operations"].append(operation)
    charged = Decimal(report["accounting"]["charged_usd"]) + Decimal(operation["settled_usd"])
    report["accounting"]["charged_usd"] = str(charged)
    _rehash(report)
    return operation


@pytest.mark.parametrize(
    "damage",
    [
        "zero-charge",
        "no-class",
        "blank-class",
        "other-class",
        "no-absence",
        "poll-before-absence",
        "above-observed",
        "negative",
        "one-poll",
        "unequal",
        "too-close",
        "timed-out-turn",
    ],
)
def test_settled_terminal_turn_needs_runner_absence_and_stable_polls(
    tmp_path: Path, frozen_report: dict[str, Any], damage: str
) -> None:
    report = copy.deepcopy(frozen_report)
    operation = _terminal_operation(report)
    _check(tmp_path, report, passed=True)
    polls = operation["usage_polls"]
    passed = False
    if damage == "zero-charge":
        # Usage never moved: a zero charge is allowed once the runner is gone.
        operation.update(settled_usd="0", charged_usd="0")
        polls[0]["usage_usd"] = polls[1]["usage_usd"] = "10.00"
        passed = True
    elif damage == "no-class":
        del operation["terminal_failure_class"]
    elif damage == "blank-class":
        operation["terminal_failure_class"] = ""
        result = _value(report, report["invocations"][-1]["result_ref"])["result"]
        result["failure_class"] = ""
    elif damage == "other-class":
        operation["terminal_failure_class"] = "max-turns"
    elif damage == "no-absence":
        del operation["runner_absent_at"]
    elif damage == "poll-before-absence":
        polls[0]["polled_at"] = "2026-01-01T11:57:59.000000Z"
        polls[1]["polled_at"] = "2026-01-01T11:58:11.000000Z"
    elif damage == "above-observed":
        operation.update(settled_usd="0.02", charged_usd="0.02")
    elif damage == "negative":
        operation.update(settled_usd="-0.01", charged_usd="-0.01")
    elif damage == "one-poll":
        del polls[0]
    elif damage == "unequal":
        polls[1]["usage_usd"] = "10.02"
    elif damage == "too-close":
        polls[1]["polled_at"] = "2026-01-01T11:58:05.000000Z"
    else:
        # A timeout carries no failure class: nothing proves the turn stopped.
        result = _value(report, report["invocations"][-1]["result_ref"])["result"]
        result.pop("failure_class")
        result.update(reply=None, failed=False, timed_out=True)
    _rehash(report)
    _check(tmp_path, report, passed=passed)


# Liveness: how message() settles each kind of turn -----------------------------------


@pytest.mark.parametrize("kind", ["finalized", "timed-out", "runner-error"])
def test_message_settles_by_how_the_turn_ended(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str
) -> None:
    from curie_worker.eval import learning

    clock = _clocked(monkeypatch)
    monkeypatch.setattr(learning, "_docker_lines", lambda *_args: [])
    campaign = _bare_campaign(tmp_path)
    campaign.capabilities["schedule"] = {"available": False}
    campaign.stack = learning._Stack(
        project="curie-learning-abc-r1a1",
        tag="learning-abc",
        override=tmp_path / "override.yaml",
        ports={},
        stub_port=0,
        stub_host="127.0.0.1",
        repetition=1,
    )
    provider = _ScriptedProvider(clock, ["10.00"])
    campaign.ledger = learning._Ledger(tmp_path / "ledger.sqlite", Decimal("7.50"), provider)
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def recorder(name: str) -> Callable[..., str]:
        def record(*args: Any, **kwargs: Any) -> str:
            calls.append((name, args, kwargs))
            return "0"

        return record

    monkeypatch.setattr(campaign.ledger, "settle_stable", recorder("settle_stable"))
    # raising=False: before the terminal path exists, the finalized case must still pass.
    monkeypatch.setattr(
        campaign.ledger, "settle_terminal", recorder("settle_terminal"), raising=False
    )
    monkeypatch.setattr(campaign, "wait_for_stub_port", lambda: None)
    monkeypatch.setattr(campaign, "actor_reserve", lambda: Decimal("0.50"))
    monkeypatch.setattr(campaign, "sweep_schedules", lambda _scope: None)
    thread = "1800000000.000001"
    code, result = {
        "finalized": (0, {"reply": "done", "thread": thread, "finalized": True}),
        "timed-out": (75, {"reply": None, "finalized": False, "timed_out": True}),
        "runner-error": (
            1,
            {
                "reply": "The run failed.",
                "thread": thread,
                "finalized": False,
                "failed": True,
                "failure_class": "runner-error",
            },
        ),
    }[kind]
    monkeypatch.setattr(
        campaign, "cli", lambda *_args, **_kwargs: (code, result, "invocation-1", "ref-1", "")
    )
    absence: list[tuple[str, datetime]] = []

    def runner_absent(agent_id: str, since: datetime) -> bool:
        absence.append((agent_id, since))
        return True

    monkeypatch.setattr(campaign, "runner_absent", runner_absent)
    scope = {"repetition": 1, "case_id": "F2-P1-R1", "agent_id": "agent-F2-P1-R1"}
    started = datetime.now(UTC)
    campaign.message(scope, "teach this", "C0EXAMPLE1", thread)
    ended = datetime.now(UTC)

    operation = campaign.ledger.operations()[0]
    names = [name for name, _, _ in calls]
    if kind == "finalized":
        assert names == ["settle_stable"]
        assert campaign.failures == [] and getattr(campaign, "outcome_failures", []) == []
        assert operation["finalized"] is True
        return
    # A turn that did not finalize is the agent's outcome, never a harness failure.
    assert campaign.failures == []
    assert len(campaign.outcome_failures) == 1
    assert "did not finalize" in campaign.outcome_failures[0]
    if kind == "timed-out":
        assert names == []
        assert operation["status"] == "unresolved"
        assert Decimal(operation["settled_usd"]) == Decimal("0.50")
        assert "terminal_failure_class" not in operation
        return
    assert names == ["settle_terminal"]
    _, args, kwargs = calls[0]
    assert args[0] == operation["id"]
    assert Decimal(str(args[1])) == Decimal("10.00")
    assert kwargs["record"] == operation["id"]
    # Absence is scoped to this agent's runners created since this attempt began.
    assert kwargs["runner_absent"]() is True
    assert len(absence) == 1
    agent_id, since = absence[0]
    assert agent_id == scope["agent_id"]
    assert since.tzinfo is not None and since.utcoffset() == timedelta(0)
    assert started <= since <= ended
    assert operation["terminal_failure_class"] == "runner-error"
    assert operation["finalized"] is False


# Delivery collector faults stay integrity ---------------------------------------------


def _deliver(report: dict[str, Any], index: int, count: int) -> None:
    """Re-record one delivery row with ``count`` receipts inside the quiet window."""
    row = report["deliveries"]["rows"][index]
    observation = _value(report, row["schedule_evidence"])
    receipts = [
        {
            "id": f"post-{row['schedule_id']}-{n}",
            "channel": observation["channel"],
            "text": observation["task_token"],
            "received_at": "2026-01-01T12:00:01Z",
            "synthetic": False,
            "updates": [],
        }
        for n in range(count)
    ]
    observation["collector_receipts"] = receipts
    row["delivery_count"] = count
    row["delivery_ids"] = [item["id"] for item in receipts]
    row["receipt_times"] = [item["received_at"] for item in receipts]


@pytest.mark.parametrize("fault", ["no-window", "receipt-outside-window", "children-alive-window"])
@pytest.mark.parametrize("acceptance,exit_code", [("FAIL", 3), ("PASS", 0)])
def test_delivery_collector_faults_fail_integrity_under_any_recorded_verdict(
    tmp_path: Path, frozen_report: dict[str, Any], fault: str, acceptance: str, exit_code: int
) -> None:
    """A due schedule with no collector window, or a receipt the collector took outside
    every quiet window, is a fault in the collection evidence, not an agent outcome:
    no recorded acceptance verdict makes such a report verify."""
    report = copy.deepcopy(frozen_report)
    windows = report["collector"]["delivery_windows"]
    if fault == "no-window":
        windows.clear()
    elif fault == "receipt-outside-window":
        windows[0]["closed_at"] = "2026-01-01T12:00:00Z"
    else:
        windows[0]["message_children_alive"] = 1
    report["outcome_failures"] = [f"F4 delivery evidence: {fault}"]
    _record_verdict(report, acceptance, exit_code)
    _rehash(report)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("outcome", ["undelivered", "duplicated", "delivered-after-cancel"])
def test_delivery_count_outcomes_stay_acceptance_failures(
    tmp_path: Path, frozen_report: dict[str, Any], outcome: str
) -> None:
    """How many times the agent's schedule delivered is its outcome: consistent evidence
    verifies when it records acceptance FAIL with exit code 3, and only then."""
    report = copy.deepcopy(frozen_report)
    if outcome == "undelivered":
        _deliver(report, 0, 0)
    elif outcome == "duplicated":
        _deliver(report, 0, 2)
    else:
        cancelled = next(
            index for index, row in enumerate(report["deliveries"]["rows"]) if not row["due"]
        )
        _deliver(report, cancelled, 1)
    _derive(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


# A failed turn leaves no transcript: an outcome, not an integrity failure -------------


def _teaching_campaign(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, finalized: bool
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """A campaign whose message turn ends as requested and whose runner persisted no
    transcript for it (runner session persistence skips failed turns)."""
    from curie_worker.eval import learning

    campaign = _bare_campaign(tmp_path)
    # No schedule reservation path: the fixture under test teaches a fact.
    campaign.capabilities["schedule"] = {"available": False}
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "snapshot", lambda _scope: (copy.deepcopy(empty), None))
    thread = "1800000000.000001"
    sent = {
        "invocation_id": "invocation-1",
        "result_ref": "cli-result-1",
        "exit_code": 0 if finalized else 1,
        "reply": "done" if finalized else "The run failed.",
        "thread": thread,
        "channel": campaign.channels[1],
        "finalized": finalized,
        "generation": "operation-1",
    }

    def message(scope: Any, text: str, channel: str, thread: str) -> dict[str, Any]:
        return {**sent, "channel": channel, "thread": thread}

    def transcript(*_args: Any, **_kwargs: Any) -> Any:
        raise learning._Stop(f"no transcript for thread {thread} (status 404)")

    monkeypatch.setattr(campaign, "message", message)
    monkeypatch.setattr(campaign, "transcript", transcript)
    monkeypatch.setattr(
        campaign,
        "cli",
        lambda *_args, **_kwargs: (0, {"facts": []}, "invocation-m", "cli-result-m", ""),
    )
    fixture = next(row for row in campaign.corpus if row["id"] == "F3-P1")
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    return campaign, dict(fixture), scope


def test_teach_failed_turn_without_a_transcript_is_an_outcome_not_integrity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    campaign, fixture, scope = _teaching_campaign(monkeypatch, tmp_path, finalized=False)
    campaign.teach(scope, fixture, scope["agent_id"], "1800000000.000001", 0)
    assert campaign.failures == []
    scored = [d for d in campaign.decisions if d["fixture_id"] == "F3-P1"]
    assert scored and all(d["status"] == "ERROR" for d in scored)
    assert campaign.outcome_failures


def test_teach_finalized_turn_without_a_transcript_stays_integrity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    campaign, fixture, scope = _teaching_campaign(monkeypatch, tmp_path, finalized=True)
    campaign.teach(scope, fixture, scope["agent_id"], "1800000000.000001", 0)
    assert any("transcript" in failure for failure in campaign.failures)


@pytest.mark.parametrize("finalized", [False, True])
def test_retention_probe_missing_transcript_is_integrity_only_for_a_finalized_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, finalized: bool
) -> None:
    campaign, _fixture, scope = _teaching_campaign(monkeypatch, tmp_path, finalized=finalized)
    campaign.retention_probe(scope, "fact-1", "value-abc123", "What is it?", scope["agent_id"])
    transcript_failures = [f for f in campaign.failures if "transcript" in f]
    if finalized:
        assert transcript_failures
    else:
        assert transcript_failures == []


# Report acceptance folds in the recomputed auxiliary outcomes -------------------------


def test_auxiliary_outcomes_recompute_procedure_negative_and_delivery_outcomes(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    """``_auxiliary_outcomes(report_data)`` returns the agent outcomes verify-report
    recomputes from the procedure, memory negative and delivery sections."""
    from curie_worker.eval import learning

    report = copy.deepcopy(frozen_report)
    learned = report["procedure_probes"]["rows"][0]
    learned["successes"] = 0
    for ref in learned["probe_ids"]:
        _value(report, ref).update(success=False, output="unknown")
    _recalled_after_delete(report, 0)
    _deliver(report, 0, 0)
    _derive(report)
    # The same evidence is consistent: verify-report accepts it only as an outcome FAIL.
    _record_verdict(report, "FAIL", 3)
    assert verify_report(_write(tmp_path / "report.json", report)).passed
    assert learning._auxiliary_outcomes(copy.deepcopy(frozen_report)) == []
    outcomes = learning._auxiliary_outcomes(report)
    joined = "; ".join(outcomes)
    assert "shows no improvement" in joined
    assert "did not flip safely" in joined
    assert "delivered 0 times" in joined


def test_campaign_report_folds_auxiliary_outcomes_into_acceptance(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A procedure that shows no improvement is an outcome the campaign never appended
    to its lists; report() must still record it so the self-check does not turn a
    legitimate acceptance FAIL into an integrity FAIL."""
    from curie_worker.eval import learning

    monkeypatch.setattr(
        learning, "verify_report", lambda _path: ScoreResult(passed=True, detail="stub")
    )
    seen: list[dict[str, Any]] = []
    aux = "repetition 1 procedure task-1 shows no improvement"

    def auxiliary(report_data: dict[str, Any]) -> list[str]:
        seen.append(report_data)
        return [aux]

    monkeypatch.setattr(learning, "_auxiliary_outcomes", auxiliary, raising=False)
    campaign = _bare_campaign(tmp_path)
    campaign.failures = []
    campaign.outcome_failures = []
    report = campaign.report("a" * 40, {"verified": True, "problems": []})
    assert seen and "procedure_probes" in seen[0]
    assert report["implementation_verification"] == "PASS"
    assert report["acceptance"] == "FAIL"
    assert report["exit_code"] == 3
    assert aux in report["outcome_failures"]
    written = json.loads((tmp_path / "report.json").read_bytes())
    assert written["acceptance"] == "FAIL"
    assert written["exit_code"] == 3
    assert aux in written["outcome_failures"]


# The agent never learned the skill or stored the fact: an outcome, not missing rows ---


def _add_failed_control(report: dict[str, Any], case_id: str, control_id: str, detail: str) -> None:
    """Record the campaign's FAIL control for a case whose auxiliary rows never existed."""
    repetition = int(case_id.rsplit("-R", 1)[1])
    scope = {"repetition": repetition, "case_id": case_id, "agent_id": f"agent-{case_id}"}
    check = {"kind": "observed", "reason": detail}
    report["controls"].append(
        {
            **scope,
            "fixture_id": case_id.rsplit("-R", 1)[0],
            "control_id": control_id,
            "status": "FAIL",
            "evidence_ref": _evidence(
                report,
                "control-observation",
                {
                    **scope,
                    "control_id": control_id,
                    "status": "FAIL",
                    "detail": detail,
                    "checks": [check],
                },
            ),
        }
    )


_UNLEARNED = {
    "memory_negative": ("F3-P1-R1", "deleted-fact", "no live fact for the subject"),
    "procedure_probes": ("F1-P1-R1", "removed-skill", "no single learned skill"),
}


def _drop_rows(report: dict[str, Any], section: str, case_id: str) -> None:
    rows = report[section]["rows"]
    kept = [row for row in rows if row["case_id"] != case_id]
    assert len(kept) < len(rows)
    report[section]["rows"] = kept


# Binding for the inventory exemption (state evidence of absence):
# 1. The case subject is the fixture "subject" rendered with the "substitutions" of the
#    case's primary teaching observation (cases[].transcript_ref), as the live campaign
#    records them for _rederived_predicates.
# 2. removed-skill: the "after" state of that primary teaching observation holds no
#    single skill whose slug contains the subject (control_procedure's own test).
# 3. deleted-fact: the "after" state of the case's fact-correction control check
#    (kind "predicates") holds no fact that is not archived whose statement contains
#    the subject (control_fact's own test).
_SUBJECTS = {"F1": ("procedure_name", "sample-procedure"), "F3": ("subject", "cabinet-0a0a0a")}


def _primary_observation(report: dict[str, Any], case_id: str) -> dict[str, Any]:
    case = next(row for row in report["cases"] if row["case_id"] == case_id)
    return _value(report, case["transcript_ref"])


def _bind_subject(report: dict[str, Any], case_id: str) -> None:
    key, subject = _SUBJECTS[case_id[:2]]
    _primary_observation(report, case_id)["substitutions"] = {
        "values": {key: subject},
        "literals": {},
    }


def _forget_learned_item(report: dict[str, Any], section: str, case_id: str) -> None:
    """Record consistent state evidence that the agent never kept the item."""
    if section == "procedure_probes":
        # The teach stored no skill: the primary decision is an honest FAIL.
        case = next(row for row in report["cases"] if row["case_id"] == case_id)
        # The synthetic consent approval shares this observation, so it fails too.
        for row in report["decisions"]:
            if row["evidence_ref"] == case["transcript_ref"]:
                row.update(predicted=[], mutations=[], status="FAIL")
        observation = _value(report, case["transcript_ref"])
        observation["after"] = copy.deepcopy(observation["before"])
        observation["deterministic"] = False
        _derive(report)
        return
    # The correction left no live fact: fact-correction recomputes to FAIL.
    control = next(
        row
        for row in report["controls"]
        if row["case_id"] == case_id and row["control_id"] == "fact-correction"
    )
    control["status"] = "FAIL"
    observation = _value(report, control["evidence_ref"])
    observation["status"] = "FAIL"
    observation["checks"][0]["after"]["facts"] = []


def _excuse(report: dict[str, Any], section: str, case_id: str) -> None:
    _, control_id, detail = _UNLEARNED[section]
    _drop_rows(report, section, case_id)
    _bind_subject(report, case_id)
    _forget_learned_item(report, section, case_id)
    _add_failed_control(report, case_id, control_id, detail)


@pytest.mark.parametrize("section", sorted(_UNLEARNED))
def test_unlearned_case_with_its_failed_control_is_an_outcome_without_rows(
    tmp_path: Path, frozen_report: dict[str, Any], section: str
) -> None:
    """A case whose agent never learned the skill or stored the fact has no auxiliary
    rows; its FAIL control makes that an acceptance FAIL, not an integrity failure."""
    case_id, control_id, detail = _UNLEARNED[section]
    report = copy.deepcopy(frozen_report)
    _excuse(report, section, case_id)
    report["outcome_failures"] = [f"{case_id} control {control_id}: {detail}"]
    _rehash(report)
    _record_verdict(report, "PASS", 0)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


@pytest.mark.parametrize("section", sorted(_UNLEARNED))
@pytest.mark.parametrize("control", ["absent", "passing", "other-case"])
def test_missing_auxiliary_rows_without_a_failed_control_stay_integrity(
    tmp_path: Path, frozen_report: dict[str, Any], section: str, control: str
) -> None:
    case_id, control_id, detail = _UNLEARNED[section]
    report = copy.deepcopy(frozen_report)
    _drop_rows(report, section, case_id)
    if control == "passing":
        _add_failed_control(report, case_id, control_id, detail)
        record = report["controls"][-1]
        record["status"] = "PASS"
        # A check that recomputes to PASS, so only the missing rows can fail.
        _value(report, record["evidence_ref"]).update(
            status="PASS", checks=[{"kind": "aux", "section": section}]
        )
    elif control == "other-case":
        other = case_id.replace("-P1-", "-P2-")
        _add_failed_control(report, other, control_id, detail)
    _rehash(report)
    _record_verdict(report, "FAIL", 3)
    result = verify_report(_write(tmp_path / "report.json", report))
    assert not result.passed
    assert "do not match the inventory" in result.detail


def test_negative_validator_accepts_an_unlearned_case_beside_demonstrable_flips(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    for section, (case_id, _control_id, _detail) in _UNLEARNED.items():
        _excuse(report, section, case_id)
    _rehash(report)
    _check(tmp_path, report, verify_negative, passed=True)


def test_negative_validator_still_needs_one_demonstrable_flip(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    for phrase in range(1, 4):
        _excuse(report, "memory_negative", f"F3-P{phrase}-R1")
    _rehash(report)
    result = verify_negative(_write(tmp_path / "report.json", report))
    assert not result.passed
    assert "no memory negative row retained its fact" in result.detail


def test_control_fact_without_a_live_fact_records_a_failed_deleted_fact_control(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    campaign = _bare_campaign(tmp_path)
    campaign.capabilities["schedule"] = {"available": False}
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "snapshot", lambda _scope: (copy.deepcopy(empty), None))

    def message(scope: Any, text: str, channel: str, thread: str) -> dict[str, Any]:
        return {
            "invocation_id": "invocation-1",
            "result_ref": "cli-result-1",
            "exit_code": 0,
            "reply": "noted",
            "thread": thread,
            "channel": channel,
            "finalized": True,
            "generation": "operation-1",
        }

    def no_cli(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no deletion or retention probe without a live fact")

    monkeypatch.setattr(campaign, "message", message)
    monkeypatch.setattr(campaign, "cli", no_cli)
    fixture = next(row for row in campaign.corpus if row["id"] == "F3-P1")
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    campaign.control_fact(scope, dict(fixture), scope["agent_id"], "1800000000.000001", None, {})
    assert campaign.negative_rows == []
    deleted = [c for c in campaign.controls if c["control_id"] == "deleted-fact"]
    assert len(deleted) == 1 and deleted[0]["status"] == "FAIL"
    observation = campaign.evidence[deleted[0]["evidence_ref"]]["value"]
    assert str(observation["detail"]).startswith("no live fact")
    assert not learning._control_check(observation["checks"][0])
    assert any("deleted-fact" in failure for failure in campaign.outcome_failures)
    assert campaign.failures == []


@pytest.mark.parametrize("section", sorted(_UNLEARNED))
def test_failed_control_label_cannot_excuse_rows_when_state_shows_the_item_learned(
    tmp_path: Path, frozen_report: dict[str, Any], section: str
) -> None:
    """The FAIL label alone is no evidence: the bound state still holds the learned
    skill (primary teaching observation) or the live fact (fact-correction after)."""
    case_id, control_id, detail = _UNLEARNED[section]
    report = copy.deepcopy(frozen_report)
    _drop_rows(report, section, case_id)
    _bind_subject(report, case_id)
    _add_failed_control(report, case_id, control_id, detail)
    report["outcome_failures"] = [f"{case_id} control {control_id}: {detail}"]
    _rehash(report)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=False)


# A failed retention probe turn never demonstrates a flip ---------------------------


def _fail_probe_turn(report: dict[str, Any], ref: str) -> None:
    """The probe's message turn exited 1 without finalizing; its reply stays recorded."""
    probe = _value(report, ref)
    invocation = next(
        row for row in report["invocations"] if row["invocation_id"] == probe["invocation_id"]
    )
    invocation["exit_code"] = 1
    _value(report, invocation["result_ref"])["result"].update(
        finalized=False, failed=True, failure_class="runner-error"
    )


def test_failed_after_probe_turn_is_not_a_demonstrable_flip(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _fail_probe_turn(report, report["memory_negative"]["rows"][0]["after_evidence"])
    _not_stored(report, 1)
    _not_stored(report, 2)
    _rehash(report)
    _check(tmp_path, report, verify_negative, passed=False)


def test_failed_before_probe_turn_cannot_count_as_retained(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _fail_probe_turn(report, report["memory_negative"]["rows"][0]["before_evidence"])
    _not_stored(report, 1)
    _not_stored(report, 2)
    _rehash(report)
    _check(tmp_path, report, verify_negative, passed=False)


def test_failed_after_probe_turn_counts_against_acceptance(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    """The strict every-row rule treats a row whose after probe failed as not flipped."""
    report = copy.deepcopy(frozen_report)
    _fail_probe_turn(report, report["memory_negative"]["rows"][0]["after_evidence"])
    _rehash(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


# Tool-evidence errors on a finalized turn are integrity -----------------------------

_ORPHAN_RESULT = [
    {
        "role": "user",
        "content": [{"type": "tool_result", "tool_use_id": "toolu_orphan", "content": "ok"}],
    }
]


def _error_decision(report: dict[str, Any], evidence: str, *, finalized: bool) -> None:
    """Record F3-P1-R1's primary decision as an ERROR from a tool-evidence problem."""
    row = next(
        row
        for row in report["decisions"]
        if row["fixture_id"] == "F3-P1" and row["expected"] == "memory-fact"
    )
    observation = _value(report, row["evidence_ref"])
    if evidence == "orphan-result":
        row.update(predicted=[], mutations=[], status="ERROR", detail="transcript evidence")
        observation["after"] = copy.deepcopy(observation["before"])
        observation["transcript"] = {"messages": copy.deepcopy(_ORPHAN_RESULT)}
    else:
        # Durable state changed but no matched, non-error persistence attempt exists.
        row.update(status="ERROR", attempts=[], detail="missing tool evidence")
        observation["transcript"] = {"messages": []}
    observation["deterministic"] = False
    if not finalized:
        scope = {key: row[key] for key in ("repetition", "case_id", "agent_id")}
        invocation_id, result_ref = _invocation(
            report,
            scope,
            ["--json", "local", "message", "teach", "--agent", scope["agent_id"]],
            {"reply": "The run failed.", "finalized": False, "failure_class": "runner-error"},
        )
        report["invocations"][-1]["exit_code"] = 1
        observation["invocation_id"] = invocation_id
    report["outcome_failures"] = [f"F3-P1-R1 {row['decision_id']} ERROR: {row['detail']}"]
    _derive(report)


@pytest.mark.parametrize("evidence", ["orphan-result", "missing-tool-evidence"])
def test_tool_evidence_error_on_a_finalized_turn_fails_integrity(
    tmp_path: Path, frozen_report: dict[str, Any], evidence: str
) -> None:
    report = copy.deepcopy(frozen_report)
    _error_decision(report, evidence, finalized=True)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=False)


@pytest.mark.parametrize("evidence", ["orphan-result", "missing-tool-evidence"])
def test_tool_evidence_error_on_a_failed_turn_stays_an_outcome(
    tmp_path: Path, frozen_report: dict[str, Any], evidence: str
) -> None:
    report = copy.deepcopy(frozen_report)
    _error_decision(report, evidence, finalized=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


def _score_turn(
    tmp_path: Path, *, finalized: bool, turn: list[dict[str, Any]], learned: bool
) -> Any:
    """Drive the real score_decision for F3-P1's teach decision on a recorded turn."""
    campaign = _bare_campaign(tmp_path)
    campaign.capabilities["schedule"] = {"available": False}
    fixture = next(row for row in campaign.corpus if row["id"] == "F3-P1")
    spec = next(spec for spec in fixture["decisions"] if int(spec["message"]) == 0)
    before = {"skills": [], "facts": [], "schedules": []}
    after = copy.deepcopy(before)
    if learned:
        after["facts"].append(_fact_row("fact-learned", "cabinet-0a0a0a is violet-a1b2c3"))
    sent = {
        "invocation_id": "invocation-1",
        "result_ref": "cli-result-1",
        "exit_code": 0 if finalized else 1,
        "reply": "noted",
        "thread": "1800000000.000001",
        "channel": campaign.channels[0],
        "finalized": finalized,
        "generation": "operation-1",
    }
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    campaign.score_decision(
        scope,
        dict(fixture),
        spec,
        (before, None),
        (after, None),
        sent,
        {"messages": turn},
        turn,
    )
    return campaign


@pytest.mark.parametrize(
    "turn,learned,reason",
    [(_ORPHAN_RESULT, False, "transcript evidence"), ([], True, "missing tool evidence")],
    ids=["orphan-result", "missing"],
)
def test_campaign_records_tool_evidence_errors_on_finalized_turns_as_integrity(
    tmp_path: Path, turn: list[dict[str, Any]], learned: bool, reason: str
) -> None:
    campaign = _score_turn(tmp_path, finalized=True, turn=turn, learned=learned)
    assert campaign.decisions[-1]["status"] == "ERROR"
    assert campaign.decisions[-1]["detail"].startswith(reason)
    assert campaign.failures


def test_campaign_keeps_a_failed_turn_with_bad_tool_evidence_an_outcome(tmp_path: Path) -> None:
    campaign = _score_turn(tmp_path, finalized=False, turn=_ORPHAN_RESULT, learned=False)
    assert campaign.decisions[-1]["status"] == "ERROR"
    assert campaign.failures == []
    assert campaign.outcome_failures


# Quiet collector coverage of every due time, independent of receipts ---------------


def _undeliver_all(report: dict[str, Any]) -> None:
    for index, row in enumerate(report["deliveries"]["rows"]):
        if row["due"]:
            _deliver(report, index, 0)


@pytest.mark.parametrize("window", ["children-alive", "closes-early", "opens-late"])
def test_due_time_without_quiet_collector_coverage_fails_integrity(
    tmp_path: Path, frozen_report: dict[str, Any], window: str
) -> None:
    """With no receipts at all, the collector must still prove it listened: a quiet
    window (no live message children) covering due_at through due_at plus the
    observation's tolerance_seconds, for every due schedule of the repetition."""
    report = copy.deepcopy(frozen_report)
    _undeliver_all(report)
    record = report["collector"]["delivery_windows"][0]
    if window == "children-alive":
        record["message_children_alive"] = 1
    elif window == "closes-early":
        record["closed_at"] = "2026-01-01T12:01:00Z"
    else:
        record["opened_at"] = "2026-01-01T12:00:30Z"
    report["outcome_failures"] = ["F4 scheduled delivery rate is not 100%"]
    _derive(report)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=False)


def test_undelivered_schedules_under_quiet_coverage_are_an_outcome(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _undeliver_all(report)
    _derive(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


# score_state evidence errors on a finalized turn are integrity ------------------------


def _evidence_error_decision(report: dict[str, Any], *, finalized: bool) -> None:
    """F3-P1-R1's primary decision recorded ERROR because score_state raised: its
    admitted predicate targets a class the recorded snapshots do not admit."""
    # No durable change, so the tool-evidence replay has nothing to object to.
    row = _fail_memory_fact_decision(report)
    observation = _value(report, row["evidence_ref"])
    observation.update(
        admitted_predicates=[
            {"name": "active_count", "class": "memory-fact", "target": "fact-x", "value": 1}
        ],
        available=["skill"],
        deterministic=False,
    )
    row.update(status="ERROR", detail="evidence error: predicate targets unadmitted memory-fact")
    if not finalized:
        _error_decision_turn(report, row, failure_class=True)
    report["outcome_failures"] = [f"F3-P1-R1 {row['decision_id']} ERROR: {row['detail']}"]
    _derive(report)


def _error_decision_turn(
    report: dict[str, Any], row: dict[str, Any], *, result: object = None, failure_class: bool
) -> None:
    """Bind the decision observation to its own non-finalized message invocation."""
    scope = {key: row[key] for key in ("repetition", "case_id", "agent_id")}
    if result is None:
        # cli/src/message.rs message_failed_json (MessageOutcomeOutput::Failed::to_json).
        result = {
            "reply": "The run failed.",
            "thread": "1800000009.000002",
            "finalized": False,
            "failed": True,
            "failure_class": "runner-error",
        }
    invocation_id, _ = _invocation(
        report,
        scope,
        ["--json", "local", "message", "teach", "--agent", scope["agent_id"]],
        result,
    )
    report["invocations"][-1]["exit_code"] = 1
    _value(report, row["evidence_ref"])["invocation_id"] = invocation_id


def test_score_state_evidence_error_on_a_finalized_turn_fails_integrity(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _evidence_error_decision(report, finalized=True)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=False)


def test_score_state_evidence_error_on_a_failed_turn_stays_an_outcome(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _evidence_error_decision(report, finalized=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


@pytest.mark.parametrize("finalized", [True, False])
def test_campaign_records_score_state_evidence_errors_by_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, finalized: bool
) -> None:
    from curie_worker.eval import learning_metrics

    def broken(*_args: Any, **_kwargs: Any) -> Any:
        raise learning_metrics.EvidenceError("predicate targets unadmitted memory-fact")

    monkeypatch.setattr(learning_metrics, "score_state", broken)
    campaign = _score_turn(tmp_path, finalized=finalized, turn=[], learned=False)
    assert campaign.decisions[-1]["status"] == "ERROR"
    if finalized:
        assert campaign.decisions[-1]["detail"].startswith("evidence error")
        assert campaign.failures
    else:
        assert campaign.failures == []
        assert campaign.outcome_failures


@pytest.mark.parametrize(
    "completion,semantic",
    [
        ("not a verdict", "ERROR"),
        (
            json.dumps(
                {"verdict": "UNCLEAR", "action": None, "reason": "Unsure.", "evidence_refs": []}
            ),
            "UNCLEAR",
        ),
    ],
    ids=["malformed", "unclear"],
)
def test_malformed_or_unclear_judge_output_is_a_decision_fail_outcome(
    tmp_path: Path, frozen_report: dict[str, Any], completion: str, semantic: str
) -> None:
    """Contract pin: an UNCLEAR or malformed judge verdict counts as the decision's FAIL,
    an agent-scored outcome, never a harness integrity failure."""
    report = copy.deepcopy(frozen_report)
    # F3-P1's primary decision owns its observation (F1 shares one with consent).
    row = next(
        row for row in report["decisions"] if row["case_id"] == "F3-P1-R1" and "judge_ref" in row
    )
    ref = row["judge_ref"]
    ref["completion"] = completion
    ref["completion_sha256"] = _sha(completion.encode())
    generation = next(g for g in report["generations"] if g["id"] == ref["generation_id"])
    generation["completion_sha256"] = ref["completion_sha256"]
    observation = _value(report, row["evidence_ref"])
    observation.update(semantic=semantic, semantic_action=None)
    row["status"] = "FAIL"
    report["outcome_failures"] = [f"{row['case_id']} {row['decision_id']} FAIL: judge"]
    _derive(report)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


# Only a structured turn outcome proves the turn reached the agent ---------------------

# Result shapes the pinned CLI writes for `local message --json`:
# 1. cli/src/message.rs message_failed_json: reply, thread, finalized false, failed true,
#    failure_class.
# 2. cli/src/message.rs message_timeout_json: reply null, finalized false, timed_out true.
# 3. cli/src/message.rs message_awaiting_approval_json: reply, thread, finalized false,
#    awaiting_approval true.
# 4. cli/src/exit.rs error_json, the generic --json error payload of a failure before
#    enqueue (Valkey, agent lookup): {"error": "<rendered error>", "fix": <hint or null>}.
_TURN_RESULTS: dict[str, tuple[int, dict[str, Any]]] = {
    "failed": (
        1,
        {
            "reply": "The run failed.",
            "thread": "1800000009.000002",
            "finalized": False,
            "failed": True,
            "failure_class": "runner-error",
        },
    ),
    "timed-out": (75, {"reply": None, "finalized": False, "timed_out": True}),
    "awaiting-approval": (
        1,
        {
            "reply": "Approval needed.",
            "thread": "1800000009.000002",
            "finalized": False,
            "awaiting_approval": True,
        },
    ),
    "generic-error": (1, {"error": "agent agent-F3-P1-R1 was not found", "fix": None}),
}


@pytest.mark.parametrize("kind", sorted(_TURN_RESULTS))
def test_decision_turn_must_be_a_structured_turn_outcome(
    tmp_path: Path, frozen_report: dict[str, Any], kind: str
) -> None:
    report = copy.deepcopy(frozen_report)
    row = _fail_memory_fact_decision(report)
    row.update(status="ERROR", detail="the turn did not exit 0 and finalize")
    code, result = _TURN_RESULTS[kind]
    _error_decision_turn(report, row, result=copy.deepcopy(result), failure_class=False)
    report["invocations"][-1]["exit_code"] = code
    report["outcome_failures"] = ["F3-P1-R1 turn did not finalize"]
    _derive(report)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=kind != "generic-error")


def _message_campaign(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str
) -> tuple[Any, dict[str, Any]]:
    """A campaign whose real message() receives the pinned CLI's ``kind`` result."""
    from curie_worker.eval import learning

    clock = _clocked(monkeypatch)
    monkeypatch.setattr(learning, "_docker_lines", lambda *_args: [])
    campaign = _bare_campaign(tmp_path)
    campaign.capabilities["schedule"] = {"available": False}
    campaign.stack = learning._Stack(
        project="curie-learning-abc-r1a1",
        tag="learning-abc",
        override=tmp_path / "override.yaml",
        ports={},
        stub_port=0,
        stub_host="127.0.0.1",
        repetition=1,
    )
    provider = _ScriptedProvider(clock, ["10.00"])
    campaign.ledger = learning._Ledger(tmp_path / "ledger.sqlite", Decimal("7.50"), provider)
    monkeypatch.setattr(campaign.ledger, "settle_stable", lambda *_a, **_k: "0")
    monkeypatch.setattr(campaign.ledger, "settle_terminal", lambda *_a, **_k: "0")
    monkeypatch.setattr(campaign, "wait_for_stub_port", lambda: None)
    monkeypatch.setattr(campaign, "actor_reserve", lambda: Decimal("0.50"))
    monkeypatch.setattr(campaign, "sweep_schedules", lambda _scope: None)
    code, result = _TURN_RESULTS[kind]
    monkeypatch.setattr(
        campaign,
        "cli",
        lambda *_args, **_kwargs: (code, copy.deepcopy(result), "invocation-1", "ref-1", ""),
    )
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    return campaign, scope


@pytest.mark.parametrize("kind", sorted(_TURN_RESULTS))
def test_message_records_a_pre_enqueue_error_as_integrity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str
) -> None:
    campaign, scope = _message_campaign(monkeypatch, tmp_path, kind)
    campaign.message(scope, "teach this", "C0EXAMPLE1", "1800000000.000001")
    if kind == "generic-error":
        assert campaign.failures
        assert campaign.outcome_failures == []
    else:
        assert campaign.failures == []
        assert campaign.outcome_failures


def test_teach_never_records_a_stub_bind_for_a_pre_enqueue_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    campaign, scope = _message_campaign(monkeypatch, tmp_path, "generic-error")
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "snapshot", lambda _scope: (copy.deepcopy(empty), None))

    def transcript(*_args: Any, **_kwargs: Any) -> Any:
        raise learning._Stop("no transcript for thread 1800000000.000001 (status 404)")

    monkeypatch.setattr(campaign, "transcript", transcript)
    fixture = next(row for row in campaign.corpus if row["id"] == "F3-P1")
    campaign.teach(scope, dict(fixture), scope["agent_id"], "1800000000.000001", 0)
    assert campaign.cases[-1]["stub_bind_succeeded"] is not True
    assert campaign.failures


# A control ERROR is a harness observation fault; a control FAIL is an outcome --------


def _rerecord_control(report: dict[str, Any], status: str) -> None:
    """Re-record F3-P1-R1's fact-correction control with ``status`` from a failed read."""
    control = next(
        row
        for row in report["controls"]
        if row["case_id"] == "F3-P1-R1" and row["control_id"] == "fact-correction"
    )
    control["status"] = status
    _value(report, control["evidence_ref"]).update(
        status=status,
        detail="the memory read after the correction failed",
        checks=[{"kind": "observed", "reason": "the memory read after the correction failed"}],
    )
    report["outcome_failures"] = [f"F3-P1-R1 control fact-correction: {status}"]
    _rehash(report)


@pytest.mark.parametrize("acceptance,exit_code", [("FAIL", 3), ("PASS", 0)])
def test_control_error_fails_integrity_under_any_recorded_verdict(
    tmp_path: Path, frozen_report: dict[str, Any], acceptance: str, exit_code: int
) -> None:
    report = copy.deepcopy(frozen_report)
    _rerecord_control(report, "ERROR")
    _record_verdict(report, acceptance, exit_code)
    _check(tmp_path, report, passed=False)


def test_control_fail_stays_an_acceptance_outcome(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    _rerecord_control(report, "FAIL")
    _record_verdict(report, "PASS", 0)
    _check(tmp_path, report, passed=False)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=True)


@pytest.mark.parametrize("status", ["ERROR", "FAIL"])
def test_record_control_classes_error_as_integrity_and_fail_as_outcome(
    tmp_path: Path, status: str
) -> None:
    campaign = _bare_campaign(tmp_path)
    scope = {"repetition": 1, "case_id": "F5-P1-R1", "agent_id": "agent-F5-P1-R1"}
    detail = "the schedule read after cancel failed"
    campaign.record_control(
        scope, "revise-and-cancel", status, detail, [campaign.observed("schedule read")]
    )
    assert campaign.controls[-1]["status"] == status
    if status == "ERROR":
        assert any(detail in failure for failure in campaign.failures)
        assert campaign.outcome_failures == []
    else:
        assert campaign.failures == []
        assert any(detail in failure for failure in campaign.outcome_failures)


# Evidence errors while replaying a control's predicates are integrity ---------------


def _fact_correction_control(report: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    control = next(
        row
        for row in report["controls"]
        if row["case_id"] == "F3-P1-R1" and row["control_id"] == "fact-correction"
    )
    return control, _value(report, control["evidence_ref"])


def _predicates_check(scope: dict[str, Any], *, malformed: bool) -> dict[str, Any]:
    """The fact-correction predicates check. ``malformed`` records available classes
    that exclude memory-fact, so score_state raises EvidenceError on replay; otherwise
    the correction left no fact and the check is a well-formed FAIL."""
    before, after = _fact_correction_snapshots(scope)
    check: dict[str, Any] = {
        "kind": "predicates",
        "predicates": _fact_correction_predicates(scope),
        "before": before,
        "after": after if malformed else {**after, "facts": []},
    }
    if malformed:
        check["available"] = ["skill"]
    return check


@pytest.mark.parametrize("malformed,passed", [(True, False), (False, True)])
def test_control_predicates_evidence_error_fails_integrity(
    tmp_path: Path, frozen_report: dict[str, Any], malformed: bool, passed: bool
) -> None:
    """A recorded FAIL whose predicates replay raises EvidenceError is a harness
    evidence fault; a well-formed check that recomputes to FAIL is an outcome."""
    report = copy.deepcopy(frozen_report)
    control, observation = _fact_correction_control(report)
    scope = {key: control[key] for key in ("repetition", "case_id", "agent_id")}
    control["status"] = "FAIL"
    observation.update(status="FAIL", checks=[_predicates_check(scope, malformed=malformed)])
    report["outcome_failures"] = ["F3-P1-R1 control fact-correction: FAIL"]
    _rehash(report)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=passed)


@pytest.mark.parametrize("malformed", [True, False])
def test_record_control_classes_predicates_evidence_errors_as_integrity(
    tmp_path: Path, malformed: bool
) -> None:
    campaign = _bare_campaign(tmp_path)
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    campaign.record_control(
        scope,
        "fact-correction",
        None,
        "1 active facts for the subject after correction",
        [_predicates_check(scope, malformed=malformed)],
    )
    if malformed:
        assert campaign.failures
        assert campaign.outcome_failures == []
        assert campaign.controls[-1]["status"] != "PASS"
    else:
        assert campaign.controls[-1]["status"] == "FAIL"
        assert campaign.failures == []
        assert campaign.outcome_failures


def test_deterministic_replay_never_turns_an_evidence_error_into_false() -> None:
    """The live decision consistency check compares this replay with the recorded
    deterministic flag; an EvidenceError swallowed into False would let a recorded
    FAIL agree with snapshots that cannot be scored."""
    from curie_worker.eval import learning
    from curie_worker.eval.learning_metrics import EvidenceError

    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    before, after = _fact_correction_snapshots(scope)
    observation = {
        "admitted_predicates": _fact_correction_predicates(scope),
        "available": ["skill"],
        "before": before,
        "after": after,
    }
    with pytest.raises((learning._Invalid, EvidenceError)):
        learning._recompute_deterministic(observation)


# Every control check is evaluated: no short circuit past a failing one -------------

# A well-formed check that recomputes to False: the reply lacks the expected value.
_FAILING_CHECK = {"kind": "contains", "reply": "I do not know.", "expected": "violet"}


def test_control_replay_evaluates_a_malformed_check_after_a_failing_one(
    tmp_path: Path, frozen_report: dict[str, Any]
) -> None:
    report = copy.deepcopy(frozen_report)
    control, observation = _fact_correction_control(report)
    scope = {key: control[key] for key in ("repetition", "case_id", "agent_id")}
    control["status"] = "FAIL"
    observation.update(
        status="FAIL",
        checks=[dict(_FAILING_CHECK), _predicates_check(scope, malformed=True)],
    )
    report["outcome_failures"] = ["F3-P1-R1 control fact-correction: FAIL"]
    _rehash(report)
    _record_verdict(report, "FAIL", 3)
    _check(tmp_path, report, passed=False)


def test_record_control_evaluates_a_malformed_check_after_a_failing_one(
    tmp_path: Path,
) -> None:
    campaign = _bare_campaign(tmp_path)
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    campaign.record_control(
        scope,
        "fact-correction",
        None,
        "1 active facts for the subject after correction",
        [dict(_FAILING_CHECK), _predicates_check(scope, malformed=True)],
    )
    assert campaign.controls[-1]["status"] == "ERROR"
    assert campaign.failures
    assert campaign.outcome_failures == []


# Runner absence is scoped to the turn's agent and attempt ------------------------------

_RUNNER_PROJECT = "curie-learning-2d28d964-r1a1"
_RUNNER_AGENT = "le-2d28d964-r1-f2-p3"
_SINCE = datetime(2026, 10, 23, 3, 30, 0, 500000, tzinfo=UTC)


def _runner_campaign(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, answers: list[Any]
) -> tuple[Any, list[tuple[str, ...]]]:
    """A campaign whose Docker reads answer from ``answers`` in order (the last
    repeats); an Exception answer is raised. Lines use the recorded shape of
    `docker container ls --format '{{.ID}} {{.CreatedAt}}'`, whose CreatedAt prints
    like "2026-10-23 03:23:11 +0000 UTC"."""
    from curie_worker.eval import learning

    _clocked(monkeypatch)
    calls: list[tuple[str, ...]] = []

    def docker_lines(*args: str) -> list[str]:
        calls.append(args)
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if isinstance(answer, Exception):
            raise answer
        return list(answer)

    monkeypatch.setattr(learning, "_docker_lines", docker_lines)
    campaign = _bare_campaign(tmp_path)
    campaign.stack = learning._Stack(
        project=_RUNNER_PROJECT,
        tag="learning-2d28d964",
        override=tmp_path / "override.yaml",
        ports={},
        stub_port=0,
        stub_host="127.0.0.1",
        repetition=1,
    )
    return campaign, calls


def _runner_line(created: str) -> str:
    return f"0123456789ab {created} +0000 UTC"


@pytest.mark.parametrize(
    "lines,absent",
    [
        ([], True),
        # The same agent's runner from an earlier thread keeps running between turns.
        ([_runner_line("2026-10-23 03:23:11")], True),
        ([_runner_line("2026-10-23 03:30:05")], False),
        # CreatedAt has whole-second resolution: the second that contains since counts.
        ([_runner_line("2026-10-23 03:30:00")], False),
        ([_runner_line("2026-10-23 03:23:11"), _runner_line("2026-10-23 03:31:00")], False),
    ],
    ids=["none", "older-same-agent", "newer", "same-second", "older-and-newer"],
)
def test_runner_absent_counts_only_this_agents_runners_since_the_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, lines: list[str], absent: bool
) -> None:
    campaign, calls = _runner_campaign(monkeypatch, tmp_path, [lines])
    assert campaign.runner_absent(_RUNNER_AGENT, _SINCE) is absent
    filtered = [call for call in calls if "status=running" in call]
    assert filtered
    for call in filtered:
        assert f"label=curietech.ai/stack={_RUNNER_PROJECT}_runner" in call
        assert f"label=curietech.ai/agent={_RUNNER_AGENT}" in call
        assert "{{.ID}} {{.CreatedAt}}" in call


def test_runner_absent_treats_a_docker_error_as_not_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    error = learning._Invalid("the caller's Docker daemon could not be observed")
    campaign, calls = _runner_campaign(monkeypatch, tmp_path, [error])
    assert campaign.runner_absent(_RUNNER_AGENT, _SINCE) is False
    assert calls


def test_runner_absent_keeps_a_grace_for_a_claim_runner_being_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    newer = [_runner_line("2026-10-23 03:30:05")]
    campaign, calls = _runner_campaign(monkeypatch, tmp_path, [newer, newer, []])
    # Absence is seen within the grace: the third read, after two sleeps.
    assert learning.RUNNER_ABSENT_CHECKS >= 3
    assert campaign.runner_absent(_RUNNER_AGENT, _SINCE) is True
    assert len(calls) >= 3


# Echoing the probe's own subject id is not a fabrication ------------------------------

# Recorded live shapes from a full campaign on a throwaway stack: every F3 deletion
# negative flipped (the before probe recalled the value), and the after probe echoed the
# subject id from its own input without stating any value.
_LIVE_VALUE = "amber-821774"
_LIVE_INPUT = "What is our current reference value for cabinet-a48f91?"
_LIVE_ECHO = "I couldn't find a reference value for cabinet-a48f91..."
_LIVE_UNSEEN = "I couldn't find it for cabinet-a48f91; it might be amber-ffffff."


def _live_negative_row(report: dict[str, Any], index: int, after_output: str) -> None:
    """Re-record one memory negative row with the live value, input and replies."""
    row = report["memory_negative"]["rows"][index]
    for ref, present, output in (
        (row["before_evidence"], True, _LIVE_VALUE),
        (row["after_evidence"], False, after_output),
    ):
        probe = _value(report, ref)
        probe.update(input=_LIVE_INPUT, required_value=_LIVE_VALUE)
        probe["retention_scorer"]["required_value"] = _LIVE_VALUE
        invocation = next(
            r for r in report["invocations"] if r["invocation_id"] == probe["invocation_id"]
        )
        invocation["argv"] = [
            _LIVE_INPUT if arg == "What is the reference value?" else arg
            for arg in invocation["argv"]
        ]
        _rewrite_probe(report, ref, present=present, output=output)
    row["retention_before"] = _value(report, row["before_evidence"])["retention"]
    row["retention_after"] = _value(report, row["after_evidence"])["retention"]


@pytest.mark.parametrize(
    "output,passed", [(_LIVE_ECHO, True), (_LIVE_UNSEEN, False)], ids=["echo", "unseen-nonce"]
)
def test_negative_flip_allows_echoing_the_subject_from_the_probe_input(
    tmp_path: Path, frozen_report: dict[str, Any], output: str, passed: bool
) -> None:
    report = copy.deepcopy(frozen_report)
    # Row 0 is the only demonstrable row, so it alone decides verify-negative.
    _live_negative_row(report, 0, output)
    _not_stored(report, 1)
    _not_stored(report, 2)
    after = _value(report, report["memory_negative"]["rows"][0]["after_evidence"])
    assert after["retention"] == "FAIL" and after["no_recall"] is True
    assert after["no_fabrication"] is passed
    _rehash(report)
    _check(tmp_path, report, verify_negative, passed=passed)


@pytest.mark.parametrize(
    "output,clean", [(_LIVE_ECHO, True), (_LIVE_UNSEEN, False)], ids=["echo", "unseen-nonce"]
)
def test_retention_probe_judges_fabrication_against_its_own_input(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, output: str, clean: bool
) -> None:
    campaign = _bare_campaign(tmp_path)

    def message(scope: Any, text: str, channel: str, thread: str) -> dict[str, Any]:
        return {
            "invocation_id": "invocation-1",
            "result_ref": "cli-result-1",
            "exit_code": 0,
            "reply": output,
            "thread": thread,
            "channel": channel,
            "finalized": True,
            "generation": "operation-1",
        }

    def transcript(*_args: Any, **_kwargs: Any) -> Any:
        return {"binding_key": "slack:C:T", "messages": []}, []

    monkeypatch.setattr(campaign, "message", message)
    monkeypatch.setattr(campaign, "transcript", transcript)
    monkeypatch.setattr(
        campaign,
        "cli",
        lambda *_args, **_kwargs: (0, {"facts": []}, "invocation-m", "cli-result-m", ""),
    )
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    ref = campaign.retention_probe(scope, "fact-1", _LIVE_VALUE, _LIVE_INPUT, scope["agent_id"])
    probe = campaign.evidence[ref]["value"]
    assert probe["no_recall"] is True
    assert probe["no_fabrication"] is clean


@pytest.mark.parametrize("validator", [verify_negative, verify_report], ids=["negative", "report"])
def test_retention_probe_input_must_match_the_message_it_sent(
    tmp_path: Path, frozen_report: dict[str, Any], validator: Callable[[Path], ScoreResult]
) -> None:
    """The fabrication exemption reads the probe input, so that input must be the text
    the recorded `local message <text>` invocation sent: editing only the evidence
    input to carry the unseen nonce cannot exempt it."""
    report = copy.deepcopy(frozen_report)
    _live_negative_row(report, 0, _LIVE_UNSEEN)
    after = _value(report, report["memory_negative"]["rows"][0]["after_evidence"])
    after["input"] = f"{_LIVE_INPUT} amber-ffffff"
    after["no_fabrication"] = True
    _rehash(report)
    result = validator(_write(tmp_path / "report.json", report))
    assert not result.passed
    assert "input" in result.detail


# A lagging usage counter still settles a finalized turn --------------------------------

# Recorded live shape: after a finalized turn the shared-key usage stayed flat at
# 337.158826792 from 06:20:13 to 06:22:14 and had moved to 337.197819782 by 06:23:08.
_LAG_BEFORE = "337.158826792"
_LAG_AFTER = "337.197819782"


def test_stable_settlement_waits_out_a_lagging_usage_counter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    assert learning.STABLE_SETTLE_SECONDS >= 360
    clock = _clocked(monkeypatch)
    sleep = learning.STABLE_POLL_SLEEP_SECONDS
    # Flat for about 180 s of polls, then the turn's spend appears and holds.
    flat = [_LAG_BEFORE] * (180 // sleep + 3)
    provider = _ScriptedProvider(clock, [*flat, _LAG_AFTER])
    ledger, identity = _ledger(tmp_path, provider)
    ledger.settle_stable(identity, Decimal(_LAG_BEFORE), record=identity)
    operation = _operation(ledger, identity)
    assert operation["status"] == "settled"
    assert Decimal(operation["settled_usd"]) == Decimal(_LAG_AFTER) - Decimal(_LAG_BEFORE)
    # Settled on two equal polls of the moved value, one poll interval apart.
    assert provider.calls[-1] - provider.calls[-2] == sleep
    assert provider.calls[-2] >= 180


# The F4 reservation peak fits the campaign budget --------------------------------------

# Recorded live ledger when a 3.55 USD campaign stopped "budget exhausted" in F4 with
# 0.36 USD of real spend: about 0.50 USD charged, four schedule fire reservations held
# (two still at 0.7625 = the 0.75 native cap plus a 0.0125 overshoot), the next actor
# reservation at 0.7625, and SHUTDOWN_RESERVE.
_PEAK_CHARGED = Decimal("0.50")
_PEAK_OVERSHOOT = Decimal("0.0125")
_PEAK_FIRES = 4
_PEAK_LIMIT = Decimal("2.80")


def test_f4_reservation_peak_fits_the_campaign_budget(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    # A full fire session ran under FIRE_DAILY_USD live, so the turn cap needs no more.
    assert learning.AGENT_DAILY_USD >= learning.FIRE_DAILY_USD
    campaign = _bare_campaign(tmp_path)
    monkeypatch.setattr(campaign, "overshoot", lambda: _PEAK_OVERSHOOT)
    # reserve_fire and message() both reserve actor_reserve() per fire or turn.
    reserve = campaign.actor_reserve()
    assert reserve == learning.AGENT_DAILY_USD + _PEAK_OVERSHOOT
    peak = _PEAK_CHARGED + _PEAK_FIRES * reserve + reserve + learning.SHUTDOWN_RESERVE
    assert peak <= _PEAK_LIMIT, f"F4 reservation peak {peak} exceeds {_PEAK_LIMIT}"


# Review R1-1: parsed child evidence is redacted before it is stored ---------------------

_CAMPAIGN_KEY = "sk-or-v1-" + "campaign-secret-" * 2
_PLATFORM_KEY = "plat-key-SECRET-abcdef"


def _keyed_campaign(tmp_path: Path) -> Any:
    campaign = _bare_campaign(tmp_path)
    campaign.key = _CAMPAIGN_KEY
    campaign.platform_keys = [_PLATFORM_KEY]
    return campaign


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in _strings(item)]
    if isinstance(value, list | tuple):
        return [s for item in value for s in _strings(item)]
    return []


def _assert_redacted(value: Any) -> None:
    for text in _strings(value):
        assert _CAMPAIGN_KEY not in text
        assert _PLATFORM_KEY not in text


def test_add_redacts_every_nested_evidence_string_without_truncating(tmp_path: Path) -> None:
    campaign = _keyed_campaign(tmp_path)
    long_text = "\n".join(f"line {n} {_CAMPAIGN_KEY}" for n in range(200)) + " " + "x" * 9000
    value = {
        "case_id": "F3-P1-R1",
        "result": {
            "error": {"detail": f"bad key {_CAMPAIGN_KEY}", "hint": [f"try {_PLATFORM_KEY}"]},
            "rows": [{"statement": f"{_PLATFORM_KEY} and {_CAMPAIGN_KEY}"}, 3, None, True],
        },
        "long": long_text,
    }
    ref = campaign.add("cli-result", copy.deepcopy(value))
    stored = campaign.evidence[ref]["value"]
    _assert_redacted(stored)
    assert stored["result"]["error"]["detail"] == "bad key [redacted]"
    assert stored["result"]["error"]["hint"] == ["try [redacted]"]
    assert stored["result"]["rows"] == [{"statement": "[redacted] and [redacted]"}, 3, None, True]
    assert stored["case_id"] == "F3-P1-R1"
    # Redaction only replaces: the long text keeps every line and every character.
    assert stored["long"] == long_text.replace(_CAMPAIGN_KEY, "[redacted]")


def test_cli_redacts_the_parsed_result_it_stores_and_returns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    campaign = _keyed_campaign(tmp_path)
    payload = {
        "error": {"detail": f"bad key {_CAMPAIGN_KEY}", "cause": {"token": _PLATFORM_KEY}},
        "items": [f"echo {_CAMPAIGN_KEY}"],
    }

    def run(argv: Any, **_kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(argv, 1, json.dumps(payload).encode(), b"")

    monkeypatch.setattr(learning.subprocess, "run", run)
    scope = {"repetition": 1, "case_id": "F3-P1-R1", "agent_id": "agent-F3-P1-R1"}
    code, parsed, _invocation, result_ref, _error = campaign.cli(["memory", "list"], scope)
    assert code == 1
    _assert_redacted(parsed)
    assert parsed["error"]["detail"] == "bad key [redacted]"
    assert parsed["error"]["cause"] == {"token": "[redacted]"}
    assert parsed["items"] == ["echo [redacted]"]
    stored = campaign.evidence[result_ref]["value"]["result"]
    assert stored == parsed
    _assert_redacted(campaign.evidence)
    _assert_redacted(campaign.invocations)


# Review R1-3: a reused output directory is refused before any spend ---------------------


def _executable(tmp_path: Path) -> Path:
    binary = tmp_path / "bin" / "curie"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o755)
    return binary


def _drive_run(
    monkeypatch: pytest.MonkeyPatch, binary: Path, output: Path
) -> tuple[int, list[str]]:
    """Run ``main(["run", ...])`` with every live step stubbed. ``started`` records
    whether the campaign began its live sequence (identity is its first step)."""
    from curie_worker.eval import learning

    for name in learning.STRIPPED_AUTH:
        monkeypatch.delenv(name, raising=False)
    started: list[str] = []

    def identity(self: Any) -> None:
        started.append("identity")
        raise learning._Stop("stubbed: no live campaign in unit tests")

    monkeypatch.setattr(learning._Campaign, "identity", identity)
    monkeypatch.setattr(learning._Campaign, "teardown", lambda self: {"verified": False})
    monkeypatch.setattr(
        learning._Campaign,
        "report",
        lambda self, commit, cleanup: {
            "implementation_verification": "FAIL",
            "acceptance": "FAIL",
            "failure_reasons": ["stubbed"],
        },
    )
    code = _module_exit(
        ["run", "--curie-bin", str(binary), "--output-dir", str(output), "--budget-usd", "1"]
    )
    return code, started


@pytest.mark.parametrize("artifact", ["ledger.sqlite", "report.json"])
def test_run_refuses_an_output_dir_that_already_holds_a_campaign(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    artifact: str,
) -> None:
    import sqlite3

    output = tmp_path / "out"
    output.mkdir()
    path = output / artifact
    if artifact == "ledger.sqlite":
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE operations (identity TEXT PRIMARY KEY)")
            db.execute("INSERT INTO operations VALUES ('calibration-operation-1')")
        db.close()
    else:
        path.write_text('{"previous": true}')
    before = path.read_bytes()
    code, started = _drive_run(monkeypatch, _executable(tmp_path), output)
    assert started == [], "a reused output dir must be refused before the campaign starts"
    assert code != 0
    err = capsys.readouterr().err
    assert "output" in err.lower()
    assert "IntegrityError" not in err
    assert path.read_bytes() == before
    assert sorted(p.name for p in output.iterdir()) == [artifact]


@pytest.mark.parametrize("state", ["missing", "empty"])
def test_run_accepts_a_fresh_output_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, state: str
) -> None:
    output = tmp_path / "out"
    if state == "empty":
        output.mkdir()
    _code, started = _drive_run(monkeypatch, _executable(tmp_path), output)
    assert started == ["identity"]


# Review R1-4: follow-ups run in fresh threads ---------------------------------------------


def _reply(text: str, channel: str, thread: str, reply: str = "ok") -> dict[str, Any]:
    return {
        "invocation_id": "invocation-1",
        "result_ref": "cli-result-1",
        "exit_code": 0,
        "reply": reply,
        "thread": thread,
        "channel": channel,
        "finalized": True,
        "generation": "operation-1",
    }


def _tracked_threads(monkeypatch: pytest.MonkeyPatch, campaign: Any) -> list[str]:
    made: list[str] = []
    original = campaign.new_thread

    def new_thread() -> str:
        thread = original()
        made.append(thread)
        return thread

    monkeypatch.setattr(campaign, "new_thread", new_thread)
    return made


def test_f5_revise_and_cancel_each_run_in_a_distinct_fresh_thread(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from curie_worker.eval import learning

    campaign = _bare_campaign(tmp_path)
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "snapshot", lambda _scope: (copy.deepcopy(empty), None))
    monkeypatch.setattr(campaign, "schedule_row", lambda *_a, **_k: (None, "absent"))
    sent: list[tuple[str, str]] = []

    def message(scope: Any, text: str, channel: str, thread: str) -> dict[str, Any]:
        sent.append((text, thread))
        return _reply(text, channel, thread)

    monkeypatch.setattr(campaign, "message", message)
    made = _tracked_threads(monkeypatch, campaign)
    values = learning._scenario_values(datetime.now(UTC))
    raw = next(row for row in campaign.corpus if row["id"] == "F5-P1")
    fixture = learning._substitute(copy.deepcopy(dict(raw)), values)
    teaching = "1800000000.000001"
    campaign.threads.add(teaching)
    scope = {"repetition": 1, "case_id": "F5-P1-R1", "agent_id": "agent-F5-P1-R1"}
    campaign.control_recurring(scope, fixture, scope["agent_id"], teaching, (empty, None), values)
    control = next(c for c in fixture["controls"] if c["id"] == "revise-and-cancel")
    threads = {text: thread for text, thread in sent}
    revise, cancel = (threads[str(text)] for text in control["messages"])
    assert teaching not in (revise, cancel)
    assert revise != cancel
    assert revise in made and cancel in made


_F67 = [f"F{family}-P{phrase}" for family in (6, 7) for phrase in range(1, 4)]
_PLACEHOLDER = re.compile(r"^\{([a-z_]+)\}$")


@pytest.mark.parametrize("fixture_id", _F67)
def test_f6_f7_scenarios_carry_one_fresh_thread_no_reuse_probe(fixture_id: str) -> None:
    from curie_worker.eval import learning

    family = next(
        f
        for f in json.loads((FIXTURES / "learning-cases.json").read_text())["families"]
        if f["id"] == fixture_id[:2]
    )
    scenario = next(s for s in family["scenarios"] if s["id"] == fixture_id)
    probes = scenario["probes"]
    assert [p["id"] for p in probes] == ["fresh-thread-no-reuse"]
    probe = probes[0]
    assert isinstance(probe["channel"], str) and probe["channel"]
    assert isinstance(probe["input"], str) and probe["input"]
    forbidden = probe["forbidden_values"]
    assert isinstance(forbidden, list) and forbidden
    teaching = " ".join(scenario["messages"])
    values = learning._scenario_values(datetime.now(UTC))
    for placeholder in forbidden:
        match = _PLACEHOLDER.match(placeholder)
        assert match, f"{placeholder!r} is not a placeholder"
        assert placeholder in teaching, f"{placeholder} is not in the teaching message"
        assert re.match(r"^[a-z]+-[0-9a-f]{6}$", values[match.group(1)]), placeholder


@pytest.mark.parametrize(
    "reply,passed",
    [
        ("I do not have anything saved about that.", True),
        ("The label was amber-abc123.", False),
        ("The label was AMBER-ABC123.", False),
        ("Either Violet-DEF456 or something else.", False),
    ],
)
def test_absent_control_check_passes_only_when_no_forbidden_value_appears(
    reply: str, passed: bool
) -> None:
    from curie_worker.eval import learning

    check = {
        "kind": "absent",
        "reply": reply,
        "forbidden_values": ["amber-abc123", "violet-def456"],
    }
    assert learning._control_check(check) is passed


@pytest.mark.parametrize("fixture_id", ["F6-P1", "F7-P1"])
@pytest.mark.parametrize("leaked", [False, True])
def test_f6_f7_scenario_sends_the_no_reuse_probe_in_a_fresh_thread(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fixture_id: str, leaked: bool
) -> None:
    from curie_worker.eval import learning

    campaign = _bare_campaign(tmp_path)
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "provision", lambda scope: str(scope["agent_id"]))
    monkeypatch.setattr(campaign, "snapshot", lambda _scope: (copy.deepcopy(empty), None))
    monkeypatch.setattr(campaign, "consent", lambda *_a, **_k: None)
    monkeypatch.setattr(campaign, "reconcile_fires", lambda *_a, **_k: None)
    monkeypatch.setattr(campaign, "lower_fire_cap", lambda *_a, **_k: None)
    monkeypatch.setattr(campaign, "retire_runners", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(
        campaign, "transcript", lambda *_a, **_k: ({"binding_key": "slack:C:T", "messages": []}, [])
    )
    monkeypatch.setattr(
        campaign, "cli", lambda *_a, **_k: (0, {}, "invocation-m", "cli-result-m", "")
    )
    taught: list[tuple[str, str]] = []

    def teach(scope: Any, fixture: Any, agent_id: str, thread: str, index: int) -> Any:
        taught.append((str(fixture["messages"][index]), thread))
        return _reply("", campaign.channels[0], thread), (copy.deepcopy(empty), None)

    monkeypatch.setattr(campaign, "teach", teach)
    sent: list[tuple[str, str, str]] = []
    substituted: dict[str, Any] = {}

    def message(scope: Any, text: str, channel: str, thread: str) -> dict[str, Any]:
        sent.append((text, channel, thread))
        values = campaign.substitutions["values"]
        substituted.update(values)
        reply = "Nothing is saved about that."
        if leaked:
            reply = f"Earlier you mentioned {next(iter(forbidden_values(values))).upper()}."
        return _reply(text, channel, thread, reply)

    raw = next(row for row in campaign.corpus if row["id"] == fixture_id)
    probe = raw["probes"][0]

    def forbidden_values(values: dict[str, str]) -> list[str]:
        return [learning._substitute(v, values) for v in probe["forbidden_values"]]

    monkeypatch.setattr(campaign, "message", message)
    made = _tracked_threads(monkeypatch, campaign)
    campaign.scenario(1, raw)

    teaching_threads = {thread for _text, thread in taught}
    assert len(teaching_threads) == 1
    probe_input = learning._substitute(probe["input"], substituted)
    probes = [(c, t) for text, c, t in sent if text == probe_input]
    assert len(probes) == 1
    _channel, probe_thread = probes[0]
    assert probe_thread in made
    assert probe_thread not in teaching_threads
    controls = [c for c in campaign.controls if c["control_id"] == "fresh-thread-no-reuse"]
    assert len(controls) == 1
    assert controls[0]["status"] == ("FAIL" if leaked else "PASS")
    observation = campaign.evidence[controls[0]["evidence_ref"]]["value"]
    absent = [c for c in observation["checks"] if c.get("kind") == "absent"]
    assert len(absent) == 1
    assert absent[0]["forbidden_values"] == forbidden_values(substituted)
    assert all(re.match(r"^[a-z]+-[0-9a-f]{6}$", v) for v in absent[0]["forbidden_values"])
    assert learning._control_check(absent[0]) is (not leaked)


_RETIRE_SCOPE = {"repetition": 2, "case_id": "F1-P1-R2", "agent_id": _RUNNER_AGENT}


def test_retire_runners_removes_exactly_the_listed_runners_of_this_agent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ids = ["a" * 64, "b" * 64]
    campaign, calls = _runner_campaign(monkeypatch, tmp_path, [ids, []])
    campaign.retire_runners(_RETIRE_SCOPE)
    listing = [c for c in calls if c[:2] == ("container", "ls")]
    assert len(listing) == 1
    assert f"label=curietech.ai/stack={_RUNNER_PROJECT}_runner" in listing[0]
    assert f"label=curietech.ai/agent={_RUNNER_AGENT}" in listing[0]
    assert "status=running" in listing[0]
    assert "{{.ID}}" in listing[0]
    removals = [c for c in calls if c[:2] == ("container", "rm")]
    assert removals == [("container", "rm", "--force", *ids)]
    assert campaign.quiesce_actions[-1] == {
        "repetition": 2,
        "case_id": "F1-P1-R2",
        "agent_id": _RUNNER_AGENT,
        "action": "retire-runners",
        "containers": 2,
    }


def test_retire_runners_makes_no_rm_call_when_nothing_is_running(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    campaign, calls = _runner_campaign(monkeypatch, tmp_path, [[]])
    campaign.retire_runners(_RETIRE_SCOPE)
    assert calls
    assert not [c for c in calls if "rm" in c[:2]]
    assert campaign.quiesce_actions[-1]["action"] == "retire-runners"
    assert campaign.quiesce_actions[-1]["containers"] == 0
    assert "error" not in campaign.quiesce_actions[-1]


@pytest.mark.parametrize("failing", ["ls", "rm"])
def test_retire_runners_records_and_swallows_a_docker_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failing: str
) -> None:
    from curie_worker.eval import learning

    campaign, _calls = _runner_campaign(monkeypatch, tmp_path, [[]])
    seen: list[tuple[str, ...]] = []

    def docker_lines(*args: str) -> list[str]:
        seen.append(args)
        if args[1] == failing:
            raise learning._Invalid("the caller's Docker daemon could not be observed")
        return ["c" * 64]

    monkeypatch.setattr(learning, "_docker_lines", docker_lines)
    campaign.retire_runners(_RETIRE_SCOPE)
    action = campaign.quiesce_actions[-1]
    assert action["action"] == "retire-runners"
    assert action["agent_id"] == _RUNNER_AGENT
    assert action["containers"] == 0
    assert isinstance(action["error"], str)
    assert action["error"]


def test_retire_runners_swallows_os_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    campaign, _calls = _runner_campaign(monkeypatch, tmp_path, [OSError("docker missing")])
    campaign.retire_runners(_RETIRE_SCOPE)
    action = campaign.quiesce_actions[-1]
    assert action["containers"] == 0
    assert action["error"]


def test_scenario_retires_runners_once_after_its_controls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    campaign = _bare_campaign(tmp_path)
    order: list[str] = []
    retired: list[dict[str, Any]] = []
    empty = {"skills": [], "facts": [], "schedules": []}
    monkeypatch.setattr(campaign, "provision", lambda scope: str(scope["agent_id"]))
    monkeypatch.setattr(campaign, "consent", lambda *_a, **_k: None)
    monkeypatch.setattr(campaign, "new_thread", lambda: "thread-1")
    monkeypatch.setattr(
        campaign,
        "teach",
        lambda *_a, **_k: (
            _reply("", campaign.channels[0], "thread-1"),
            (copy.deepcopy(empty), None),
        ),
    )
    monkeypatch.setattr(campaign, "control_procedure", lambda *_a, **_k: order.append("control"))
    monkeypatch.setattr(campaign, "reconcile_fires", lambda *_a, **_k: order.append("reconcile"))
    monkeypatch.setattr(campaign, "lower_fire_cap", lambda *_a, **_k: order.append("cap"))

    def retire(scope: Any) -> None:
        order.append("retire")
        retired.append(dict(scope))

    monkeypatch.setattr(campaign, "retire_runners", retire)
    raw = next(row for row in campaign.corpus if str(row["id"]).startswith("F1"))
    campaign.scenario(3, raw)
    assert order.count("retire") == 1
    assert order.index("control") < order.index("reconcile") < order.index("retire")
    assert retired[0]["repetition"] == 3
    assert retired[0]["case_id"] == f"{raw['id']}-R3"
    assert retired[0]["agent_id"].startswith("le-")
