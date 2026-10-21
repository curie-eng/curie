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
from collections import Counter
from collections.abc import Callable
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
            "budget_usd": "5.00",
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
                        {**scope, "control_id": control_id, "status": "PASS"},
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
        accounting["charged_usd"] = "5.01"
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
        assert Decimal(report["accounting"]["budget_usd"]) <= Decimal("5.00")
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
    # The same operation held unresolved at its full reservation is honest and passes.
    operation.pop("settled_usd")
    operation.pop("usage_polls")
    operation.update(status="unresolved", charged_usd=operation["reserve_usd"])
    _rehash(report)
    _check(tmp_path, report, passed=True)


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
    "control_id", ["instruction-correction", "conflicting-request", "revise-and-cancel", "revert"]
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
